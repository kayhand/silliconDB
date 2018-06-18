#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>

#include "thread/Thread.h"
#include "thread/ThreadHandler.h"
#include "thread/Syncronizer.h"
#include "network/server/TCPAcceptor.h"
#include "data/DataLoader.h"
#include "data/Partitioner.h"
#include "util/Query.h"
#include "util/WorkQueue.h"
#include "sched/ProcessingUnit.h"
#include "util/Helper.h"

std::atomic<int> li_cnt = ATOMIC_VAR_INIT(0);
std::atomic<int> o_cnt = ATOMIC_VAR_INIT(0);

int main(int argc, char** argv) {
	if (argc < 9) {
		printf(
				"usage: %s <workers> <port> <ip> <fileName> <fileName2> <part_size> <scale_factor> <technique> \n",
				argv[0]);
		exit(-1);
	}

	int workers = atoi(argv[1]);
	int port = atoi(argv[2]);
	string ip = argv[3];
	string fileName = argv[4]; //lineorder
	string fileName2 = argv[5]; //supplier
	string fileName3 = argv[6]; //customer
	string fileName4 = argv[7]; //date
	int part_size = atoi(argv[8]);
	int dax_queue_size = atoi(argv[9]);
	int sf = atoi(argv[10]);
	int technique = atoi(argv[11]); // 0: sDB, 1: op_at_a_time, 2: data division

	//Partitioner: Init partitioners
	//Partitioner: Create partition metadata
	Partitioner line_part;
	line_part.roundRobin(fileName, part_size);
	Partitioner supp_part;
	supp_part.roundRobin(fileName2, part_size);
	Partitioner customer_part;
	customer_part.roundRobin(fileName3, part_size);
	Partitioner date_part;
	date_part.roundRobin(fileName4, part_size);

	//DataLoader: Init loader
	//DataLoader: Create data compressor (merge with initialization)
	//DataLoader: Do compression
	DataLoader data_loader(4);

	data_loader.initializeCompressor(fileName2, 1, 4, supp_part, sf); //4: s_nation
	data_loader.parseTable(1);
	data_loader.compressTable(1);

	data_loader.initializeCompressor(fileName3, 2, 4, customer_part, sf); //4: c_nation
	data_loader.parseTable(2);
	data_loader.compressTable(2);

	data_loader.initializeCompressor(fileName4, 3, 4, date_part, sf); //4: d_year
	data_loader.parseTable(3);
	data_loader.compressTable(3);

	data_loader.initializeCompressor(fileName, 0, -1, line_part, sf); //-1: no lineorder columns as agg. key
	data_loader.parseTable(0);
	data_loader.compressTable(0);

	//SiliconDB Handler
	// 1) Create processing units and resource handlers for them
	// 2) Create network and start listening for queries
	// 3) When client sends the query, create and populate initial queues for each resource handler 

	TCPAcceptor* connectionAcceptor;
	connectionAcceptor = new TCPAcceptor(port, (char*) ip.c_str());
	if (!connectionAcceptor || connectionAcceptor->start() != 0) {
		printf("Could not create a connection acceptor\n");
		exit(1);
	}

	std::vector<ProcessingUnit*> proc_units;
	proc_units.reserve(10);

	EXEC_TYPE e_type = EXEC_TYPE::SDB;
	if(technique == 0)
		e_type = EXEC_TYPE::SDB;
	else if(technique == 1)
		e_type = EXEC_TYPE::OAT;
	else if(technique == 2)
		e_type = EXEC_TYPE::DD;

	//Synchronization: Init params
	int num_of_barriers = workers + 1;
	Syncronizer thr_sync;
	thr_sync.initBarriers(num_of_barriers);
	thr_sync.initAggCounter(line_part.getNumberOfParts());

	for (int i = 0; i < 1; i++) {
		ProcessingUnit *proc_unit = new ProcessingUnit(workers);
		proc_unit->addCompressedTable(data_loader.getDataCompressor(0));
		proc_unit->addCompressedTable(data_loader.getDataCompressor(1));
		proc_unit->addCompressedTable(data_loader.getDataCompressor(2));
		proc_unit->addCompressedTable(data_loader.getDataCompressor(3));

		proc_unit->initializeAPI();
		proc_unit->createProcessingUnit(&thr_sync, e_type, dax_queue_size);
		if (e_type == EXEC_TYPE::SDB || e_type == EXEC_TYPE::OAT) {
			proc_unit->addWork(date_part.getNumberOfParts(), 3, sf,
					proc_unit->getSharedQueue());
			proc_unit->addWork(customer_part.getNumberOfParts(), 2, sf,
					proc_unit->getSharedQueue());
			proc_unit->addWork(supp_part.getNumberOfParts(), 1, sf,
					proc_unit->getSharedQueue());
			proc_unit->addWork(line_part.getNumberOfParts(), 0, sf,
					proc_unit->getSharedQueue());
		}
		else if(e_type == EXEC_TYPE::DD){
			int totalDimScans = date_part.getNumberOfParts() + customer_part.getNumberOfParts() + supp_part.getNumberOfParts();
			int totalScans = totalDimScans + line_part.getNumberOfParts();
			int totalDaxScans = totalScans / (2.56 + 1) * 2.56;

			printf("Dax will do %d scans out of %d in total!\n", totalDaxScans, totalScans);

			proc_unit->addWork(date_part.getNumberOfParts(), 3, sf, proc_unit->getHWQueue());
			proc_unit->addWork(customer_part.getNumberOfParts(), 2, sf, proc_unit->getHWQueue());
			proc_unit->addWork(supp_part.getNumberOfParts(), 1, sf, proc_unit->getHWQueue());
			totalDaxScans -= totalDimScans;
			proc_unit->addWork(totalDaxScans, 0, sf, proc_unit->getHWQueue());

			//Add remaining lineorder scans to the sw_queue
			for (int p_id = totalDaxScans; p_id < line_part.getNumberOfParts(); p_id++) {
				Query item(0, p_id, 0); //(0: sw - 1:hw, part_id, table_id)
				Node<Query> *newNode = new Node<Query>(item);
				proc_unit->getSWQueue()->add(newNode);
			}

			printf("DAX Scan Queue\n");
			proc_unit->getHWQueue()->printQueue();
			printf("Core Scan Queue\n");
			proc_unit->getSWQueue()->printQueue();
		}

		proc_units.push_back(proc_unit);
	}

	int numberOfConnections = 1;
	TCPStream* connection;
	while (numberOfConnections > 0) {
		printf("Gimme some connection!\n");
		connection = connectionAcceptor->accept();
		if (!connection) {
			printf("Could not accept a connection!\n");
			continue;
		}

		for (ProcessingUnit *proc_unit : proc_units) {
			proc_unit->startThreads(connection);
		}

		for (ProcessingUnit *proc_unit : proc_units) {
			proc_unit->joinThreads();
		}

		numberOfConnections--;
		thr_sync.destroyBarriers();
		delete connection;
	}

	for (ProcessingUnit *proc_unit : proc_units) {
		proc_unit->writeResults();
		delete proc_unit;
	}
	delete connectionAcceptor;
	return 0;
}

