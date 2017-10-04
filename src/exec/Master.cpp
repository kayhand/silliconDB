#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>

#include "../thread/Thread.h"
#include "../thread/ThreadHandler.h"
#include "../thread/Syncronizer.h"
#include "../network/server/TCPAcceptor.h"
#include "../data/DataLoader.h"
#include "../data/Partitioner.h"
#include "../util/Query.h"
#include "../bench/vldb2018/tpch/Query3.h"
#include "util/WorkQueue.h"
#include "../sched/ProcessingUnit.h"

std::atomic<int> li_cnt = ATOMIC_VAR_INIT(0);
std::atomic<int> o_cnt = ATOMIC_VAR_INIT(0);

int main(int argc, char** argv)
{
	if ( argc < 7 ) {
		printf("usage: %s <workers> <port> <ip> <fileName> <fileName2> <part_size> [scale_factor] \n", argv[0]);
		exit(-1);
	}

	int workers = atoi(argv[1]);
	int port = atoi(argv[2]);
	string ip = argv[3];
	string fileName = argv[4];
	string fileName2 = argv[5];
	int part_size = atoi(argv[6]);
	int sf = 1;
	if(argc == 8)
	    sf = atoi(argv[7]);

	//Synchronization: Init params
	int num_of_barriers = workers + 1;
	Syncronizer thr_sync;
	thr_sync.initBarriers(num_of_barriers);

	//Partitioner: Init partitioners
	//Partitioner: Create partition metadata
	Partitioner line_part;
	line_part.roundRobin(fileName, part_size); 
	Partitioner order_part;
	order_part.roundRobin(fileName2, part_size); 

	//DataLoader: Init loader
	//DataLoader: Create data compressor (merge with initialization)
	//DataLoader: Do compression
	DataLoader data_loader(2);
	data_loader.initializeCompressor(fileName, 0, line_part, sf);
	data_loader.parseTable(0);
	data_loader.compressTable(0);

	data_loader.initializeCompressor(fileName2, 1, order_part, sf);
	data_loader.parseTable(1);
	data_loader.compressTable(1);

	//SiliconDB Handler
	// 1) Create processing units and resource handlers for them
	// 2) Create network and start listening for queries
	// 3) When client sends the query, create and populate initial queues for each resource handler 

	TCPAcceptor* connectionAcceptor;
	connectionAcceptor = new TCPAcceptor(port, (char*)ip.c_str());
	if (!connectionAcceptor || connectionAcceptor->start() != 0) {
		printf("Could not create a connection acceptor\n");
		exit(1);
	}

	std::vector<ProcessingUnit*> proc_units;
    	proc_units.reserve(10);
	
        for(int i = 0; i < 1; i++){
	    ProcessingUnit *proc_unit = new ProcessingUnit(workers);
	    proc_unit->addCompressedTable(data_loader.getDataCompressor(0));
	    proc_unit->addCompressedTable(data_loader.getDataCompressor(1));

	    proc_unit->createProcessingUnit(&thr_sync);
	    //for(int i = 0; i < 10; i++){
	    	proc_unit->addWork(line_part.getNumberOfParts(), 0, sf);
	    	proc_unit->addWork(order_part.getNumberOfParts(), 1, sf);
	    //}
	    proc_units.push_back(proc_unit);
        }

        int numberOfConnections = 1;
        TCPStream* connection;
        while(numberOfConnections > 0){
	    printf("Gimme some connection!\n");
	    connection = connectionAcceptor->accept();
	    if(!connection){
	        printf("Could not accept a connection!\n");
	        continue;
	    }
	  
	    for(ProcessingUnit *proc_unit : proc_units){
		proc_unit->startThreads(connection);
	    }

	    for(ProcessingUnit *proc_unit : proc_units){
		proc_unit->joinThreads();
	    }

	    numberOfConnections--;
	    thr_sync.destroyBarriers();
	    delete connection;
        }   

    for(ProcessingUnit *proc_unit : proc_units){
    	proc_unit->writeResults();
	delete proc_unit;
    }
    delete connectionAcceptor;
    return 0;
}


