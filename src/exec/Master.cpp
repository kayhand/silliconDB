#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#include <istream>

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
#include "api/ParserApi.h"

std::unordered_map<string, string> params{
	{"num_of_cores", "1"},
	{"port_id", "9999"},
	{"client", "localhost"},
	{"data_path", "/tmp/ssb_data/toy_data/"},
	{"part_size", "12800"},
	{"dax_queue_size", "1"},
	{"sf", "1"},
	{"scheduling", "operator_at_a_time"},
	{"q_id", "31"}
};

void setParams(int argc, char **argv){
	string paramPair;

	for(int argId = 1; argId < argc; argId++){
		paramPair = argv[argId];
		cout << paramPair << endl;
		int splitPos = paramPair.find(':'); // 12
		string param = paramPair.substr(0, splitPos); //"num_of_cores"
		string val = paramPair.substr(splitPos + 1); //"4"

		if(params.find(param) == params.end()){
			printf("Undefined param name: %s!\n", param.c_str());
		}
		else{
			params[param] = val;
		}
	}

	printf("\n+++++++++PARAMS++++++\n");
	for(auto parPair : params){
		std::cout << parPair.first << " : " << parPair.second << std::endl;
	}
	printf("+++++++++++++++++++++\n\n");
}

int main(int argc, char** argv) {
	if(argc == 1){
		printf("\nNo parameters specified, will start with default paramaters!\n\n");
	}
	else{
		printf("\n%d parameters specified, will override the default values!\n\n", argc-1);
	}
	setParams(argc, argv);

	int workers = atoi(params["num_of_cores"].c_str());
	int port = atoi(params["port_id"].c_str());
	int part_size = atoi(params["part_size"].c_str());
	int dax_queue_size = atoi(params["dax_queue_size"].c_str());
	int sf = atoi(params["sf"].c_str());
	string queryFile = params["q_id"];
	string ip = params["client"];
	string dataPath = params["data_path"];
	string technique = params["scheduling"]; // 0: sDB, 1: op_at_a_time, 2: data division
	//int q_id = atoi(queryFile.c_str()); //31: SSB Q3_1, 32: SSB Q3_2 ...

	cout << "technique: " << technique << endl;

	DataLoader dataLoader(queryFile);
	dataLoader.processTables(dataPath, part_size, sf);

	//dataLoader.initializeDataCompressors(dataPath, part_size, sf);
	//dataLoader.parseTables();
	//dataLoader.compressTables();

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

	//Synchronization: Init params
	int num_of_barriers = workers + 1;
	Syncronizer thr_sync;
	thr_sync.initBarriers(num_of_barriers);
	thr_sync.initAggCounter(dataLoader.TotalFactParts());

	for (int i = 0; i < 1; i++) {
		ProcessingUnit *proc_unit = new ProcessingUnit(workers, technique, &dataLoader);

		//proc_unit->addCompressedTables(dataLoader.getDataCompressors());
		proc_unit->initializeAPI(dataLoader.getQueryParser());
		proc_unit->createProcessingUnit(&thr_sync, dax_queue_size);
		proc_unit->initWorkQueues(sf);

		proc_units.push_back(proc_unit);
	}

	int numberOfConnections = 1;
	TCPStream* connection = NULL;
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

