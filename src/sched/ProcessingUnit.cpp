#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

#include "ProcessingUnit.h"

ProcessingUnit::ProcessingUnit(int num_of_cu){
    this->numOfComputeUnits = num_of_cu;
}

ProcessingUnit::~ProcessingUnit(){
    for(CoreHandler<Query>* curHandler : coreHandlers)
        delete curHandler; 
    #ifdef __sun 
        for(DaxHandler<Query>* daxHandler : daxHandlers)
            delete daxHandler;
    #endif

    delete sharedQueue.getHead();
    delete swQueue.getHead();
    delete daxQueue.getHead();
}

void ProcessingUnit::addCompressedTable(DataCompressor *compTable){
    dataArr.push_back(compTable);
}

void ProcessingUnit::createProcessingUnit(Syncronizer *thr_sync){
    for(int i = 0; i < numOfComputeUnits; i++){
	CoreHandler<Query> *handler = new CoreHandler<Query>(thr_sync, &sharedQueue, dataArr);
	handler->setId(i);
	coreHandlers.push_back(handler);	
    }
    #ifdef __sun
        DaxHandler<Query> *daxHandler = new DaxHandler<Query>(thr_sync, &sharedQueue, dataArr, 4, false);
        daxHandler->setId(63);
	daxHandlers.push_back(daxHandler);
    #endif
}

void ProcessingUnit::addWork(int total_parts, int table_id, int sf){
    for(int i = 0; i <  1 * sf; i++){
        for(int p_id = 0; p_id < total_parts; p_id++){
	    Query item(0, p_id, table_id); //(0: sw - 1:hw, part_id, table_id)
	    Node<Query> *newNode = new Node<Query>(item);
	    sharedQueue.add(newNode);
	 }
    }
}	

void ProcessingUnit::startThreads(TCPStream* connection){
    sharedQueue.printQueue();
    for(CoreHandler<Query> *curHandler : coreHandlers){
        curHandler->setStream(connection);
        curHandler->start(curHandler->getId(), false);
    }
    #ifdef __sun 
        for(DaxHandler<Query> *daxHandler : daxHandlers){
            daxHandler->setStream(connection);
            daxHandler->start(daxHandler->getId(), true);
        }
    #endif
}

void ProcessingUnit::joinThreads(){
    #ifdef __sun 
        for(DaxHandler<Query>* daxHandler : daxHandlers){
            daxHandler->join();
        }
    #endif
    for(CoreHandler<Query>* curHandler : coreHandlers){
        curHandler->join();
    }
}

/*Move this to another class*/
void ProcessingUnit::writeResults(){
    FILE *sscan_f = fopen("sw_scan.txt", "a");
    FILE *dscan_f = fopen("dax_scan.txt", "a");
    FILE *agg_f = fopen("agg.txt", "a");
    FILE *aggr_f = fopen("agg_result.txt", "a");
    FILE *count_f = fopen("count.txt", "a");
    FILE *countr_f = fopen("count_result.txt", "a");
    FILE *join_f = fopen("join.txt", "a");

    unordered_map<uint32_t, float> agg_result_f;

    vector<int> count_result_f {0, 0, 0, 0}; //0: lineitem, 1: orders, 2: join, 3: count

    for(CoreHandler<Query>* curHandler : coreHandlers){
        Result result = curHandler->getResult();

    	result.writeResults(SW_SCAN, sscan_f);
    	result.writeResults(DAX_SCAN, dscan_f);
    	result.writeResults(AGG, agg_f);
    	result.writeResults(JOIN, join_f);
    	result.writeAggResultsQ3(agg_result_f);
	result.writeCountResults(count_result_f);
    }
    Result result = daxHandlers[0]->getResult();
    result.writeResults(SW_SCAN, sscan_f);
    result.writeResults(DAX_SCAN, dscan_f);
    result.writeResults(AGG, agg_f);
    result.writeResults(JOIN, join_f);
    result.writeCountResults(count_result_f);
	     
    for(auto &curr : agg_result_f)
        fprintf(aggr_f, "%d -> %.2lf\n", curr.first, curr.second);

    fprintf(countr_f, "l_count: %d\n", count_result_f[0]);
    fprintf(countr_f, "o_count: %d\n", count_result_f[1]);
    fprintf(countr_f, "j_count: %d\n", count_result_f[2]);

    fclose(sscan_f);
    fclose(dscan_f);
    fclose(agg_f);
    fclose(aggr_f);
    fclose(count_f);
    fclose(countr_f);
    fclose(join_f);
}
