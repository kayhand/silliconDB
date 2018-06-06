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
    for(CoreQtBenchHandler<Query>* curHandler : coreHandlers)
        delete curHandler; 
    #ifdef __sun 
        for(DaxQtBenchHandler<Query>* daxHandler : daxHandlers)
            delete daxHandler;
    #endif

    delete sharedQueue.getHead();
    delete swQueue.getHead();
    delete hwQueue.getHead();

    for(ScanApi *curScan : scanAPIs)
        delete curScan;
    for(JoinApi *curJoin : joinAPIs)
        delete curJoin;
    for(AggApi *curAgg : aggAPIs)
        delete curAgg;
}

void ProcessingUnit::addCompressedTable(DataCompressor *compTable){
    dataArr.push_back(compTable);
}

void ProcessingUnit::createProcessingUnit(Syncronizer *thr_sync, EXEC_TYPE e_type, int dax_queue_size){
    for(int i = 0; i < numOfComputeUnits; i++){
	CoreQtBenchHandler<Query> *handler = new CoreQtBenchHandler<Query>(thr_sync, &sharedQueue, dataArr);
	//CoreQtBenchHandler<Query> *handler = new CoreQtBenchHandler<Query>(thr_sync, this);
	handler->setId(i);
	handler->setExecType(e_type);
	handler->setQueues(&swQueue, &hwQueue);
	handler->setAPIs(scanAPIs, joinAPIs, aggAPIs);
	coreHandlers.push_back(handler);	
    }
    #ifdef __sun
        DaxQtBenchHandler<Query> *daxHandler = new DaxQtBenchHandler<Query>(thr_sync, &sharedQueue, dataArr, dax_queue_size, true);
        //DaxQtBenchHandler<Query> *daxHandler = new DaxQtBenchHandler<Query>(thr_sync, dax_queue_size, true, this);
        daxHandler->setId(63);
	daxHandler->setExecType(e_type);
	daxHandler->setQueues(&swQueue, &hwQueue);
	daxHandler->setAPIs(scanAPIs, joinAPIs, aggAPIs);
	daxHandlers.push_back(daxHandler);
    #endif
}

void ProcessingUnit::addWork(int total_parts, int table_id, int sf, WorkQueue<Query> *queue){
    for(int i = 0; i <  1 * sf; i++){
        for(int p_id = 0; p_id < total_parts; p_id++){
	    Query item(0, p_id, table_id); //(0: sw - 1:hw, part_id, table_id)
	    Node<Query> *newNode = new Node<Query>(item);
	    queue->add(newNode);
	 }
    }
    queue->printQueue();
}	

void ProcessingUnit::initializeAPI(){
    scanAPIs.reserve(3);
    joinAPIs.reserve(3);
    aggAPIs.reserve(3);

    ScanApi *lo_scan = new ScanApi(dataArr[0], 8);
    ScanApi *s_scan = new ScanApi(dataArr[1], 5);
    ScanApi *c_scan = new ScanApi(dataArr[2], 5);

    scanAPIs.push_back(lo_scan);
    scanAPIs.push_back(s_scan);
    scanAPIs.push_back(c_scan);

    JoinApi *lc_join = new JoinApi(lo_scan, c_scan, 2); //2: lo_custkey
    JoinApi *ls_join = new JoinApi(lo_scan, s_scan, 4); //4: lo_suppkey

    joinAPIs.push_back(lc_join);
    joinAPIs.push_back(ls_join);

    AggApi *api_agg = new AggApi(lc_join, ls_join);
    aggAPIs.push_back(api_agg);
}

void ProcessingUnit::startThreads(TCPStream* connection){
    for(CoreQtBenchHandler<Query> *curHandler : coreHandlers){
        curHandler->setStream(connection);
        curHandler->start(curHandler->getId(), false);
    }
    #ifdef __sun 
        for(DaxQtBenchHandler<Query> *daxHandler : daxHandlers){
            daxHandler->setStream(connection);
            daxHandler->start(daxHandler->getId(), true);
        }
    #endif
}

void ProcessingUnit::joinThreads(){
    #ifdef __sun 
        for(DaxQtBenchHandler<Query>* daxHandler : daxHandlers){
            daxHandler->join();
        }
    #endif
    for(CoreQtBenchHandler<Query>* curHandler : coreHandlers){
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
    FILE *sjoin_f = fopen("sw_join.txt", "a");
    FILE *djoin_f = fopen("dax_join.txt", "a");

    unordered_map<string, uint64_t> agg_result_f;

    //0: lineitem, 1: date, 2: customer, 3: j1, 4: j2, 5: and, 6: agg
    vector<int> count_result_f {0, 0, 0, 0, 0, 0, 0}; 

    for(CoreQtBenchHandler<Query>* curHandler : coreHandlers){
        Result result = curHandler->getResult();

    	result.writeResults(SW_SCAN, sscan_f);
    	result.writeResults(DAX_SCAN, dscan_f);
    	result.writeResults(AGG, agg_f);
    	result.writeResults(SW_JOIN, sjoin_f);
    	result.writeResults(DAX_JOIN, djoin_f);
    	result.writeAggResults(agg_result_f);
	result.writeCountResults(count_result_f);
    }
    Result result = daxHandlers[0]->getResult();
    result.writeResults(SW_SCAN, sscan_f);
    result.writeResults(DAX_SCAN, dscan_f);
    result.writeResults(AGG, agg_f);
    result.writeResults(SW_JOIN, sjoin_f);
    result.writeResults(DAX_JOIN, djoin_f);
    result.writeCountResults(count_result_f);
	     
    for(auto &curr : agg_result_f)
        fprintf(aggr_f, "%s -> %lu\n", curr.first.c_str(), curr.second);

    fprintf(countr_f, "\nlo_count: %d\n", count_result_f[0]);
    fprintf(countr_f, "d_count: %d\n", count_result_f[1]);
    fprintf(countr_f, "c_count: %d\n", count_result_f[2]);
    fprintf(countr_f, "j1_count: %d\n", count_result_f[3]);
    fprintf(countr_f, "j2_count: %d\n", count_result_f[4]);
    fprintf(countr_f, "and_count: %d\n", count_result_f[5]);
    fprintf(countr_f, "agg_count: %d\n", count_result_f[6]);

    fclose(sscan_f);
    fclose(dscan_f);
    fclose(agg_f);
    fclose(aggr_f);
    fclose(count_f);
    fclose(countr_f);
    fclose(sjoin_f);
    fclose(sjoin_f);
}
