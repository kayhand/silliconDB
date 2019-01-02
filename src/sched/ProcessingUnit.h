#ifndef __processing_unit_h__
#define __processing_unit_h__

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

#include "thread/Thread.h"
#include "thread/ThreadHandler.h"
#include "thread/CoreHandler.h"
#include "thread/DaxHandler.h"

#include "util/Query.h"
#include "data/DataCompressor.h"
#include "exec/util/WorkQueue.h"
#include "thread/Syncronizer.h"

#include "api/ScanApi.h"
#include "api/JoinApi.h"
#include "api/AggApi.h"

class ProcessingUnit{
    std::vector<CoreHandler<Query>*> coreHandlers;
    std::vector<DaxHandler<Query>*> daxHandlers;

    int numOfComputeUnits = 1;
    EXEC_TYPE sched_approach = EXEC_TYPE::SDB;

    WorkQueue<Query> sharedQueue; 
    WorkQueue<Query> swQueue; 
    WorkQueue<Query> hwQueue;

    DataLoader *dataLoader;

    std::vector<ScanApi*> factScanAPIs;
    std::vector<ScanApi*> dimScanAPIs;
    std::vector<JoinApi*> joinAPIs;
    AggApi* aggAPI = NULL;

    public: 
        //ProcessingUnit(int, string);
        ProcessingUnit(int, EXEC_TYPE, DataLoader*);
        ~ProcessingUnit();

    void createProcessingUnit(Syncronizer*, int);
    void addScanItems(int, JOB_TYPE, int, WorkQueue<Query>*);
    void initWorkQueues(int);
    void startThreads(TCPStream*);
    void joinThreads();
    void writeResults();

    void initializeAPI(ParserApi&);

    WorkQueue<Query>* getSharedQueue(){
        return &sharedQueue;
    }

    WorkQueue<Query>* getSWQueue(){
        return &swQueue;
    }

    WorkQueue<Query>* getHWQueue(){
        return &hwQueue;
    }

    void printAPIInfo(){
    	cout << "===========" << endl;
    	cout << "AGG..." << endl;
    	cout << "Fact table id: " << aggAPI->FactTable()->t_meta.t_id << endl;

    	for(JoinApi *curJoin : aggAPI->Joins()){
    		cout << "Join Col. Id " << curJoin->JoinColId() << endl;
    		printf("Dimension table id: %d\n", curJoin->DimensionScan()->getBaseTable()->t_meta.t_id);
    	}
    }

    int totalFactParts(){
    	int sum = 0;
    	for(ScanApi *factScan : factScanAPIs)
    		sum += factScan->getBaseTable()->NumOfParts();
    	return sum;
    }

    int totalDimParts(){
    	int sum = 0;
    	for(ScanApi *dimScan : dimScanAPIs)
    		sum += dimScan->getBaseTable()->NumOfParts();
    	return sum;
    }
};

#endif
