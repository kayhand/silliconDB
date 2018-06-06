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
#include "thread/CoreQtBenchHandler.h"
#include "thread/DaxQtBenchHandler.h"

#include "util/Query.h"
#include "data/DataCompressor.h"
#include "exec/util/WorkQueue.h"
#include "thread/Syncronizer.h"

#include "api/ScanApi.h"
#include "api/JoinApi.h"
#include "api/AggApi.h"

class ProcessingUnit{
    std::vector<CoreQtBenchHandler<Query>*> coreHandlers;
    #ifdef __sun 
        std::vector<DaxQtBenchHandler<Query>*> daxHandlers;
    #endif

    int numOfComputeUnits;

    WorkQueue<Query> sharedQueue; 
    WorkQueue<Query> swQueue; 
    WorkQueue<Query> hwQueue;

    std::vector<DataCompressor*> dataArr;
    std::vector<ScanApi*> scanAPIs;
    std::vector<JoinApi*> joinAPIs;
    std::vector<AggApi*> aggAPIs;

    public: 
        ProcessingUnit(int);
        ~ProcessingUnit();

    void addCompressedTable(DataCompressor*);
    void createProcessingUnit(Syncronizer*, EXEC_TYPE, int);
    void addWork(int, int, int, WorkQueue<Query>*);
    void startThreads(TCPStream*);
    void joinThreads();
    void writeResults();

    void initializeAPI();


    WorkQueue<Query>* getSharedQueue(){
        return &sharedQueue;
    }

    WorkQueue<Query>* getSWQueue(){
        return &swQueue;
    }

    WorkQueue<Query>* getHWQueue(){
        return &hwQueue;
    }
};

#endif
