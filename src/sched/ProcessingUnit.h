#ifndef __processing_unit_h__
#define __processing_unit_h__

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

#include "../thread/Thread.h"
#include "../thread/ThreadHandler.h"

#include "../util/Query.h"
#include "../data/DataCompressor.h"
#include "../exec/util/WorkQueue.h"
#include "../thread/Syncronizer.h"

class ProcessingUnit{
    std::vector<CoreHandler<Query>*> coreHandlers;
    #ifdef __sun 
        std::vector<DaxHandler<Query>*> daxHandlers;
    #endif

    int numOfComputeUnits;

    WorkQueue<Query> sharedQueue; 
    WorkQueue<Query> swQueue; 
    WorkQueue<Query> daxQueue;

    std::vector<DataCompressor*> dataArr;

    public: 
        ProcessingUnit(int);
        ~ProcessingUnit();

    void addCompressedTable(DataCompressor*);
    void createProcessingUnit(Syncronizer*);
    void addWork(int, int, int);
    void startThreads(TCPStream*);
    void joinThreads();
    void writeResults();

    WorkQueue<Query>& getSharedQueue(){
        return sharedQueue;
    }

    WorkQueue<Query>& getSWQueue(){
        return swQueue;
    }

    WorkQueue<Query>& getHWQueue(){
        return daxQueue;
    }
};

#endif
