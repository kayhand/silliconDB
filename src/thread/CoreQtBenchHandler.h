#ifndef __core_qt_bench_handler_h__
#define __core_qt_bench_handler_h__

#include <pthread.h>
#include <vector>
#include <atomic>

#include "Thread.h"
#include "ThreadHandler.h"
#include "Syncronizer.h"

#include "exec/util/WorkQueue.h"
#include "network/server/TCPAcceptor.h"
#include "log/Result.h"

#include "bench/vldb2018/tpch/Query3.h"
#include "api/ScanApi.h"

#ifdef __sun 
#include <sys/processor.h>
#include <sys/procset.h>
#include <thread.h>
#endif

template <class T>
class CoreQtBenchHandler : public ThreadHandler<T>
{
    void startExec(WorkQueue<T> *work_queue){
        Node<T> *node_item;
	T item;

	ScanApi *api_ls = this->scanAPIs[0];
	ScanApi *api_ds = this->scanAPIs[1];
	ScanApi *api_cs = this->scanAPIs[2];

        while(work_queue->getHead() != work_queue->getTail()){
//	    break;
            node_item = work_queue->remove_ref_core();
	    if(node_item == NULL)
	        break;
            item = node_item->value;
	    if(item.getTableId() == 0){
//            	Query3::simdScan_16(this->dataVector[0], item.getPart(), 8, &(this->result), true);
		api_ls->simdScan16(node_item, &(this->result));
            	this->putFreeNode(node_item);
//		this->addNewJob(1, item.getPart(), 11, work_queue);
	    }
	    else if(item.getTableId() == 1){
//            	Query3::simdScan_16(this->dataVector[1], item.getPart(), 4, &(this->result), false);
		api_ds->simdScan16(node_item, &(this->result));
            	this->putFreeNode(node_item);
	    }
	    else if(item.getTableId() == 2){
//            	Query3::simdScan_16(this->dataVector[1], item.getPart(), 4, &(this->result), false);
		api_cs->simdScan16(node_item, &(this->result));
            	this->putFreeNode(node_item);
	    }

	    else if(item.getTableId() == 11){
	        //Query3::join_sw(this->dataVector[0], this->dataVector[1], item.getPart(), &(this->result));
		this->putFreeNode(node_item);
		this->addNewJob(1, item.getPart(), 11, work_queue);
		break;
	    }
        }
    }

    public:
        CoreQtBenchHandler(Syncronizer *sync, WorkQueue<T> *shared_queue, std::vector<DataCompressor*> &dataVector ) : ThreadHandler <T> (sync, shared_queue, dataVector){}
        //CoreQtBenchHandler(Syncronizer *sync, ProcessingUnit *procUnit) : ThreadHandler <T> (sync, procUnit){}

    void *run(){
        this->thr_sync->waitOnStartBarrier();

        this->startExec(this->shared_queue);

        this->thr_sync->waitOnAggBarrier();
        this->thr_sync->waitOnEndBarrier();

        this->t_stream->send("", 0);
        return NULL;
    }

    void setSWQueue(WorkQueue<T> *queue){
        this->sw_queue = queue;
    }

};

#endif
