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

#ifdef __sun 
#include <sys/processor.h>
#include <sys/procset.h>
#include <thread.h>
#endif

template <class T>
class CoreQtBenchHandler : public ThreadHandler<T>
{
    void startExec(WorkQueue<T> *work_queue, WorkQueue<T> *join_queue){
        Node<T> *node_item;
	T item;
        while(work_queue->getHead() != work_queue->getTail()){
            node_item = work_queue->remove_ref_core();
	    if(node_item == NULL)
	        break;
	    else{
	        item = node_item->value;
	        if(item.getTableId() == 0){
                    Query3::simdScan_16(this->dataVector[0], item.getPart(), 10, &(this->result), true);
		}
	        else if(item.getTableId() == 1){
                    Query3::simdScan_16(this->dataVector[1], item.getPart(), 4, &(this->result), true);
		}
		else if(item.getTableId() == 11){
                    Query3::join_sw(this->dataVector[0], this->dataVector[1], item.getPart(), &(this->result));
		}
                this->putFreeNode(node_item);
	    }
        }
    }

    public:
        CoreQtBenchHandler(Syncronizer *sync, WorkQueue<T> *shared_queue, std::vector<DataCompressor*> &dataVector ) 
        : ThreadHandler <T> (sync, shared_queue, dataVector){}

    void *run(){
        this->thr_sync->waitOnStartBarrier();

        //this->startExec(this->sw_queue, this->shared_queue);

        this->thr_sync->waitOnAggBarrier();
	this->startExec(this->sw_queue, this->sw_queue);
        this->thr_sync->waitOnEndBarrier();

        this->t_stream->send("", 0);
        return NULL;
    }

    void setSWQueue(WorkQueue<T> *queue){
        this->sw_queue = queue;
    }

};

#endif
