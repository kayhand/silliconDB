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

        //while(!this->thr_sync->isQueryCompleted()){
        while(true){
		if(this->sw_queue->getHead() != this->sw_queue->getTail()){
		    node_item = this->sw_queue->remove_ref_core();
		    if(node_item == NULL)
		    	continue;
		    item = node_item->value;
		    if(item.getTableId() == 21){
			this->api_agg->agg(node_item, &(this->result));
			this->putFreeNode(node_item);
			this->thr_sync->incrementAggCounter();
		    }
		}
		else if(work_queue->getHead() != work_queue->getTail()){
		    node_item = work_queue->remove_ref_core();
		    if(node_item == NULL)
		    	continue;
		    item = node_item->value;
		    if(item.getTableId() == 0){
			this->api_ls->simdScan16(node_item, &(this->result));
			this->api_ls->incrementCounter();
			this->putFreeNode(node_item);
			this->addNewJob(1, item.getPart(), 11, work_queue);
			this->addNewJob(1, item.getPart(), 12, work_queue);
		    }
		    else if(item.getTableId() == 1){
			this->api_ss->simdScan16(node_item, &(this->result));
			this->api_ss->incrementCounter();
			this->putFreeNode(node_item);
		    }
		    else if(item.getTableId() == 2){
			this->api_cs->simdScan16(node_item, &(this->result));
			this->api_cs->incrementCounter();
			this->putFreeNode(node_item);
		    }
		    else if(item.getTableId() == 11){
			this->j1_lc->swJoin(node_item, &(this->result));
			this->j1_lc->setExecFlag(item.getPart());
			this->putFreeNode(node_item);
			this->addNewJob(1, item.getPart(), 21, this->sw_queue);
			/*
			int p_id = item.getPart();
    		        if(p_id == 45 || p_id == 46){
		            printf("Printing join result for part %d ...\n", p_id);
		    	    uint64_t * result_vector = (uint64_t *) this->j1_lc->getJoinBitVector(p_id);
		            this->j1_lc->printBitVector(result_vector[6], 7);
		        }
			*/
		    }
		    else if(item.getTableId() == 12){
			this->j2_ls->swJoin(node_item, &(this->result));
			this->j2_ls->setExecFlag(item.getPart());
			this->putFreeNode(node_item);
		    }
		}
		else{
		    break;
		}
	}
    }

    public:
        CoreQtBenchHandler(Syncronizer *sync, WorkQueue<T> *shared_queue, std::vector<DataCompressor*> &dataVector ) : ThreadHandler <T> (sync, shared_queue, dataVector){}

    void *run(){
        this->thr_sync->initAggCounter(this->api_ls->totalParts());
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
