#ifndef __dax_qt_bench_handler_h__
#define __dax_qt_bench_handler_h__

#include <pthread.h>
#include <vector>
#include <atomic>

#include "Thread.h"
#include "ThreadHandler.h"
#include "Syncronizer.h"

#include "sched/ProcessingUnit.h"

#include "exec/util/WorkQueue.h"
#include "network/server/TCPAcceptor.h"
#include "log/Result.h"

#ifdef __sun 
#include <sys/processor.h>
#include <sys/procset.h>
#include <thread.h>
#endif

#ifdef __sun 
template <class T>
class DaxQtBenchHandler : public ThreadHandler<T>{

    dax_queue_t *dax_queue;
    int q_size;
    bool async;
    dax_context_t *ctx;

    WorkQueue<T> *work_queue;

    void startExec(WorkQueue<T> *w_queue){
        work_queue = w_queue;
	int items_done = q_size;
	Node<T> *node_item;

        while(work_queue->getHead() != work_queue->getTail()){
	    for(int i = 0; i < items_done; i++){
                if(work_queue->getHead() != work_queue->getTail()){
                    node_item = work_queue->remove_ref_dax();
		    if(node_item == NULL)
		        continue;
		    else if(node_item->value.getTableId() == 0){
	                this->api_ls->hwScan(&dax_queue, node_item);
		    }
		    else if(node_item->value.getTableId() == 1){
	                this->api_ss->hwScan(&dax_queue, node_item);
		    }
		    else if(node_item->value.getTableId() == 2){
	                this->api_cs->hwScan(&dax_queue, node_item);
		    }
		    else if(node_item->value.getTableId() == 11){
	                this->j1_lc->hwJoin(&dax_queue, node_item);
		    }
		    else if(node_item->value.getTableId() == 12){
	                this->j2_ls->hwJoin(&dax_queue, node_item);
		    }
		}
	    }

            dax_poll_t poll_data[q_size];                
            items_done = dax_poll(dax_queue, poll_data, -1, 0);
            if(items_done > 0){
                handlePollReturn(items_done, poll_data);
            }
	    /*else{
	        if(work_queue->getHead() == work_queue->getTail()){
		    //printf("Busy polling...\n");
		    items_done = 0;
		    while(items_done != dax_status_t::DAX_EQEMPTY){ // busy polling
		        dax_poll_t poll_data[q_size];
		        items_done = dax_poll(dax_queue, poll_data, 1, -1);
		        if(items_done > 0){
		    	    handlePollReturn(items_done, poll_data);           
		        }
		    }
		    items_done = q_size;
		}
	    }*/
        }

        //work_queue->printQueue();
        items_done = 0;
        while(items_done != dax_status_t::DAX_EQEMPTY){ // busy polling
            dax_poll_t poll_data[q_size];
            items_done = dax_poll(dax_queue, poll_data, 1, -1);
            if(items_done > 0){
                handlePollReturn(items_done, poll_data);           
	    }
        }
  
        printf("DAX DONE! (%d), %d\n", items_done, (int) this->daxDone);

    }

    public:
        DaxQtBenchHandler(Syncronizer *sync, WorkQueue<T> *shared_queue, std::vector<DataCompressor*> &dataVector, int size, bool isAsync) : ThreadHandler<T> (sync, shared_queue, dataVector){
            q_size = size;
            async = isAsync;
            if(!async)
                q_size = 1;
        }

        void *run(){
            createDaxContext();
            this->thr_sync->waitOnStartBarrier();
            this->startExec(this->shared_queue);
	    printf("Dax is done...\n");
            this->thr_sync->waitOnAggBarrier();
            this->thr_sync->waitOnEndBarrier();

            //this->and_lcd->printResultVector(10); 
            this->t_stream->send("", 0);
            return NULL;
        }
 
        void createDaxContext(){
            int lFile = open("/tmp/dax_log.txt", O_RDWR);

            dax_status_t res = dax_thread_init(1, 1, 0, NULL, &ctx);
            if(res != 0)
                printf("Problem with DAX Context Creation! Return code is %d.\n", res);

            res = dax_queue_create(ctx, q_size, &dax_queue);
            if(res != 0)
                printf("Problem with DAX Queue Creation! Return code is %d.\n", res);

            res = dax_set_log_file(ctx, DAX_LOG_VERBOSE, lFile);
            if(res != 0)
                printf("Problem with DAX Logger Creation! Return code is %d.\n", res);

	    //dax_props_t props;
            //res = dax_get_props(ctx, &props);
            //if(res != 0)
                //printf("Problem with DAX Props! Return code is %d.\n", res);
        }

        inline void handlePollReturn(int items_done, dax_poll_t *poll_data){
            Node<T> *node_item;
	    int t_id;
	    int p_id;
            for(int i = 0; i < items_done; i++){
	        q_udata *post_data = (q_udata*) (poll_data[i].udata);
                node_item = (Node<T> *) (post_data->node_ptr);

                //this->result.addRuntime(DAX_SCAN, make_tuple(node_item->t_start, ts));
                t_id = node_item->value.getTableId();
                p_id = node_item->value.getPart();
		if(t_id == 0 || t_id == 1 || t_id == 2){
		    this->result.addRuntime(DAX_SCAN, make_tuple(post_data->t_start, gethrtime(), t_id, p_id));
		}

		if(t_id == 11){
		    this->result.addRuntime(DAX_JOIN, make_tuple(post_data->t_start, gethrtime(), t_id, p_id));
		    this->result.addCountResult(make_tuple(poll_data[i].count, 3));
		    this->j1_lc->setExecFlag(p_id);

		    /*if(p_id == 45 || p_id == 46){
		        printf("Printing join result for part %d ...\n", p_id);
		    	uint64_t * result_vector = (uint64_t *) this->j1_lc->getJoinBitVector(p_id);
		        this->j1_lc->printBitVector(result_vector[6], 7);
		    }*/
		}
		else if(t_id == 12){
		    this->result.addCountResult(make_tuple(poll_data[i].count, 4));
		    this->j2_ls->setExecFlag(p_id);
		}
		else
		    this->result.addCountResult(make_tuple(poll_data[i].count, t_id));

                this->putFreeNode(node_item);

	        if(t_id == 0){
		    //schedule joins: lineorder-customer, lineorder-order
		    this->api_ls->incrementCounter();
		    //this->addNewJob(0, node_item->value.getPart(), 12, this->sw_queue); 
		    this->addNewJob(0, node_item->value.getPart(), 11, this->work_queue); 
		    this->addNewJob(0, node_item->value.getPart(), 12, this->work_queue); 
		    //printf("Dax Count (scan): ");
		}
		else if(t_id == 1){
		    this->api_ss->incrementCounter();
		}
		else if(t_id == 2){
		    this->api_cs->incrementCounter();
		}
		else if (t_id == 11){
		    //printf("Dax Count (join1): ");
		    this->addNewJob(0, node_item->value.getPart(), 21, this->sw_queue); 
		}
		else if(t_id == 12){
		    //schedule and operation
		    //printf("Dax Count (join2): ");
		}
		/*
		else if(t_id == 11){
		    printf("Dax Count (lc. join): ");
		}
		else if(t_id == 12){
		    printf("Dax Count (ld. join): ");
		}
		*/
		//printf("%lu for part: %d\n", poll_data[i].count, node_item->value.getPart());
                
	    }
        } 

        void setHWQueue(WorkQueue<T> *queue){
	    this->hw_queue->printQueue();
        }
};
#endif
#endif
