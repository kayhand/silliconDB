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

#include "bench/vldb2018/qt/SDBApi.h"
#include "bench/vldb2018/qt/SDBAndApi.h"
#include "bench/vldb2018/qt/SDBJoinApi.h"

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

	ScanApi *api_ls = this->scanAPIs[0];
	ScanApi *api_ds = this->scanAPIs[1];
	ScanApi *api_cs = this->scanAPIs[2];

        //ScanApi api_ls(this->dataVector[0], 8);
        //ScanApi api_ds(this->dataVector[1], 4);
        //ScanApi api_cs(this->dataVector[2], 5);

	//SDBApi api_ls(this->dataVector[0], 8);
	//api_ls.initialize();
	
	//SDBApi api_ds(this->dataVector[1], 4);
	//api_ds.initialize();
	
	//SDBApi api_cs(this->dataVector[2], 5);
	//api_cs.initialize();

	SDBJoinApi j1_api(this->dataVector[0], this->dataVector[2], 2); 
	j1_api.initDaxVectors();

	SDBJoinApi j2_api(this->dataVector[0], this->dataVector[1], 5); 
	j2_api.initDaxVectors();

	SDBAndApi and_api(this->dataVector[0], this->dataVector[2], 5);
	and_api.initDaxVectors();

        while(work_queue->getHead() != work_queue->getTail()){
	    for(int i = 0; i < items_done; i++){
                if(work_queue->getHead() != work_queue->getTail()){
                    node_item = work_queue->remove_ref_dax();
		    if(node_item == NULL)
		        break;
		    else if(node_item->value.getTableId() == 0){
	                api_ls->hwScan(&dax_queue, node_item);
		    }
		    else if(node_item->value.getTableId() == 1){
	                api_ds->hwScan(&dax_queue, node_item);
		    }
		    else if(node_item->value.getTableId() == 2){
	                api_cs->hwScan(&dax_queue, node_item);
		    }
		    else if(node_item->value.getTableId() == 11){
	                j1_api.hwJoin(&dax_queue, node_item);
		    }
		    else if(node_item->value.getTableId() == 21){
		        and_api.hwAnd(&dax_queue, node_item);
		    }
		    else if(node_item->value.getTableId() == 12){
	                j2_api.hwJoin(&dax_queue, node_item);
		    }
		}
	    }

            dax_poll_t poll_data[q_size];                
            items_done = dax_poll(dax_queue, poll_data, -1, 0);
            if(items_done > 0){
                handlePollReturn(items_done, poll_data);
            }
        }

        items_done = 0;
        while(items_done != dax_status_t::DAX_EQEMPTY){ // busy polling
            dax_poll_t poll_data[q_size];
            items_done = dax_poll(dax_queue, poll_data, 1, -1);
            if(items_done > 0){
                handlePollReturn(items_done, poll_data);           
	    }
        }
        //printf("DAX DONE! (%d), %d\n", items_done, (int) this->daxDone);

/*	
        DataCompressor *leftComp = this->dataVector[1];
        int num_of_segments = leftComp->getTable()->t_meta.num_of_segments; 
        uint64_t *bit_vector = (uint64_t*) (leftComp->getFilterBitVector(0));
	for(int seg_id = 0; seg_id < 2; seg_id++){
	    uint64_t cur_vector = bit_vector[seg_id];
	    printf("%d: ", seg_id);
	    for(int i = 0; i < 64; i++){
	        int val = cur_vector & 1;
		cur_vector = cur_vector >> 1;
		if(val == 1){
		    int index = seg_id * 63 + (64 - i);
		    printf("%d - ", index);
		}
	    }
	    printf("\n");
	}
	printf("Num of segments: %d\n", num_of_segments);
*/
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

            this->thr_sync->waitOnAggBarrier();
            this->thr_sync->waitOnEndBarrier();

            this->t_stream->send("", 0);
            return NULL;
        }
 
        void createDaxContext(){
            int lFile = open("/tmp/dax_log.txt", O_RDWR);

            dax_status_t res = dax_thread_init(1, 1, 0, NULL, &ctx);
            if(res != 0)
                printf("Problem with DAX Context Creation! Return code is %d.\n", res);

            printf("Qsize: %d\n", q_size);
            res = dax_queue_create(ctx, q_size, &dax_queue);
            if(res != 0)
                printf("Problem with DAX Queue Creation! Return code is %d.\n", res);

            res = dax_set_log_file(ctx, DAX_LOG_RETURN, lFile);
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
            for(int i = 0; i < items_done; i++){
	        q_udata *post_data = (q_udata*) (poll_data[i].udata);
                node_item = (Node<T> *) (post_data->node_ptr);

                //this->result.addRuntime(DAX_SCAN, make_tuple(node_item->t_start, ts));
                t_id = node_item->value.getTableId();
		if(t_id == 0 || t_id == 1 || t_id == 2)
		    this->result.addRuntime(DAX_SCAN, make_tuple(post_data->t_start, gethrtime()));

		if(t_id == 11)
		    this->result.addCountResult(make_tuple(poll_data[i].count, 3));
		else if(t_id == 12){
		    this->result.addCountResult(make_tuple(poll_data[i].count, 4));
		    printf("lo-date join DONE! (%lu)\n", poll_data[i].count);
		}
		else
		    this->result.addCountResult(make_tuple(poll_data[i].count, t_id));

                this->putFreeNode(node_item);
/*		if(t_id == 2)
		    printf("customer scan DONE!\n");
		else if(t_id == 1)
		    printf("date scan DONE!\n");
		else if(t_id == 0){
		    printf("lo scan DONE!\n");
		    //this->addNewJob(0, node_item->value.getPart(), 11, work_queue);
		}
		else if(t_id == 11){
		    printf("lo-customer join DONE!\n");
		    //this->addNewJob(0, node_item->value.getPart(), 12, work_queue);
		    //this->addNewJob(0, node_item->value.getPart(), 21, work_queue);
		    //and bit_vector results
		}*/
		printf("Dax Count: %lu for part: %d\n", poll_data[i].count, node_item->value.getPart());

            }
        } 

        void setHWQueue(WorkQueue<T> *queue){
	    this->hw_queue->printQueue();
        }
};
#endif
#endif
