#ifndef __threadhandler_h__
#define __threadhandler_h__

#include <pthread.h>
#include <vector>
#include <atomic>

#include "Thread.h"
#include "ThreadHandler.h"
#include "Syncronizer.h"

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
class DaxHandler : public ThreadHandler<T>
{
    dax_queue_t *dax_queue;
    int q_size;
    bool async;

    dax_context_t *ctx;

    void startExec(WorkQueue<T> *work_queue){
        int items_done = dax_status_t::DAX_EQEMPTY;
        int new_jobs = 0;
	bool done = false;
	Node<T> *node_item;

        while(!done){
            if(work_queue->getHead() != work_queue->getTail()){
                dax_poll_t poll_data[q_size];                
                if(async){
                    items_done = dax_poll(dax_queue, poll_data, 1, -1);
                    if(items_done == 0){
                        continue;
                    }
                    else if(items_done > 0){
                        handlePollReturn(items_done, poll_data, gethrtime());
                    }
                    else{
                        new_jobs = q_size;
                    }
                }   
                else{
                    items_done = 1;
                    new_jobs = 1;
                }
     
                if(!async || new_jobs == q_size){
                    for(int job_id = 0; job_id < new_jobs; job_id++){
                        node_item = work_queue->remove_ref_dax();
                        if(node_item == NULL){ //Aggregation
                            break;
                        }
                        else{
                            postWorkToDAX(node_item);
                        }
                    }
                }
            }
            else{
                if(async){
                    //Work_queue is empty but there might still be elements in dax internal queue
                    items_done = 0;
                    while(items_done != dax_status_t::DAX_EQEMPTY){ // busy polling
                        dax_poll_t poll_data[q_size];
                        items_done = dax_poll(dax_queue, poll_data, 1, -1);
                        if(items_done > 0)
                            handlePollReturn(items_done, poll_data, gethrtime());            
                    }
                }
		done = true;
            }
        }
        printf("DAX DONE! (%d), %d\n", items_done, (int) this->daxDone);
    }

    public:
        DaxHandler(Syncronizer *sync, WorkQueue<T> *shared_queue, std::vector<DataCompressor*> &dataVector, int size, bool isAsync) 
        : ThreadHandler<T> (sync, shared_queue, dataVector){
            q_size = size;
            async = isAsync;
            if(!async)
                q_size = 1;
        }

        void *run(){
            createDaxContext();
            this->thr_sync->waitOnStartBarrier();

            this->startExec(this->hw_queue);

	    printf("Dax in agg barrier (%d)...\n", (int) this->daxDone);
	    this->daxDone.exchange(true);
            this->thr_sync->waitOnAggBarrier();
	    printf("Dax left from agg barrier (%d)...\n", (int) this->daxDone);
            this->thr_sync->waitOnEndBarrier();
      
            this->t_stream->send("", 0);
            return NULL;
        }
 
        void postWorkToDAX(Node<T> *node_item){
	    T work_item = node_item->value;
            int partId = work_item.getPart();
            if(work_item.getTableId() == 0){
                Query3::hwScan(this->dataVector[0], partId, 10, dax_compare_t::DAX_GT, &(this->result), &ctx, &dax_queue, async, (void *) node_item); //true == dax_poll
		if(!async)
                    this->addNewJob(1, partId, 11, this->hw_queue); //add join job
		    
            }
            else if(work_item.getTableId() == 1){
                //printf("hw_order(%d) pushed to the DAX queue (itr. %d)\n", partId, loop_id++);
                Query3::hwScan(this->dataVector[1], partId, 4, dax_compare_t::DAX_LT, &(this->result), &ctx, &dax_queue, async, (void *) node_item); //true == dax_poll
            }
            else if(work_item.getTableId() == 11){
                Query3::join_hw(this->dataVector[0], this->dataVector[1], partId, &(this->result), &ctx, &dax_queue, async, (void *) node_item);
                   if(!async){
		       this->addNewJob(1, partId, 4, this->sw_queue); //add agg job
		   }
            }
	    if(!async)
                this->putFreeNode(node_item);
        }

        void createDaxContext(){
            int lFile = open("/tmp/dax_log.txt", O_RDWR);

            dax_status_t res = dax_thread_init(1, 1, 0, NULL, &ctx);
            if(res != 0)
                printf("Problem with DAX Context Creation! Return code is %d.\n", res);

            res = dax_queue_create(ctx, q_size, &dax_queue);
            if(res != 0)
                printf("Problem with DAX Queue Creation! Return code is %d.\n", res);

            res = dax_set_log_file(ctx, DAX_LOG_OFF, lFile);
            if(res != 0)
                printf("Problem with DAX Logger Creation! Return code is %d.\n", res);

	    //dax_props_t props;
            //res = dax_get_props(ctx, &props);
            //if(res != 0)
                //printf("Problem with DAX Props! Return code is %d.\n", res);

        }

        void handlePollReturn(int items_done, dax_poll_t *poll_data, hrtime_t ts){
            Node<T> *node_item;
            T item;
            int job_id;
            int part_id;
            int cnt;
            for(int i = 0; i < items_done; i++){
                node_item = (Node<T> *) (poll_data[i].udata);
                item = node_item->value;
                job_id = item.getTableId();
                part_id = item.getPart();

                cnt = poll_data[i].count;
                if(job_id == 0){
                    this->addNewJob(1, part_id, 11, this->hw_queue); //add join job to the shared queue
                    this->result.addCountResult(make_tuple(cnt, job_id));     
                    if(async)
                        this->result.addRuntime(DAX_SCAN, make_tuple(node_item->t_start, ts));
                }
                else if(job_id == 1){
                    //printf("hw_order(%d) completed (itr. %d) - Count (%d)\n", part_id, loop_id, cnt);
                    this->result.addCountResult(make_tuple(cnt, job_id));     
                    if(async)
                        this->result.addRuntime(DAX_SCAN, make_tuple(node_item->t_start, ts));
                }
                else{
                    //printf("hw_join(%d) completed (itr. %d) - Count (%d)\n", part_id, loop_id, cnt);
                    this->addNewJob(1, part_id, 4, this->sw_queue); //add agg job
                    this->result.addCountResult(make_tuple(cnt, 2));     
                    if(async)
                        this->result.addRuntime(JOIN, make_tuple(node_item->t_start, ts));
                }
                this->putFreeNode(node_item);
            }
        } 
        void setHWQueue(WorkQueue<T> *queue){
	    this->hw_queue->printQueue();
        }
};
#endif

#endif
