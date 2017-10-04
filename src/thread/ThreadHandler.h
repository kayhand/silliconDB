#ifndef __threadhandler_h__
#define __threadhandler_h__

#include <pthread.h>
#include <vector>
#include <atomic>

#include "Thread.h"
#include "Syncronizer.h"
#include "../exec/util/WorkQueue.h"
#include "../network/server/TCPAcceptor.h"
#include "../log/Result.h"

#include "../bench/vldb2018/tpch/Query3.h"

#ifdef __sun 
#include <sys/processor.h>
#include <sys/procset.h>
#include <thread.h>
#endif

template <class T>
class ThreadHandler : public Thread<T>{
    protected:
        Syncronizer* thr_sync;
        WorkQueue<T>* shared_queue;
        TCPStream* t_stream;
        std::vector<DataCompressor*> dataVector;
        Result result;

    public:
        ThreadHandler(Syncronizer *sync, WorkQueue<T> *queue, std::vector<DataCompressor*> &data){
            thr_sync = sync;
            shared_queue = queue;
	    dataVector = data;
        }

        void addDataCompressor(DataCompressor *dataComp){
            dataVector.push_back(dataComp);    
        }

        void addNewJob(int r_id, int p_id, int j_type){
            Node<T> *newNode = this->returnNextNode(r_id, p_id, j_type);
            shared_queue->add(newNode);
        }

        Result &getResult(){
            return result;
        }

	void setStream(TCPStream *stream){
	    t_stream = stream;
	}

};

template <class T>
class CoreHandler : public ThreadHandler<T>{
    WorkQueue<T>* sw_queue;

    public:
        CoreHandler(Syncronizer *sync, WorkQueue<T> *shared_queue, std::vector<DataCompressor*> &dataVector ) 
        : ThreadHandler <T> (sync, shared_queue, dataVector){}

    void setSWQueue(WorkQueue<T> &queue){
        sw_queue = queue;
    }

    void *run(){
    	Node<T> *node_item;
	T item;
        this->thr_sync->waitOnStartBarrier();
        for(int i = 0; ; i++){
            while(this->shared_queue->getHead() != this->shared_queue->getTail()){
                node_item = this->shared_queue->remove_ref_core();
                if(node_item == NULL){
                    this->shared_queue->printQueue();
                }
                item = node_item->value;
                if(item.getTableId() == 0){
                    Query3::simdScan_24(this->dataVector[0], item.getPart(), 10, &(this->result), true);
                    this->putFreeNode(node_item);
                    //addNewJob(1, item.getPart(), 4); //add agg job -- id = 4
                }
                else if(item.getTableId() == 1){
                    Query3::simdScan_24(this->dataVector[1], item.getPart(), 4, &(this->result), false);
                    this->putFreeNode(node_item);
                }
                else if(item.getTableId() == 11){
                    Query3::join_sw(this->dataVector[0], this->dataVector[1], item.getPart(), &(this->result));
                    this->putFreeNode(node_item);
                }
            }
            //if(!daxDone){
                //thr_sync.waitOnAggBarrier();
            //}
            //else{
                break;
            //}
        }
        this->thr_sync->waitOnEndBarrier();
        this->t_stream->send("", 0);
        return NULL;
    }
};

#ifdef __sun 
template <class T>
class DaxHandler : public ThreadHandler<T>
{
    dax_queue_t *dax_queue;
    int q_size;
    bool async;

    dax_context_t *ctx;
    public:
        DaxHandler(Syncronizer *sync, WorkQueue<T> *shared_queue, std::vector<DataCompressor*> &dataVector, int size, bool isAsync) 
        : ThreadHandler<T> (sync, shared_queue, dataVector){
            q_size = size;
            async = isAsync;
            if(!async)
                q_size = 1;
        }

        void *run(){
            int items_done = dax_status_t::DAX_EQEMPTY;
            bool daxDone = false;
            int new_jobs = 0;
	    Node<T> *node_item;
      
            createDaxContext();
            this->thr_sync->waitOnStartBarrier();
            while(!daxDone){
                if(this->shared_queue->getHead() != this->shared_queue->getTail()){
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
                            node_item = this->shared_queue->remove_ref_dax();
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
                    daxDone = true;
                }
	    }
            printf("DAX DONE! (%d)\n", items_done);
            //this->thr_sync->waitOnAggBarrier();
            this->thr_sync->waitOnEndBarrier();
      
            this->t_stream->send("", 0);
            return NULL;
        }
 
        void postWorkToDAX(Node<T> *node_item){
	    T work_item = node_item->value;
            int partId = work_item.getPart();
            if(work_item.getTableId() == 0){
                Query3::hwScan(this->dataVector[0], partId, 10, dax_compare_t::DAX_GT, &(this->result), &ctx, &dax_queue, async, (void *) node_item); //true == dax_poll
            }
            else if(work_item.getTableId() == 1){
                //printf("hw_order(%d) pushed to the DAX queue (itr. %d)\n", partId, loop_id++);
                Query3::hwScan(this->dataVector[1], partId, 4, dax_compare_t::DAX_LT, &(this->result), &ctx, &dax_queue, async, (void *) node_item); //true == dax_poll
            }
            else if(work_item.getTableId() == 11){
                Query3::join_hw(this->dataVector[0], this->dataVector[1], partId, &(this->result), &ctx, &dax_queue, async, (void *) node_item);
            }
        }

        void createDaxContext(){
            int lFile = open("/tmp/dax_log.txt", O_RDWR);

            dax_status_t res = dax_thread_init(1, 1, 0, NULL, &ctx);
            if(res != 0)
                printf("Problem with DAX Context Creation! Return code is %d.\n", res);

            res = dax_queue_create(ctx, q_size, &dax_queue);
            if(res != 0)
                printf("Problem with DAX Queue Creation! Return code is %d.\n", res);

            res = dax_set_log_file(ctx, DAX_LOG_ALL, lFile);
            if(res != 0)
                printf("Problem with DAX Logger Creation! Return code is %d.\n", res);
        }

        void handlePollReturn(int items_done, dax_poll_t *poll_data, hrtime_t ts){
            Node<T> *node_item;
            T item;
            int job_id;
            //int part_id;
            int cnt;
            for(int i = 0; i < items_done; i++){
                node_item = (Node<T> *) (poll_data[i].udata);
                item = node_item->value;
                job_id = item.getTableId();
                //part_id = item.getPart();

                cnt = poll_data[i].count;
                if(job_id == 0){
                    //printf("hw_line-%d completed. count:%d \n", part_id, cnt);
                    //addNewJob(1, part_id, 11); //add join job
                    this->result.addCountResult(make_tuple(cnt, job_id));     
                    if(async)
                        this->result.addRuntime(DAX_SCAN, make_tuple(node_item->t_start, ts));
                }
                else if(job_id == 1){
                    //printf("hw_order(%d) completed (itr. %d) - Count (%d)\n", part_id, loop_id, cnt);
                    this->result.addCountResult(make_tuple(cnt, job_id));     
                }
                else{
                    //printf("hw_join(%d) completed (itr. %d) - Count (%d)\n", part_id, loop_id, cnt);
                    //addNewJob(1, part_id, 4); //add agg job
                    this->result.addCountResult(make_tuple(cnt, 2));     
                }
                this->putFreeNode(node_item);
            }
        } 
};
#endif

#endif
