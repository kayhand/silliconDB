#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <string>
#include <vector>
#include <fcntl.h>

#include "../thread/Thread.h"
#include "../network/server/TCPAcceptor.h"
#include "../data/DataLoader.h"
#include "../util/Partitioner.h"
#include "../util/Query.h"
#include "../util/Query1.h"
#include "util/WorkQueue.h"

std::atomic<int> li_cnt = ATOMIC_VAR_INIT(0);

pthread_barrier_t barrier;
pthread_mutex_t queue_mutex;
pthread_cond_t queue_cond;

class CoreHandler : public Thread<Query>
{
	WorkQueue<Query> *sw_queue;
	WorkQueue<Query> *shared_queue;
	TCPStream* t_stream;
	DataCompressor *lineitemComp = sw_queue->getDataLoader()->getDataCompressor(0);
	Result *result;
	int loop_id = 0;

	public:
	    CoreHandler(WorkQueue<Query>* sw_queue, Result *res) : sw_queue(sw_queue), result(res){}
	    CoreHandler(WorkQueue<Query>* sw_queue, WorkQueue<Query>* shared_queue, Result *res) : sw_queue(sw_queue), shared_queue(shared_queue), result(res){}

	void *run(){
		pthread_barrier_wait(&barrier);
		while(true){
			if(sw_queue->getHead() != sw_queue->getTail()){
			    Query item = sw_queue->remove_ref_core();
			    Query1::agg(lineitemComp, item.getPart(), result, item.isDax());
			}
			else if(shared_queue->getHead() != shared_queue->getTail()){
			    Query item = shared_queue->remove_ref_core();
			    Query1::linescan_sw(lineitemComp, item.getPart(), result);

	    		    Query agg_item(1, item->getPart(), 2); //(0: hw/sw - 1:sw only, part_id, job type) -- scan or agg or count
			    addNewJob(agg_item);
			}
			else{
	    	            t_stream->send("", 0);
			    return NULL;
			}
		}
	    	t_stream->send("", 0);
		return NULL;
		return(0);
	}

	void addNewJob(Query item){
 	    Node<Query> *newNode = this->returnNextNode(item);
            sw_queue->add(newNode);		
	}

	void setStream(TCPStream* stream){
		this->t_stream = stream;
	}
};

#ifdef __sun 
class DaxHandler : public Thread<Query>
{
	WorkQueue<Query> *sw_queue;
	WorkQueue<Query> *shared_queue;
	TCPStream* t_stream;
	dax_context_t *ctx;
	DataCompressor *lineitemComp = shared_queue->getDataLoader()->getDataCompressor(0);
	Result *result;
	int loop_id = 0;

	dax_queue_t *queue;
	static const int q_size = 2;
	dax_poll_t poll_data[q_size];
	Query jobs[2];
	int job_count = q_size;
	int n_jobs = 0; //in the queue
	int poll_res = -14;

	public:
	    DaxHandler(WorkQueue<Query>* shared_queue, Result *res) : shared_queue(shared_queue), result(res){}
	    DaxHandler(WorkQueue<Query>* sw_queue, WorkQueue<Query>* shared_queue, Result *res) : sw_queue(sw_queue), shared_queue(shared_queue), result(res){}

	void *run(){
		createDaxContext();
		pthread_barrier_wait(&barrier);
		while(true){
			if(shared_queue->getHead() != shared_queue->getTail()){
				int j_id = 0;
				for(; j_id < q_size - n_jobs; j_id++){
				    jobs[j_id] = shared_queue->remove_ref_dax();
				    if(jobs[j_id] == NULL)
				    	break;
				}
				int job_count = j_id;
				//int job_count = scan_queue->remove_multiple(1, 2 - n_jobs, jobs); //$1: specify dax, $2: how many jobs to push, $3: jobs to process

				for(int j_ind = 0; j_ind < job_count; j_ind++){
			    	    Query1::linescan_post(lineitemComp, &queue, jobs[j_ind]);
				    //printf("Partition %d posted!\n", jobs[j_ind]->getPart());
				    n_jobs++;
				}

				poll_res = dax_poll(queue, poll_data, n_jobs, -1);

				for(int j_ind = 0; j_ind < poll_res; j_ind++){
				    handlePostReturn(poll_data[j_ind], result);
				    n_jobs--;
				}
				//printf("DAX got from table id %d, partition %d - loop: %d in thread %d!\n", item.getTableId(), item.getPart(), loop_id++, this->getId());
			}
			else{
				t_stream->send("", 0);
				return NULL;
			}
		}
		return(0);
	}
	
	void handlePostReturn(dax_poll_t p_data, Result *res){
	    struct q_udata *q_up = (struct q_udata *) (p_data.udata);
	    res->addRuntime(DAX_SCAN, make_tuple(q_up->t_start, gethrtime()));
	    memcpy(q_up->bit_vector, q_up->dst_data, q_up->copy_size);
	    free(q_up->src_data);
	    free(q_up->dst_data);
	    //printf("DAX Count (%d) : %lu\n", q_up->p_id, p_data.count);
	    
	    Query agg_item(1, q_up->p_id, 2); 
	    agg_item.flipDax();
	    addNewJob(agg_item);
	}

	void setStream(TCPStream* stream){
		this->t_stream = stream;
	}

        void addNewJob(Query item){
    	    Node<Query> *newNode = this->returnNextNode(item);
	    sw_queue->add(newNode);
	    sw_queue->printQueue();
        }

	void createDaxContext(){
	    	//int lFile = open("/tmp/dax_log.txt", O_RDWR);

		dax_status_t res = dax_thread_init(1, 1, 0, NULL, &ctx);
		if(res != 0)
			printf("Problem with DAX Context Creation! Return code is %d.\n", res);
		res = dax_queue_create(ctx, 5, &queue);
		if(res != 0)
			printf("Problem with DAX Queue Creation! Return code is %d.\n", res);
		//res = dax_set_log_file(ctx, DAX_LOG_ALL, lFile);
		//if(res != 0)
			//printf("Problem with DAX Logger Creation! Return code is %d.\n", res);
	}
};
#endif

class ProcessingUnit{
	std::vector<CoreHandler*> conn_handlers; //4 cpu cores, 1 DAX Unit
	#ifdef __sun 
	DaxHandler *dax_handler;
	#endif
	int num_of_units;
	WorkQueue<Query> sw_queue;     
	WorkQueue<Query> shared_queue;     
	vector<Node<Query>*> initialNodes;
	vector<Result*> results;

	public: 
 	    ProcessingUnit(int num_of_threads, WorkQueue<Query> sw_queue, WorkQueue<Query> shared_queue) : num_of_units(num_of_threads), sw_queue(sw_queue), shared_queue(shared_queue){}

	void createProcessingUnit(){
	    conn_handlers.reserve(50);
	    initialNodes.reserve(100);
	    results.reserve(num_of_units + 1);
	    for(int i = 0; i < num_of_units; i++){
	    	Result *res = new Result();
		CoreHandler *handler = new CoreHandler(&sw_queue, &shared_queue, res);
		handler->setId(i);
		conn_handlers.push_back(handler);	
		results.push_back(res);
	    }
	    #ifdef __sun 
	    Result *res = new Result();
	    dax_handler = new DaxHandler(&sw_queue, &shared_queue, res);
	    dax_handler->setId(num_of_units);
	    results.push_back(res);
	    #endif
	}

	void addWork(int num_of_parts, int table_id, int sf){
	    for(int part_id = 0; part_id < num_of_parts * sf; part_id++){
	    	Query item(part_id % 2, part_id, table_id); //(0: sw - 1:hw, part_id, table_id)
	    	Node<Query> *newNode = new Node<Query>(item);
	    	shared_queue.add(newNode);
    	   	initialNodes.push_back(newNode);
	    }
	}	

	void startThreads(TCPStream* connection){
	    for(CoreHandler* curConn : conn_handlers){
	        curConn->setStream(connection);
	        curConn->start(curConn->getId());
		curConn->reserveNodes(50);
 	    }
	    #ifdef __sun 
	    dax_handler->setStream(connection);
	    dax_handler->start(dax_handler->getId());
	    dax_handler->reserveNodes(50);
	    #endif
	}

	void joinThreads(){
	    #ifdef __sun 
	    dax_handler->join();
	    #endif
	    for(CoreHandler* curConn : conn_handlers){
	        curConn->join();
 	    }
	}

	WorkQueue<Query> &getSharedQueue(){
	    return shared_queue;
	}

	WorkQueue<Query> &getSWQueue(){
	    return sw_queue;
	}

	void printResults(){
	    DataCompressor *lineitemComp = sw_queue.getDataLoader()->getDataCompressor(0);
	    int count = 0;
	    for(int ind = 0; ind < lineitemComp->getTable()->num_of_segments; ind++){
	        count += __builtin_popcountl(lineitemComp->getBitVector()[ind]);
	    }
	    printf("Lineitem scan count: %d - total segments: %d\n", count, lineitemComp->getTable()->num_of_segments);
	}

	void writeResults(){
	    FILE *sscan_f = fopen("sw_scan.txt", "a");
	    FILE *dscan_f = fopen("dax_scan.txt", "a");
	    FILE *agg_f = fopen("agg.txt", "a");
	    FILE *aggr_f = fopen("agg_result.txt", "a");

	    for(Result *result : results){
	    	result->writeResults(SW_SCAN, sscan_f);
	    	result->writeResults(DAX_SCAN, dscan_f);
	    	result->writeResults(AGG, agg_f);
	    	result->writeResults(AGG_RES, aggr_f);
	    }
	    
	    fclose(sscan_f);
	    fclose(dscan_f);
	    fclose(agg_f);
	    fclose(aggr_f);
	}

	void clearResources(){
            for(CoreHandler* curHandler : conn_handlers){
	        delete curHandler; 
            }
	    #ifdef __sun 
	    delete dax_handler;
	    #endif
	    Node<Query> *sw_head = sw_queue.getHead();
	    delete sw_head;
	    Node<Query> *shared_head = shared_queue.getHead();
	    delete shared_head;
	    for(Node<Query> *curNode : initialNodes){
	    	delete curNode;
	    }
	    for(Result *res : results)
	    	delete res;
	}	
};

int main(int argc, char** argv)
{
	if ( argc < 6 ) {
		printf("usage: %s <workers> <port> <ip> <fileName> <part_size> [scale_factor] \n", argv[0]);
		exit(-1);
	}

	int workers = atoi(argv[1]); //Number of partitions
	int port = atoi(argv[2]);
	string ip = argv[3];
	string fileName = argv[4];
	int part_size = atoi(argv[5]);
	int sf = 1;
	if(argc == 7)
	    sf = atoi(argv[5]);

	pthread_barrier_init(&barrier, NULL, workers + 1); 

	/*Lineitem table*/
	Partitioner line_part;
	line_part.roundRobin(fileName, part_size); 

	DataLoader data_loader(1);
	data_loader.initializeCompressor(fileName, 0, line_part, sf);

	data_loader.parseTable(0);
	data_loader.compressTable(0);

	TCPAcceptor* connectionAcceptor;
	connectionAcceptor = new TCPAcceptor(port, (char*)ip.c_str());
	if (!connectionAcceptor || connectionAcceptor->start() != 0) {
		printf("Could not create a connection acceptor\n");
		exit(1);
	}

	std::vector<ProcessingUnit> proc_units;
    	proc_units.reserve(10);
	
	WorkQueue<Query> sw_queue(0, &data_loader); 
	WorkQueue<Query> shared_queue(0, &data_loader); 
        for(int i = 0; i < 1; i++){
	    ProcessingUnit proc_unit (workers, sw_queue, shared_queue);
	    proc_unit.createProcessingUnit();
	    proc_unit.addWork(line_part.getNumberOfParts(), 0, sf);
	    proc_units.push_back(proc_unit);
	    shared_queue.printQueue();
        }

        int numberOfConnections = 1;
        TCPStream* connection;
        while(numberOfConnections > 0){
	    printf("Gimme some connection!\n");
	    connection = connectionAcceptor->accept();
	    if(!connection) {
		    printf("Could not accept a connection!\n");
		    continue;
	    }
	  
	    for(ProcessingUnit proc_unit : proc_units){
		proc_unit.startThreads(connection);
	    }
	    for(ProcessingUnit proc_unit : proc_units){
		proc_unit.joinThreads();
	    }

	    numberOfConnections--;
	    pthread_barrier_destroy(&barrier);
	    delete connection;
        }   

    delete connectionAcceptor;

    for(ProcessingUnit proc_unit : proc_units){
    	//proc_unit.printResults();
    	proc_unit.writeResults();
        proc_unit.clearResources();
    }

    return 0;
}
