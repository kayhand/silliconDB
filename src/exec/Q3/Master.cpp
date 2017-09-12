#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <fcntl.h>

#include "../../thread/Thread.h"
#include "../../network/server/TCPAcceptor.h"
#include "../../data/DataLoader.h"
#include "../../util/Partitioner.h"
#include "../../util/Query.h"
#include "../../util/Query3.h"
#include "../util/WorkQueue.h"

std::atomic<int> li_cnt = ATOMIC_VAR_INIT(0);
std::atomic<int> o_cnt = ATOMIC_VAR_INIT(0);

pthread_barrier_t barrier;
pthread_barrier_t agg_barrier;
pthread_barrier_t end_barrier;

class CoreHandler : public Thread<Query>{
	WorkQueue<Query> *work_queue;
	TCPStream* t_stream;
	int loop_id = 0;
	DataCompressor *lineitemComp = work_queue->getDataLoader()->getDataCompressor(0);
	DataCompressor *ordersComp = work_queue->getDataLoader()->getDataCompressor(1);
	Result result;
	Query item;
	Node<Query> *node_item;
	bool agg_barr = false;
	bool daxDone = false;

	public:
	    CoreHandler(WorkQueue<Query>* queue) : work_queue(queue){}

	void *run(){
		pthread_barrier_wait(&barrier);
		for(int i = 0; ; i++){
		    while(work_queue->getHead() != work_queue->getTail()){
		    	work_queue->printQueue();
			node_item = work_queue->remove_ref_core();
			if(node_item == NULL){
			    work_queue->printQueue();
			    printf("CORE WAITING...\n");
		    	    //pthread_barrier_wait(&agg_barrier);
			    agg_barr = true;
			    printf("core continues...\n");
			    continue;
			}
			item = node_item->value;
			if(item.getTableId() == 0){
			    printf("sw_line(%d) in thr. %d (%d itr.)\n", item.getPart(), this->getId(), loop_id++);
			    //Query3::linescan_sw(lineitemComp, item.getPart(), &result);
			    Query3::linescan_simd(lineitemComp, item.getPart(), &result);
			    li_cnt++;
			    //put free node to thread local free list
			    this->putFreeNode(node_item);
			    addNewJob(0, item.getPart(), 11); //(0: hw/sw - 1:sw only, part_id, job type) -- join job
			}
			else if(item.getTableId() == 1){
			    printf("sw_order(%d) in thr. %d (%d itr.)\n", item.getPart(), this->getId(), loop_id++);
			    Query3::orderscan_simd(ordersComp, item.getPart(), &result);
			    o_cnt++;
			    //put free node to thread local free list
			    this->putFreeNode(node_item);
			    //addNewJob(0, item.getPart(), 3); //(0: hw/sw - 1:sw only, part_id, job type)
			}
			else if(item.getTableId() == 4){
			    printf("agg(%d) in thr. %d (%d itr.)\n", item.getPart(), this->getId(), loop_id++);
			    Query3::agg<uint32_t, uint32_t, uint8_t>(lineitemComp, item.getPart(), &result);
			    //Query3::count(orderComp, item.getPart(), &result);
			    this->putFreeNode(node_item);
			}
			else if(item.getTableId() == 11){
			    printf("sw_join(%d) in thr. %d (%d itr.)\n", item.getPart(), this->getId(), loop_id++);
			    Query3::join_sw(lineitemComp, ordersComp, item.getPart(), &result);
		            this->putFreeNode(node_item);
			    //addNewJob(1, item.getPart(), 4); //add agg job -- id = 4
			}
		    }
		    if(!daxDone){
			printf("Core on agg_barrier...\n");
		    	pthread_barrier_wait(&agg_barrier);
			daxDone = true;
		    }
		    else{
		    	break;
		    }
		}
		
		printf("Core on end_barrier...\n");
		pthread_barrier_wait(&end_barrier);
		printf("Core releasing...\n");
	        t_stream->send("", 0);
		return NULL;
		return(0);
	}

	Result &getResult(){
	    return result;
	}

	void addNewJob(int r_id, int p_id, int j_type){
 	    Node<Query> *newNode = this->returnNextNode(r_id, p_id, j_type);
            work_queue->add(newNode);		
	}

	void setStream(TCPStream* stream){
		this->t_stream = stream;
	}
};

#ifdef __sun 
class DaxHandler : public Thread<Query>
{
	WorkQueue<Query> *work_queue;
	TCPStream* t_stream;
	dax_context_t *ctx;
	int loop_id = 0;
	DataCompressor *lineitemComp = work_queue->getDataLoader()->getDataCompressor(0);
	DataCompressor *ordersComp = work_queue->getDataLoader()->getDataCompressor(1);
	int o_parts = ordersComp->getNumOfParts();
	Result result;
	Query item;
	Node<Query> *node_item;

	dax_queue_t *dax_queue;
	static const int q_size = 1;
	int items_done = dax_status_t::DAX_EQEMPTY;
	bool daxDone = false;

	vector<Node<Query>*> items; 

	public:
	    DaxHandler(WorkQueue<Query>* queue) : work_queue(queue){}

	void *run(){
		createDaxContext(&dax_queue);
		pthread_barrier_wait(&barrier);
		for(int i = 0; ; i++){
			if(daxDone){
			    printf("DAX on agg_barrier now!\n");
		       	    pthread_barrier_wait(&agg_barrier);
	    	    	    work_queue->printQueue();

			    printf("DAX on end_barrier now!\n");
		      	    pthread_barrier_wait(&end_barrier);
			    printf("DAX releasing...\n");
			    t_stream->send("", 0);
			    return NULL;
			}
			else if(work_queue->getHead() != work_queue->getTail()){
			    if(items_done == dax_status_t::DAX_EQEMPTY){
			    	work_queue->printQueue();
			        node_item = work_queue->remove_ref_dax();
			        if(node_item == NULL){ //Aggregation job -- DAX is done
			            daxDone = true;
				    printf("DAX DONE!\n");
				    continue;
		                }
				else{
				    postWorkToDAX(node_item->value);
				    items_done = 0;
				}
			    }
			}
			//TODO: Check the case where work_queue is empty but there are still elements in dax internal queue
			else if(items_done == dax_status_t::DAX_EQEMPTY){
				    printf("DAX DONE!\n");
				    daxDone = true;
				    continue;
			}

			dax_poll_t poll_data[q_size];
			items_done = dax_poll(dax_queue, poll_data, 1, -1);
			//items_done = dax_status_t::DAX_EQEMPTY;
			if(items_done > 0){
			    handlePollReturn(items_done, poll_data);
			    items_done = dax_status_t::DAX_EQEMPTY;
			}
		}
		return(0);
	}

	void postWorkToDAX(Query work_item){
	    int partId = work_item.getPart();
	    if(work_item.getTableId() == 0){
	     	printf("hw_line(%d) pushed to the DAX queue (itr. %d)\n", partId, loop_id++);
	        Query3::linescan_hw(lineitemComp, partId, &result, &ctx, &dax_queue, true, (void *) node_item); //true == dax_poll
		li_cnt++;
	    }
	    else if(work_item.getTableId() == 1){
    	    	printf("hw_order(%d) pushed to the DAX queue (itr. %d)\n", partId, loop_id++);
	        Query3::orderscan_hw(ordersComp, partId, &result, &ctx, &dax_queue, true, (void *) node_item);
		o_cnt++;
	    }
    	    else if(work_item.getTableId() == 11){
   	    	printf("hw_join(%d) pushed to the DAX queue in (itr. %d)\n", partId, loop_id++);
	        Query3::join_hw(lineitemComp, ordersComp, partId, &result, &ctx, &dax_queue, true, (void *) node_item);
	    }
	}

	Result &getResult(){
	    return result;
	}

	void setStream(TCPStream* stream){
		this->t_stream = stream;
	}

	void addNewJob(int r_id, int p_id, int j_type){
 	    Node<Query> *newNode = this->returnNextNode(r_id, p_id, j_type);
	    newNode->value.flipDax();
            work_queue->add(newNode);		
	}

	void createDaxContext(dax_queue_t **queue){
	    	int lFile = open("/tmp/dax_log.txt", O_RDWR);

		dax_status_t res = dax_thread_init(1, 1, 0, NULL, &ctx);
		if(res != 0)
			printf("Problem with DAX Context Creation! Return code is %d.\n", res);
		res = dax_queue_create(ctx, 1, queue);
		if(res != 0)
			printf("Problem with DAX Queue Creation! Return code is %d.\n", res);
		res = dax_set_log_file(ctx, DAX_LOG_ALL, lFile);
		if(res != 0)
		    printf("Problem with DAX Logger Creation! Return code is %d.\n", res);
	}

	void handlePollReturn(int items_done, dax_poll_t *poll_data){
	    Node<Query> *node_item;
	    Query item;
	    int job_id;
	    int part_id;
	    int cnt;
	    for(int i = 0; i < items_done; i++){
		node_item = (Node<Query> *) poll_data[i].udata;
	    	this->putFreeNode(node_item);
		item = node_item->value;
		job_id = item.getTableId();
		part_id = item.getPart();
		cnt = poll_data[i].count;
		if(job_id == 0){
		    printf("hw_line(%d) completed (itr. %d) - Count (%d) \n", part_id, loop_id, cnt);
	    	    addNewJob(1, part_id, 11); //add join job
    	   	    result.addCountResult(make_tuple(poll_data[i].count, job_id)); 	
		}
		else if(job_id == 1){
		    printf("hw_order(%d) completed (itr. %d) - Count (%d)\n", part_id, loop_id, cnt);
    	   	    result.addCountResult(make_tuple(poll_data[i].count, job_id)); 	
		}
		else{
		    printf("hw_join(%d) completed (itr. %d) - Count (%d)\n", part_id, loop_id, cnt);
		    //addNewJob(1, part_id, 4); //add agg job
    	   	    result.addCountResult(make_tuple(poll_data[i].count, 2)); 	
		}
	    }
	}
};
#endif

class ProcessingUnit{
	std::vector<CoreHandler*> conn_handlers; //4 cpu cores, 1 DAX Unit
	#ifdef __sun 
	DaxHandler *dax_handler;
	#endif
	int num_of_units;
	WorkQueue<Query> job_queue;     

	public: 
 	    ProcessingUnit(int num_of_threads, WorkQueue<Query> queue) : num_of_units(num_of_threads), job_queue(queue){}

	void createProcessingUnit(){
	    conn_handlers.reserve(50);
	    for(int i = 0; i < num_of_units; i++){
		CoreHandler *handler = new CoreHandler(&job_queue);
		handler->setId(i);
		conn_handlers.push_back(handler);	
	    }
	    #ifdef __sun
	    dax_handler = new DaxHandler(&job_queue);
	    dax_handler->setId(num_of_units);
	    #endif
	}

	void addWork(int num_of_parts, int table_id){
	    for(int part_id = 0; part_id < num_of_parts; part_id++){
	    	Query item(part_id % 2, part_id, table_id); //(0: sw - 1:hw, part_id, table_id)
	    	Node<Query> *newNode = new Node<Query>(item);
	    	job_queue.add(newNode);
	    }
	}	

	void startThreads(TCPStream* connection){
	    for(CoreHandler* curConn : conn_handlers){
	        curConn->setStream(connection);
	        curConn->start(curConn->getId());
 	    }
	    #ifdef __sun 
	    dax_handler->setStream(connection);
	    dax_handler->start(dax_handler->getId());
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

	WorkQueue<Query> &getJobQueue(){
	    return job_queue;
	}

	void writeResults(){
	    FILE *sscan_f = fopen("sw_scan.txt", "a");
	    FILE *dscan_f = fopen("dax_scan.txt", "a");
	    FILE *agg_f = fopen("agg.txt", "a");
	    FILE *aggr_f = fopen("agg_result.txt", "a");
	    FILE *count_f = fopen("count.txt", "a");
	    FILE *countr_f = fopen("count_result.txt", "a");
	    FILE *join_f = fopen("join.txt", "a");

	    unordered_map<uint32_t, float> agg_result_f;

	    vector<int> count_result_f {0, 0, 0, 0}; //0: lineitem, 1: orders, 2: join, 3: count

            for(CoreHandler* curHandler : conn_handlers){
	    	Result result = curHandler->getResult();

	    	result.writeResults(SW_SCAN, sscan_f);
	    	result.writeResults(DAX_SCAN, dscan_f);
	    	result.writeResults(AGG, agg_f);
	    	result.writeResults(JOIN, join_f);
	    	result.writeAggResultsQ3(agg_result_f);
		result.writeCountResults(count_result_f);
	    }
	    Result result = dax_handler->getResult();
	    result.writeResults(SW_SCAN, sscan_f);
	    result.writeResults(DAX_SCAN, dscan_f);
	    result.writeResults(AGG, agg_f);
	    result.writeResults(JOIN, join_f);
	    result.writeCountResults(count_result_f);
	    
	    for(auto &curr : agg_result_f)
	        fprintf(aggr_f, "%d -> %.2lf\n", curr.first, curr.second);

	    fprintf(countr_f, "l_count: %d\n", count_result_f[0]);
	    fprintf(countr_f, "o_count: %d\n", count_result_f[1]);
	    fprintf(countr_f, "j_count: %d\n", count_result_f[2]);

	    fclose(sscan_f);
	    fclose(dscan_f);
	    fclose(agg_f);
	    fclose(aggr_f);
	    fclose(count_f);
	    fclose(countr_f);
	    fclose(join_f);
	}

	void clearResources(){
            for(CoreHandler* curHandler : conn_handlers){
	        delete curHandler; 
            }
	    #ifdef __sun 
	    //dax_queue_destroy(dax_queue);
	    delete dax_handler;
	    #endif
	    Node<Query> *head = job_queue.getHead();
	    delete head;
	}	
};

int main(int argc, char** argv)
{
	if ( argc < 7 ) {
		printf("usage: %s <workers> <port> <ip> <fileName> <fileName2> <part_size> [scale_factor] \n", argv[0]);
		exit(-1);
	}

	int workers = atoi(argv[1]);
	int port = atoi(argv[2]);
	string ip = argv[3];
	string fileName = argv[4];
	string fileName2 = argv[5];
	int part_size = atoi(argv[6]);
	int sf = 1;
	//if(argc == 7)
	  //  sf = atoi(argv[6]);

	printf("Reading from %s and %s\n", fileName.c_str(), fileName2.c_str());
	
	int num_of_barriers = workers + 1;
	pthread_barrier_init(&barrier, NULL, num_of_barriers); 
	pthread_barrier_init(&agg_barrier, NULL, num_of_barriers); 
	pthread_barrier_init(&end_barrier, NULL, num_of_barriers); 

	/*Lineitem table*/
	Partitioner line_part;
	line_part.roundRobin(fileName, part_size); 
	Partitioner order_part;
	order_part.roundRobin(fileName2, part_size); 

	DataLoader data_loader(2);

	data_loader.initializeCompressor(fileName, 0, line_part, sf);
	data_loader.parseTable(0);
	data_loader.compressTable(0);

	data_loader.initializeCompressor(fileName2, 1, order_part, sf);
	data_loader.parseTable(1);
	data_loader.compressTable(1);

	TCPAcceptor* connectionAcceptor;
	connectionAcceptor = new TCPAcceptor(port, (char*)ip.c_str());
	if (!connectionAcceptor || connectionAcceptor->start() != 0) {
		printf("Could not create a connection acceptor\n");
		exit(1);
	}

	std::vector<ProcessingUnit> proc_units;
    	proc_units.reserve(10);
	
	WorkQueue<Query> work_queue(0, &data_loader); 
        for(int i = 0; i < 1; i++){
	    ProcessingUnit proc_unit (workers, work_queue);
	    proc_unit.createProcessingUnit();
	    proc_unit.addWork(line_part.getNumberOfParts(), 0);
	    proc_unit.addWork(order_part.getNumberOfParts(), 1);
	    proc_units.push_back(proc_unit);
	    work_queue.printQueue();
        }

        int numberOfConnections = 1;
        TCPStream* connection;
        while(numberOfConnections > 0){
	    printf("Gimme some connection!\n");
	    connection = connectionAcceptor->accept();
	    if(!connection){
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
    	proc_unit.writeResults();
        proc_unit.clearResources();
    }

    return 0;
}
