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
	WorkQueue<Query> *scan_queue;
	WorkQueue<Query> *join_queue;
	TCPStream* t_stream;
	int loop_id = 0;
	DataCompressor *lineitemComp = scan_queue->getDataLoader()->getDataCompressor(0);
	DataCompressor *ordersComp = scan_queue->getDataLoader()->getDataCompressor(1);
	Result result;
	Query item;
	Node<Query> *node_item;
	bool agg_barr = false;

	public:
	    CoreHandler(WorkQueue<Query>* queue, WorkQueue<Query>* j_queue) : scan_queue(queue), join_queue(j_queue){}

	void *run(){
		pthread_barrier_wait(&barrier);
		for(int i = 0; ; i++){
		    while(scan_queue->getHead() != scan_queue->getTail()){
			node_item = scan_queue->remove_ref_core();
			if(node_item == NULL){
			    scan_queue->printQueue();
			    printf("core waiting...\n");
		    	    pthread_barrier_wait(&agg_barrier);
			    agg_barr = true;
			    printf("core continues...\n");
			    continue;
			}
			item = node_item->value;
			if(item.getTableId() == 0){
			    printf("Core got from table id %d, partition %d - loop: %d in thread %d!\n", item.getTableId(), item.getPart(), loop_id++, this->getId());
			    //Query3::linescan_sw(lineitemComp, item.getPart(), &result);
			    Query3::linescan_simd(lineitemComp, item.getPart(), &result);
			    li_cnt++;
			    //put free node to thread local free list
			    this->putFreeNode(node_item);
			    addNewJob(0, item.getPart(), 11); //(0: hw/sw - 1:sw only, part_id, job type) -- join job
			}
			else if(item.getTableId() == 1){
			    printf("Core got from table id %d, partition %d - loop: %d in thread %d!\n", item.getTableId(), item.getPart(), loop_id++, this->getId());
			    Query3::orderscan_simd(ordersComp, item.getPart(), &result);
			    o_cnt++;
			    //put free node to thread local free list
			    this->putFreeNode(node_item);
			    //addNewJob(0, item.getPart(), 3); //(0: hw/sw - 1:sw only, part_id, job type)
			}
			else if(item.getTableId() == 4){
			    printf("Core got an aggregation job for partition %d - loop: %d in thread %d!\n", item.getPart(), loop_id++, this->getId());
			    Query3::agg<uint32_t, uint32_t, uint8_t>(lineitemComp, item.getPart(), &result);
			    //Query3::count(orderComp, item.getPart(), &result);
			    this->putFreeNode(node_item);
			}
			else if(item.getTableId() == 11){
			    printf("Core got a join job for part %d\n", item.getPart());
			    Query3::join_sw(lineitemComp, ordersComp, item.getPart(), &result);
		            this->putFreeNode(node_item);
			    addNewJob(1, item.getPart(), 4); //add agg job -- id = 4
			}
		    }
		    if(!agg_barr)
		    	pthread_barrier_wait(&agg_barrier);
		    pthread_barrier_wait(&end_barrier);
	            t_stream->send("", 0);
		    return NULL;
		}
		return(0);
	}

	Result &getResult(){
	    return result;
	}

	void addNewJob(int r_id, int p_id, int j_type){
 	    Node<Query> *newNode = this->returnNextNode(r_id, p_id, j_type);
            //join_queue->add(newNode);		
//	    if(j_type == 11)
  //              join_queue->add(newNode);		
//	    else
                scan_queue->add(newNode);		
	}

	void setStream(TCPStream* stream){
		this->t_stream = stream;
	}
};

#ifdef __sun 
class DaxHandler : public Thread<Query>
{
	WorkQueue<Query> *scan_queue;
	WorkQueue<Query> *join_queue;
	TCPStream* t_stream;
	dax_context_t *ctx;
	int loop_id = 0;
	DataCompressor *lineitemComp = scan_queue->getDataLoader()->getDataCompressor(0);
	DataCompressor *ordersComp = scan_queue->getDataLoader()->getDataCompressor(1);
	int o_parts = ordersComp->getNumOfParts();
	Result result;
	Query item;
	Node<Query> *node_item;

	public:
	    DaxHandler(WorkQueue<Query>* queue, WorkQueue<Query>* j_queue) : scan_queue(queue), join_queue(j_queue){}

	void *run(){
		createDaxContext();
		pthread_barrier_wait(&barrier);
		for(int i = 0; ; i++){
/*			if(o_parts == o_cnt){
			    if(join_queue->getHead() != join_queue->getTail()){
				node_item = join_queue->remove_ref_dax();
				item = node_item->value;
				if(item.getTableId() == 11){
				    printf("DAX got a join job for part %d\n", item.getPart());
				    Query3::join_hw(lineitemComp, ordersComp, item.getPart(), &result, &ctx);
			            this->putFreeNode(node_item);
				    //addNewJob(1, item.getPart(), 22); //add count job -- id = 3
				}
				else{
				    printf("DAX got a count job for part %d\n", item.getPart());
				    Query3::count(lineitemComp, item.getPart(), &result);
			            this->putFreeNode(node_item);
			        }
			    }	
			    else{
		    		pthread_barrier_wait(&end_barrier);
				t_stream->send("", 0);
				return NULL;
			    }
			}*/
			if(scan_queue->getHead() != scan_queue->getTail()){
				node_item = scan_queue->remove_ref_dax();
				if(node_item == NULL){
				    printf("DAX DONE and on agg_barrier now!\n");
		    		    pthread_barrier_wait(&agg_barrier);
	    			    scan_queue->printQueue();
		    	    	    pthread_barrier_wait(&end_barrier);
				    continue;
				}
				item = node_item->value;
				if(item.getTableId() == 0){
				    printf("DAX got from table id %d, partition %d - loop: %d in thread %d!\n", item.getTableId(), item.getPart(), loop_id++, this->getId());
				    Query3::linescan_hw(lineitemComp, item.getPart(), &result, &ctx);
				    li_cnt++;
				    this->putFreeNode(node_item);
				    addNewJob(1, item.getPart(), 11); //add join job -- id = 2
				}
				else if(item.getTableId() == 1){
				    printf("DAX got from table id %d, partition %d - loop: %d in thread %d!\n", item.getTableId(), item.getPart(), loop_id++, this->getId());
				    Query3::orderscan_hw(ordersComp, item.getPart(), &result, &ctx);
				    o_cnt++;
				    this->putFreeNode(node_item);
			    	    //addNewJob(1, item.getPart(), 3); //(0: hw/sw - 1:sw only, part_id, job type)
				    //delete node_item;
				}
				else if(item.getTableId() == 11){
				    printf("DAX got a join job for part %d\n", item.getPart());
				    Query3::join_hw(lineitemComp, ordersComp, item.getPart(), &result, &ctx);
			            this->putFreeNode(node_item);
				    addNewJob(1, item.getPart(), 4); //add agg job -- id = 4
				}
			}
			else{
			    t_stream->send("", 0);
			    return NULL;
		        }
		}
		return(0);
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
	//    if(j_type % 11 == 0)
          //      join_queue->add(newNode);		
 	    //else
                scan_queue->add(newNode);		
	}

	void createDaxContext(){
	    	//int lFile = open("/tmp/dax_log.txt", O_RDWR);

		dax_status_t res = dax_thread_init(1, 1, 0, NULL, &ctx);
		if(res != 0)
			printf("Problem with DAX Context Creation! Return code is %d.\n", res);
		//res = dax_queue_create(ctx, 5, &queue);
		//if(res != 0)
		//	printf("Problem with DAX Queue Creation! Return code is %d.\n", res);
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
	WorkQueue<Query> job_queue;     
	WorkQueue<Query> join_queue;     

	public: 
 	    ProcessingUnit(int num_of_threads, WorkQueue<Query> queue, WorkQueue<Query> j_queue) : num_of_units(num_of_threads), job_queue(queue), join_queue(j_queue){}

	void createProcessingUnit(){
	    conn_handlers.reserve(50);
	    for(int i = 0; i < num_of_units; i++){
		CoreHandler *handler = new CoreHandler(&job_queue, &join_queue);
		handler->setId(i);
		conn_handlers.push_back(handler);	
	    }
	    #ifdef __sun 
	    dax_handler = new DaxHandler(&job_queue, &join_queue);
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

	WorkQueue<Query> &getJoinQueue(){
	    return join_queue;
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
	    delete dax_handler;
	    #endif
	    Node<Query> *head = job_queue.getHead();
	    delete head;
	    Node<Query> *j_head = join_queue.getHead();
	    delete j_head;
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
	
	pthread_barrier_init(&barrier, NULL, workers + 1); 
	pthread_barrier_init(&agg_barrier, NULL, workers + 1); 
	pthread_barrier_init(&end_barrier, NULL, workers + 1); 

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
	
	WorkQueue<Query> scan_queue(0, &data_loader); 
	WorkQueue<Query> join_queue(1); 
        for(int i = 0; i < 1; i++){
	    ProcessingUnit proc_unit (workers, scan_queue, join_queue);
	    proc_unit.createProcessingUnit();
	    proc_unit.addWork(line_part.getNumberOfParts(), 0);
	    proc_unit.addWork(order_part.getNumberOfParts(), 1);
	    proc_units.push_back(proc_unit);
	    scan_queue.printQueue();
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
