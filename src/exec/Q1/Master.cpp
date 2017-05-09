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
#include "../../util/Query1.h"
#include "../util/WorkQueue.h"

std::atomic<int> li_cnt = ATOMIC_VAR_INIT(0);

pthread_barrier_t barrier;
pthread_barrier_t end_barrier;

bool done = 0;

class CoreHandler : public Thread<Query>
{
	WorkQueue<Query> *scan_queue;
	TCPStream* t_stream;
	int loop_id = 0;
	DataCompressor *lineitemComp = scan_queue->getDataLoader()->getDataCompressor(0);
	Result result;
	Query item;
	Node<Query> *node_item;

	public:
	    CoreHandler(WorkQueue<Query>* queue) : scan_queue(queue){}

	void *run(){
		pthread_barrier_wait(&barrier);
		for(int i = 0; ; i++){
		    while(scan_queue->getHead() != scan_queue->getTail()){
			node_item = scan_queue->remove_ref_core();
			item = node_item->value;
			printf("Core got from table id %d, partition %d - loop: %d in thread %d! :: isDax? %d\n", item.getTableId(), item.getPart(), loop_id++, this->getId(), item.isDax());
			if(item.getTableId() == 0){
			    Query1::linescan_sw(lineitemComp, item.getPart(), &result);
			    li_cnt++;
			    //put free node to thread local free list
			    this->putFreeNode(node_item);
			    addNewJob(0, item.getPart(), 2); //(0: hw/sw - 1:sw only, part_id, job type) -- scan or agg or count
			}
			else if(item.getTableId() == 2){
			    Query1::agg(lineitemComp, item.getPart(), &result, item.isDax());
			    //Query1::count(lineitemComp, item.getPart());
			}
		    }
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
	TCPStream* t_stream;
	dax_context_t *ctx;
	int loop_id = 0;
	DataCompressor *lineitemComp = scan_queue->getDataLoader()->getDataCompressor(0);
	Result result;
	Query item;
	Node<Query> *node_item;

	public:
	    DaxHandler(WorkQueue<Query>* queue) : scan_queue(queue){}

	void *run(){
		createDaxContext();
		pthread_barrier_wait(&barrier);
		for(int i = 0; ; i++){
			while(scan_queue->getHead() != scan_queue->getTail()){
				node_item = scan_queue->remove_ref_dax();
				if(node_item == NULL){
				    printf("DAX DONE!\n");
	    			    scan_queue->printQueue();
			  	    break;
				}
				item = node_item->value;
				printf("DAX got from table id %d, partition %d - loop: %d in thread %d!\n", item.getTableId(), item.getPart(), loop_id++, this->getId());
				if(item.getTableId() == 0){
				    Query1::linescan_hw(lineitemComp, item.getPart(), &result, &ctx);
				    li_cnt++;
				    this->putFreeNode(node_item);
				    addNewJob(1, item.getPart(), 2);
				    //delete node_item;
				}
			}
		    	pthread_barrier_wait(&end_barrier);
			t_stream->send("", 0);
			return NULL;
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

	void printResults(){
	    DataCompressor *lineitemComp = job_queue.getDataLoader()->getDataCompressor(0);
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

	    unordered_map<string, tuple<int, uint32_t>> agg_result_f;
	    agg_result_f.emplace("AF", make_tuple(0, 0.0));
	    agg_result_f.emplace("NO", make_tuple(0, 0.0));
	    agg_result_f.emplace("RF", make_tuple(0, 0.0));
	    agg_result_f.emplace("NF", make_tuple(0, 0.0));

            for(CoreHandler* curHandler : conn_handlers){
	    	Result result = curHandler->getResult();

	    	result.writeResults(SW_SCAN, sscan_f);
	    	result.writeResults(DAX_SCAN, dscan_f);
	    	result.writeResults(AGG, agg_f);
	    	result.writeAggResults(agg_result_f);
	    }
	    Result result = dax_handler->getResult();
	    result.writeResults(SW_SCAN, sscan_f);
	    result.writeResults(DAX_SCAN, dscan_f);
	    result.writeResults(AGG, agg_f);
	    result.writeAggResults(agg_result_f);
	    
	    for(auto &curr : agg_result_f)
	        fprintf(aggr_f, "%s -> %d:%u\n", (curr.first).c_str(), get<0>(curr.second), get<1>(curr.second));

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
	    Node<Query> *head = job_queue.getHead();
	    delete head;
	}	
};

int main(int argc, char** argv)
{
	if ( argc < 6 ) {
		printf("usage: %s <workers> <port> <ip> <fileName> <part_size> [scale_factor] \n", argv[0]);
		exit(-1);
	}

	int workers = atoi(argv[1]);
	int port = atoi(argv[2]);
	string ip = argv[3];
	string fileName = argv[4];
	int part_size = atoi(argv[5]);
	int sf = 1;
	//if(argc == 7)
	  //  sf = atoi(argv[6]);

	pthread_barrier_init(&barrier, NULL, workers + 1); 
	pthread_barrier_init(&end_barrier, NULL, workers + 1); 

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
	
	WorkQueue<Query> scan_queue(0, &data_loader); 
        for(int i = 0; i < 1; i++){
	    ProcessingUnit proc_unit (workers, scan_queue);
	    proc_unit.createProcessingUnit();
	    proc_unit.addWork(line_part.getNumberOfParts(), 0);
	    proc_units.push_back(proc_unit);
	    scan_queue.printQueue();
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
    	proc_unit.writeResults();
    	//proc_unit.printResults();
        proc_unit.clearResources();
    }

    return 0;
}
