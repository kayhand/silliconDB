#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <fcntl.h>

#include "../../thread/Thread.h"
#include "../../network/server/TCPAcceptor.h"
#include "../../data/DataLoader.h"
#include "../../log/Result.h"
#include "../../util/Partitioner.h"
#include "../../util/Query.h"
#include "../../util/Query1.h"
#include "../util/WorkQueue.h"

std::atomic<int> li_cnt = ATOMIC_VAR_INIT(0);
std::atomic<int> c_cnt = ATOMIC_VAR_INIT(0);

pthread_barrier_t barrier;
pthread_barrier_t end_barrier;

class CoreHandler : public Thread<Query>
{
	WorkQueue<Query> *sw_queue;
	WorkQueue<Query> *shared_queue;
	TCPStream* t_stream;
	int loop_id = 0;
	DataCompressor *lineitemComp = sw_queue->getDataLoader()->getDataCompressor(0);
	Result result;
	Query item;
	Node<Query> *node_item;

	public:
	    CoreHandler(WorkQueue<Query>* sw_queue) : sw_queue(sw_queue){}
	    CoreHandler(WorkQueue<Query>* sw_queue, WorkQueue<Query>* shared_queue) : sw_queue(sw_queue), shared_queue(shared_queue){}

	void *run(){
		pthread_barrier_wait(&barrier);
		while(true){
			if(sw_queue->getHead() != sw_queue->getTail()){
			    node_item = sw_queue->remove_ref_core();
			    item = node_item->value;
			    printf("Core got job id %d, partition %d - loop: %d in thread %d! dax? %d\n", item.getTableId(), item.getPart(), loop_id++, this->getId(), item.isDax());
			    //std::atomic_fetch_add(&c_cnt, 1);
			    Query1::agg(lineitemComp, item.getPart(), &result, item.isDax());
			}
			else if(shared_queue->getHead() != shared_queue->getTail()){
			    	node_item = shared_queue->remove_ref_core();
			    	item = node_item->value;
			    	printf("Core' got job id %d, partition %d - loop: %d in thread %d!\n", item.getTableId(), item.getPart(), loop_id++, this->getId());
				
				Query1::linescan_sw(lineitemComp, item.getPart(), &result);
				//std::atomic_fetch_add(&li_cnt, 1);
				this->putFreeNode(node_item);
				addNewJob(0, item.getPart(), 2);
			}
			else{
				pthread_barrier_wait(&end_barrier);
	    	        	t_stream->send("", 0);
				return NULL;
			}
		}
		return(0);
	}
	
	void addNewJob(int r_id, int p_id, int j_type){
 	    Node<Query> *newNode = this->returnNextNode(r_id, p_id, j_type);
            sw_queue->add(newNode);		
	}

	void setStream(TCPStream* stream){
		this->t_stream = stream;
	}

	Result &getResult(){
	    return result;
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
	Result result;
	int num_of_parts = lineitemComp->getNumOfParts();
	Query item;
	Node<Query> *node_item;

	int loop_id = 0;

	public:
	    DaxHandler(WorkQueue<Query>* shared_queue) : shared_queue(shared_queue){}
	    DaxHandler(WorkQueue<Query>* sw_queue, WorkQueue<Query>* shared_queue) : sw_queue(sw_queue), shared_queue(shared_queue){}

	void *run(){
		createDaxContext();
		pthread_barrier_wait(&barrier);
		while(true){
			if(shared_queue->getHead() != shared_queue->getTail()){
			    node_item = shared_queue->remove_ref_dax(); //We don't need the check as in ref_dax function
			    item = node_item->value;
			    printf("DAX got job id %d, partition %d - loop: %d in thread %d!\n", item.getTableId(), item.getPart(), loop_id++, this->getId());
			    Query1::linescan_hw(lineitemComp, item.getPart(), &result, &ctx);
			    //std::atomic_fetch_add(&li_cnt, 1);

			    this->putFreeNode(node_item);
			    addNewJob(1, item.getPart(), 2);
			}
			else{
			    //Wait for cores to finish
			    pthread_barrier_wait(&end_barrier);
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
            sw_queue->add(newNode);		
	}

	void createDaxContext(){
	    	//int lFile = open("/tmp/dax_log.txt", O_RDWR);
		dax_status_t res = dax_thread_init(1, 1, 0, NULL, &ctx);
		if(res != 0)
			printf("Problem with DAX Context Creation! Return code is %d.\n", res);
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

	public: 
 	    ProcessingUnit(int num_of_threads, WorkQueue<Query> sw_queue, WorkQueue<Query> shared_queue) : num_of_units(num_of_threads), sw_queue(sw_queue), shared_queue(shared_queue){}

	void createProcessingUnit(){
	    conn_handlers.reserve(50);
	    for(int i = 0; i < num_of_units; i++){
		CoreHandler *handler = new CoreHandler(&sw_queue, &shared_queue);
		handler->setId(i);
		conn_handlers.push_back(handler);	
	    }
	    #ifdef __sun 
	    dax_handler = new DaxHandler(&sw_queue, &shared_queue);
	    dax_handler->setId(num_of_units);
	    #endif
	}

	void addWork(int num_of_parts, int table_id){
	    for(int part_id = 0; part_id < num_of_parts; part_id++){
	    	Query item(part_id % 2, part_id, table_id); //(0: sw - 1:hw, part_id, table_id)
	    	Node<Query> *newNode = new Node<Query>(item);
	    	shared_queue.add(newNode);
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

	    Node<Query> *sw_head = sw_queue.getHead();
	    delete sw_head;
	    Node<Query> *shared_head = shared_queue.getHead();
	    delete shared_head;
	}	
};

int main(int argc, char** argv)
{
	if ( argc != 6 ) {
		printf("usage: %s <workers> <port> <ip> <fileName> <part_size> [scale_factor]\n", argv[0]);
		exit(-1);
	}

	int workers = atoi(argv[1]); //Number of partitions
	int port = atoi(argv[2]);
	string ip = argv[3];
	string fileName = argv[4];
	int part_size = atoi(argv[5]);
	
	int sf = 1;
	//if( argc == 7)
	    //sf = atoi(argv[6]);

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
	//connectionAcceptor = new TCPAcceptor(port, (char*)ip.c_str());
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
	    proc_unit.addWork(line_part.getNumberOfParts(), 0);
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
	    pthread_barrier_destroy(&end_barrier);
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
