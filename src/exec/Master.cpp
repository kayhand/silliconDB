#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "util/DaxWrapper.h"
#include "util/WorkQueue.h"
#include "../thread/Thread.h"
#include "../network/server/TCPAcceptor.h"
#include "../data/DataLoader.h"
#include "../util/Partitioner.h"
#include "../util/Query.h"
#include "../util/Query3.h"
#include "../util/WorkItem.h"

std::atomic<int> o_cnt = ATOMIC_VAR_INIT(0);
std::atomic<int> li_cnt = ATOMIC_VAR_INIT(0);

pthread_barrier_t barrier;
pthread_mutex_t queue_mutex;
pthread_cond_t queue_cond;

class CoreHandler : public Thread<Query>
{
	WorkQueue<Query> *scan_queue;
	TCPStream* t_stream;
	DataCompressor *lineitemComp = scan_queue->getDataLoader()->getDataCompressor(0);
	DataCompressor *ordersComp = scan_queue->getDataLoader()->getDataCompressor(1);
	int loop_id = 0;
	bool done = false;

	public:
	    CoreHandler(WorkQueue<Query>* queue) : scan_queue(queue){}

	void *run(){
		pthread_barrier_wait(&barrier);
		for(int i = 0; ; i++){
				while(scan_queue->getHead() != scan_queue->getTail()){
					Query item = scan_queue->remove();
					printf("Got from table id %d, partition %d - loop: %d in thread %d!\n", item.getTableId(), item.getPart(), loop_id++, this->getId());

					if(item.getTableId() == 0){
					    Query3::linescan_sw(lineitemComp, item.getPart());
					    li_cnt++;
					}
					else if(item.getTableId() == 1){
					    Query3::orderscan_sw(ordersComp, item.getPart());
					    o_cnt++;
					}
				}
		/*		if(!done){
			    	    pthread_mutex_lock(&queue_mutex);
				    while(scan_queue->getHead() == scan_queue->getTail()){
				        pthread_cond_wait(&queue_cond, &queue_mutex); 
				    }
				    pthread_mutex_unlock(&queue_mutex);
				}*/
				//if(done){
		    	            t_stream->send("", 0);
				    return NULL;
				//}
		}
		return(0);
	}

	void setFlag(){
	    done = true;
	}

	void setStream(TCPStream* stream){
		this->t_stream = stream;
	}
};

class DaxHandler : public Thread<Query>
{
	WorkQueue<Query> *scan_queue;
	TCPStream* t_stream;
	dax_context_t *ctx;
	DataCompressor *lineitemComp = scan_queue->getDataLoader()->getDataCompressor(0);
	DataCompressor *ordersComp = scan_queue->getDataLoader()->getDataCompressor(1);
	int loop_id = 0;
	bool done = false;

	public:
	    DaxHandler(WorkQueue<Query>* queue) : scan_queue(queue){}

	void *run(){
		pthread_barrier_wait(&barrier);
		createDaxContext();
		for(int i = 0; ; i++){
			while(scan_queue->getHead() != scan_queue->getTail()){
				Query item = scan_queue->remove();
				//printf("Got from table id %d, partition %d - loop: %d in thread %d!\n", item.getTableId(), item.getPart(), loop_id++, this->getId());

				if(item.getTableId() == 0){
				    Query3::linescan_hw(lineitemComp, item.getPart(), &ctx);
				    li_cnt++;
				}
				else if(item.getTableId() == 1){
				    Query3::orderscan_hw(ordersComp, item.getPart(), &ctx);
				    //Query3::orderscan_sw(ordersComp, item.getPart());
				    o_cnt++;
				}
				else if(item.getTableId() == 2){ //join
				    Query3::join_hw(lineitemComp, ordersComp, item.getPart(), &ctx);
				}
			}
			if(!done){
			    pthread_mutex_lock(&queue_mutex);
    			    while(scan_queue->getHead() == scan_queue->getTail()){
				pthread_cond_wait(&queue_cond, &queue_mutex); 
			    }
			    pthread_mutex_unlock(&queue_mutex);
			}
			else{
				int count = 0;
				for(int ind = 0; ind < ordersComp->getTable()->num_of_segments; ind++){
				count += __builtin_popcountl(ordersComp->getBitVector()[ind]);
				}
				printf("Orders scan count: %d\n", count);

				count = 0;
				for(int ind = 0; ind < lineitemComp->getTable()->num_of_segments; ind++){
				count += __builtin_popcountl(lineitemComp->getBitVector()[ind]);
				}
				printf("Lineitem scan count: %d\n", count);

				t_stream->send("", 0);
				return NULL;
			}
		}
		return(0);
	}

	void setStream(TCPStream* stream){
		this->t_stream = stream;
	}

	void setFlag(){
	    done = true;
	}

	bool getFlag(){
	    return done;
	}

	void createDaxContext(){
		dax_status_t res = dax_thread_init(1, 1, 0, NULL, &ctx);
		if(res != 0)
			printf("Problem with DAX Context Creation! Return code is %d.\n", res);
	}
};



class ProcessingUnit{
	std::vector<CoreHandler*> conn_handlers; //4 cpu cores, 1 DAX Unit
	DaxHandler *dax_handler;
	int num_of_units;
	WorkQueue<Query> job_queue;     
	vector<Node<Query>*> initialNodes;

	public: 
 	    ProcessingUnit(int num_of_threads, WorkQueue<Query> queue) : num_of_units(num_of_threads), job_queue(queue){}

	void createProcessingUnit(){
	    conn_handlers.reserve(50);
	    initialNodes.reserve(100);
	    for(int i = 0; i < num_of_units - 1; i++){
		CoreHandler *handler = new CoreHandler(&job_queue);
		handler->setId(i);
		conn_handlers.push_back(handler);	
	    }
	    dax_handler = new DaxHandler(&job_queue);
	    dax_handler->setId(num_of_units);
	}

	void addWork(int num_of_parts, int table_id){
	    for(int part_id = 0; part_id < num_of_parts; part_id++){
	    	Query item(part_id % 2, part_id, table_id); //(0: sw - 1:hw, part_id, table_id)
	    	Node<Query> *newNode = new Node<Query>(item);
	    	job_queue.add(newNode);
    	   	initialNodes.push_back(newNode);
	    }
	}

	void appendWork(int num_of_parts, int table_id){
	    for(int part_id = 0; part_id < num_of_parts; part_id++){
   	        Query item(1, part_id, table_id); //(0: sw - 1:hw, part_id, table_id)
	    	Node<Query> *newNode = new Node<Query>(item);
	    	job_queue.add(newNode);
    	   	initialNodes.push_back(newNode);
 	        //Node<Query> *newNode = this->returnNextNode(item);
                //job_queue.add(newNode);
	    }
	}

	void startThreads(TCPStream* connection){
	    for(CoreHandler* curConn : conn_handlers){
	        curConn->setStream(connection);
	        curConn->start(curConn->getId());
		curConn->reserveNodes(50);
 	    }
	    dax_handler->setStream(connection);
	    dax_handler->start(dax_handler->getId());
	    dax_handler->reserveNodes(10);
	}

	void joinThreads(){
	    dax_handler->join();
	    for(CoreHandler* curConn : conn_handlers){
	        curConn->join();
 	    }
	}

	WorkQueue<Query> &getJobQueue(){
	    return job_queue;
	}

	void endResources(){
            for(CoreHandler* curHandler : conn_handlers){
		curHandler->setFlag();
	    }
	    dax_handler->setFlag();
	}

	void clearResources(){
            for(CoreHandler* curHandler : conn_handlers){
	        delete curHandler; 
            }
	    delete dax_handler;
	    Node<Query> *head = job_queue.getHead();
	    delete head;
	    for(Node<Query> *curNode : initialNodes){
	    	delete curNode;
	    }
	}	
};

int main(int argc, char** argv)
{
	if ( argc != 6 ) {
		printf("usage: %s <workers> <port> <ip> <fileName> <fileName2>\n", argv[0]);
		exit(-1);
	}

	int workers = atoi(argv[1]); //Number of partitions
	int port = atoi(argv[2]);
	string ip = argv[3];
	string fileName = argv[4];
	string fileName2 = argv[5];

	pthread_barrier_init(&barrier, NULL, workers); 
	pthread_mutex_init(&queue_mutex, NULL);
	pthread_cond_init (&queue_cond, NULL);

	/*Lineitem table*/
	Partitioner line_part, orders_part;
	line_part.roundRobin(fileName); 
	orders_part.roundRobin(fileName2); 

	DataLoader data_loader(2);
	data_loader.initializeCompressor(fileName, 0, line_part);
	data_loader.initializeCompressor(fileName2, 1, orders_part);

	data_loader.parseTable(0);
	data_loader.compressTable(0);

	data_loader.parseTable(1);
	data_loader.compressTable(1);

	TCPAcceptor* connectionAcceptor;
	//connectionAcceptor = new TCPAcceptor(port, (char*)ip.c_str());
	connectionAcceptor = new TCPAcceptor(port, (char*)ip.c_str());
	if (!connectionAcceptor || connectionAcceptor->start() != 0) {
		printf("Could not create a connection acceptor\n");
		exit(1);
	}

	std::vector<ProcessingUnit> proc_units;
    	proc_units.reserve(10);
	
	WorkQueue<Query> scan_queue(0, &data_loader); 

        for(int i = 0; i < 1; i++){
	    //WorkQueue<Query> scan_queue(i, &data_loader); 
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

	    pthread_barrier_destroy(&barrier);

	    while(li_cnt < line_part.getNumberOfParts()){
	    }
    	    printf("li parts: %u\n", unsigned(li_cnt));

	    proc_units[0].endResources();
	    proc_units[0].appendWork(orders_part.getNumberOfParts(), 1);
	    for(ProcessingUnit proc_unit : proc_units){
		proc_unit.joinThreads();
	    }

	    pthread_mutex_lock(&queue_mutex);
	    pthread_cond_broadcast(&queue_cond); 
	    pthread_mutex_unlock(&queue_mutex);

	    while(o_cnt < orders_part.getNumberOfParts()){
	    }
    	    printf("o parts: %u\n", unsigned(o_cnt));

	    numberOfConnections--;
	    delete connection;
        }   

    delete connectionAcceptor;

    for(ProcessingUnit proc_unit : proc_units){
        proc_unit.clearResources();
    }

    //pthread_barrier_destroy(&barrier);
    pthread_mutex_destroy(&queue_mutex);
    pthread_cond_destroy(&queue_cond);

    return 0;
}
