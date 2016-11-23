#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "util/DaxWrapper.h"
#include "util/WorkQueue.h"
//#include "util/WorkQueueLocked.h"
#include "../thread/Thread.h"
#include "../network/server/TCPAcceptor.h"
#include "../data/DataLoader.h"
#include "../util/Partitioner.h"
#include "../util/Query.h"
#include "../util/Query3.h"
#include "../util/WorkItem.h"

pthread_barrier_t barrier;

class ConnectionHandler : public Thread<Query>
{
	//DaxWrapper::createContext(ctx);
	WorkQueue<Query> &scan_queue;
	TCPStream* t_stream;
	dax_context_t *ctx;
	int loop_id = 0;

	public:
	    ConnectionHandler(WorkQueue<Query>& queue) : scan_queue(queue){}

	void *run(){
//		DataCompressor* dataComp = scan_queue.getCompressor();
		//Partitioner *partitioner = scan_queue.getPartitioner();

		pthread_barrier_wait(&barrier);
		for(int i = 0; ; i++){
			//Change wait condition, maybe master can broadcast
			//while(scan_queue.size() > 0){
			while(scan_queue.getHead() != scan_queue.getTail()){
				Query item = scan_queue.remove();
				
				printf("Got id %d - loop: %d in thread %d!\n", item.getPart(), loop_id++, this->getId());
				scan_queue.printQueue();

				if(item.getType())
					break;
				//	Query3::linescan_sw(dataComp, item.getPart());
//				else
//				       break;
				//else
					//item.linescan_hw(dataComp, partitioner->assignNextPartition(), &ctx);
			}
			t_stream->send("", 0);
			return NULL;
		}
		return(0);
	}

	void setStream(TCPStream* stream){
		this->t_stream = stream;
	}

	void createDaxContext(){
		dax_status_t res = dax_thread_init(1, 1, 0, NULL, &ctx);
		if(res != 0)
			printf("Problem with DAX Context Creation! Return code is %d.\n", res);
	}
};

class ProcessingUnit{
	std::vector<ConnectionHandler*> conn_handlers; //4 cpu cores, 1 DAX Unit
	int num_of_units;
	WorkQueue<Query> job_queue;     
	vector<Node<Query>*> initialNodes;

	public: 
 	    ProcessingUnit(int num_of_threads, WorkQueue<Query> queue) : num_of_units(num_of_threads), job_queue(queue){}

	void createProcessingUnit(){
	    conn_handlers.reserve(50);
	    initialNodes.reserve(50);
	    for(int i = 0; i < num_of_units; i++){
		ConnectionHandler *handler = new ConnectionHandler(job_queue);
		handler->setId(i);
		conn_handlers.push_back(handler);	
		addWork(i);
	    }
	}

	void addWork(int part_id){
	    Query sw_item(0, part_id, 1); //lineitem
	    Node<Query> *newNode = new Node<Query>(sw_item);
	    //Query hw_item(1);
	    job_queue.add(newNode);
	    initialNodes.push_back(newNode);
	    //job_queue.add(hw_item);
	}

	void startThreads(TCPStream* connection){
	    for(ConnectionHandler* curConn : conn_handlers){
	        curConn->setStream(connection);
	        curConn->start(curConn->getId());
		curConn->reserveNodes(10);
 	    }
	}

	void joinThreads(){
	    for(ConnectionHandler* curConn : conn_handlers){
	        curConn->join();
 	    }
	}

	void clearResources(){
            for(ConnectionHandler* curHandler : conn_handlers){
	        delete curHandler; 
            }
	    Node<Query> *head = job_queue.getHead();
	    delete head;
	    for(Node<Query> *curNode : initialNodes){
	    	delete curNode;
	    }
	}
	
};

int main(int argc, char** argv)
{
	if ( argc != 5 ) {
		printf("usage: %s <workers> <port> <ip> <fileName>\n", argv[0]);
		exit(-1);
	}

	int workers = atoi(argv[1]); //Number of partitions
	int port = atoi(argv[2]);
	string ip = argv[3];
	string fileName = argv[4];

	pthread_barrier_init(&barrier, NULL, workers);

	Partitioner part;
	part.roundRobin(workers, fileName);

	DataLoader data_loader(fileName);
	data_loader.parseTable(part);
	table *t = data_loader.compressedTable->getTable();
	t->nb_lines = part.getEls();
	data_loader.compressTable();

	TCPAcceptor* connectionAcceptor;
	//connectionAcceptor = new TCPAcceptor(port, (char*)ip.c_str());
	connectionAcceptor = new TCPAcceptor(port, (char*)ip.c_str());
	if (!connectionAcceptor || connectionAcceptor->start() != 0) {
		printf("Could not create a connection acceptor\n");
		exit(1);
	}

	std::vector<ProcessingUnit> proc_units;
    	proc_units.reserve(10);

        for(int i = 0; i < 1; i++){
	    WorkQueue<Query> scan_queue(i, &part, data_loader.compressedTable); 
	    ProcessingUnit proc_unit (workers, scan_queue);
	    proc_unit.createProcessingUnit();
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

	    pthread_barrier_destroy(&barrier);

	    numberOfConnections--;
	    delete connection;
        }   

    delete connectionAcceptor;

    for(ProcessingUnit proc_unit : proc_units){
        proc_unit.clearResources();
    }

    return 0;
}
