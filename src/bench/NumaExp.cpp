#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <fcntl.h>

#include "util/DaxWrapper.h"
#include "util/WorkQueue.h"
#include "../thread/Thread.h"
#include "../network/server/TCPAcceptor.h"
#include "../data/DataLoader.h"
#include "../util/Partitioner.h"
#include "../util/Query.h"
#include "../util/Query1.h"

std::atomic<int> li_cnt = ATOMIC_VAR_INIT(0);

pthread_barrier_t barrier, dax_barrier;
pthread_mutex_t queue_mutex;
pthread_cond_t queue_cond;

class CoreHandler : public Thread<Query>
{
	WorkQueue<Query> *scan_queue;
	TCPStream* t_stream;
	DataCompressor *lineitemComp = scan_queue->getDataLoader()->getDataCompressor(0);
	int loop_id = 0;
	bool done = false;

	public:
	    CoreHandler(WorkQueue<Query>* queue) : scan_queue(queue){}

	void *run(){
		pthread_barrier_init(&dax_barrier, NULL, 2); 
		//for(int i = 0; i < 32 * 8; i++){
		//	setAffinity(i);
		//	if(i == 0)
			pthread_barrier_wait(&barrier);
			printf("Starting count...\n");
		        Query1::count(lineitemComp, 0);
		//}
		//pthread_barrier_wait(&dax_barrier);
	    	t_stream->send("", 0);
		return NULL;
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
	int loop_id = 0;
	bool done = false;

	public:
	    DaxHandler(WorkQueue<Query>* queue) : scan_queue(queue){}

	void *run(){
		createDaxContext();
		for(int i = 0; ; i++){
			printf("DAX starting...\n");
			//Query1::linescan_hw(lineitemComp, 0, &ctx, &barrier, &dax_barrier);	
			Query1::linescan_hw(lineitemComp, 0, &ctx);	
			printf("Bye bye DAX...\n");
			pthread_barrier_wait(&barrier);
			break;
		}
		releaseDaxContext();
		t_stream->send("", 0);
		return NULL;
		return(0);
	}

	void addNewJob(Query item){
 	    Node<Query> *newNode = this->returnNextNode(item);
            scan_queue->add(newNode);		
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
	    	//int lFile = open("/tmp/dax_log.txt", O_RDWR);

		dax_status_t res = dax_thread_init(1, 1, 0, NULL, &ctx);
		if(res != 0)
			printf("Problem with DAX Context Creation! Return code is %d.\n", res);

		//res = dax_set_log_file(ctx, DAX_LOG_ALL, lFile);
		if(res != 0)
			printf("Problem with DAX Logger Creation! Return code is %d.\n", res);
	}
	void releaseDaxContext(){	
		dax_status_t res = dax_thread_fini(ctx);
		if(res != 0)
			printf("Problem with DAX Context Deletion! Return code is %d.\n", res);
	}
};

class ProcessingUnit{
	CoreHandler *core_handler;
	DaxHandler *dax_handler;
	int num_of_units;
	WorkQueue<Query> job_queue;     
	vector<Node<Query>*> initialNodes;

	public: 
 	    ProcessingUnit(int num_of_threads, WorkQueue<Query> queue) : num_of_units(num_of_threads), job_queue(queue){}

	void createProcessingUnit(int cpu_id){
	    initialNodes.reserve(10);

	    core_handler = new CoreHandler(&job_queue);
	    core_handler->setId(cpu_id);
	    dax_handler = new DaxHandler(&job_queue);
	    dax_handler->setId(0); //always at core 0
	}

	void addWork(int num_of_parts, int table_id){
	    for(int part_id = 0; part_id < num_of_parts; part_id++){
	    	Query item(part_id % 2, part_id, table_id); //(0: sw - 1:hw, part_id, table_id)
	    	Node<Query> *newNode = new Node<Query>(item);
	    	job_queue.add(newNode);
    	   	initialNodes.push_back(newNode);
	    }
	}	

	void startThreads(TCPStream* connection){
	    core_handler->setStream(connection);
	    core_handler->start(core_handler->getId());
	    core_handler->reserveNodes(50);
	    dax_handler->setStream(connection);
	    dax_handler->start(dax_handler->getId());
	    dax_handler->reserveNodes(50);
	}

	void joinThreads(){
	    core_handler->join();
	    dax_handler->join();
	}

	WorkQueue<Query> &getJobQueue(){
	    return job_queue;
	}

	void endResources(){
	    core_handler->setFlag();
	    dax_handler->setFlag();
	}

	void printResults(){
	    DataCompressor *lineitemComp = job_queue.getDataLoader()->getDataCompressor(0);
	    int count = 0;
	    for(int ind = 0; ind < lineitemComp->getTable()->num_of_segments; ind++){
	        count += __builtin_popcountl(lineitemComp->getBitVector()[ind]);
	    }
	    printf("Lineitem scan count: %d\n", count);
	}

	void clearResources(){
	    delete core_handler;
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
	if ( argc != 5 ) {
		printf("usage: %s <workers> <port> <ip> <fileName>\n", argv[0]);
		exit(-1);
	}

	int workers = atoi(argv[1]); //Number of cpu_cores
	int port = atoi(argv[2]);
	string ip = argv[3];
	string fileName = argv[4];

	int sf = 1;
	if(argc == 6)
	    sf = atoi(argv[5]);

	TCPAcceptor* connectionAcceptor;
	connectionAcceptor = new TCPAcceptor(port, (char*)ip.c_str());
	if (!connectionAcceptor || connectionAcceptor->start() != 0) {
		printf("Could not create a connection acceptor\n");
		exit(1);
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
	  
	    for(int cpu_id = 0; cpu_id < 32 * 8; cpu_id+=32){
		    pthread_barrier_init(&barrier, NULL, 2); 

		    /*Lineitem table*/
		    Partitioner line_part;
	            line_part.roundRobin(fileName); 
	 	    DataLoader data_loader(1);
		    data_loader.initializeCompressor(fileName, 0, line_part, sf);
	 	    data_loader.parseTable(0);
	   	    data_loader.compressTable(0);
		    WorkQueue<Query> scan_queue(0, &data_loader); 
		    ProcessingUnit proc_unit (workers, scan_queue);
		    proc_unit.createProcessingUnit(cpu_id);
		    proc_unit.addWork(line_part.getNumberOfParts(), 0);

		    proc_unit.startThreads(connection);
		    proc_unit.joinThreads();
		    //proc_unit.printResults();
		    proc_unit.clearResources();
	    	    pthread_barrier_destroy(&barrier);
	    	    pthread_barrier_destroy(&dax_barrier);
	    }
	    numberOfConnections--;
	    delete connection;
        }   

    delete connectionAcceptor;


    return 0;
}
