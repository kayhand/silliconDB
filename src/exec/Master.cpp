#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "../thread/Thread.h"
#include "util/WorkQueue.h"
#include "../network/server/TCPAcceptor.h"
#include "../data/DataLoader.h"
#include "../util/Partitioner.h"

class WorkItem
{
    int t_start, t_end;
    public:
        WorkItem(){}
        ~WorkItem() {}

    //Define a scan function here ?
    void printRange(int start, int end) { 
	printf("Scanning from %d to %d \n", start, end);
    } 
};

class ConnectionHandler : public Thread
{
    //CurJobType : scan or aggregation 
    
    //May have two queues defined
    //Scan jobs queue
    //Agg jobs queue
    WorkQueue<WorkItem> &scan_queue;
    TCPStream* t_stream;

    public:
        ConnectionHandler(WorkQueue<WorkItem>& queue) : scan_queue(queue){}

    void *run() {
        for(int i = 0; ; i++)  {
            printf("thread %lu, loop %d - waiting for item in queue %d...\n", (long unsigned int) self(), i, scan_queue.getId());
            //Change wait condition, maybe master can broadcast
            printf("Queue size : %d\n", scan_queue.size());
            while(scan_queue.size() > 0){
                //Call bitweving tpc-h q1 implementation
                //item->function();
                WorkItem item = scan_queue.remove();
                printf("thread %lu, loop %d - got one item...\n\n", (long unsigned int) self(), i);
		int start = scan_queue.getMap()[scan_queue.getId()].first;
		int end = scan_queue.getMap()[scan_queue.getId()].second;
                item.printRange(start, end);
            }
            t_stream->send("", 0);
            return NULL;
        }
        return(0);
    }

    void setStream(TCPStream* stream){
        this->t_stream = stream;
    }
};

int main(int argc, char** argv)
{
    if ( argc != 5 ) {
        printf("usage: %s <workers> <port> <ip> <fileName>\n", argv[0]);
        exit(-1);
    }

    int workers = atoi(argv[1]);
    int port = atoi(argv[2]);
    string ip = argv[3];
    string fileName = argv[4];
    DataLoader data_loader(fileName);
    data_loader.compressTable();
    int numOfElements = data_loader.compressedTable->getTable()->nb_lines;
    
    TCPAcceptor* connectionAcceptor;
    connectionAcceptor = new TCPAcceptor(port, (char*)ip.c_str());
    if (!connectionAcceptor || connectionAcceptor->start() != 0) {
        printf("Could not create an connection acceptor\n");
        exit(1);
    }

    Partitioner part;
    part.roundRobin(numOfElements, workers);
  
    std::vector<WorkQueue<WorkItem>> scan_queues;
    scan_queues.reserve(50);
    std::vector<ConnectionHandler*> conn_handlers;
    conn_handlers.reserve(50);

    //Create the thread pool
    for(int i = 0; i < workers; i++){
        WorkQueue<WorkItem> scan_queue(2, i, part.getMap()); 
        scan_queues.push_back(scan_queue);
        ConnectionHandler *handler = new ConnectionHandler(scan_queues.back());
        conn_handlers.push_back(handler);
    }

    int numberOfConnections = 1;
    TCPStream* connection;
    while(numberOfConnections > 0){
        connection = connectionAcceptor->accept();
        if(!connection) {
            printf("Could not accept a connection!\n");
            continue;
        }

        printf("Now pushing works into the queues ...\n");
        //Client requested connection
        WorkItem item;
        WorkItem item2;
        for(WorkQueue<WorkItem> &queue : scan_queues){
            printf("Pushing into queue %d ...\n", queue.getId());
            queue.add(item);
            queue.add(item2);
        }
        printf("Added two items to the queue\n\n");

        int ind = 0;
        for(ConnectionHandler* curConn : conn_handlers){
            curConn->setStream(connection);
            curConn->start(ind);
            ind++;
        }
        for(ConnectionHandler* curConn : conn_handlers){
            curConn->join();
        }
        printf("# of connections : %d\n\n", numberOfConnections);
        numberOfConnections--;
        delete connection;
    }   

    delete connectionAcceptor;
    for(ConnectionHandler* curHandler : conn_handlers){
        delete curHandler; 
    }
    return 0;
}
