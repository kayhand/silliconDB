#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "../thread/Thread.h"
#include "util/WorkQueue.h"
#include "../network/server/TCPAcceptor.h"
#include "../data/DataLoader.h"
#include "../util/Partitioner.h"
#include "../util/WorkItem.h"

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

    void *run(){

	DataCompressor* dataComp = scan_queue.getCompressor();
        for(int i = 0; ; i++)  {
            //printf("thread %lu, loop %d - waiting for item in queue %d...\n", (long unsigned int) self(), i, scan_queue.getId());
            //Change wait condition, maybe master can broadcast
            //printf("Queue size : %d\n", scan_queue.size());
            while(scan_queue.size() > 0){
                //Call bitweving tpc-h q1 implementation
                //item->function();
                WorkItem item = scan_queue.remove();
                //printf("thread %lu, loop %d - got one item...\n\n", (long unsigned int) self(), i);
                item.scan(dataComp, scan_queue.getId());
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
        printf("Could not create an connection acceptor\n");
        exit(1);
    }

 
    std::vector<WorkQueue<WorkItem>> scan_queues;
    scan_queues.reserve(50);
    std::vector<ConnectionHandler*> conn_handlers;
    conn_handlers.reserve(50);

    //Create the thread pool
    for(int i = 0; i < workers; i++){
        WorkQueue<WorkItem> scan_queue(1, i, part.getMap(), data_loader.compressedTable); 
        scan_queues.push_back(scan_queue);
        ConnectionHandler *handler = new ConnectionHandler(scan_queues.back());
        conn_handlers.push_back(handler);
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

        printf("Now pushing works into the queues ...\n");
        WorkItem item;
        for(WorkQueue<WorkItem> &queue : scan_queues){
            printf("Pushing into queue %d ...\n", queue.getId());
            queue.add(item);
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
