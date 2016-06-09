#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include "util/TCPConnector.h"
#include "server/TCPAcceptor.h"

using namespace std;

void server(int argc, char** argv){
    TCPStream* stream = NULL;
    TCPAcceptor* acceptor = NULL;
    if(argc == 4){
        acceptor = new TCPAcceptor(atoi(argv[2]), argv[3]);
    }
    else{
        acceptor = new TCPAcceptor(atoi(argv[2]));
    }
    if(acceptor->start() == 0){
        while(1){
            stream = acceptor->accept();
            if(stream != NULL){
                size_t len;
                char line[256];
                while((len = stream->receive(line, sizeof(line))) > 0){
                    line[len] = 0;
                    printf("receive - %s\n", line);
                    stream->send(line, len);
                }
                delete stream;
            }
        }
    }
    perror("Could not start the server");
    exit(-1);
}

void client(char** argv){
    int len;
    string message;
    char line[256];
    TCPConnector* connector = new TCPConnector();
    TCPStream* stream = connector->connect(argv[3], atoi(argv[2]));

    if(stream){
        message = "Yo what's up !?";
        stream->send(message.c_str(), message.size());
        printf("sent - %s\n", message.c_str());
        len = stream->receive(line, sizeof(line));
        line[len] = 0;
        printf("received - %s\n", line);
        delete stream;
    }
    exit(0);

}

int main(int argc, char** argv)
{
    if(strcmp(argv[1], "server") == 0){
        if (argc < 3) {
            printf("usage: server <port> [<ip>]\n");
            exit(1);
        }
        else{
            printf("Starting server...\n");
            server(argc, argv);
        }
    }
    else if(strcmp(argv[1], "client") == 0){
        if (argc != 4) {
            printf("usage: %s client <port> <ip>\n", argv[0]);
            exit(1);
        }
        else{
            printf("Starting client...\n");
            client(argv);
        }
    }
    else{
        printf("Usage: <server|client> ...\n");
    }
}


