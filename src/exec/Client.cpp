#include <stdio.h>
#include <stdlib.h>
#include <string>

#include "network/util/TCPConnector.h"

using namespace std;

int main(int argc, char** argv)
{
    if (argc != 3) {
        printf("usage: %s <port> <ip>\n", argv[0]);
        exit(1);
    }

    int len;
    string query;
    char line[256];
    TCPConnector* connector = new TCPConnector();
    TCPStream* stream = connector->connect(argv[2], atoi(argv[1]));
    if (stream) {
        query = "Q3_1";
        stream->send(query.c_str(), query.size());
        printf("sent - %s\n", query.c_str());
        len = stream->receive(line, sizeof(line));
	if(len < 0)
            line[0] = 0;
        printf("received - %s\n", line);
        delete stream;
    }

   exit(0);
}
