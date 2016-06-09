#ifndef __tcpconnector_h__
#define __tcpconnector_h__

#include <netinet/in.h>
#include "TCPStream.h"
 
class TCPConnector
{
  public:
      TCPStream* connect(const char* server, int port);
           
  private:
      int resolveHostName(const char* host, struct in_addr* addr);
};

#endif
