#include <arpa/inet.h>
#include "TCPStream.h"

TCPStream::TCPStream(int sd, struct sockaddr_in* address) : m_sd(sd) 
{
    char ip[25];
    inet_ntop(PF_INET, (struct in_addr*)&(address->sin_addr.s_addr), ip, sizeof(ip) - 1);

    m_peerIP = ip;
    m_peerPort = ntohs(address->sin_port);
}

TCPStream::~TCPStream()
{
    close(m_sd);
}

ssize_t TCPStream::send(const char* buffer, size_t len)
{
    return write(m_sd, buffer, len);
}

ssize_t TCPStream::receive(char* buffer, size_t len)
{
    return read(m_sd, buffer, len);
}

string TCPStream::getPeerIP()
{
    return m_peerIP;
}

int TCPStream::getPeerPort()
{
    return m_peerPort;
}
