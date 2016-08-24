set(NETWORK_UTIL_SRC
    TCPConnector.h
    TCPConnector.cpp
    TCPStream.h
    TCPStream.cpp
)

add_library(network_util ${NETWORK_UTIL_SRC})
