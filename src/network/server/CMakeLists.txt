set(SERVER_SRC
    TCPAcceptor.h
    TCPAcceptor.cpp
)

add_library(server_lib ${SERVER_SRC})
#target_include_directories(server_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(server_lib network_util)
