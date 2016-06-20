add_executable(sDB_master Master.cpp)

target_link_libraries(sDB_master
    server_lib
    network_util
    thread_lib
)

add_executable(sDB_client Client.cpp)

target_link_libraries(sDB_client
    server_lib
    network_util
    thread_lib
)
