add_executable(sDB_master Master.cpp)
target_link_libraries(
    sDB_master
    data_lib
    network_util
    server_lib
    pthread
#    util_src
    sched_lib
)

add_executable(sDB_client Client.cpp)
target_link_libraries(sDB_client
    server_lib
    network_util
)
