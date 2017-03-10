link_directories(/opt/dax)
include_directories(/opt/dax)

add_executable(sDB_master Master.cpp)
target_link_libraries(
    sDB_master
    data_lib
    network_util
    server_lib
    pthread
    util_src
)

add_executable(two_queues 2Q_Master.cpp)
target_link_libraries(
    two_queues
    data_lib
    network_util
    server_lib
    pthread
    util_src
)

add_executable(dax_queue DaxQueue.cpp)
target_link_libraries(
    dax_queue
    data_lib
    network_util
    server_lib
    pthread
    util_src
)


#add_executable(numa_exp NumaExp.cpp)
#target_link_libraries(
#    numa_exp
#    data_lib
#    network_util
#    server_lib
#    util_src
#    -lumem
#)

add_executable(sDB_client Client.cpp)
target_link_libraries(sDB_client
    server_lib
    network_util
)
