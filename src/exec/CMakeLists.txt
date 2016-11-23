add_executable(sDB_master Master.cpp)

link_directories(/opt/dax)
include_directories(/opt/dax)

target_link_libraries(
    sDB_master
    -ldax
    data_lib
    network_util
    server_lib
    #    thread_lib
    util_src
)

add_executable(sDB_client Client.cpp)

target_link_libraries(sDB_client
    server_lib
    network_util
    #thread_lib
)
