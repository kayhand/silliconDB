#add_subdirectory(Q1)
add_subdirectory(Q3)

add_executable(sDB_client Client.cpp)
target_link_libraries(sDB_client
    server_lib
    network_util
)
