link_directories(/opt/dax)
include_directories(/opt/dax)

add_executable(q3_onequeue Master.cpp)
target_link_libraries(
    q3_onequeue
    data_lib
    network_util
    server_lib
    pthread
    util_src
)
