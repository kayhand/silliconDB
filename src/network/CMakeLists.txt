add_subdirectory(server)
add_subdirectory(util)

add_executable(silliconDB Main.cpp)

target_link_libraries(silliconDB
    server_lib
    network_util
)
