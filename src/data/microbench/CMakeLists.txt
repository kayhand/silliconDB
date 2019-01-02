cmake_minimum_required(VERSION 2.8)

project (SILLICONDB)
message(${SILLICONDB_SOURCE_DIR})
message(${SILLICONDB_BINARY_DIR})

set (SILLICONDB_SOURCE_DIR "/Users/kayhan/Repos/silliconDB")
message(${SILLICONDB_SOURCE_DIR})

set (CMAKE_CXX_FLAGS "-g -O3 -std=c++1y")
set (CMAKE_ARCHITECTURE_FLAGS "-m64")

INCLUDE_DIRECTORIES(/lib)
LINK_DIRECTORIES(/lib)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_ARCHITECTURE_FLAGS} -Wall -Wextra -Werror")

INCLUDE_DIRECTORIES(${SILLICONDB_SOURCE_DIR}/src)

add_executable(test MicroBench.cpp)
