cmake_minimum_required(VERSION 2.8)

project (SILLICONDB)
message(${SILLICONDB_SOURCE_DIR})
message(${SILLICONDB_BINARY_DIR})

set (CMAKE_CXX_FLAGS " -O3 -std=c++1y")
set (CMAKE_ARCHITECTURE_FLAGS "-m64")

INCLUDE_DIRECTORIES(/usr/lib/sparcv9)
LINK_DIRECTORIES(/usr/lib/sparcv9)

INCLUDE_DIRECTORIES(/lib)
LINK_DIRECTORIES(/lib)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_ARCHITECTURE_FLAGS} -lnsl -lsocket -ldax -Wall -Wextra -Werror")

INCLUDE_DIRECTORIES(${SILLICONDB_SOURCE_DIR}/src)

add_subdirectory(microbench)