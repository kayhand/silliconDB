cmake_minimum_required(VERSION 2.8)

project (SILLICONDB)
message(${SILLICONDB_SOURCE_DIR})
message(${SILLICONDB_BINARY_DIR})

set (CMAKE_CXX_FLAGS "-g -O3 -std=c++1y")
set (CMAKE_ARCHITECTURE_FLAGS "-m64 -mcpu=ultrasparc3 -mvis2")

INCLUDE_DIRECTORIES(/usr/lib/sparcv9)
LINK_DIRECTORIES(/usr/lib/sparcv9)

INCLUDE_DIRECTORIES(/lib)
LINK_DIRECTORIES(/lib)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAG} -fno-omit-frame-pointer -m64 -g -DDEBUG -std=c++1y -Wall -Wextra -Werror")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAG} -m64 -O3 -std=c++1y -lnsl -lsocket -ldax -Wall -Wextra -Werror")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_ARCHITECTURE_FLAGS} -lnsl -lsocket -ldax -Wall -Wextra -Werror")

INCLUDE_DIRECTORIES(${SILLICONDB_SOURCE_DIR}/src)

add_subdirectory(src)
