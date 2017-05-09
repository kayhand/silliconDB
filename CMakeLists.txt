cmake_minimum_required(VERSION 2.8)

project(silliconDB)
#set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/modules)
#set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)

INCLUDE_DIRECTORIES(/opt/dax)
LINK_DIRECTORIES(/opt/dax)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAG} -fno-omit-frame-pointer -m64 -g -DDEBUG -std=c++1y -Wall -Wextra -Werror")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAG} -m64 -g -DDEBUG -std=c++1y -mcpu=ultrasparc3 -mvis2 -lnsl -lsocket -ldax -Wall -Wextra -Werror")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAG} -m64 -O3 -std=c++1y -lnsl -lsocket -ldax -Wall -Wextra -Werror")

add_subdirectory(src)
