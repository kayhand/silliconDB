cmake_minimum_required(VERSION 2.8)

project(silliconDB)
#set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/modules)
#set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAG} -g -DLINUX -DDEBUG -std=c++11 -Wall -Wextra -Werror")

add_subdirectory(src)
