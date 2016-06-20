cmake_minimum_required(VERSION 2.8)
project(parser)

include_directories(headers)

set( CMAKE_CXX_FLAGS "-std=c++11")

file(GLOB SOURCES "src/*.cpp")


add_executable(parse ${SOURCES})
