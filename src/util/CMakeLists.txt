set(UTIL_SRC
    Partitioner.h
    Partitioner.cpp
    Query1.h
    Query1.cpp
)

link_directories(/opt/dax)
include_directories(/opt/dax)

add_library(util_src ${UTIL_SRC})
