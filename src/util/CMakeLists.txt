set(UTIL_SRC
    Partitioner.h
    Partitioner.cpp
    WorkItem.h
    WorkItem.cpp
    Query3.h
    Query3.cpp
)

link_directories(/opt/dax)
include_directories(/opt/dax)

add_library(util_src ${UTIL_SRC})
