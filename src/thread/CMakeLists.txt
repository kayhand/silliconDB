set(THREAD_SRC
    Thread.h
    Thread.cpp
)

add_library(thread_lib ${THREAD_SRC})

find_package(Threads)

target_link_libraries(thread_lib ${CMAKE_THREAD_LIBS_INIT})
