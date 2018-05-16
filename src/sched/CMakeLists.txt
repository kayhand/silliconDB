set(SCHED_SRC
    ProcessingUnit.h
    ProcessingUnit.cpp
)

add_library(sched_lib ${SCHED_SRC})
target_link_libraries(sched_lib tpch_lib)
target_link_libraries(sched_lib sdbapi_lib)
target_link_libraries(sched_lib api_lib)


