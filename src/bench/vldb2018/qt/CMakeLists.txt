set(SDBApi_SRC
    SDBApi.h
    SDBApi.cpp
    SDBJoinApi.h
    SDBJoinApi.cpp
    SDBAndApi.h
    SDBAndApi.cpp
)

add_library(sdbapi_lib ${SDBApi_SRC})
