set(Api_SRC
    ScanApi.h
    ScanApi.cpp
    JoinApi.h
    JoinApi.cpp
    AggApi.h
    AggApi.cpp       
)

add_library(api_lib ${Api_SRC})
