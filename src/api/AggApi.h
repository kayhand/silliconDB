#ifndef __agg_api_h__
#define __agg_api_h__

#ifdef __sun
extern "C"{
    #include "/usr/include/dax.h"
}
#endif

#include "util/Query.h"
#include "data/DataLoader.h"
#include "log/Result.h"
#include "thread/Thread.h"

#include "api/JoinApi.h"

class AggApi {
    private:
        JoinApi *j1;
        JoinApi *j2;

        //vector<unordered_map <uint32_t, uint32_t>> hashTables;

    public:
        AggApi(JoinApi *j1, JoinApi *j2){
	    this->j1 = j1;
	    this->j2 = j2;
	    
	    initializeAgg();
	}

        ~AggApi(){
	}

	void initializeAgg(){
	    //hashTables.resize(j1->numOfParts());
	    //hashTables.reserve(j1->numOfParts());
	}

	void printBitVector(uint64_t cur_result, int segId){
	    //print bit vals
	    printf("%d: ", segId);
	    for(int j = 0; j < 64; j++){
	        printf("%lu|", (cur_result & 1));
	        cur_result >>= 1;
	    }
	    printf("\n");
	}


        void agg(Node<Query>* node, Result *result);
};

#endif
