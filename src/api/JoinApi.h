#ifndef __join_api_h__
#define __join_api_h__

#ifdef __sun
extern "C"{
    #include "/usr/include/dax.h"
}
#endif

#include "util/Query.h"
#include "data/DataLoader.h"
#include "log/Result.h"
#include "thread/Thread.h"

#include "api/ScanApi.h"

typedef int v2si __attribute__ ((vector_size (8))); //2 of 32 bit units -- in total 8 bytes
typedef short v4hi __attribute__ ((vector_size (8))); //4 of 16 bit units -- in total 8 bytes
typedef unsigned short v4qi __attribute__ ((vector_size (8))); //4 of 16 bit units -- in total 8 bytes
typedef unsigned char v8qi __attribute__ ((vector_size (8))); //8 of 8 bit units -- in total 8 bytes

class JoinApi {
    private:
        ScanApi *factScan; //left relation
        ScanApi *dimensionScan; //right relation

	column *factCols;
	int joinColId; //id of the join column

        uint64_t *bit_map_res;

	void *join_vector; //keep the join result -- defines which items of the fact table satisfies
	int block_size;

        int num_of_columns;
	int segs_per_part;

        std::vector<bool> exec_flag;
	std::vector<int> part_els;

        dax_vec_t src;
        dax_vec_t bit_map;
	dax_vec_t dst;
	uint64_t flag;

	void reserveBitVector(int block_size){
	    this->block_size = block_size;
            posix_memalign(&join_vector, 4096, block_size);	
	}

        friend class AggApi;

    public:
        JoinApi(ScanApi *factScan, ScanApi *dimScan, int joinColId){
	    this->factScan = factScan;
	    this->dimensionScan = dimScan;

	    this->joinColId = joinColId;
	    this->factCols = factScan->getBaseTable()->columns;

            this->num_of_columns = factScan->getBaseTable()->t_meta.num_of_columns;
	    this->segs_per_part = factScan->getSegmentsPerPartition();

            this->exec_flag.assign(this->factScan->num_of_parts, false);
	    this->part_els.assign(this->factScan->num_of_parts, 0);

	    initializeJoin();
	}

        ~JoinApi(){
	    free(join_vector);
	}

	void initializeJoin(){
	    reserveBitVector(factScan->bitVectorBlockSize());
	    bit_map_res = (uint64_t*) dimensionScan->getBitResult();
	    initDaxVectors();
	}

        void initDaxVectors(){
	    memset(&src, 0, sizeof(dax_vec_t));
	    memset(&bit_map, 0, sizeof(dax_vec_t));
	    memset(&dst, 0, sizeof(dax_vec_t));

            src.format = DAX_BITS;
	    src.elem_width = (&factCols[joinColId])->encoder.num_of_bits + 1;

            bit_map.format = DAX_BITS;
	    bit_map.elem_width = 1;
	    bit_map.data = dimensionScan->getBitResult();

            dst.offset = 0;
	    dst.format = DAX_BITS;
	    dst.elem_width = 1;	

	    flag = DAX_CACHE_DST;
	}

	int numOfParts(){
	    return dimensionScan->num_of_parts;
	}

	bool isPartDone(int p_id){
	    return exec_flag[p_id];
	}

	void setExecFlag(int p_id){
	    exec_flag[p_id] = true;
	}

	void* getJoinBitVector(int offset){
	    return static_cast<uint64_t*> (join_vector) + offset * this->segs_per_part;
	}

	#ifdef __sun 
	void hwJoin(dax_queue_t**, Node<Query>*);
	#endif
        void swJoin(Node<Query>* node, Result *result);

	void printBitVector(uint64_t cur_result, int segId){
	    //print bit vals
	    printf("%d: ", segId);
	    for(int j = 0; j < 64; j++){
	        printf("%lu|", (cur_result & 1));
	        cur_result >>= 1;
	    }
	    printf("\n");
	}

	//helper
	void printBitVector(uint64_t *bit_vector, int segs, uint64_t clear_vector){
	    //print bit vals
	    uint64_t cur_result = 0ul;
	    printf("\n");
	    for(int i = 0; i < segs; i++){
		cur_result = bit_vector[i];
		printf("%d: ", i + 1);
		for(int j = 0; j < 64; j++){
		    printf("%lu|", (cur_result & 1));
		    cur_result >>= 1;
		}
		printf("\n");
	    }
	    printf("\n");

	    printf("c: ");
	    for(int j = 0; j < 64; j++){
	        printf("%lu|", (clear_vector & 1));
		clear_vector >>= 1;
	    }
	    printf("\n\n");

	}

	bool bitAtPosition(uint64_t* bit_map, int bit_pos){
	    //printf("bit_pos: %d\n", bit_pos);
    	    int arr_ind = bit_pos / 64;
    	    int local_pos = 63 - (bit_pos % 64); 

    	    uint64_t localVector = bit_map[arr_ind];
    	    return (localVector >> local_pos) & 1;
        }


};

#endif
