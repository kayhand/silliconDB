#ifndef __scan_api_h__
#define __scan_api_h__

#ifdef __sun
extern "C"{
    #include "/usr/include/dax.h"
}
#endif

#include "util/Query.h"
#include "data/DataLoader.h"
#include "log/Result.h"
#include "thread/Thread.h"

typedef int v2si __attribute__ ((vector_size (8))); //2 of 32 bit units -- in total 8 bytes
typedef short v4hi __attribute__ ((vector_size (8))); //4 of 16 bit units -- in total 8 bytes
typedef unsigned short v4qi __attribute__ ((vector_size (8))); //4 of 16 bit units -- in total 8 bytes
typedef unsigned char v8qi __attribute__ ((vector_size (8))); //8 of 8 bit units -- in total 8 bytes

class ScanApi {
    private:
        table *baseTable;
	int colId; //column id to scan

        int num_of_parts;
	int num_of_segments;
	int blockSize;

	uint64_t comp_predicate;
	dax_compare_t cmp;

	void *filter_vector;

        dax_int_t predicate;
        dax_vec_t src;
	dax_vec_t dst;
	uint64_t flag;

	atomic<int> partsDone;

	void reserveBitVector(int block_size){
            posix_memalign(&filter_vector, 4096, block_size);	
	}

	void* getFilterBitVector(int offset){
	    return static_cast<uint64_t*> (filter_vector) + offset;
	}

	friend class JoinApi;

    public:
        ScanApi(DataCompressor *dataComp, int selCol){
	    this->baseTable = dataComp->getTable();
	    this->colId = selCol;
	    this->num_of_segments = dataComp->getPartitioner()->getSegsPerPart();
	    this->blockSize = baseTable->t_meta.num_of_segments * 8;

	    initializeScan();
	}

        ~ScanApi(){
	    free(filter_vector);
	}

	int totalParts(){
	    return num_of_parts;
	}

	bool isCompleted(){
	    return partsDone == num_of_parts; 
	}

	void incrementCounter(){
	    partsDone++;
	}

	void initializeScan(){
            this->num_of_parts = baseTable->t_meta.num_of_parts;

	    int t_id = baseTable->t_meta.t_id;
            if(t_id == 0){ //lo
	        comp_predicate = baseTable->columns[colId].i_keys[25]; //lo_quantity
	        cmp = dax_compare_t::DAX_LE;
	    }
	    else if(t_id == 1){ //supplier
	        comp_predicate = baseTable->columns[colId].keys["ASIA"]; //s_region
	        cmp = dax_compare_t::DAX_EQ;
	    }
	    else if(t_id == 2){ //customer
	        comp_predicate = baseTable->columns[colId].keys["ASIA"]; //c_region
	        cmp = dax_compare_t::DAX_EQ;
	    }

	    reserveBitVector(blockSize);
	    initDaxVectors();
	}

        void initDaxVectors(){
	    memset(&predicate, 0, sizeof(dax_int_t));
	    memset(&src, 0, sizeof(dax_vec_t));
	    memset(&dst, 0, sizeof(dax_vec_t));

            src.format = DAX_BITS;
	    src.elem_width = baseTable->columns[colId].encoder.num_of_bits + 1;
            
	    printf("Dax bits: %d\n", src.elem_width);
            predicate.format = src.format;
	    predicate.elem_width = src.elem_width;
	    predicate.dword[2] = this->comp_predicate;

            dst.offset = 0;
	    dst.format = DAX_BITS;
	    dst.elem_width = 1;	

	    flag = DAX_CACHE_DST;
	}
	
	int bitVectorBlockSize(){
	    return this->blockSize;
	}

	table* getBaseTable(){
	    return this->baseTable;    
	}

	void* getBitResult(){
	    return getFilterBitVector(0);
	    //return this->filter_vector;
	}

	int getSegmentsPerPartition(){
	    return this->num_of_segments;
	}


	#ifdef __sun 
	void hwScan(dax_queue_t**, Node<Query>*);
	#endif
	void simdScan16(Node<Query>*, Result *result);

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

};

#endif
