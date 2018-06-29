#ifndef __sdb_and_api_h__
#define __sdb_and_api_h__

#ifdef __sun
extern "C"{
    #include "/usr/include/dax.h"
}
#endif

#include "util/Query.h"
#include "data/DataLoader.h"
#include "log/Result.h"
#include "thread/Thread.h"

class SDBAndApi {
    private:
        DataCompressor *leftComp;
        table *leftTable; //lineitem
        int base_bits;

        DataCompressor *rightComp;
        table *rightTable; //orders

	int num_of_segments;
        int num_of_parts; //lineitem parts
	int num_of_columns; 

	int baseColId;

        dax_vec_t src;
        dax_vec_t src2;
	dax_vec_t dst;
	dax_vec_t bit_map;

	uint64_t flag;

    public:
        //scan
        SDBAndApi(DataCompressor *leftComp, int selCol){
	    this->leftComp = leftComp;
	    this->leftTable = leftComp->getTable();
	    this->base_bits = (&(leftTable->columns[selCol]))->encoder.num_of_bits;

            this->num_of_segments = leftComp->getPartitioner()->getSegsPerPart();
	    this->num_of_parts = leftTable->t_meta.num_of_parts;
	    this->num_of_columns = leftTable->t_meta.num_of_columns;

	    this->baseColId = selCol;
	}
        //join
        SDBAndApi(DataCompressor *leftComp, DataCompressor *rightComp, int joinCol){
	    this->leftComp = leftComp;
	    this->rightComp = rightComp;
	    this->baseColId = joinCol;

	    this->leftTable = leftComp->getTable();
	    this->rightTable = rightComp->getTable();

	    this->base_bits = (&(leftTable->columns[joinCol]))->encoder.num_of_bits;

            this->num_of_segments = leftComp->getPartitioner()->getSegsPerPart();
	    this->num_of_parts = leftTable->t_meta.num_of_parts;
	    this->num_of_columns = leftTable->t_meta.num_of_columns;
	}

        ~SDBAndApi(){}

        void initDaxVectors(){
	    memset(&src, 0, sizeof(dax_vec_t));
	    memset(&src2, 0, sizeof(dax_vec_t));
	    memset(&dst, 0, sizeof(dax_vec_t));

            src.offset = 0;
            src.format = DAX_BITS;
	    src.elem_width = 1;
            
            src2.offset = 0;
            src2.format = DAX_BITS;
	    src2.elem_width = 1;
            
            dst.offset = 0;
	    dst.format = DAX_BITS;
	    dst.elem_width = 1;	

	    bit_map.format = DAX_BITS;
	    bit_map.elem_width = 1;
	    bit_map.data = this->rightComp->getFilterBitVector(0);

	    flag = DAX_CACHE_DST;
	}

	#ifdef __sun 
	void hwAnd(dax_queue_t**, Node<Query>*);
	#endif
};

#endif