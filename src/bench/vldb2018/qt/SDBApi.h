#ifndef __sdb_api_h__
#define __sdb_api_h__

#ifdef __sun
extern "C"{
    #include "/usr/include/dax.h"
}
#endif

#include "util/Query.h"
#include "data/DataLoader.h"
#include "log/Result.h"
#include "thread/Thread.h"

class SDBApi {
    private:
        DataCompressor *dataComp;
        table *compTable;
        column *base_col;

	int selCol; //defines which column to scan over
	int num_of_segments;

        int num_of_parts;
	int num_of_columns;
	uint64_t comp_predicate;

        dax_int_t predicate;
        dax_vec_t src;
	dax_vec_t dst;
	dax_compare_t cmp;
	uint64_t flag;

    public:
        SDBApi(DataCompressor *dataComp, int selCol){
	    this->dataComp = dataComp;
	    this->selCol = selCol;

	    this->compTable = dataComp->getTable();
            this->num_of_segments = dataComp->getPartitioner()->getSegsPerPart();
	}

        ~SDBApi(){
	}

	void initialize(){
	    num_of_parts = compTable->t_meta.num_of_parts;
	    num_of_columns = compTable->t_meta.num_of_columns;
	    base_col = &(compTable->columns[this->selCol]);

	    int t_id = compTable->t_meta.t_id;
	    if(t_id == 0){ //lo
	        comp_predicate = compTable->columns[this->selCol].i_keys[25]; //lo_quantity
	        cmp = dax_compare_t::DAX_LT;
	    }
	    else if(t_id == 1){ //date
	        comp_predicate = compTable->columns[this->selCol].i_keys[1993]; //d_year
	        cmp = dax_compare_t::DAX_EQ;
	    }
	    else if(t_id == 2){ //customer
	        comp_predicate = compTable->columns[this->selCol].keys["ASIA"]; //c_region
	        cmp = dax_compare_t::DAX_EQ;
	    }
	    initDaxVectors();
	    /*
	    if(t_id == 0){ //ls
	        comp_predicate = compTable->columns[this->selCol].keys["1996-01-10"];
	        cmp = dax_compare_t::DAX_GT;
	    }
	    else if(t_id == 1){ //os
	        comp_predicate = compTable->columns[this->selCol].keys["1996-01-10"];
	        cmp = dax_compare_t::DAX_LE;
	    }
	    */
	}

        void initDaxVectors(){
	    memset(&predicate, 0, sizeof(dax_int_t));
	    memset(&src, 0, sizeof(dax_vec_t));
	    memset(&dst, 0, sizeof(dax_vec_t));

            src.format = DAX_BITS;
	    src.elem_width = base_col->encoder.num_of_bits + 1;
            
	    printf("Dax bits: %d\n", src.elem_width);
            predicate.format = src.format;
	    predicate.elem_width = src.elem_width;
	    predicate.dword[2] = this->comp_predicate;

            dst.offset = 0;
	    dst.format = DAX_BITS;
	    dst.elem_width = 1;	

	    flag = DAX_CACHE_DST;
	}

	#ifdef __sun 
	void hwScan(dax_queue_t**, Node<Query>*);
	#endif
};

#endif
