#ifndef __query3_h__
#define __query3_h__

#ifdef __sun
extern "C"{
    #include "/opt/dax/dax.h"
}
#endif
#include "../data/DataLoader.h"
#include "Helper.h"

class Query3 {
    public:
        Query3();
        Query3(bool type); //0 = sw; 1 = dax
        ~Query3();

	void linescan_hw(DataCompressor *dataComp, int curPart, dax_context_t **ctx); 
	static void linescan_sw(DataCompressor *dataComp, int curPart); 
	void orderscan_hw(DataCompressor *dataComp, int curPart, dax_context_t **ctx); 
	void orderscan_sw(); 

	void join_sw();
	void join_hw(dax_context_t** ctx, dax_vec_t* src, dax_vec_t* bit_map, dax_vec_t* l_scan_res);

	void agg(table *compTable, uint64_t *bit_vector, int curPart, int compLines, int dataEnd, int numOfBits);
	#ifdef __sun
		dax_result_t scan_res;
	#endif
	bool &getType(){
	    return this->type;
	}
    private:
        bool type;
};

#endif
