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

	#ifdef __sun 
	static void linescan_hw(DataCompressor *dataComp, int curPart, dax_context_t **ctx); 
	static void orderscan_hw(DataCompressor *dataComp, int curPart, dax_context_t **ctx); 
	static void join_hw(DataCompressor *lineitemComp, DataCompressor *ordersComp, int curPart, dax_context_t **ctx);
	#endif

	static void linescan_sw(DataCompressor *dataComp, int curPart); 
	static void orderscan_sw(DataCompressor *dataComp, int curPart); 

	void join_sw();

	void agg(table *compTable, uint64_t *bit_vector, int curPart, int compLines, int dataEnd, int numOfBits);
	bool &getType(){
	    return this->type;
	}

    private:
        bool type;
};

#endif
