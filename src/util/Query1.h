#ifndef __query1_h__
#define __query1_h__

#ifdef __sun
extern "C"{
    #include "/opt/dax/dax.h"
}
#endif
#include "../data/DataLoader.h"
#include "Helper.h"

class Query1 {
    public:
        Query1();
        Query1(bool type); //0 = sw; 1 = dax
        ~Query1();
	void scan_hw(DataCompressor *dataComp, int curPart); 
	void scan_sw(DataCompressor *dataComp, int curPart); 
	void agg(DataCompressor *dataComp, int curPart, int dataEnd, int numOfBits);

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