#ifndef __workitem_h__
#define __workitem_h__

#ifdef __sun
extern "C"{
    #include "/opt/dax/dax.h"
}
#endif
#include "../data/DataLoader.h"
#include "Helper.h"

class WorkItem {
    public:
        WorkItem();
        WorkItem(bool type); //0 = sw; 1 = dax
        ~WorkItem();
	void scan_hw(DataCompressor *dataComp, int curPart); 
	void scan_sw(DataCompressor *dataComp, int curPart); 
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
