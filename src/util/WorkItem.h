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
        ~WorkItem();
	void scan(DataCompressor *dataComp, int curPart); 
	void agg(table &compTable, uint64_t *bit_vector, int curPart, int compLines, int dataStart);
	#ifdef __sun
		dax_result_t scan_res;
	#endif
};

#endif
