#ifndef __query1_h__
#define __query1_h__

#ifdef __sun
extern "C"{
    #include "/usr/include/dax.h"
}
#endif
#include "../data/DataLoader.h"
#include "../log/Result.h"
#include "Helper.h"

class Query1 {
    public:
        Query1();
        ~Query1();

	#ifdef __sun 
	static void linescan_hw(DataCompressor *dataComp, int curPart, Result *result, dax_context_t **ctx); 
	static void linescan_post(DataCompressor *dataComp, dax_queue_t **queue, Query *item); 
	static void linescan_hw(DataCompressor *dataComp, int curPart, dax_context_t **ctx, pthread_barrier_t *barrier, pthread_barrier_t *dax_barrier); 
	#endif
	static void linescan_sw(DataCompressor *dataComp, int curPart, Result *result); 
	static void agg(DataCompressor *dataComp, int curPart, Result *result, bool isDax);
	static void count(DataCompressor *dataComp, int curPart, Result *result);
};

#endif
