#ifndef __query3_h__
#define __query3_h__

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

class Query3 {
    public:
        Query3();
        ~Query3();

	#ifdef __sun 
	static void hwScan(DataCompressor *dataComp, int scaledPart, int selCol, dax_compare_t cmp, Result *result, dax_context_t **ctx, dax_queue_t **queue, bool async, void *udata);
	static void join_hw(DataCompressor *lineitemComp, DataCompressor *ordersComp, int curPart, Result *result, dax_context_t **ctx, dax_queue_t **queue, bool async, void *udata);
	#endif

	static void swScan(DataCompressor *dataComp, int scaledPart, int selCol, Result *result, bool gt);
	static void join_sw(DataCompressor *lineitemComp, DataCompressor *ordersComp, int curPart, Result *result);

	static void simdScan_16(DataCompressor *dataComp, int scaledPart, int selCol, Result *result, bool gt); 
	static void simdScan_24(DataCompressor *dataComp, int scaledPart, int selCol, Result *result, bool gt); 

	static void agg(DataCompressor *dataComp, int curPart, Result *result);

	static void count(DataCompressor *dataComp, int curPart, Result *result);
};

#endif
