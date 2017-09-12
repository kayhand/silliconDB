#ifndef __query3_h__
#define __query3_h__

#ifdef __sun
extern "C"{
    #include "/usr/include/dax.h"
}
#endif
#include "../data/DataLoader.h"
#include "../log/Result.h"
#include "Helper.h"

typedef int v2si __attribute__ ((vector_size (8))); //2 of 32 bits units -- in total 8 bytes
typedef unsigned short v4qi __attribute__ ((vector_size (8))); //4 of 16 bits units -- in total 8 bytes
typedef unsigned char v8qi __attribute__ ((vector_size (8))); //8 of 8 bits units -- in total 8 bytes

class Query3 {
    public:
        Query3();
        ~Query3();

	#ifdef __sun 
	static void linescan_hw(DataCompressor *dataComp, int curPart, Result *result, dax_context_t **ctx, dax_queue_t **queue, bool async, void *udata);
	static void orderscan_hw(DataCompressor *dataComp, int curPart, Result *result, dax_context_t **ctx, dax_queue_t **queue, bool async, void *udata);
	static void join_hw(DataCompressor *lineitemComp, DataCompressor *ordersComp, int curPart, Result *result, dax_context_t **ctx, dax_queue_t **queue, bool async, void *udata);
	#endif

	static void linescan_sw(DataCompressor *dataComp, int curPart, Result *result); 
	static void orderscan_sw(DataCompressor *dataComp, int curPart, Result *result); 
	static void join_sw(DataCompressor *lineitemComp, DataCompressor *ordersComp, int curPart, Result *result);

	static void linescan_simd(DataCompressor *dataComp, int scaledPart, Result *result);
	static void orderscan_simd(DataCompressor *dataComp, int scaledPart, Result *result);

	template <class T_okey, class T_extp, class T_disc>
	static void agg(DataCompressor *dataComp, int curPart, Result *result);

	static void count(DataCompressor *dataComp, int curPart, Result *result);
};

#endif
