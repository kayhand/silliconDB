#ifndef __types_h__
#define __types_h__
#define NANOSEC 1000000000


#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>

enum JOB_TYPE{
	LO_SCAN = -1,
	S_SCAN,
	C_SCAN,
	D_SCAN,
	P_SCAN,
	LS_JOIN = 11,
	LC_JOIN,
	LD_JOIN,
	AGG = 21
};

typedef int v2si __attribute__ ((vector_size (8))); //2 of 32 bit units -- in total 8 bytes
typedef short v4hi __attribute__ ((vector_size (8))); //4 of 16 bit units -- in total 8 bytes
typedef unsigned short v4qi __attribute__ ((vector_size (8))); //4 of 16 bit units -- in total 8 bytes
typedef unsigned char v8qi __attribute__ ((vector_size (8))); //8 of 8 bit units -- in total 8 bytes


#ifndef __sun
typedef unsigned long hrtime_t;
unsigned long gethrtime(void) {
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0) {
		return (-1);
	}

	return ((ts.tv_sec * NANOSEC) + ts.tv_nsec);
}
#endif

#endif


