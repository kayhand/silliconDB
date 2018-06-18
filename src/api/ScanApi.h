#ifndef __scan_api_h__
#define __scan_api_h__

#ifdef __sun
extern "C" {
#include "/usr/include/dax.h"
}
#else
extern "C" {
#include "/usr/local/dax.h"
}
#endif

#include <util/Types.h>
#include "util/Query.h"
#include "data/DataLoader.h"
#include "log/Result.h"
#include "thread/Thread.h"

class ScanApi {
private:
	table *baseTable;
	int colId; //column id to scan

	int num_of_parts;
	int num_of_segments; //actually segments per partition
	int blockSize;

	uint64_t pred1;
	uint64_t pred2;
	dax_compare_t cmp;

	void *filter_vector;

	dax_int_t predicate1;
	dax_int_t predicate2;

	dax_vec_t src;
	dax_vec_t dst;
	uint64_t flag;

	atomic<int> partsDone;

	void reserveBitVector(int block_size) {
		posix_memalign(&filter_vector, 4096, block_size);
	}

	void* getFilterBitVector(int offset) {
		return static_cast<uint64_t*>(filter_vector) + offset;
	}

	friend class JoinApi;

public:
	ScanApi(table *baseTable, int selCol) {
		this->baseTable = baseTable;
		this->colId = selCol;
		this->num_of_segments = baseTable->t_meta.num_of_segments / baseTable->t_meta.num_of_parts;
		this->blockSize = baseTable->t_meta.num_of_segments * 8;

		initializeScan();
	}

	~ScanApi() {
		free(filter_vector);
	}

	int totalParts() {
		return num_of_parts;
	}

	bool isCompleted() {
		return partsDone == num_of_parts;
	}

	void incrementCounter() {
		partsDone++;
	}

	void initializeScan() {
		this->num_of_parts = baseTable->t_meta.num_of_parts;

		int t_id = baseTable->t_meta.t_id;
		if (t_id == 0) { //lo
			pred1 = baseTable->columns[colId].i_keys[25]; //lo_quantity
			cmp = dax_compare_t::DAX_LE;
		} else if (t_id == 1) { //supplier
			pred1 = baseTable->columns[colId].keys["ASIA"]; //s_region
			cmp = dax_compare_t::DAX_EQ;
		} else if (t_id == 2) { //customer
			pred1 = baseTable->columns[colId].keys["ASIA"]; //c_region
			cmp = dax_compare_t::DAX_EQ;
		} else if (t_id == 3) { //date
			pred1 = baseTable->columns[colId].i_keys[1991]; //d_year
			pred2 = baseTable->columns[colId].i_keys[1997]; //d_year
			cmp = dax_compare_t::DAX_GT_AND_LE;
		}

		reserveBitVector(blockSize);
		initDaxVectors();
	}

	void initDaxVectors() {
		memset(&predicate1, 0, sizeof(dax_int_t));
		memset(&predicate2, 0, sizeof(dax_int_t));

		memset(&src, 0, sizeof(dax_vec_t));
		memset(&dst, 0, sizeof(dax_vec_t));

		src.format = DAX_BITS;
		src.elem_width = baseTable->columns[colId].encoder.num_of_bits + 1;

		printf("Dax bits: %d\n", src.elem_width);

		predicate1.format = src.format;
		predicate1.elem_width = src.elem_width;
		predicate1.dword[2] = this->pred1;

		predicate2.format = src.format;
		predicate2.elem_width = src.elem_width;
		predicate2.dword[2] = this->pred2;

		dst.offset = 0;
		dst.format = DAX_BITS;
		dst.elem_width = 1;

		flag = DAX_CACHE_DST;
	}

	int bitVectorBlockSize() {
		return this->blockSize;
	}

	table* getBaseTable() {
		return this->baseTable;
	}

	void* getBitResult() {
		return getFilterBitVector(0);
		//return this->filter_vector;
	}

	int getSegmentsPerPartition() {
		return this->num_of_segments;
	}

#ifdef __sun
	void hwScan(dax_queue_t**, Node<Query>*);
#endif
	void simdScan16(Node<Query>*, Result *result);

	//helper
	void printBitVector(uint64_t *bit_vector, int segs, uint64_t clear_vector) {
		//print bit vals
		uint64_t cur_result = 0ul;
		printf("\n");
		for (int i = 0; i < segs; i++) {
			cur_result = bit_vector[i];
			printf("%d: ", i + 1);
			for (int j = 0; j < 64; j++) {
				printf("%lu|", (cur_result & 1));
				cur_result >>= 1;
			}
			printf("\n");
		}
		printf("\n");

		printf("c: ");
		for (int j = 0; j < 64; j++) {
			printf("%lu|", (clear_vector & 1));
			clear_vector >>= 1;
		}
		printf("\n\n");

	}

};

#endif
