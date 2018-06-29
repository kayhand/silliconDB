#ifndef __join_api_h__
#define __join_api_h__

#ifdef __sun
extern "C" {
#include "/usr/include/dax.h"
}
#else
extern "C" {
#include "/usr/local/dax.h"
}
#endif

#include "util/Query.h"
#include "data/DataLoader.h"
#include "log/Result.h"
#include "thread/Thread.h"
#include "api/ScanApi.h"

class JoinApi {
private:
	//ScanApi *factScan; //left relation
	table *factTable;
	ScanApi *dimensionScan; //right relation

	int joinColId; //id of the join column

	uint64_t *bit_map_res;
	void *join_vector; //keep the join result -- defines which items of the fact table satisfies
	int block_size;

	int num_of_columns;
	int segs_per_part;

	dax_vec_t src;
	dax_vec_t bit_map;
	dax_vec_t dst;
	uint64_t flag;

	JOB_TYPE j_type;

	void reserveBitVector() {
		this->block_size = factTable->t_meta.num_of_segments * 8;
		posix_memalign(&join_vector, 64, block_size);
	}

	friend class AggApi;

public:
	JoinApi(table *factTable, ScanApi *dimScan, int joinColId, JOB_TYPE j_type) {
		this->factTable = factTable;
		this->dimensionScan = dimScan;
		this->joinColId = joinColId;
		this->j_type = j_type;

		this->num_of_columns = factTable->t_meta.num_of_columns;
		this->segs_per_part = factTable->t_meta.num_of_segments / factTable->t_meta.num_of_parts;

		initializeJoin();
	}

	~JoinApi() {
		free(join_vector);
	}

	void initializeJoin() {
		bit_map_res = (uint64_t *) dimensionScan->getBitResult();
		reserveBitVector();
		initDaxVectors();
	}

	void initDaxVectors() {
		memset(&src, 0, sizeof(dax_vec_t));
		memset(&bit_map, 0, sizeof(dax_vec_t));
		memset(&dst, 0, sizeof(dax_vec_t));

		src.format = DAX_BITS;
		src.elem_width = (&factTable->columns[joinColId])->encoder.num_of_bits + 1;

		bit_map.format = DAX_BITS;
		bit_map.elem_width = 1;
		bit_map.data = bit_map_res;

		dst.offset = 0;
		dst.format = DAX_BITS;
		dst.elem_width = 1;

		flag = DAX_CACHE_DST;
	}

	int numOfParts() {
		return dimensionScan->num_of_parts;
	}

	void* getJoinBitVector(int part) {
		int offset = part * this->segs_per_part;
		return (static_cast<uint64_t*>(join_vector)) + offset;
	}


	void hwJoin(dax_queue_t**, Node<Query>*);
	void swJoin(Node<Query>* node, Result *result);

	void printBitVector(uint64_t cur_result, int segId) {
		//print bit vals
		printf("%d: ", segId);
		for (int j = 0; j < 64; j++) {
			printf("%lu|", (cur_result & 1));
			cur_result >>= 1;
		}
		printf("\n");
	}

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

	bool bitAtPosition(uint64_t* bit_map, int bit_pos) {
		//printf("bit_pos: %d\n", bit_pos);
		int arr_ind = bit_pos / 64;
		int local_pos = 63 - (bit_pos % 64);

		uint64_t localVector = bit_map[arr_ind];
		return (localVector >> local_pos) & 1;
	}

	int JoinColId(){
		return this->joinColId;
	}

	ScanApi* DimensionScan(){
		return this->dimensionScan;
	}

};

#endif
