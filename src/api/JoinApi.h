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
	table *factTable;
	ScanApi *dimensionScan; //right relation

	int joinColId; //id of the join column

	uint64_t *bit_map_res;
	void *join_vector; //keep the join result

	//if repartitioned join
	vector<void*> join_vectors; //keep the join results

	int block_size;

	int num_of_columns;
	int segs_per_part;

	dax_vec_t src;
	dax_vec_t bit_map;
	dax_vec_t dst;

	uint64_t flag;

	JOB_TYPE j_type;
	bool co_part = false;
	bool join_rw = false;

	void reserveBitVector() {
		this->block_size = factTable->t_meta.num_of_segments * 8;
		posix_memalign(&join_vector, 4096, block_size);
		if(co_part & join_rw){
			reserveSubVectors();
			printf("join_rw approach!\n");
		}
	}

	void reserveSubVectors(){
		int size = 4;
		join_vectors.reserve(size);

		for(int i = 0; i < size; i++){
			void *cp_vector;
			posix_memalign(&cp_vector, 4096, block_size);
			join_vectors.push_back(cp_vector);
		}
	}

	friend class AggApi;

public:
	JoinApi(table *factTable, ScanApi *dimScan, int joinColId, JOB_TYPE j_type, bool coPart, bool joinRw) {
		this->factTable = factTable;
		this->dimensionScan = dimScan;
		this->joinColId = joinColId;
		this->j_type = j_type;

		this->num_of_columns = factTable->t_meta.num_of_columns;
		this->segs_per_part = factTable->t_meta.num_of_segments / factTable->t_meta.num_of_parts;

		this->co_part = coPart;
		this->join_rw = joinRw;

		initializeJoin();
	}

	~JoinApi() {
		free(join_vector);
		if(this->co_part & this->join_rw){
			for(void* cp_vector : join_vectors){
				free(cp_vector);
			}
		}
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

		src.elem_width = (&factTable->columns[joinColId])->encoder.num_of_bits + 1;
		if(co_part && join_rw)
		    src.elem_width /= 2;
		src.format = DAX_BITS;

		bit_map.format = DAX_BITS;
		bit_map.elem_width = 1;
		bit_map.data = bit_map_res;

		dst.offset = 0;
		dst.format = DAX_BITS;
		dst.elem_width = 1;

		flag = DAX_CACHE_DST;

		cout << "Dim. table ("<< dimensionScan->baseTable->t_meta.t_id << ") (" << joinColId << ") " << src.elem_width << endl;
	}

	int numOfParts() {
		return dimensionScan->num_of_parts;
	}

	void* getJoinBitVector(int part) {
		int offset = part * this->segs_per_part;
		return (static_cast<uint64_t*>(join_vector)) + offset;
	}

	void* getSubJoinBitVector(int j_id, int part) {
		int offset = part * this->segs_per_part;
		return (static_cast<uint64_t*>(join_vectors[j_id])) + offset;
	}

	void* getSubBitMap(int j_id) {
		int offset = j_id * 4096; //byte offset for the bit_map
		return (void*) (bit_map_res + offset);
	}

	void hwJoin(dax_queue_t**, Node<Query>*);
	void hwJoinCp(dax_context**, Node<Query>*);

	template<typename BitSize>
	void swJoin_(Node<Query>*, Result*);

	void swJoin(Node<Query>* node, Result* result){
		int curPart = node->value.getPart();
		int ind = joinColId + curPart * this->num_of_columns;
		column *join_col = &(factTable->columns[ind]);

		if(join_col->encoder.num_of_bits < 8){
			this->swJoin_<uint8_t>(node, result);
		}
		else if(join_col->encoder.num_of_bits < 16){
			this->swJoin_<uint16_t>(node, result);
		}
		else{
			this->swJoin_<uint32_t>(node, result);
		}
	}

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

	bool bitAtPosition(uint32_t bit_pos) {
		int arr_ind = bit_pos / 64;
		int local_pos = 63 - (bit_pos % 64);

		uint64_t localVector = bit_map_res[arr_ind];
		bool res = (localVector >> local_pos) & 1;
		return res;
	}

	bool bitAtPosition(int bit_pos) {
		int arr_ind = bit_pos / 64;
		int local_pos = 63 - (bit_pos % 64);

		uint64_t localVector = bit_map_res[arr_ind];
		bool res = (localVector >> local_pos) & 1;
		return res;
	}

	int bitsAtWord(uint64_t &cur_line, int &num_of_items) {
		uint32_t cur_pos;
		int result = 0;
		for(int item_id = num_of_items; item_id > 0; item_id--){
			result <<= 1;
			cur_pos = cur_line >> (src.elem_width * (item_id - 1));
			result |= bitAtPosition(cur_pos);
		}
		return result;
	}

	/*
	uint8_t bytes_to_bits(uint64_t val){
		uint8_t condensed_vec[8];
		uint8_t return_val;
		condensed_vec = (uint8_t *) val;
		for(int i = 0; i < 8; i++){
			return_val |= (condensed_vec[i] << i);
		}
		return return_val;
	}*/

	int JoinColId(){
		return this->joinColId;
	}

	ScanApi* DimensionScan(){
		return this->dimensionScan;
	}

	JOB_TYPE &JoinType(){
		return j_type;
	}

	bool isCoPartitioned(){
		return this->co_part;
	}

};

#endif
