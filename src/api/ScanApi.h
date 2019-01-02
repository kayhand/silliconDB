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
	bool hasFilter = true;
	int colId; //column id to scan

	int num_of_parts;
	int num_of_segments; //actually segments per partition
	int blockSize;

	uint64_t pred1 = 0ul;
	uint64_t pred2 = 0ul;
	dax_compare_t cmp = DAX_EQ;

	void *filter_vector = NULL;

	dax_int_t predicate1;
	dax_int_t predicate2;
	uint64_t flag;

	dax_vec_t src;
	dax_vec_t dst;

	JOB_TYPE j_type;

	ScanApi *subScan = NULL; //predicates on multiple columns

	atomic<int> partsDone{0};

	void reserveBitVector(int block_size) {
		this->blockSize = block_size;
		posix_memalign(&filter_vector, 4096, block_size);
	}

	friend class JoinApi;
	friend class AggApi;

public:
	ScanApi(table *baseTable, int selCol, JOB_TYPE j_type, query_predicate &q_pred) {
		this->baseTable = baseTable;
		this->colId = selCol;
		this->num_of_segments = baseTable->t_meta.num_of_segments / baseTable->t_meta.num_of_parts;
		this->blockSize = baseTable->t_meta.num_of_segments * 8;
		this->num_of_parts = baseTable->t_meta.num_of_parts;
		this->j_type = j_type;

		initScanPredicates(q_pred);
	}

	ScanApi(table *baseTable, JOB_TYPE j_type) {
		this->baseTable = baseTable;
		this->colId = -1;
		this->num_of_segments = baseTable->t_meta.num_of_segments / baseTable->t_meta.num_of_parts;
		this->blockSize = baseTable->t_meta.num_of_segments * 8;
		this->num_of_parts = baseTable->t_meta.num_of_parts;
		this->j_type = j_type;
		hasFilter = false;

		reserveBitVector(blockSize);
	}

	~ScanApi() {
		free(filter_vector);
	}

	void setSubScan(ScanApi *subScan){
		this->subScan = subScan;
	}

	ScanApi* SubScan(){
		if(this->subScan == NULL)
			return NULL;
		return this->subScan;
	}

	JOB_TYPE &Type(){
		return j_type;
	}

	bool HasFilter(){
		return hasFilter;
	}

	bool HasAggKey(){
		return baseTable->t_meta.hasAggKey;
	}

	int totalParts() {
		return num_of_parts;
	}

	bool isCompleted(){
		return partsDone == num_of_parts;
	}

	void PartDone(){
		partsDone++;
		//printf("%u\n", (unsigned) partsDone);
	}

	void initScanPredicates(query_predicate &q_pred){
		printf("\tInitializing predicates for ScanAPI ...\n");
		q_pred.printPred();

		cmp = q_pred.comparison;

		if(q_pred.columnType == data_type_t::STRING){
			pred1 = baseTable->columns[colId].keys[q_pred.params[0]];
			if(q_pred.params[1] != "")
				pred2 = baseTable->columns[colId].keys[q_pred.params[1]];
		}
		else if(q_pred.columnType == data_type_t::INT){
			printf("INTEGER VALS!\n");
			printf("%s - %s\n", q_pred.params[0].c_str(), q_pred.params[1].c_str());
			pred1 = baseTable->columns[colId].i_keys[atoi(q_pred.params[0].c_str())];
			if(q_pred.params[1] != "")
				pred2 = baseTable->columns[colId].i_keys[atoi(q_pred.params[1].c_str())];
		}
		cout << "\t\t" << " set predicates to:  " << pred1 << " " << pred2
				 << " for relation with id: " << getBaseTable()->t_meta.t_id << endl;

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

		//printf("Dax bits: %d\n", src.elem_width);

		predicate1.format = src.format;
		predicate1.elem_width = src.elem_width;
		predicate1.dword[2] = this->pred1;

		predicate2.format = src.format;
		predicate2.elem_width = src.elem_width;
		predicate2.dword[2] = this->pred2;

		dst.offset = 0;
		dst.format = DAX_BITS;
		dst.elem_width = 1;

		flag = DAX_NOWAIT;
	}

	int bitVectorBlockSize() {
		return this->blockSize;
	}

	table* getBaseTable() {
		return this->baseTable;
	}

	void* getFilterBitVector(int part) {
		int offset = part * this->num_of_segments;
		return static_cast<uint64_t*>(filter_vector) + offset;
	}

	void* getBitResult() {
		return getFilterBitVector(0);
		//return this->filter_vector;
	}

	int getSegmentsPerPartition() {
		return this->num_of_segments;
	}


	bool hwScan(dax_queue_t**, Node<Query>*);
	void hwScanQT(dax_queue_t**, Node<Query>*, Result*);

	bool simdScan(Node<Query>* node, Result *result){
		int curPart = node->value.getPart();
		int ind = colId + (curPart) * baseTable->t_meta.num_of_columns;
		column *col = &(baseTable->columns[ind]);
		if(col->encoder.num_of_bits < 8){
			return simdScan8(node, result);
		}
		else if(col->encoder.num_of_bits < 16){
			return simdScan16(node, result);
		}
		else{
			return false;
		}
	}
	bool simdScan8(Node<Query>*, Result *result);
	bool simdScan16(Node<Query>*, Result *result);

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
