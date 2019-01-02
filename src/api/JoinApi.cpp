#include "JoinApi.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <sys/time.h>

template void JoinApi::swJoin_<uint8_t>(Node<Query>*, Result*);
template void JoinApi::swJoin_<uint16_t>(Node<Query>*, Result*);
template void JoinApi::swJoin_<uint32_t>(Node<Query>*, Result*);

void JoinApi::hwJoin(dax_queue_t **queue, Node<Query>* node) {
	//while(dimensionScan->isCompleted() == false);

	int curPart = node->value.getPart();
	int ind = joinColId + curPart * this->num_of_columns;
	//printf("HW Join -- part: %d, col: %d\n", curPart, ind);
	column *join_col = &(factTable->columns[ind]);

	int num_of_els = join_col->c_meta.col_size;

	this->src.data = join_col->compressed;
	this->src.elements = num_of_els;
	this->dst.elements = num_of_els;

	void* join_vector = (void*) getJoinBitVector(curPart);
	dst.data = join_vector;

	node->t_start = gethrtime();
	node->post_data.t_start = gethrtime();
	node->post_data.node_ptr = (void *) node;

	dax_status_t join_status = dax_translate_post(*queue, flag, &src, &dst, &bit_map, src.elem_width, (void *) &(node->post_data));

	if(join_status != 0)
		printf("Dax Join Error! %d\n", join_status);
    //else
        //printf("Dax Join (p: %d, els: %d)\n", curPart, num_of_els);
}

void JoinApi::hwJoinCp(dax_context **ctx, Node<Query>* node) {
	int curPart = node->value.getPart();
	int ind = joinColId + curPart * this->num_of_columns;
	//printf("HW CP Join -- part: %d, col: %d\n", curPart, ind);

	column *join_col = &(factTable->columns[ind]);
	this->src.data = join_col->co_partitioned.right_partition;

	int num_of_els = join_col->c_meta.col_size;
	this->src.elements = num_of_els;
	this->dst.elements = num_of_els;

	dax_result_t res;
	node->t_start = gethrtime();
	for(int j_id = 0; j_id < 4; j_id++){
		bit_map.data = getSubBitMap(j_id);
		dst.data = (void*) getSubJoinBitVector(j_id, curPart);

		res = dax_translate(*ctx, flag, &src, &dst, &bit_map, src.elem_width);

		if(res.status != DAX_SUCCESS)
			printf("Dax Join Error! %d\n", res.status);
		else{
        	printf("Dax Part Join (p: %d, count: %lu)\n", curPart, res.count);
		}
	}
	printf("HW Cp Join -- part: %d done. Now will merge result vectors!\n", curPart);
	node->count = res.count;

	/*
	uint64_t *join_vector = getJoinBitVector(curPart);
	uint64_t *left_partition = (uint64_t *) join_col->co_partitioned.left_partition;
	for(uint32_t j_id = 0; j_id < 3; j_id++){
		uint8_t *bit_res = (uint64_t*) getSubJoinBitVector(j_id, curPart);
		uint64_t part_mask = j_id | (j_id << 8) | (j_id << 16) | (j_id << 24);
		part_mask = part_mask | (part_mask << 32);
		cout << "cur mask: " << part_mask << endl;

		uint64_t cur_parts;
		uint64_t xnor;
		uint8_t bit_vals[8];
		int bit_group = 0;
		//Each index contains 8 partition values (each value is 8 bits)
		for(int val_group = 0; val_group < num_of_els / 8; val_group++){
			cur_parts = left_partition[val_group]; //8 part values
			xnor = ~(cur_parts ^ part_mask); //XNOR

			bit_vals[bit_group++] = bytes_to_bits(xnor);
			if(bit_group == 8){

			}

		}
	}*/
}

template<typename BitSize>
void JoinApi::swJoin_(Node<Query>* node, Result *result) {
	int curPart = node->value.getPart();
	int ind = joinColId + curPart * this->num_of_columns;
	column *join_col = &(factTable->columns[ind]);

	int num_of_els = join_col->c_meta.col_size;

	BitSize *pos_vector = (BitSize*) join_col->compressed;
	uint64_t* res_vector = (uint64_t *) getJoinBitVector(curPart);

	int dataInd = 0;
	int i = 0;
	int cnt = 0;

	int cur_pos = 0;
	uint64_t cur_result = 0;
	int count = 0;
	int total_lines = join_col->encoder.num_of_bits + 1;
	int items_per_line = join_col->c_meta.num_of_codes; //2 or 4

	int remainder = num_of_els % 64;
	if(remainder == 0)
		remainder = 64;

	hrtime_t t_start, t_end;
	t_start = gethrtime();
	for (i = 0; i < join_col->c_meta.num_of_segments - 1; i++) {
		for (int j = 0; j < total_lines * items_per_line; j++) {
				cur_result <<= 1;
				dataInd = i * total_lines * items_per_line + j;
				cur_pos = pos_vector[dataInd];

				cur_result |= bitAtPosition(cur_pos);

		}
		res_vector[i] = cur_result;
		cnt = __builtin_popcountl(cur_result);
		count += cnt;
		cur_result = 0;
	}

	dataInd++;
	//last segment
	for (int j = 0; j < remainder; j++) {
		cur_result <<= 1;
		cur_pos = pos_vector[dataInd++];

		cur_result |= bitAtPosition(cur_pos);
	}

	for(int j = remainder; j < 64; j++){
		cur_result <<= 1;
	}
	res_vector[i] = cur_result;
	cnt = __builtin_popcountl(cur_result);
	count += cnt;
	cur_result = 0;

	t_end = gethrtime();

	//printf("Core Join (p: %d, els: %d)\n", curPart, num_of_els);

	result->addRuntime(false, this->j_type, make_tuple(t_start, t_end, -1, curPart));
	result->addCountResult(make_tuple(this->j_type, count));
	//printf("Core Count (join): %d for part %d\n", count, curPart);
}

/*
void JoinApi::swJoin16(Node<Query>* node, Result *result) {
	//while (dimensionScan->isCompleted() == false);

	int curPart = node->value.getPart();
	int ind = joinColId + curPart * this->num_of_columns;
	//printf("SW Join -- part: %d, col: %d\n", curPart, ind);
	column *join_col = &(factTable->columns[ind]);

	int num_of_els = join_col->c_meta.col_size;

	uint16_t *pos_vector = (uint16_t*) join_col->compressed;
	uint64_t* res_vector = (uint64_t *) getJoinBitVector(curPart);

	int dataInd = 0;
	int i = 0;
	int cnt = 0;

	uint16_t cur_pos = 0;
	uint64_t cur_result = 0;
	int count = 0;
	int total_lines = join_col->encoder.num_of_bits + 1;
	int items_per_line = join_col->c_meta.num_of_codes; //2 or 4

	int remainder = num_of_els % 64;
	if(remainder == 0)
		remainder = 64;

	hrtime_t t_start, t_end;
	t_start = gethrtime();
	for (i = 0; i < join_col->c_meta.num_of_segments - 1; i++) {
		for (int j = 0; j < total_lines * items_per_line; j++) {
				cur_result <<= 1;
				dataInd = i * total_lines * items_per_line + j;
				cur_pos = pos_vector[dataInd];

				cur_result |= bitAtPosition(cur_pos);

		}
		res_vector[i] = cur_result;
		cnt = __builtin_popcountl(cur_result);
		count += cnt;
		cur_result = 0;
	}

	dataInd++;
	//last segment
	for (int j = 0; j < remainder; j++) {
		cur_result <<= 1;
		cur_pos = pos_vector[dataInd++];

		cur_result |= bitAtPosition(cur_pos);
	}

	for(int j = remainder; j < 64; j++){
		cur_result <<= 1;
	}
	res_vector[i] = cur_result;
	cnt = __builtin_popcountl(cur_result);
	count += cnt;
	cur_result = 0;

	t_end = gethrtime();

	//printf("Core Join (p: %d, els: %d)\n", curPart, num_of_els);

	result->addRuntime(false, this->j_type, make_tuple(t_start, t_end, -1, curPart));
	result->addCountResult(make_tuple(this->j_type, count));
	//printf("Core Count (join): %d for part %d\n", count, curPart);
}*/

/*
void JoinApi::swJoin32(Node<Query>* node, Result *result) {
	//while (dimensionScan->isCompleted() == false);

	int curPart = node->value.getPart();
	int ind = joinColId + curPart * this->num_of_columns;
	//printf("SW Join -- part: %d, col: %d\n", curPart, ind);
	column *join_col = &(factTable->columns[ind]);

	int num_of_els = join_col->c_meta.col_size;

	uint32_t *pos_vector = (uint32_t*) join_col->compressed;
	uint64_t* res_vector = (uint64_t *) getJoinBitVector(curPart);

	int dataInd = 0;
	int i = 0;
	int cnt = 0;

	uint32_t cur_pos = 0;
	uint64_t cur_result = 0;
	int count = 0;
	int total_lines = join_col->encoder.num_of_bits + 1;
	int items_per_line = join_col->c_meta.num_of_codes; //2 or 4

	int remainder = num_of_els % 64;
	if(remainder == 0)
		remainder = 64;

	hrtime_t t_start, t_end;
	t_start = gethrtime();
	for (i = 0; i < join_col->c_meta.num_of_segments - 1; i++) {
		for (int j = 0; j < total_lines * items_per_line; j++) {
				cur_result <<= 1;
				dataInd = i * total_lines * items_per_line + j;
				cur_pos = pos_vector[dataInd];

				cur_result |= bitAtPosition(cur_pos);

		}
		res_vector[i] = cur_result;
		cnt = __builtin_popcountl(cur_result);
		count += cnt;
		cur_result = 0;
	}

	dataInd++;
	//last segment
	for (int j = 0; j < remainder; j++) {
		cur_result <<= 1;
		cur_pos = pos_vector[dataInd++];

		cur_result |= bitAtPosition(cur_pos);
	}

	for(int j = remainder; j < 64; j++){
		cur_result <<= 1;
	}
	res_vector[i] = cur_result;
	cnt = __builtin_popcountl(cur_result);
	count += cnt;
	cur_result = 0;

	t_end = gethrtime();

	//printf("Core Join (p: %d, els: %d)\n", curPart, num_of_els);

	result->addRuntime(false, this->j_type, make_tuple(t_start, t_end, -1, curPart));
	result->addCountResult(make_tuple(this->j_type, count));
	//printf("Core Count (join): %d for part %d\n", count, curPart);
}*/
