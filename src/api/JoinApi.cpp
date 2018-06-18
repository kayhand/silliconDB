#include "JoinApi.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <sys/time.h>

//#ifdef __sun
void JoinApi::hwJoin(dax_queue_t **queue, Node<Query>* node) {
	//while(dimensionScan->isCompleted() == false);

	int curPart = node->value.getPart();
	int ind = joinColId + curPart * this->num_of_columns;
	column *join_col = &(factTable->columns[ind]);

	int num_of_els = join_col->c_meta.col_size;

	this->src.data = join_col->compressed;
	this->src.elements = num_of_els;
	this->dst.elements = num_of_els;

	void* join_vector = getJoinBitVector(curPart);
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
//#endif

void JoinApi::swJoin(Node<Query>* node, Result *result) {
	//while (dimensionScan->isCompleted() == false);

	int curPart = node->value.getPart();
	int ind = joinColId + curPart * this->num_of_columns;
	column *join_col = &(factTable->columns[ind]);

	int num_of_els = join_col->c_meta.col_size;

	uint16_t* pos_vector = (uint16_t*) join_col->compressed;
	uint64_t* res_vector = (uint64_t *) getJoinBitVector(curPart);

	int dataInd = 0;
	int i = 0;
	int cnt = 0;

	uint16_t cur_pos = 0;
	uint64_t cur_result = 0;
	int count = 0;
	int total_lines = join_col->encoder.num_of_bits + 1;

	int remainder = num_of_els % 64;
	if(remainder == 0)
		remainder = 64;

	hrtime_t t_start, t_end;
	t_start = gethrtime();
	for (i = 0; i < join_col->c_meta.num_of_segments - 1; i++) {
		for (int j = 0; j < total_lines * 4; j++) {
			cur_result <<= 1;

			dataInd = i * total_lines * 4 + j;
			cur_pos = pos_vector[dataInd];

			cur_result |= bitAtPosition(bit_map_res, cur_pos);
		}
		res_vector[i] = cur_result;
		cnt = __builtin_popcountl(cur_result);
		count += cnt;
		cur_result = 0;
	}

	/*if(remainder < 64){
		printf("Last segment written: %d", i);
		printf("Last read %d\nNext is %d ...\n\n", dataInd, i * total_lines * 4);
	}*/

	dataInd++;
	//last segment
	for (int j = 0; j < remainder; j++) {
		cur_result <<= 1;
		cur_pos = pos_vector[dataInd++];
		cur_result |= bitAtPosition(bit_map_res, cur_pos);
	}

	for(int j = remainder; j < 64; j++){
		cur_result <<= 1;
	}
	res_vector[i] = cur_result;
	cnt = __builtin_popcountl(cur_result);
	count += cnt;
	cur_result = 0;

	t_end = gethrtime();
	//if(remainder < 64)
		//printf("els: %d, last data: %d, last seg. pos: %d\n", num_of_els, dataInd, i);

	//printf("Core Join (p: %d, els: %d)\n", curPart, num_of_els);

	result->addRuntime(SW_JOIN, make_tuple(t_start, t_end, -1, curPart));
	result->addCountResult(make_tuple(count, node->value.getTableId()));
	//printf("Core Count (join): %d for part %d\n", count, curPart);
}
