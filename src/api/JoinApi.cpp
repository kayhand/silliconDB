#include "JoinApi.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <sys/time.h>

#ifdef __sun 
void JoinApi::hwJoin(dax_queue_t **queue, Node<Query>* node){ 
    while(dimensionScan->isCompleted() == false);
    
    int curPart = node->value.getPart();
    int ind = joinColId + curPart * this->num_of_columns;
    column *join_col = &(this->factCols[ind]);

    int num_of_els = join_col->c_meta.col_size;
    this->part_els[curPart] = num_of_els;

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
//    else
  //      printf("Dax Join (%d), (%d)\n", curPart, node->value.getTableId());
} 
#endif

void JoinApi::swJoin(Node<Query>* node, Result *result){
    while(dimensionScan->isCompleted() == false);

    int curPart = node->value.getPart();
    int ind = joinColId + curPart * this->num_of_columns;
    column *join_col = &(this->factCols[ind]);

    int num_of_els = join_col->c_meta.col_size;
    this->part_els[curPart] = num_of_els;

    //num_of_els values each having 16bits of size
    uint16_t* pos_vector = (uint16_t*) join_col->compressed; 
    //uint64_t* bit_map_res = this->dimensionScan->getBitResult(); // right handside of the join operator
    //num_of_els bit values
    uint64_t* res_vector = (uint64_t*) getJoinBitVector(curPart);

    int dataInd = 0;
    int i = 0;
    int cnt = 0;

    uint16_t cur_pos = 0;
    uint64_t cur_result = 0;
    int count = 0;
    int total_lines = join_col->encoder.num_of_bits + 1; 

    hrtime_t t_start, t_end;
    t_start = gethrtime();
    for(i = 0; i < join_col->c_meta.num_of_segments; i++){
        for(int j = 0; j < total_lines * 4; j++){
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
    t_end = gethrtime();

    result->addRuntime(SW_JOIN, make_tuple(t_start, t_end, -1, curPart));
    if(joinColId == 2)
        result->addCountResult(make_tuple(count, 3));
    else if(joinColId == 4)
        result->addCountResult(make_tuple(count, 4));
    //printf("Core Count (join): %d for part %d\n", count, curPart);
}

