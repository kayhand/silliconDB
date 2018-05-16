#include "ScanApi.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <sys/time.h>

#ifdef __sun 
void ScanApi::hwScan(dax_queue_t **queue, Node<Query>* node){ 
    int curPart = node->value.getPart();
    int ind = colId + (curPart) * baseTable->t_meta.num_of_columns;
    column *col = &(baseTable->columns[ind]);
    int num_of_els = col->c_meta.col_size;

    this->src.data = col->compressed;
    this->src.elements = num_of_els;
    this->dst.elements = num_of_els;

    void* bit_vector = getFilterBitVector(curPart * this->num_of_segments);
//    printf("Dax will scan part (%d) for table (%d) -- \n bit vector start: %d \n", curPart, node->value.getTableId(), curPart * this->num_of_segments);
    dst.data = bit_vector;//*

    node->t_start = gethrtime();
    node->post_data.t_start = gethrtime();
    node->post_data.node_ptr = (void *) node;

    //dax_status_t scan_status = dax_scan_value_post(*queue, this->flag, &(this->src), &(this->dst), this->cmp, &(this->predicate), (void *) node);
    dax_status_t scan_status = dax_scan_value_post(*queue, flag, &src, &dst, cmp, &predicate, (void *) &(node->post_data));
  //  printf("Dax (%d), (%d)\n", curPart, node->value.getTableId());
    if(scan_status != 0)
        printf("Dax Error! %d\n", scan_status);

} 
#endif

void ScanApi::simdScan16(Node<Query>* node, Result *result){
    int curPart = node->value.getPart();
    int ind = colId + (curPart) * baseTable->t_meta.num_of_columns;
    column *col = &(baseTable->columns[ind]);

    v4hi data_vec = {0, 0, 0, 0};
    v4hi converted_pred = {0, 0, 0, 0};

    converted_pred += (uint16_t) this->comp_predicate;
   
    uint64_t cur_result = 0;
    int count = 0;
    int total_lines = col->encoder.num_of_bits + 1; 

    uint64_t* bit_vector = (uint64_t*) getFilterBitVector(curPart * this->num_of_segments);
    int extra_vals = (col->c_meta.num_of_segments * 64) - col->c_meta.col_size; 

    if(extra_vals > 0){
        printf("Size: %d - extra: %d, segs: %d (part: %d)\n", col->c_meta.col_size, extra_vals, col->c_meta.num_of_segments, curPart); 
    }
    uint64_t clear_vector = ((1ul << col->c_meta.col_size) - 1) << extra_vals;

    uint64_t *data_p = 0;
    int dataInd = 0;
    int i = 0;
    int cnt = 0;

    hrtime_t t_start, t_end;
    t_start = gethrtime();
    for(i = 0; i < col->c_meta.num_of_segments; i++){
        for(int j = 0; j < total_lines; j++){
	    cur_result <<= 4;
	    dataInd = i * total_lines + j;
	    data_p = col->compressed + dataInd;

            data_vec = (v4hi) *(data_p);
	    if(cmp == dax_compare_t::DAX_EQ)
	        cur_result |= __builtin_vis_fcmpeq16(data_vec, converted_pred);  
	    else
	        cur_result |= __builtin_vis_fcmpgt16(data_vec, converted_pred);  
        }

	if(cmp == dax_compare_t::DAX_LE){
	    cur_result = ~cur_result;
	    //if(extra_vals > 0)
	        //cur_result &= clear_vector;
	}

	bit_vector[i] = cur_result;
	cnt = __builtin_popcountl(cur_result);
	count += cnt;
        cur_result = 0;
    } 
    t_end = gethrtime();

    if(cmp == dax_compare_t::DAX_LE && extra_vals > 0){
        count -= cnt;
        bit_vector[i - 1] &= clear_vector;
	count += __builtin_popcountl(bit_vector[i - 1]);
    }

    result->addRuntime(SW_SCAN, make_tuple(t_start, t_end));
    result->addCountResult(make_tuple(count, baseTable->t_meta.t_id));
    printf("Core Count: %d for part %d\n", count, curPart);

    //if(extra_vals > 0)
        //printBitVector(bit_vector, col->c_meta.num_of_segments, clear_vector);
}


