
#include "Query.h"
#include "Query1.h"
#include "../log/Result.h"

#include <algorithm>
#include <cstring>
#include <sys/time.h>

Query1::Query1(){}
Query1::~Query1(){}

#ifdef __sun 
void Query1::linescan_hw(DataCompressor *dataComp, int scaledPart, Result *result, dax_context_t **ctx)
{ 
    table *compTable = dataComp->getTable();
    int num_of_parts = dataComp->getNumOfParts();
    int curPart = scaledPart % num_of_parts;
    uint64_t comp_predicate = 0;
    comp_predicate = compTable->columns[10].keys["1996-01-10"];

    int ind = 10 + (curPart) * compTable->nb_columns;
    column *col = &(compTable->columns[ind]);

    dax_int_t predicate;
    dax_vec_t src;
    dax_vec_t dst;

    memset(&predicate, 0, sizeof(dax_int_t));
    memset(&src, 0, sizeof(dax_vec_t));
    memset(&dst, 0, sizeof(dax_vec_t));

    int start = col->start; 
    int end = col->end; 

    int num_of_els = end - start + 1;
    if(num_of_els % 64 != 0)
        num_of_els = ((num_of_els / 64) + 1) * 64;

    src.elements = num_of_els;
    src.format = DAX_BITS;
    src.elem_width = col->num_of_bits + 1;
    src.data = col->compressed;

    predicate.format = DAX_BITS;
    predicate.elem_width = src.elem_width;
    predicate.dword[2] = comp_predicate;

    dst.elements = src.elements;
    dst.offset = 0;
    dst.format = DAX_BITS;
    dst.elem_width = 1;
    int num_of_segments = compTable->num_of_segments / dataComp->getNumOfParts();
    void* bit_vector = dataComp->getBitVector(curPart * num_of_segments);
    dst.data = bit_vector;

    hrtime_t t_start, t_end;
    t_start = gethrtime();
    dax_result_t scan_res = dax_scan_value(*ctx, 0, &src, &dst, DAX_LT, &predicate);
    t_end = gethrtime();
    if(scan_res.status != 0)
        printf("Dax Error! %d\n", scan_res.status);
    //printf("DAX Count: %lu\n", scan_res.count);

    result->addRuntime(DAX_SCAN, make_tuple(t_start, t_end));
} 

void Query1::linescan_post(DataCompressor *dataComp, dax_queue_t **queue, Query *item)
{ 
    table *compTable = dataComp->getTable();
    int num_of_parts = dataComp->getNumOfParts();
    int curPart = item->getPart() % num_of_parts;
    uint64_t comp_predicate = 0;
    comp_predicate = compTable->columns[10].keys["1996-01-10"];

    int ind = 10 + (curPart) * compTable->nb_columns;
    column *col = &(compTable->columns[ind]);

    dax_int_t predicate;
    dax_vec_t src;
    dax_vec_t dst;

    memset(&predicate, 0, sizeof(dax_int_t));
    memset(&src, 0, sizeof(dax_vec_t));
    memset(&dst, 0, sizeof(dax_vec_t));

    int start = col->start; 
    int end = col->end; 

    int num_of_els = end - start + 1;
    if(num_of_els % 64 != 0)
        num_of_els = ((num_of_els / 64) + 1) * 64;

    src.elements = num_of_els;
    src.format = DAX_BITS;
    src.elem_width = col->num_of_bits + 1;

    src.data = malloc(src.elements * src.elem_width);
    src.data = col->compressed;

    predicate.format = DAX_BITS;
    predicate.elem_width = src.elem_width;
    predicate.dword[2] = comp_predicate;

    dst.elements = src.elements;
    dst.offset = 0;
    dst.format = DAX_BITS;
    dst.elem_width = 1;
    int num_of_segments = compTable->num_of_segments / dataComp->getNumOfParts();
    void* bit_vector = dataComp->getBitVector(curPart * num_of_segments);
    dst.data = bit_vector;

    item->get_udata().t_start = gethrtime();
    item->get_udata().src_data = src.data;
    item->get_udata().dst_data = dst.data;
    item->get_udata().bit_vector = (uint64_t*) bit_vector;
    item->get_udata().copy_size = col->num_of_segments * 8;

    void *udata_p = &(item->get_udata());

    dax_status_t scan_status = dax_scan_value_post(*queue, 0, &src, &dst, DAX_LT, &predicate, udata_p);
    if(scan_status != 0)
        printf("Dax Error! %d\n", scan_status);
} 


void Query1::linescan_hw(DataCompressor *dataComp, int curPart, dax_context_t **ctx, pthread_barrier_t *barrier, pthread_barrier_t *dax_barrier)
{ 
    table *compTable = dataComp->getTable();
    uint64_t comp_predicate = 0;
    comp_predicate = compTable->columns[10].keys["1996-01-10"];
    /*if(curPart == 0){
        for(auto &curr : compTable->columns[10].keys)
	    printf("%s -> %d\n", curr.first.c_str(), curr.second);    
    }*/

    int ind = 10 + curPart * compTable->nb_columns;
    column *col = &(compTable->columns[ind]);

    dax_int_t predicate;
    dax_vec_t src;
    dax_vec_t dst;

    memset(&predicate, 0, sizeof(dax_int_t));
    memset(&src, 0, sizeof(dax_vec_t));
    memset(&dst, 0, sizeof(dax_vec_t));

    int start = col->start; 
    int end = col->end; 

    int num_of_els = end - start + 1;
    if(num_of_els % 64 != 0)
        num_of_els = ((num_of_els / 64) + 1) * 64;

    src.elements = num_of_els;
    src.format = DAX_BITS;
    src.elem_width = col->num_of_bits + 1;

    src.data = malloc(src.elements * src.elem_width);
    src.data = col->compressed;

    predicate.format = DAX_BITS;
    predicate.elem_width = src.elem_width;
    predicate.dword[2] = comp_predicate;

    dst.elements = src.elements;
    dst.offset = 0;
    dst.format = DAX_BITS;
    dst.elem_width = 1;
    int num_of_segments = compTable->num_of_segments / dataComp->getNumOfParts();
    void* bit_vector = dataComp->getBitVector(curPart * num_of_segments);
    dst.data = bit_vector;

    hrtime_t t_start, t_end;
    t_start = gethrtime();
    dax_result_t scan_res = dax_scan_value(*ctx, DAX_CACHE_DST, &src, &dst, DAX_GT, &predicate);
    t_end = gethrtime();
    long long t_res = t_end - t_start;
    if(scan_res.status != 0)
        printf("Dax Error! %d\n", scan_res.status);
    //printf("DAX Count: %lu\n", scan_res.count);

    FILE *pFile;
    pFile = fopen("dax_results.txt", "a");
    fprintf(pFile, "DAX Scan took ::%lld\n", t_res);
    fclose(pFile);
    //int remainder = WORD_SIZE - ((col->end - col->start + 1) % WORD_SIZE); 
    //printf("DAX Scan took %llu ns!\n", t_res);
    //printf("Count: %lu\n", scan_res.count - remainder);

    //printf("Writing %d bits to bit_vector index %d\n", col->num_of_segments, curPart * col->num_of_segments);
    pthread_barrier_wait(barrier);
    pthread_barrier_wait(dax_barrier);
} 

#endif

void Query1::linescan_sw(DataCompressor *dataComp, int scaledPart, Result *result)
{ 
    table *compTable = dataComp->getTable();
    int num_of_parts = dataComp->getNumOfParts();
    int curPart = scaledPart % num_of_parts;
    int ind = 10 + curPart * compTable->nb_columns;
    column *col = &(compTable->columns[ind]);

    uint64_t predicate = compTable->columns[10].keys["1996-01-10"];
    uint64_t upper_bound = (0 << col->num_of_bits) | predicate; 
    uint64_t mask = (0 << col->num_of_bits) | (uint64_t) (pow(2, col->num_of_bits) - 1);

    int i, j;
    for(i = 0; i < WORD_SIZE / (col->num_of_bits + 1) - 1; i++){
        upper_bound |= (upper_bound << (col->num_of_bits + 1));
        mask |= (mask << (col->num_of_bits + 1));
    }

    uint64_t cur_result;
    int count = 0;
    int total_lines = col->num_of_bits + 1; 
    uint64_t local_res = 0;
    uint64_t data_vector = 0;
    int num_of_segments = compTable->num_of_segments / dataComp->getNumOfParts();
    uint64_t* bit_vector = (uint64_t *) dataComp->getBitVector(curPart * num_of_segments);
        
    hrtime_t t_start, t_end;
    t_start = gethrtime();
    /* GREATER THAN OPERATOR
    for(i = 0; i < col->num_of_segments; i++){
        for(j = 0; j < total_lines; j++){
            data_vector = col->compressed[i * total_lines + j];
            cur_result = upper_bound ^ mask;
            cur_result += data_vector;
            cur_result = cur_result & ~mask;
            count += __builtin_popcountl(cur_result);
            local_res = local_res | (cur_result >> j); 
        }
	bit_vector[i] = local_res;
        local_res = 0;
    }*/

    /* LESS THAN OPERATOR */
    for(i = 0; i < col->num_of_segments; i++){
        for(j = 0; j < total_lines; j++){
            data_vector = col->compressed[i * total_lines + j];
            cur_result = data_vector ^ mask;
            cur_result += upper_bound;
            cur_result = cur_result & ~mask;
            count += __builtin_popcountl(cur_result);
            local_res = local_res | (cur_result >> j); 
        }
	bit_vector[i] = local_res;
        local_res = 0;
    }
 
    t_end = gethrtime();
    result->addRuntime(SW_SCAN, make_tuple(t_start, t_end));

    //printf("SW Scan took %llu ns!\n", t_res);
    //int remainder = WORD_SIZE - ((col->end - col->start + 1) % WORD_SIZE); 
    //printf("Count: %d\n", count - remainder);
    //agg(compTable, bit_vector, curPart, dataComp->getCompLines(curPart), col->end - col->start, 0);
}

void Query1::count(DataCompressor *dataComp, int scaledPart, Result *result){
    table *compTable = dataComp->getTable();
    int num_of_parts = dataComp->getNumOfParts();
    int sf = dataComp->getScaleFactor();
    int curPart = scaledPart % num_of_parts;

    int num_of_segments = compTable->num_of_segments / dataComp->getNumOfParts();
    uint64_t* bit_vector = (uint64_t *) dataComp->getBitVector(curPart * num_of_segments);

    int count = 0;

    #ifdef __sun 
    hrtime_t t_start, t_end;
    t_start = gethrtime();
    #endif

    for(int ind = 0; ind < num_of_segments * sf; ind++){
        count += __builtin_popcountl(bit_vector[ind]);
    }

    #ifdef __sun 
    t_end = gethrtime();
    #endif
    printf("Part %d found count: %d\n", scaledPart, count);
 
    result->addRuntime(SW_SCAN, make_tuple(t_start, t_end));
}
/*
void Query1::agg(DataCompressor *dataComp, int scaledPart, Result *result, bool isDax){
    table *compTable = dataComp->getTable();
    int num_of_parts = dataComp->getNumOfParts();
    //int sf = dataComp->getScaleFactor();
    int curPart = scaledPart % num_of_parts;
    int ind = 10 + curPart * compTable->nb_columns;

    column *col = &(compTable->columns[ind]);
    //int remaining_data = col->end - col->start + 1;
    //std::unordered_map<uint64_t, std::tuple<int, uint64_t>>  local_ht;    
    //std::tuple<int, uint64_t> local_ht[4] = make_tuple(0, 0);
    int lq_val = 0;
    //uint64_t lext_val = 0;

    int prev_ind = 10 + (curPart - 1) * compTable->nb_columns;
    int prev_num_segments = (curPart == 0) ? 0 : (&(compTable->columns[prev_ind]))->num_of_segments;
    uint64_t *bit_vector = dataComp->getBitVector() + scaledPart * prev_num_segments;
    //printf("Part %d : Start segment %d : Bit Vector %lu \n", scaledPart, scaledPart * prev_num_segments, bit_vector[0]);

    //<(rf + ls), (l_q, l_ext)> 
    //Columns: l_quantity, l_extprice, l_discount, l_tax, l_rf, l_ls 
    //vector<int> columns = {8, 9, 4, 5, 6, 7};
    vector<int> columns = {8, 9, 4, 5};
    vector<column*> baseCols(columns.size());
    vector<column*> partCols(columns.size());
    int partId = 0;
    vector<int> actual_columns;
    int loop_id = 0;
    for(int colId : columns){
        column *baseCol = &(compTable->columns[colId]);
	baseCols[loop_id] = baseCol; 
	partId = colId + curPart * compTable->nb_columns;
        column *colPart = &(compTable->columns[partId]);
	partCols[loop_id] = colPart; 
	loop_id++;
    } 
    vector<int> indices = Helper::createIndexArray(bit_vector, col, isDax); 

    #ifdef __sun 
    hrtime_t t_start, t_end;
    t_start = gethrtime();
    #endif

    for(int data_ind : indices){
	lq_val += baseCols.at(2)->i_dict[partCols[2]->data[data_ind]];
	//lext_val += baseCols.at(3)->i_dict[partCols[3]->data[data_ind]];
    }

    #ifdef __sun 
    t_end = gethrtime();
    #endif
     
    result->addRuntime(AGG, make_tuple(t_start, t_end));
    result->addResult(lq_val);
}
*/

void Query1::agg(DataCompressor *dataComp, int scaledPart, Result *result, bool isDax){
    table *compTable = dataComp->getTable();
    int num_of_parts = dataComp->getNumOfParts();
    //int sf = dataComp->getScaleFactor();
    int curPart = scaledPart % num_of_parts;
    int ind = 10 + curPart * compTable->nb_columns;

    column *col = &(compTable->columns[ind]);
    int remaining_data = col->end - col->start + 1;
    //std::unordered_map<uint64_t, std::tuple<int, uint64_t>>  local_ht;    
    std::tuple<int, uint64_t> local_ht[4] = make_tuple(0, 0.0);

    int num_of_segments = compTable->num_of_segments / dataComp->getNumOfParts();
    uint64_t* bit_vector = (uint64_t*) dataComp->getBitVector(curPart * num_of_segments);
    //printf("Part %d : Start segment %d : Bit Vector %lu \n", scaledPart, scaledPart * prev_num_segments, bit_vector[0]);

    //<(rf + ls), (l_q, l_ext)> 
    //Columns: l_quantity, l_extprice, l_discount, l_tax, l_rf, l_ls 
    //vector<int> columns = {8, 9, 4, 5, 6, 7};
    vector<int> columns = {8, 9, 4, 5};
    vector<column*> baseCols(columns.size());
    vector<column*> partCols(columns.size());
    int partId = 0;
    vector<int> actual_columns;
    int loop_id = 0;
    for(int colId : columns){
        column *baseCol = &(compTable->columns[colId]);
	baseCols[loop_id] = baseCol; 
	partId = colId + curPart * compTable->nb_columns;
        column *colPart = &(compTable->columns[partId]);
	partCols[loop_id] = colPart; 
	loop_id++;
    } 

    unordered_map<uint32_t, int> *keyMap = &(compTable->keyMap);
    int index;
    uint32_t key;
    uint64_t cur_vec;
 
    //vector<int> indices = Helper::createIndexArray(bit_vector, col, isDax); 
    vector<int> indices = Helper::createIndexArray(bit_vector, col, isDax); 
    /*uint32_t *data_2 = partCols[2]->data;
    uint32_t *data_3 = partCols[2]->data;

    int *dict_2 = baseCols[2]->i_dict;
    int *dict_3 = baseCols[3]->i_dict;*/

    #ifdef __sun 
    hrtime_t t_start, t_end;
    t_start = gethrtime();
    #endif
    for(int i = 0; i < col->num_of_segments; i++){
    	//std::bitset<64> cur_vec(bit_vector[i]);
	cur_vec = bit_vector[i];
    	int data_ind = 0;
	int offset = 64 * i;
	//int max_bit = (remaining_data >= 64) | isDax ? 64 : remaining_data;  
	/*for(int bit_ind = 0; bit_ind < max_bit; bit_ind++){
	    //if(((test_val << (63 - bit_ind)) & bit_vector[i]) != 0){	    	
	    if(cur_vec.test(63 - bit_ind)){	    	
	    	data_ind = (isDax) ? col->index_mapping[bit_ind] : bit_ind;
		if(data_ind < remaining_data){
		    data_ind += offset;
		    key = partCols.at(0)->data[data_ind];
		    key = (key << 32) | partCols.at(1)->data[data_ind];
		    index = keyMap->at(key);
		    get<0>(local_ht[index]) += baseCols.at(2)->i_dict[partCols[2]->data[data_ind]];
		    get<1>(local_ht[index]) += baseCols.at(3)->i_dict[partCols[3]->data[data_ind]];
		}
	    }
	}*/ 			 
  
	for(; cur_vec;){
	    data_ind = __builtin_ffsl(cur_vec) - 1;
	    data_ind = 63 - data_ind;
	    if (isDax) data_ind = col->index_mapping[data_ind];
	    if(data_ind < remaining_data){
	        data_ind += offset;
		key = partCols.at(0)->data[data_ind];
		key = (key << 16) | partCols.at(1)->data[data_ind];
		index = keyMap->at(key);
	        get<0>(local_ht[index]) += baseCols[2]->i_dict[partCols[2]->data[data_ind]];
	        get<1>(local_ht[index]) += baseCols[3]->i_dict[partCols[3]->data[data_ind]];
	    }
	    cur_vec &= (cur_vec - 1);
	}
	remaining_data -= 64;
    }

    #ifdef __sun 
    t_end = gethrtime();
    #endif
     
    result->addRuntime(AGG, make_tuple(t_start, t_end));

    unordered_map<int, uint32_t> *reversedMap = &(compTable->reversedMap);
    tuple<char, char, int, uint64_t> agg_r;

    for(int i = 0; i < 4; i++){
        uint32_t key = (*reversedMap)[i];
        string first_part = compTable->columns[8].dict[(uint16_t) (key >> 16)];
        string second_part = compTable->columns[9].dict[(uint16_t) key];

	agg_r = make_tuple(first_part[0], second_part[0], get<0>(local_ht[i]), get<1>(local_ht[i]));
    	result->addAggResult(agg_r);
    }
}


