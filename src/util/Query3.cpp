#include "Query.h"
#include "Query3.h"
#include "../log/Result.h"
#include <algorithm>
#include <cstring>
#include <memory>
#include <sys/time.h>

Query3::Query3(){}
Query3::~Query3(){}

#ifdef __sun 
void Query3::linescan_hw(DataCompressor *dataComp, int scaledPart, Result *result, dax_context_t **ctx, dax_queue_t **queue, bool async, void *udata)
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

    src.elements = num_of_els;
    src.format = DAX_BITS;
    src.elem_width = col->num_of_bits + 1;
    if(src.elem_width > 16){
        src.format = DAX_BYTES;
	src.elem_width /= 8;
    }

    src.data = col->compressed;
    predicate.format = src.format;
    predicate.elem_width = src.elem_width;
    predicate.dword[2] = comp_predicate;

    dst.elements = src.elements;
    dst.offset = 0;
    dst.format = DAX_BITS;
    dst.elem_width = 1;
    int num_of_segments = dataComp->getPartitioner()->getSegsPerPart();
    if(scaledPart == 0)
        num_of_segments = 0;
    void* bit_vector = dataComp->getBitVector(curPart * num_of_segments);
    dst.data = bit_vector;

    hrtime_t t_start, t_end;
    t_start = gethrtime();
    if(!async){
    	dax_result_t scan_res = dax_scan_value(*ctx, 0, &src, &dst, DAX_GT, &predicate);
    	if(scan_res.status != 0)
            printf("Dax Error! %d\n", scan_res.status);
    	result->addCountResultDax(make_tuple(scan_res.count, 0));
    }
    else{
    	dax_status_t scan_status = dax_scan_value_post(*queue, 0, &src, &dst, DAX_GT, &predicate, udata);
    	if(scan_status != 0)
            printf("Dax Error! %d\n", scan_status);
    }
    t_end = gethrtime();
    result->addRuntime(DAX_SCAN, make_tuple(t_start, t_end));
} 
#endif

#ifdef __sun 
void Query3::orderscan_hw(DataCompressor *dataComp, int curPart, Result *result, dax_context_t **ctx, dax_queue_t **queue, bool async, void *udata)

{ 
    table *compTable = dataComp->getTable();
    uint64_t comp_predicate = 0;
    comp_predicate = compTable->columns[4].keys["1996-01-10"];

    int ind = 4 + (curPart) * compTable->nb_columns;
    column *col = &(compTable->columns[ind]);

    dax_int_t predicate;
    dax_vec_t src;
    dax_vec_t dst;

    memset(&predicate, 0, sizeof(dax_int_t));
    memset(&src, 0, sizeof(dax_vec_t));
    memset(&dst, 0, sizeof(dax_vec_t));

    int num_of_els = col->end - col->start + 1;
    //int remainder = 64 - num_of_els % 64;

    src.elements = num_of_els;
    src.format = DAX_BITS;
    src.elem_width = col->num_of_bits + 1;
    src.data = col->compressed;

    if(src.elem_width > 16){
        src.format = DAX_BYTES;
	src.elem_width /= 8;
    }

    predicate.format = src.format;
    predicate.elem_width = src.elem_width;
    predicate.dword[2] = comp_predicate;

    dst.elements = src.elements;
    dst.offset = 0;
    dst.format = DAX_BITS;
    dst.elem_width = 1;

    int num_of_segments = dataComp->getPartitioner()->getSegsPerPart();
    void* bit_vector = dataComp->getBitVector(curPart * num_of_segments);
    dst.data = bit_vector;

    hrtime_t t_start, t_end;
    t_start = gethrtime();
    if(!async){
    	dax_result_t scan_res = dax_scan_value(*ctx, 0, &src, &dst, DAX_LE, &predicate);
    	if(scan_res.status != 0)
            printf("Dax Error! %d\n", scan_res.status);
    	result->addCountResult(make_tuple(scan_res.count, 1)); 
    	printf("DAX Count: %lu - %d\n", scan_res.count, (int) dst.elements);
    }
    else{
    	dax_status_t scan_status = dax_scan_value_post(*queue, 0, &src, &dst, DAX_LE, &predicate, udata);
    	if(scan_status != 0)
            printf("Dax Error! %d\n", scan_status);
    }

    t_end = gethrtime();
    result->addRuntime(DAX_SCAN, make_tuple(t_start, t_end));
} 
#endif

void Query3::linescan_sw(DataCompressor *dataComp, int scaledPart, Result *result)
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
    int num_of_segments = dataComp->getPartitioner()->getSegsPerPart();
    uint64_t* bit_vector = (uint64_t*) dataComp->getBitVector(curPart * num_of_segments);
        
    hrtime_t t_start, t_end;
    t_start = gethrtime();
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
    }  
    t_end = gethrtime();
    result->addRuntime(SW_SCAN, make_tuple(t_start, t_end));
    result->addCountResult(make_tuple(count, 0));
    //printf("SW Count: %d\n", count);
}

void Query3::linescan_simd(DataCompressor *dataComp, int scaledPart, Result *result){ 
    table *compTable = dataComp->getTable();
    int num_of_parts = dataComp->getNumOfParts();
    int curPart = scaledPart % num_of_parts;
    int ind = 10 + curPart * compTable->nb_columns;
    column *col = &(compTable->columns[ind]);
    
    v8qi data_vec = {0, 0, 0, 0, 0, 0, 0, 0};
    v8qi data_vec2 = {0, 0, 0, 0, 0, 0, 0, 0};
    v8qi data_vec3 = {0, 0, 0, 0, 0, 0, 0, 0};
    v2si converted_vec = {0, 0};
    v2si converted_pred = {0, 0};

    v8qi temp_vec = v8qi(data_vec);
    v8qi mask1 = {0, 0, 1, 2, 3, 3, 4, 5};
    v8qi mask2 = {6, 6, 7, 8, 9, 9, 10, 11};
    v8qi mask3 = {4, 4, 5, 6, 7, 7, 8, 9};
    v8qi mask4 = {2, 2, 3, 4, 5, 5, 6, 7};

    uint32_t predicate = compTable->columns[10].keys["1996-01-10"];
    converted_pred += predicate;

    uint64_t cur_result = 0;
    int count = 0;
    int total_lines = col->num_of_bits + 1; 
    int num_of_segments = dataComp->getPartitioner()->getSegsPerPart();
    //Template
    uint64_t* bit_vector = (uint64_t*) dataComp->getBitVector(curPart * num_of_segments);
    uint64_t *data_p = 0;

    int dataInd = 0;
    int i = 0, j = 0;
        
    hrtime_t t_start, t_end;
    t_start = gethrtime();
    for(i = 0; i < col->num_of_segments; i++){
        for(j = 0; j < total_lines; j+=3){
	    cur_result <<= 2;
	    dataInd = i * total_lines + j;
	    data_p = col->compressed + dataInd;

            data_vec = (v8qi) *(data_p);
            data_vec2 = (v8qi) *(data_p + 1);
            data_vec3 = (v8qi) *(data_p + 2);

	    temp_vec = __builtin_shuffle(data_vec, mask1);
	    converted_vec = (v2si) temp_vec;
	    cur_result |= __builtin_vis_fcmpgt32( converted_vec, converted_pred);  
	    cur_result <<= 2;

	    temp_vec = __builtin_shuffle(data_vec, data_vec2, mask2);
	    converted_vec = (v2si) temp_vec;
	    cur_result |= __builtin_vis_fcmpgt32( converted_vec, converted_pred);  
	    cur_result <<= 2;

	    temp_vec = __builtin_shuffle(data_vec2, data_vec3, mask3);
	    converted_vec = (v2si) temp_vec;
	    cur_result |= __builtin_vis_fcmpgt32( converted_vec, converted_pred);  
	    cur_result <<= 2;

	    temp_vec = __builtin_shuffle(data_vec3, mask4);
	    converted_vec = (v2si) temp_vec;
	    cur_result |= __builtin_vis_fcmpgt32( converted_vec, converted_pred);  
        }
	bit_vector[i] = cur_result;
	int cnt = __builtin_popcountl(cur_result);
	count += cnt;
        cur_result = 0;
    }  
    t_end = gethrtime();
    result->addRuntime(SW_SCAN, make_tuple(t_start, t_end));
    result->addCountResult(make_tuple(count, 0));
    //printf("SW Count(%d): %d\n", curPart, count);
}

void Query3::orderscan_sw(DataCompressor *dataComp, int curPart, Result *result)
{ 
    table *compTable = dataComp->getTable();
    int ind = 4 + curPart * compTable->nb_columns;
    column *col = &(compTable->columns[ind]);
    int remaining_data = col->end - col->start + 1;

    uint64_t predicate = compTable->columns[4].keys["1996-01-10"];
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
    int num_of_segments = dataComp->getPartitioner()->getSegsPerPart();
    uint64_t* bit_vector = (uint64_t *) dataComp->getBitVector(curPart * num_of_segments);
        
    hrtime_t t_start, t_end;
    t_start = gethrtime();
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
	remaining_data -= 64;
    }
    t_end = gethrtime();
    result->addRuntime(SW_SCAN, make_tuple(t_start, t_end));
    result->addCountResult(make_tuple(count, 1));

    //printf("SW Scan took %llu ns!\n", t_res);
    //printf("SW Count: %d\n", count);
    //printf("Predicate: %lu - %d - %d\n", predicate, col->num_of_bits, ind);
    //agg(compTable, bit_vector, curPart, dataComp->getCompLines(curPart), col->end - col->start, 0);
}

//24bits encoding
void Query3::orderscan_simd(DataCompressor *dataComp, int curPart, Result *result){ 
    table *compTable = dataComp->getTable();
    int ind = 4 + curPart * compTable->nb_columns;
    column *col = &(compTable->columns[ind]);

    v8qi data_vec = {0, 0, 0, 0, 0, 0, 0, 0};
    v8qi data_vec2 = {0, 0, 0, 0, 0, 0, 0, 0};
    v8qi data_vec3 = {0, 0, 0, 0, 0, 0, 0, 0};

    v2si converted_vec = {0, 0};
    v2si converted_pred = {0, 0};

    v8qi temp_vec = v8qi(data_vec);
    v8qi mask1 = {0, 0, 1, 2, 3, 3, 4, 5};
    v8qi mask2 = {6, 6, 7, 8, 9, 9, 10, 11};
    v8qi mask3 = {4, 4, 5, 6, 7, 7, 8, 9};
    v8qi mask4 = {2, 2, 3, 4, 5, 5, 6, 7};

    uint32_t predicate = compTable->columns[4].keys["1996-01-10"];
    converted_pred += predicate;

    uint64_t cur_result = 0;
    int count = 0;
    int total_lines = col->num_of_bits + 1; 
    int num_of_segments = dataComp->getPartitioner()->getSegsPerPart();
    uint64_t* bit_vector = (uint64_t*) dataComp->getBitVector(curPart * num_of_segments);
    uint64_t *data_p = 0;

    int dataInd = 0;
    int i = 0, j = 0;
        
    hrtime_t t_start, t_end;
    t_start = gethrtime();
    for(i = 0; i < col->num_of_segments; i++){
        for(j = 0; j < total_lines; j+=3){
	    cur_result <<= 2;
	    dataInd = i * total_lines + j;
	    data_p = col->compressed + dataInd;

            data_vec = (v8qi) *(data_p);
            data_vec2 = (v8qi) *(data_p + 1);
            data_vec3 = (v8qi) *(data_p + 2);

	    temp_vec = __builtin_shuffle(data_vec, mask1);
	    converted_vec = (v2si) temp_vec;
	    cur_result |= __builtin_vis_fcmple32( converted_vec, converted_pred);  
	    cur_result <<= 2;

	    temp_vec = __builtin_shuffle(data_vec, data_vec2, mask2);
	    converted_vec = (v2si) temp_vec;
	    cur_result |= __builtin_vis_fcmple32( converted_vec, converted_pred);  
	    cur_result <<= 2;

	    temp_vec = __builtin_shuffle(data_vec2, data_vec3, mask3);
	    converted_vec = (v2si) temp_vec;
	    cur_result |= __builtin_vis_fcmple32( converted_vec, converted_pred);  
	    cur_result <<= 2;

	    temp_vec = __builtin_shuffle(data_vec3, mask4);
	    converted_vec = (v2si) temp_vec;
	    cur_result |= __builtin_vis_fcmple32( converted_vec, converted_pred);  
        }
	bit_vector[i] = cur_result;
	int cnt = __builtin_popcountl(cur_result);
	count += cnt;
        cur_result = 0;
    }  

    t_end = gethrtime();
    result->addRuntime(SW_SCAN, make_tuple(t_start, t_end));
    result->addCountResult(make_tuple(count, 1));

    //printf("SW Scan took %llu ns!\n", t_res);
    //printf("SW Count: %d\n", count);
    //printf("Predicate: %lu - %d - %d\n", predicate, col->num_of_bits, ind);
    //agg(compTable, bit_vector, curPart, dataComp->getCompLines(curPart), col->end - col->start, 0);
}

#ifdef __sun 
void Query3::join_hw(DataCompressor *lineitemComp, DataCompressor *ordersComp, int curPart, Result *result, dax_context_t **ctx, dax_queue_t **queue, bool async, void *udata){ 
    table *lineTable = lineitemComp->getTable();
    table *ordersTable = ordersComp->getTable();

    int ind = curPart * lineTable->nb_columns; //l_orderkey
    column *lokey_col = &(lineTable->columns[ind]);

    int start = lokey_col->start; 
    int end = lokey_col->end; 
    int num_of_els = end - start + 1;
    //int remainder = 64 - num_of_els % 64;

    dax_vec_t src;
    dax_vec_t dst;
    dax_vec_t dst2;
    dax_vec_t bit_map;

    memset(&src, 0, sizeof(dax_vec_t));
    memset(&dst, 0, sizeof(dax_vec_t));
    memset(&dst2, 0, sizeof(dax_vec_t));
    memset(&bit_map, 0, sizeof(dax_vec_t));

    src.elements = num_of_els;
    src.offset = 0;
    int elem_bits = lokey_col->num_of_bits + 1;
    src.elem_width = elem_bits;
    src.format = DAX_BITS;
    if(src.elem_width > 16){
        src.format = DAX_BYTES;
	src.elem_width /= 8;
    	printf("Bytes:%d\n", src.elem_width);
    }
    src.data = lokey_col->compressed;

    int num_of_segments = lineitemComp->getPartitioner()->getSegsPerPart();
    void* bit_vector = lineitemComp->getJoinBitVector(curPart * num_of_segments);

    dst.elements = src.elements;
    dst.offset = 0;
    dst.format = DAX_BITS;
    dst.elem_width = 1;
    dst.data = bit_vector;

    bit_map.elements = ordersTable->nb_lines;
    bit_map.format = DAX_BITS;
    bit_map.elem_width = 1;
    bit_map.data = ordersComp->getBitVector(0);
    // src -- l_orderkey column from lineitem table
    // bit_map -- global bit vector result coming from the orders scan

    hrtime_t t_start, t_end;
    t_start = gethrtime();
    if(!async){
    	dax_result_t join_res = dax_translate(*ctx, DAX_CACHE_DST, &src, &dst, &bit_map, elem_bits);
    	if(join_res.status != 0)
            printf("Dax Error! %d\n", join_res.status);
    	result->addCountResult(make_tuple(join_res.count, 2)); 
    }
    else{
    	dax_status_t join_status = dax_translate_post(*queue, 0, &src, &dst, &bit_map, elem_bits, udata);
    	if(join_status != 0)
            printf("Dax Join Error! %d for part %d\n", join_status, curPart);
    }
    t_end = gethrtime();
    result->addRuntime(JOIN, make_tuple(t_start, t_end));
} 
#endif

void Query3::join_sw(DataCompressor *lineitemComp, DataCompressor *ordersComp, int curPart, Result *result){ 
    table *lineTable = lineitemComp->getTable();
    int ind = curPart * lineTable->nb_columns; //l_orderkey
    column *lokey_col = &(lineTable->columns[ind]);

    int num_of_segments = lineitemComp->getPartitioner()->getSegsPerPart();
    int remaining_els = lokey_col->end - lokey_col->start + 1;

    uint32_t* lokey_vals = lokey_col->data;
    uint64_t* bitmap_vector = (uint64_t *) ordersComp->getBitVector(0);
    uint64_t* join_vector = (uint64_t *) lineitemComp->getJoinBitVector(curPart * num_of_segments);

    // src -- l_orderkey column from lineitem table
    // bit_map -- global bit vector result coming from the orders scan

    uint64_t local_bit_map;
    int bit_id = 0;
    uint32_t cur_okey;
    uint64_t shifted_val = 1ul << 63;

    hrtime_t t_start, t_end;
    t_start = gethrtime();
    for(int i = 0; i < num_of_segments && remaining_els > 0; i++){
	uint64_t &loc_join = join_vector[i];
    	for(int d_ind = 0; d_ind < min(64 , remaining_els); d_ind++){
	    cur_okey = lokey_vals[i * 64 + d_ind];
	    bit_id = 63 - (cur_okey % 64);
	    local_bit_map = bitmap_vector[cur_okey / 64];
	    if((local_bit_map >> bit_id) & 1)
	        loc_join |= (shifted_val >> d_ind);
	}
	remaining_els -= 64;
    }
    t_end = gethrtime();

    int count = 0;
    for(int i = 0; i < num_of_segments; i++){
        count += __builtin_popcountl(join_vector[i]);
    }
    printf("Join count:%d\n\n", count);
    result->addRuntime(JOIN, make_tuple(t_start, t_end));
    result->addCountResult(make_tuple(count, 2));

}

template <class T_okey, class T_extp, class T_disc>
void Query3::agg(DataCompressor *dataComp, int curPart, Result *result){
    table *lineTable = dataComp->getTable();

    int okey_ind = 0 + curPart * lineTable->nb_columns;

    column *okey_col = &(lineTable->columns[okey_ind]);
    column *extp_col = &(lineTable->columns[okey_ind + 5]);
    //column *disc_col = &(lineTable->columns[okey_ind + 6]);

    //int distinct_okeys = dataComp->getDistinctKeys(0);
    //vector<int> results(0, distinct_okeys); 
    //results.resize(distinct_okeys);
    unordered_map<uint32_t, float> results;

    //int num_of_segments = okey_col->num_of_segments;
    int num_of_segments = okey_col->end - okey_col->start + 1;
    num_of_segments = ceil(num_of_segments / 64.0);
    uint64_t* j_vector = (uint64_t *) dataComp->getJoinBitVector(curPart * num_of_segments);
    uint64_t cur_vec;

    uint32_t o_key = 0;
    double ext_price = 0;
    //double disc = 0;

    T_okey *compressed_data = (T_okey *) okey_col->data;
    T_extp *extp_comp = (T_extp *) extp_col->data;
    //T_disc *disc_comp =(T_disc *) disc_col->data;

    #ifdef __sun 
    hrtime_t t_start, t_end;
    t_start = gethrtime();
    #endif
    for(int i = 0; i < num_of_segments; i++){
	cur_vec = j_vector[i];
    	int data_ind = 0;

	for(; cur_vec;){
	    //data_ind = (63 - (__builtin_ffsl(cur_vec) - 1)) + i * 64; //find the first set bit
	    data_ind = 64 - __builtin_ffsl(cur_vec);
	    data_ind = data_ind + i * 64; //find the first set bit
            o_key = okey_col->i_dict[compressed_data[data_ind]]; 
			
	    ext_price = extp_col->d_dict[extp_comp[data_ind]]; 
	    //disc = disc_col->d_dict[disc_comp[data_ind]]; 
	    if(results.find(o_key) == results.end())
	        //results[o_key] = ext_price * (1 - disc);
	        results[o_key] = ext_price;
	    else
	        //results.at(o_key) += ext_price * (1 - disc);
	        results.at(o_key) += ext_price;
	    cur_vec &= (cur_vec - 1);
	}
    }

    #ifdef __sun 
    t_end = gethrtime();
    #endif
     
    result->addRuntime(AGG, make_tuple(t_start, t_end));
    tuple <int, float> agg_r;
    for(auto &agg_r : results){
    	result->addAggResultQ3(agg_r);
    }
}
template void Query3::agg<uint16_t, uint32_t, uint8_t>(DataCompressor *dataComp, int curPart, Result *result);
template void Query3::agg<uint32_t, uint32_t, uint8_t>(DataCompressor *dataComp, int curPart, Result *result);

void Query3::count(DataCompressor *dataComp, int curPart, Result *result){
    //table *compTable = dataComp->getTable();
    //int ind = 4 + curPart * compTable->nb_columns;

    //column *col = &(compTable->columns[ind]);
    //int remaining_data = col->end - col->start + 1;
    //int remainder = 64 - remaining_data % 64;
    
    int num_of_segments = dataComp->getPartitioner()->getSegsPerPart();
    uint64_t* bit_vector = (uint64_t *) dataComp->getJoinBitVector(curPart * num_of_segments);
    printf("YOO FIX ME!!!\n");

    int count = 0;

    #ifdef __sun 
    hrtime_t t_start, t_end;
    t_start = gethrtime();
    #endif

    for(int ind = 0; ind < num_of_segments; ind++){
        count += __builtin_popcountl(bit_vector[ind]);
    }

    #ifdef __sun 
    t_end = gethrtime();
    #endif
 
    result->addRuntime(COUNT, make_tuple(t_start, t_end));
    result->addCountResult(make_tuple(count, 3));
}


