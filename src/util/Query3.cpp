#include "Query3.h"
#include <cstring>
#include <sys/time.h>
#include <bitset>

Query3::Query3()
{
}

Query3::Query3(bool type)
{
    this->type = type; 
}

Query3::~Query3(){}

#ifdef __sun 
void Query3::linescan_hw(DataCompressor *dataComp, int curPart, dax_context_t **ctx)
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
    dst.data = memalign(8192, DAX_OUTPUT_SIZE(dst.elements, dst.elem_width));

    //hrtime_t t_start, t_end;
    //t_start = gethrtime();
    dax_result_t scan_res = dax_scan_value(*ctx, 0, &src, &dst, DAX_GT, &predicate);
    if(scan_res.status != 0)
        printf("Dax Error! %d\n", scan_res.status);
    //t_end = gethrtime();
    //long long t_res = t_end - t_start;

    //FILE *pFile;
    //pFile = fopen("dax_results.txt", "a");
    //fprintf(pFile, "%lld\n", t_res);
    //fclose(pFile);
    //printf("Count (lineitem): %lu  -- DAX -- %llu \n", scan_res.count, t_res);

    uint64_t* bit_vector = (dataComp->getBitVector() + curPart * col->num_of_segments); 
    memcpy(bit_vector, dst.data, col->num_of_segments * 8);

    free(src.data);
    free(dst.data);
} 
#endif

void Query3::linescan_sw(DataCompressor *dataComp, int curPart)
{ 
    table *compTable = dataComp->getTable();
    int ind = 10 + curPart * compTable->nb_columns;
    column *col = &(compTable->columns[ind]);

    uint64_t predicate = compTable->columns[10].keys["1996-01-10"];
    uint64_t data_vector = 0;
    uint64_t lower_bound = (0 << col->num_of_bits) | predicate; //Y vector 
    uint64_t mask = (0 << col->num_of_bits) | (uint64_t) (pow(2, col->num_of_bits) - 1);

    int i, j;
    for(i = 0; i < WORD_SIZE / (col->num_of_bits + 1) - 1; i++){
        lower_bound |= (lower_bound << (col->num_of_bits + 1));
        mask |= (mask << (col->num_of_bits + 1));
    }

    uint64_t upper_bound = lower_bound;
    uint64_t cur_result;
    int count = 0;
    int total_lines = col->num_of_bits + 1; 
    uint64_t local_res = 0;
    uint64_t* bit_vector = (dataComp->getBitVector() + curPart * col->num_of_segments); 
        
  //  hrtime_t t_start, t_end;
//    t_start = gethrtime();

    /* GREATER THAN OPERATOR */
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
 
    
  //  t_end = gethrtime();
    //long long t_res = t_end - t_start;

    //FILE *pFile;
    //pFile = fopen("sw_results.txt", "a");
    //fprintf(pFile, "%lld\n", t_res);
    //fclose(pFile);

//    printf("Count (lineitem): %d -- SW -- %llu \n", count, t_res);
}

#ifdef __sun 
void Query3::orderscan_hw(DataCompressor *dataComp, int curPart, dax_context_t **ctx)
{ 
    table *compTable = dataComp->getTable();
    uint64_t comp_predicate = 0;
    comp_predicate = compTable->columns[4].keys["1996-01-10"];
    /*if(curPart == 0){
        for(auto &curr : compTable->columns[10].keys)
	    printf("%s -> %d\n", curr.first.c_str(), curr.second);    
    }*/

    int ind = 4 + curPart * compTable->nb_columns;
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
    dst.data = memalign(8192, DAX_OUTPUT_SIZE(dst.elements, dst.elem_width));
    //dst.data = &bit_vector;

    //hrtime_t t_start, t_end;
    //t_start = gethrtime();
    dax_scan_value(*ctx, 0, &src, &dst, DAX_LT, &predicate);
    //t_end = gethrtime();
    //long long t_res = t_end - t_start;

    //FILE *pFile;
    //pFile = fopen("dax_results.txt", "a");
    //fprintf(pFile, "%lld\n", t_res);
    //fclose(pFile);
    //int remainder = (WORD_SIZE - ((col->end - col->start + 1) % WORD_SIZE)) % 64; 
    //printf("Count (orders): %lu -- DAX", scan_res.count - remainder);
    //printf("Took %llu ns!\n", t_res);

    uint64_t* bit_vector = (dataComp->getBitVector() + curPart * col->num_of_segments); 
    memcpy(bit_vector, dst.data, col->num_of_segments * 8);
    //int remainder = WORD_SIZE - ((col->end - col->start + 1) % WORD_SIZE); 
    //printf("Count: %lu, r: %d\n", scan_res.count - remainder, remainder);
    free(src.data);
    free(dst.data);
} 
#endif

void Query3::orderscan_sw(DataCompressor *dataComp, int curPart)
{ 
    table *compTable = dataComp->getTable();
    int ind = 4 + curPart * compTable->nb_columns;
    column *col = &(compTable->columns[ind]);

    uint64_t predicate = compTable->columns[4].keys["1996-01-10"];
    uint64_t data_vector = 0;
    uint64_t upper_bound = (0 << col->num_of_bits) | predicate; //Y vector 
    uint64_t mask = (0 << col->num_of_bits) | (uint64_t) (pow(2, col->num_of_bits) - 1);

    int i, j;
    for(i = 0; i < WORD_SIZE / (col->num_of_bits + 1) - 1; i++){
        upper_bound |= (upper_bound << (col->num_of_bits + 1));
        mask |= (mask << (col->num_of_bits + 1));
    }

    uint64_t cur_result;
    //int count = 0;
    int total_lines = col->num_of_bits + 1; 
    uint64_t local_res = 0;
    uint64_t* bit_vector = (dataComp->getBitVector() + curPart * col->num_of_segments); 
     
    //hrtime_t t_start, t_end;
    //t_start = gethrtime();

    /* LESS THAN OPERATOR */
    for(i = 0; i < col->num_of_segments; i++){
        for(j = 0; j < total_lines; j++){
            data_vector = col->compressed[i * total_lines + j];
            cur_result = data_vector ^ mask;
            cur_result += upper_bound;
            cur_result = cur_result & ~mask;
            //count += __builtin_popcountl(cur_result);
            local_res = local_res | (cur_result >> j); 
        }
	bit_vector[i] = local_res;
        local_res = 0;
    }
    
    //t_end = gethrtime();
    //long long t_res = t_end - t_start;

    //dataComp->scan_vector 
    //FILE *pFile;
    //pFile = fopen("sw_results.txt", "a");
    //fprintf(pFile, "%lld\n", t_res);
    //fclose(pFile);

    //int remainder = (WORD_SIZE - ((col->end - col->start + 1) % WORD_SIZE)) % 64; 
    //printf("Count (orders): %d -- SW %llu -- ", count - remainder, t_res);
    //printf("\n");
    //printf("Took %llu ns!\n", t_res);
}

#ifdef __sun 
void Query3::join_hw(DataCompressor *lineitemComp, DataCompressor *ordersComp, int curPart, dax_context_t **ctx)
{ 
    table *lineTable = lineitemComp->getTable();
    table *ordersTable = ordersComp->getTable();

    int ind = curPart * lineTable->nb_columns; //l_orderkey
    column *lokey_col = &(lineTable->columns[ind]);

    int start = lokey_col->start; 
    int end = lokey_col->end; 

    int num_of_els = end - start + 1;
    if(num_of_els % 64 != 0)
        num_of_els = ((num_of_els / 64) + 1) * 64;

    dax_vec_t src;
    dax_vec_t dst;
    dax_vec_t dst2;
    dax_vec_t bit_map;

    memset(&src, 0, sizeof(dax_vec_t));
    memset(&dst, 0, sizeof(dax_vec_t));
    memset(&dst2, 0, sizeof(dax_vec_t));
    memset(&bit_map, 0, sizeof(dax_vec_t));

    dax_result_t join_res;

    src.elements = num_of_els;
    src.format = DAX_BITS;
    src.elem_width = lokey_col->num_of_bits + 1;

    src.data = malloc(src.elements * src.elem_width);
    src.data = lokey_col->compressed;

    dst.elements = src.elements;
    dst.offset = 0;
    dst.format = DAX_BITS;
    dst.elem_width = 1;
    dst.data = memalign(8192, DAX_OUTPUT_SIZE(dst.elements, dst.elem_width));

    bit_map.elements = ordersTable->nb_lines;
    bit_map.format = DAX_BITS;
    bit_map.elem_width = 1;
    bit_map.data = malloc(bit_map.elements);
    bit_map.data = ordersComp->getBitVector();
    // src -- l_orderkey column from lineitem table
    // bit_map -- global bit vector result coming from the orders scan

    hrtime_t t_start, t_end;
    t_start = gethrtime();
    join_res = dax_translate(*ctx, 0, &src, &dst, &bit_map, src.elem_width);
    //join_res = dax_and(*ctx, 0, &dst, lineitemComp->getBitVector(), &dst2);
    t_end = gethrtime();
    long long t_res = t_end - t_start;

    //FILE *pFile;
    //pFile = fopen("dax_results.txt", "a");
    //fprintf(pFile, "%lld\n", t_res);
    //fclose(pFile);
    printf("Join between lineitem and orders took %llu ns!\n", t_res);
    printf("Count: %lu\n", join_res.count);

    free(src.data);
    free(dst.data);
    free(bit_map.data);
} 
#endif

void Query3::join_sw(){
}

void Query3::agg(table *compTable, uint64_t *bit_vector, int curPart, int compLines, int dataEnd, int numOfBits){
    std::unordered_map<uint64_t, std::tuple<int, uint64_t>>  local_ht;    
    //<(rf + ls), (l_q, l_ext)> 
    //Columns: l_quantity, l_extprice, l_discount, l_tax, l_rf, l_ls 
    vector<int> columns = {8, 9, 4, 5, 6, 7};
    vector<int> actual_columns;
    for(int colId : columns)
        actual_columns.push_back(colId + curPart * compTable->nb_columns);

    int vals_in_segment = 64; //This may change in the future, for now we have perfectly aligned cases.
    vector<int> ind_arr = Helper::createIndexArray(bit_vector, compLines, numOfBits, dataEnd, vals_in_segment);

    //ind_arr = Helper::createIndexArray(bit_vector, compLines, numOfBits);

    tuple <int, uint64_t> value = make_tuple(0, 0);
    //sum(l_quantity), sum(l_extprice)

    for(int curInd : ind_arr){
        uint64_t key = 0;
        int baseId = 0;
	//printf("%d ", curInd);
        for(int &colId : actual_columns){
            baseId = colId % compTable->nb_columns;
            column *baseCol = &(compTable->columns[baseId]);
            column *curCol = &(compTable->columns[colId]);
            if(baseId == 8){
                key = curCol->data[curInd];
                //printf("%d. %s | ", curInd, baseCol->dict[(uint32_t) key].c_str());
                key <<= 32;
            }
            else if(baseId == 9){
                key |= curCol->data[curInd];
                //printf("%s -> \n", baseCol->dict[(uint32_t) key].c_str());
                if(local_ht.find(key) == local_ht.end()){
                    local_ht.emplace(key, value);
                }
            }
            else if(baseId == 4){
                if(local_ht.find(key) != local_ht.end()){
                	get<0>(local_ht.at(key)) += baseCol->i_dict[curCol->data[curInd]];
		}
                //printf(" <(%d,%d), ", baseCol->i_dict[curCol->data[curInd]], get<0>(local_ht[key]));
            }
            else if(baseId == 5){
                get<1>(local_ht.at(key)) += (uint64_t) baseCol->d_dict[curCol->data[curInd]];
                //printf("(%d, %d)> \n", baseCol->d_dict[curCol->data[curInd]], get<1>(local_ht[key]));
            }
        }
    }
	printf("\n");

    for(auto &curr : local_ht){
        uint64_t key = curr.first;
        string first_part = compTable->columns[8].dict[(uint32_t) (key >> 32)];
        string second_part = compTable->columns[9].dict[(uint32_t) key];

        printf("(%s | %s) -> <%d, %lu>\n", first_part.c_str(), second_part.c_str(), get<0>(curr.second), get<1>(curr.second));
    }
}
