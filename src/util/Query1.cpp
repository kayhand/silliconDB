
#include "Query1.h"
#include <cstring>
#include <sys/time.h>

Query1::Query1()
{
}

Query1::Query1(bool type)
{
    this->type = type;
}

Query1::~Query1()
{
}

#ifdef __sun 
void Query1::scan_hw(DataCompressor *dataComp, int curPart, dax_context_t **ctx)
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

    hrtime_t t_start, t_end;
    t_start = gethrtime();
    scan_res = dax_scan_value(ctx, 0, &src, &dst, DAX_LT, &predicate);
    t_end = gethrtime();
    long long t_res = t_end - t_start;
    if(scan_res.status != 0)
        printf("Dax Error! %d\n", scan_res.status);

    //FILE *pFile;
    //pFile = fopen("dax_results.txt", "a");
    //fprintf(pFile, "%lld\n", t_res);
    //fclose(pFile);
    //int remainder = WORD_SIZE - ((col->end - col->start + 1) % WORD_SIZE); 
    //printf("DAX Scan took %llu ns!\n", t_res);
    //printf("Count: %lu\n", scan_res.count - remainder);

    uint64_t* bit_vector = (dataComp->getBitVector() + curPart * col->num_of_segments);
    memcpy(bit_vector, dst.data, col->num_of_segments * 8);

    free(src.data);
    free(dst.data);
} 
#endif

void Query1::scan_sw(DataCompressor *dataComp, int curPart)
{ 
    table *compTable = dataComp->getTable();
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
    uint64_t* bit_vector = (dataComp->getBitVector() + curPart * col->num_of_segments);
    //long long res;
    //res = tick();
        
    hrtime_t t_start, t_end;
    t_start = gethrtime();
    for(i = 0; i < col->num_of_segments; i++){
        for(j = 0; j < total_lines; j++){
            cur_result = col->compressed[i * total_lines + j];
            cur_result = cur_result ^ mask;
            cur_result += upper_bound;
            cur_result = cur_result & ~mask;
            count += __builtin_popcountl(cur_result);
            local_res = local_res | (cur_result >> j); 
        }
	bit_vector[i] = local_res;
        local_res = 0;
    }
    t_end = gethrtime();
    long long t_res = t_end - t_start;

    //FILE *pFile;
    //pFile = fopen("sw_results.txt", "a");
    //fprintf(pFile, "%lld\n", t_res);
    //fclose(pFile);

    //printf("SW Scan took %llu ns!\n", t_res);
    //int remainder = WORD_SIZE - ((col->end - col->start + 1) % WORD_SIZE); 
    //printf("Count: %d\n", count - remainder);
    //agg(compTable, bit_vector, curPart, dataComp->getCompLines(curPart), col->end - col->start, 0);
}

void Query1::agg(DataCompressor *dataComp, int curPart, int dataEnd, int numOfBits){
    table *compTable = dataComp->getTable();
    int ind = 10 + curPart * compTable->nb_columns;

    column *col = &(compTable->columns[ind]);
    int dataEnd = col->end - col->start;
    int compLines = dataComp->getCompLines(curPart);
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
