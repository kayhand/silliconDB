
#include "WorkItem.h"
#include <cstring>

//WorkItem::WorkItem()
//{
//}

//WorkItem::~WorkItem()
//{
//}

#ifdef __sun 
void WorkItem::scan(DataCompressor *dataComp, int curPart)
{ 
    dax_context_t *ctx;
    dax_status_t status = dax_thread_init(1, 1, 0, NULL, &ctx);
    printf("Status %d\n", status);

    table *compTable = dataComp->getTable();
    uint64_t comp_predicate = 0;
    comp_predicate = compTable->columns[10].keys["1996-01-10"];

    int ind = 10 + curPart * compTable->nb_columns;
    uint64_t *comp_col = compTable->columns[ind].compressed;

    dax_int_t predicate;
    dax_vec_t src;
    dax_vec_t dst;

    memset(&predicate, 0, sizeof(dax_int_t));
    memset(&src, 0, sizeof(dax_vec_t));
    memset(&dst, 0, sizeof(dax_vec_t));

    int start = dataComp->getPartitioner()->getMap().at(curPart).first;
    int end = dataComp->getPartitioner()->getMap().at(curPart).second;

    printf("s: %d, e: %d\n", start, end);
    src.elements = end - start + 1;
    src.format = DAX_BITS;
    src.elem_width = compTable->columns[10].compression_scheme;

    src.data = malloc(src.elements * src.elem_width * 8);
    src.data = comp_col;

    predicate.format = DAX_BITS;
    predicate.elem_width = src.elem_width;
    predicate.dword[2] = comp_predicate;

    dst.elements = src.elements;
    dst.offset = 0;
    dst.format = DAX_BITS;
    dst.elem_width = 1;
    dst.data = memalign(8192, DAX_OUTPUT_SIZE(dst.elements, dst.elem_width));
    scan_res = dax_scan_value(ctx, 0, &src, &dst, DAX_LT, &predicate);

    //printf("Result status: %d\n", res.status);
    printf("Result count: %lu\n", scan_res.count);
    agg(*compTable, dst, curPart, dataComp->getCompLines(curPart), start);
    free(src.data);
    free(dst.data);
} 

#else
void WorkItem::scan(DataCompressor *dataComp, int curPart)
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
    int elements_checked = -1;
    uint64_t* bit_vector = new uint64_t[col->num_of_segments];
    //long long res;
    //res = tick();
    for(i = 0; i < col->num_of_segments; i++){
        for(j = 0; j < total_lines; j++){
            cur_result = col->compressed[i * total_lines + j];
            cur_result = cur_result ^ mask;
            cur_result += upper_bound;
            cur_result = cur_result & ~mask;
            count += __builtin_popcountl(cur_result);
	    elements_checked += col->num_of_codes; 
            local_res = local_res | (cur_result >> j); 
        }
	bit_vector[i] = local_res;
        local_res = 0;
    }
    printf("\n");
    printf("Count: %d\n", count);
    agg(compTable, bit_vector, curPart, dataComp->getCompLines(curPart), dataComp->getPartitioner()->getMap().at(curPart).first, dataComp->getPartitioner()->getMap().at(curPart).second);
    //int remainder = numberOfElements - (numberOfSegments - 1) * (numberOfElements / (numberOfSegments - 1));
    int remainder = WORD_SIZE - ((col->end - col->start + 1) % WORD_SIZE); 
    printf("Remainder %d\n", remainder);
    printf("Start: %d - End: %d\n", col->start, col->end);
    //printResultVector(global_res, numberOfSegments, WORD_SIZE - (num_of_bits + 1) * (WORD_SIZE / (num_of_bits + 1)), remainder);
    delete[] bit_vector;
}
#endif

void WorkItem::agg(table *compTable, uint64_t *bit_vector, int curPart, int compLines, int dataStart, int dataEnd){
    std::unordered_map<uint64_t, std::tuple<int, uint64_t>>  local_ht;    
    //<(rf + ls), (l_q, l_ext)> 
    //Columns: l_quantity, l_extprice, l_discount, l_tax, l_rf, l_ls 
    vector<int> columns = {8, 9, 4, 5, 6, 7};
    vector<int> actual_columns;
    for(int colId : columns)
        actual_columns.push_back(colId + curPart * compTable->nb_columns);

    vector<int> ind_arr = Helper::createIndexArray(bit_vector, dataStart, compLines);
    //for(int curInd : actual_columns)
        //printf("%d ", curInd);
    //printf("\n");

    tuple <int, uint64_t> value = make_tuple(0, 0);
    local_ht.reserve(100);
    //sum(l_quantity), sum(l_extprice)

    for(int curInd : ind_arr){
        uint64_t key = 0;
        int baseId = 0;
	if(curInd > dataEnd)
	    break;
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

    for(auto &curr : local_ht){
        uint64_t key = curr.first;
        string first_part = compTable->columns[8].dict[(uint32_t) (key >> 32)];
        string second_part = compTable->columns[9].dict[(uint32_t) key];

        printf("(%s | %s) -> <%d, %lu>\n", first_part.c_str(), second_part.c_str(), get<0>(curr.second), get<1>(curr.second));
    }
}
