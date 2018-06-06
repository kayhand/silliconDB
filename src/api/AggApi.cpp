#include "AggApi.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <sys/time.h>

void AggApi::agg(Node<Query>* node, Result *result){ 
    int curPart = node->value.getPart();
    
    //int lokey_ind = curPart * j1->num_of_columns;
    //column *lo_okey = &(j1->factCols[lokey_ind]);

    int ckey_id = 2;
    int ckey_ind = ckey_id + curPart * j1->num_of_columns;
    column *lo_ckey = &(j1->factCols[ckey_ind]);

    int rev_id = 12;
    int rev_ind = rev_id + curPart * j1->num_of_columns;
    column *lo_revenue = &(j1->factCols[rev_ind]);

    /*
    int s = lo_ckey->c_meta.start;
    int e = lo_ckey->c_meta.end;
    printf("\nAgg. part %d \n", curPart);
    printf("s:%d, e:%d -- %d, %d, %d\n", s, e, lo_okey->encoder.num_of_bits, lo_ckey->encoder.num_of_bits, lo_revenue->encoder.num_of_bits);
    */

    int num_of_segs = lo_ckey->c_meta.num_of_segments;

    uint64_t* bit_vector = (uint64_t *) j1->getJoinBitVector(curPart);

    //uint32_t *lo_okey_comp = (uint32_t *) lo_okey->data;
    //uint32_t orderkey;

    uint32_t *lo_rev_comp = (uint32_t *) lo_revenue->data;
    uint32_t lo_rev;

    uint32_t *lo_ckey_comp = (uint32_t *) lo_ckey->data;
    uint32_t c_nation;

 /*   if(curPart == 0){
        for(int i = 0; i < 64; i++){
	    printf("%i) %u->%u\n", i, lo_rev_comp[i], lo_revenue->encoder.i_dict[lo_rev_comp[i]]);
	}
    }
 */     
    unordered_map<uint32_t, uint32_t> *groupByMap = &(j1->dimensionScan->getBaseTable()->t_meta.groupByMap);

    /*unordered_map<uint32_t, string> *s_dict = &(c_nation_col->encoder.dict);
    for(auto &curr : *s_dict){
        printf("%u -> %s\n", curr.first, curr.second.c_str());
    }*/

    //bool first = true;
    unordered_map<uint32_t, uint64_t> local_agg;
    #ifdef __sun 
    hrtime_t t_start, t_end;
    t_start = gethrtime();
    #endif
    for(int i = 0; i < num_of_segs; i++){
        uint64_t cur_vec = bit_vector[i];
	int data_ind = 0;
	for(; cur_vec;){
	    data_ind = 64 - __builtin_ffsl(cur_vec);
            data_ind = data_ind + i * 64;
            lo_rev = lo_revenue->encoder.i_dict[lo_rev_comp[data_ind]];
            //orderkey = lo_okey->encoder.i_dict[lo_okey_comp[data_ind]];
            c_nation = groupByMap->at(lo_ckey_comp[data_ind]);
	    local_agg[c_nation] += lo_rev;
	    /*if(c_nation != 4 && c_nation != 9 && c_nation != 10 && c_nation != 13 && c_nation != 24){
		if(first){
		    first = false;
                    printBitVector(cur_vec, i + 1);
		}
	        printf("%d(%d)|%d|: ", data_ind - i * 64 + 1, data_ind + 1, s + data_ind + 1);
	        printf("%u. %u -> %u\n", orderkey, c_nation, lo_rev); 
	    }*/
	    cur_vec &= (cur_vec - 1);
        }
	//first = true;
    }

    #ifdef __sun
    t_end = gethrtime();
    #endif
    result->addRuntime(AGG, make_tuple(t_start, t_end, -1, curPart));

    table *cust_table = j1->dimensionScan->getBaseTable();
    column *c_nation_col = &(cust_table->columns[cust_table->t_meta.groupByColId]);
    unordered_map<uint32_t, string> *s_dict = &(c_nation_col->encoder.dict);
    for(auto curPair : local_agg){
        string s_key = s_dict->at(curPair.first);
	result->addAggResult(make_pair(s_key, curPair.second));
    }
} 
