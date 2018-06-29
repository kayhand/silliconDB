#include "AggApi.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <sys/time.h>

void AggApi::agg(Node<Query>* node, Result *result) {
	int curPart = node->value.getPart();

	int ckey_id = joins[0]->joinColId;
	int ckey_ind = ckey_id + curPart * factTable->t_meta.num_of_columns;
	column *lo_ckey = &(factTable->columns[ckey_ind]);

	int skey_id = joins[1]->joinColId;
	int skey_ind = skey_id + curPart * factTable->t_meta.num_of_columns;
	column *lo_skey = &(factTable->columns[skey_ind]);

	int dkey_id = joins[2]->joinColId;
	int dkey_ind = dkey_id + curPart * factTable->t_meta.num_of_columns;
	column *lo_dkey = &(factTable->columns[dkey_ind]);

	int rev_id = 12;
	int rev_ind = rev_id + curPart * factTable->t_meta.num_of_columns;
	column *lo_revenue = &(factTable->columns[rev_ind]);

	int num_of_segs = lo_dkey->c_meta.num_of_segments;

	uint64_t* bit_vector_j1 = (uint64_t *) joins[0]->getJoinBitVector(curPart);
	uint64_t* bit_vector_j2 = (uint64_t *) joins[1]->getJoinBitVector(curPart);
	uint64_t* bit_vector_j3 = (uint64_t *) joins[2]->getJoinBitVector(curPart);

	uint32_t *lo_ckey_comp = (uint32_t *) lo_ckey->data;
	uint32_t c_nation;

	uint32_t *lo_skey_comp = (uint32_t *) lo_skey->data;
	uint32_t s_nation;

	uint32_t *lo_dkey_comp = (uint32_t *) lo_dkey->data;
	uint32_t d_year;

	uint32_t *lo_rev_comp = (uint32_t *) lo_revenue->data;
	uint32_t lo_rev;

	unordered_map<uint32_t, uint32_t> *groupByMap_cust =
			&(joins[0]->dimensionScan->getBaseTable()->t_meta.groupByMap);
	unordered_map<uint32_t, uint32_t> *groupByMap_supp =
			&(joins[1]->dimensionScan->getBaseTable()->t_meta.groupByMap);
	unordered_map<uint32_t, uint32_t> *groupByMap_date =
				&(joins[2]->dimensionScan->getBaseTable()->t_meta.groupByMap);

	std::map<uint32_t, uint64_t> local_agg;

	//bool first = true;
	/*vector<int> keys = { 833, 834, 835, 836, 837, 838, 873, 874, 875, 876, 877,
			878, 881, 882, 883, 884, 885, 886, 905, 906, 907, 908, 909, 910,
			993, 994, 995, 996, 997, 998, 1833, 1834, 1835, 1836, 1837, 1838,
			1873, 1874, 1875, 1876, 1877, 1878, 1881, 1882, 1883, 1884, 1885,
			1886, 1905, 1906, 1907, 1908, 1909, 1910, 1993, 1994, 1995, 1996,
			1997, 1998, 2033, 2034, 2035, 2036, 2037, 2038, 2073, 2074, 2075,
			2076, 2077, 2078, 2081, 2082, 2083, 2084, 2085, 2086, 2105, 2106,
			2107, 2108, 2109, 2110, 2193, 2194, 2195, 2196, 2197, 2198, 2633,
			2634, 2635, 2636, 2637, 2638, 2673, 2674, 2675, 2676, 2677, 2678,
			2681, 2682, 2683, 2684, 2685, 2686, 2705, 2706, 2707, 2708, 2709,
			2710, 2793, 2794, 2795, 2796, 2797, 2798, 4833, 4834, 4835, 4836,
			4837, 4838, 4873, 4874, 4875, 4876, 4877, 4878, 4881, 4882, 4883,
			4884, 4885, 4886, 4905, 4906, 4907, 4908, 4909, 4910, 4993, 4994,
			4995, 4996, 4997, 4998 };*/
	//vector<int> keys = {1, 2, 3, 4, 5, 6};
	//for(int key : keys)
		//local_agg[key] = 0ul;

	int aggKey;
	hrtime_t t_start, t_end;
	t_start = gethrtime();
	for (int i = 0; i < num_of_segs; i++) {
		uint64_t cur_vec = bit_vector_j1[i] & bit_vector_j2[i] & bit_vector_j3[i];
		//uint64_t cur_vec = bit_vector_j3[i];
		int data_ind = 0;
		for (; cur_vec;) {
			data_ind = 64 - __builtin_ffsl(cur_vec);
			data_ind = data_ind + i * 64;

			//composite aggregation key
			c_nation = groupByMap_cust->at(lo_ckey_comp[data_ind]);
			s_nation = groupByMap_supp->at(lo_skey_comp[data_ind]);
			d_year = groupByMap_date->at(lo_dkey_comp[data_ind]);
			aggKey = aggKeyMap3D[c_nation][s_nation][d_year];
			//aggKey = d_year;

			//aggregation value
			lo_rev = lo_revenue->encoder.i_dict[lo_rev_comp[data_ind]];

			/*if (local_agg.find(aggKey) == local_agg.end()) {
				if (first) {
					first = false;
					printf("====Partition: %d\n====", curPart);
					printf("j1: ");
					printBitVector(bit_vector_j1[i], i + 1);
					printf("j2: ");
					printBitVector(bit_vector_j2[i], i + 1);
					printf("j3: ");
					printBitVector(bit_vector_j3[i], i + 1);
				}
			}*/

			local_agg[aggKey] += lo_rev;
			cur_vec &= (cur_vec - 1);
		}
	}
	t_end = gethrtime();
	result->addRuntime(false, JOB_TYPE::AGG, make_tuple(t_start, t_end, -1, curPart));

	/*table *cust_table = joins[0]->dimensionScan->getBaseTable();
	 column *c_nation_col =
	 &(cust_table->columns[cust_table->t_meta.groupByColId]);
	 unordered_map<uint32_t, string> *s_dict = &(c_nation_col->encoder.dict);*/
	for (auto curPair : local_agg) {
		//string s_key = s_dict->at(curPair.first);
		//printf("%u, %lu\n", curPair.first, curPair.second);
		result->addAggResult(make_pair(curPair.first, curPair.second));
	}
}
