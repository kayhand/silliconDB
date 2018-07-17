#include "AggApi.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <sys/time.h>


void AggApi::agg(Node<Query>* node, Result *result) {
	int curPart = node->value.getPart();

	vector<uint64_t*> bit_results;
	map<int, uint32_t*> lo_key_comps;

	int rev_id = 12;
	int rev_ind = rev_id + curPart * FactTable()->t_meta.num_of_columns;
	column *lo_revenue = &(FactTable()->columns[rev_ind]);
	int num_of_segs = lo_revenue->c_meta.num_of_segments;

	uint32_t *lo_rev_comp = (uint32_t *) lo_revenue->data;
	uint32_t lo_rev;

	map<int, unordered_map<uint32_t, uint32_t>*> keyMaps;
	for(JoinApi *curJoin : joins){
		ScanApi *dimScan = curJoin->dimensionScan;

		if(dimScan->HasFilter())
			bit_results.push_back((uint64_t *) curJoin->getJoinBitVector(curPart));

		JOB_TYPE &s_type = dimScan->Type();
		if(dimScan->HasAggKey()){
			printf("Join: %d\n", s_type);
			int key_ind = curJoin->joinColId + curPart * FactTable()->t_meta.num_of_columns;
			column *lo_key = &(FactTable()->columns[key_ind]);
			lo_key_comps[s_type] = lo_key->data;
			keyMaps[s_type] = &(dimScan->getBaseTable()->t_meta.groupByMap);
		}
	}

	if(factScan->HasFilter()){
		bit_results.push_back((uint64_t *) factScan->getFilterBitVector(curPart));
		if(factScan->SubScan() != NULL){
			bit_results.push_back((uint64_t *) factScan->SubScan()->getFilterBitVector(curPart));
		}
	}

	std::map<uint32_t, uint64_t> local_agg;
	vector<int> curKeys {0, 0, 0};
	int aggKey;
	hrtime_t t_start, t_end;
	t_start = gethrtime();
	for (int i = 0; i < num_of_segs; i++) {
		uint64_t cur_vec = bit_results[0][i];
		for(auto bit_res : bit_results){
			//printf("%lu -- ", bit_res[i]);
			cur_vec &= bit_res[i];
		}
		//printf("%lu\n\n", cur_vec);
		//uint64_t cur_vec = bit_results[0][i] & bit_results[1][i] & bit_results[2][i];

		int data_ind = 0;
		for (; cur_vec;) {
			data_ind = 64 - __builtin_ffsl(cur_vec);
			data_ind = data_ind + i * 64;

			int ind = 0;
			//composite aggregation key
			for(auto &curMap : keyMaps){
				int job_id = curMap.first;
				curKeys[ind++] = curMap.second->at(lo_key_comps[job_id][data_ind]);
			}
			aggKey = aggKeyMap[curKeys[0]][curKeys[1]][curKeys[2]];

			//aggregation value
			lo_rev = lo_revenue->encoder.i_dict[lo_rev_comp[data_ind]];

			local_agg[aggKey] += lo_rev;
			cur_vec &= (cur_vec - 1);
		}
	}
	t_end = gethrtime();
	result->addRuntime(false, JOB_TYPE::AGG, make_tuple(t_start, t_end, -1, curPart));

	for (auto curPair : local_agg)
		result->addAggResult(make_pair(curPair.first, curPair.second));
}

void AggApi::agg_q1(Node<Query>* node, Result *result) {
	int curPart = node->value.getPart();

	vector<uint64_t*> bit_results;
	map<int, uint32_t*> lo_key_comps;

	int ext_p_id = 9;
	int ext_p_ind = ext_p_id + curPart * FactTable()->t_meta.num_of_columns;
	column *lo_ext_p = &(FactTable()->columns[ext_p_ind]);

	uint32_t *lo_ext_p_comp = (uint32_t *) lo_ext_p->data;
	uint32_t lo_ext_price;

	int disc_id = 11;
	int disc_ind = disc_id + curPart * FactTable()->t_meta.num_of_columns;
	column *lo_disc = &(FactTable()->columns[disc_ind]);

	uint32_t *lo_disc_comp = (uint32_t *) lo_disc->data;
	uint32_t lo_discount;

	int num_of_segs = lo_ext_p->c_meta.num_of_segments;
	for(JoinApi *curJoin : joins){
		bit_results.push_back((uint64_t *) curJoin->getJoinBitVector(curPart));
	}

	if(factScan->HasFilter()){
		bit_results.push_back((uint64_t *) factScan->getFilterBitVector(curPart));
		if(factScan->SubScan() != NULL)
			bit_results.push_back((uint64_t *) factScan->SubScan()->getFilterBitVector(curPart));
	}

	uint64_t local_sum = 0ul;
	hrtime_t t_start, t_end;
	t_start = gethrtime();
	for (int i = 0; i < num_of_segs; i++) {
		uint64_t cur_vec = bit_results[0][i];
		for(auto bit_res : bit_results){
			cur_vec &= bit_res[i];
		}

		int data_ind = 0;
		for (; cur_vec;) {
			data_ind = 64 - __builtin_ffsl(cur_vec);
			data_ind = data_ind + i * 64;

			//aggregation value
			lo_ext_price = lo_ext_p->encoder.i_dict[lo_ext_p_comp[data_ind]];
			lo_discount = lo_disc->encoder.i_dict[lo_disc_comp[data_ind]];

			local_sum = local_sum + lo_ext_price * lo_discount;
			cur_vec &= (cur_vec - 1);
		}
	}
	t_end = gethrtime();
	result->addRuntime(false, JOB_TYPE::AGG, make_tuple(t_start, t_end, -1, curPart));

	result->addAggResult(make_pair(0, local_sum));
}


void AggApi::agg_q4(Node<Query>* node, Result *result) {
	int curPart = node->value.getPart();

	vector<uint64_t*> bit_results;
	map<int, uint32_t*> lo_key_comps;

	int rev_id = 12;
	int rev_ind = rev_id + curPart * FactTable()->t_meta.num_of_columns;
	column *lo_revenue = &(FactTable()->columns[rev_ind]);

	int supp_id = 13;
	int supp_ind = supp_id + curPart * FactTable()->t_meta.num_of_columns;
	column *lo_supp_cost = &(FactTable()->columns[supp_ind]);

	int num_of_segs = lo_revenue->c_meta.num_of_segments;
	cout << num_of_segs << " " << lo_supp_cost->c_meta.num_of_segments;

	uint32_t *lo_rev_comp = (uint32_t *) lo_revenue->data;
	uint32_t lo_rev;

	uint32_t *lo_supp_comp = (uint32_t *) lo_supp_cost->data;
	uint32_t lo_supp;

	map<int, unordered_map<uint32_t, uint32_t>*> keyMaps;
	for(JoinApi *curJoin : joins){
		ScanApi *dimScan = curJoin->dimensionScan;

		if(dimScan->HasFilter())
			bit_results.push_back((uint64_t *) curJoin->getJoinBitVector(curPart));

		JOB_TYPE &s_type = dimScan->Type();
		if(dimScan->HasAggKey()){
			//printf("Join: %d\n", s_type);
			int key_ind = curJoin->joinColId + curPart * FactTable()->t_meta.num_of_columns;
			column *lo_key = &(FactTable()->columns[key_ind]);
			lo_key_comps[s_type] = lo_key->data;
			keyMaps[s_type] = &(dimScan->getBaseTable()->t_meta.groupByMap);
		}
	}

	if(factScan->HasFilter()){
		bit_results.push_back((uint64_t *) factScan->getFilterBitVector(curPart));
		if(factScan->SubScan() != NULL){
			bit_results.push_back((uint64_t *) factScan->SubScan()->getFilterBitVector(curPart));
		}
	}

	std::map<uint32_t, uint64_t> local_agg;
	vector<int> curKeys {0, 0, 0};
	int aggKey;
	hrtime_t t_start, t_end;
	t_start = gethrtime();
	for (int i = 0; i < num_of_segs; i++) {
		uint64_t cur_vec = bit_results[0][i];
		for(auto bit_res : bit_results){
			cur_vec &= bit_res[i];
		}

		int data_ind = 0;
		for (; cur_vec;) {
			data_ind = 64 - __builtin_ffsl(cur_vec);
			data_ind = data_ind + i * 64;

			int ind = 0;
			//composite aggregation key
			for(auto &curMap : keyMaps){
				int job_id = curMap.first;
				curKeys[ind++] = curMap.second->at(lo_key_comps[job_id][data_ind]);
			}
			aggKey = aggKeyMap[curKeys[0]][curKeys[1]][curKeys[2]];

			//aggregation value
			lo_rev = lo_revenue->encoder.i_dict[lo_rev_comp[data_ind]];
			lo_supp = lo_supp_cost->encoder.i_dict[lo_supp_comp[data_ind]];

			local_agg[aggKey] += lo_rev - lo_supp;
			cur_vec &= (cur_vec - 1);
		}
	}
	t_end = gethrtime();
	result->addRuntime(false, JOB_TYPE::AGG, make_tuple(t_start, t_end, -1, curPart));

	for (auto curPair : local_agg)
		result->addAggResult(make_pair(curPair.first, curPair.second));
}

/*
void AggApi::agg(Node<Query>* node, Result *result) {
	int curPart = node->value.getPart();

	int ckey_id = joins[0]->joinColId;
	int ckey_ind = ckey_id + curPart * FactTable()->t_meta.num_of_columns;
	column *lo_ckey = &(FactTable()->columns[ckey_ind]);

	int skey_id = joins[1]->joinColId;
	int skey_ind = skey_id + curPart * FactTable()->t_meta.num_of_columns;
	column *lo_skey = &(FactTable()->columns[skey_ind]);

	int dkey_id = joins[2]->joinColId;
	int dkey_ind = dkey_id + curPart * FactTable()->t_meta.num_of_columns;
	column *lo_dkey = &(FactTable()->columns[dkey_ind]);

	int rev_id = 12;
	int rev_ind = rev_id + curPart * FactTable()->t_meta.num_of_columns;
	column *lo_revenue = &(FactTable()->columns[rev_ind]);

	int num_of_segs = lo_dkey->c_meta.num_of_segments;


	uint64_t* bit_vector_j1 = (uint64_t *) joins[0]->getJoinBitVector(curPart);
	uint64_t* bit_vector_j2 = (uint64_t *) joins[1]->getJoinBitVector(curPart);
	uint64_t* bit_vector_j3 = (uint64_t *) joins[2]->getJoinBitVector(curPart);
	vector<uint64_t*> bit_results {bit_vector_j1, bit_vector_j2, bit_vector_j3};


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

	//vector<int> keys = {1, 2, 3, 4, 5, 6};
	//for(int key : keys)
		//local_agg[key] = 0ul;


	int aggKey;
	hrtime_t t_start, t_end;
	t_start = gethrtime();
	for (int i = 0; i < num_of_segs; i++) {
		uint64_t cur_vec = bit_vector_j1[i] & bit_vector_j2[i] & bit_vector_j3[i];
		//uint64_t cur_vec = bit_results[0][i] & bit_results[1][i] & bit_results[2][i];

		int data_ind = 0;
		for (; cur_vec;) {
			data_ind = 64 - __builtin_ffsl(cur_vec);
			data_ind = data_ind + i * 64;

			//composite aggregation key
			c_nation = groupByMap_cust->at(lo_ckey_comp[data_ind]);
			s_nation = groupByMap_supp->at(lo_skey_comp[data_ind]);
			d_year = groupByMap_date->at(lo_dkey_comp[data_ind]);

			aggKey = aggKeyMap[c_nation][s_nation][d_year];
			//aggKey = d_year;

			//aggregation value
			lo_rev = lo_revenue->encoder.i_dict[lo_rev_comp[data_ind]];


			local_agg[aggKey] += lo_rev;
			cur_vec &= (cur_vec - 1);
		}
	}
	t_end = gethrtime();
	result->addRuntime(false, JOB_TYPE::AGG, make_tuple(t_start, t_end, -1, curPart));

	for (auto curPair : local_agg) {
		//string s_key = s_dict->at(curPair.first);
		//printf("%u, %lu\n", curPair.first, curPair.second);
		result->addAggResult(make_pair(curPair.first, curPair.second));
	}
}

*/
