#ifndef __agg_api_h__
#define __agg_api_h__

#ifdef __sun
extern "C" {
#include "/usr/include/dax.h"
}
#endif

#include "util/Types.h"
#include "util/Query.h"
#include "util/Helper.h"

#include "data/DataLoader.h"
#include "log/Result.h"
#include "thread/Thread.h"

#include "api/JoinApi.h"

class AggApi {
private:
	ScanApi *factScan;
	std::vector<JoinApi*> joins;

	vector<vector<vector<int>>> aggKeyMap;

	void initializeAgg() {
		vector<int> key_sizes {1, 1, 1};

		int curDim = 0;
		for(JoinApi *curJoin : joins){
			table_meta &t_meta = (curJoin->dimensionScan->getBaseTable()->t_meta);
			if(t_meta.hasAggKey){
				int key_size = t_meta.groupByKeys.size();
				key_sizes[curDim++] = key_size;
			}
		}
		cout << key_sizes[0] << " -- " << key_sizes[1] << " -- " << key_sizes[2] << endl;

		vector<int> vec3(key_sizes[2], -1); //aggKeyMap1D
		vector<vector<int>> vec2(key_sizes[1], vec3); //aggKeyMap2D
		aggKeyMap.resize(key_sizes[0], vec2);

		int id_1D = 0;
		int id_2D = 0;
		int id_3D = 0;
		for (int i = 0; i < key_sizes[0]; i++) {
			aggKeyMap[i][0][0] = id_1D++;
			for (int j = 0; j < key_sizes[1]; j++) {
				aggKeyMap[i][j][0] = id_2D++;
				for (int k = 0; k < key_sizes[2]; k++) {
					aggKeyMap[i][j][k] = id_3D++;
				}
			}
		}
	}

public:
	AggApi(ScanApi *factScan, std::vector<JoinApi*> &joins) {
		this->factScan = factScan;
		this->joins = joins;

		initializeAgg();
	}

	~AggApi() {
	}

	void printBitVector(uint64_t cur_result, int segId) {
		//print bit vals
		printf("%d: ", segId);
		for (int j = 0; j < 64; j++) {
			printf("%lu|", (cur_result & 1));
			cur_result >>= 1;
		}
		printf("\n");
	}

	table* FactTable(){
		return this->factScan->getBaseTable();
	}

	int TotalJoins(){
		return this->joins.size();
	}

	std::vector<JoinApi*> Joins(){
		return joins;
	}

	void agg(Node<Query>* node, Result *result);
	void agg_q1(Node<Query>* node, Result *result);
	void agg_q4(Node<Query>* node, Result *result);
};

#endif
