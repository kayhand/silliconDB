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
	table *factTable;
	std::vector<JoinApi*> joins;

	//vector<vector<int>> aggKeyMap2D;
	vector<vector<vector<int>>> aggKeyMap3D;

	void initializeAgg() {
		table_meta *t1 = &(joins[0]->dimensionScan->getBaseTable()->t_meta); //customer
		table_meta *t2 = &(joins[1]->dimensionScan->getBaseTable()->t_meta); //supplier
		table_meta *t3 = &(joins[2]->dimensionScan->getBaseTable()->t_meta); //supplier

		int keys1 = t1->groupByKeys.size();
		int keys2 = t2->groupByKeys.size();
		int keys3 = t3->groupByKeys.size();

		vector<int> vec3(keys3, -1);
		vector<vector<int>> vec2(keys2, vec3);

		//aggKeyMap2D.resize(keys1, vec3);
		aggKeyMap3D.resize(keys1, vec2);

		//int id_2D = 0;
		int id_3D = 0;
		for (int i = 0; i < keys1; i++) {
			for (int j = 0; j < keys2; j++) {
				//aggKeyMap2D[i][j] = id_2D++;
				for (int k = 0; k < keys3; k++) {
					aggKeyMap3D[i][j][k] = id_3D++;
				}
			}
		}

		/*std::cout << "Customer nation keys: " << endl;
		for (auto &keyValue : t1->groupByKeys) {
			int key = keyValue.first;
			std::cout << key << " ";
		}
		std::cout << std::endl;
		std::cout << std::endl;*/

		/*std::cout << "Composite keys mapping" << std::endl;
		 for (int i = 0; i < keys1; i++) {
		 	 for (int j = 0; j < keys2; j++) {
		 	 	 for (int k = 0; k < keys3; k++) {
		 	 	 	 cout << "(" << i << ", " << j << ", " << k << ") -> " << aggKeyMap[i][j][k] << endl;
		 	 	 }
		 	 }
		 }*/
	}

public:
	AggApi(table *factTable, std::vector<JoinApi*> &joins) {
		this->factTable = factTable;
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
		return this->factTable;
	}

	int TotalJoins(){
		return this->joins.size();
	}

	void agg(Node<Query>* node, Result *result);
};

#endif
