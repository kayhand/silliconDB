#ifndef __result_h__
#define __result_h__

#include <algorithm>
#include <string.h>

typedef long long unsigned int timestamp;
typedef std::tuple<timestamp, timestamp> time_pair;
typedef std::tuple<timestamp, timestamp, int, int> time_tuple;

class Result {
public:
	Result() {
		printf("Hello!\n");
	};
	~Result() {}

	void addRuntime(bool isDax, JOB_TYPE j_type, time_tuple t_pair) {
		if (j_type <= P_SCAN) {
			if (isDax)
				dax_scan_runtimes.push_back(t_pair);
			else
				sw_scan_runtimes.push_back(t_pair);
		} else if (j_type <= LD_JOIN) {
			if (isDax)
				dax_join_runtimes.push_back(t_pair);
			else
				sw_join_runtimes.push_back(t_pair);
		} else if (j_type == AGG)
			agg_runtimes.push_back(t_pair);
	}

	//T tuple: <int, JOB_TYPE>
	template<typename T>
	void addCountResult(T tuple) {
		job_counts.push_back(tuple);
	}

	//T tuple: <key_type, value_type>
	template<typename T>
	void addAggResult(T tuple) {
		local_agg_results.push_back(tuple);
	}

	void mergeCountResults(map<int, int> &count_results) {
		int j_type;
		for (auto val : job_counts) {
			j_type = get<0>(val);
			count_results[j_type] += get<1>(val);
		}
	}

	template<typename Key, typename Value>
	void mergeAggResults(map<Key, Value> &merged_agg_results) {
		for (auto val : local_agg_results) {
			Key key = get < 0 > (val);
			merged_agg_results[key] += get < 1 > (val);
		}
	}

	static void writeCountsToFile(map<int, int> &merged_counts){
		FILE *file_ptr = fopen("count_result.txt", "a");

		std::unordered_map<int, std::string> job_names {
				{LO_SCAN, "lo_scan"}, {S_SCAN, "supp_scan"},
				{C_SCAN, "cust_scan"}, {D_SCAN, "date_scan"}, {P_SCAN, "p_scan"},
				{LC_JOIN, "lo_cust_join"}, {LS_JOIN, "lo_supp_join"}, {LD_JOIN, "lo_date_join"}
		};

		for(auto &res_pair : merged_counts){
			fprintf(file_ptr, "%s: %d\n", (job_names[res_pair.first]).c_str(), res_pair.second);
		}

		fclose(file_ptr);
	}

	static void writeAggResultsToFile(map<int, uint64_t> &merged_agg_result){
		FILE *file_ptr = fopen("agg_result.txt", "a");

		for (auto &aggPair : merged_agg_result){
			fprintf(file_ptr, "%d -> %lu\n", aggPair.first, aggPair.second);
		}
		fprintf(file_ptr, "\n");

		fclose(file_ptr);
	}

	void writeRuntimeResultsToFile(std::unordered_map<std::string, FILE*> f_ptrs) {
		FILE *file_ptr;
		vector<time_tuple> *runtimes;

		string job_name;
		for (auto &curPair : f_ptrs) {
			job_name = curPair.first;
			runtimes = all_runtimes[job_name];

			file_ptr = curPair.second;
			time_tuple curr;
			for(int id = 0; id < (int) runtimes->size(); id++){
				curr = runtimes->at(id);
				fprintf(file_ptr, "%s (%d,%d) %llu %llu %llu\n", job_name.c_str(),
						get<2>(curr), get<3>(curr), get<0>(curr), get<1>(curr),
						get<1>(curr) - get<0>(curr));
			}
		}
	}

private:
	std::vector<time_tuple> sw_scan_runtimes;
	std::vector<time_tuple> dax_scan_runtimes;
	std::vector<time_tuple> sw_join_runtimes;
	std::vector<time_tuple> dax_join_runtimes;
	std::vector<time_tuple> agg_runtimes;

	std::unordered_map<std::string, std::vector<time_tuple>*> all_runtimes { {
			"SW_SCAN", &sw_scan_runtimes }, { "DAX_SCAN", &dax_scan_runtimes },
			{ "SW_JOIN", &sw_join_runtimes },
			{ "DAX_JOIN", &dax_join_runtimes }, { "AGG", &agg_runtimes } };

	std::vector<tuple<int, uint64_t>> local_agg_results;
	std::vector<tuple<JOB_TYPE, int>> job_counts;
};

#endif
