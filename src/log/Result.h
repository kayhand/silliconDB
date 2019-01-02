#ifndef __result_h__
#define __result_h__

#include <algorithm>
#include <string.h>

typedef long long unsigned int timestamp;
typedef std::tuple<timestamp, timestamp> time_pair;
typedef std::tuple<timestamp, timestamp, int, int> time_tuple;

class Result {
public:
	hrtime_t start_ts = 0ul;

	Result() {
		dax_scan_runtimes.reserve(5000);
		job_counts.reserve(1000);

		posting_times.reserve(5000);

		polling_times.reserve(5000);
		polling_sizes.reserve(5000);

		arrival_timestamps.reserve(5000);
		job_runtimes.reserve(5000);
	}

	~Result() {}

	void logPollCalls(int p_time, int poll_size){
		polling_times.push_back(p_time);
		polling_sizes.push_back(poll_size);
	}

	void logPollReturns(int p_return, int poll_size){
		polling_times.push_back(p_return);
		polling_sizes.push_back(poll_size);
	}

	void logArrivalTS(hrtime_t arrival_ts) {
		arrival_timestamps.push_back(arrival_ts);
	}

	void logExecTime(int exec_time){
		job_runtimes.push_back(exec_time);
	}

	void addRuntime(bool isDax, JOB_TYPE j_type, time_tuple t_pair) {
		if (j_type <= D_SCAN) {
			if (isDax)
				dax_scan_runtimes.push_back(t_pair);
			else
				sw_scan_runtimes.push_back(t_pair);
		} else if (j_type <= LD_JOIN) {
			if (isDax)
				dax_join_runtimes.push_back(t_pair);
			else
				sw_join_runtimes.push_back(t_pair);
		} else if (j_type == AGG || j_type == POST_AGG || j_type == PRE_AGG)
			agg_runtimes.push_back(t_pair);
	}


	//T tuple: <int, JOB_TYPE>
	template<typename T>
	void addCountResult(T tuple) {
		job_counts.push_back(tuple);
	}

	void addAggResult(tuple<uint32_t, uint32_t> tuple) {
		local_agg_results.push_back(tuple);
	}

	void addAggResult(tuple<string, uint32_t> tuple) {
		local_agg_results_str.push_back(tuple);
	}

	void mergeCountResults(map<int, int> &count_results) {
		int j_type;
		for (auto val : job_counts) {
			j_type = get<0>(val);
			count_results[j_type] += get<1>(val);
		}
	}

	void mergeAggResults(map<string, uint64_t> &merged_agg_results) {
		for (auto val : local_agg_results_str) {
			string key = get < 0 > (val);
			merged_agg_results[key] += get < 1 > (val);
		}
	}

	void mergeAggResults(map<int, uint64_t> &merged_agg_results) {
		for (auto val : local_agg_results) {
			int key = get < 0 > (val);
			merged_agg_results[key] += get < 1 > (val);
		}
	}


	static void writeCountsToFile(map<int, int> &merged_counts){
		FILE *file_ptr = fopen("count_result.txt", "w+");

		std::unordered_map<int, std::string> job_names {
				{LO_SCAN, "lo_scan"}, {LO_SCAN_2, "lo_scan_2"}, {S_SCAN, "supp_scan"},
				{C_SCAN, "cust_scan"}, {D_SCAN_2, "date_scan_2"}, {D_SCAN, "date_scan"}, {P_SCAN, "p_scan"},
				{LC_JOIN, "lo_cust_join"}, {LS_JOIN, "lo_supp_join"}, {LP_JOIN, "lo_part_join"}, {LD_JOIN, "lo_date_join"}
		};

		for(auto &res_pair : merged_counts){
			if(res_pair.second > 0)
				fprintf(file_ptr, "%s: %d\n", (job_names[res_pair.first]).c_str(), res_pair.second);
		}

		fclose(file_ptr);
	}

	static void writeAggResultsToFile(map<int, uint64_t> &merged_agg_result){
		FILE *file_ptr = fopen("agg_result.txt", "w+");

		for (auto &aggPair : merged_agg_result){
			fprintf(file_ptr, "%d -> %lu\n", aggPair.first, aggPair.second);
		}
		fprintf(file_ptr, "\n");

		fclose(file_ptr);
	}

	static void writeAggResultsToFile(map<string, uint64_t> &merged_agg_result){
		FILE *file_ptr = fopen("agg_result.txt", "w+");

		for (auto &aggPair : merged_agg_result){
			fprintf(file_ptr, "%s -> %lu\n", aggPair.first.c_str(), aggPair.second);
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
				//fprintf(file_ptr, "%d %llu\n", get<3>(curr), get<1>(curr) - get<0>(curr));
				fprintf(file_ptr, "%s (%d,%d) %llu %llu %d %llu\n", job_name.c_str(),
						get<2>(curr), get<3>(curr), get<0>(curr), get<1>(curr),
						ThreadId(), get<1>(curr) - get<0>(curr));
			}
		}
	}

	void writeArrivalsToFile(FILE *file_ptr) {
		hrtime_t s_ts = this->start_ts;

		int rate_at = 100000; //ns
		int total_jobs = 0;

		//log how many jobs were submitted in the last 0.1 milliseconds
		for(hrtime_t arr_ts_ : arrival_timestamps){
			int arr_ts = arr_ts_ - s_ts;
			if(arr_ts > rate_at){
				rate_at += 100000;
				fprintf(file_ptr, "%d %d\n", arr_ts, total_jobs);
				total_jobs = 0;
			}
			total_jobs++;
		}
	}

	void writeArrivalRatesToFile(FILE *file_ptr) {
		if(arrival_timestamps.size() == 0)
			return;

		int start_ts = arrival_timestamps[1] - this->start_ts;
		int prev_ts = start_ts;

		int total = 0;
		for(int i = 2; i < (int) arrival_timestamps.size(); i++){
			int curr_ts = arrival_timestamps[i] - this->start_ts;
			int curr_rate = curr_ts - prev_ts;
			prev_ts = curr_ts;

			fprintf(file_ptr, "%d \n", curr_rate);
			total += curr_rate;
		}
	}

	void writeFgPostTimesToFile(FILE *file_ptr) {
		//unsigned long total = 0ul;
		std::vector<time_tuple> *scan_rts = all_runtimes["DAX_SCAN"];
		time_tuple curr_tuple;

		for(int i = 0; i < (int) pre_posting_times.size(); i++){
			fprintf(file_ptr, "%d %d %d %d %d ", i,
					pre_posting_times[i], just_posting_times[i], polling_times[i], polling_sizes[i]);

			curr_tuple = scan_rts->at(i);
			fprintf(file_ptr, "%d \n", (int) (get<1>(curr_tuple) - get<0>(curr_tuple)));

			//total += posting_times[i];
		}

		/*for(int curr_time : posting_times){
			fprintf(file_ptr, "%d\n", curr_time);
			total += curr_time;
		}*/

		//fprintf(file_ptr, "\n\n In average: %lu \n", total / posting_times.size());
	}

	void logPostCalls(int pre_post, int just_post){
		pre_posting_times.push_back(pre_post);
		just_posting_times.push_back(just_post);
	}

	void logPostCalls(int post_time){
		posting_times.push_back(post_time);
	}

	void writePostTimesToFile(FILE *file_ptr) {
		//unsigned long total = 0ul;
		for(int p_time : posting_times){
			fprintf(file_ptr, "%d\n", p_time);
		//	total += p_time;
		}
		//fprintf(file_ptr, "\n\n In average: %lu \n", total / posting_times.size());
	}

	void writeFineGrainedPostTimesToFile(FILE *file_ptr) {
		int rate_at = 100000; //ns
		int throughput = 0;
		for(int i = 0; i < (int) pre_posting_times.size(); i++){
			if(pre_posting_times[i] > rate_at){
				rate_at += 100000;
				fprintf(file_ptr, "%d %d\n", pre_posting_times[i], throughput);
				throughput = 0;
			}
			throughput += just_posting_times[i];
		}
	}

	void writePollTimesToFile(FILE *file_ptr) {
		if(polling_times.size() == 0)
			return;

		int start_ts = polling_times[0];
		for(int i = 1; i < (int) polling_times.size(); i++){
			if(polling_sizes [i] > 0){
				fprintf(file_ptr, "%d %d\n", polling_times[i] - start_ts, polling_sizes[i]);
			}
		}
	}

	void writePollReturnsToFile(FILE *file_ptr) {
		if(polling_times.size() == 0)
			return;

		int total_jobs = 0;
		int total_time = 0;
		int avg_per_job = 0;
		int start_ts = polling_times[0];

		for(int i = 1; i < (int) polling_times.size(); i++){
			total_time = polling_times[i] - start_ts;
			total_jobs += polling_sizes[i];
			avg_per_job = total_time / total_jobs;

			fprintf(file_ptr, "%d %d -- %d\n", total_time, total_jobs, avg_per_job);
		}

	}

	void writeExecutionTimesToFile(FILE *file_ptr) {
		for(int rt : job_runtimes){
			fprintf(file_ptr, "%d \n", rt);
		}
	}

	void setThreadId(int id){
		this->thread_id = id;
		printf("\tThread id: %d\n", this->thread_id);
	}

	int ThreadId(){
		return this->thread_id;
	}

private:
	std::vector<time_tuple> sw_scan_runtimes;
	std::vector<time_tuple> dax_scan_runtimes;
	std::vector<time_tuple> sw_join_runtimes;
	std::vector<time_tuple> dax_join_runtimes;
	std::vector<time_tuple> agg_runtimes;

	std::vector<hrtime_t> arrival_timestamps;

	std::vector<int> job_runtimes;

	std::vector<int> polling_times;
	std::vector<int> polling_sizes;

	std::vector<int> pre_posting_times;
	std::vector<int> just_posting_times;
	std::vector<int> posting_times;

	std::unordered_map<std::string, std::vector<time_tuple>*> all_runtimes { {
			"SW_SCAN", &sw_scan_runtimes }, { "DAX_SCAN", &dax_scan_runtimes },
			{ "SW_JOIN", &sw_join_runtimes }, { "DAX_JOIN", &dax_join_runtimes },
			{ "AGG", &agg_runtimes } };

	std::vector<tuple<int, uint64_t>> local_agg_results;
	std::vector<tuple<string, uint64_t>> local_agg_results_str;

	std::vector<tuple<JOB_TYPE, int>> job_counts;

	int thread_id = -1;
};

#endif
