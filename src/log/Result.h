#ifndef __result_h__
#define __result_h__

#include <algorithm>
#include <string.h>

typedef long long unsigned int timestamp;
typedef std::tuple<timestamp, timestamp> time_pair;

enum JobType{SW_SCAN, DAX_SCAN, AGG, AGG_RES, COUNT, COUNT_RES, JOIN, JOIN_RES};

class Result {
    public:
        Result(){
	    reserveResources();
	};		
        ~Result(){
	    sw_scan_runtimes.clear();
	    dax_scan_runtimes.clear();
	    agg_runtimes.clear();
	    count_runtimes.clear();
	    join_runtimes.clear();
	}   
	
	std::vector<tuple<int, float>> agg_result_q3;

	void reserveResources(){
	    sw_scan_runtimes.reserve(50);
	    dax_scan_runtimes.reserve(50);
	    
	    agg_runtimes.reserve(50);
	    agg_result.reserve(200);
	    agg_result_q3.reserve(200);

	    count_runtimes.reserve(50);
	    count_result.reserve(200);

	    join_runtimes.reserve(50);
	    join_result.reserve(200);
	}

	void writeResults(JobType j_type, FILE *f_pointer){
	    if(j_type == SW_SCAN){
	        for(time_pair curr : sw_scan_runtimes)
	    	    fprintf(f_pointer, "SW Scan %llu::%llu::%llu\n", get<0>(curr), get<1>(curr), get<1>(curr) - get<0>(curr));
	    }
	    else if(j_type == DAX_SCAN){
	        for(time_pair curr : dax_scan_runtimes)
	    	    fprintf(f_pointer, "DAX Scan %llu::%llu::%llu\n", get<0>(curr), get<1>(curr), get<1>(curr) - get<0>(curr));
	    }
	    else if(j_type == AGG){
	        for(time_pair curr : agg_runtimes)
	    	    fprintf(f_pointer, "SW Agg %llu::%llu::%llu\n", get<0>(curr), get<1>(curr), get<1>(curr) - get<0>(curr));
	    }
	    else if(j_type == COUNT){
	        for(time_pair curr : count_runtimes)
	    	    fprintf(f_pointer, "SW Count %llu::%llu::%llu\n", get<0>(curr), get<1>(curr), get<1>(curr) - get<0>(curr));
	    }
	    else if(j_type == JOIN){
	        for(time_pair curr : join_runtimes)
	    	    fprintf(f_pointer, "DAX Join %llu::%llu::%llu\n", get<0>(curr), get<1>(curr), get<1>(curr) - get<0>(curr));
	    }
	}

	template <typename Key, typename Value> 
	void writeAggResults(unordered_map<Key, Value> &agg_result_f){
	    for(auto val : agg_result){
	    	    string key;
		    key += get<0>(val);
		    key += get<1>(val);
		    get<0>(agg_result_f.at(key)) += get<2>(val);
		    get<1>(agg_result_f.at(key)) += get<3>(val);
	        }
	}

	template <typename Key, typename Value> 
	void writeAggResultsQ3(unordered_map<Key, Value> &agg_result_f){
	    for(auto val : agg_result_q3){
	        Key key = get<0>(val);
	        agg_result_f[key] += get<1>(val);
	    }
	}

	void writeCountResults(vector<int> &count_result_f){
	    int job_id;
	    for(auto val : count_result){
	        job_id = get<1>(val);
	        count_result_f[job_id] += get<0>(val); 
	    }
	}

	void addRuntime(JobType j_type, time_pair t_pair){
	    if(j_type == SW_SCAN){
	    	sw_scan_runtimes.push_back(t_pair);	
	    }
	    else if(j_type == DAX_SCAN){
	    	dax_scan_runtimes.push_back(t_pair);	
	    }
	    else if(j_type == AGG){
	    	agg_runtimes.push_back(t_pair);	
	    }	
	    else if(j_type == COUNT){
	    	count_runtimes.push_back(t_pair);	
	    }	
	    else if(j_type == JOIN){
	    	join_runtimes.push_back(t_pair);	
	    }	
	}

	template <typename T> 
	void addAggResult(T tuple){
   	    agg_result.push_back(tuple);
	}

	template <typename T> 
	void addAggResultQ3(T tuple){
   	    agg_result_q3.push_back(tuple);
	}

	template <typename T> 
	void addCountResult(T tuple){
   	    count_result.push_back(tuple);
	}

	template <typename T> 
	void addJoinResult(T tuple){
   	    join_result.push_back(tuple);
	}

    private:
	std::vector<time_pair> sw_scan_runtimes;
	std::vector<time_pair> dax_scan_runtimes;
	std::vector<time_pair> agg_runtimes;
	std::vector<time_pair> count_runtimes;
	std::vector<time_pair> join_runtimes;

	std::vector<tuple<char, char, int, uint32_t>> agg_result;
	std::vector<tuple<int, int>> count_result;
	std::vector<int> join_result;
};

#endif
