#ifndef __result_h__
#define __result_h__

#include <algorithm>
#include <string.h>

typedef long long unsigned int timestamp;
typedef std::tuple<timestamp, timestamp> time_pair;
typedef std::tuple<timestamp, timestamp, int, int> time_tuple;

enum JobType{SW_SCAN, DAX_SCAN, SW_AND, AGG, AGG_RES, COUNT, COUNT_RES, SW_JOIN, DAX_JOIN, JOIN_RES};

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
	    sw_join_runtimes.clear();
	    dax_join_runtimes.clear();
	}   
	
	void reserveResources(){
	    sw_scan_runtimes.reserve(50);
	    dax_scan_runtimes.reserve(50);
	    
	    agg_runtimes.reserve(50);
	    agg_result.reserve(200);

	    count_runtimes.reserve(50);
	    count_result.reserve(200);

	    dax_join_runtimes.reserve(50);
	    sw_join_runtimes.reserve(50);
	    join_result.reserve(200);
	}

	void writeResults(JobType j_type, FILE *f_pointer){
	    if(j_type == SW_SCAN){
	        for(time_tuple curr : sw_scan_runtimes)
	    	    fprintf(f_pointer, "SW Scan(%d,%d) %llu %llu %llu\n", get<2>(curr), get<3>(curr), get<0>(curr), get<1>(curr), get<1>(curr) - get<0>(curr));
	    }
	    else if(j_type == DAX_SCAN){
	        for(time_tuple curr : dax_scan_runtimes)
	    	    fprintf(f_pointer, "DAX Scan(%d,%d) %llu %llu %llu\n", get<2>(curr), get<3>(curr), get<0>(curr), get<1>(curr), get<1>(curr) - get<0>(curr));
	    }
	    else if(j_type == AGG){
	        for(time_tuple curr : agg_runtimes)
	    	    fprintf(f_pointer, "SW Agg(%d,%d) %llu %llu %llu\n", get<2>(curr), get<3>(curr), get<0>(curr), get<1>(curr), get<1>(curr) - get<0>(curr));
	    }
	    else if(j_type == COUNT){
	        for(time_tuple curr : count_runtimes)
	    	    fprintf(f_pointer, "SW Count %llu %llu %llu\n", get<0>(curr), get<1>(curr), get<1>(curr) - get<0>(curr));
	    }
	    else if(j_type == DAX_JOIN){
	        for(time_tuple curr : dax_join_runtimes)
	    	    fprintf(f_pointer, "DAX Join(%d,%d) %llu %llu %llu\n", get<2>(curr), get<3>(curr), get<0>(curr), get<1>(curr), get<1>(curr) - get<0>(curr));
	    }
	    else if(j_type == SW_JOIN){
	        for(time_tuple curr : sw_join_runtimes)
	    	    fprintf(f_pointer, "SW Join(%d,%d) %llu %llu %llu\n", get<2>(curr), get<3>(curr), get<0>(curr), get<1>(curr), get<1>(curr) - get<0>(curr));
	    }

	}

	template <typename T> 
	void addAggResult(T tuple){
   	    agg_result.push_back(tuple);
	}

	template <typename Key, typename Value> 
	void writeAggResults(unordered_map<Key, Value> &agg_result_f){
	    for(auto val : agg_result){
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

	void addRuntime(JobType j_type, time_tuple t_pair){
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
	    else if(j_type == SW_JOIN){
	    	sw_join_runtimes.push_back(t_pair);	
	    }	
	    else if(j_type == DAX_JOIN){
	    	dax_join_runtimes.push_back(t_pair);	
	    }	
	}

	template <typename T> 
	void addCountResult(T tuple){
   	    count_result.push_back(tuple);
	}

	template <typename T> 
	void addCountResultDax(T tuple){
   	    count_result.push_back(tuple);
	}

	template <typename T> 
	void addJoinResult(T tuple){
   	    join_result.push_back(tuple);
	}

    private:
	std::vector<time_tuple> sw_scan_runtimes;
	std::vector<time_tuple> dax_scan_runtimes;
	std::vector<time_tuple> agg_runtimes;
	std::vector<time_tuple> count_runtimes;
	std::vector<time_tuple> sw_join_runtimes;
	std::vector<time_tuple> dax_join_runtimes;

	std::vector<tuple<string, uint64_t>> agg_result;
	std::vector<tuple<int, int>> count_result;
	std::vector<int> join_result;
};

#endif
