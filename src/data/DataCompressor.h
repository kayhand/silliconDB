#ifndef __data_compressor_h__
#define __data_compressor_h__

#include <fstream>
#include <string>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <cinttypes>
#include <cmath>
#include <iostream>
#include <istream>
#include <map>
#include <sstream>
#include <iomanip>
#include <utility>
#include <vector>
#include <unordered_map>
#include <bitset>

#include "Partitioner.h"

#define WORD_SIZE 64

using namespace std;

enum data_type_t{
    INT,
    DOUBLE,
    STRING
};

struct column_meta{
    data_type_t data_type; 

    int column_id = -1;

    int start = 0;
    int end = 0;
    int col_size;

    //Bitweaving style layout information
    int num_of_codes = -1; //Number of codes in each processor word (WORD_SIZE / num_of_bits + 1)
    int codes_per_segment = -1; //num_of_codes * (num_of_bits + 1); //Number of codes that fit in a single segment
    int num_of_segments =  -1; //ceil(num_of_elements * 1.0 / codes_per_segment); //Total number of segments needed to keep the data
};

struct column_encoder{
    int num_of_bits = -1;

    //Compressed to decompressed values
    unordered_map<uint32_t, string> dict;
    int* i_dict;
    double* d_dict; 
};

struct column{
    column_meta c_meta;
    column_encoder encoder;

    uint32_t *data = 0;  //this is actually compressed but kept line by line
    uint64_t *compressed ; //compressed bit vector

    map<string, uint32_t> keys;
    map<int, uint32_t> i_keys;
    map<double, uint32_t> d_keys;

    //Position (line index) of each value in the original file
    vector<pair<int, int>> i_pairs;
    vector<pair<double, int>> d_pairs;
    vector<pair<string, int>> str_pairs;
 
    int index_mapping[64];
};

struct table_meta{
    int t_id;

    int num_of_columns;
    int num_of_lines;
    int num_of_segments;

    int num_of_parts = 1;
    int part_size = 0;
    
    string path;

    unordered_map<uint32_t, int> keyMap; //For aggregation keys use a mapping for each key to avoid concat costs
    unordered_map<int, uint32_t> reversedMap; 
};

struct table{
    table_meta t_meta;
    column *columns;
};

class DataCompressor{
    private:
    	struct table t;
    	Partitioner *partitioner;

	int* distinct_keys;
	void* bit_vector;
	void* join_vector;
	int scale_factor = 1;

	void cleanUp();
	void initTable();

    public:
    	DataCompressor(string filename, int t_id, Partitioner &partitioner, int sf);

	//Copy constructor
	DataCompressor(const DataCompressor& source);
	//Overloaded assigment
	DataCompressor& operator=(const DataCompressor& source);
	//Destructor
    	~DataCompressor();
    	
	void createTableMeta();
    	void parseData();
	void createDictionaries();
	void createEncoders();
	void createColumns();
    	void compress();
    	void bit_compression(column &c);
	void bw_compression(column &c);
	void calculateBitSizes();

    	table *getTable(){
    		return &t;
    	}

    	int getPartSize(int p_id){
		return partitioner->getPartitionSize(p_id);
	}

	int getCompLines(int p_id){
		return ceil(getPartSize(p_id) / 64.0);
	}

	Partitioner *getPartitioner(){
		return partitioner;
	}

	void* getBitVector(int offset){
	    return static_cast<uint64_t *> (bit_vector) + offset;
	}

	void* getJoinBitVector(int offset){
	    return static_cast<uint64_t *> (join_vector) + offset;
	}
	
	int getNumOfParts(){
	    return t.t_meta.num_of_parts;
	}

	int getScaleFactor(){
	    return this->scale_factor;
	}
	int getDistinctKeys(int col_id){
	    return *(distinct_keys + col_id);
	}
};

#endif

