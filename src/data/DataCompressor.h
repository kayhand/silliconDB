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

#ifdef __sun
extern "C" {
#include "/usr/include/dax.h"
}
#else
extern "C" {
#include "/usr/local/dax.h"
}
#endif

#define WORD_SIZE 64

using namespace std;

enum data_type_t {
	INT, DOUBLE, STRING
};

struct column_meta {
	bool isFK = false;
	bool coPart = false;

	data_type_t data_type;

	int column_id = -1;

	uint32_t start = 0;
	uint32_t end = 0;
	int col_size = 0;

	//Bitweaving style layout information
	int num_of_codes = -1; //Number of codes in each processor word (WORD_SIZE / num_of_bits + 1)
	int codes_per_segment = -1; //num_of_codes * (num_of_bits + 1); //Number of codes that fit in a single segment
	int num_of_segments = -1; //ceil(num_of_elements * 1.0 / codes_per_segment); //Total number of segments needed to keep the data
};

struct column_encoder {
	column_meta *c_meta;

	int num_of_bits = -1; //assigned in calculateBitSizes()
	uint16_t n_syms; //assigned in calculateBitSizes()
	uint8_t* widths;
	void* symbols; //int or string (char*) symbols

	dax_zip_t *codec;

	//Compressed to decompressed values
	unordered_map<uint32_t, string> dict;
	uint32_t* i_dict;
	double* d_dict;

	bool zipped = false;

	void createCodecMeta(){
		if(c_meta->data_type == STRING){ //string column
			widths = new uint8_t[dict.size()];
			int byte_len = 0;
			for(auto &pair : dict){
			    widths[pair.first] = pair.second.length();
			    byte_len += widths[pair.first];
			}

			for(auto &pair : dict){
				symbols = new char[byte_len];
				string sym = pair.second;
				strcat((char*) symbols, sym.c_str());
			}
		}
		else if(c_meta->data_type == INT){
			int bytes_req = (n_syms / 2) + (n_syms % 2);
			cout << "\t === " << bytes_req << endl;
			widths = new uint8_t[bytes_req];

			for(int i = 0;  i < bytes_req; i++)
				widths[i] = 68;
			symbols = (void *) i_dict;
		}

		zipped = true;
		cout << "===== Symbols for compressed column =====" << endl;
		for(int i = 0; i < n_syms; i++){
			cout << ((uint32_t*) symbols) [i] << "-";
		}
		cout << endl;
		cout << "============" << endl;
	}
};

struct co_part{
	uint16_t *left_partition;
	uint16_t *right_partition;

	int part1_count;
	int part2_count;
};

struct column {
	column_meta c_meta;
	column_encoder encoder;

	uint32_t *data = 0;  //this is actually compressed but kept line by line
	uint64_t *compressed; //compressed bit vector

	co_part co_partitioned;

	map<string, uint32_t> keys;
	map<int, uint32_t> i_keys;
	map<double, uint32_t> d_keys;

	//Position (line index) of each value in the original file
	vector<pair<int, uint32_t>> i_pairs;
	vector<pair<double, uint32_t>> d_pairs;
	vector<pair<string, uint32_t>> str_pairs;

	int index_mapping[64];

	vector<uint64_t*> compressed_parts;
};

struct table_meta {
	int t_id;
	bool isFact = false;

	bool hasAggKey = false;
	int groupByColId; //customer (c_nation): 4, date(d_year): 4

	int num_of_columns;
	int num_of_lines;
	int num_of_segments;

	int num_of_parts = 1;
	int part_size = 0;

	string path;

	unordered_map<uint32_t, uint32_t> groupByMap; //pk to compressed_val
};

struct table {
	table_meta t_meta;
	column *columns;

	int NumOfParts(){
		return t_meta.num_of_parts;
	}
};

class DataCompressor {
private:
	struct table t;
	Partitioner *partitioner;

	int* distinct_keys = NULL;
	int scale_factor = 1;
	bool join_rw = false;

	void cleanUp();
	void initTable();

public:

	DataCompressor(string filename, int t_id, int gby_id,
			int part_size, int sf, bool join_rw){
		table_meta *t_meta = &(this->t.t_meta);
		t_meta->path = filename;
		t_meta->t_id = t_id;

		if(gby_id ==  -1){
			t_meta->hasAggKey = false;
		}
		else{
			t_meta->groupByColId = gby_id;
			t_meta->hasAggKey = true;
		}

		this->scale_factor = sf;
		this->join_rw = join_rw;

		partitioner = new Partitioner();
		t_meta->num_of_parts = partitioner->roundRobin(filename, part_size);
	}

	//Copy constructor
	DataCompressor(const DataCompressor& source);
	//Overloaded assigment
	DataCompressor& operator=(const DataCompressor& source);
	//Destructor
	~DataCompressor() {
		for (int i = 0; i < t.t_meta.num_of_columns; i++) {
			column *col = &(t.columns[i]);
			if (col->c_meta.isFK){ //Skip foreign keys in the fact table
				continue;
			}
			switch (t.columns[i].c_meta.data_type) {
			case data_type_t::INT:
				delete[] t.columns[i].encoder.i_dict;
				break;
			case data_type_t::STRING:
				t.columns[i].encoder.dict.clear();
				break;
			case data_type_t::DOUBLE:
				delete[] t.columns[i].encoder.d_dict;
				break;
			}

			if(col->encoder.zipped == true){
				delete[] col->encoder.widths;
				if(col->c_meta.data_type == STRING)
					delete[] ((char*) col->encoder.symbols);
			}
		}

		int all_parts = t.t_meta.num_of_columns * t.t_meta.num_of_parts;
		for (int i = 0; i < all_parts; i++) {
			column *col = &(t.columns[i]);
			switch (col->c_meta.data_type) {
			case data_type_t::INT:
				col->i_keys.clear();
				col->i_pairs.clear();
				break;
			case data_type_t::STRING:
				col->keys.clear();
				col->str_pairs.clear();
				break;
			case data_type_t::DOUBLE:
				col->d_keys.clear();
				col->d_pairs.clear();
				break;
			}
			delete[] col->data;
			delete[] col->compressed;
			if(col->c_meta.isFK && col->c_meta.coPart && this->join_rw){
				delete[] col->co_partitioned.left_partition;
				delete[] col->co_partitioned.right_partition;
			}
		}
		delete[] t.columns;
		delete[] distinct_keys;
		delete partitioner;
	}

	void createTableMeta(bool);
	void parseMicroBenchTable();
	void parseDimensionTable();
	void parseFactTable(unordered_map<int, column*> &, unordered_map<int, int> &);
	void parseFactTable(unordered_map<int, DataCompressor*> &, unordered_map<int, int> &);
	void createDictionaries();
	void createEncoders();
	void createColumns();
	void compress();
	void bit_compression(column &c);
	//void co_partition(column &c);
	void co_partition_new(column &c);
	//void co_partition_test(column &c);
	void bw_compression(column &c);
	void calculateBitSizes();

	table *getTable() {
		return &t;
	}

	int getPartSize(int p_id) {
		return partitioner->getPartitionSize(p_id);
	}

	int getCompLines(int p_id) {
		return ceil(getPartSize(p_id) / 64.0);
	}

	Partitioner *getPartitioner() {
		return partitioner;
	}

	int getNumOfParts() {
		return t.t_meta.num_of_parts;
	}

	int getScaleFactor() {
		return this->scale_factor;
	}
	int getDistinctKeys(int col_id) {
		return *(distinct_keys + col_id);
	}
};

#endif
