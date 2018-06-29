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
//#include "api/ParserApi.h"

#define WORD_SIZE 64

using namespace std;

enum data_type_t {
	INT, DOUBLE, STRING
};

struct column_meta {
	bool isFK = false;
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
	int num_of_bits = -1;

	//Compressed to decompressed values
	unordered_map<uint32_t, string> dict;
	uint32_t* i_dict;
	double* d_dict;
};

struct column {
	column_meta c_meta;
	column_encoder encoder;

	uint32_t *data = 0;  //this is actually compressed but kept line by line
	uint64_t *compressed; //compressed bit vector

	map<string, uint32_t> keys;
	map<int, uint32_t> i_keys;
	map<double, uint32_t> d_keys;

	//Position (line index) of each value in the original file
	vector<pair<int, uint32_t>> i_pairs;
	vector<pair<double, uint32_t>> d_pairs;
	vector<pair<string, uint32_t>> str_pairs;

	int index_mapping[64];
};

struct table_meta {
	int t_id;
	int groupByColId; //customer (c_nation): 4, date(d_year): 4
	bool isFact = false;

	int num_of_columns;
	int num_of_lines;
	int num_of_segments;

	int num_of_parts = 1;
	int part_size = 0;

	string path;

	unordered_map<uint32_t, uint32_t> groupByMap; //pk to compressed_val
	unordered_map<int, bool> groupByKeys; //distinct gro
};

struct table {
	table_meta t_meta;
	column *columns;
};

class DataCompressor {
private:
	struct table t;
	Partitioner *partitioner;

	int* distinct_keys = NULL;
	int scale_factor = 1;

	void cleanUp();
	void initTable();

public:

	DataCompressor(string filename, int t_id, int gby_id,
			int part_size, int sf){
		table_meta *t_meta = &(this->t.t_meta);
		t_meta->path = filename;
		t_meta->t_id = t_id;
		t_meta->groupByColId = gby_id;

		partitioner = new Partitioner(filename, part_size);
		t_meta->num_of_parts = partitioner->getNumberOfParts();
		this->scale_factor = sf;
	}

	//Copy constructor
	DataCompressor(const DataCompressor& source);
	//Overloaded assigment
	DataCompressor& operator=(const DataCompressor& source);
	//Destructor
	~DataCompressor() {
		for (int i = 0; i < t.t_meta.num_of_columns; i++) {
			if (t.t_meta.t_id == 0 && (i == 2 || i == 4 || i == 5)) //Skip foreign keys in the fact table
				continue;
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
		}

		int all_parts = t.t_meta.num_of_columns * t.t_meta.num_of_parts;
		for (int i = 0; i < all_parts; i++) {
			switch (t.columns[i].c_meta.data_type) {
			case data_type_t::INT:
				t.columns[i].i_keys.clear();
				t.columns[i].i_pairs.clear();
				break;
			case data_type_t::STRING:
				t.columns[i].keys.clear();
				t.columns[i].str_pairs.clear();
				break;
			case data_type_t::DOUBLE:
				t.columns[i].d_keys.clear();
				t.columns[i].d_pairs.clear();
				break;
			}
			delete[] t.columns[i].data;
			delete[] t.columns[i].compressed;
		}
		delete[] t.columns;
		delete[] distinct_keys;
		delete partitioner;
	}

	void createTableMeta(bool);
	void parseDimensionTable();
	void parseFactTable(unordered_map<int, column*> &, unordered_map<int, int> &);
	//void parseFactTable(column*, column*, column*);
	void createDictionaries();
	void createEncoders();
	void createColumns();
	void compress();
	void bit_compression(column &c);
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
