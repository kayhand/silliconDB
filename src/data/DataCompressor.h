#include <string>
#include <fstream>
#include <cstdint>
#include <cinttypes>
#include <cmath>
#include <iostream>
#include <istream>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

using namespace std;

struct column{
	enum data_type_t{
		INT,
		DOUBLE,
		STRING
	}data_type;
	/*
	 *For all data types, the data is present as a list of keys. 
	 */
	union{
		int *data;
		uint64_t *compressed;
	};

	int compression_scheme = -1;
	int compressed_nb_of_lines;

	int nb_keys = 0;

	union{
		string *dictionnary = NULL;
		int *i_dic;
		double *d_dic;
	};

	map<string,int> keys;
	map<int,int> i_keys;
	map<double,int> d_keys;
};

struct table{
	int nb_columns;
	int nb_lines;
	column *columns = NULL;
};

class DataCompressor{
	struct table t;
	string path;
	bool isInt(string s){
		for(char e : s){
			if(e > '9' || e < '0')
				return false;
		}
		return true;
	}

	//Replaced data with a compressed version
	void actual_compression(column &c, int bits, int nb_values);

	public:
	DataCompressor(string filename);
	~DataCompressor();
	table *parse();
	table *compress();
	table *getTable(){return &t;}

};

