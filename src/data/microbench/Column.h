/*
 * Column.h
 *
 *  Created on: Nov 8, 2018
 *      Author: kayhan
 */

#ifndef SRC_DATA_MICROBENCH_COLUMN_H_
#define SRC_DATA_MICROBENCH_COLUMN_H_

#include "Partition.h"

#include <unordered_map>
#include <vector>
#include <fstream>

enum data_type_t {
	INT, DOUBLE, STRING
};

class Column {
private:
	struct column_encoder {
		int num_of_bits;
		uint32_t distinct_values;

		std::unordered_map<uint32_t, uint32_t> i_dict;
	} c_encoder;

	struct column_meta {
		data_type_t data_type = INT;

		std::string colPath;
		int col_id = -1;
	} c_meta;

	std::vector<Partition*> partitions;

public:
	Column(std::string colPath, int c_id){
		c_meta.colPath = colPath;
		c_meta.col_id = c_id;
	}

	~Column(){
		for(Partition *part : partitions){
			delete part;
		}
	}

	/*
	 * Creates the dictionary mapping between base values and bit encodings
	 */
	void createEncoder(){
		//Scan the whole file
	    std::ifstream file;
	    file.open(c_meta.colPath);
	    std::string bit_size;
	    getline(file, bit_size);
	    file.close();

	    c_encoder.num_of_bits = std::atoi(bit_size.c_str());
	    c_encoder.distinct_values = pow(2, c_encoder.num_of_bits);

	    std::cout << c_encoder.num_of_bits << " bits and " << c_encoder.distinct_values << " keys " << std::endl;

	    //For the toy datasets we have, the values in the file represents their bit encodings
	    for(uint32_t index = 0; index < c_encoder.distinct_values; index++){
	    	c_encoder.i_dict[index] = index;
	    }
	}

	void createPartitions(int num_of_parts, std::unordered_map<int, std::pair<int, int>> &partMaps){
		for(int partId = 0; partId < num_of_parts; partId++){
			int start = std::get<0>(partMaps[partId]);
			int end = std::get<1>(partMaps[partId]);

			Partition *newPartition = new Partition(partId, start, end);
			newPartition->initializePartition(c_encoder.num_of_bits);

			partitions.push_back(newPartition);
		}
	}

	void compressPartitions(){
	    std::ifstream file;
	    file.open(c_meta.colPath);
	    std::string curVal;
	    getline(file, curVal); //skip the first line

		for(Partition *curPart : partitions){
			curPart->compress(file, c_encoder.num_of_bits);
		}

		file.close();
	}

	void printInfo(){
		std::cout << "Column id: " << c_meta.col_id << "\nFile path: " << c_meta.colPath << std::endl;
		printf("Has %d partitions in total...\n", (int) partitions.size());
	}
};



#endif /* SRC_DATA_MICROBENCH_COLUMN_H_ */
