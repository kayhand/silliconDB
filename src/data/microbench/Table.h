/*
 * Table.h
 *
 *  Created on: Nov 8, 2018
 *      Author: kayhan
 */

#ifndef SRC_DATA_MICROBENCH_TABLE_H_
#define SRC_DATA_MICROBENCH_TABLE_H_

#include "Column.h"
#include <sstream>

class Table {
private:
	struct table_meta {
		std::string path; //Metadata file (keeps # of elements, # of columns)

		int num_of_elements;
		int num_of_columns;

		int t_id;
		int partition_size;
		int num_of_partitions;

		std::unordered_map<int, std::pair<int, int>> partRanges;
		//For each partition register the start and end item indices
		void createPartRanges(){
			this->num_of_partitions = num_of_elements / partition_size;

			for(int part_id = 0; part_id < num_of_partitions; part_id++){
				int start = part_id * partition_size;
				int end = start + partition_size - 1; //inclusive
				partRanges[part_id] = std::make_pair(start, end);
			}
		}
	} t_meta;

	std::vector<Column*> columns;

public:
	Table(std::string dataPath, int t_id){
		t_meta.path = dataPath;
		t_meta.t_id = t_id;
	}

	~Table(){
		for(Column *col : columns){
			delete col;
		}
	}

	void initializeTable(int partition_size){
	    std::ifstream file;
	    file.open(t_meta.path);
	    std::string num_of_vals;
	    getline(file, num_of_vals);
	    std::string num_of_cols;
	    getline(file, num_of_cols);
	    file.close();

		t_meta.num_of_elements = atoi(num_of_vals.c_str());
		t_meta.num_of_columns = atoi(num_of_cols.c_str());

		t_meta.partition_size = partition_size;
		t_meta.createPartRanges();
	}

	void initializeColumns(){
		for(int colId = 0; colId < t_meta.num_of_columns; colId++){
			std::string col_path = t_meta.path; // /tmp/ssb_data/mb_data
			std::stringstream col_id_str;
			col_id_str << colId;
			col_path += "_cols/col" + col_id_str.str() + ".dat";// /tmp/ssb_data/mb_data_cols/col_0

			std::cout << "Reading column from " << col_path << " " << std::endl;

			Column *newColumn = new Column(col_path, colId);
			newColumn->createEncoder();
			newColumn->createPartitions(t_meta.num_of_partitions, t_meta.partRanges);

			columns.push_back(newColumn);
		}
	}

	void compressColumns(){
		for(Column *curCol : columns){
			curCol->compressPartitions();
		}
	}

	void printInfo(){
		printf("\n\n");

		std::cout << t_meta.path << ": ";
		std::cout << t_meta.num_of_elements << " vals and ";
		std::cout << t_meta.num_of_columns << " cols " << std::endl;

		for(Column *col : columns){
			col->printInfo();
		}

		printf("\n\n");
	}
};


#endif /* SRC_DATA_MICROBENCH_TABLE_H_ */
