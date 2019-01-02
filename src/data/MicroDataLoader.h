#ifndef __dataloader_h__
#define __dataloader_h__

#include "DataCompressor.h"
#include "util/Helper.h"
#include "api/ParserApi.h"

class DataLoader {

public:
	~DataLoader() {
		for(auto &curComp : dimCompressors){
			delete curComp.second;
		}
		delete factCompressor;
	}

	void parseQuery(string queryFile){
		queryParser.setFile(queryFile);
		queryParser.parseQuery();
		queryParser.printQueryInfo();
		this->numOfDimTables = queryParser.NumOfDimTables();
	}

	void processTables(string filePath, int part_size, int sf, string run_type, bool join_rw) {
		//unordered_map<int, column*> dimPKCols;

		for (auto &curPair : queryParser.DimRelations()) {
			ssb_table &dim_table = curPair.second;
			int t_id = dim_table.t_id;
			printf("Dim. table name: %s (%d)\n", dim_table.tableName.c_str(), t_id);

			DataCompressor *dimCompressor = new DataCompressor(
					filePath + dim_table.tableName + ".tbl", t_id,
					dim_table.groupByKeyId, part_size, sf, join_rw);
			dimCompressor->createTableMeta(dim_table.isFact);
			dimCompressor->parseDimensionTable();
			dimCompressor->compress();

			dimCompressors[t_id] = dimCompressor;
		}

		string path = filePath;
		if(run_type == "toy")
			path += "toy_data/";

		ssb_table &fact_table = queryParser.FactRelation();
		factCompressor = new DataCompressor(
				path + fact_table.tableName + ".tbl", fact_table.t_id,
				fact_table.groupByKeyId, part_size, sf, join_rw);
		factCompressor->createTableMeta(fact_table.isFact);
		factCompressor->parseFactTable(dimCompressors, fact_table.joinColumnIds);
		factCompressor->compress();
	}

	void processMicroTable(string filePath, int morsel_size){
		microBenchCompressor = new DataCompressor(filePath, -1, -1, morsel_size, -1, -1);
		microBenchCompressor->createTableMeta(false);
		microBenchCompressor->parseMicroBenchTable();
		microBenchCompressor->compress();
	}

	ParserApi &getQueryParser() {
		return this->queryParser;
	}

	JOB_TYPE ScanType(int t_id) {
		return this->queryParser.ScanType(t_id);
	}

	DataCompressor* getFactTableCompressor() {
		return factCompressor;
	}

	unordered_map<int, DataCompressor*> &getDimTableCompressors() {
		return dimCompressors;
	}

	int numOfPartitions(int t_id) {
		return dimCompressors[t_id]->getNumOfParts();
	}

	int TotalFactParts() {
		return factCompressor->getNumOfParts();
	}

	int TotalDimParts(){
		int sum = 0;
		for(auto &cur : dimCompressors){
			sum += cur.second->getNumOfParts();
		}
		return sum;
	}

private:
	DataCompressor* microBenchCompressor = NULL;

	DataCompressor* factCompressor = NULL;
	unordered_map<int, DataCompressor*> dimCompressors;

	ParserApi queryParser;
	int numOfDimTables;
};

#endif
