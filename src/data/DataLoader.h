#ifndef __dataloader_h__
#define __dataloader_h__

#include "DataCompressor.h"
#include "util/Helper.h"
#include "api/ParserApi.h"

class DataLoader {

public:
	DataLoader(string queryFile) {
		queryParser.setFile(queryFile);
		queryParser.parseQuery();
		queryParser.printQueryInfo();

		this->numOfDimTables = queryParser.NumOfDimTables();
	}

	~DataLoader() {
		for (DataCompressor* curComp : dimCompressors) {
			delete curComp;
		}
	}

	void processTables(string filePath, int part_size, int sf) {
		dimCompressors.resize(numOfDimTables);
		unordered_map<int, column*> dimPKCols;

		for (auto &curPair : queryParser.DimRelations()) {
			ssb_table &dim_table = curPair.second;
			int t_id = dim_table.t_id;
			printf("Dim. table name: %s (%d)\n", dim_table.tableName.c_str(),
					t_id);

			DataCompressor *dimCompressor = new DataCompressor(
					filePath + dim_table.tableName + ".tbl", t_id,
					dim_table.groupByKeyId, part_size, sf);
			dimCompressor->createTableMeta(dim_table.isFact);
			dimCompressor->parseDimensionTable();
			dimCompressor->compress();

			dimCompressors[t_id] = dimCompressor;
			column* dimPKCol = &(dimCompressor->getTable()->columns[0]);
			dimPKCols[t_id] = dimPKCol;
		}

		ssb_table &fact_table = queryParser.FactRelation();
		factCompressor = new DataCompressor(
				filePath + fact_table.tableName + ".tbl", fact_table.t_id,
				fact_table.groupByKeyId, part_size, sf);
		factCompressor->createTableMeta(fact_table.isFact);
		factCompressor->parseFactTable(dimPKCols, fact_table.joinColumnIds);
		factCompressor->compress();
	}

	/*void createQueryAPIs(){
	 vector<table*> parsedTables;
	 table *fact_t;
	 for(DataCompressor *comp : dataCompressors){
	 table *cur_t = comp->getTable();
	 if(cur_t->t_meta.t_id == 0){
	 fact_t = cur_t;
	 }
	 parsedTables.push_back(cur_t);
	 }
	 parsedTables.push_back(fact_t);
	 }*/

	ParserApi &getQueryParser() {
		return this->queryParser;
	}

	JOB_TYPE ScanType(int t_id) {
		return this->queryParser.ScanType(t_id);
	}

	DataCompressor* getFactTableCompressor() {
		return factCompressor;
	}

	std::vector<DataCompressor*> &getDimTableCompressors() {
		return dimCompressors;
	}

	int numOfPartitions(int t_id) {
		return dimCompressors[t_id]->getNumOfParts();
	}

	int TotalFactParts() {
		return factCompressor->getNumOfParts();
	}

private:
	DataCompressor* factCompressor = NULL;
	std::vector<DataCompressor*> dimCompressors;

	ParserApi queryParser;
	int numOfDimTables;
};

#endif
