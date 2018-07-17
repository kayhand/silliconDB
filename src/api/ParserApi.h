#ifndef parser_api_
#define parser_api_

#ifdef __sun
extern "C" {
#include "/usr/include/dax.h"
}
#else
extern "C" {
#include "/usr/local/dax.h"
}
#endif

#include <string>
#include <iostream>
#include "ssb.h"
#include "util/Types.h"

using namespace std;

struct query_predicate{
	string column;
	int columnId;
	data_type_t columnType = STRING;

	dax_compare_t comparison; //dax_compare_t enum value
	string params[2];

	void printPred(){
		printf("\tPredicate Info: \n");
		printf("\t\tColumn name: %s (%d) - Type_id: %d \n", column.c_str(), columnId, columnType);
		printf("\t\tComparison: %d with params: %s", comparison, params[0].c_str());
		if(params[1] != ""){
			printf(" and %s", params[1].c_str());
		}
		printf("\n");
	}
};

struct ssb_table{
	int t_id;
	string tableName;
	bool isFact = false;
	bool hasFilter = false;

	vector<query_predicate> q_preds;
	string groupByKey;
	string groupByValue;

	int groupByKeyId = -1;
	int groupByValueId = -1;

	vector<string> joinColumns; //customer, date, supplier
	unordered_map<int, int> joinColumnIds;
	//lo_custkey (2), lo_orderdate (4), lo_suppkey (5)
	//dim_t_id -> fact_fk_col_id
};

/*
struct query_api_wrapper{
    std::vector<ScanApi*> scans;
    std::vector<JoinApi*> joins;
    AggApi *agg;
};*/

class ParserApi {
private:
	std::string queryFile;

	map<int, ssb_table> dimRelations; //key:t_id
	ssb_table factRelation;

	ssb_tables_meta ssb_t_meta;

	std::unordered_map<std::string, int> ssb_t_id{
		{"lineorder", -1},
		{"supplier", 0},
		{"customer", 1},
		{"part", 2},
		{"date", 3}
	};

	std::unordered_map<int, JOB_TYPE> t_scan_type{
		{-2, JOB_TYPE::LO_SCAN_2},
		{-1, JOB_TYPE::LO_SCAN},
		{0, JOB_TYPE::S_SCAN},
		{1, JOB_TYPE::C_SCAN},
		{2, JOB_TYPE::P_SCAN},
		{3, JOB_TYPE::D_SCAN}
	};

	std::unordered_map<int, JOB_TYPE> t_join_type{
		{0, JOB_TYPE::LS_JOIN},
		{1, JOB_TYPE::LC_JOIN},
		{2, JOB_TYPE::LP_JOIN},
		{3, JOB_TYPE::LD_JOIN}
	};

	//query_api_wrapper q_api;

public:
	ParserApi(){}
	ParserApi(string file) : queryFile(file){}

	int NumOfDimTables(){
		return dimRelations.size();
	}

	void setFile(string file){
		this->queryFile = file;
	}

	ssb_table &FactRelation(){
		return factRelation;
	}

	map<int, ssb_table> &DimRelations(){
		return dimRelations;
	}

	JOB_TYPE ScanType(int t_id){
		return t_scan_type[t_id];
	}

	JOB_TYPE JoinType(int t_id){
		return t_join_type[t_id];
	}

    void parseQuery(){
    	cout << "Query file: " << queryFile << endl;
    	ifstream file;
    	file.open(queryFile);

    	string curLine;
    	getline(file, curLine); //SSB QUERY 3.1;
    	cout << "Parsing " << curLine << endl;

    	ssb_table ssb_t;

    	vector<int> fkColIds;
    	bool started = false;
    	while(getline(file, curLine)){
    		if(curLine == "START"){
    			started = true;
    			//cout << "Start of relation!" << endl;
    		}
    		else if(curLine == "END"){
    			if(started == false){
    				cout << "END keyword without a START!" << endl;
    				exit(0);
    			}
    			else{
    				started = false;
    			}

    		    if(ssb_t.isFact)
    		    	factRelation = ssb_t;
    		    else
    		    	dimRelations[ssb_t.t_id] = ssb_t;

    		    //cout << "# of dimension Tables: " << dimRelations.size() << endl;
    		    ssb_table new_t;
    		    ssb_t = new_t;
    		}
    		else if(int pos = curLine.find(':')){ //dimension:date.tbl
    			string paramKey = curLine.substr(0, pos);
    			string paramValue = curLine.substr(pos + 1);
    			if(paramKey == "dimension"){
      				ssb_t.tableName = paramValue;
      				ssb_t.t_id = ssb_t_id.at(ssb_t.tableName);

      				ssb_t.isFact = false;
    			}
    			else if(paramKey == "fact"){
      				ssb_t.tableName = paramValue;
      				ssb_t.t_id = ssb_t_id.at(ssb_t.tableName);
        				ssb_t.isFact = true;

      				ssb_t.joinColumns.reserve(DimRelations().size());
      				ssb_t.joinColumnIds.reserve(DimRelations().size());
    			}
    			else if(paramKey == "predicates"){
    				query_predicate new_pred;
    				new_pred = parsePredicate(paramValue);
    				new_pred.columnId = ssb_t_meta.TableColId(ssb_t.tableName, new_pred.column);
    				new_pred.columnType = ssb_t_meta.TableColType(ssb_t.tableName, new_pred.columnId);

    				ssb_t.hasFilter = true;
    				ssb_t.q_preds.push_back(new_pred);
    			}
    			else if(paramKey == "groupByColumn"){
    				ssb_t.groupByKey = paramValue;
    				ssb_t.groupByKeyId = ssb_t_meta.TableColId(ssb_t.tableName, ssb_t.groupByKey);
    			}
    			else if(paramKey == "groupByValue"){
    				ssb_t.groupByValue = paramValue;
    				ssb_t.groupByValueId = ssb_t_meta.TableColId(ssb_t.tableName, ssb_t.groupByValue);
    			}
    			else if(paramKey == "join"){ //customer-lo_custkey
    				int sPos = paramValue.find("-");
    				string dimTable = paramValue.substr(0, sPos); //customer
    				int dim_t_id = ssb_t_id.at(dimTable); //1

    				string joinColumn = paramValue.substr(sPos + 1); //lo_custkey

    				ssb_t.joinColumns.push_back(joinColumn);
    				ssb_t.joinColumnIds[dim_t_id] =
    						ssb_t_meta.TableColId(ssb_t.tableName, joinColumn);
    			}
    		}
    	}
    }

	query_predicate parsePredicate(string predStr){ //d_year >= 1992 AND d_year <= 1997
		query_predicate q_pred;

		int lPos;
		int cPos1, cPos2;
		if((lPos = predStr.find(" AND ")) != -1){
			string firstPred = predStr.substr(0, lPos); //lo_quantity > 25
			string secondPred = predStr.substr(lPos + 1); //lo_quantity <= 35

			if((cPos1 = firstPred.find(" > ")) && (cPos2 = secondPred.find(" <= "))){
				q_pred.comparison = dax_compare_t::DAX_GT_AND_LE;
			}
			else{
				cout << "Error parsing string " << predStr << "!" << endl;
				exit(-1);
			}
			q_pred.column = firstPred.substr(0, cPos1); //lo_quantity
			q_pred.params[0] = firstPred.substr(cPos1 + 3); //1991
			q_pred.params[1] = secondPred.substr(cPos2 + 4); //1997
		}
		else if((lPos = predStr.find(" OR ")) != -1){
			string firstPred = predStr.substr(0, lPos); //d_year > 1991
			string secondPred = predStr.substr(lPos + 1); //d_year <= 1997

			if((cPos1 = firstPred.find(" = ")) && (cPos2 = secondPred.find(" = "))){
				q_pred.comparison = dax_compare_t::DAX_EQ_OR_EQ;
			}
			else{
				cout << "Error parsing string " << predStr << "!" << endl;
				exit(-1);
			}
			q_pred.column = firstPred.substr(0, cPos1); //d_year
			q_pred.params[0] = firstPred.substr(cPos1 + 3); //1991
			q_pred.params[1] = secondPred.substr(cPos2 + 3); //1997
		}
		else{
			if((cPos1 = predStr.find(" = ")) != -1){
				q_pred.comparison = dax_compare_t::DAX_EQ;
			}
			else if((cPos1 = predStr.find(" <= ")) != -1){
				q_pred.comparison = dax_compare_t::DAX_LE;
			}
			else if((cPos1 = predStr.find(" >= ")) != -1){
				q_pred.comparison = dax_compare_t::DAX_GE;
			}
			else{
				cout << "Error parsing string " << predStr << "!" << endl;
				exit(-1);
			}

			q_pred.column = predStr.substr(0, cPos1);
			q_pred.params[0] = predStr.substr(cPos1 + 3);
		}
		return q_pred;
	}

    void printQueryInfo(){
    	cout << dimRelations.size() << " dimension tables in current query" << endl;
    	dimRelations[factRelation.t_id] = factRelation;
    	for(auto &curr : dimRelations){
    		ssb_table ssb_t = curr.second;
    		cout << "==========" << endl;
    		string tableName = ssb_t.tableName;

    		cout << "\tTable: " << ssb_t.tableName << endl;
    		if(ssb_t.groupByKey != ""){
    			cout << "\tGroupByKey: " << ssb_t.groupByKey << endl;
    			cout << "\tGroupByKeyId: " << ssb_t.groupByKeyId << endl;
    		}
    		if(ssb_t.groupByValue != ""){
    			cout << "\tGroupByValue: " << ssb_t.groupByValue << endl;
    			cout << "\tGroupByValueId: " << ssb_t.groupByValueId << endl;
    		}

    		for(int jId = 0; jId < (int) ssb_t.joinColumns.size(); jId++){
    			cout << "\tJoin Column Name: " << ssb_t.joinColumns[jId];
    			cout << ", Id: " << ssb_t.joinColumnIds[jId];
    			cout << endl;
    		}

    		for(query_predicate pred : ssb_t.q_preds){
    			if(pred.column != ""){
    				pred.printPred();
    			}
    		}
    		cout << "==========" << endl;
    	}
    	dimRelations.erase(factRelation.t_id);
    	cout << dimRelations.size() << " dimension tables in current query" << endl;

    }
};



#endif /* parser_api */
