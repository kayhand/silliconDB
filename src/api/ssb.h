/*
 * SSB.h
 *
 *  Created on: Jun 22, 2018
 *      Author: kayhan
 */

#ifndef SRC_API_SSB_H_
#define SRC_API_SSB_H_

#include <string>
#include <unordered_map>

struct ssb_lineorder_t{
	std::unordered_map<std::string, int> columns{
		{"lo_orderkey", 0},
		{"lo_linenumber", 1},
		{"lo_custkey", 2},
		{"lo_partkey", 3},
		{"lo_suppkey", 4},
		{"lo_orderdate", 5},
		{"lo_orderpriority", 6},
		{"lo_shippriority", 7},
		{"lo_quantity", 8},
		{"lo_extendedprice", 9},
		{"lo_ordtotalprice", 10},
		{"lo_discount", 11},
		{"lo_revenue", 12}
	};

	std::unordered_map<int, data_type_t> column_types{
		{0, INT},
		{1, INT},
		{2, INT},
		{3, INT},
		{4, INT},
		{5, INT},
		{6, STRING},
		{7, INT},
		{8, INT},
		{9, INT},
		{10, INT},
		{11, INT},
		{12, INT}
	};

};

struct ssb_supplier_t{
	std::unordered_map<std::string, int> columns{
		{"s_suppkey", 0},
		{"s_name", 1},
		{"s_address", 2},
		{"s_city", 3},
		{"s_nation", 4},
		{"s_region", 5},
		{"s_phone", 6}
	};

	std::unordered_map<int, data_type_t> column_types{
		{0, INT},
		{1, STRING},
		{2, STRING},
		{3, STRING},
		{4, STRING},
		{5, STRING},
		{6, STRING}
	};
};


struct ssb_customer_t {
	std::unordered_map<std::string, int> columns{
		{"c_custkey", 0},
		{"c_name", 1},
		{"c_address", 2},
		{"c_city", 3},
		{"c_nation", 4},
		{"c_region", 5},
		{"c_phone", 6},
		{"c_mktsegment", 7}
	};

	std::unordered_map<int, data_type_t> column_types{
		{0, INT},
		{1, STRING},
		{2, STRING},
		{3, STRING},
		{4, STRING},
		{5, STRING},
		{6, STRING},
		{7, STRING}
	};
};

struct ssb_part_t {
	std::unordered_map<std::string, int> columns{
		{"p_partkey", 0},
		{"p_name", 1},
		{"p_mfgr", 2},
		{"p_category", 3},
		{"p_brand1", 4},
		{"p_color", 5},
		{"p_type", 6},
		{"p_size", 7},
		{"p_container", 8}
	};

	std::unordered_map<int, data_type_t> column_types{
		{0, INT},
		{1, STRING},
		{2, STRING},
		{3, STRING},
		{4, STRING},
		{5, STRING},
		{6, STRING},
		{7, INT},
		{8, STRING}
	};
};

struct ssb_date_t {
	std::unordered_map<std::string, int> columns{
		{"d_datekey", 0},
		{"d_date", 1},
		{"d_dayofweek", 2},
		{"d_month", 3},
		{"d_year", 4},
		{"d_yearmonthnum", 5},
		{"d_yearmonth", 6},
		{"d_daynuminweek", 7},
		{"d_daynuminmonth", 8},
		{"d_daynuminyear", 9},
		{"d_monthnuminyear", 10},
		{"d_weeknuminyear", 11},
		{"d_sellingseason", 12},
		{"d_lastdayinweekfl", 13},
		{"d_lastdayinmothnfl", 14},
		{"d_holidayfl", 15},
		{"d_weekdayfl", 16}
	};

	std::unordered_map<int, data_type_t> column_types{
		{0, INT},
		{1, STRING},
		{2, STRING},
		{3, STRING},
		{4, INT},
		{5, INT},
		{6, STRING},
		{7, INT},
		{8, INT},
		{9, INT},
		{10, INT},
		{11, INT},
		{12, INT},
		{13, INT},
		{14, INT},
		{15, INT},
		{16, INT}
	};
};

struct ssb_tables_meta{

private:
	ssb_lineorder_t lo_t;
	ssb_supplier_t s_t;
	ssb_customer_t c_t;
	ssb_date_t d_t;
	ssb_part_t p_t;

	std::unordered_map<std::string, std::unordered_map<std::string, int>> ssb_columns{
		{"lineorder", lo_t.columns},
		{"supplier", s_t.columns},
		{"customer", c_t.columns},
		{"date", d_t.columns},
		{"part", p_t.columns}
	};

	std::unordered_map<std::string, std::unordered_map<int, data_type_t>> ssb_column_types{
		{"lineorder", lo_t.column_types},
		{"supplier", s_t.column_types},
		{"customer", c_t.column_types},
		{"date", d_t.column_types},
		{"part", p_t.column_types}
	};

public:
	int TableColId(string tableName, string columnName){
		if(ssb_columns[tableName].find(columnName) == ssb_columns[tableName].end()){
			printf("Column %s does not exist in table %s!\n", columnName.c_str(), tableName.c_str());
			exit(-1);
		}
		else{
			return ssb_columns[tableName][columnName];
		}
	}

	data_type_t TableColType(string tableName, int columnId){
		return ssb_column_types[tableName][columnId];
	}

};

#endif /* SRC_API_SSB_H_ */
