#include <string>
#include <iostream>

#include "data/microbench/Table.h"

using namespace std;

std::unordered_map<string, string> params{
	{"num_of_cores", "4"},
	{"port_id", "9999"},
	{"client", "localhost"},
	{"data_path", "/Users/kayhan/Repos/silliconDB/src/bench/data/"},
	{"run_type", "full"},
	{"part_size", "128"},
	{"dax_queue_size", "4"},
	{"scheduling", "operator_at_a_time"},
};

void setParams(int argc, char **argv){
	string paramPair;

	for(int argId = 1; argId < argc; argId++){
		paramPair = argv[argId];
		cout << paramPair << endl;
		int splitPos = paramPair.find(':'); // 12
		string param = paramPair.substr(0, splitPos); //"num_of_cores"
		string val = paramPair.substr(splitPos + 1); //"4"

		if(params.find(param) == params.end()){
			printf("Undefined param name: %s!\n", param.c_str());
		}
		else{
			params[param] = val;
		}
	}

	if(params["run_type"] == "toy"){
		params["part_size"] = "12800";
		params["dax_queue_size"] = "1";
		params["num_of_cores"] = "1";
	}

}

void printParams(){
	printf("\n+++++++++PARAMS++++++\n");
	for(auto parPair : params){
		std::cout << parPair.first << " : " << parPair.second << std::endl;
	}
	printf("+++++++++++++++++++++\n\n");
}

int main(int argc, char** argv) {
	if(argc == 1){
		printf("\nNo parameters specified, will start with default paramaters!\n\n");
	}
	else{
		printf("\n%d parameters specified, will override the default values!\n\n", argc-1);
	}
	setParams(argc, argv);
	//printParams();

	//int num_of_cores = atoi(params["num_of_cores"].c_str());
	//int dax_queue_size = atoi(params["dax_queue_size"].c_str());
	int morsel_size = atoi(params["part_size"].c_str());

	//int port = atoi(params["port_id"].c_str());

	string dataPath = params["data_path"];

	Table lineorder(dataPath + "mb_data", 0);
	lineorder.initializeTable(morsel_size);
	lineorder.initializeColumns();
	lineorder.compressColumns();

	lineorder.printInfo();

	return 0;
}
