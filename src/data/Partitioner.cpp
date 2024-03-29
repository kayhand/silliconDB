#include "Partitioner.h"
#include <math.h>
#include <fstream>

int Partitioner::roundRobin(std::string path, int part_size){
    std::ifstream file;
    std::string buff;
    file.open(path);
    getline(file, buff);
    this->schema = explode(buff, DELIMITER);
    this->num_of_atts = schema.size();

    int num_of_els = 0;
    while(getline(file, buff)){
        num_of_els++;
    }
    file.close();

    part_size = ceil ( (double) part_size / 64) * 64; //round it up to the power of 64 (segment size)

    this->element_size = num_of_els;		//30000
    this->segs_per_part = part_size / 64.0;	//187.5
    //this->segs_per_part = ceil((double) segs_per_part / 64) * 64;

    //int els_per_part = this->segs_per_part * 64;
    int els_per_part = part_size;	//12000
    this->num_of_parts = num_of_els / els_per_part;	//2.5

    //int remainder = num_of_els - this->num_of_parts * els_per_part;
    int remainder = num_of_els % part_size;		//6000

    int curPart;
    for(curPart = 0; curPart < this->num_of_parts; curPart++){
        partitionMap[curPart] = std::make_pair(curPart * els_per_part, curPart * els_per_part + (els_per_part - 1));
        partitionSizes[curPart] = els_per_part; //12000
    }

    if(remainder > 0){
        partitionMap[curPart] = std::make_pair(curPart * els_per_part, num_of_els - 1);
        partitionSizes[curPart] = remainder;
        num_of_parts++;
    }

    printf("Number of elements: %d, ", num_of_els);
    printf("Number of partitions: %d, Remainder: %d\n", this->num_of_parts, remainder);
	
    return this->num_of_parts;
    //for(auto &curr : partitionMap){
        //printf("p_id: %d : s: %d, e: %d \n", curr.first, curr.second.first, curr.second.second);
    //}
    /*for(int i = 0; i < this->num_of_parts; i++)
    	printf("P:%d - S:%d\n", i, partitionSizes[i]);*/
}

int Partitioner::roundRobinMicro(std::string path, int part_size){
    std::ifstream file;
    file.open(path);

    std::string num_of_values;
    std::string distinct_values;

    getline(file, num_of_values);
    getline(file, distinct_values);

    file.close();

    int num_of_els = atoi(num_of_values.c_str());
    int num_of_distinct = atoi(distinct_values.c_str());

    this->num_of_atts = 1;
    this->schema.push_back("INT");
    this->element_size = num_of_els;
    this->num_of_parts = num_of_els / part_size;
    this->num_of_distinct = num_of_distinct;
    this->segs_per_part = part_size / 64;

    int remainder = num_of_els % part_size;

    int curPart;
    for(curPart = 0; curPart < this->num_of_parts; curPart++){
        partitionMap[curPart] = std::make_pair(curPart * part_size, curPart * part_size + (part_size - 1));
        partitionSizes[curPart] = part_size;
    }
    printf("Number of elements: %d, ", num_of_els);
    printf("Number of partitions: %d, Remainder: %d\n", this->num_of_parts, remainder);

    return this->num_of_parts;
}

int Partitioner::rangePartitioner(uint32_t value){
	return ((value >> 16) | 0);
}
