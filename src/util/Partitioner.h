#define DELIMITER '|'

#ifndef __partitioner_h__
#define __partitioner_h__

#include <unordered_map>
#include <vector>

std::vector<std::string> explode(std::string const & s, char delim);

class Partitioner {
    public:
	Partitioner(){}
	~Partitioner(){}

        //void roundRobin(int numberOfPartitions, std::string filePath);
        void roundRobin(std::string filePath, int part_size);

	std::unordered_map<int, std::pair<int,int>>& getMap(){
            return partitionMap;
	}
	int getNumOfElements(){
	    return element_size;
	}
	std::vector<std::string>& getSchema(){
	    return schema;
	}
	int getNumOfAtts(){
	    return num_of_atts;
	}
	int getNumberOfParts(){
	    return num_of_parts;
	}
	int getPartitionSize(int p_id){
	    return partitionSizes[p_id];
	}
	int getSegsPerPart(){
	    return segs_per_part;
	}

    private:
	std::unordered_map<int, std::pair<int,int>> partitionMap; 
	int element_size;
	std::vector<std::string> schema; //first line of each file to define attribute data types
	int num_of_atts;
	std::unordered_map<int, int> partitionSizes; 
	int num_of_parts;
	int segs_per_part;
};

#endif
