#define DELIMITER '|'

#ifndef __partitioner_h__
#define __partitioner_h__

#include <unordered_map>
#include <vector>

std::vector<std::string> explode(std::string const & s, char delim);

class Partitioner {
public:
	Partitioner() {}

	~Partitioner() {}

	int roundRobin(std::string filePath, int part_size);
	int roundRobinMicro(std::string filePath, int part_size);

	static int rangePartitioner(uint32_t value);

	std::unordered_map<int, std::pair<int, int>>& getMap() {
		return partitionMap;
	}
	int getNumOfElements() {
		return element_size;
	}
	std::vector<std::string>* getSchema() {
		return &schema;
	}
	int getNumOfAtts() {
		return num_of_atts;
	}
	int getNumberOfParts() {
		return num_of_parts;
	}
	int getPartitionSize(int p_id) {
		return partitionSizes[p_id];
	}
	int getSegsPerPart() {
		return segs_per_part;
	}

private:
	int element_size = 0;
	int num_of_atts = 0;
	int num_of_parts = 0;
	int segs_per_part = 0;
	int num_of_distinct = 0;


	std::unordered_map<int, int> partitionSizes;
	std::unordered_map<int, std::pair<int, int>> partitionMap;
	std::vector<std::string> schema; //column types
};

#endif
