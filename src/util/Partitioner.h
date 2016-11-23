#ifndef __partitioner_h__
#define __partitioner_h__

#include <unordered_map>

class Partitioner {
    public:
	Partitioner();
	~Partitioner();
        void roundRobin(int numberOfPartitions, std::string filePath);
	std::unordered_map<int, std::pair<int,int>>& getMap(){
            return partitionMap;
	}
	int getEls(){
	    return num_of_elements;
	}
	int assignNextPartition(){
	    nextPartition++;
	    return nextPartition;
	}

    private:
	std::unordered_map<int, std::pair<int,int> > partitionMap; //ThreadId-><start,end> pair
	int num_of_elements = 0;
	int nextPartition = -1;

};

#endif
