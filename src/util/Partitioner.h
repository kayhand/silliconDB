#ifndef __partitioner_h__
#define __partitioner_h__

#include <unordered_map>

class Partitioner {
    public:
	Partitioner();
	~Partitioner();
        void roundRobin(int numberOfElements, int numberOfPartitions);
	std::unordered_map<int, std::pair<int,int>>& getMap(){
            return partitionMap;
	}
    private:
	std::unordered_map<int, std::pair<int,int> > partitionMap; //ThreadId-><start,end> pair
};

#endif
