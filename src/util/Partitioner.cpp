#include "Partitioner.h"

Partitioner::Partitioner()
{

}

Partitioner::~Partitioner()
{

}

void Partitioner::roundRobin(int numberOfElements, int numberOfPartitions)
{
    int partSize = (numberOfElements - 1) / numberOfPartitions; //first element is the header
    int remainder = (numberOfElements - 1) % numberOfPartitions;
    printf("Number of elements %d\n", numberOfElements);
    printf("PArt size %d, remainder %d\n", partSize, remainder);

    for(int curPart = 0; curPart < numberOfPartitions; curPart++){
        if(remainder > 0 && curPart == numberOfPartitions - 1){
	    partitionMap[curPart] = std::make_pair(curPart * partSize, numberOfElements  - 1);	   
	    continue;
	}
	partitionMap[curPart] = std::make_pair(curPart * partSize, curPart * partSize + (partSize - 1));
    }
}
