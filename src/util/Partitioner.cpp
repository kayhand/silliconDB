#include "Partitioner.h"
#include <fstream>

Partitioner::Partitioner()
{

}

Partitioner::~Partitioner()
{

}

void Partitioner::roundRobin(int numberOfPartitions, std::string path)
{
    std::ifstream file;
    std::string buff;
    file.open(path);
    getline(file, buff);
    while(getline(file, buff)){
	this->num_of_elements++;
    }
    file.close();

    int partSize = (this->num_of_elements) / numberOfPartitions; //first element is the header
    int remainder = (this->num_of_elements) % numberOfPartitions;
    printf("Number of elements %d\n", this->num_of_elements);
    printf("PArt size %d, remainder %d\n", partSize, remainder);

    for(int curPart = 0; curPart < numberOfPartitions; curPart++){
        if(remainder > 0 && curPart == numberOfPartitions - 1){
	    partitionMap[curPart] = std::make_pair(curPart * partSize, num_of_elements - 1);	   
	    continue;
	}
	partitionMap[curPart] = std::make_pair(curPart * partSize, curPart * partSize + (partSize - 1));
    }
}
