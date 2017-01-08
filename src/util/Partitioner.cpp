#define PART_SIZE 128000
//#define PART_SIZE 110

#include "Partitioner.h"
#include <fstream>

/*void Partitioner::roundRobin(int numberOfPartitions, std::string path)
{
    std::ifstream file;
    std::string buff;
    file.open(path);
    getline(file, buff);
    int num_of_elements;
    while(getline(file, buff)){
    	num_of_elements++;
    }
    file.close();

    int partSize = (num_of_elements) / numberOfPartitions; //first element is the header
    int remainder = (num_of_elements) % numberOfPartitions;
    printf("Number of elements %d\n", num_of_elements);
    printf("PArt size %d, remainder %d\n", partSize, remainder);

    for(int curPart = 0; curPart < numberOfPartitions; curPart++){
    	if(remainder > 0 && curPart == numberOfPartitions - 1){
    		partitionMap[curPart] = std::make_pair(curPart * partSize, num_of_elements - 1);	   
    		continue;
    	}
    	partitionMap[curPart] = std::make_pair(curPart * partSize, curPart * partSize + (partSize - 1));
    }
}*/


void Partitioner::roundRobin(std::string path)
{
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

   this->element_size = num_of_els;

   int num_of_partitions = num_of_els / PART_SIZE;
   if(num_of_partitions == 0)
   num_of_partitions++;
   this->num_of_parts = num_of_partitions;

   int remainder = num_of_els % PART_SIZE; //For now we ignore the remaining values -- number of elements is actually the highest multiple of 128K smaller than the file size

   printf("Number of elements %d\n", element_size);
   printf("Number of partitions: %d, remainder: %d\n", num_of_partitions, remainder);

   for(int curPart = 0; curPart < num_of_partitions; curPart++){
   /* 	if(remainder > 0 && curPart == num_of_partitions - 1){
    		partitionMap[curPart] = std::make_pair(curPart * PART_SIZE, num_of_els - 1);	   
       		partitionSizes[curPart] = PART_SIZE + remainder;
    		continue;
    	}*/
       partitionMap[curPart] = std::make_pair(curPart * PART_SIZE, curPart * PART_SIZE + (PART_SIZE - 1));
       partitionSizes[curPart] = PART_SIZE;
   }

}
