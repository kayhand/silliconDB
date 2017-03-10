//#define PART_SIZE 128000
//#define PART_SIZE 6001215
#define PART_SIZE 130

#include "Partitioner.h"
#include <math.h>
#include <fstream>

void Partitioner::roundRobin(std::string path, int part_size)
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

   int num_of_partitions = ceil((double) num_of_els / part_size);
   if(num_of_partitions == 0)
       num_of_partitions++;
   this->num_of_parts = num_of_partitions;

   int remainder = num_of_els % part_size; //For now we ignore the remaining values -- number of elements is actually the highest multiple of 128K smaller than the file size

   printf("Number of elements %d\n", element_size);
   printf("Number of partitions: %d, remainder: %d\n", num_of_partitions, remainder);

   for(int curPart = 0; curPart < num_of_partitions; curPart++){
    	if(remainder > 0 && curPart == num_of_partitions - 1){
    		partitionMap[curPart] = std::make_pair(curPart * part_size, num_of_els - 1);	   
       		partitionSizes[curPart] = remainder;
    		continue;
    	}
       partitionMap[curPart] = std::make_pair(curPart * part_size, curPart * part_size + (part_size - 1));
       partitionSizes[curPart] = part_size;
   }

   //for(auto &curr : partitionMap){
	//printf("%d : <%d, %d> \n", curr.first, curr.second.first, curr.second.second);	
   //}
}
