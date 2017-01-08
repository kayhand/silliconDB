#ifndef __dataloader_h__
#define __dataloader_h__

#include "DataCompressor.h"
#include "../util/Partitioner.h"

class DataLoader{

    public:
	DataLoader(int num_of_tables){
	    dataCompressors.reserve(num_of_tables);
	    dataCompressors.resize(num_of_tables);
        }
        ~DataLoader(){
	    for(DataCompressor* curComp : dataCompressors)
	        delete curComp;
	}
	std::vector<DataCompressor*> dataCompressors;

    void initializeCompressor(string fileName, int t_id, Partitioner &part){
        DataCompressor *compressedTable = new DataCompressor(fileName, t_id, part);
   	dataCompressors.at(t_id) = compressedTable;
    }

    void parseTable(int t_id){
 	 dataCompressors[t_id]->createTable();
 	 dataCompressors[t_id]->parse();
    }

    void compressTable(int t_id){
	 dataCompressors[t_id]->compress();
    }

    DataCompressor* getDataCompressor(int t_id){
        return dataCompressors[t_id];   
    }

};

#endif
