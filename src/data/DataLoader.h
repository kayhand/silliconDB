#ifndef __dataloader_h__
#define __dataloader_h__

#include "DataCompressor.h"

class DataLoader{

    public:
	DataLoader(int num_of_tables){
	    dataCompressors.reserve(num_of_tables);
	    dataCompressors.resize(num_of_tables);
        }
        ~DataLoader(){
	    for(DataCompressor* curComp : dataCompressors){
	        delete curComp;
	    }
	}
	std::vector<DataCompressor*> dataCompressors;

    void initializeCompressor(string fileName, int t_id, Partitioner &part, int sf){
        DataCompressor *compressedTable = new DataCompressor(fileName, t_id, part, sf);
   	dataCompressors.at(t_id) = compressedTable;
    }

    void parseTable(int t_id){
 	 dataCompressors[t_id]->createTableMeta();
	 if(t_id == 0){
	     column* datePKCol = &(dataCompressors[1]->getTable()->columns[0]);
	     column* custPKCol = &(dataCompressors[2]->getTable()->columns[0]);
	     dataCompressors[t_id]->parseFactTable(datePKCol, custPKCol);
	 }
	 else
 	     dataCompressors[t_id]->parseData();
    }

    void compressTable(int t_id){
	 dataCompressors[t_id]->compress();
    }

    DataCompressor* getDataCompressor(int t_id){
        return dataCompressors[t_id];   
    }

};

#endif
