#ifndef __dataloader_h__
#define __dataloader_h__

#include "DataCompressor.h"
#include "../util/Partitioner.h"

class DataLoader{
    string fileName;

    public:
	DataLoader(string file){
	    fileName = file;
        }
        ~DataLoader(){
	    delete compressedTable;
	}
        DataCompressor *compressedTable;

   void parseTable(Partitioner &part){
        compressedTable = new DataCompressor(fileName, part);
        compressedTable->parse();
   }
   void compressTable(){
        compressedTable->compress();    
   }

};

#endif
