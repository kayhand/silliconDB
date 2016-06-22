#ifndef __dataloader_h__
#define __dataloader_h__

#include "DataCompressor.h"

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

    void compressTable(){
        compressedTable = new DataCompressor(fileName);
        compressedTable->parse();
        compressedTable->compress();    
    }
};

#endif
