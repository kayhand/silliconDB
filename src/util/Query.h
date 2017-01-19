#ifndef __query_h__
#define __query_h__

#ifdef __sun
extern "C"{
    #include "/opt/dax/dax.h"
}
#endif
#include "../data/DataLoader.h"
#include "Helper.h"

class Query {
    public:
        Query(){part_id = -1;}
        Query(int type, int p_id, int t_id) : p_type(type), part_id(p_id), table_id(t_id){}
        ~Query(){}

	int &getType(){
	    return this->p_type;
	}
	int &getPart(){
	    return this->part_id;
	}
	int &getTableId(){
	    return this->table_id;
	}
	
    private:
        int p_type;
	int part_id;
	int table_id; // 0 -> lineitem, 1 -> agg
};

#endif
