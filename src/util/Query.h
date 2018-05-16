#ifndef __query_h__
#define __query_h__

#include "data/DataLoader.h"
#include "Helper.h"

class Query {
    public:
        Query(){
	    p_type = -1;
	    part_id = -1;
	    table_id = -1;
	}
        Query(int type, int p_id, int t_id) : p_type(type), part_id(p_id), table_id(t_id){
	    //udata.p_id = this->part_id;
	}
        ~Query(){}

	void setFields(int r_id, int p_id, int t_id){
	   this->p_type = r_id;
	   this->part_id = p_id;
	   this->table_id = t_id;
	}

	void setPartId(int p_id){
	    this->part_id = p_id;
	}

	int &getType(){
	    return this->p_type;
	}
	int &getPart(){
	    return this->part_id;
	}
	int &getTableId(){
	    return this->table_id;
	}
	void flipDax(){
	    dax_flag = !dax_flag;
	}
	bool &isDax(){
	    return dax_flag;
	}
	
    private:
        int p_type;
	int part_id;
	int table_id; // 0 -> lineitem, 1 -> agg
	bool dax_flag = false;
};

#endif
