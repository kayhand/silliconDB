#ifndef __query_h__
#define __query_h__

#include "../data/DataLoader.h"
#include "Helper.h"

struct q_udata{
    int p_id = -1;
    hrtime_t t_start;
    void *src_data; 
    void *dst_data;
    uint64_t *bit_vector;
    int copy_size;
};

class Query {
    public:
        Query(){
	    part_id = -1;
	}
        Query(int type, int p_id, int t_id) : p_type(type), part_id(p_id), table_id(t_id){
	    udata.p_id = this->part_id;
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
	q_udata &get_udata(){
	    return udata;
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
	q_udata udata;
	bool dax_flag = false;
};

#endif
