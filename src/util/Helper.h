#ifndef __helper_h
#define __helper_h

#include <iostream>
#include <bitset>

class Helper {
    public:
        Helper(){}
        ~Helper(){}

	static std::vector<int> createIndexArray(uint64_t *bitVector, column *c, bool isDax){
	    std::vector<int> indArray;
	    int index;
	    int remaining_data = c->end - c->start + 1;
	    for(int i = 0; i < c->num_of_segments; i++){
	    	int cur_bit = 63;
	        uint64_t vec = bitVector[i];
		std::bitset<64> bit_vec(vec);
		while(cur_bit >= 0){
		    if(bit_vec.test(cur_bit)){
			if(!isDax)
		 	    index = 63 - cur_bit;			
			else
		 	    index = c->index_mapping[63 - cur_bit];			
			if(index < remaining_data)
  		            indArray.push_back(64 * i + index);
		    }
		    cur_bit--;		    
		}
		remaining_data -= 64;
	    }
	    return indArray;
	}	

	static void translateDaxVector(uint64_t *bitVector, column *c){
	    int index;
	    int remaining_data = c->end - c->start + 1;
	    for(int i = 0; i < c->num_of_segments; i++){
	    	int cur_bit = 63;
	        uint64_t vec = bitVector[i];
		std::bitset<64> bit_vec(vec);
		std::bitset<64> new_vec;
		while(cur_bit >= 0){
		    index = c->index_mapping[63 - cur_bit];			
		    if(index < remaining_data){
		    	new_vec[63 - index] = bit_vec[cur_bit];
		    }
		    cur_bit--;		    
		}
		bitVector[i] = new_vec.to_ulong();
		remaining_data -= 64;
	    }
	}	
};

#endif
