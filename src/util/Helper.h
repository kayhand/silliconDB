#ifndef __helper_h
#define __helper_h__

#include <iostream>
#include <bitset>

class Helper {
    public:
        Helper();
        ~Helper();
	static std::vector<int> createIndexArray(uint64_t *bitVector, int dataStart, int compLines){
	    std::vector<int> indArray;
	    for(int i = 0; i < compLines; i++){
	    	int cur_bit = 63;
	        uint64_t vec = bitVector[i];
		std::bitset<64> bit_vec(vec);
		while(cur_bit >= 0){
		    if(bit_vec.test(cur_bit)){
  		        indArray.push_back(dataStart + ((64 * i) + 63 - cur_bit));
		    }
		    cur_bit--;		    
		}
	    }
	    return indArray;
	}	
};

#endif
