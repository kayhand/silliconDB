#ifndef __helper_h
#define __helper_h

#include <iostream>
#include <bitset>

class Helper {
    public:
        Helper();
        ~Helper();
	static std::vector<int> createIndexArray(uint64_t *bitVector, int compLines, int num_of_bits, int dataEnd, int vals_in_segment){
	    std::vector<int> indArray;
	    int index;
	    for(int i = 0; i < compLines; i++){
	    	int cur_bit = 63;
	        uint64_t vec = bitVector[i];
		std::bitset<64> bit_vec(vec);
		while(cur_bit >= 0){
		    if(bit_vec.test(cur_bit)){
		 	index = 0 + ((64 * i) + 63 - cur_bit);
			if(num_of_bits > 0){ //then it is coming from dax
	    		    //printf("(%d, ", index);
			    index = i * vals_in_segment + realIndex(index, num_of_bits, vals_in_segment);
	    		    //printf("%d)", index);
			}
			if(index < dataEnd)
  		            indArray.push_back(index);
		    }
		    cur_bit--;		    
		}
	    }
	    printf("\n");
	    return indArray;
	}	
	static int realIndex(int index, int num_of_bits, int vals_in_segment){
	    int codes_per_line = vals_in_segment / num_of_bits;
	    index %= vals_in_segment; 
	    int newIndex = (index / codes_per_line) + num_of_bits * (index % codes_per_line);
	    //int newIndex = (index % num_of_bits) * num_of_bits + (index % vals_in_segment / num_of_bits);
	    return newIndex;
	}
};

#endif
