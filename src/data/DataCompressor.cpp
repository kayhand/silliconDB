#include "DataCompressor.h"
using namespace std;

DataCompressor::DataCompressor(string filename, int t_id, Partitioner &partitioner, int sf){
    this->path = filename;
    this->t_id = t_id;
    this->partitioner = &partitioner;
    this->num_of_parts = partitioner.getMap().size();
    this->scale_factor = sf;
}

DataCompressor::~DataCompressor(){
    for(int i=0; i < t.nb_columns * num_of_parts; i++){
        switch(t.columns[i].data_type){
            case column::data_type_t::INT:
		t.columns[i].i_keys.clear();
		t.columns[i].i_pairs.clear();
		delete[] t.columns[i].i_dict;
                break;
            case column::data_type_t::STRING:
		t.columns[i].keys.clear();
		t.columns[i].str_pairs.clear();
		t.columns[i].dict.clear();
                break;
            case column::data_type_t::DOUBLE:
		t.columns[i].d_keys.clear();
		t.columns[i].d_pairs.clear();
		delete[] t.columns[i].d_dict;
                break;
	}
	delete[] t.columns[i].data;
	delete[] t.columns[i].compressed;
    }
    delete[] t.columns;
    t.keyMap.clear();
    delete[] distinct_keys;
    free(bit_vector);
    free(join_vector);
}

void DataCompressor::createTable(){
    t.nb_columns = partitioner->getNumOfAtts();
    t.nb_lines = partitioner->getNumOfElements();
    t.columns = new column [t.nb_columns * num_of_parts];
    vector<string> schema = partitioner->getSchema();
    printf("Cols: %d, Els %d, Psize: %d\n", t.nb_columns, t.nb_lines, num_of_parts);

    for(int curCol = 0; curCol < t.nb_columns * num_of_parts; curCol++){
	int actual_id = curCol % t.nb_columns;
	int part_id = curCol / t.nb_columns;

        if(!schema[actual_id].find("INT")){        
            t.columns[curCol].data_type = column::data_type_t::INT;
	}
        else if(!schema[actual_id].find("DOUBLE")){        
            t.columns[curCol].data_type = column::data_type_t::DOUBLE;
	}
	else{
            t.columns[curCol].data_type = column::data_type_t::STRING;
	}

	t.columns[curCol].start = partitioner->getMap().at(part_id).first;
	t.columns[curCol].end = partitioner->getMap().at(part_id).second;
	t.columns[curCol].column_id = curCol;
    }
    distinct_keys = new int[t.nb_columns]();
    schema.clear();
}

void DataCompressor::parse(){
    printf("Parsing the file...\n");
    ifstream file;
    string  buff;
    int line = 0;
    int value;
    float d_value;
    vector<string> exploded;
    column *cur_col;

    file.open(path);
    getline(file, buff);
    while(getline(file, buff)){
        exploded = explode(buff, DELIMITER);
        for(int col = 0; col < t.nb_columns * num_of_parts; col++){	   
	    int actual_col = col % t.nb_columns;
	    if(t.columns[col].start > line || line > t.columns[col].end){
		continue;
	    } 
	    cur_col = &(t.columns[col]);
	    switch(t.columns[col].data_type){
		case column::data_type_t::INT:
		    value = atoi(exploded[actual_col].c_str());
		    cur_col->i_pairs.push_back(make_pair(value, line));
		    if(t.columns[actual_col].i_keys.find(value) == t.columns[actual_col].i_keys.end()){
			t.columns[actual_col].i_keys[value] = line;
			distinct_keys[actual_col] += 1;
		    }
		    break;
		case column::data_type_t::DOUBLE:
		    d_value = atof(exploded[actual_col].c_str());
		    cur_col->d_pairs.push_back(make_pair(d_value, line));
		    if(t.columns[actual_col].d_keys.find(d_value) == t.columns[actual_col].d_keys.end()){
			t.columns[actual_col].d_keys[d_value] = line;
			distinct_keys[actual_col] += 1;
		    }
		    break;
		case column::data_type_t::STRING:
		    cur_col->str_pairs.push_back(make_pair(exploded[actual_col], line));
		    if(t.columns[actual_col].keys.find(exploded[actual_col]) == t.columns[actual_col].keys.end()){
			t.columns[actual_col].keys[exploded[actual_col]] = line;
			distinct_keys[actual_col] += 1;
		    }
		    break;
	    }
        }
        line++;
	exploded.clear();
    }
    file.close();

    //Give keys indices in the increasing order
    for(int col = 0; col < t.nb_columns; col++){
	    int index = 0;
	    if(t.columns[col].i_keys.size() > 0)
	    	t.columns[col].i_dict = new int[distinct_keys[col]];
	    else if(t.columns[col].d_keys.size() > 0)
	    	t.columns[col].d_dict = new double[distinct_keys[col]];
	    for(auto curPair : t.columns[col].i_keys){
		t.columns[col].i_keys[curPair.first] = index;
		t.columns[col].i_dict[index] = curPair.first;
		index++;
	    }

	    for(auto curPair : t.columns[col].d_keys){
		t.columns[col].d_keys[curPair.first] = index;
		t.columns[col].d_dict[index] = curPair.first;
		index++;
	    }

	    for(auto curPair : t.columns[col].keys){
		t.columns[col].keys[curPair.first] = index;
		t.columns[col].dict[index] = curPair.first;
		index++;
	    }	
    }

    getNumberOfBits();
    for(int col = 0; col < t.nb_columns * num_of_parts; col++){
	int actual_col = col % t.nb_columns;
	int part_id = col / t.nb_columns;
        cur_col = &(t.columns[col]);
	if(col >= t.nb_columns){
	    t.columns[col].i_dict = t.columns[actual_col].i_dict; 
	    t.columns[col].d_dict = t.columns[actual_col].d_dict; 
	}

        int n_bits = cur_col->num_of_bits;
	int curPartSize = getPartSize(part_id); //How many data elements in the current part

        cur_col->num_of_codes = WORD_SIZE / (n_bits + 1);
        cur_col->codes_per_segment = cur_col->num_of_codes * (n_bits + 1);
	cur_col->num_of_segments = partitioner->getSegsPerPart();
	if(part_id == num_of_parts - 1)
            cur_col->num_of_segments = ceil((partitioner->getPartitionSize(part_id)) / 64.0);

        cur_col->data = new uint32_t[curPartSize];
	int curInd = 0;

	for(auto &curPair : cur_col->i_pairs){
		cur_col->data[curInd++] = t.columns[actual_col].i_keys[curPair.first];  //Each partition's data should start from index 0
	}
	for(auto &curPair : cur_col->d_pairs){
		cur_col->data[curInd++] = t.columns[actual_col].d_keys[curPair.first];
	}
	for(auto &curPair : cur_col->str_pairs){
		cur_col->data[curInd++] = t.columns[actual_col].keys[curPair.first];
	}
    }

    t.num_of_segments = (this->num_of_parts) * partitioner->getSegsPerPart();
    //t.num_of_segments += ceil((partitioner->getPartitionSize(this->num_of_parts - 1)) / 64.0);

    posix_memalign(&bit_vector, 4096, t.num_of_segments * 8);
    posix_memalign(&join_vector, 4096, t.num_of_segments * 8);

    //Create key map for efficient aggregation
    //Q1 has col[8] and col[9] as keys
    if(this->t_id == 0){ //for lineitem table
	    uint32_t curKey;
	    auto it1 = t.columns[8].keys.begin();
	    auto it2 = t.columns[9].keys.begin();
	    while(it1 != t.columns[8].keys.end()){
		curKey = it1->second;
		curKey = (curKey << 16) | it2->second;

		if(t.keyMap.find(curKey) == t.keyMap.end())
		    t.keyMap.emplace(curKey, t.keyMap.size());
		it1++;
		it2++;
	    }
	    uint32_t final_key = 65536; 
	    t.keyMap.emplace(final_key, t.keyMap.size());

	    for(auto &curr : t.keyMap)
		t.reversedMap.emplace(curr.second, curr.first);
    }
}

void DataCompressor::bw_compression(column &c){
    int curSegment = 0, prevSegment = 0;
    int values_written = 0, codes_written = 0;
    uint64_t newVal;
    uint64_t curVal;
    int rowId = 0;
    int rows = c.num_of_bits + 1;

    int shift_amount = 0;
    int num_of_els = c.end - c.start + 1;
    int curInd;

    for(int i = 0 ; i < num_of_els; i++){	    
	newVal = c.data[i];
        curSegment = values_written / c.codes_per_segment;
        if(curSegment >= c.num_of_segments)
                break;
        if(curSegment != prevSegment){
                codes_written = 0;
                rowId = 0;
        }
        rowId = codes_written % (c.num_of_bits + 1);
	shift_amount = (codes_written / (c.num_of_bits + 1)) * (c.num_of_bits + 1);

	newVal <<= (WORD_SIZE - (c.num_of_bits + 1));
	newVal >>= shift_amount;

	curInd = curSegment * rows + rowId;
        if(codes_written < (c.num_of_bits + 1)){
	    c.compressed[curInd] = 0;
	    c.compressed[curInd] = newVal;
        }
        else{
            curVal = c.compressed[curInd];
            c.compressed[curInd] = curVal | newVal;
        }
        values_written++;
        codes_written++;
        prevSegment = curSegment;
    	//if(c.column_id == 49)
	    //printf("Ind:%d Line val: %lu\n", curInd, c.compressed[curInd]);
    }

    int n_bits = c.num_of_bits + 1;
    int codes_per_line = c.codes_per_segment / n_bits;
    int newIndex;
    for(int i = 0; i < c.codes_per_segment; i++){
    	newIndex = (i / codes_per_line) + n_bits * (i % codes_per_line);
	c.index_mapping[i] = newIndex;
    }

    /* 
    if(c.column_id != 10)
	return;
    for(int i = 0; i < c.num_of_segments; i++){
        printf("Segment %d: \n", i);
        for(int j = 0; j < c.num_of_bits; j++){
            printf("%lu\n", c.compressed[i * rows + j]);
        }
    }   

    for(auto &cur : c.keys){
	printf("%s : %d\n", cur.first.c_str(), cur.second);
    }
    */
}

void DataCompressor::bit_compression(column &c){
    uint64_t newVal = 0, prevVal = 0;
    uint64_t writtenVal = 0;
    unsigned long curIndex = 0;

    int bits_remaining = 64;

    int num_of_bits = c.num_of_bits + 1;
    int num_of_els = c.end - c.start + 1;
    for(int i = 0; i < num_of_els; i++){	    
	    newVal = c.data[i];
	    if(bits_remaining < 0){
		c.compressed[curIndex] = writtenVal;
	    	bits_remaining += 64;
		curIndex++;
		writtenVal = prevVal << bits_remaining; 
	    }
	    else if (bits_remaining == 0){
		c.compressed[curIndex] = writtenVal;
		bits_remaining = 64;
		writtenVal = 0;
		curIndex++;
	    }
	    bits_remaining -= num_of_bits;
	    if(bits_remaining >= 0)
	        writtenVal |= newVal << bits_remaining;
	    else
	        writtenVal |= newVal >> (bits_remaining * -1);
	    prevVal = newVal;
    }
    c.compressed[curIndex] = writtenVal;
}

void DataCompressor::getNumberOfBits(){
    int n_bits = 0;
    int actual_col = 0;
    int count = 0;
    int round = 0;
    for(int col = 0; col < t.nb_columns * num_of_parts; col++){
        if(t.columns[col].num_of_bits != -1)
            continue;
	actual_col = col % t.nb_columns;
	count = distinct_keys[actual_col];
	if(count == 1)
	    n_bits = 1;
	else{
	    round = ceil(log2(count));	
	    n_bits = ceil(log2(round));
	    if(pow(2, n_bits) == round)
	        n_bits++;
	    n_bits = pow(2, n_bits) - 1;
	}
	if(n_bits >= 15)
	    n_bits = 23;
        t.columns[col].num_of_bits = n_bits;
	//printf("%d->%d\n", col, t.columns[col].num_of_bits);
	//if(n_bits > 23)
            //t.columns[col].num_of_bits = 23;
	//printf("%d->%d\n", col, t.columns[col].num_of_bits);
    }
}

void DataCompressor::compress(){
    column *cur_col;
    for(int col = 0; col < t.nb_columns * num_of_parts; col++){
        cur_col = &(t.columns[col]);

	int part_id = col / t.nb_columns;
	int compLines = getCompLines(part_id); // gets segment size for that partition
        cur_col->compressed = new uint64_t[compLines * (cur_col->num_of_bits + 1)]();
        bit_compression(*cur_col);
        //bw_compression(*cur_col);
    }
}

std::vector<std::string> explode(std::string const & s, char delim){
    std::vector<std::string> result;
    std::istringstream iss(s);

    std::string token;
    while(std::getline(iss, token, delim)){
        result.push_back(token);
    }

    token.clear();
    iss.clear();

    return result;
}
