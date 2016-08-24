#define DELIMITER '|'
#include "DataCompressor.h"

using namespace std;
std::vector<std::string> explode(std::string const & s, char delim);

/* (
 * Opens the file once and checks the number of lines and the type of 
 * each column.
 * Allocates enough memory to store all the data present in the file.
 *
 */

DataCompressor::DataCompressor(){
}

DataCompressor::DataCompressor(string filename, Partitioner &partitioner){
    this->partitioner = &partitioner;
    path = filename;
    p_size = partitioner.getMap().size();

    ifstream file;
    string buff;
    file.open(path);
    getline(file, buff);
    vector<string> exploded;
    exploded = explode(buff, DELIMITER);
    file.close();

    t.nb_columns = exploded.size();
    t.columns = new column [t.nb_columns * p_size];
    t.nb_lines = partitioner.getEls();
    //printf("Cols: %d, Els %d, Psize: %d\n", t.nb_columns, t.nb_lines, p_size);

    int maxCompLines = 0, maxPartSize = 0;
    for(int curCol = 0; curCol < t.nb_columns * p_size; curCol++){
	int part_id = curCol / t.nb_columns;
	int curPartSize = getPartSize(part_id);
	int curCompLines = getCompLines(part_id);	
	
	if(maxPartSize < curPartSize)
	    maxPartSize = curPartSize;
	if(maxCompLines < curCompLines)
	    maxCompLines = curCompLines;
    }

    for(int curCol = 0; curCol < t.nb_columns * p_size; curCol++){
	int actual_id = curCol % t.nb_columns;
	int part_id = curCol / t.nb_columns;
        if(!exploded[actual_id].find("INT")){        
            t.columns[curCol].data_type = column::data_type_t::INT;
	}
        else if(!exploded[actual_id].find("DOUBLE")){        
            t.columns[curCol].data_type = column::data_type_t::DOUBLE;
	}
	else{
            t.columns[curCol].data_type = column::data_type_t::STRING;
	}

	t.columns[curCol].start = partitioner.getMap().at(part_id).first;
	t.columns[curCol].end = partitioner.getMap().at(part_id).second;
        t.columns[curCol].data = new uint32_t[10 * p_size * maxPartSize]; 
        t.columns[curCol].compressed = new uint64_t[10 * p_size * maxCompLines];
	t.columns[curCol].column_id = curCol;
    }
    distinct_keys = new int[t.nb_columns]();
    exploded.clear();
}

//Copy constructor
DataCompressor::DataCompressor(const DataCompressor& source){
    printf("Copy constructor called!\n");

    partitioner = source.partitioner;
    path = source.path;
    p_size = source.p_size;

    t.nb_columns = source.t.nb_columns;
    t.nb_lines = source.t.nb_lines;

    t.columns = new column [t.nb_columns * p_size];

    int maxCompLines = 0, maxPartSize = 0;
    for(int curCol = 0; curCol < t.nb_columns * p_size; curCol++){
	int part_id = curCol / t.nb_columns;
	int curPartSize = getPartSize(part_id);
	int curCompLines = getCompLines(part_id);	
	
	if(maxPartSize < curPartSize)
	    maxPartSize = curPartSize;
	if(maxCompLines < curCompLines)
	    maxCompLines = curCompLines;
    }

    for(int curCol = 0; curCol < t.nb_columns * p_size; curCol++){
        t.columns[curCol].data_type = source.t.columns[curCol].data_type;
        t.columns[curCol].start = source.t.columns[curCol].start;
        t.columns[curCol].end = source.t.columns[curCol].end;

        t.columns[curCol].data = new uint32_t[10 * p_size * maxPartSize];
        t.columns[curCol].compressed = new uint64_t[10 * p_size * maxCompLines];

    	copy(source.t.columns[curCol].data, source.t.columns[curCol].data + (p_size * maxPartSize), t.columns[curCol].data);
    	copy(source.t.columns[curCol].compressed, source.t.columns[curCol].compressed + (p_size * maxCompLines), t.columns[curCol].compressed);

        t.columns[curCol].column_id = source.t.columns[curCol].column_id;

    }
    distinct_keys = new int[t.nb_columns]();

    copy(source.t.columns, source.t.columns + (t.nb_columns * p_size), t.columns);
    copy(source.distinct_keys, source.distinct_keys + t.nb_columns, distinct_keys);

    t.keyMap = source.t.keyMap;
}

//Overloaded assigment
DataCompressor& DataCompressor::operator=(const DataCompressor& source){
    printf("Overloaded assignment called!\n");

    //Check for self assigment
    if(this == &source){
	return *this;
    }

    partitioner = source.partitioner;
    path = source.path;
    p_size = source.p_size;

    t.nb_columns = source.t.nb_columns;
    t.nb_lines = source.t.nb_lines;

    int maxCompLines = 0, maxPartSize = 0;
    for(int curCol = 0; curCol < t.nb_columns * p_size; curCol++){
	int part_id = curCol / t.nb_columns;
	int curPartSize = getPartSize(part_id);
	int curCompLines = getCompLines(part_id);	
	
	if(maxPartSize < curPartSize)
	    maxPartSize = curPartSize;
	if(maxCompLines < curCompLines)
	    maxCompLines = curCompLines;
    }

    for(int curCol = 0; curCol < t.nb_columns * p_size; curCol++){
        t.columns[curCol].data_type = source.t.columns[curCol].data_type;
        t.columns[curCol].start = source.t.columns[curCol].start;
        t.columns[curCol].end = source.t.columns[curCol].end;

    	copy(source.t.columns[curCol].data, source.t.columns[curCol].data + (p_size * maxPartSize), t.columns[curCol].data);
    	copy(source.t.columns[curCol].compressed, source.t.columns[curCol].compressed + (p_size * maxCompLines), t.columns[curCol].compressed);

        t.columns[curCol].column_id = source.t.columns[curCol].column_id;

    }

    copy(source.t.columns, source.t.columns + (t.nb_columns * p_size), t.columns);
    copy(source.distinct_keys, source.distinct_keys + t.nb_columns, distinct_keys);

    t.keyMap = source.t.keyMap;

    return *this;
}


DataCompressor::~DataCompressor(){
    printf("Destroy data compressor\n");
    for(int i=0; i < t.nb_columns * p_size; i++){
        switch(t.columns[i].data_type){
            case column::data_type_t::INT:
		t.columns[i].i_keys.clear();
		t.columns[i].i_pairs.clear();
		t.columns[i].i_dict.clear();
                break;
            case column::data_type_t::STRING:
		t.columns[i].keys.clear();
		t.columns[i].str_pairs.clear();
		t.columns[i].dict.clear();
                break;
            case column::data_type_t::DOUBLE:
		t.columns[i].d_keys.clear();
		t.columns[i].d_pairs.clear();
		t.columns[i].d_dict.clear();
                break;
	}
	delete[] t.columns[i].data;
	delete[] t.columns[i].compressed;
    }
    delete[] t.columns;
    t.keyMap.clear();
    delete[] distinct_keys;
}

void DataCompressor::parse(){
    printf("Parsing the file...\n");
    ifstream file;
    string  buff;
    int line = 0;
    int value;
    double d_value;
    vector<string> exploded;
    column *cur_col;

    file.open(path);
    getline(file, buff);
    while(getline(file, buff)){
        exploded = explode(buff, DELIMITER);
        for(int col = 0; col < t.nb_columns * p_size; col++){	   
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
			//printf("d_val: %s\n", d_value.c_str());
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
	    for(auto curPair : t.columns[col].i_keys){
		t.columns[col].i_keys[curPair.first] = index;
		t.columns[col].i_dict[index] = curPair.first;
		index++;
	    }

	    index = 0;	
	    for(auto curPair : t.columns[col].d_keys){
		t.columns[col].d_keys[curPair.first] = index;
		t.columns[col].d_dict[index] = curPair.first * 100;
		index++;
	    }

	    index = 0;	
	    for(auto curPair : t.columns[col].keys){
		t.columns[col].keys[curPair.first] = index;
		t.columns[col].dict[index] = curPair.first;
		index++;
	    }
    }

    //Create key map for efficient aggregation
    //Q1 has col[8] and col[9] as keys
    uint64_t curKey;
    auto it2 = t.columns[9].keys.begin();
    for(auto it1 = t.columns[8].keys.begin(); it1 != t.columns[8].keys.end(); ++it1, ++it2){
    	curKey = it1->second;
	curKey <<= 32;
	curKey |= it2->second;

	if(t.keyMap.begin() != t.keyMap.end() && t.keyMap.find(curKey) == t.keyMap.end())
	    t.keyMap.emplace(curKey, t.keyMap.size());
    }

    for(int col = 0; col < t.nb_columns * p_size; col++){
	int actual_col = col % t.nb_columns;
        cur_col = &(t.columns[col]);
	for(auto &curPair : cur_col->i_pairs){
		cur_col->data[curPair.second] = t.columns[actual_col].i_keys[curPair.first];
	}

	for(auto &curPair : cur_col->d_pairs){
		cur_col->data[curPair.second] = t.columns[actual_col].d_keys[curPair.first];
	}

	for(auto &curPair : cur_col->str_pairs){
		cur_col->data[curPair.second] = t.columns[actual_col].keys[curPair.first];
	}
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
    for(int i = c.start ; i <= c.end; i++){	    
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
	//if(c.column_id == 10 && rowId == 0)
	    //printf("CurVal: %lu, id: %d \n", newVal, i);

	newVal <<= (WORD_SIZE - (c.num_of_bits + 1));
	newVal >>= shift_amount;

        if(codes_written < (c.num_of_bits + 1)){
	    c.compressed[curSegment * rows + rowId] = 0;
	    c.compressed[curSegment * rows + rowId] = newVal;
        }
        else{
            curVal = c.compressed[curSegment * rows + rowId];
            c.compressed[curSegment * rows + rowId] = curVal | newVal;
        }
        values_written++;
        codes_written++;
        prevSegment = curSegment;
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

void DataCompressor::actual_compression(column &c){
    uint64_t newVal = 0, prevVal = 0;
    uint64_t writtenVal = 0;
    unsigned long lineIndex = 0;
    unsigned long curIndex = 0, prevIndex = 0;

    int bits_written = 0;
    int bits_remaining = 0;

    for(int i = c.start ; i <= c.end; i++){	    
	    newVal = c.data[i];
	    newVal = newVal << (64 - c.num_of_bits);
	    curIndex = ((lineIndex) * c.num_of_bits / 64);

	    if(curIndex != prevIndex){
		    c.compressed[prevIndex] = writtenVal;
		    bits_remaining = bits_written - 64;
		    bits_written = 0;

		    if(bits_remaining > 0){
			    writtenVal = prevVal << (c.num_of_bits - bits_remaining); 
			    bits_written = bits_remaining;
			    writtenVal = (writtenVal) | (newVal >> bits_written);
			    bits_written += c.num_of_bits;
		    }
		    else {
			    writtenVal = (uint64_t) newVal;
			    bits_written += c.num_of_bits;
		    }
	    }
	    else{
		writtenVal = (writtenVal) | ((uint64_t) newVal >> bits_written);
		bits_written += c.num_of_bits;
		c.compressed[curIndex] = writtenVal;
	    }
	    prevIndex = curIndex;
	    prevVal = newVal;
	    lineIndex++;
    }
}

void DataCompressor::compress(){
    int n_bits = 0;
    int actual_col = 0;
    int count = 0;
    int round = 0;
    for(int col = 0; col < t.nb_columns * p_size; col++){
        if(t.columns[col].num_of_bits != -1)
            continue;
	actual_col = col % t.nb_columns;
	count = distinct_keys[actual_col];
	round = ceil(log2(count));	
	n_bits = ceil(log2(round));
	n_bits = pow(2, n_bits) - 1;
        t.columns[col].num_of_bits = n_bits;
	int num_of_elements = t.columns[col].end - t.columns[col].start + 1;

        t.columns[col].num_of_codes = WORD_SIZE / (n_bits + 1);
        t.columns[col].codes_per_segment = t.columns[col].num_of_codes * (n_bits + 1);
        t.columns[col].num_of_segments =  ceil((double) num_of_elements / t.columns[col].codes_per_segment);

        //actual_compression(t.columns[col]);
        bw_compression(t.columns[col]);
    }
}

std::vector<std::string> explode(std::string const & s, char delim){
    std::vector<std::string> result;
    std::istringstream iss(s);

    std::string token;
    while(std::getline(iss, token, delim))
    {
        result.push_back(std::move(token));
    }

    token.clear();
    iss.clear();

    return result;
}
