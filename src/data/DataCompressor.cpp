#include "DataCompressor.h"
using namespace std;

void DataCompressor::initTable() {
	table_meta *t_meta = &(t.t_meta);

	t_meta->num_of_columns = partitioner->getNumOfAtts();
	t_meta->num_of_lines = partitioner->getNumOfElements();

	int all_parts = t_meta->num_of_columns * t_meta->num_of_parts;
	t.columns = new column[all_parts];
}

void DataCompressor::createTableMeta(bool isFact) {
	initTable();

	table_meta *t_meta = &(t.t_meta);
	t_meta->isFact = isFact;
	int all_parts = t_meta->num_of_columns * t_meta->num_of_parts;
	vector<string> *schema = partitioner->getSchema();

	for (int curCol = 0; curCol < all_parts; curCol++) {
		int actual_id = curCol % t_meta->num_of_columns;
		int part_id = curCol / t_meta->num_of_columns;

		column_meta *c_meta = &(t.columns[curCol].c_meta);

		if (!schema->at(actual_id).find("INT")) {
			c_meta->data_type = data_type_t::INT;
		} else if (!schema->at(actual_id).find("DOUBLE")) {
			c_meta->data_type = data_type_t::DOUBLE;
		} else {
			c_meta->data_type = data_type_t::STRING;
		}

		c_meta->start = partitioner->getMap().at(part_id).first;
		c_meta->end = partitioner->getMap().at(part_id).second;
		c_meta->col_size = c_meta->end - c_meta->start + 1;

		c_meta->column_id = curCol;
	}
	distinct_keys = new int[t_meta->num_of_columns]();
	schema->clear();
}

void DataCompressor::parseFactTable(unordered_map<int, DataCompressor*> &dimComps,
		unordered_map<int, int> &joinFKIds) {
	printf("Parsing the lineorder table...\n");

	printf("Registering info for some dimension tables ...\n");
	for (auto &curPair : dimComps) {
		int t_id = curPair.first;
		int dimFKeyId = joinFKIds[t_id];

		table *dimTable = curPair.second->getTable();
		column* dimPKCol = &(dimTable->columns[0]);

		column *factFKCol = &t.columns[dimFKeyId];
		factFKCol->c_meta.isFK = true;

		printf("Dim. FK Id in Fact Table (%d) - ", dimFKeyId);
		printf("Dim. PK Id (%d)\n", dimPKCol->c_meta.column_id);

		/*if(dimPKCol->encoder.num_of_bits > 16){
			printf("This column needs to be co-partitioned!\n");
			printf("Num. of bits: %d\n", dimPKCol->encoder.num_of_bits);
			factFKCol->c_meta.coPart = true;
		}*/
		printf("Dim. Table Size: (%d)\n", dimTable->t_meta.num_of_lines);

		for (auto &curPair : dimPKCol->i_keys)
			factFKCol->i_keys.insert(curPair);
		distinct_keys[dimFKeyId] = factFKCol->i_keys.size();
		factFKCol->encoder = dimPKCol->encoder;
	}

	ifstream file;
	string val_str;
	int val_int;
	double val_double;

	uint32_t line;
	column *cur_col;
	column_meta *c_meta;

	string root_path = t.t_meta.path; // /tmp/ssb_data/lineorder.tbl
	int pos = root_path.rfind('/'); //13
	root_path = root_path.substr(0, pos + 1); // /tmp/ssb_data/
	root_path += "lo_cols/lo_col_";

	//cols are in: /tmp/ssb_data/lo_cols/lo_col
	//for each col append _colId

	int num_of_cols = t.t_meta.num_of_columns;
	int num_of_parts = t.t_meta.num_of_parts;

	for (int colId = 0; colId < num_of_cols; colId++) {
		string col_path = root_path;
		stringstream col_id_str;
		col_id_str << colId;
		col_path += col_id_str.str() + ".tbl";
		//cout << "Reading col from path: " << col_path << endl;

		file.open(col_path);
		getline(file, val_str); //val_str = column_type

		line = 0;
		for (int partId = 0; partId < num_of_parts; partId++) {
			int col = colId + partId * num_of_cols;
			cur_col = &(t.columns[col]);
			c_meta = &(cur_col->c_meta);

			while (getline(file, val_str)) {
				switch (c_meta->data_type) {
				case INT:
					val_int = atoi(val_str.c_str());
					cur_col->i_pairs.push_back(make_pair(val_int, line));
					if (t.columns[colId].i_keys.find(val_int)
							== t.columns[colId].i_keys.end()) {
						t.columns[colId].i_keys[val_int] = line;
						distinct_keys[colId] += 1;
					}
					break;
				case STRING:
					cur_col->str_pairs.push_back(make_pair(val_str, line));
					if (t.columns[colId].keys.find(val_str)
							== t.columns[colId].keys.end()) {
						t.columns[colId].keys[val_str] = line;
						distinct_keys[colId] += 1;
					}
					break;
				case DOUBLE:
					val_double = atof(val_str.c_str());
					cur_col->d_pairs.push_back(make_pair(val_double, line));
					if (t.columns[colId].d_keys.find(val_double)
							== t.columns[colId].d_keys.end()) {
						t.columns[colId].d_keys[val_double] = line;
						distinct_keys[colId] += 1;
					}
					break;
				}
				line++;
				if (line < c_meta->start || line > c_meta->end) {
					break;
				}
			}
		}
		file.close();
	}

	createDictionaries();
	calculateBitSizes();
	createEncoders();

	t.t_meta.num_of_segments = (t.t_meta.num_of_parts)
			* partitioner->getSegsPerPart();
}

void DataCompressor::parseDimensionTable() {
	ifstream file;
	string buff;
	uint32_t line = 0;
	int value;
	float d_value;
	vector<string> exploded;
	column *cur_col;
	column_meta *c_meta;

	file.open(t.t_meta.path);
	getline(file, buff);
	while (getline(file, buff)) {
		exploded = explode(buff, DELIMITER);
		for (int col = 0; col < t.t_meta.num_of_columns * t.t_meta.num_of_parts;
				col++) {
			int actual_col = col % t.t_meta.num_of_columns;
			cur_col = &(t.columns[col]);
			c_meta = &(cur_col->c_meta);

			if (line < c_meta->start || line > c_meta->end) {
				continue;
			}

			switch (c_meta->data_type) {
			case data_type_t::INT:
				value = atoi(exploded[actual_col].c_str());
				cur_col->i_pairs.push_back(make_pair(value, line));
				if (t.columns[actual_col].i_keys.find(value)
						== t.columns[actual_col].i_keys.end()) {
					t.columns[actual_col].i_keys[value] = line;
					distinct_keys[actual_col] += 1;
				}
				break;
			case data_type_t::DOUBLE:
				d_value = atof(exploded[actual_col].c_str());
				cur_col->d_pairs.push_back(make_pair(d_value, line));
				if (t.columns[actual_col].d_keys.find(d_value)
						== t.columns[actual_col].d_keys.end()) {
					t.columns[actual_col].d_keys[d_value] = line;
					distinct_keys[actual_col] += 1;
				}
				break;
			case data_type_t::STRING:
				cur_col->str_pairs.push_back(
						make_pair(exploded[actual_col], line));
				if (t.columns[actual_col].keys.find(exploded[actual_col])
						== t.columns[actual_col].keys.end()) {
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

	createDictionaries();
	calculateBitSizes();

	createEncoders();

	table_meta *t_meta = &(t.t_meta);

	t_meta->num_of_segments = (t_meta->num_of_parts)
			* partitioner->getSegsPerPart();
	//t.num_of_segments += ceil((partitioner->getPartitionSize(this->num_of_parts - 1)) / 64.0);
}

void DataCompressor::createEncoders() {
	//t.t_meta.groupByColId
	for (int col = 0; col < t.t_meta.num_of_columns * t.t_meta.num_of_parts;
			col++) {
		int actual_col = col % t.t_meta.num_of_columns;
		int part_id = col / t.t_meta.num_of_columns;

		column &cur_col = (t.columns[col]);
		column_meta &c_meta = (cur_col.c_meta);
		if (col >= t.t_meta.num_of_columns) {
			cur_col.encoder.i_dict = t.columns[actual_col].encoder.i_dict;
			cur_col.encoder.d_dict = t.columns[actual_col].encoder.d_dict;
			cur_col.c_meta.isFK = t.columns[actual_col].c_meta.isFK;
		}

		int n_bits = cur_col.encoder.num_of_bits;
		int curPartSize = c_meta.col_size;

		c_meta.num_of_codes = WORD_SIZE / (n_bits + 1);
		c_meta.codes_per_segment = c_meta.num_of_codes * (n_bits + 1);
		c_meta.num_of_segments = partitioner->getSegsPerPart();

		if (part_id == t.t_meta.num_of_parts - 1)
			c_meta.num_of_segments = ceil(
					(partitioner->getPartitionSize(part_id)) / 64.0);

		cur_col.data = new uint32_t[curPartSize];
		int curInd = 0;

		if(t.columns[actual_col].c_meta.coPart)
			printf("Will do co-partitioning on column %d for part %d\n", actual_col, part_id);

		//Each column partition's data starts from index 0
		for (auto &curPair : cur_col.i_pairs) {
			cur_col.data[curInd++] = t.columns[actual_col].i_keys[curPair.first];
		}
		for (auto &curPair : cur_col.d_pairs) {
			cur_col.data[curInd++] = t.columns[actual_col].d_keys[curPair.first];
		}
		for (auto &curPair : cur_col.str_pairs) {
			cur_col.data[curInd++] = t.columns[actual_col].keys[curPair.first];
		}
	}

	if (t.t_meta.hasAggKey){
		column* pk_col;
		column* agg_col;
		int total_cols = t.t_meta.num_of_columns * t.t_meta.num_of_parts;
		for (int col = t.t_meta.groupByColId; col < total_cols;
			col = col + t.t_meta.num_of_columns) {
			pk_col = &(t.columns[col - t.t_meta.groupByColId]);
			agg_col = &(t.columns[col]);
			int colSize = agg_col->c_meta.col_size;
			for (int ind = 0; ind < colSize; ind++) {
				t.t_meta.groupByMap[pk_col->data[ind]] = agg_col->data[ind];
				t.t_meta.groupByKeys[agg_col->data[ind]] = true;
			}
		}
	}
}

void DataCompressor::createDictionaries() {
	if(t.t_meta.isFact){
		printf("Creating dictionaries\n");
	}
	for (int col = 0; col < t.t_meta.num_of_columns; col++) {
		column &cur_col = t.columns[col];
		if (t.t_meta.isFact && cur_col.c_meta.isFK) {
			//printf("Skipping FK column %d\n", col);
			continue;
		} else {
			int index = 0;
			if (cur_col.i_keys.size() > 0)
				cur_col.encoder.i_dict = new uint32_t[distinct_keys[col]];
			else if (cur_col.d_keys.size() > 0)
				cur_col.encoder.d_dict = new double[distinct_keys[col]];

			for (auto curPair : cur_col.i_keys) {
				cur_col.i_keys[curPair.first] = index;
				cur_col.encoder.i_dict[index] = curPair.first;
				index++;
			}

			for (auto curPair : cur_col.d_keys) {
				cur_col.d_keys[curPair.first] = index;
				cur_col.encoder.d_dict[index] = curPair.first;
				index++;
			}

			for (auto curPair : cur_col.keys) {
				cur_col.keys[curPair.first] = index;
				cur_col.encoder.dict[index] = curPair.first;
				index++;
			}
		}
	}
}

void DataCompressor::bw_compression(column &c) {
	int curSegment = 0, prevSegment = 0;
	int values_written = 0, codes_written = 0;
	uint64_t newVal;
	uint64_t curVal;
	int rowId = 0;
	int rows = c.encoder.num_of_bits + 1;

	column_meta *c_meta = &(c.c_meta);

	int shift_amount = 0;
	int num_of_els = c_meta->end - c_meta->start + 1;
	int curInd = -1;

	for (int i = 0; i < num_of_els; i++) {
		newVal = c.data[i];
		curSegment = values_written / c_meta->codes_per_segment;
		if (curSegment >= c_meta->num_of_segments)
			break;
		if (curSegment != prevSegment) {
			codes_written = 0;
			rowId = 0;
		}
		rowId = codes_written % (c.encoder.num_of_bits + 1);
		shift_amount = (codes_written / (c.encoder.num_of_bits + 1))
				* (c.encoder.num_of_bits + 1);

		newVal <<= (WORD_SIZE - (c.encoder.num_of_bits + 1));
		newVal >>= shift_amount;

		curInd = curSegment * rows + rowId;
		if (codes_written < (c.encoder.num_of_bits + 1)) {
			c.compressed[curInd] = 0;
			c.compressed[curInd] = newVal;
		} else {
			curVal = c.compressed[curInd];
			c.compressed[curInd] = curVal | newVal;
		}
		values_written++;
		codes_written++;
		prevSegment = curSegment;
		//if(c.column_id == 49)
		//printf("Ind:%d Line val: %lu\n", curInd, c.compressed[curInd]);
	}

	int n_bits = c.encoder.num_of_bits + 1;
	int codes_per_line = c_meta->codes_per_segment / n_bits;
	int newIndex;
	for (int i = 0; i < c_meta->codes_per_segment; i++) {
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
	 , segs: %d}
	 }

	 for(auto &cur : c.keys){
	 printf("%s : %d\n", cur.first.c_str(), cur.second);
	 }
	 */
}


void DataCompressor::bit_compression(column &c) {
	uint64_t newVal = 0, prevVal = 0;
	uint64_t writtenVal = 0;
	unsigned long curIndex = 0;

	int bits_remaining = 64;

	column_meta *c_meta = &(c.c_meta);
	int num_of_bits = c.encoder.num_of_bits + 1;
	int i;
	for (i = 0; i < c_meta->col_size; i++) {
		newVal = c.data[i];
		if (bits_remaining < 0) {
			c.compressed[curIndex] = writtenVal;
			bits_remaining += 64;
			curIndex++;
			writtenVal = prevVal << bits_remaining;
		} else if (bits_remaining == 0) {
			c.compressed[curIndex] = writtenVal;
			bits_remaining = 64;
			writtenVal = 0;
			curIndex++;
		}
		bits_remaining -= num_of_bits;
		if (bits_remaining >= 0)
			writtenVal |= newVal << bits_remaining;
		else
			writtenVal |= newVal >> (bits_remaining * -1);
		prevVal = newVal;
	}
	c.compressed[curIndex] = writtenVal;
}

/*co-partition column*/
void DataCompressor::co_partition(column &c) {
	//if this function is called the column is compressed with 32 bits encoding
	int num_of_bits = c.encoder.num_of_bits; //31 bits
	int new_bit_size = 16;
	int num_of_segments = c.c_meta.num_of_segments;

	int num_of_lines = num_of_bits + 1; //32
	uint64_t *comp_col = c.compressed;
	uint16_t *copart_col = (uint16_t*) c.co_partitioned;

	for(int seg_id = 0; seg_id < num_of_segments; seg_id++){
		uint64_t *seg_start = comp_col + seg_id * num_of_lines;

		uint16_t *cp_start = copart_col + seg_id * num_of_lines * 2;
		uint16_t *first_part = cp_start;
		uint16_t *second_part = cp_start + num_of_lines;

		int p1_ind = 0;
		int p2_ind = 0;
		for(int ind = 0; ind < num_of_lines; ind++){
			uint64_t curLine = seg_start[ind];

			uint32_t item1 = curLine >> 32;
			uint32_t item2 = curLine;

			if((item1 >> 16) | 0){ //second partition
				second_part[p2_ind++] = (uint16_t) (item1 & ((2 ^ new_bit_size) - 1));
			}
			else{
				first_part[p1_ind++] = (uint16_t) item1;
			}

			if((item2 >> 16) | 0){ //second partition
				second_part[p2_ind++] = (uint16_t) (item2 & ((2 ^ new_bit_size) - 1));
			}
			else{
				first_part[p1_ind++] = (uint16_t) item2;
			}
		}

	}
}

/*co-partition column*/
void DataCompressor::co_partition_test(column &c) {
	if(c.c_meta.column_id != 4)
		return;

	//if this function is called the column is compressed with 15 bits encoding
	int num_of_bits = c.encoder.num_of_bits; //15 bits
	int new_bit_size = 8;
	int clear_vec = pow(2, new_bit_size) - 1;
	int num_of_segments = c.c_meta.num_of_segments;

	int num_of_lines = num_of_bits + 1; //16
	uint64_t* comp_col = c.compressed;
	uint8_t* copart_col = (uint8_t*) c.co_partitioned;

	printf("clear_vec: %d\n", clear_vec);
	for(int seg_id = 0; seg_id < num_of_segments; seg_id++){
		uint64_t* seg_start = comp_col + seg_id * num_of_lines;

		uint8_t* cp_start = copart_col + seg_id * num_of_lines * 2;
		uint8_t* first_part = cp_start;
		uint8_t* second_part = cp_start + num_of_lines;

		int p1_ind = 0;
		int p2_ind = 0;
		for(int ind = 0; ind < num_of_lines; ind++){
			uint64_t curLine = seg_start[ind];

			uint16_t item1 = curLine >> 48;
			uint16_t item2 = curLine >> 32;
			uint16_t item3 = curLine >> 16;
			uint16_t item4 = curLine;

			cout << "1) " << item1 << " 2) " << item2 << " 3) " << item3 << " 4) " << item4 << endl;
			if((item1 >> 8) | 0){ //second partition
				if(seg_id == 2)
					printf("%d -- %d\n", item1, (item1 & clear_vec));
				second_part[p2_ind++] = (uint8_t) (item1 & clear_vec);
				if(seg_id == 2)
					printf("==> %d \n\n", second_part[p2_ind - 1]);
			}
			else{
				first_part[p1_ind++] = (uint8_t) item1;
			}

			if((item2 >> 8) | 0){
				second_part[p2_ind++] = (uint8_t) (item2 & clear_vec);
			}
			else{
				first_part[p1_ind++] = (uint8_t) item2;
			}

			if((item3 >> 8) | 0){
				second_part[p2_ind++] = (uint8_t) (item3 & clear_vec);
			}
			else{
				first_part[p1_ind++] = (uint8_t) item3;
			}

			if((item4 >> 8) | 0){
				second_part[p2_ind++] = (uint8_t) (item4 & clear_vec);
			}
			else{
				first_part[p1_ind++] = (uint8_t) item4;
			}
		}
		printf("Segment %d\n", seg_id);
		printf("Total items: first_part: %d, second_part: %d\n\n", p1_ind, p2_ind);

		cout << "Vals in first part:" << endl;
		for(int i = 0; i < p1_ind; i++){
			printf("%d - ", first_part[i]);
		}
		printf("\n");

		cout << "Vals in second part:" << endl;
		for(int i = 0; i < p2_ind; i++){
			printf("%d - ", second_part[i]);
		}
		printf("\n\n");
	}
}


void DataCompressor::calculateBitSizes() {
	int n_bits = 0;
	int base_col = 0;
	int count = 0;
	int round = 0;

	if(t.t_meta.isFact){
		printf("Calculating bit sizes\n");
	}
	for (int col = 0; col < t.t_meta.num_of_columns * t.t_meta.num_of_parts; col++) {
		base_col = col % t.t_meta.num_of_columns;

		if (t.t_meta.isFact && t.columns[col].c_meta.isFK){
			//printf("Skipping FK column %d\n", col);
			continue;
		}
		else {

			count = distinct_keys[base_col];
			if (count == 1)
				n_bits = 1;
			else {
				round = ceil(log2(count));
				n_bits = ceil(log2(round));
				if (pow(2, n_bits) == round)
					n_bits++;
				n_bits = pow(2, n_bits) - 1;
			}
			//if(n_bits >= 15)
			//n_bits = 23;
			if (n_bits < 15)
				n_bits = 15;
			t.columns[col].encoder.num_of_bits = n_bits;
			//printf("%d->%d\n", col, t.columns[col].encoder.num_of_bits);
			//if(n_bits > 23)
			//t.columns[col].num_of_bits = 23;
			//printf("%d->%d\n", col, t.columns[col].num_of_bits);
		}
	}
}

/*co-partition at this level?*/
void DataCompressor::compress() {
	int total_col_parts = t.t_meta.num_of_columns * t.t_meta.num_of_parts;
	for (int col = 0; col < total_col_parts; col++) {
		column &cur_col = t.columns[col];

		int num_of_segments = cur_col.c_meta.num_of_segments;
		int c_size = num_of_segments * (cur_col.encoder.num_of_bits + 1);

		cur_col.compressed = new uint64_t[c_size]();
		bit_compression(cur_col);

		/*
		if(cur_col.c_meta.isFK){
			cout << "FK col " << col << " is encoded with " << cur_col.encoder.num_of_bits << "bits!" <<endl;
			cout << num_of_segments << endl;
			cur_col.co_partitioned = new uint16_t[c_size * 2]();

			//co_partition(cur_col);
			co_partition_test(cur_col);
		}*/

		//bw_compression(*cur_col);
	}
}

std::vector<std::string> explode(std::string const & s, char delim) {
	std::vector<std::string> result;
	std::istringstream iss(s);

	std::string token;
	while (std::getline(iss, token, delim)) {
		result.push_back(token);
	}

	token.clear();
	iss.clear();

	return result;
}
