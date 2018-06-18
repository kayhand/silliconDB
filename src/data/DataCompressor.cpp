#include "DataCompressor.h"
using namespace std;

DataCompressor::DataCompressor(string filename, int t_id, int aggCol,
		Partitioner &partitioner, int sf) {
	table_meta *t_meta = &(this->t.t_meta);
	t_meta->path = filename;
	t_meta->t_id = t_id;
	t_meta->num_of_parts = partitioner.getMap().size();
	t_meta->groupByColId = aggCol;

	this->partitioner = &partitioner;
	this->scale_factor = sf;
}

DataCompressor::~DataCompressor() {
	for (int i = 0; i < t.t_meta.num_of_columns; i++) {
		if (t.t_meta.t_id == 0 && (i == 2 || i == 4|| i == 5)) //Skip foreign keys in the fact table
			continue;
		switch (t.columns[i].c_meta.data_type) {
		case data_type_t::INT:
			delete[] t.columns[i].encoder.i_dict;
			break;
		case data_type_t::STRING:
			t.columns[i].encoder.dict.clear();
			break;
		case data_type_t::DOUBLE:
			delete[] t.columns[i].encoder.d_dict;
			break;
		}
	}

	int all_parts = t.t_meta.num_of_columns * t.t_meta.num_of_parts;
	for (int i = 0; i < all_parts; i++) {
		switch (t.columns[i].c_meta.data_type) {
		case data_type_t::INT:
			t.columns[i].i_keys.clear();
			t.columns[i].i_pairs.clear();
			break;
		case data_type_t::STRING:
			t.columns[i].keys.clear();
			t.columns[i].str_pairs.clear();
			break;
		case data_type_t::DOUBLE:
			t.columns[i].d_keys.clear();
			t.columns[i].d_pairs.clear();
			break;
		}
		delete[] t.columns[i].data;
		delete[] t.columns[i].compressed;
	}
	delete[] t.columns;
	delete[] distinct_keys;
}

void DataCompressor::initTable() {
	table_meta *t_meta = &(t.t_meta);
	t_meta->num_of_columns = partitioner->getNumOfAtts();
	t_meta->num_of_lines = partitioner->getNumOfElements();

	int all_parts = t_meta->num_of_columns * t_meta->num_of_parts;
	t.columns = new column[all_parts];

	//printf("\nCols: %d, Els %d, Psize: %d\n", t_meta->num_of_columns, t_meta->num_of_lines, t_meta->num_of_parts);
}

void DataCompressor::createTableMeta() {
	initTable();

	table_meta *t_meta = &(t.t_meta);
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

void DataCompressor::parseFactTable(column *suppPKCol, column *custPKCol, column *datePKCol) {
	printf("Parsing the lineorder table...\n");
	int custKeyId = 2;
	int suppKeyId = 4;
	int dateKeyId = 5;

	column *custFKCol = &t.columns[custKeyId];
	column *suppFKCol = &t.columns[suppKeyId];
	column *dateFKCol = &t.columns[dateKeyId];

	for (auto &curPair : custPKCol->i_keys)
		custFKCol->i_keys.insert(curPair);
	distinct_keys[custKeyId] = custFKCol->i_keys.size();
	custFKCol->encoder = custPKCol->encoder;

	for (auto &curPair : suppPKCol->i_keys)
		suppFKCol->i_keys.insert(curPair);
	distinct_keys[suppKeyId] = suppFKCol->i_keys.size();
	suppFKCol->encoder = suppPKCol->encoder;

	for (auto &curPair : datePKCol->i_keys)
		dateFKCol->i_keys.insert(curPair);
	distinct_keys[dateKeyId] = dateFKCol->i_keys.size();
	dateFKCol->encoder = datePKCol->encoder;

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

void DataCompressor::parseData() {
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
	column *cur_col;
	column_meta *c_meta;

	//t.t_meta.groupByColId
	for (int col = 0; col < t.t_meta.num_of_columns * t.t_meta.num_of_parts;
			col++) {
		int actual_col = col % t.t_meta.num_of_columns;
		int part_id = col / t.t_meta.num_of_columns;
		cur_col = &(t.columns[col]);
		c_meta = &(cur_col->c_meta);
		if (col >= t.t_meta.num_of_columns) {
			cur_col->encoder.i_dict = t.columns[actual_col].encoder.i_dict;
			cur_col->encoder.d_dict = t.columns[actual_col].encoder.d_dict;
		}

		int n_bits = cur_col->encoder.num_of_bits;
		//int curPartSize = getPartSize(part_id); //How many data elements in the current part
		int curPartSize = c_meta->col_size;

		c_meta->num_of_codes = WORD_SIZE / (n_bits + 1);
		c_meta->codes_per_segment = c_meta->num_of_codes * (n_bits + 1);
		c_meta->num_of_segments = partitioner->getSegsPerPart();

		if (part_id == t.t_meta.num_of_parts - 1)
			c_meta->num_of_segments = ceil(
					(partitioner->getPartitionSize(part_id)) / 64.0);

		cur_col->data = new uint32_t[curPartSize];
		int curInd = 0;

		for (auto &curPair : cur_col->i_pairs) {
			//Each partition's data should start from index 0
			cur_col->data[curInd++] =
					t.columns[actual_col].i_keys[curPair.first];
		}
		for (auto &curPair : cur_col->d_pairs) {
			cur_col->data[curInd++] =
					t.columns[actual_col].d_keys[curPair.first];
		}
		for (auto &curPair : cur_col->str_pairs) {
			cur_col->data[curInd++] = t.columns[actual_col].keys[curPair.first];
		}
	}

	if (t.t_meta.groupByColId < 0)
		return;

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

void DataCompressor::createDictionaries() {
	for (int col = 0; col < t.t_meta.num_of_columns; col++) {
		if (t.t_meta.t_id == 0 && (col == 2 || col == 4 || col == 5))
			continue;

		int index = 0;
		if (t.columns[col].i_keys.size() > 0)
			t.columns[col].encoder.i_dict = new uint32_t[distinct_keys[col]];
		else if (t.columns[col].d_keys.size() > 0)
			t.columns[col].encoder.d_dict = new double[distinct_keys[col]];
		for (auto curPair : t.columns[col].i_keys) {
			t.columns[col].i_keys[curPair.first] = index;
			t.columns[col].encoder.i_dict[index] = curPair.first;
			index++;
		}

		for (auto curPair : t.columns[col].d_keys) {
			t.columns[col].d_keys[curPair.first] = index;
			t.columns[col].encoder.d_dict[index] = curPair.first;
			index++;
		}

		for (auto curPair : t.columns[col].keys) {
			t.columns[col].keys[curPair.first] = index;
			t.columns[col].encoder.dict[index] = curPair.first;
			index++;
		}
	}
	//if(t.t_meta.t_id == 0){
	//  for(int i = 0; i < distinct_keys[2]; i++)
	//    printf("%d -> %d\n", i, t.columns[2].encoder.i_dict[i]); 
	//}
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

void DataCompressor::calculateBitSizes() {
	int n_bits = 0;
	int actual_col = 0;
	int count = 0;
	int round = 0;

	for (int col = 0; col < t.t_meta.num_of_columns * t.t_meta.num_of_parts;
			col++) {
		if (t.columns[col].encoder.num_of_bits != -1)
			continue;
		actual_col = col % t.t_meta.num_of_columns;
		if (t.t_meta.t_id == 0 && (col == 2 || col == 4 || col == 5))
			continue;
		count = distinct_keys[actual_col];
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

void DataCompressor::compress() {
	column *cur_col;
	for (int col = 0; col < t.t_meta.num_of_columns * t.t_meta.num_of_parts;
			col++) {
		cur_col = &(t.columns[col]);

		int num_of_segments = cur_col->c_meta.num_of_segments;
		//int compLines = getCompLines(part_id); // gets segment size for that partition
		int c_size = num_of_segments * (cur_col->encoder.num_of_bits + 1);
		cur_col->compressed = new uint64_t[c_size]();
		bit_compression(*cur_col);
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
