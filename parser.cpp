#include <fstream>
#include <iostream>
#include <istream>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

using namespace std;
std::vector<std::string> explode(std::string const & s, char delim);


struct column{
	enum data_type_t{
		NATIVE, //Data is an integer, store as is.
		COMPRESSED //Store data as a string
	}data_type;
	int *data = NULL;
	int nb_keys = 0;
	string *dictionnary = NULL;
	map<string,int> keys;
};

struct table{
	int nb_columns, nb_lines;
	column *columns = NULL;
};

class Parser{
	struct table t;
	string path;
	bool isInt(string s){
		for(char e : s){
			if(e > '9' || e < '0')
				return false;
		}
		return true;
	}


	public:
		Parser(string filename);
		~Parser();
		table *parse();

};
/* (
 * Opens the file once and checks the number of lines and the type of 
 * each column.
 * Allocates enough memory to store all the data present in the file.
 *
 */
Parser::Parser(string filename){
	char buff[256];
	int length = 1, i;
	ifstream file;
	vector<string> exploded;

	path = filename;

	file.open(filename);


	file.getline(buff,256);
	exploded = explode(buff, '|');

	//Ignore the comments section
	t.nb_columns = exploded.size() - 1;
	t.columns = new column [t.nb_columns];

	for(i=0;i<t.nb_columns;i++){
		t.columns[i].data_type = isInt(exploded[i]) ? column::data_type_t::NATIVE :
			 column::data_type_t::COMPRESSED;

	}

	while(!file.eof()){
		file.getline(buff,256);
		length++;
	}


	for(i=0;i<t.nb_columns;i++){
		t.columns[i].data = new int [length];
	}


	file.close();
}

table *Parser::parse(){

	ifstream file;
	int line = 0;
	char buff[256];
	vector<string> exploded;

	file.open(path);
	while(!file.eof()){
		file.getline(buff, 256);
		exploded = explode(buff, '|');
		if(exploded.size() == 0)
			break;
		for(int col = 0; col < t.nb_columns ; col++){
			/*
			 * Stores integer values as they are, treat every other type as a string
			 * and create a dictionnary.
			 */
			switch(t.columns[col].data_type){
				case column::data_type_t::NATIVE:
					t.columns[col].data[line] = stoi(exploded[col]);
					break;
				case column::data_type_t::COMPRESSED:

					if(t.columns[col].keys.find(exploded[col]) == t.columns[col].keys.end()){
						t.columns[col].keys[exploded[col]] = t.columns[col].nb_keys++;
					}

					t.columns[col].data[line] = t.columns[col].keys[exploded[col]];
					break;
			}
		}
		line++;
	}

	for(int col=0; col < t.nb_columns ; col++){
		t.columns[col].dictionnary = new string [t.columns[col].nb_keys];
		for(map<string,int>::iterator iter = t.columns[col].keys.begin(); 
				iter != t.columns[col].keys.end(); 
				++iter){
			t.columns[col].dictionnary[iter->second] = iter->first;
		}
	}


	file.close();
	return &t;
}

Parser::~Parser(){
	for(int i=0;i<t.nb_columns;i++){
		delete[] t.columns[i].data;
		t.columns[i].data = nullptr;
		delete[] t.columns[i].dictionnary;
		t.columns[i].dictionnary = nullptr;
	}
	delete[] t.columns;
	t.columns = nullptr;
}

int main(int argc, char ** argv){
	if(argc < 2){
		cerr << "Usage: " << argv[0] << " filename\n";
		return EXIT_FAILURE;
	}

	Parser p = Parser(argv[1]);
	p.parse();

	return 0;
}

std::vector<std::string> explode(std::string const & s, char delim)
{
	std::vector<std::string> result;
	std::istringstream iss(s);

	for (std::string token; std::getline(iss, token, delim); )
	{
		result.push_back(std::move(token));
	}

	return result;
}
