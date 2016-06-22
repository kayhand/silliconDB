#include "DataCompressor.h"
#define DELIMITER '|'

using namespace std;
std::vector<std::string> explode(std::string const & s, char delim);

void DataCompressor::actual_compression(column &c, int bits, int nb_values){
    uint64_t *new_tab;
    uint64_t element = 0;
    int size, n = 64;
    int mask, cpt = 0;
    int *tab = c.data;

    size = ceil(nb_values * bits / 64.0);
    new_tab = new uint64_t [size]();
    mask = pow(2, bits) - 1;

    for(int i = 0 ; i < nb_values ; i++){
        if(n > bits){
            element <<= bits;
            n -= bits;
            element |= (tab[i]) & mask ;
        } else if(n==0){
            new_tab[cpt] = element;
            cpt++;
            element = 0;
        }else{
            element <<= bits;
            element &= ((tab[i]) & mask) >> (bits - n);
            new_tab[cpt] = element;
            cpt++;
            n = 64 - (bits - n);
        }
    }
    delete[] c.data;
    c.compressed = new_tab;
}

/* DONE: int
 *
 */
table *DataCompressor::compress(){
    int col, target, cpt = 0;

    for(col = 0; col < t.nb_columns; col++){
        //Only compress once
        if(t.columns[col].compression_scheme != -1)
            continue;
        target = t.columns[col].nb_keys;

        /*
         * Smallest number of bits that contains the
         * target values.
         */
        for(cpt=0 ; pow(2,cpt) < target ; cpt++);
        	actual_compression(t.columns[col], cpt, t.nb_lines);
        t.columns[col].compression_scheme = cpt;
    }
    return &t;
}

/* (
 * Opens the file once and checks the number of lines and the type of 
 * each column.
 * Allocates enough memory to store all the data present in the file.
 *
 */
DataCompressor::DataCompressor(string filename){
    string buff;
    int length = 1, i;
    ifstream file;
    vector<string> exploded;

    path = filename;
    file.open(path);

    getline(file, buff);
    exploded = explode(buff, DELIMITER);

    t.nb_columns = exploded.size();
    t.columns = new column [t.nb_columns];

    for(i=0;i<t.nb_columns;i++){
        if(!exploded[i].find("INT"))        
            t.columns[i].data_type = column::data_type_t::INT;
        else if(!exploded[i].find("DOUBLE"))        
            t.columns[i].data_type = column::data_type_t::DOUBLE;
        else if(!exploded[i].find("STRING"))        
            t.columns[i].data_type = column::data_type_t::STRING;
        else{
            cerr << "Unrecognized data type: " << exploded[i] << endl;
            exit(EXIT_FAILURE);
        }
    }

    while(getline(file, buff)){
        length++;
    }

    t.nb_lines = length;
    for(i=0;i<t.nb_columns;i++){
        t.columns[i].data = new int [length]();
    }

    file.close();
}

table *DataCompressor::parse(){
    ifstream file;
    string  buff;
    int line = 0;
    int value, d_value;
    vector<string> exploded;
    struct column *current;

    file.open(path);
    //Skip first line
    getline(file, buff);
    while(getline(file, buff)){
        exploded = explode(buff, DELIMITER);

        for(int col = 0; col < t.nb_columns ; col++){
            current = &(t.columns[col]);
            /*
         * Stores integer values as they are, treat every other type as a string
         * and create a dictionnary.
         */
        switch(t.columns[col].data_type){
            case column::data_type_t::INT:
                    value = stoi(exploded[col]);
                if(current->i_keys.find(value) == current->i_keys.end())
                    current->i_keys[value] = current->nb_keys++;
                current->data[line] = current->i_keys[value];
                break;
            case column::data_type_t::DOUBLE:
                d_value = stof(exploded[col]);
                if(current->d_keys.find(d_value) == current->d_keys.end())
                    current->d_keys[d_value] = current->nb_keys++;
                current->data[line] = current->d_keys[d_value];
                break;
            case column::data_type_t::STRING:
                if(t.columns[col].keys.find(exploded[col]) == t.columns[col].keys.end()){
                    t.columns[col].keys[exploded[col]] = t.columns[col].nb_keys++;
            	}
                t.columns[col].data[line] = t.columns[col].keys[exploded[col]];
                break;
            }
        }
        line++;
        if(line > t.nb_lines)
            break;
    }

    //The dictionnary of each colum is constructed from the keys map
    //The maps are also destroyed, to save space
    for(int col=0; col < t.nb_columns ; col++){
        switch(t.columns[col].data_type){
            case column::data_type_t::INT:
                t.columns[col].i_dic = new int [t.columns[col].nb_keys];
                for(map<int,int>::iterator iter = t.columns[col].i_keys.begin(); iter != t.columns[col].i_keys.end(); iter++)
                    t.columns[col].i_dic[iter->second] = iter->first;
                //t.columns[col].i_keys.clear();
                break;
            case column::data_type_t::DOUBLE:
                t.columns[col].d_dic = new double [t.columns[col].nb_keys];
                for(map<double,int>::iterator iter = t.columns[col].d_keys.begin(); iter != t.columns[col].d_keys.end(); iter++)
                    t.columns[col].d_dic[iter->second] = iter->first;
            	//t.columns[col].d_keys.clear();
            	break;
            case column::data_type_t::STRING:
                t.columns[col].dictionnary = new string [t.columns[col].nb_keys];
                for(map<string,int>::iterator iter = t.columns[col].keys.begin(); iter != t.columns[col].keys.end(); ++iter){
                    t.columns[col].dictionnary[iter->second] = iter->first;
                }
                //t.columns[col].keys.clear();
                break;
            default:
                cerr << "This wasn't supposed to happen!\n";
                exit(-1);
        }
    }
    //C++ is suppose to do this on it's own
    //file.close();
    return &t;
}

DataCompressor::~DataCompressor(){
    for(int i=0;i<t.nb_columns;i++){

        if(t.columns[i].compression_scheme == -1)
            delete[] t.columns[i].data;
        else
            delete[] t.columns[i].compressed;

        switch(t.columns[i].data_type){
            case column::data_type_t::INT:
                delete[] t.columns[i].i_dic;
                break;
            case column::data_type_t::STRING:
                delete[] t.columns[i].dictionnary;
                break;
            case column::data_type_t::DOUBLE:
                delete[] t.columns[i].d_dic;
                break;
        }

    }
    delete[] t.columns;
    t.columns = nullptr;
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


