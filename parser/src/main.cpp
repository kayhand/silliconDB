#include "parser.h"

using namespace std;

int main(int argc, char ** argv){
	if(argc < 2){
		cerr << "Usage: " << argv[0] << " filename\n";
		return EXIT_FAILURE;
	}

	Parser p = Parser(argv[1]);
	p.parse();
	p.compress();
	

	//cout << "Done.\n";
	cout << "All values: " << p.get()->nb_columns << " and " << p.get()->nb_lines << endl;

	return 0;
}
