#ifndef __agg_api_h__
#define __agg_api_h__



#ifdef __sun
extern "C" {
#include "/usr/include/dax.h"
}
#endif

#include "util/Types.h"
#include "util/Query.h"
#include "util/Helper.h"

#include "data/DataLoader.h"
#include "log/Result.h"
#include "thread/Thread.h"

#include "api/JoinApi.h"



class AggApi {
private:
	ScanApi *factScan;
	std::vector<JoinApi*> joins;

	vector<vector<vector<int>>> aggKeyMap;
	map<int, unordered_map<uint32_t, uint32_t>*> keyMaps;

	bool isPreAgg = false;
	void *bit_res; //result of pre-aggregation

	/* to use in extract */
	dax_vec_t src;
	dax_vec_t dst;

	void reservePreAggVector(){
		posix_memalign(&bit_res, 4096, this->factScan->blockSize);
	}

	void* getPreAggBitVector(int part) {
		int offset = part * this->factScan->num_of_segments;
		return (static_cast<uint64_t*>(bit_res)) + offset;
	}

	void initializeAgg() {
		vector<int> key_sizes {1, 1, 1};

		int curDim = 0;
		for(JoinApi *curJoin : joins){
			table *t_ = (curJoin->dimensionScan->getBaseTable());
			table_meta &t_meta = (t_->t_meta);

			column_encoder &c_encoder = t_->columns[t_meta.groupByColId].encoder;

			if(t_meta.hasAggKey){
				int key_size = c_encoder.n_syms;
				key_sizes[curDim++] = key_size;

				ScanApi *dimScan = curJoin->dimensionScan;
				keyMaps[dimScan->Type()] = &(dimScan->getBaseTable()->t_meta.groupByMap);
			}
		}

		cout << key_sizes[0] << " -- " << key_sizes[1] << " -- " << key_sizes[2] << endl;

		vector<int> vec3(key_sizes[2], -1); //aggKeyMap1D
		vector<vector<int>> vec2(key_sizes[1], vec3); //aggKeyMap2D
		aggKeyMap.resize(key_sizes[0], vec2);

		int id_1D = 0;
		int id_2D = 0;
		int id_3D = 0;
		for (int i = 0; i < key_sizes[0]; i++) {
			aggKeyMap[i][0][0] = id_1D++;
			for (int j = 0; j < key_sizes[1]; j++) {
				aggKeyMap[i][j][0] = id_2D++;
				for (int k = 0; k < key_sizes[2]; k++) {
					aggKeyMap[i][j][k] = id_3D++;
				}
			}
		}
	}

public:
	AggApi(ScanApi *factScan, std::vector<JoinApi*> &joins, bool isPreAgg) {
		this->factScan = factScan;
		this->joins = joins;
		this->isPreAgg = isPreAgg;

		initializeAgg();
		if(isPreAgg)
			reservePreAggVector();
	}

	~AggApi() {
		if(isPreAgg){
			free(bit_res);
		}
	}

	void printBitVector(uint64_t cur_result, int segId) {
		//print bit vals
		printf("%d: ", segId);
		for (int j = 0; j < 64; j++) {
			printf("%lu|", (cur_result & 1));
			cur_result >>= 1;
		}
		printf("\n");
	}

	table* FactTable(){
		return this->factScan->getBaseTable();
	}

	int TotalJoins(){
		return this->joins.size();
	}

	std::vector<JoinApi*> Joins(){
		return joins;
	}

	void ZipCreate(dax_context_t *ctx){
		for(JoinApi *curJoin : joins){
			table *t_ = (curJoin->dimensionScan->getBaseTable());
			table_meta &t_meta = (t_->t_meta);

			if(t_meta.hasAggKey){
				column &col = t_->columns[t_meta.groupByColId];
				column_encoder &c_encoder = col.encoder;

				dax_status_t status = dax_zip_create(ctx,
						c_encoder.n_syms, c_encoder.widths, (void *) c_encoder.symbols, &c_encoder.codec);
				printf("dax_zip_create status: %d\n", status);

				ExtractTest(ctx, col);
			}
		}
	}

	void ExtractTest(dax_context_t *ctx, column &cur_col){
		column_meta &c_meta = cur_col.c_meta;
		column_encoder &c_enc = cur_col.encoder;

		cout << "Col. size: " << c_meta.col_size << endl;
		cout << "# of syms: " << c_enc.n_syms << endl;

		cout << "widths: " << (int) c_enc.widths[0];
		cout << " " << (int) c_enc.widths[1];
		cout << " " << (int) c_enc.widths[2];
		cout << " " << (int) c_enc.widths[3];
		cout << endl;

		cout << "symbols: ";
		cout << ((uint32_t*) (c_enc.symbols))[0] << " ";
		cout << ((uint32_t*) (c_enc.symbols))[2] << " ";
		cout << ((uint32_t*) (c_enc.symbols))[4] << " ";
		cout << ((uint32_t*) (c_enc.symbols))[6] << " ";
		cout << endl;


		dax_vec_t src;
		src.format = DAX_ZIP;
		src.elements = c_meta.col_size;
		src.data = (void *) (cur_col.compressed);
		src.elem_width = c_enc.num_of_bits + 1;
		src.codec = c_enc.codec;
		src.offset = 0;
		//src.codewords = 0;

		dax_vec_t dst;
		dst.format = DAX_BYTES;
		dst.elements = src.elements;
		dst.elem_width = 4; //4 bytes for integers
		void *uncompressed = memalign(64, dst.elements * dst.elem_width);
		dst.data = uncompressed;
		dst.offset = 0;

		dax_result_t res = dax_extract(ctx, 0, &src, &dst);

		uint8_t *comp = (uint8_t *) (cur_col.compressed);
		uint32_t *i_dict = cur_col.encoder.i_dict;
		uint32_t *uncomp = (uint32_t *) dst.data;
		for(int ind = 0; ind < (int) dst.elements; ind++){
			cout << (int) comp[ind] << " (" << i_dict[comp[ind]] << ") "
					<< "ext: (" << uncomp[ind] << ") " << endl;
		}
		cout << endl;

		printf("\tExtract result: %d, ", res.status);
		printf("count: %lu\n", res.count);
		printf("# of elements: %lu\n", dst.elements);
		printf("Bit_size: %d\n", (int) src.elem_width);

		free(uncompressed);
	}

	void ZipRelease(dax_context_t *ctx){
		for(JoinApi *curJoin : joins){
			table *t_ = (curJoin->dimensionScan->getBaseTable());
			table_meta &t_meta = (t_->t_meta);

			if(t_meta.hasAggKey){
				column_encoder &c_encoder = t_->columns[t_meta.groupByColId].encoder;
				dax_zip_free(ctx, c_encoder.codec);
			}
		}
	}

	void pre_agg(Node<Query>* node, Result *result);
	void post_agg(Node<Query>* node, Result *result);
	void agg(Node<Query>* node, Result *result);
	void agg_q1(Node<Query>* node, Result *result);
	void agg_q1x(Node<Query>* node, Result *result);
	void agg_q1x_dax(Node<Query>* node, Result *result);
	void agg_q4(Node<Query>* node, Result *result);
};

#endif
