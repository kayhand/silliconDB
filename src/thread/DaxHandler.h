#ifndef __dax_handler_h__
#define __dax_handler_h__

#include <pthread.h>
#include <vector>
#include <atomic>

#include "Thread.h"
#include "ThreadHandler.h"
#include "Syncronizer.h"

#include "sched/ProcessingUnit.h"

#include "exec/util/WorkQueue.h"
#include "network/server/TCPAcceptor.h"
#include "log/Result.h"

#ifdef __sun 
#include <sys/processor.h>
#include <sys/procset.h>
#include <thread.h>
#endif

template<class T>
class DaxHandler: public ThreadHandler<T> {
//#ifdef __sun

	void siliconDB() {
		int items_done = this->q_size;
		printf("Start Items: %d\n", this->q_size);
		Node<T> *node_item;
		JOB_TYPE j_type;

		bool hasFilter = true;
		int p_id;

		while (this->shared_queue->getHead() != this->shared_queue->getTail()) {
			for (int i = 0; i < items_done; i++) {
				if (this->shared_queue->getHead() != this->shared_queue->getTail()) {
					node_item = this->shared_queue->remove_ref();
					if (node_item == NULL) {
						printf("Null node...\n");
						continue;
					}
					j_type = node_item->value.getJobType();
					if(j_type == JOB_TYPE::LO_SCAN){
						hasFilter = this->factScanAPI->hwScan(&dax_queue, node_item);
					}
					else if (j_type == JOB_TYPE::LO_SCAN_2) {
						hasFilter = this->factScanAPI2->hwScan(&dax_queue, node_item);
					}
					else if (j_type >= S_SCAN && j_type <= D_SCAN) {
						//printf("Scan (%d)\n", j_type);
						hasFilter = this->dimScanAPIs[j_type]->hwScan(&dax_queue, node_item);
					}
					else if (j_type >= LS_JOIN && j_type <= LD_JOIN) {
						//printf("Join (%d)\n", j_type);
						this->joinAPIs[j_type]->hwJoin(&dax_queue, node_item);
					}

					if(hasFilter == false){
						i--;
						if(j_type == LO_SCAN){
							p_id = node_item->value.getPart();
							this->addNewJoins(p_id, this->shared_queue);
						}
						hasFilter = true;
					}
					//this->shared_queue->printQueue();
				}
				else{
					break;
				}
			}

			dax_poll_t poll_data[this->q_size];
			items_done = dax_poll(dax_queue, poll_data, -1, 0);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, this->shared_queue);
				//break;
			}
		}

		//work_queue->printQueue();
		items_done = 0;
		while (items_done != dax_status_t::DAX_EQEMPTY) { // busy polling
			dax_poll_t poll_data[this->q_size];
			items_done = dax_poll(dax_queue, poll_data, 1, -1);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, this->shared_queue);
			}
		}

		printf("DAX DONE! (%d)\n", items_done);
		this->thr_sync->waitOnAggBarrier();
	}

	void opAtaTime() {
		printf("Operator at-a-time Approach (Dax Handler)\n");

		WorkQueue<T> *scan_queue = this->shared_queue;
		WorkQueue<T> *join_queue = this->hw_queue;

		int items_done = this->q_size;
		Node<T> *node_item;
		int j_type = -1;

		bool hasFilter = true;
		int p_id;

		while (scan_queue->getHead() != scan_queue->getTail()) {
			for (int i = 0; i < items_done; i++) {
				if (scan_queue->getHead() != scan_queue->getTail()) {
					node_item = scan_queue->remove_ref();
					if (node_item == NULL) {
						printf("Null node...\n");
						continue;
					}
					j_type = node_item->value.getJobType();
					if(j_type == JOB_TYPE::LO_SCAN){
						hasFilter = this->factScanAPI->hwScan(&dax_queue, node_item);
						if(!hasFilter){
							//printf("Fact scan\n");
							p_id = node_item->value.getPart();
							this->addNewJoins(p_id, join_queue);
							i--;
						}
					}
					else if(j_type == JOB_TYPE::LO_SCAN_2){
						this->factScanAPI2->hwScan(&dax_queue, node_item);
					}
					else if(j_type <= JOB_TYPE::D_SCAN){
						hasFilter = this->dimScanAPIs[j_type]->hwScan(&dax_queue, node_item);
						if(!hasFilter){
							i--;
							this->dimScanAPIs[j_type]->PartDone();
						}
					}
					//scan_queue->printQueue();
				}
				else{
					break;
				}
			}

			dax_poll_t poll_data[this->q_size];
			items_done = dax_poll(dax_queue, poll_data, this->q_size, 0);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, join_queue);
			}
		}

		items_done = 0;
		while (items_done != dax_status_t::DAX_EQEMPTY) { // busy polling
			dax_poll_t poll_data[this->q_size];
			items_done = dax_poll(dax_queue, poll_data, 1, -1);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, join_queue);
			}
		}
		printf("DAX SCANS DONE, JOINS STARTING...\n");

		join_queue->printQueue();

		items_done = this->q_size;
		while (join_queue->getHead() != join_queue->getTail()) {
			for (int i = 0; i < items_done; i++) {
				if (join_queue->getHead() != join_queue->getTail()) {
					node_item = join_queue->remove_ref();
					if (node_item == NULL) {
						printf("Null node...\n");
						continue;
					}
					j_type = node_item->value.getJobType();
					//printf("Join with id: %d\n", j_type);
					if(j_type <= JOB_TYPE::LD_JOIN){
						this->joinAPIs[j_type]->hwJoin(&dax_queue, node_item);
					}
				}
			}

			dax_poll_t poll_data[this->q_size];
			items_done = dax_poll(dax_queue, poll_data, this->q_size, 0);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, join_queue);
			}
			//join_queue->printQueue();
		}

		items_done = 0;
		while (items_done != dax_status_t::DAX_EQEMPTY) { // busy polling
			dax_poll_t poll_data[this->q_size];
			items_done = dax_poll(dax_queue, poll_data, 1, -1);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, join_queue);
			}
		}
		printf("DAX JOINS DONE! (%d)\n", items_done);
		this->thr_sync->waitOnAggBarrier();
	}

	void dataDivision() {
		printf("Data Division Approach (Dax Handler)\n");

		int items_done = this->q_size;
		Node<T> *node_item;
		int j_type = -1;
		bool hasFilter = true;

		while (this->hw_queue->getHead() != this->hw_queue->getTail()) {
			for (int i = 0; i < items_done; i++) {
				if (this->hw_queue->getHead() != this->hw_queue->getTail()) {
					node_item = this->hw_queue->remove_ref();
					if (node_item == NULL) {
						printf("Null node...\n");
						continue;
					}
					j_type = node_item->value.getJobType();

					if(j_type == JOB_TYPE::LO_SCAN){
						hasFilter = this->factScanAPI->hwScan(&dax_queue, node_item);
					}
					else if(j_type == JOB_TYPE::LO_SCAN_2){
						hasFilter = this->factScanAPI2->hwScan(&dax_queue, node_item);
					}
					else if(j_type <= JOB_TYPE::D_SCAN){
						hasFilter = this->dimScanAPIs[j_type]->hwScan(&dax_queue, node_item);
					}
					if(hasFilter == false){
						i--;
					}
					//this->hw_queue->printQueue();
				}
				else{
					break;
				}
			}

			dax_poll_t poll_data[this->q_size];
			items_done = dax_poll(dax_queue, poll_data, -1, 0);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, this->shared_queue);
			}
		}

		items_done = 0;
		while (items_done != dax_status_t::DAX_EQEMPTY) { // busy polling
			dax_poll_t poll_data[this->q_size];
			items_done = dax_poll(dax_queue, poll_data, 1, -1);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, this->shared_queue);
			}
		}
		printf("Dax done with scans!\n");

		while (true) {
			if (this->sw_queue->getHead() != this->sw_queue->getTail()) {
				continue;
			} else {
				int totalFactParts = this->factScanAPI->totalParts();
				int totalJoinItems = this->api_agg->TotalJoins() * totalFactParts;

				int coreJoins = (int) ceil(totalJoinItems / (1.70 + 1) * 1.70);
				int daxJoins = totalJoinItems - coreJoins;

				printf("Dax will do %d joins out of %d in total!\n", daxJoins, totalJoinItems);

				int p_id, j_id;
				int ind = 0;
				JOB_TYPE j_type;
				while(ind < totalJoinItems){
					p_id = ind % totalFactParts; //partition_id
					j_id = ind / totalFactParts; //join_type

					j_type = this->api_agg->Joins()[j_id]->JoinType();
					if(j_type == LP_JOIN || coreJoins > 0){
						this->addNewJob(0, p_id, j_type, this->sw_queue);
						coreJoins--;
					}
					else{
						this->addNewJob(0, p_id, j_type, this->hw_queue);
						daxJoins--;
					}
					ind++;
				}
				cout << coreJoins << " " << daxJoins << endl;

				printf("\nDAX Join Queue\n");
				this->hw_queue->printQueue();

				printf("\nCore Join Queue\n");
				this->sw_queue->printQueue();

				break;
			}
		}

		this->thr_sync->waitOnJoinBarrier();

		items_done = this->q_size;
		while (this->hw_queue->getHead() != this->hw_queue->getTail()) {
			for (int i = 0; i < items_done; i++) {
				if (this->hw_queue->getHead() != this->hw_queue->getTail()) {
					node_item = this->hw_queue->remove_ref();
					if (node_item == NULL) {
						printf("Null node...\n");
						continue;
					}
					j_type = node_item->value.getJobType();
					if(j_type <= JOB_TYPE::LD_JOIN){
						this->joinAPIs[j_type]->hwJoin(&dax_queue, node_item);
					}
				}
			}

			dax_poll_t poll_data[this->q_size];
			items_done = dax_poll(dax_queue, poll_data, -1, 0);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, NULL);
			}
		}

		items_done = 0;
		while (items_done != dax_status_t::DAX_EQEMPTY) { // busy polling
			dax_poll_t poll_data[this->q_size];
			items_done = dax_poll(dax_queue, poll_data, 1, -1);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, NULL);
			}
		}

		printf("Dax done with joins!\n");
		this->thr_sync->waitOnJoinEndBarrier();

		int totalFactParts = this->factScanAPI->totalParts();
		for(int p_id = 0; p_id < totalFactParts; p_id++)
			this->addNewJob(0, p_id, JOB_TYPE::AGG, this->sw_queue);

		printf("Agg. Queue -- %d elements in total!\n", totalFactParts);
		this->sw_queue->printQueue();

		this->thr_sync->waitOnAggBarrier();
	}

public:
	DaxHandler(Syncronizer *sync, int size, bool isAsync, EXEC_TYPE e_type, int thr_id) : ThreadHandler<T>(sync, e_type, thr_id) {
		this->q_size = size;
		async = isAsync;
		if (!async)
			this->q_size = 1;
	}

	void *run() {
		createDaxContext();
		this->thr_sync->waitOnStartBarrier();

		if (this->eType == EXEC_TYPE::SDB) {
			printf("Starting sdb in DAX!\n");
			this->siliconDB();
		} else if (this->eType == EXEC_TYPE::OAT) {
			printf("Starting op-at-a-time in DAX!\n");
			this->opAtaTime();
		} else if (this->eType == EXEC_TYPE::DD) {
			printf("Starting data-division in DAX!\n");
			this->dataDivision();
		}

		printf("Dax is done...\n");
		//this->thr_sync->waitOnAggBarrier();
		this->thr_sync->waitOnEndBarrier();
		//printf("Dax resources releasing ...\n");

		//printf("jc | js | jd \n");
		//for(int i = 0; i < jc_flags.size(); i++){
		//cout << jc_flags[i] << " | " << js_flags[i] << " | " << jd_flags[i] << endl;
		//}
		this->t_stream->send("", 0);
		return NULL;
	}

	void createDaxContext() {
		int lFile = open("/tmp/dax_log.txt", O_RDWR);

		dax_status_t res = dax_thread_init(1, 1, 0, NULL, &ctx);
		if (res != 0)
			printf("Problem with DAX Context Creation! Return code is %d.\n", res);

		res = dax_queue_create(ctx, this->q_size, &dax_queue);
		if (res != 0)
			printf("Problem with DAX Queue Creation! Return code is %d.\n",
					res);

		res = dax_set_log_file(ctx, DAX_LOG_ERROR, lFile);
		if(res != 0)
			printf("Problem with DAX Logger Creation! Return code is %d.\n", res);

		//res = dax_set_debug(ctx, DAX_DEBUG_PERF);
		//if(res != 0)
			//printf("Problem with DAX Debug Creation! Return code is %d.\n", res);

		dax_props_t props;
		res = dax_get_props(ctx, &props);
		if(res != 0)
			printf("Problem with DAX Props! Return code is %d.\n", res);
		cout << "Best bitmap align:" <<props.trans_bitmap_align_best << endl;
	}

	inline void handlePollReturn(int items_done, dax_poll_t *poll_data,
			WorkQueue<T> *work_queue) {
		Node<T> *node_item;
		JOB_TYPE j_type;
		int p_id;
		for (int i = 0; i < items_done; i++) {
			q_udata *post_data = (q_udata*) (poll_data[i].udata);
			node_item = (Node<T> *) (post_data->node_ptr);

			j_type = node_item->value.getJobType();
			p_id = node_item->value.getPart();
			//finished(j_type, p_id);

			//1) log runtimes
			if (j_type <= JOB_TYPE::D_SCAN) {
				this->result.addRuntime(true, j_type,
						make_tuple(post_data->t_start, gethrtime(), j_type, p_id));
				if(j_type > LO_SCAN){
					this->dimScanAPIs[j_type]->PartDone();
				}
			} else{ //must be a join
				this->result.addRuntime(true, j_type,
						make_tuple(post_data->t_start, gethrtime(), j_type, p_id));
			}
			//2) log count results
			this->result.addCountResult(make_tuple(j_type, poll_data[i].count));

			this->putFreeNode(node_item);
			//3) create follow up jobs if any
			if (j_type == JOB_TYPE::LO_SCAN && this->eType != EXEC_TYPE::DD) {
				//this->api_ls->incrementCounter();
				this->addNewJoins(p_id, work_queue);
				//printf("adding joins after filter...\n");
				//work_queue->printQueue();
			} else if (j_type == JOB_TYPE::LD_JOIN) {
				if(this->eType == EXEC_TYPE::DD){
					this->addNewJob(0, p_id, JOB_TYPE::AGG, this->shared_queue);
				}
				else{
					this->addNewJob(0, p_id, JOB_TYPE::AGG, this->sw_queue);
 				}
			}

			/*if(j_type == LC_JOIN){
				uint64_t* join_result = (uint64_t *) this->j1_lc->getJoinBitVector(p_id);
				printf("j1 partition: %d\n", p_id);
				this->j1_lc->printBitVector(join_result[6], 7);
			}
			else if(j_type == LS_JOIN){
				uint64_t* join_result = (uint64_t *) this->j2_ls->getJoinBitVector(p_id);
				printf("j2 partition: %d\n", p_id);
				this->j2_ls->printBitVector(join_result[6], 7);
			}
			else if(j_type == LD_JOIN){
				uint64_t* join_result = (uint64_t *) this->j3_ld->getJoinBitVector(p_id);
				printf("j3 partition: %d\n", p_id);
				this->j3_ld->printBitVector(join_result[6], 7);
			}*/

			//printf("%lu for part: %d\n", poll_data[i].count, node_item->value.getPart());
		}

		//this->api_ss->incrementCounter();
		//this->j2_ls->setExecFlag(p_id);
	}

	void finished(int j_type, int p_id) {
		if (j_type == 0) {
			printf("(e_ls)(p: %d)\n", p_id);
		} else if (j_type == 1) {
			printf("(e_ss)(p: %d)\n", p_id);
		} else if (j_type == 2) {
			printf("(e_cs)(p: %d)\n", p_id);
		} else if (j_type == 3) {
			printf("(e_ds)(p: %d)\n", p_id);
		} else if (j_type == LC_JOIN) {
			printf("(e_jc)(p: %d)\n", p_id);
			//jc_flags[p_id]--;
		} else if (j_type == LS_JOIN) {
			printf("(e_js)(p: %d)\n", p_id);
			//js_flags[p_id]--;
		} else if (j_type == LD_JOIN) {
			printf("(e_jd)(p: %d)\n", p_id);
			//jd_flags[p_id]--;
		}
	}
private:
	int q_size;
	bool async;

	dax_queue_t *dax_queue = NULL;
	dax_context_t *ctx = NULL;

#endif
};
//#endif
