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
		//int p_id = -1;

		//return;
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
						//printf("fact scan...\n");
						this->factScanAPI->hwScan(&dax_queue, node_item);
					}
					else if(j_type <= JOB_TYPE::P_SCAN){
						//printf("Join (%d)\n", j_type);
						this->dimScanAPIs[j_type]->hwScan(&dax_queue, node_item);
					}
					else if(j_type <= JOB_TYPE::LD_JOIN){
						//printf("Join (%d)\n", j_type);
						this->joinAPIs[j_type - 11]->hwJoin(&dax_queue, node_item);
					}
					//this->shared_queue->printQueue();
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

		printf("DAX DONE! (%d), %d\n", items_done, (int) this->daxDone);
		this->thr_sync->waitOnAggBarrier();
	}

	void opAtaTime() {
		printf("Operator at-a-time Approach (Dax Handler)\n");

		WorkQueue<T> *scan_queue = this->shared_queue;
		WorkQueue<T> *join_queue = this->hw_queue;

		int items_done = this->q_size;
		Node<T> *node_item;
		int j_type = -1;
		//int p_id = -1;

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
						this->factScanAPI->hwScan(&dax_queue, node_item);
					}
					else if(j_type <= JOB_TYPE::P_SCAN){
						this->dimScanAPIs[j_type]->hwScan(&dax_queue, node_item);
					}
					//scan_queue->printQueue();
				}
			}

			dax_poll_t poll_data[this->q_size];
			items_done = dax_poll(dax_queue, poll_data, -1, 0);
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
					if(j_type <= JOB_TYPE::LD_JOIN){
						this->joinAPIs[j_type - 11]->hwJoin(&dax_queue, node_item);
					}
				}
			}

			dax_poll_t poll_data[this->q_size];
			items_done = dax_poll(dax_queue, poll_data, -1, 0);
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
		printf("DAX JOINS DONE! (%d), %d\n", items_done, (int) this->daxDone);
		this->thr_sync->waitOnAggBarrier();
	}

	void dataDivision() {
		printf("Data Division Approach (Dax Handler)\n");

		int items_done = this->q_size;
		Node<T> *node_item;
		int j_type = -1;
		//int p_id = -1;

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
						this->factScanAPI->hwScan(&dax_queue, node_item);
					}
					else if(j_type <= JOB_TYPE::P_SCAN){
						this->dimScanAPIs[j_type]->hwScan(&dax_queue, node_item);
					}
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
				int daxJoins = totalJoinItems / (1.70 + 1) * 1.70;

				printf("Dax will do %d joins out of %d in total!\n", daxJoins, totalJoinItems);

				int p_id, j_id;
				JOB_TYPE j_type;
				for(int ind = 0; ind < totalJoinItems; ind++){
					p_id = ind % totalFactParts; //parj_type
					j_id = ind / totalFactParts; //join_id

					if(j_id == 0)
						j_type = JOB_TYPE::LC_JOIN;
					else if(j_id == 1)
						j_type = JOB_TYPE::LS_JOIN;
					else if(j_id == 2)
						j_type = JOB_TYPE::LD_JOIN;

					if(ind < daxJoins)
						this->addNewJob(0, p_id, j_type, this->hw_queue);
					else
						this->addNewJob(0, p_id, j_type, this->sw_queue);
				}

				/*printf("DAX Join Queue\n");
				this->hw_queue->printQueue();

				printf("Core Join Queue\n");
				this->sw_queue->printQueue();*/
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
						this->joinAPIs[j_type - 11]->hwJoin(&dax_queue, node_item);
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

		while (true) {
			if (this->sw_queue->getHead() != this->sw_queue->getTail()) {
				continue;
			} else {
				int totalFactParts = this->factScanAPI->totalParts();
				for(int p_id = 0; p_id < totalFactParts; p_id++)
					this->addNewJob(0, p_id, JOB_TYPE::AGG, this->sw_queue);

				//printf("Agg. Queue\n");
				//this->sw_queue->printQueue();

				break;
			}
		}
		this->thr_sync->waitOnAggBarrier();
	}

public:
	DaxHandler(Syncronizer *sync, int size, bool isAsync, EXEC_TYPE e_type) : ThreadHandler<T>(sync, e_type) {
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
		//int lFile = open("/tmp/dax_log.txt", O_RDWR);

		dax_status_t res = dax_thread_init(1, 1, 0, NULL, &ctx);
		if (res != 0)
			printf("Problem with DAX Context Creation! Return code is %d.\n",
					res);

		printf("DAX Queue Size: %d\n", this->q_size);
		res = dax_queue_create(ctx, this->q_size, &dax_queue);
		if (res != 0)
			printf("Problem with DAX Queue Creation! Return code is %d.\n",
					res);

		//res = dax_set_log_file(ctx, DAX_LOG_ERROR | DAX_LOG_WARNING, lFile);
		//if(res != 0)
		//printf("Problem with DAX Logger Creation! Return code is %d.\n", res);

		//res = dax_set_debug(ctx, DAX_DEBUG_ALL);
		//if(res != 0)
		//printf("Problem with DAX Debug Creation! Return code is %d.\n", res);

		//dax_props_t props;
		//res = dax_get_props(ctx, &props);
		//if(res != 0)
		//printf("Problem with DAX Props! Return code is %d.\n", res);
	}

	inline void handlePollReturn(int items_done, dax_poll_t *poll_data,
			WorkQueue<T>* work_queue) {
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
			if (j_type <= JOB_TYPE::P_SCAN) {
				this->result.addRuntime(true, j_type,
						make_tuple(post_data->t_start, gethrtime(), j_type,
								p_id));
			} else{ //must be a join
				this->result.addRuntime(true, j_type,
						make_tuple(post_data->t_start, gethrtime(), j_type,
								p_id));
			}
			//2) log count results
			this->result.addCountResult(make_tuple(j_type, poll_data[i].count));

			this->putFreeNode(node_item);
			//3) create follow up jobs if any
			if (j_type == JOB_TYPE::LO_SCAN && this->eType != EXEC_TYPE::DD) {
				//this->api_ls->incrementCounter();
				this->addNewJob(0, p_id, JOB_TYPE::LC_JOIN, work_queue);
				this->addNewJob(0, p_id, JOB_TYPE::LS_JOIN, work_queue);
				this->addNewJob(0, p_id, JOB_TYPE::LD_JOIN, work_queue);
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

	void setHHWQueue(WorkQueue<T> *queue) {
		this->hw_queue->printQueue();
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
