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
		Node<T> *work_unit;
		bool hasFilter = true;

		this->shared_queue->printQueue();
		//while (this->shared_queue->isNotEmpty()){ -- for q1_y
		while (!this->thr_sync->isQueryDone()){
			for (int i = 0; i < items_done; i++) {
				if (this->shared_queue->isNotEmpty()) {
					work_unit = this->shared_queue->nextElement();
					hasFilter = this->executeDaxItem(work_unit);

					//i -= (int) !hasFilter;
					if(hasFilter == false){
						i--;
						hasFilter = true;
					}
				}
				else if(this->thr_sync->isQueryDone()){
					break;
				}
			}

			items_done = pollDaxUnits(this->shared_queue);

			/*if(items_done < 0 && (this->shared_queue->isNotEmpty())){
				items_done = this->q_size;
			}*/
		}

		pollForLastItems(this->shared_queue);

		printf("DAX DONE!\n");
		this->thr_sync->waitOnAggBarrier();
	}

	void opAtaTime() {
		printf("Operator at-a-time Approach (Dax Handler)\n");

		WorkQueue<T> *scan_queue = this->shared_queue;
		WorkQueue<T> *join_queue = this->hw_queue;

		int items_done = this->q_size;
		Node<T> *node_item;
		bool hasFilter = true;

		while (scan_queue->isNotEmpty()) {
			for (int i = 0; i < items_done; i++) {
				if (scan_queue->isNotEmpty()) {
					node_item = scan_queue->nextElement();
					hasFilter = this->executeDaxItem(node_item);

					if(!hasFilter){
						i--;
					}
				}
				else{
					break;
				}
			}
			items_done = pollDaxUnits(join_queue);
		}

		pollForLastItems(join_queue);
		printf("DAX SCANS DONE, JOINS STARTING...\n");

		join_queue->printQueue();

		items_done = this->q_size;
		while (join_queue->isNotEmpty()) {
			for (int i = 0; i < items_done; i++) {
				if (join_queue->isNotEmpty()) {
					node_item = join_queue->nextElement();
					this->executeDaxItem(node_item);
				}
			}

			items_done = pollDaxUnits(join_queue);
		}

		pollForLastItems(join_queue);
		printf("DAX JOINS DONE!\n");
		this->thr_sync->waitOnAggBarrier();
	}

	void dataDivision() {
		printf("Data Division Approach (Dax Handler)\n");

		int items_done = this->q_size;
		Node<T> *node_item;
		bool hasFilter = true;

		while (this->hw_queue->isNotEmpty()) {
			for (int i = 0; i < items_done; i++) {
				if (this->hw_queue->isNotEmpty()) {
					node_item = this->hw_queue->nextElement();
					hasFilter = this->executeDaxItem(node_item);
					if(hasFilter == false){
						i--;
					}
				}
				else{
					break;
				}
			}
			items_done = pollDaxUnits(this->shared_queue);
		}

		printf("Polling last elements...\n");
		pollForLastItems(this->shared_queue);
		printf("Dax done with scans!\n");

		scheduleJoins();
		this->thr_sync->waitOnJoinBarrier();

		items_done = this->q_size;
		while (this->hw_queue->isNotEmpty()) {
			for (int i = 0; i < items_done; i++) {
				if (this->hw_queue->isNotEmpty()) {
					node_item = this->hw_queue->nextElement();
					this->executeDaxItem(node_item);
				}
			}

			items_done = pollDaxUnits(NULL);
		}

		pollForLastItems(NULL);
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
		//this->api_agg->ZipCreate(ctx);

		this->thr_sync->waitOnStartBarrier();

		if (this->eType == EXEC_TYPE::SDB || this->eType == EXEC_TYPE::REWRITE) {
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
		//this->api_agg->ZipRelease(ctx);

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

		/*dax_props_t props;
		res = dax_get_props(ctx, &props);
		if(res != 0)
			printf("Problem with DAX Props! Return code is %d.\n", res);
		*/
	}

	inline int pollDaxUnits(WorkQueue<T> *queue){
		dax_poll_t poll_data[this->q_size];
		int items_done = dax_poll(dax_queue, poll_data, this->q_size, 0);
		if (items_done > 0) {
			handlePollReturn(items_done, poll_data, queue);
		}
		return items_done;
	}

	inline void pollForLastItems(WorkQueue<T> *queue){
		int items_done = 0;
		while (items_done != dax_status_t::DAX_EQEMPTY) { // busy polling
			dax_poll_t poll_data[this->q_size];
			items_done = dax_poll(dax_queue, poll_data, 1, -1);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, queue);
			}
		}
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

			this->result.addRuntime(true, j_type, make_tuple(post_data->t_start, gethrtime(), j_type, p_id));
			this->result.addCountResult(make_tuple(j_type, poll_data[i].count));

			this->putFreeNode(node_item);

			//3) create follow up jobs if any
			if (Types::isFactScan(j_type) && this->eType != DD) {
				this->addNewJoins(p_id, work_queue);
			}
			else if(Types::isJoin(j_type)){
				this->thr_sync->joinPartCompleted(p_id);
				if(this->thr_sync->areJoinsDoneForPart(p_id)){
					if(this->eType == EXEC_TYPE::DD){
						this->addNewJob(0, p_id, JOB_TYPE::AGG, this->shared_queue);
					}
					else{
						this->addNewJob(0, p_id, JOB_TYPE::AGG, this->sw_queue);
					}
				}
			}
		}
	}

private:
	int q_size;
	bool async;

	dax_queue_t *dax_queue = NULL;
	dax_context_t *ctx = NULL;

	inline bool executeDaxItem(Node<Query> *work_unit){
		if(work_unit == NULL){
			return true;
		}
		else{
			bool isAsync = true;
			JOB_TYPE &j_type = work_unit->value.getJobType();
			if(Types::isScan(j_type)){
				isAsync = executeDaxScan(work_unit);
			}
			else if(Types::isJoin(j_type)){
				isAsync = executeDaxJoin(work_unit);
			}


			if(!isAsync){
				int p_id = work_unit->value.getPart();
				if(j_type == LO_SCAN){
					if(this->eType == SDB)
						this->addNewJoins(p_id, this->shared_queue);
					else if(this->eType == OAT){
						this->addNewJoins(p_id, this->hw_queue);
					}
				}
				else if(j_type == LP_JOIN){
					printf("Hello...\n");
					this->result.addRuntime(true, j_type, make_tuple(work_unit->t_start, gethrtime(), j_type, p_id));
					this->result.addCountResult(make_tuple(j_type, 0));

					this->putFreeNode(work_unit);
					this->addNewJob(0, p_id, JOB_TYPE::AGG, this->sw_queue);
				}
			}
			return isAsync;
		}
	}

	inline bool executeDaxScan(Node<Query> *work_unit){
		JOB_TYPE &j_type = work_unit->value.getJobType();

		if(Types::isFactScan(j_type)){
			return this->factScanAPI->hwScan(&dax_queue, work_unit);
		}
		else if(Types::isFact2Scan(j_type)){
			return this->factScanAPI2->hwScan(&dax_queue, work_unit);
		}
		else{
			return this->dimScanAPIs[j_type]->hwScan(&dax_queue, work_unit);
		}
	}

	inline bool executeDaxJoin(Node<Query> *work_unit){
		JOB_TYPE &j_type = work_unit->value.getJobType();
		if(j_type == LP_JOIN){
			this->joinAPIs[j_type]->hwJoinCp(&ctx, work_unit);
			return false;
		}
		else{
			this->joinAPIs[j_type]->hwJoin(&dax_queue, work_unit);
			return true;
		}
	}

	void scheduleJoins(){
		while (true) {
			if (this->sw_queue->isNotEmpty()) {
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
	}

#endif
};
//#endif
