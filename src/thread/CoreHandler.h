#ifndef __core_handler_h__
#define __core_handler_h__

#include <pthread.h>
#include <vector>
#include <atomic>

#include "Thread.h"
#include "ThreadHandler.h"
#include "Syncronizer.h"

#include "exec/util/WorkQueue.h"
#include "network/server/TCPAcceptor.h"
#include "log/Result.h"

#include "api/ScanApi.h"

#ifdef __sun
#include <sys/processor.h>
#include <sys/procset.h>
#include <thread.h>
#endif

template<class T>
class CoreHandler: public ThreadHandler<T> {

	void siliconDB() {
		Node<T> *node_item;
		T item;
		JOB_TYPE j_type;
		do {
			if (this->sw_queue->getHead() != this->sw_queue->getTail()) {
				node_item = this->sw_queue->remove_ref();
				if (node_item == NULL) {
					continue;
				}
				item = node_item->value;
				j_type = item.getJobType();
				if (j_type == JOB_TYPE::AGG) {
					this->api_agg->agg(node_item, &(this->result));
					this->putFreeNode(node_item);
					this->thr_sync->incrementAggCounter();
				}
			} else if (this->shared_queue->getHead()
					!= this->shared_queue->getTail()) {
				node_item = this->shared_queue->remove_ref();
				if (node_item == NULL)
					continue;
				item = node_item->value;
				j_type = item.getJobType();

				if (j_type == JOB_TYPE::LO_SCAN) {
					this->factScanAPI->simdScan16(node_item, &(this->result));
				} else if (j_type <= JOB_TYPE::P_SCAN) {
					this->dimScanAPIs[j_type]->simdScan16(node_item,
							&(this->result));
				} else if (j_type <= JOB_TYPE::LD_JOIN) {
					this->joinAPIs[j_type - 11]->swJoin(node_item,
							&(this->result));
				}

				this->putFreeNode(node_item);
				if (j_type == JOB_TYPE::LO_SCAN) {
					this->addNewJob(1, item.getPart(), JOB_TYPE::LS_JOIN,
							this->shared_queue);
					this->addNewJob(1, item.getPart(), JOB_TYPE::LC_JOIN,
							this->shared_queue);
					this->addNewJob(1, item.getPart(), JOB_TYPE::LD_JOIN,
							this->shared_queue);
				} else if (j_type == JOB_TYPE::LD_JOIN) {
					this->addNewJob(1, item.getPart(), JOB_TYPE::AGG,
							this->sw_queue);
				}
			}
		} while (!this->thr_sync->isQueryDone());

	}

	void opAtaTime() {
		Node<T> *node_item;
		T item;
		JOB_TYPE j_type;
		//int p_id = -1;
		while (!this->thr_sync->isQueryDone()) {
			if (this->sw_queue->getHead() != this->sw_queue->getTail()) {
				node_item = this->sw_queue->remove_ref();
				if (node_item == NULL) {
					continue;
				}
				item = node_item->value;
				j_type = item.getJobType();
				//p_id = item.getPart();
				//printf("    (s_agg)(p: %d)\n", p_id);
				if (j_type == JOB_TYPE::AGG) {
					this->api_agg->agg(node_item, &(this->result));
					this->putFreeNode(node_item);
					this->thr_sync->incrementAggCounter();
				}
				//printf("    (e_agg)(p: %d)\n", p_id);
			}
		}
	}

	void dataDivision() {
		Node<T> *node_item;
		T item;
		JOB_TYPE j_type;

		while (this->sw_queue->getHead() != this->sw_queue->getTail()) {
			node_item = this->sw_queue->remove_ref();
			if (node_item == NULL)
				continue;
			item = node_item->value;
			j_type = item.getJobType();

			if (j_type == JOB_TYPE::LO_SCAN) {
				this->factScanAPI->simdScan16(node_item, &(this->result));
			} else if (j_type <= JOB_TYPE::P_SCAN) {
				this->dimScanAPIs[j_type]->simdScan16(node_item,
						&(this->result));
			}
			this->putFreeNode(node_item);
		}
		//printf("Core done with scans!\n");

		this->thr_sync->waitOnJoinBarrier();

		while (this->sw_queue->getHead() != this->sw_queue->getTail()) {
			node_item = this->sw_queue->remove_ref();
			if (node_item == NULL)
				continue;
			item = node_item->value;
			j_type = item.getJobType();

			if (j_type <= JOB_TYPE::LD_JOIN) {
				this->joinAPIs[j_type - 11]->swJoin(node_item, &(this->result));
			}
			this->putFreeNode(node_item);
		}
		//printf("Core done with joins!\n");

		this->thr_sync->waitOnAggBarrier();

		while (this->sw_queue->getHead() != this->sw_queue->getTail()) {
			node_item = this->sw_queue->remove_ref();
			if (node_item == NULL) {
				continue;
			}
			item = node_item->value;
			j_type = item.getJobType();
			if (j_type == JOB_TYPE::AGG) {
				this->api_agg->agg(node_item, &(this->result));
				this->putFreeNode(node_item);
				this->thr_sync->incrementAggCounter();
			}
		}
	}

public:
	CoreHandler(Syncronizer *sync, EXEC_TYPE e_type) :
			ThreadHandler<T>(sync, e_type) {
	}

	void *run() {
		this->thr_sync->waitOnStartBarrier();

		if (this->eType == EXEC_TYPE::SDB) {
			this->siliconDB();
			this->thr_sync->waitOnAggBarrier();
		} else if (this->eType == EXEC_TYPE::OAT) {
			this->thr_sync->waitOnAggBarrier();
			this->opAtaTime();
		} else if (this->eType == EXEC_TYPE::DD) {
			this->dataDivision();
		}
		this->thr_sync->waitOnEndBarrier();

		//printf("Core resources releasing ...\n");
		this->t_stream->send("", 0);
		return NULL;
	}

	void setSSWQueue(WorkQueue<T> *queue) {
		this->sw_queue = queue;
	}

	void finished(int t_id, int p_id) {
		if (t_id == -1) {
			printf("    (ls)(p: %d)\n", p_id);
		} else if (t_id == 0) {
			printf("    (ss)(p: %d)\n", p_id);
		} else if (t_id == 1) {
			printf("    (cs)(p: %d)\n", p_id);
		} else if (t_id == 2) {
			printf("    (ds)(p: %d)\n", p_id);
		} else if (t_id == 11) {
			printf("    (jc)(p: %d)\n", p_id);
		} else if (t_id == 12) {
			printf("    (js)(p: %d)\n", p_id);
		} else if (t_id == 13) {
			printf("    (jd)(p: %d)\n", p_id);
		}
	}

};

#endif

