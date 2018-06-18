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
	void startExec() {
		Node<T> *node_item;
		T item;
		int t_id = -1;
		//int p_id = -1;

		while (!this->thr_sync->isQueryDone()) {
			if (this->sw_queue->getHead() != this->sw_queue->getTail()) {
				node_item = this->sw_queue->remove_ref();
				if (node_item == NULL) {
					continue;
				}
				item = node_item->value;
				t_id = item.getTableId();
				//p_id = item.getPart();
				//printf("    (s_agg)(p: %d)\n", p_id);
				if (t_id == 21) {
					this->api_agg->agg(node_item, &(this->result));
					this->putFreeNode(node_item);
					this->thr_sync->incrementAggCounter();
				}
				//printf("    (e_agg)(p: %d)\n", p_id);
			} else if (this->shared_queue->getHead()
					!= this->shared_queue->getTail()) {
				node_item = this->shared_queue->remove_ref();
				if (node_item == NULL)
					continue;
				item = node_item->value;
				t_id = item.getTableId();
				//p_id = item.getPart();

				//printf("(s')(%d, %d)\n", t_id, p_id);
				if (t_id == 0) {
					this->api_ls->simdScan16(node_item, &(this->result));
				} else if (t_id == 1) {
					this->api_ss->simdScan16(node_item, &(this->result));
				} else if (t_id == 2) {
					this->api_cs->simdScan16(node_item, &(this->result));
				} else if (t_id == 3) {
					this->api_ds->simdScan16(node_item, &(this->result));
				} else if (t_id == 11) {
					//printf("   (s_js')(%d)\n", p_id);
					this->j1_lc->swJoin(node_item, &(this->result));
				} else if (t_id == 12) {
					//printf("   (s_jc')(%d)\n", p_id);
					this->j2_ls->swJoin(node_item, &(this->result));
				} else if (t_id == 13) {
					//printf("   (s_jd')(%d)\n", p_id);
					this->j3_ld->swJoin(node_item, &(this->result));
				}
				//printf("(e')(%d, %d)\n", t_id, p_id);
				//finished(t_id, p_id);
				this->putFreeNode(node_item);
				if (t_id == 0) {
					this->addNewJob(1, item.getPart(), 11, this->shared_queue);
					this->addNewJob(1, item.getPart(), 12, this->shared_queue);
					this->addNewJob(1, item.getPart(), 13, this->shared_queue);
				} else if (t_id == 13) {
					this->addNewJob(1, item.getPart(), 21, this->sw_queue);
				}
			}

			//this->api_ds->incrementCounter();
			//this->j1_lc->setExecFlag(item.getPart());
		}
	}

	void opAtaTime() {
		Node<T> *node_item;
		T item;
		int t_id = -1;
		//int p_id = -1;
		while (!this->thr_sync->isQueryDone()) {
			if (this->sw_queue->getHead() != this->sw_queue->getTail()) {
				node_item = this->sw_queue->remove_ref();
				if (node_item == NULL) {
					continue;
				}
				item = node_item->value;
				t_id = item.getTableId();
				//p_id = item.getPart();
				//printf("    (s_agg)(p: %d)\n", p_id);
				if (t_id == 21) {
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
		int t_id = -1;

		while (this->sw_queue->getHead() != this->sw_queue->getTail()) {
			node_item = this->sw_queue->remove_ref();
			if (node_item == NULL)
				continue;
			item = node_item->value;
			t_id = item.getTableId();

			if (t_id == 0) {
				this->api_ls->simdScan16(node_item, &(this->result));
			} else if (t_id == 1) {
				this->api_ss->simdScan16(node_item, &(this->result));
			} else if (t_id == 2) {
				this->api_cs->simdScan16(node_item, &(this->result));
			} else if (t_id == 3) {
				this->api_ds->simdScan16(node_item, &(this->result));
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
			t_id = item.getTableId();

			if (t_id == 11) {
				this->j1_lc->swJoin(node_item, &(this->result));
			} else if (t_id == 12) {
				this->j2_ls->swJoin(node_item, &(this->result));
			} else if (t_id == 13) {
				this->j3_ld->swJoin(node_item, &(this->result));
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
			t_id = item.getTableId();
			if (t_id == 21) {
				this->api_agg->agg(node_item, &(this->result));
				this->putFreeNode(node_item);
				this->thr_sync->incrementAggCounter();
			}
		}
	}

public:
	CoreHandler(Syncronizer *sync, WorkQueue<T> *shared_queue,
			std::vector<DataCompressor*> &dataVector) :
			ThreadHandler<T>(sync, shared_queue, dataVector) {
	}

	void *run() {
		this->thr_sync->initAggCounter(this->api_ls->totalParts());
		this->thr_sync->waitOnStartBarrier();

		if (this->eType == EXEC_TYPE::SDB) {
			this->startExec();
			this->thr_sync->waitOnAggBarrier();
		} else if (this->eType == EXEC_TYPE::OAT) {
			this->thr_sync->waitOnAggBarrier();
			this->opAtaTime();
		} else if (this->eType == EXEC_TYPE::DD) {
			this->dataDivision();
		}

		this->thr_sync->waitOnEndBarrier();

		printf("Core resources releasing ...\n");
		this->t_stream->send("", 0);
		return NULL;
	}

	void setSWQueue(WorkQueue<T> *queue) {
		this->sw_queue = queue;
	}

	void finished(int t_id, int p_id) {
		if (t_id == 0) {
			printf("    (ls)(p: %d)\n", p_id);
		} else if (t_id == 1) {
			printf("    (ss)(p: %d)\n", p_id);
		} else if (t_id == 2) {
			printf("    (cs)(p: %d)\n", p_id);
		} else if (t_id == 3) {
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

