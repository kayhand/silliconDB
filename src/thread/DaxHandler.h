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
	dax_queue_t *dax_queue;
	int q_size;
	bool async;
	dax_context_t *ctx;
	//WorkQueue<T> *shared_queue;
	WorkQueue<T> *scan_queue;
	WorkQueue<T> *join_queue;

	void startExec() {
		//shared_queue = this->shared_queue;
		int items_done = q_size;
		Node<T> *node_item;
		int t_id = -1;
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
					t_id = node_item->value.getTableId();
					//p_id = node_item->value.getPart();
					if (t_id == 0) {
						//printf("(s_ls)(p: %d)\n", p_id);
						this->api_ls->hwScan(&dax_queue, node_item);
					} else if (t_id == 1) {
						//printf("(s_ss)(p: %d)\n", p_id);
						this->api_ss->hwScan(&dax_queue, node_item);
					} else if (t_id == 2) {
						//printf("(s_cs)(p: %d)\n", p_id);
						this->api_cs->hwScan(&dax_queue, node_item);
					} else if (t_id == 3) {
						//printf("(s_ds)(p: %d)\n", p_id);
						this->api_ds->hwScan(&dax_queue, node_item);
					} else if (t_id == 11) {
						//printf("(s_jc)(p: %d)\n", p_id);
						this->j1_lc->hwJoin(&dax_queue, node_item);
						//jc_flags[p_id] = 1;
					} else if (t_id == 12) {
						//printf("(s_js)(p: %d)\n", p_id);
						this->j2_ls->hwJoin(&dax_queue, node_item);
						//js_flags[p_id] = 1;
					} else if (t_id == 13) {
						//printf("(s_jd)(p: %d)\n", p_id);
						this->j3_ld->hwJoin(&dax_queue, node_item);
						//jd_flags[p_id] = 1;
					}
				}
			}

			dax_poll_t poll_data[q_size];
			items_done = dax_poll(dax_queue, poll_data, -1, 0);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, this->shared_queue);
				//break;
			}
		}

		//work_queue->printQueue();
		items_done = 0;
		while (items_done != dax_status_t::DAX_EQEMPTY) { // busy polling
			dax_poll_t poll_data[q_size];
			items_done = dax_poll(dax_queue, poll_data, 1, -1);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, this->shared_queue);
			}
		}

		printf("DAX DONE! (%d), %d\n", items_done, (int) this->daxDone);

	}

	void opAtaTime() {
		scan_queue = this->shared_queue;
		join_queue = this->hw_queue;

		int items_done = q_size;
		Node<T> *node_item;
		int t_id = -1;
		//int p_id = -1;

		while (scan_queue->getHead() != scan_queue->getTail()) {
			for (int i = 0; i < items_done; i++) {
				if (scan_queue->getHead() != scan_queue->getTail()) {
					node_item = scan_queue->remove_ref();
					if (node_item == NULL) {
						printf("Null node...\n");
						continue;
					}
					t_id = node_item->value.getTableId();
					if (t_id == 0) {
						this->api_ls->hwScan(&dax_queue, node_item);
					} else if (t_id == 1) {
						this->api_ss->hwScan(&dax_queue, node_item);
					} else if (t_id == 2) {
						this->api_cs->hwScan(&dax_queue, node_item);
					} else if (t_id == 3) {
						this->api_ds->hwScan(&dax_queue, node_item);
					}
				}
			}

			dax_poll_t poll_data[q_size];
			items_done = dax_poll(dax_queue, poll_data, -1, 0);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, join_queue);
			}
		}

		items_done = 0;
		while (items_done != dax_status_t::DAX_EQEMPTY) { // busy polling
			dax_poll_t poll_data[q_size];
			items_done = dax_poll(dax_queue, poll_data, 1, -1);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, join_queue);
			}
		}
		printf("DAX SCANS DONE! (%d), %d\n", items_done, (int) this->daxDone);

		join_queue->printQueue();
		items_done = q_size;
		while (join_queue->getHead() != join_queue->getTail()) {
			for (int i = 0; i < items_done; i++) {
				if (join_queue->getHead() != join_queue->getTail()) {
					node_item = join_queue->remove_ref();
					if (node_item == NULL) {
						printf("Null node...\n");
						continue;
					}
					t_id = node_item->value.getTableId();
					//p_id = node_item->value.getPart();

					if (t_id == 11) {
						//printf("(s_jc)(p: %d)\n", p_id);
						this->j1_lc->hwJoin(&dax_queue, node_item);
					} else if (t_id == 12) {
						//printf("(s_js)(p: %d)\n", p_id);
						this->j2_ls->hwJoin(&dax_queue, node_item);
					} else if (t_id == 13) {
						//printf("(s_jd)(p: %d)\n", p_id);
						this->j3_ld->hwJoin(&dax_queue, node_item);
					}
				}
			}

			dax_poll_t poll_data[q_size];
			items_done = dax_poll(dax_queue, poll_data, -1, 0);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, join_queue);
			}
		}

		items_done = 0;
		while (items_done != dax_status_t::DAX_EQEMPTY) { // busy polling
			dax_poll_t poll_data[q_size];
			items_done = dax_poll(dax_queue, poll_data, 1, -1);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, join_queue);
			}
		}
		printf("DAX JOINS DONE! (%d), %d\n", items_done, (int) this->daxDone);
	}

	void dataDivision() {
		printf("Data Division Approach (Dax Handler)\n");
		int items_done = q_size;
		Node<T> *node_item;
		int t_id = -1;
		//int p_id = -1;

		while (this->hw_queue->getHead() != this->hw_queue->getTail()) {
			for (int i = 0; i < items_done; i++) {
				if (this->hw_queue->getHead() != this->hw_queue->getTail()) {
					node_item = this->hw_queue->remove_ref();
					if (node_item == NULL) {
						printf("Null node...\n");
						continue;
					}
					t_id = node_item->value.getTableId();
					if (t_id == 0) {
						this->api_ls->hwScan(&dax_queue, node_item);
					} else if (t_id == 1) {
						this->api_ss->hwScan(&dax_queue, node_item);
					} else if (t_id == 2) {
						this->api_cs->hwScan(&dax_queue, node_item);
					} else if (t_id == 3) {
						this->api_ds->hwScan(&dax_queue, node_item);
					}
				}
			}

			dax_poll_t poll_data[q_size];
			items_done = dax_poll(dax_queue, poll_data, -1, 0);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, this->shared_queue);
			}
		}

		items_done = 0;
		while (items_done != dax_status_t::DAX_EQEMPTY) { // busy polling
			dax_poll_t poll_data[q_size];
			items_done = dax_poll(dax_queue, poll_data, 1, -1);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, this->shared_queue);
			}
		}
		//printf("Dax done with scans!\n");

		while (true) {
			if (this->sw_queue->getHead() != this->sw_queue->getTail()) {
				continue;
			} else {
				int totalFactParts = this->api_ls->totalParts();
				int totalJoinItems = this->api_agg->TotalJoins() * totalFactParts;
				int daxJoins = totalJoinItems / (1.70 + 1) * 1.70;

				//printf("Dax will do %d joins out of %d in total!\n", daxJoins, totalJoinItems);

				int p_id, j_id;
				for(int ind = 0; ind < totalJoinItems; ind++){
					p_id = ind % totalFactParts; //part_id
					j_id = ind / totalFactParts; //join_id

					if(ind < daxJoins)
						this->addNewJob(0, p_id, 11 + j_id, this->hw_queue);
					else
						this->addNewJob(0, p_id, 11 + j_id, this->sw_queue);
				}

				/*printf("DAX Join Queue\n");
				this->hw_queue->printQueue();

				printf("Core Join Queue\n");
				this->sw_queue->printQueue();*/

				break;
			}
		}

		this->thr_sync->waitOnJoinBarrier();

		items_done = q_size;
		while (this->hw_queue->getHead() != this->hw_queue->getTail()) {
			for (int i = 0; i < items_done; i++) {
				if (this->hw_queue->getHead() != this->hw_queue->getTail()) {
					node_item = this->hw_queue->remove_ref();
					if (node_item == NULL) {
						printf("Null node...\n");
						continue;
					}
					t_id = node_item->value.getTableId();
					//p_id = node_item->value.getPart();

					if (t_id == 11) {
						//printf("(s_jc)(p: %d)\n", p_id);
						this->j1_lc->hwJoin(&dax_queue, node_item);
					} else if (t_id == 12) {
						//printf("(s_js)(p: %d)\n", p_id);
						this->j2_ls->hwJoin(&dax_queue, node_item);
					} else if (t_id == 13) {
						//printf("(s_jd)(p: %d)\n", p_id);
						this->j3_ld->hwJoin(&dax_queue, node_item);
					}
				}
			}

			dax_poll_t poll_data[q_size];
			items_done = dax_poll(dax_queue, poll_data, -1, 0);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, NULL);
			}
		}

		items_done = 0;
		while (items_done != dax_status_t::DAX_EQEMPTY) { // busy polling
			dax_poll_t poll_data[q_size];
			items_done = dax_poll(dax_queue, poll_data, 1, -1);
			if (items_done > 0) {
				handlePollReturn(items_done, poll_data, NULL);
			}
		}
		//printf("Dax done with joins!\n");

		while (true) {
			if (this->sw_queue->getHead() != this->sw_queue->getTail()) {
				continue;
			} else {
				int totalFactParts = this->api_ls->totalParts();
				for(int p_id = 0; p_id < totalFactParts; p_id++)
					this->addNewJob(0, p_id, 21, this->sw_queue);

				//printf("Agg. Queue\n");
				//this->sw_queue->printQueue();

				break;
			}
		}

	}

public:
	DaxHandler(Syncronizer *sync, WorkQueue<T> *shared_queue,
			std::vector<DataCompressor*> &dataVector, int size, bool isAsync) :
			ThreadHandler<T>(sync, shared_queue, dataVector) {
		q_size = size;
		async = isAsync;
		if (!async)
			q_size = 1;
	}

	void *run() {
		createDaxContext();
		this->thr_sync->waitOnStartBarrier();

		if (this->eType == EXEC_TYPE::SDB) {
			this->startExec();
		} else if (this->eType == EXEC_TYPE::OAT) {
			this->opAtaTime();
		} else if (this->eType == EXEC_TYPE::DD) {
			this->dataDivision();
		}

		printf("Dax is done...\n");
		this->thr_sync->waitOnAggBarrier();
		this->thr_sync->waitOnEndBarrier();
		printf("Dax resources releasing ...\n");

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

		res = dax_queue_create(ctx, q_size, &dax_queue);
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
		int t_id;
		int p_id;
		for (int i = 0; i < items_done; i++) {
			q_udata *post_data = (q_udata*) (poll_data[i].udata);
			node_item = (Node<T> *) (post_data->node_ptr);

			t_id = node_item->value.getTableId();
			p_id = node_item->value.getPart();
			//finished(t_id, p_id);

			//1) log runtimes
			if (t_id == 0 || t_id == 1 || t_id == 2 || t_id == 3) {
				this->result.addRuntime(DAX_SCAN,
						make_tuple(post_data->t_start, gethrtime(), t_id,
								p_id));
			} else if (t_id == 11 || t_id == 12 || t_id == 13) {
				this->result.addRuntime(DAX_JOIN,
						make_tuple(post_data->t_start, gethrtime(), t_id,
								p_id));
			}
			//2) log count results
			this->result.addCountResult(make_tuple(poll_data[i].count, t_id));

			this->putFreeNode(node_item);
			//3) create follow up jobs if any
			if (t_id == 0 && this->eType != EXEC_TYPE::DD) {
				//this->api_ls->incrementCounter();
				this->addNewJob(0, node_item->value.getPart(), 11, work_queue);
				this->addNewJob(0, node_item->value.getPart(), 12, work_queue);
				this->addNewJob(0, node_item->value.getPart(), 13, work_queue);
			} else if (t_id == 13) {
				if(this->eType == EXEC_TYPE::DD){
					this->addNewJob(0, node_item->value.getPart(), 21, this->shared_queue);
				}
				else{
					this->addNewJob(0, node_item->value.getPart(), 21, this->sw_queue);
 				}
			}

			//printf("%lu for part: %d\n", poll_data[i].count, node_item->value.getPart());
		}

		//this->api_ss->incrementCounter();
		//this->j2_ls->setExecFlag(p_id);
	}

	void setHWQueue(WorkQueue<T> *queue) {
		this->hw_queue->printQueue();
	}

	void finished(int t_id, int p_id) {
		if (t_id == 0) {
			printf("(e_ls)(p: %d)\n", p_id);
		} else if (t_id == 1) {
			printf("(e_ss)(p: %d)\n", p_id);
		} else if (t_id == 2) {
			printf("(e_cs)(p: %d)\n", p_id);
		} else if (t_id == 3) {
			printf("(e_ds)(p: %d)\n", p_id);
		} else if (t_id == 11) {
			printf("(e_jc)(p: %d)\n", p_id);
			//jc_flags[p_id]--;
		} else if (t_id == 12) {
			printf("(e_js)(p: %d)\n", p_id);
			//js_flags[p_id]--;
		} else if (t_id == 13) {
			printf("(e_jd)(p: %d)\n", p_id);
			//jd_flags[p_id]--;
		}
	}
#endif
};
//#endif
