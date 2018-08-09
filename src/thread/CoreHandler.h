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
		Node<T> *work_unit;
		while (!this->thr_sync->isQueryDone()) {
			if (this->sw_queue->isNotEmpty()) {
				work_unit = this->sw_queue->nextElement();
				this->executeItem(work_unit);
			}
			else if (this->shared_queue->isNotEmpty()) {
				work_unit = this->shared_queue->nextElement();
				this->executeItem(work_unit);
			}
		}
	}

	void opAtaTime() {
		Node<T> *work_unit;
		while (!this->thr_sync->isQueryDone()) {
			if (this->sw_queue->isNotEmpty()) {
				work_unit = this->sw_queue->nextElement();
				this->executeItem(work_unit);
			}
		}
	}

	void dataDivision() {
		Node<T> *work_unit;
		//this->sw_queue->printQueue();
		while (this->sw_queue->isNotEmpty()) {
			work_unit = this->sw_queue->nextElement();
			if(work_unit != NULL)
				this->executeScan(work_unit);
		}

		this->thr_sync->waitOnJoinBarrier();
		while (this->sw_queue->isNotEmpty()) {
			work_unit = this->sw_queue->nextElement();
			if(work_unit != NULL)
				this->executeJoin(work_unit);
		}
		this->thr_sync->waitOnJoinEndBarrier();

		this->thr_sync->waitOnAggBarrier();
		while (this->sw_queue->isNotEmpty()) {
			work_unit = this->sw_queue->nextElement();
			if(work_unit != NULL)
				this->executeAggregation(work_unit);
		}
	}

public:
	CoreHandler(Syncronizer *sync, EXEC_TYPE e_type, int thr_id) :
			ThreadHandler<T>(sync, e_type, thr_id) {
	}

	void *run() {
		this->thr_sync->waitOnStartBarrier();

		if (this->eType == EXEC_TYPE::SDB || this->eType == EXEC_TYPE::REWRITE) {
			this->siliconDB();
			this->thr_sync->waitOnAggBarrier();
		} else if (this->eType == EXEC_TYPE::OAT) {
			this->thr_sync->waitOnAggBarrier();
			this->opAtaTime();
		} else if (this->eType == EXEC_TYPE::DD) {
			this->dataDivision();
		}
		this->thr_sync->waitOnEndBarrier();

		this->t_stream->send("", 0);
		return NULL;
	}

	inline void executeItem(Node<Query> *work_unit){
		if(work_unit == NULL){
			return;
		}
		else{
			JOB_TYPE &j_type = work_unit->value.getJobType();
			if(Types::isScan(j_type)){
				executeScan(work_unit);
			}
			else if(Types::isJoin(j_type)){
				executeJoin(work_unit);
			}
			else{
				executeAggregation(work_unit);
			}
			handleJobReturn(work_unit);
		}
	}

	inline void executeScan(Node<Query> *work_unit){
		JOB_TYPE &j_type = work_unit->value.getJobType();
		if(Types::isFactScan(j_type)){
			this->factScanAPI->simdScan(work_unit, &(this->result));
		}
		else if(Types::isFact2Scan(j_type)){
			this->factScanAPI2->simdScan(work_unit, &(this->result));
		}
		else{
			this->dimScanAPIs[j_type]->simdScan(work_unit, &(this->result));
		}
	}

	inline void executeJoin(Node<Query> *work_unit){
		JOB_TYPE &j_type = work_unit->value.getJobType();
		int &p_id = work_unit->value.getPart();
		this->joinAPIs[j_type]->swJoin(work_unit, &(this->result));
		this->thr_sync->joinPartCompleted(p_id);
	}

	inline void executeAggregation(Node<Query> *work_unit){
		this->api_agg->agg(work_unit, &(this->result));
	}

	inline void handleJobReturn(Node<Query> *work_unit){
		Query item = work_unit->value;
		this->putFreeNode(work_unit);

		JOB_TYPE j_type = item.getJobType();
		int &p_id = item.getPart();

		if (j_type == LO_SCAN) {
			this->addNewJoins(item.getPart(), this->shared_queue);
		}
		else if (Types::isJoin(j_type)){
			if(this->thr_sync->areJoinsDoneForPart(p_id)){
				this->addNewJob(1, item.getPart(), AGG, this->sw_queue);
			}
		}
		else if (j_type == AGG){
			this->thr_sync->incrementAggCounter();
		}
	}

};

#endif

