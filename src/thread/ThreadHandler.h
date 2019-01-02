#ifndef __threadhandler_h__
#define __threadhandler_h__

#include <pthread.h>
#include <vector>
#include <atomic>

#include "Thread.h"
#include "Syncronizer.h"

#include "exec/util/WorkQueue.h"
#include "network/server/TCPAcceptor.h"
#include "log/Result.h"

#include "api/ScanApi.h"
#include "api/JoinApi.h"
#include "api/AggApi.h"

#ifdef __sun 
#include <sys/processor.h>
#include <sys/procset.h>
#include <thread.h>
#endif

enum EXEC_TYPE {
	SDB = 1,
	OAT,
	DD ,

	JOIN_RW,
	AGG_RW,

	QT_MICRO
};

template<class T>
class ThreadHandler: public Thread<T> {
protected:
	Syncronizer* thr_sync;
	EXEC_TYPE eType;
	int thr_id = -1;

	WorkQueue<T>* shared_queue = NULL;
	WorkQueue<T>* hw_queue = NULL;
	WorkQueue<T>* sw_queue = NULL;

	TCPStream* t_stream = NULL;
	Result result;

	ScanApi *factScanAPI = NULL;
	ScanApi *factScanAPI2 = NULL;
	unordered_map<int, ScanApi*> dimScanAPIs;
	unordered_map<int, JoinApi*> joinAPIs;
	AggApi *api_agg = NULL;

	virtual void siliconDB() = 0;
	virtual void opAtaTime() = 0;
	virtual void dataDivision() = 0;

public:
	ThreadHandler(Syncronizer *sync, EXEC_TYPE e_type, int thr_id) {
		this->thr_sync = sync;
		this->eType = e_type;
		this->thr_id = thr_id;

		result.setThreadId(thr_id);
	}

	int getId() {
		return this->thr_id;
	}

	void addNewJob(int r_id, int p_id, JOB_TYPE j_type, WorkQueue<T> *queue) {
		Node<T> *newNode = this->returnNextNode(r_id, p_id, j_type);
		queue->add(newNode);
	}

	void addNewJoins(int p_id, WorkQueue<T> *work_queue){
		for(auto &curPair : joinAPIs){
			JoinApi *curJoin = curPair.second;
			if(curJoin->isCoPartitioned()){
				if((this->eType & JOIN_RW) == JOIN_RW)
					this->addNewJob(0, p_id, curJoin->JoinType(), work_queue);
				else{
					this->addNewJob(0, p_id, curJoin->JoinType(), this->sw_queue);
				}
			}
			else{
				this->addNewJob(0, p_id, curJoin->JoinType(), work_queue);
			}
		}
	}

	Result &getResult() {
		return result;
	}

	void setStream(TCPStream *stream) {
		t_stream = stream;
	}

	void setQueues(WorkQueue<T> *shared_queue, WorkQueue<T> *sw_queue, WorkQueue<T> *hw_queue) {
		this->shared_queue = shared_queue;
		this->sw_queue = sw_queue;
		this->hw_queue = hw_queue;
	}

	void setAPIs(std::vector<ScanApi*> factScans, std::vector<ScanApi*> &dimScans,
			std::vector<JoinApi*> &joinAPIs, AggApi* aggAPI) {

		factScanAPI = factScans[0];
		if(factScans.size() == 2){
			factScanAPI2 = factScans[1];
		}

		for(ScanApi *curScan : dimScans){
			this->dimScanAPIs[curScan->Type()] = curScan;
		}

		for(JoinApi *curJoin : joinAPIs){
			this->joinAPIs[curJoin->JoinType()] = curJoin;
		}

		this->api_agg = aggAPI;
	}

};

#endif
