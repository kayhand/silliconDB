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
	SDB,
	OAT,
	DD
};

template<class T>
class ThreadHandler: public Thread<T> {
protected:
	Syncronizer* thr_sync;
	std::atomic<bool> daxDone = ATOMIC_VAR_INIT(false);

	WorkQueue<T>* shared_queue;
	WorkQueue<T>* hw_queue;
	WorkQueue<T>* sw_queue;

	TCPStream* t_stream;
	std::vector<DataCompressor*> dataVector;
	Result result;
	EXEC_TYPE eType;

	std::vector<ScanApi*> scanAPIs;
	std::vector<JoinApi*> joinAPIs;
	std::vector<AggApi*> aggAPIs;

	ScanApi *api_ls;
	ScanApi *api_ss;
	ScanApi *api_cs;
	ScanApi *api_ds;

	JoinApi *j1_lc;
	JoinApi *j2_ls;
	JoinApi *j3_ld;

	AggApi *api_agg;

	virtual void startExec() = 0;

public:
	ThreadHandler(Syncronizer *sync, WorkQueue<T> *queue, // @suppress("Class members should be properly initialized")
			std::vector<DataCompressor*> &data) {
		thr_sync = sync;
		shared_queue = queue;
		dataVector = data;
	}

	void addDataCompressor(DataCompressor *dataComp) {
		dataVector.push_back(dataComp);
	}

	void addNewJob(int r_id, int p_id, int j_type, WorkQueue<T> *queue) {
		Node<T> *newNode = this->returnNextNode(r_id, p_id, j_type);
		queue->add(newNode);
	}

	Result &getResult() {
		return result;
	}

	void setStream(TCPStream *stream) {
		t_stream = stream;
	}

	void setExecType(EXEC_TYPE type) {
		eType = type;
	}

	void setQueues(WorkQueue<T> *sw_queue, WorkQueue<T> *hw_queue) {
		this->sw_queue = sw_queue;
		this->hw_queue = hw_queue;
	}

	void setAPIs(std::vector<ScanApi*> &scanAPIs,
			std::vector<JoinApi*> &joinAPIs, std::vector<AggApi*> &aggAPIs) {
		this->scanAPIs = scanAPIs;
		this->joinAPIs = joinAPIs;
		this->aggAPIs = aggAPIs;

		api_ls = this->scanAPIs[0];
		api_ss = this->scanAPIs[1];
		api_cs = this->scanAPIs[2];
		api_ds = this->scanAPIs[3];

		j1_lc = this->joinAPIs[0];
		j2_ls = this->joinAPIs[1];
		j3_ld = this->joinAPIs[2];

		api_agg = this->aggAPIs[0];
	}

};

#endif
