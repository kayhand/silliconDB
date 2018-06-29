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
	EXEC_TYPE eType;

	std::atomic<bool> daxDone = ATOMIC_VAR_INIT(false);

	WorkQueue<T>* shared_queue = NULL;
	WorkQueue<T>* hw_queue = NULL;
	WorkQueue<T>* sw_queue = NULL;

	TCPStream* t_stream = NULL;
	std::vector<DataCompressor*> dataVector;
	Result result;

	ScanApi* factScanAPI = NULL;
	std::vector<ScanApi*> dimScanAPIs;
	std::vector<JoinApi*> joinAPIs;
	AggApi *api_agg = NULL;

	virtual void siliconDB() = 0;
	virtual void opAtaTime() = 0;
	virtual void dataDivision() = 0;

public:
	ThreadHandler(Syncronizer *sync, EXEC_TYPE e_type) {
		thr_sync = sync;
		this->eType = e_type;
	}

	void addNewJob(int r_id, int p_id, JOB_TYPE j_type, WorkQueue<T> *queue) {
		Node<T> *newNode = this->returnNextNode(r_id, p_id, j_type);
		queue->add(newNode);
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

	void setAPIs(ScanApi* factScan, std::vector<ScanApi*> &dimScans,
			std::vector<JoinApi*> &joinAPIs, AggApi* aggAPI) {

		this->factScanAPI = factScan;
		this->dimScanAPIs = dimScans;
		this->joinAPIs = joinAPIs;
		this->api_agg = aggAPI;
	}

};

#endif
