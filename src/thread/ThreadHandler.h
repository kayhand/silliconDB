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

#include "bench/vldb2018/tpch/Query3.h"

#ifdef __sun 
#include <sys/processor.h>
#include <sys/procset.h>
#include <thread.h>
#endif

enum EXEC_TYPE{SHARED_QUEUE,SEPERATE_QUEUES};

template <class T>
class ThreadHandler : public Thread<T>{
    protected:
        Syncronizer* thr_sync;
	std::atomic<bool> daxDone = ATOMIC_VAR_INIT(false);

        WorkQueue<T>* shared_queue;
    	WorkQueue<T>* sw_queue;
    	WorkQueue<T>* hw_queue;

        TCPStream* t_stream;
        std::vector<DataCompressor*> dataVector;
        Result result;
	EXEC_TYPE eType;

	std::vector<ScanApi*> scanAPIs;

	virtual void startExec(WorkQueue<T> *work_queue) = 0;

    public:
        ThreadHandler(Syncronizer *sync, WorkQueue<T> *queue, std::vector<DataCompressor*> &data){
            thr_sync = sync;
            shared_queue = queue;
	    dataVector = data;
        }

        //ThreadHandler(Syncronizer *sync, ProcessingUnit *pUnit){
            //thr_sync = sync;
	    //procUnit = (void*) pUnit;
        //}

        void addDataCompressor(DataCompressor *dataComp){
            dataVector.push_back(dataComp);    
        }

        void addNewJob(int r_id, int p_id, int j_type, WorkQueue<T> *queue){
            Node<T> *newNode = this->returnNextNode(r_id, p_id, j_type);
            queue->add(newNode);
        }

        Result &getResult(){
            return result;
        }

	void setStream(TCPStream *stream){
	    t_stream = stream;
	}

	void setExecType(EXEC_TYPE type){
	    eType = type;
	}

	void setQueues(WorkQueue<T> *sw_queue, WorkQueue<T> *hw_queue){
	    this->sw_queue = sw_queue;
	    this->hw_queue = hw_queue;
	}

	void setScanAPIs(std::vector<ScanApi*> &scanAPIs){
	    this->scanAPIs = scanAPIs;
	}

};

#endif
