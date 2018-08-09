#ifndef __thread_h__
#define __thread_h__

#include <pthread.h>
#include <vector>
#include <atomic>

#ifdef __sun 
#include <sys/processor.h>
#include <sys/procset.h>
#include <thread.h>
//#else
#endif

#include "util/Types.h"
#include "util/Query.h"


#define FREELISTSIZE 6

struct q_udata {
	void* node_ptr;

	hrtime_t t_start;
	hrtime_t t_end;
};

template<class T>
struct Node {
	T value;
	std::atomic<Node<T>*> next;
	Node() :
			next(nullptr) {
	}
	Node(T value) :
			value(value), next(nullptr) {
	}
	Node(int r_id, int p_id, JOB_TYPE j_type) :
			next(nullptr) {
		value.setFields(r_id, p_id, j_type);
	}
	Node(const Node &source) {
		next.store(source.next.load());
	}

	JOB_TYPE &JobType(){
		return ((Query) this->value).getJobType();
	}

	q_udata post_data;
	hrtime_t t_start = 0ul;
	hrtime_t t_end = 0ul;
};

template<class T>
class Thread {
	pthread_t tid;
	int running;
	int detached;

	int nextSlot = 0;
	std::vector<Node<Query>*> node_pool;

public:
	Thread() :
			tid(0), running(0), detached(0), nextSlot(0){
	}

	virtual ~Thread() {
		if (running == 1) {
			pthread_cancel(tid);
		}

		//for (int id = 0; id < nextSlot; id++) { //between nextSlot and freeListSize are already release
		    //delete node_pool[id];
		//}

		for (Node<T>* curNode : node_pool) {
			delete curNode;
		}
	}

	int start(int cpuid, bool daxHandler) {
		pthread_attr_t attr;
		pthread_attr_init(&attr);
		pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

		int result = pthread_create(&tid, &attr, runThread, this);
		if (result == 0) {
			running = 1;
			if (daxHandler) {
				setAffinity(cpuid);
			} else
				//setAffinity(cpuid);
				setAffinity(cpuid * 8);
		}
		return result;
	}

	void exit() {
		pthread_exit(NULL);
	}

	int join() {
		int result = -1;
		if (running == 1) {
			result = pthread_join(tid, NULL);
			if (result == 0) {
				detached = 0;
			}
		}
		return result;
	}

	int detach() {
		int result = -1;
		if (running == 1 && detached == 0) {
			result = pthread_detach(tid);
			if (result == 0) {
				detached = 1;
			}
		}
		return result;
	}

	int setAffinity(int cpuid) {
		int result = -1;
#ifdef __sun
		procset_t ps;
		uint_t nids = 1;
		id_t ids[1] = {cpuid};

		uint32_t flags = PA_TYPE_CPU | PA_AFF_STRONG;
		setprocset(&ps, POP_AND, P_PID, P_MYID, P_LWPID, thr_self());

		result = processor_affinity(&ps, &nids, ids, &flags);
		if (result != 0) {
			fprintf(stderr, "Error setting affinity.\n");
			perror(NULL);
		}

		flags = PA_QUERY;
		id_t read_ids[1];
		result = processor_affinity(&ps, &nids, read_ids, &flags);
		//printf("Affinity set to core number %d\n", read_ids[0]);
		return result;
#else
		cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(cpuid, &cpuset);
		//printf("Affinity set to core number %d\n", cpuid);
		result = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
		return result;

#endif
		return cpuid;
	}

	pthread_t self() {
		return tid;
	}

	virtual void* run() = 0;

	static void* runThread(void* arg) {
		return ((Thread*) arg)->run();
	}

	void clearNodes() {
		for (Node<T> *node : node_pool)
			delete node;
	}

	void putFreeNode(Node<T> *node) {
		node_pool.push_back(node);
		//printf("(%d) ...\n", nextSlot++);
	}

	Node<T>* returnNextNode(int r_id, int p_id, JOB_TYPE j_type) {
		if (nextSlot < 50) {
			//printf("new node...\n");
			Node<T>* newNode = new Node<T>();

			newNode->value.setFields(r_id, p_id, j_type);
			return newNode;
		} else {
			//printf("old node (%d) ...\n", nextSlot);
			Node<T>* oldNode = node_pool.back();
			node_pool.pop_back();
			nextSlot--;

			oldNode->next = nullptr;
			oldNode->value.setFields(r_id, p_id, j_type);
			return oldNode;
		}
	}
};

#endif
