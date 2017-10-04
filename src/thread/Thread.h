#ifndef __thread_h__
#define __thread_h__

#include <pthread.h>
#include <vector>
#include <atomic>

#ifdef __sun 
#include <sys/processor.h>
#include <sys/procset.h>
#include <thread.h>
#endif

template <class T> 
struct Node{
    T value;
    std::atomic<Node<T>*> next; 
    Node() : next(nullptr){}
    Node(T value) : value(value), next(nullptr){}
    Node(int r_id, int p_id, int j_type) : next(nullptr){
    	value.setFields(r_id, p_id, j_type);
    }
    Node(const Node &source){
    	next.store(source.next.load());
    }

    hrtime_t t_start;
    hrtime_t t_end;
};

template <class T>
class Thread
{
   pthread_t  tid;
   int    running;
   int    detached;
   int	  pid;

   std::vector<Node<T>*> node_pool;
   int nextNodeId = 0;

  public:
    Thread() : tid(0), running(0), detached(0), pid(0){} 
    virtual ~Thread()
    {
        if (running == 1) {
            pthread_cancel(tid);
    	}

	for(Node<T>* curNode : node_pool){
	    delete curNode;
	}
    }

    int start(int cpuid, bool daxHandler)
    {
	pthread_attr_t attr;
    	pthread_attr_init(&attr);
    	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	node_pool.reserve(10);

	int result = pthread_create(&tid, &attr, runThread, this);
    	if (result == 0){
        	running = 1;
		if(daxHandler){
    		    setAffinity(cpuid);
		}
		else
    		    setAffinity(cpuid);
    		    //setAffinity(cpuid * 8);
    	}
    	return result;
    }

    void setId(int p_id)
    {
    	pid = p_id;
    }

    int getId()
    {
    	return pid;
    }

    void exit(){
    	pthread_exit(NULL);
    }

    int join()
    {
        int result = -1;
    	if (running == 1) {
            result = pthread_join(tid, NULL);
            if (result == 0) {
                detached = 0;
            }	
    	}
    	return result;
    }

    int detach()
    {
        int result = -1;
    	if (running == 1 && detached == 0) {
            result = pthread_detach(tid);
            if (result == 0) {
            	detached = 1;
            }
    	}
    	return result;
    }

    int setAffinity(int cpuid)
    {
        int result = -1;
    #ifdef __gnu_linux__
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(cpuid, &cpuset);
        //printf("Affinity set to core number %d\n", cpuid);
        result  = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
        return result;
    #else
	procset_t ps;
	uint_t nids = 1;
	id_t ids[1] = {cpuid};

	uint32_t flags = PA_TYPE_CPU | PA_AFF_STRONG;
	setprocset(&ps, POP_AND, P_PID, P_MYID, P_LWPID, thr_self());

        result = processor_affinity(&ps, &nids, ids, &flags);
	if(result != 0){
	    fprintf(stderr, "Error setting affinity.\n");
	    perror(NULL);
	}

	flags = PA_QUERY;
	id_t read_ids[1];
        result = processor_affinity(&ps, &nids, read_ids, &flags);
        //printf("Affinity set to core number %d\n", read_ids[0]);
	return result;
    #endif 
        return cpuid;
    }

    pthread_t self() 
    {
    	return tid;
    }

    virtual void* run() = 0;

    static void* runThread(void* arg)
    {
    	return ((Thread*)arg)->run();
    }

    void clearNodes(){
    	for(Node<T> *node : node_pool)
            delete node; 
    }

    void putFreeNode(Node<T> *node){
    	//node->next = NULL;
	node_pool.push_back(node);
    }

    Node<T>* returnNextNode(T value){
    	if(node_pool.size() < 5){
	    Node<T>* newNode = new Node<T>();
            newNode->value = value;
	    return newNode;
	}
	else{
            Node<T>* curNode = node_pool[nextNodeId++]; //remove nodes from the front of the vector
	    curNode->next = NULL;
            curNode->value = value;
	    return curNode;
	}
    }

    Node<T>* returnNextNode(int r_id, int p_id, int j_type){
    	if(node_pool.size() < 5){
	    Node<T>* newNode = new Node<T>();
            newNode->value.setFields(r_id, p_id, j_type);
	    return newNode;
	}
	else{
            Node<T>* curNode = node_pool[nextNodeId++];
	    curNode->next = NULL;
            curNode->value.setFields(r_id, p_id, j_type);
	    return curNode;
	}
    }
};

#endif
