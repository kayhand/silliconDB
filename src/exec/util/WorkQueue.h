#ifndef __workqueue_h__
#define __workqueue_h__

#include <atomic>
#include <pthread.h>
#include <list>
#include <unordered_map>
#include <algorithm>

#include "../../data/DataLoader.h"
#include "../../thread/Thread.h"

using namespace std;

template <typename T> class WorkQueue
{
    int mq_id;
    DataLoader *dataLoader;

    atomic<Node<T>*> head;
    atomic<Node<T>*> tail;
    public:
        WorkQueue(){}

        WorkQueue(int q_id){
	    this->mq_id = q_id;
	    T dummy;
	    head.store(new Node<T>(dummy));
	    tail.store(head.load());
        }

        WorkQueue(int q_id, DataLoader *dataLoader){
	    this->mq_id = q_id;
	    this->dataLoader = dataLoader;
	    T dummy;
	    head.store(new Node<T>(dummy));
	    tail.store(head.load());
        }

	WorkQueue(const WorkQueue &source){
	    this->mq_id = source.mq_id;
	    this->dataLoader = source.dataLoader;
	    head.store(source.head.load(std::memory_order_relaxed));
	    tail.store(source.tail.load(std::memory_order_relaxed));
	}

        ~WorkQueue() {	    
	}

    Node<T>* getHead(){
        return head.load(std::memory_order_relaxed);
    }

    Node<T>* getTail(){
        return tail.load(std::memory_order_relaxed);
    }

    void printQueue(){
    	Node<T>* curNode = head.load(std::memory_order_relaxed);
	curNode = curNode->next;
	while(curNode != NULL){
	    printf("(%d : %d) - ", curNode->value.getPart(), curNode->value.getTableId());
	    curNode = curNode->next;
	}
	printf("\n"); 
    }

    void add(Node<T> *node){	
	//Node<T> *node = self().returnNextNode(item);
	while(true){
	    Node<T> *last = tail;
	    Node<T> *next = (last->next).load(std::memory_order_relaxed);
	    //Node<T> *next = last->next;
	    if(last == tail){
	        if(next == NULL){
		    if((last->next).compare_exchange_weak(next, node)){		  
		        tail.compare_exchange_weak(last, node);
		        return;
		    }		
	        }
	        else{
	    	    tail.compare_exchange_weak(last, next);	
	        }
	    }
	    //printQueue();
	}
    }
    
    T remove(int r_id){
    	T dummy;
	while(true){
	    Node<T> *first = head.load(std::memory_order_relaxed);
	    Node<T> *last = tail.load(std::memory_order_relaxed);
	    Node<T> *next = (first->next).load(std::memory_order_relaxed);
	    if(next->value.getTableId() != 0 && r_id == 1){ //This happens when table id is not zero (not scan) and resource id is 1 (DAX) unit;
	        return dummy;
	    }
	    if(first == head){
	        if(first == last){
		    if(next == NULL){
		    	printf("Queue is empty!\n!");
			return dummy;
		    }
		    tail.compare_exchange_weak(last, next);
		}
		else{
		    if(head.compare_exchange_weak(first, next)){
		    	T value = next->value;	
			return value;
		    }
		}
	    }	
	}
    }

    Node<T>* remove_ref_dax(){
	while(true){
	    Node<T> *first = head.load(std::memory_order_relaxed);
	    Node<T> *last = tail.load(std::memory_order_relaxed);
	    Node<T> *next = (first->next).load(std::memory_order_relaxed);
	    if(first == head){
	        if(first == last){
		    if(next == NULL){
		    	//tail.compare_exchange_weak(last, next);
		    	printf("Queue is empty!\n!");
			return NULL;
		    }
		    tail.compare_exchange_weak(last, next);
		}
		else{
	    	    if(next->value.getTableId() == 4){ //This happens when table id is 4 (aggegation) and resource id is 1 (DAX) unit;
		        return NULL;
	            }
		    else if(head.compare_exchange_weak(first, next)){
			return next;
		    }
		}
	    }	
	}
    }

    Node<T>* remove_ref_core(){
	while(true){
	    Node<T> *first = head.load(std::memory_order_relaxed);
	    Node<T> *last = tail.load(std::memory_order_relaxed);
	    Node<T> *next = (first->next).load(std::memory_order_relaxed);
	    if(first == head){
	        if(first == last){
		    if(next == NULL){
		    	//tail.compare_exchange_weak(last, next);
		    	printf("Queue is empty!\n!");
			return NULL;
		    }
		    tail.compare_exchange_weak(last, next);
		}
		else{
	    	    if(next->value.getTableId() == 55){ 
		        return NULL;
	            }
		    else if(head.compare_exchange_weak(first, next)){
			return next;
		    }
		}
	    }	
	}
    }

    int remove_multiple(int r_id, int n_jobs, vector<T> &jobs, int &p_id1, int &p_id2){
	int j_cnt = 0;
	while(true){
	    Node<T> *first = head.load(std::memory_order_relaxed);
	    Node<T> *last = tail.load(std::memory_order_relaxed);
	    Node<T> *next = (first->next).load(std::memory_order_relaxed);
	    if(next->value.getTableId() != 0 && r_id == 1) //This happens when table id is 1 (aggegation) and resource id is 1 (DAX) unit;
	        return j_cnt; //part_id == -1
	    if(first == head){
	        if(first == last){
		    if(next == NULL){
		    	printf("Queue is empty!\n!");
			return j_cnt;
		    }
		    tail.compare_exchange_weak(last, next);
		}
		else{
		    T value = next->value;
		    if(head.compare_exchange_weak(first, next)){
		    	p_id1 = value.getPart();
		    	jobs.at(p_id1) = value;
			j_cnt++;
			if(j_cnt == n_jobs)
			    return j_cnt;
		    }
		}
	    }	
	}
    }   

    int getId(){
        return mq_id;
    }

    DataLoader* getDataLoader(){
        return this->dataLoader;
    }

};

#endif
