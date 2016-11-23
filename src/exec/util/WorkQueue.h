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
    std::unordered_map<int, std::pair<int,int>> *partMap;  
    Partitioner *part;
    DataCompressor *dataComp;

    atomic<Node<T>*> head;
    atomic<Node<T>*> tail;

    public:
        WorkQueue(){}
        WorkQueue(int q_id, Partitioner *partitioner, DataCompressor *dataCompressor){
	    mq_id = q_id;
	    part = partitioner;
	    dataComp = dataCompressor;
	    head.store(new Node<T>);
	    tail.store(head.load());
        }

	WorkQueue(const WorkQueue &source){
	    head.store(source.head.load(std::memory_order_relaxed));
	    tail.store(source.tail.load(std::memory_order_relaxed));
	}

        ~WorkQueue() {	    
	}

    Node<T>* getHead(){
        return head;
    }

    Node<T>* getTail(){
        return tail;
    }

    void printQueue(){
    	/*Node<T>* curNode = head.load(std::memory_order_relaxed);

	curNode = curNode->next;
	while(curNode != NULL){
	    printf("%d - ", curNode->value.getPart());
	    curNode = curNode->next;
	}
	printf("\n"); */
    }

    void add(Node<T> *node){	
	//Node<T> *node = self().returnNextNode(item);
	while(true){
	    Node<T> *last = tail;
	    Node<T> *next = (last->next).load(std::memory_order_relaxed);
	    //Node<T> *next = last->next;
	    //printQueue();
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
	}
    }
    
    T remove() {
	while(true){
	    Node<T> *first = head.load(std::memory_order_relaxed);
	    Node<T> *last = tail.load(std::memory_order_relaxed);
	    Node<T> *next = (first->next).load(std::memory_order_relaxed);
	    if(first == head){
	        if(first == last){
		    if(next == NULL){
		    	printf("Queue is empty!\n!");
		    }
		    //else if(tail.compare_exchange_weak(last, next)){
		      //  delete last;
		    //}
		    tail.compare_exchange_weak(last, next);
		}
		else{
		    T value = next->value;
		    if(head.compare_exchange_weak(first, next)){
		//	printf("New partition is %d from thread %d\n", value.getPart(), t_id);
	//		delete first;
			return value;
		    }
		}
	    }
	
	}
        //T item;
        //item = m_queue.front();
        //m_queue.pop_front();
        //return item;
    }
    
    int getId(){
        return mq_id;
    }

    Partitioner* getPartitioner(){
	return part;
    }

    DataCompressor* getCompressor(){
        return dataComp;
    }

};

#endif
