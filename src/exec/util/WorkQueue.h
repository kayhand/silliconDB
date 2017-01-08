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
        WorkQueue(int q_id, DataLoader *dataLoader){
	    this->mq_id = q_id;
	    this->dataLoader = dataLoader;
	    head.store(new Node<T>);
	    tail.store(head.load());
        }

	WorkQueue(const WorkQueue &source){
	    this->mq_id = source.mq_id;
	    this->dataLoader = source.dataLoader;
	    head.store(source.head.load(std::memory_order_relaxed));
	    tail.store(source.tail.load(std::memory_order_relaxed));
	}

        ~WorkQueue() {	    
        /*  Node<T> *curr = head;
	    Node<T> *temp = curr;
	    while(temp->next){
		curr = curr->next;
		delete temp;
		temp = curr;
	    }
	    delete temp;*/
	}

    Node<T>* getHead(){
        return head;
    }

    Node<T>* getTail(){
        return tail;
    }

    void printQueue(){
    	Node<T>* curNode = head.load(std::memory_order_relaxed);
	curNode = curNode->next;
	while(curNode != NULL){
	    printf("%d - ", curNode->value.getPart());
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
	    printQueue();
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
		    tail.compare_exchange_weak(last, next);
		}
		else{
		    T value = next->value;
		    if(head.compare_exchange_weak(first, next)){
			return value;
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
