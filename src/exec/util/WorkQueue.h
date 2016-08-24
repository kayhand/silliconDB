#ifndef __workqueue_h__
#define __workqueue_h__

#include <pthread.h>
#include <list>
#include <unordered_map>
#include "../../data/DataLoader.h"

using namespace std;

template <typename T> class WorkQueue
{
    list<T> m_queue;
    int start_size;
    int mq_id;
    std::unordered_map<int, std::pair<int,int>> *partMap;  
    DataCompressor *dataComp;

    public:
        WorkQueue(){}
        WorkQueue(int s_size, int q_id, std::unordered_map<int, std::pair<int,int>> &p_map, DataCompressor *dataCompressor){
            start_size = s_size;
            mq_id = q_id;
	    partMap = &p_map;
	    dataComp = dataCompressor;
        }
        ~WorkQueue() {}

    void add(T item) {
        m_queue.push_back(item);
    }
    
    T remove() {
        T item;
        item = m_queue.front();
        m_queue.pop_front();
        return item;
    }
    
    int size() {
        int size = m_queue.size();
        return size;
    }

    int getId(){
        return mq_id;
    }

    std::unordered_map<int, std::pair<int,int>>& getMap(){
	return *partMap;
    }

    DataCompressor* getCompressor(){
        return dataComp;
    }
};

#endif
