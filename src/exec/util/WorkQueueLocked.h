#ifndef __workqueue_h__
#define __workqueue_h__

#include <pthread.h>
#include <list>

using namespace std;

template <typename T> class WorkQueue
{
    list<T>          m_queue;
    pthread_mutex_t  m_mutex;
    pthread_cond_t   m_condv; 
    int start_size;
    int mq_id;

    public:
    WorkQueue(int s_size, int q_id) {
        pthread_mutex_init(&m_mutex, NULL);
        pthread_cond_init(&m_condv, NULL);
        start_size = s_size;
        mq_id = q_id;
    }
    ~WorkQueue() {
        pthread_mutex_destroy(&m_mutex);
        pthread_cond_destroy(&m_condv);
    }


    void add(T item) {
        pthread_mutex_lock(&m_mutex);
        m_queue.push_back(item);
        if(m_queue.size() == start_size){
            pthread_cond_signal(&m_condv);
            printf("Signalled!\n");
        }
        pthread_mutex_unlock(&m_mutex);
    }
    
    T remove(bool locking) {
        T item;
        if(locking == false) {
            item = m_queue.front();
            m_queue.pop_front();
            return item;
        }
        pthread_mutex_lock(&m_mutex);
        while (m_queue.size() < start_size) {
            printf("Size: %d\n", size());
            pthread_cond_wait(&m_condv, &m_mutex);
        }
        item = m_queue.front();
        m_queue.pop_front();
        pthread_mutex_unlock(&m_mutex);
        return item;
    }
    
    int size() {
        //pthread_mutex_lock(&m_mutex);
        int size = m_queue.size();
        //pthread_mutex_unlock(&m_mutex);
        return size;
    }

    int getId(){
        return mq_id;
    }
};

#endif


