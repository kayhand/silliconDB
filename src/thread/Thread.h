#ifndef __thread_h__
#define __thread_h__

#include <pthread.h>

/*
	Java style thread package on top of p_threads
*/

class Thread
{
  public:
    Thread();
    virtual ~Thread();

    int start(int);
    void exit();
    int join();
    int detach();
    int setAffinity(int);
    pthread_t self();

    
    virtual void* run() = 0;
    
  private:
    pthread_t  tid;
    int        running;
    int        detached;
    int        cpuid;
};

#endif
