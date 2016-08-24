#include "Thread.h"
#include <stdio.h>

static void* runThread(void* arg)
{
    return ((Thread*)arg)->run();
}

Thread::Thread() : tid(0), running(0), detached(0) {}

Thread::~Thread()
{
    printf("Destroy \n");
    //if (running == 1 && detached == 0) {
      //  pthread_detach(tid);
    //}
    if (running == 1) {
//        pthread_cancel(tid);
    }
}

int Thread::start(int cpuid)
{
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    int result = pthread_create(&tid, &attr, runThread, this);
    if (result == 0) {
        running = 1;
    	setAffinity(cpuid);
    }
    return result;
}

void Thread::exit(){
    pthread_exit(NULL);
}

int Thread::join()
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

int Thread::detach()
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

int Thread::setAffinity(int cpuid)
{
#ifdef __gnu_linux__
    int result = -1;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpuid, &cpuset);
    printf("Affinity set to core number %d\n", cpuid);
    result  = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
    return result;
#endif 
    return cpuid;
}

pthread_t Thread::self() 
{
    return tid;
}

