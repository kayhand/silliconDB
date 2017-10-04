#ifndef __syncronizer_h__
#define __syncronizer_h__

#include <pthread.h>
#include <atomic>

#ifdef __sun 
#include <thread.h>
#endif

class Syncronizer
{
    pthread_barrier_t start_barrier;
    pthread_barrier_t end_barrier;
    pthread_barrier_t agg_barrier;

    public:
        Syncronizer(){} 
 	 
        void initBarriers(int num_of_workers){
            pthread_barrier_init(&start_barrier, NULL, num_of_workers);
            pthread_barrier_init(&end_barrier, NULL, num_of_workers);
            pthread_barrier_init(&agg_barrier, NULL, num_of_workers);
        }

        void waitOnStartBarrier(){
            pthread_barrier_wait(&start_barrier);
        }

        void waitOnEndBarrier(){
            pthread_barrier_wait(&end_barrier);
        }

        void waitOnAggBarrier(){
            pthread_barrier_wait(&agg_barrier);
        }

        void destroyBarriers(){
            pthread_barrier_destroy(&start_barrier);
            pthread_barrier_destroy(&end_barrier);
            pthread_barrier_destroy(&agg_barrier);
        }
};

#endif
