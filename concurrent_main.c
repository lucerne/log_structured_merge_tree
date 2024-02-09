#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <stdint.h>
#include <time.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>    
#include "lsm.h"
#include "main.h"


// is read thread complete 
volatile sig_atomic_t gReadComplete = 0;
 
void is_read_complete(int signal){
    gReadComplete = signal;
}

// same as entry
typedef struct tuple {
  int key;
  int value;
  int op; 
} tuple_t;

// run put input
typedef struct put_thread_args {
  int num_data; // number of data generated
  int prob_delete; // how much delete, range 0 to 100; 0 is no delete, 50 is 50% delete 
  // int max_data; // maximum value of key or value 
  int skew;   // update a set of value after initial write, range 10-30 percent. 
    // 0: no update, 10-30 percent of data are updated after initial even write
} put_thread_args;

// run get input 
typedef struct get_thread_args {
  int thread_id;  // 1 to NUM_READ_THREAD 
  int freq;   // frequency of read with respect to write, range 1-6, or 1 sec to 1 usec
  int skew;   // read from a set of value, max key read, range 1-Max_size. 
  // For example, 1 is no skew, 100 is reading any data divisible by 100
  // This is not the same as reading a range of values i.e. 1-100.
} get_thread_args;

// generate random tuples
tuple_t* random_tuple_gen(int prob_delete){
  tuple_t* input = (tuple_t*) malloc(sizeof(tuple_t));
  input->key = rand();

  int r = rand() % 100;  
  if (r < prob_delete){
    input->value = 0;
    input->op = 1;
  }
  else{
    input->value = rand();
    input->op = 0;
  }
  return input;
}

// generate linear tuples from 0
tuple_t* linear_tuple_gen(int prob_delete, int index){
  tuple_t* input = (tuple_t*) malloc(sizeof(tuple_t));
  input->key = index;
  // input->value = index;

  int r = rand() % 100;  
  if (r < prob_delete){
    input->value = 0;
    input->op = 1;
  }
  else{
    input->value = index;
    input->op = 0;
  }
  return input;
}


// Automatically generate workloads
void* run_put(void* args){
  put_thread_args* put_args = (put_thread_args*) args;

  printf("finish initialization of LSM levels\n");

  // RAND_MAX = 2147483647, 2 billion 
  int r; // random delete 
  int key, value, op;

  // measure in units of CPU time 
  clock_t t;
  double time_taken;
  int i = 0;

  while (i < put_args->num_data) {
  
    // generate a tuple

    tuple_t* input = random_tuple_gen(put_args->prob_delete); 

    t = clock();
    put(input->key, input->value, input->op);
    t = clock() - t;
    time_taken += ((double)t)/CLOCKS_PER_SEC; // in seconds

    // memory leak without this? 
    free(input);

    i += 1;

    if ((i % 1000000) == 0){
      printf("Put %d million \n", i);
    }
  }
  printf("PUT: %f seconds \n", time_taken);

  raise(SIGINT);

  return NULL;
}

// expect key to be generated, no argument
// may result in runtime error if do not use lock while merge 
// make buffer size small, and periodically get while put is running to test read
// during write merge 
void* run_get(void* args){
  // printf("staring GET thread \n");

  get_thread_args* get_args = (get_thread_args*) args;

  int key, value;
  int sleep_time = pow(10, 6) / pow(10, get_args->freq);

  // measure in units of CPU time 
  clock_t t;
  double time_taken;
  int i = 0;

  // as long as read thread is running, do get 
  while(gReadComplete == 0){

    key = rand();

    t = clock();
    value = get(key);
    t = clock() - t;
    time_taken += ((double)t)/CLOCKS_PER_SEC; // in seconds

    // sleep in micro seconds
    // get_args->freq : 1 is 1 second, 10 is 0.01 second, up to 6, 1 microsecond
    // without sleep, write is very slow
    usleep(sleep_time);
    // sleep(0);

    i += 1; 
  }


  printf("Thread %d GET: %d values in %f seconds \n", get_args->thread_id, i, time_taken);

  return NULL;
}


int main(int argc, char** argv) {

  if (argc < 5){
    printf("usage: output file in rows of INT, INT, A or D\n");
    printf("i.e. ./macro_main num_data prob_delete freq skew_write skew_read\n");
    printf("num_data: number of key-value pairs to generate\n");
    printf("prob_delete: probability of set delete flag this \
      key-value pair, range 0 to 100. 0: no delete. \n");
    printf("freq: frequency of read with respect to write, range 1-6, 1 sec to 1 usec \n");

    printf("skew_write: update from a range of value, range 0-100, \
      0: no skew, 100: 10 percent of data after initial even write\n");
    printf("skew_read: max key read, range 1-Max_size. For example, 1 is no skew, \
      100 is reading any data divisible by 100 \n");
    return 1;
  }


   // initialize the levels
  init_levels();

  // One read thread, NUM_READ_THREAD read thread, does not need to run read thread
  // if there are levels on disk
  pthread_t threads[6];

  // create a write thread, wait for it to exit 
  put_thread_args* put_args = (put_thread_args*) malloc(sizeof(put_thread_args));
  put_args->num_data = atoi(argv[1]);
  put_args->prob_delete = atoi(argv[2]);
  put_args->skew = atoi(argv[4]);

  // Install a signal handler. 
  signal(SIGINT, is_read_complete);
  // printf("SignalValue:   %d\n", gReadComplete);

  pthread_create(&threads[0],NULL, run_put, put_args);
  // pthread_join(threads[0],NULL);

  // allow write to disk 
  sleep(1);

  // starting all read threads
  int freq = atoi(argv[3]);

  if (freq != 0){
    for (int i=1; i < NUM_ALL_THREAD; i++){
      get_thread_args* get_args = (get_thread_args*) malloc(sizeof(get_thread_args));
      get_args->thread_id = i;
      get_args->freq = atoi(argv[3]);
      get_args->skew = atoi(argv[5]);
      pthread_create(&threads[i],NULL, run_get, get_args);
      sleep(1);
    }
  }
  // waiting for write threads to exit 
  pthread_join(threads[0],NULL);
  print_metainfo();

  // waiting for read threads to exit 
  if (freq != 0){
    for (int i=1; i < NUM_ALL_THREAD; i++){
      pthread_join(threads[i],NULL);
    }
  }
  // check if the thread is running
  // printf("read thread: \n", pthread_kill(threads[0], 0));

  return 0;
}