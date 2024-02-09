#ifndef LIB_H
#define LIB_H
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdint.h>
#include <time.h>  
#include <math.h>
#include <stdbool.h>
#include <pthread.h>

#include "level.h"
#include "main.h"

// lib.h
// 
// Functions, data types for LSM tree 

#define CACHE_SIZE 8192	// page size is 4096, rounded down to 341 entries 

// User inputs 
#define LEVEL_C0_NUM_ENTRY 654720	// number of key value pairs in the first LEVEL 327360
#define LEVEL_BRANCHING 2	// branching factor, 2 x is twice the previous LEVEL 
#define LEVEL_NUM_TOTAL 5	// total number of LEVELs 

// int LEVEL_ID;	// k th LEVEL, k ranges from 0 to LEVEL_NUM_TOTAL
#define LEVEL_NUM_IN_MEMORY 2	// number of LEVELs in memory 
// #define MMAP_FILESIZE 8192 // mmap size read into memory
#define MAX_LINE_CHARSIZE 24 // number of maxmium character in each line, including 2 spaces,
// 2 RAND_MAX of size 10
#define MIN_LINE_CHARSIZE 5 
#define DEBUGGING 0 // debugging mode, 0 no debugging, 1 yes debugging

typedef struct entry entry_t;	// 12 bytes per entry 
// meta data  about each level 
typedef struct metalevel{
// struct metalevel{
	int size_in_entries; // total number of entries in this level  
	int size_in_bytes;  // total bytes size of the level   
	// int number_of_entries; // number of 
	// entry* next; //a pointer to the next valid entry, not necessary 
	int next; // array, an integer to the next valid entry, next = 0 for file 
	// level means that the file does not exist  

	// int num; // number of valid entries so far, same as next + 1
	// // int size; // size occupied by valid entry so far 
	// bool full; // auxilary info, 1 if size == level_size;

	// FILE* fname;	// point to file, NULL if level in memory 
	char level_fname[20]; // file name, size 0 if level in memory
	// char buf[CACHE_SIZE]; // cache size
	pthread_rwlock_t rw_lock;	// read/write shared-exclusive lock 
	//  If you use the pthread_rwlockattr_setkind_np() function to set 
	// the PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP flag on the attr 
	// that you pass to pthread_rwlock_init()

	entry_t* level_in_memory; // points to this level if this level is in memory 
								// otherwise NULL
// };
} metalevel_t;

// the meta information of all levels  
metalevel_t lsmInfo[LEVEL_NUM_TOTAL]; 

// initialize the levels : a design decision
// void init_levels();


// construct the meta information for the k-th level
// and store that information in lsminfo, also
// construct the level 
void create_memory_level(int k_level);

void create_disk_level(int k_level);

int file_exist (char *filename);

// invalidate the k-th level and reset the meta info
// void destroy_level(int k);

// merge the k-th level with the next k+1th level
// if the next level is empty, then the k-th level occupies the first half 
// of the next level. Otherwise, the k-th level is merged into the 
// occupied part of the next level. If the resulting merge makes k+1 th
// level full, then the k+1 th level is merged with the next level... continue
// until the next level is empty, or if the bottom level is reached 
void lsm_merge(int k_level);

// print the entries of level k 
void print_k_level_entries_in_memory(int k_level);

void print_k_level_entries_in_disk(int k_level);

// merge a level and associated functions 
void merge_sort_level(int k_level);

void merge_sort_in_memory(entry_t* level, entry_t* dup_level, int low, int high);

void merge(entry_t* level, entry_t* dup_level, int low, int mid, int high);

void merge_first_level();

// merge when the k+1th level is in memory 
void merge_in_memory(int k_level, int next_level);

// merge when the k th level is in memory, and k+1th level is in disk  
// since the k+th level is so big, divide k+th into pieces. Scan 
// each piece into a buffer, do merge, and write to file.  
void merge_in_memory_disk();

// merge when the k and k+1th level is in disk  
void merge_in_disk(int k_level, int next_level);

// merge when the k+1 th level is the last level 
// assume the bottom level is infinite? 
// void merge_last_level();

// search key in k_level, return value if found, otherwise return -1 (assume positive values)
int search(int k_level, int key);

int linear_search_first_level(int key);

int binary_search_in_memory(int k_level, int key);

int binary_search_in_disk(int k_level, int key);

int file_exist (char *filename);

#endif 

