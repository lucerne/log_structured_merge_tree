#ifndef LSH_H
#define LSH_H
#include <stdlib.h>
// lsm.h
// 
// API for LSM tree 

// Put key value pair into the LSM tree. 
void put(int key, int value, int op);

// search from the lowest to highest levels, return value of the key 
int get(int key);

// delete the key 
void delete(int key); 

// update the key value pair 
void update(int key, int value);

// initialize the levels : a design decision
void init_levels();

// Printing the metadata information of all levels
void print_metainfo();

// print the entries of all levels
void print_all_level_entries();


#endif 