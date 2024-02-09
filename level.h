#ifndef level_H
#define level_H
#include "lib.h"

// level.h
// 
// Functions, data types for each level of LSM tree 

// key value pairs, value are integers 
// typedef struct entry {
struct entry {
	int key;
	int value;
	int op; 
};


// append a key value pair to end of this level 
void append(int key, int value, int op);

#endif 
