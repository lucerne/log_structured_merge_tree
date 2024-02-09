#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdint.h>
#include <time.h>      
#include "level.h"

// Specify a specific implementation of data structure 
// and its relevant LSM methods 

// First try: vector 

// Always appending to level 0 
// 12 bytes per entry 
void append(int key, int value, int op){
    // creating a next pointer to the next available entry 
    entry_t* next = lsmInfo[0].level_in_memory + lsmInfo[0].next;

    next->key = key;
    next->value = value; 
    next->op = op;

    if (lsmInfo[0].next < lsmInfo[0].size_in_entries){
        lsmInfo[0].next += 1; 
    }
    else{
        printf("ERROR: lsmInfo overflow!!\n");        
    }

}

  