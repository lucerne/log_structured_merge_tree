#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdint.h>
#include <time.h>  
#include <stdbool.h>
#include <sys/stat.h>
#include "lsm.h"
#include "lib.h"

// Assume the best use case behavior or worst case behavior? 
// worst case: what if there are a lot of add and delete of the same item? 
// worst case: what if there are repeated add of the same key but not the same 
// value? Does this make a difference between array versus binary tree implementation? 

extern metalevel_t lsmInfo[LEVEL_NUM_TOTAL]; 

// Look at lsminfo to create, append, or merge
void put(int key, int value, int op){

    // first level is full
    if (lsmInfo[0].next == lsmInfo[0].size_in_entries){

        merge_sort_level(0);
        lsm_merge(0); 
    }

    append(key, value, op); 

}

// search from the highest to lowest levels 
// this can be multithreaded 
// assume positive values, -1 means not found 
// add threads here? 
// return the most recent value if the entry is not a delete, otherwise return -1
// assume in data, can't delete before append 
// merge level 0 before get? 
int get(int key){
  int value = -1;
  int k = 0;
  while (k < LEVEL_NUM_TOTAL && value == -1){
    value = search(k, key);
    k += 1;
  }

  return value;
}

// delete the key 
void delete(int key){
    printf("delete ... ");
    put(key, 0, 1);
}

// update the key value pair 
void update(int key, int value){
    printf("update ... ");
    delete(key);
    put(key, value, 0);
}

// the meta information of all levels  
// calculate if there is enough space? 
void init_levels(){

  printf("creating  ... \n");
 
  int i=0;
  while (i < LEVEL_NUM_IN_MEMORY){
    create_memory_level(i);  

    i += 1;
  }

  while (i < LEVEL_NUM_TOTAL){
    create_disk_level(i);
    i += 1; 
  }

  print_metainfo();
}


// print meta info of all levels  
void print_metainfo(){

  for (int k=0; k < LEVEL_NUM_TOTAL; k++){
    printf("information about %d th level: ", k);
    printf("size in entries: %d\n", lsmInfo[k].size_in_entries);
    printf("size in bytes: %d\n", lsmInfo[k].size_in_bytes);
    printf("number of valid entries: %d\n", lsmInfo[k].next);
    printf("filename: %s\n",lsmInfo[k].level_fname);
    printf("\n");
  }
}


void print_all_level_entries(){
  printf("\nprint_all_level_entries ... \n");
  for(int i=0; i < LEVEL_NUM_TOTAL; i++){
    if (i < LEVEL_NUM_IN_MEMORY){
      print_k_level_entries_in_memory(i);
    }
    else{
      print_k_level_entries_in_disk(i);      
    }
  }
  printf("\n");
}

void print_data_file(char* fileName){

  // print level k if level k exists 
  if (file_exist(fileName)){

    FILE* file = fopen(fileName, "r"); 

    printf("Begin printing file %s: \n", fileName); 
    // read the first entry from the level in disk
    int i=0;
    int key_from_disk;
    int value_from_disk;
    int op_from_disk; 

    // read the first tuple
    while(fscanf(file, "%d%d%d\n", &key_from_disk, &value_from_disk, 
      &op_from_disk) != EOF){
        printf("entry %d: %d %d %d\n", i, key_from_disk, value_from_disk,
        op_from_disk);
      i +=1;
    }
    fclose (file);
  }
  else{
      printf("file %s does not exist\n", fileName);
  }
}
