#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdint.h>
#include <time.h>  
#include <math.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <pthread.h>

#include "lib.h"
#include "level.h"
#include "unistd.h"
#include "main.h"


// print the entries of level k, does not work if the function is in lib.c, weird 
// print level k if level k is in memory 
void print_k_level_entries_in_memory(int k_level){

  printf("Begin printing level %d entries: \n", k_level); 
  entry_t* curr = lsmInfo[k_level].level_in_memory;  

  for (int i=0; i < lsmInfo[k_level].next; i++){
    curr = lsmInfo[k_level].level_in_memory + i;
    printf("entry %d: %d %d %d\n", i, curr->key, curr->value, curr->op);
  }
  
}

// print level k in disk
void print_k_level_entries_in_disk(int k_level){

  // print level k if level k exists 
  if (file_exist(lsmInfo[k_level].level_fname)){

    FILE* level_fname = fopen (lsmInfo[k_level].level_fname,"r");

    printf("Begin printing level %d entries: \n", k_level); 
    // read the first entry from the level in disk
    int i=0;
    int key_from_disk;
    int value_from_disk;
    int op_from_disk; 

    // read the first tuple
    while(fscanf(level_fname, "%d%d%d\n", &key_from_disk, &value_from_disk, 
      &op_from_disk) != EOF){
        printf("entry %d: %d %d %d\n", i, key_from_disk, value_from_disk,
        op_from_disk);
      i +=1;
    }
    fclose (level_fname);
  }
  else{
      printf("level %d does not exist\n", k_level);
  }
}

int file_exist (char *filename){
  struct stat buffer;   
  return (stat (filename, &buffer) == 0);
}

// construct the meta information for the k-th level
// and store that information in lsminfo, also
// construct the level 
void create_memory_level(int k){
  // computer number of entries in this level  
  int sz = pow(LEVEL_BRANCHING, k)*LEVEL_C0_NUM_ENTRY;   

  lsmInfo[k].size_in_entries = sz; 
  lsmInfo[k].size_in_bytes = sz*sizeof(entry_t);
  lsmInfo[k].next = 0;

  // prefer writer:
  pthread_rwlockattr_t attr;
  pthread_rwlockattr_setkind_np(&attr,
    PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

  int err = pthread_rwlock_init(&lsmInfo[k].rw_lock, &attr);
  if (err != 0){
    printf("Level %d cannot initiate read write lock\n", k);
  }

  lsmInfo[k].level_in_memory = malloc(lsmInfo[k].size_in_bytes);
  
}

// except last level, which is unlimited 
void create_disk_level(int k){
  int sz = pow(LEVEL_BRANCHING, k)*LEVEL_C0_NUM_ENTRY;   

  lsmInfo[k].size_in_entries = sz;
  lsmInfo[k].size_in_bytes = sz*sizeof(entry_t);
  lsmInfo[k].next = 0;  

  // prefer writer:
  pthread_rwlockattr_t attr;
  pthread_rwlockattr_setkind_np(&attr,
    PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

  int err = pthread_rwlock_init(&lsmInfo[k].rw_lock, &attr);
  if (err != 0){
    printf("Level %d cannot initiate read write lock\n", k);
  }

  snprintf(lsmInfo[k].level_fname, 20, "levels/level%d", k);

}

// invalidate the k-th level and reset the meta info
// void destroy_level(int k);

// Equivalent to flush level k. Called when level 0 is full. Merge all levels  
// the condition is that as long as there is not enough space in the next 
// level to store all the elements (regardless if there are deletes), then 
// do merge.
// assume last level has no deletes 
// assume first level sorted 
// decides when levels should merge 
void lsm_merge(int k_level){
  if (DEBUGGING){
    printf("lsm_merge: Merging level ... %d\n", k_level);
  }

  int next_level = k_level + 1; 

  // next level is the last level 
  if ((next_level + 1) == LEVEL_NUM_TOTAL){

    // last level is the only disk level 
    if (k_level < LEVEL_NUM_IN_MEMORY){

      merge_in_memory_disk();
    }
    else{
      merge_in_disk(k_level, next_level);
    }

    return;
  }

  // next level does not have enough space

  // solution 1: a level is either empty (next is 0, or level file does not exist) 
  // or half full (next is number_of_entries/2, or level file exists)

  // solution 2: smarter, more efficient, especially if there are deletes 
  if (lsmInfo[k_level].next > (lsmInfo[next_level].size_in_entries - 
    lsmInfo[next_level].next)) {

      // after lsm_merge a level, the level is guaranteed to be empty

      // merge next level
      lsm_merge(next_level);

      // next level should be empty now 
    }

  // next level has enough space and is in memory 
  if (next_level < LEVEL_NUM_IN_MEMORY){
    merge_in_memory(k_level, next_level);
  }
  // next level has enough space and is in disk, and the current level is in memory 
  else if (next_level == LEVEL_NUM_IN_MEMORY){
    merge_in_memory_disk();
  }
  // next and current levels are in disk
  else {
    merge_in_disk(k_level, next_level);
  }

}

// merge the k-th level with the next k+1th level
// if the next level is empty, then the k-th level occupies the first half 
// of the next level. Otherwise, the k-th level is merged into the 
// occupied part of the next level. If the resulting merge makes k+1 th
// level full, then the k+1 th level is merged with the next level... continue
// until the next level is empty, or if the bottom level is reached 

// merge when the k+1th level is in memory 
void merge_in_memory(int k_level, int next_level){
  if (DEBUGGING){
    printf("Merge in memory ...\n");
  }

  int err;

  // define this level
  entry_t* k_level_entry = lsmInfo[k_level].level_in_memory;

  int curr_level_index = 0;
  entry_t* curr_k_level_entry;

  // define next level
  entry_t* next_level_entry = lsmInfo[next_level].level_in_memory;
  int next_level_index = 0;
  entry_t* curr_next_level_entry; 

  // if k+1th level is empty, copy to k+1th level
  if (lsmInfo[next_level].next == 0){
    for (int i=0; i < lsmInfo[k_level].next; i++){
      (next_level_entry+i)->key = (k_level_entry+i)->key;      
      (next_level_entry+i)->value = (k_level_entry+i)->value;
      (next_level_entry+i)->op = (k_level_entry+i)->op;
    }

    // reset k and k+1 level next entry  
    lsmInfo[next_level].next = lsmInfo[k_level].next;
    lsmInfo[k_level].next = 0;

    return;
  }

  // if k+1th level is empty, move entries in k+1th level k-th level size to 
  // the right. This will not work if there are deletes, and we expect deletes, 
  // Instead, do the simplest thing of making another k+1 level in memory.

  // create tmp level 
  entry_t* tmp_level_entry = malloc(lsmInfo[next_level].size_in_bytes); 
  int tmp_level_index = 0;
  entry_t* curr_tmp_level_entry;

  while ((curr_level_index < lsmInfo[k_level].next) || (next_level_index <
    lsmInfo[next_level].next)){

    // define current entry for each level, for clarity
    curr_k_level_entry = k_level_entry + curr_level_index;
    curr_next_level_entry = next_level_entry + next_level_index; 
    curr_tmp_level_entry = tmp_level_entry + tmp_level_index;

    // merge current and next level in memory
    if ((curr_level_index < lsmInfo[k_level].next) && (next_level_index < 
    lsmInfo[next_level].next)){

      // copy current level entry
      if (curr_k_level_entry->key < curr_next_level_entry->key){

        curr_tmp_level_entry->key = curr_k_level_entry->key;
        curr_tmp_level_entry->value = curr_k_level_entry->value;
        curr_tmp_level_entry->op = curr_k_level_entry->op;

        curr_level_index += 1;
        tmp_level_index += 1;
      }

      // copy next level entry
      else if ((k_level_entry + curr_level_index)->key > (next_level_entry + 
        next_level_index)->key){

        curr_tmp_level_entry->key = curr_next_level_entry->key;
        curr_tmp_level_entry->value = curr_next_level_entry->value;
        curr_tmp_level_entry->op = curr_next_level_entry->op;

        next_level_index +=1;
        tmp_level_index += 1;
      }
      else {
        // printf("key comparison\n");
        // same key, append cancels delete
        if (curr_k_level_entry->op != curr_next_level_entry->op){
          next_level_index +=1; 
          curr_level_index += 1;        
        }
        // same key, delete the older key in the next level 
        else {
          next_level_index +=1; 
        }
      }
    }

    // copy the rest of current level 
    else if (curr_level_index < lsmInfo[k_level].next) {
      curr_tmp_level_entry->key = curr_k_level_entry->key;
      curr_tmp_level_entry->value = curr_k_level_entry->value;
      curr_tmp_level_entry->op = curr_k_level_entry->op;

      curr_level_index += 1;
      tmp_level_index += 1;
    }

    // copy the rest of next level 
    else{
      curr_tmp_level_entry->key = curr_next_level_entry->key;
      curr_tmp_level_entry->value = curr_next_level_entry->value;
      curr_tmp_level_entry->op = curr_next_level_entry->op;

      next_level_index +=1;
      tmp_level_index += 1;
    }
  }//while

  // acquire lock 
  err = pthread_rwlock_wrlock(&lsmInfo[k_level].rw_lock); 
  if (err != 0){
    perror("merge_in_memory: Cannot acquire current level memory lock\n");
  }

  err = pthread_rwlock_wrlock(&lsmInfo[next_level].rw_lock);   
  if (err != 0){
    perror("merge_in_memory: Cannot acquire next level memory lock\n");
  }

  // free next level entry
  free(lsmInfo[next_level].level_in_memory);

  // re-assign next level entry 
  lsmInfo[next_level].level_in_memory = tmp_level_entry;

  // reset next entry in current and next level 
  lsmInfo[k_level].next = 0; 
  lsmInfo[next_level].next = tmp_level_index;

  // release lock 
  err = pthread_rwlock_unlock(&lsmInfo[k_level].rw_lock); 
  if (err != 0){
    perror("merge_in_memory: Cannot release current level memory lock\n");
  }

  err = pthread_rwlock_unlock(&lsmInfo[next_level].rw_lock);    
  if (err != 0){
    perror("merge_in_memory: Cannot release next level memory lock\n");
  }

}


// merge sort first level in memory, guarantees that sorted first leve contains
// unique keys. First level is full: size_in_entries is same as next 
void merge_sort_level(int k_level){
  if (DEBUGGING){
    printf("merge_sort_level: Merge sort level 0 ... \n");
  }
  entry_t* dup_level = malloc(lsmInfo[k_level].size_in_bytes);
  merge_sort_in_memory(lsmInfo[k_level].level_in_memory, dup_level, 0, 
    lsmInfo[k_level].size_in_entries-1);

  // cancel duplicate and append / delete entries 
  merge_first_level(dup_level);
}

// cancel duplicate and append / delete entries 
void merge_first_level(entry_t* dup_level){
  if (DEBUGGING){
    printf("merge_first_level ... \n");
  }

  int err;

  entry_t* level = lsmInfo[0].level_in_memory;

  // index of unique entry
  int curr_level_index = 0;
  entry_t* curr_level_entry = level;
  entry_t* next_level_entry = level + 1;

  // index of entry, equivalent to next  
  int dup_index = 0;
  entry_t* dup_level_entry = dup_level;

  // first level is always full upon merge
  while(curr_level_index < lsmInfo[0].size_in_entries){
    // define entries 
    curr_level_entry = level + curr_level_index;
    next_level_entry = curr_level_entry + 1; 
    dup_level_entry = dup_level + dup_index;

    // skipping, don't copy
    if (curr_level_entry->key == next_level_entry->key){
      // skip 2 entries if append and delete 
      if (curr_level_entry->op != next_level_entry->op){
        curr_level_index += 2;
      }
      // skip 1 entries if duplicate op
      else{
        curr_level_index += 1;
      }
    }
    // copy to dup
    else{
      dup_level_entry->key = curr_level_entry->key;
      dup_level_entry->value = curr_level_entry->value;
      dup_level_entry->op = curr_level_entry->op;   

      curr_level_index += 1;
      dup_index += 1;
    }
  }

  // acquire lock 
  err = pthread_rwlock_wrlock(&lsmInfo[0].rw_lock); 
  if (err != 0){
    perror("merge_first_level: Cannot acquire level 0 lock\n");
  }

  // free and reset level memory 
  free(lsmInfo[0].level_in_memory);
  lsmInfo[0].level_in_memory = dup_level;
  lsmInfo[0].next = dup_index; 

  // release lock 
  err = pthread_rwlock_unlock(&lsmInfo[0].rw_lock); 
  if (err != 0){
    perror("merge_first_level: Cannot release level 0 lock\n");
  }
}


void merge_sort_in_memory(entry_t* level, entry_t* dup_level, int low, int high){
  if (low < high){
    int mid = (low + high)/2;
    merge_sort_in_memory(level, dup_level, low, mid);
    merge_sort_in_memory(level, dup_level, mid+1, high);
    merge(level, dup_level, low, mid, high);
  }
}

void merge(entry_t* level, entry_t* dup_level, int low, int mid, int high){

  // copy entries from low to high
  entry_t* curr_dup_level = dup_level + low; 
  entry_t* curr_k_level = level + low; 

  for (int i=low; i<= high; i++){
    curr_dup_level->key = curr_k_level->key;
    curr_dup_level->value = curr_k_level->value;
    curr_dup_level->op = curr_k_level->op;

    curr_dup_level += 1;
    curr_k_level += 1;
  }

  int dup_level_left_index = low;
  int dup_level_right_index = mid + 1;
  int curr_index = low;

  while(dup_level_left_index <= mid && dup_level_right_index <= high){
    if ( (dup_level+dup_level_left_index)->key <= 
      (dup_level+dup_level_right_index)->key){

      (level+curr_index)->key = (dup_level+dup_level_left_index)->key;
      (level+curr_index)->value = (dup_level+dup_level_left_index)->value;
      (level+curr_index)->op = (dup_level+dup_level_left_index)->op;

      dup_level_left_index += 1;
    } 
    else{
      (level+curr_index)->key = (dup_level+dup_level_right_index)->key;
      (level+curr_index)->value = (dup_level+dup_level_right_index)->value;
      (level+curr_index)->op = (dup_level+dup_level_right_index)->op;

      dup_level_right_index += 1;
    }
    curr_index += 1;
  }

  int remain_index = mid - dup_level_left_index;
  for(int i=0; i <= remain_index; i++){
      (level+curr_index+i)->key = (dup_level+dup_level_left_index+i)->key;
      (level+curr_index+i)->value = (dup_level+dup_level_left_index+i)->value;
      (level+curr_index+i)->op = (dup_level+dup_level_left_index+i)->op;
  }

}



// merge when the k th level is in memory, and k+1th level is in disk  
// since the k+th level is so big, divide k+th into pieces. Scan 
// each piece into a buffer, do merge, and write to file.  
// hope the OS is buffering read and write 
void merge_in_memory_disk(){
  // print_k_level_entries_in_memory(0);
  if (DEBUGGING){
    printf("merge_in_memory_disk: Merge in memory disk ...\n");
  }
  int curr_level = LEVEL_NUM_IN_MEMORY - 1;
  int next_level = LEVEL_NUM_IN_MEMORY;

  int err;
  // printf("file exists? %d\n", file_exist(lsmInfo[next_level].level_fname));
  // printf("next ... %d\n", lsmInfo[curr_level].next);

  // read the first entry from the level in memory
  entry_t* entry_in_memory = lsmInfo[curr_level].level_in_memory;

  // if next level file does not exist, write all data to disk 
  // Cannot be combined with the next if block due to read / write permission
  // next level file may be empty, then this logc will not carry out. It will
  // be taken care of by the next block 
  if(!file_exist(lsmInfo[next_level].level_fname)){

    // create next level file in disk
    FILE* next_level_in_disk = fopen(lsmInfo[next_level].level_fname, "w+");

    // write current level memory to next level file in disk 
    for(int i=0; i < lsmInfo[curr_level].next; i++){
        fprintf(next_level_in_disk, "%d %d %d\n", entry_in_memory->key, 
          entry_in_memory->value, entry_in_memory->op);        
        entry_in_memory += 1;
    }
    lsmInfo[next_level].next = lsmInfo[curr_level].next;
    lsmInfo[curr_level].next = 0;
    fclose(next_level_in_disk);

    return;
  }


  // create a temporary file on disk
  char tmp_fname[20] = "levels/tmp";
  FILE* tmp_level_in_disk = fopen(tmp_fname, "w+");
  int tmp_next = 0;

  // read from current level in memory 
  int i=0;

  // read from next level files
  FILE* next_level_in_disk = fopen(lsmInfo[next_level].level_fname, "r");

  if (!next_level_in_disk){
    perror("merge_in_memory_disk: Cannot read\n");
    return;
  }


  int next_key_from_disk;
  int next_value_from_disk;
  int next_op_from_disk;

  int next_num_read = fscanf(next_level_in_disk, "%d%d%d\n", &next_key_from_disk, 
       &next_value_from_disk, &next_op_from_disk);

  // next is not the same as size_in_entries, because of delete during merge
  while ((i < lsmInfo[curr_level].next) || 
    (next_num_read != EOF)){

    entry_in_memory = i + lsmInfo[curr_level].level_in_memory; 

    // merge current and next level file 
    if ((i < lsmInfo[curr_level].next) && 
      (next_num_read != EOF)){

      if (entry_in_memory->key < next_key_from_disk){
        fprintf(tmp_level_in_disk, "%d %d %d\n", entry_in_memory->key, 
          entry_in_memory->value, entry_in_memory->op);

        i += 1;
        tmp_next += 1;
      }
      else if (entry_in_memory->key > next_key_from_disk){

        fprintf(tmp_level_in_disk, "%d %d %d\n", next_key_from_disk, 
          next_value_from_disk, next_op_from_disk);

        next_num_read = fscanf(next_level_in_disk, "%d%d%d\n", &next_key_from_disk, 
        &next_value_from_disk, &next_op_from_disk);

        tmp_next += 1;
      }
      else{
        // same key, append cancels delete
        if (entry_in_memory->op != next_op_from_disk){
          i += 1;          

          next_num_read = fscanf(next_level_in_disk, "%d%d%d\n", &next_key_from_disk, 
          &next_value_from_disk, &next_op_from_disk);
        }
        // same key, delete the older key in the next level 
        else {
          next_num_read = fscanf(next_level_in_disk, "%d%d%d\n", &next_key_from_disk, 
          &next_value_from_disk, &next_op_from_disk);
        }
      }
    }

    // copy the rest of the current level file 
    else if (i < lsmInfo[curr_level].next){

      fprintf(tmp_level_in_disk, "%d %d %d\n", entry_in_memory->key, 
        entry_in_memory->value, entry_in_memory->op);

      i += 1;
      tmp_next += 1;
    }
    // copy the rest of the next level file 
    else {
      fprintf(tmp_level_in_disk, "%d %d %d\n", next_key_from_disk, 
        next_value_from_disk, next_op_from_disk);

      next_num_read = fscanf(next_level_in_disk, "%d%d%d\n", &next_key_from_disk, 
      &next_value_from_disk, &next_op_from_disk);

      tmp_next += 1;
    }
  }// while

  // reset level file in memory, close and delete next level file
  fclose(next_level_in_disk);
  fclose(tmp_level_in_disk);

  err = pthread_rwlock_wrlock(&lsmInfo[curr_level].rw_lock); 
  if (err != 0){
    perror("merge_in_memory_disk: Cannot acquire memory lock\n");
  }

  err = pthread_rwlock_wrlock(&lsmInfo[next_level].rw_lock);   
  if (err != 0){
    perror("merge_in_memory_disk: Cannot acquire disk lock\n");
  }

  // destroy next level in disk 
  err = unlink(lsmInfo[next_level].level_fname);
  if (err != 0){
    perror("merge_in_memory_disk: Cannot destory disk level\n");
  }

  // rename tmp to be next level file 
  err = rename(tmp_fname, lsmInfo[next_level].level_fname);
  if (err != 0){
    perror("merge_in_memory_disk: Cannot rename disk level\n");
  }
  // update meta data - reset number of valid entries in this file level 
  lsmInfo[curr_level].next = 0;
  lsmInfo[next_level].next = tmp_next;

  err = pthread_rwlock_unlock(&lsmInfo[curr_level].rw_lock); 
  if (err != 0){
    perror("merge_in_memory_disk: Cannot release memory lock\n");
  }

  err = pthread_rwlock_unlock(&lsmInfo[next_level].rw_lock);    
  if (err != 0){
    perror("merge_in_memory_disk: Cannot release disk lock\n");
  }

} 


// merge when k level is in disk  
// 
void merge_in_disk(int k_level, int next_level){
  if (DEBUGGING){
    printf("Merging in disk ...\n");
  }

  int err;

  // if next level file does not exist, rename the current level file to be the 
  // the next level file 
  if(!file_exist(lsmInfo[next_level].level_fname)){

    // rename: file_on_disk, new_name_of_file_on_disk
    err = rename(lsmInfo[k_level].level_fname, lsmInfo[next_level].level_fname);
    if (err != 0){
      perror("merge_in_disk: Cannot rename disk level\n");
    }

    // update meta data - reset number of valid entries in this level 
    lsmInfo[next_level].next = lsmInfo[k_level].next;  
    lsmInfo[k_level].next = 0; 

    return; 
  }

  // create a temporary file on disk
  char tmp_fname[20] = "levels/tmp";
  FILE* tmp_level_in_disk = fopen(tmp_fname, "w+");
  if (tmp_level_in_disk == NULL){
    perror("merge_in_disk: Cannot open tmp disk level\n");
  }

  int tmp_next = 0;

  // read from cu/rrent level files
  FILE* curr_level_in_disk = fopen(lsmInfo[k_level].level_fname, "r");
  if (curr_level_in_disk == NULL){
    perror("merge_in_disk: Cannot open current disk level\n");
  }

  int curr_key_from_disk;
  int curr_value_from_disk;
  int curr_op_from_disk;

  int curr_num_read = fscanf(curr_level_in_disk, "%d%d%d\n", &curr_key_from_disk, 
       &curr_value_from_disk, &curr_op_from_disk);

  // read from next level files
  FILE* next_level_in_disk = fopen(lsmInfo[next_level].level_fname, "r");
  if (next_level_in_disk == NULL){
    perror("merge_in_disk: Cannot open current disk level\n");
  }

  int next_key_from_disk;
  int next_value_from_disk;
  int next_op_from_disk;

  int next_num_read = fscanf(next_level_in_disk, "%d%d%d\n", &next_key_from_disk, 
       &next_value_from_disk, &next_op_from_disk);

  while ((curr_num_read != EOF) || (next_num_read != EOF)){
    // merge current and next level file 
    if ((curr_num_read != EOF) && (next_num_read != EOF)){
      // TO DO 

      if (curr_key_from_disk < next_key_from_disk){
        fprintf(tmp_level_in_disk, "%d %d %d\n", curr_key_from_disk, 
          curr_value_from_disk, curr_op_from_disk);

        curr_num_read = fscanf(curr_level_in_disk, "%d%d%d\n", &curr_key_from_disk, 
        &curr_value_from_disk, &curr_op_from_disk);

        tmp_next += 1;
      }
      else if (curr_key_from_disk > next_key_from_disk){
        fprintf(tmp_level_in_disk, "%d %d %d\n", next_key_from_disk, 
          next_value_from_disk, next_op_from_disk);

        next_num_read = fscanf(next_level_in_disk, "%d%d%d\n", &next_key_from_disk, 
        &next_value_from_disk, &next_op_from_disk);

        tmp_next += 1;
      }
      else{
        // same key, append cancels delete
        if (curr_op_from_disk != next_op_from_disk){
          curr_num_read = fscanf(curr_level_in_disk, "%d%d%d\n", &curr_key_from_disk, 
          &curr_value_from_disk, &curr_op_from_disk);            

          next_num_read = fscanf(next_level_in_disk, "%d%d%d\n", &next_key_from_disk, 
          &next_value_from_disk, &next_op_from_disk);
        }
        // same key, delete the older key in the next level 
        else {
          next_num_read = fscanf(next_level_in_disk, "%d%d%d\n", &next_key_from_disk, 
          &next_value_from_disk, &next_op_from_disk);
        }
      }
    }

    // copy the rest of the current level file 
    else if (curr_num_read != EOF){
      fprintf(tmp_level_in_disk, "%d %d %d\n", curr_key_from_disk, 
        curr_value_from_disk, curr_op_from_disk);

      curr_num_read = fscanf(curr_level_in_disk, "%d%d%d\n", &curr_key_from_disk, 
         &curr_value_from_disk, &curr_op_from_disk);

      tmp_next += 1;

    }
    // copy the rest of the next level file 
    else {
      fprintf(tmp_level_in_disk, "%d %d %d\n", next_key_from_disk, 
        next_value_from_disk, next_op_from_disk);

      next_num_read = fscanf(next_level_in_disk, "%d%d%d\n", &next_key_from_disk, 
      &next_value_from_disk, &next_op_from_disk);

      tmp_next += 1;

    }
  }// while

  // close and delete current and next level file
  fclose(curr_level_in_disk);
  fclose(next_level_in_disk);
  fclose(tmp_level_in_disk);

  // protect write thread, waiting for read threads to release lock  
  // if don't do together, may fail to find key in next level 
  // (which is in temp file) 
  err = pthread_rwlock_wrlock(&lsmInfo[k_level].rw_lock); 
  if (err != 0){
    perror("merge_in_disk: Cannot acquire current level disk lock\n");
  }

  err = pthread_rwlock_wrlock(&lsmInfo[next_level].rw_lock);   
  if (err != 0){
    perror("merge_in_disk: Cannot acquire next level disk lock\n");
  }


  // destroy current and next level files
  err = unlink(lsmInfo[k_level].level_fname);
  if (err != 0){
    perror("merge_in_disk: Cannot destroy current disk level\n");
  }

  err = unlink(lsmInfo[next_level].level_fname);
  if (err != 0){
    perror("merge_in_disk: Cannot destroy next disk level\n");
  }

  // rename tmp to be next level file 
  err = rename(tmp_fname, lsmInfo[next_level].level_fname);
  if (err != 0){
    perror("merge_in_disk: Cannot rename next disk level\n");
  }

  // update meta data - reset number of valid entries in this file level 
  lsmInfo[next_level].next = tmp_next;
  lsmInfo[k_level].next = 0;

  err = pthread_rwlock_unlock(&lsmInfo[k_level].rw_lock); 
  if (err != 0){
    perror("merge_in_disk: Cannot release current level disk lock\n");
  }

  err = pthread_rwlock_unlock(&lsmInfo[next_level].rw_lock);   
  if (err != 0){
    perror("merge_in_disk: Cannot release next level disk lock\n");
  }

}

// binary search for get 
// linear search for first level 
int search(int k_level, int key){
  if (DEBUGGING){
    printf("search %d level\n", k_level);
  }

  int r, err;
  if (k_level == 0){
    err = pthread_rwlock_rdlock(&lsmInfo[k_level].rw_lock);
    if (err != 0){
      printf("Cannot search level 0 for key %d\n", key);
    }
    r = linear_search_first_level(key);
    pthread_rwlock_unlock(&lsmInfo[k_level].rw_lock);
  }
  else if (k_level < LEVEL_NUM_IN_MEMORY){
    err = pthread_rwlock_rdlock(&lsmInfo[k_level].rw_lock);
    if (err != 0){
      printf("Cannot search level %d for key %d\n", k_level, key);
    }
    r = binary_search_in_memory(k_level, key);   
    pthread_rwlock_unlock(&lsmInfo[k_level].rw_lock);
  }
  else{
    err = pthread_rwlock_rdlock(&lsmInfo[k_level].rw_lock);
    if (err != 0){
      printf("Cannot search level %d for key %d\n", k_level, key);
    }
    r = binary_search_in_disk(k_level, key);   
    pthread_rwlock_unlock(&lsmInfo[k_level].rw_lock);
  }
  return r;
}

// Linear search of the first level, starting from the current position of next
int linear_search_first_level(int key){
  entry_t* level = lsmInfo[0].level_in_memory;

  for (int i=lsmInfo[0].next; i > 0; --i){
    if ((level+i)->key == key){
      // latest is a delete
      if ((level+i)->op == 1){
        return -1; 
      }
      // latest is an append or update
      else{
        return (level+i)->value;
      }
    }
  } // if
  return -1;
}

// level in memory 
int binary_search_in_memory(int k_level, int key){
  if (DEBUGGING){
    printf("binary_search_in_memory %d\n", k_level);
  }

  int high = lsmInfo[k_level].next-1;
  int low = 0;
  int mid; 
  entry_t* mid_entry;

  while(low <= high){

    mid = (low + high)/2;
    mid_entry = lsmInfo[k_level].level_in_memory + mid;

    if (mid_entry->key < key){
      low = mid + 1;
    }
    else if (mid_entry->key > key){
      high = mid - 1;
    }
    else{
      return mid_entry->value;
    }
  }
  return -1;
}

// Assuming level is merged, so the key is unique 
// If level does not exist, return -1 
// BUG: low scan search may not find the thread if the tuple size is small 
//  example: linear generate tuples, define 5 read threads, search for 1 to 5 
// if delete, return -1 
int binary_search_in_disk(int k_level, int key){
  if (DEBUGGING){
    printf("binary_search_in_disk ...\n");
  }
  // printf("lsmInfo[k_level].next: %d\n", lsmInfo[k_level].next);
  if (!file_exist(lsmInfo[k_level].level_fname)){
    return -1;
  }

  // compute everything in fseek, which is a long int byte size
  char *line = NULL;
  size_t len = 0;
  ssize_t read;
  
  int num, file_charsize, low_key, mid_key, high_key, value, op;

  // file position measured in long int bytes (4), point to the first char of the 
  // line
  int low, mid, high;

  FILE* level_file = fopen(lsmInfo[k_level].level_fname, "r");
  if (!level_file){
    perror("binary_search_in_disk: Cannot read\n");
    return -1;
  }

  // find low key 
  low = 0;
  num = fscanf(level_file, "%d%d%d\n", &low_key, &value, &op);

  // find high key, first get file size 
  fseek(level_file, 0L, SEEK_END);
  file_charsize = ftell(level_file);
  high = file_charsize - MAX_LINE_CHARSIZE - 1;

  // first do seek, read the incomplete line, if the incomplete line is none
  // the seek happens to read the beginning of the line, skip the line 
  fseek(level_file, high, SEEK_SET);
  read = getline(&line, &len, level_file);
       // printf("Retrieved line of length %zu :\n", read);
       // printf("%s", line);

  // seek the next complete line 
  fseek(level_file, high + read, SEEK_SET);
  num = fscanf(level_file, "%d%d%d\n", &high_key, &value, &op);

  // binary search in units of file char size 
  // invariant: low, mid, high are starts of line, such that a getline(.. low .. )
  // will get all of the line 
  while(low < high){

    // if the distance is close, just fscanf the next few lines
    if ((high - low) < (2*MAX_LINE_CHARSIZE)){
      fseek(level_file, low, SEEK_SET);
      // scan next 5 entries
      for (int i=0; i < 5; i++){
        num = fscanf(level_file, "%d%d%d\n", &mid_key, &value, &op);

        // if a delete, return -1 
        if (mid_key == key && op == 0){
          fclose(level_file);
          return value;
        }
      }
      fclose(level_file);
      return -1;
    }

    // find mid, read the incomplete line  
    mid = (low + high)/2;
    fseek(level_file, mid, SEEK_SET);
    read = getline(&line, &len, level_file);

    // seek the next complete line
    mid += read; 
    fseek(level_file, mid, SEEK_SET);
    num = fscanf(level_file, "%d%d%d\n", &mid_key, &value, &op);


    // Assuming level is merged, so the key is unique 
    // a better search that avoids missing entries is save high,
    // seek back, getline until reach high, and search for each getline 
    if (mid_key > key){
      high = mid - MAX_LINE_CHARSIZE - 1;
    }
    else if (mid_key < key){
      // since we already read mid line, skip mid line and reset low to 
      // next line right after mid line 
      fseek(level_file, mid, SEEK_SET);
      read = getline(&line, &len, level_file);

      low = mid + read;
    }
    else{
      // if delete, return -1 
      if (op == 0){
        fclose(level_file);
        return value;
      }
      else {
        fclose(level_file);
        return -1;
      }
    }
  } // while

   free(line);
   fclose(level_file);
   return -1;
}

 
