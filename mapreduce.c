#include <stdio.h>
#include <pthread.h>
#include "mapreduce.h"
#include <stdlib.h>
#include <string.h>
#define hashTableSize 5500 
#define valueListSize 20000000


struct data{
        char *key;
        char **value;
	int size;
	struct data* next;
};

int *num_keys;

int *smallest; 

int num_partitions = 0;
struct data ***store;

struct data **currPos;

struct data **freeList;

int *current; //we will traverse each partition later, this stores the current location
pthread_mutex_t *mutex;

Mapper maper;
Reducer reducer;
Partitioner partitioner;




//This is a getter function
char * getter(char *key, int partition_number) {	
//	printf("inside getter\n");
	if (current[partition_number] < store[partition_number][smallest[partition_number]]->next->size) {
		char *value = (store[partition_number][smallest[partition_number]]->next->value)[current[partition_number]];
		current[partition_number]++;
		return value;
	}
	current[partition_number] = 0;
	//free memory
	//add this node into free list
	currPos[partition_number]->next = store[partition_number][smallest[partition_number]]->next;
	store[partition_number][smallest[partition_number]]->next = store[partition_number][smallest[partition_number]]->next->next;
	currPos[partition_number] = currPos[partition_number]->next;
	
	return NULL;
}
	

//The function's role is to take a given key and map it to a number, 
//from 0 to num_partitions - 1.
//num_partitions should equal to number of reduce threads
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}


void MR_Emit(char *key, char *value) {
	int partition_num = partitioner(key, num_partitions);
	pthread_mutex_lock(&mutex[partition_num]);
	int index = MR_DefaultHashPartition(key, hashTableSize);
	//first item in this position, we do need to malloc	
	if (store[partition_num][index]->next == NULL){
		store[partition_num][index]->next  = (struct data*)malloc(sizeof(struct data));
		store[partition_num][index]->next->key = (char*)malloc(strlen(key)+1);
		strcpy(store[partition_num][index]->next->key, key);
		store[partition_num][index]->next->value = (char**)malloc(sizeof(char *) * valueListSize);
		(store[partition_num][index]->next->value)[0] = (char*)malloc(strlen(value) +1);
		strcpy((store[partition_num][index]->next->value)[0], value);
		store[partition_num][index]->next->size = 1;
		store[partition_num][index]->next->next = NULL;
	//	store[partition_num][index]->next = new;
		num_keys[partition_num] ++;
	//	free(new);
	}
	else {
		struct data* curr = store[partition_num][index];
		int find = 0;
		//find if the key is already exist in the hash table
		while (curr->next != NULL) {
			//same key, only add value
			if (strcmp(curr->next->key, key) == 0) {	
				(curr->next->value)[curr->next->size] = (char*)malloc(strlen(value) + 1);	
				strcpy((curr->next->value)[curr->next->size], value);
				curr->next->size = curr->next->size + 1;
				find = 1;
				break;
			}
			else if (strcmp(curr->next->key, key) > 0) {
			//	struct data* new = (struct data*)malloc(sizeof(struct data));
				
				struct data *temp = curr->next;
				curr->next = (struct data*)malloc(sizeof(struct data));
				curr->next->key = (char*)malloc(strlen(key)+1);
				strcpy(curr->next->key, key);
				curr->next->value = (char**)malloc(sizeof(char *) * valueListSize);
				(curr->next->value)[0] = (char*)malloc(strlen(value) +1);
				strcpy((curr->next->value)[0], value);
				curr->next->size = 1;	
				curr->next->next = temp;	
				
			//	new->next = curr->next;
				
		//		curr->next = new;
				find = 1;
				num_keys[partition_num]++;
		//		free(new);
				break;
			}
			curr = curr->next;
		}
		//the larger right now, add it at the end
		if (find == 0) {
			
			//struct data* new = (struct data*)malloc(sizeof(struct data));
                        curr->next = (struct data*)malloc(sizeof(struct data));
			curr->next->key = (char*)malloc(strlen(key)+1);
                        strcpy(curr->next->key, key);
                        curr->next->value = (char**)malloc(sizeof(char *) * valueListSize);
                        (curr->next->value)[0] = (char*)malloc(strlen(value) +1);
                        strcpy((curr->next->value)[0], value);
                        curr->next->size = 1;
			curr->next->next = NULL;
		//	curr->next = new;
			num_keys[partition_num]++;
		//	free(new);
			}
	}
	pthread_mutex_unlock(&mutex[partition_num]);
}


void* mapHelper(void *file) {
	maper((char*)file);
	return 0;
}


void * reducerHelper(void *partition_number) {
	int number = *(int*)partition_number;
	for (int i = 0; i < num_keys[number]; i++) {
		smallest[number] = -1;
		for (int j = 0; j < hashTableSize; j++) {
			if (store[number][j]->next != NULL) {
				//first non null
				if (smallest[number] == -1) {
					smallest[number] = j;
				}
				else if (strcmp(store[number][smallest[number]]->next->key, store[number][j]->next->key) > 0) {
					smallest[number] = j;
				}
			}
		}
		reducer(store[number][smallest[number]]->next->key, getter, number);
	}
	return 0;
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
            Reducer reduce, int num_reducers, Partitioner partition) {
	

	currPos = malloc(num_reducers * sizeof(struct data*));
	current = (int *)calloc(num_reducers, sizeof(int));
	num_keys = (int *)calloc(num_reducers, sizeof(int));
	smallest = (int *)calloc(num_reducers, sizeof(int));

	freeList = malloc(num_reducers * sizeof(struct data*));
	for (int i = 0; i < num_reducers; i++) {
		freeList[i] = malloc(sizeof(struct data));
		freeList[i]->next = NULL;
		currPos[i] = freeList[i];
	}
	

	mutex = (pthread_mutex_t *)malloc(num_reducers * sizeof(pthread_mutex_t));
	for (int i = 0; i < num_reducers; i++){
		pthread_mutex_init(&mutex[i], NULL);
	}
	//number of partition should equal to num reducer
	num_partitions = num_reducers;

	maper = map;
	reducer = reduce;
	partitioner = partition;

	//each store[i] stores the info of each partition
	store = (struct data ***)malloc(num_reducers*sizeof(struct data**));
        for (int i = 0; i < num_reducers; i++) {
                store[i] = (struct data **)malloc(sizeof(struct data*) * hashTableSize);
		for (int j = 0; j < hashTableSize; j++) {
			store[i][j] = (struct data*)malloc(sizeof(struct data));
			store[i][j]->next = NULL;
		}
	}

	pthread_t map_thread[argc-1];
	//for each threads;
	int thread_to_wait_index = 0;
	for (int i = 0; i < argc-1; i++) {
		if (i < num_mappers) { //still some thread that we could create
			pthread_create(&(map_thread[i]), NULL, mapHelper, (void *)argv[i+1]);
		}
		else { //number of files is larger than number of threads
			pthread_join(map_thread[thread_to_wait_index],NULL);
			thread_to_wait_index++;		
			pthread_create(&(map_thread[i]), NULL, mapHelper, (void *)argv[i+1]);
		}	
	}

	int thread_num = 0;
        if (argc-2-num_mappers > -1)
                thread_num = argc-2-num_mappers;
        else
                thread_num = -1;
        for (int i = argc-2; i > thread_num; i--) {
             pthread_join(map_thread[i],NULL);
         }

	pthread_t reduce_thread[num_reducers];	
	
	int *reducer_index = (int *)malloc(num_reducers * sizeof(int));
	for (int i = 0; i < num_reducers; i++) {
		reducer_index[i] = i;
		pthread_create(&(reduce_thread[i]), NULL, reducerHelper, (reducer_index + i));
	}

	for (int i = 0; i < num_reducers; i++) {
		pthread_join(reduce_thread[i], NULL);
	}


	free(current);
	free(num_keys);
	free(smallest);
	free(reducer_index);		
	free(mutex);
	free(currPos);
	
	//free freeList
	struct data *temp;
	for (int i = 0; i < num_reducers; i++) {
		while (freeList[i]->next != NULL) {
			temp = freeList[i]->next;
//			printf("key is %s\n", temp->key);
			freeList[i]->next = freeList[i]->next->next;
			for (int j = 0; j < temp->size; j++) {
				free((temp->value)[j]);
			}
			free(temp->value);
			free(temp->key);
			free(temp);
		}
		free(freeList[i]);
	}
	free(freeList);
	
	
	


	for(int i = 0; i< num_reducers; i++) {
		for (int j = 0; j < hashTableSize; j++) {
			if (j % 2000 == 0)
				printf("reducer is %d and j is %d\n", i, j);
		//	struct data *temp;
		//	while(store[i][j]->next != NULL) {
		//		//free value array
		//		temp = store[i][j]->next;
		//		// printf("inside while loop, the key is %s\n", temp->key);
		//		store[i][j]->next = store[i][j]->next->next;
		//		for (int k = 0; k < temp->size; k++) {
		//			free((temp->value)[k]);
		//		}
		//		free(temp->value);
		//		free(temp->key);
		//		free(temp);
		//	}
			free(store[i][j]);
		}
		free(store[i]);
	}
	free(store);


}
