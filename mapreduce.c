#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "unistd.h"
#include "mapreduce.h"
#include "semaphore.h"
#include <pthread.h>
#include <assert.h>

struct Entry
{
  char *key;
  char *value;
};

struct Entry *immediate;
struct List
{
  struct Entry *etrs;
  int capacity;
  int pos;
  int occupied;
  pthread_mutex_t list_lock;
};

struct partition
{
  struct List list;
};

pthread_mutex_t fileLock = PTHREAD_MUTEX_INITIALIZER;
int count_files;
int total_files;
char **fileNames;
int count_partitions;
int all_partition;
struct partition **partitions;

int num_keys = 0;

void add(struct List *list, char *key, char *value)
{
  pthread_mutex_lock(&list->list_lock);
  if (list->occupied == list->capacity)
  {
    list->capacity *= 2;
    list->capacity++;
    list->etrs = realloc(list->etrs, list->capacity * sizeof(struct Entry));
  }
  list->etrs[list->occupied].key = key;
  list->etrs[list->occupied].value = value;
  list->occupied++;
  pthread_mutex_unlock(&list->list_lock);
}

Partitioner partitioner;
Mapper mapper;
Reducer reducer;

void MR_EmitToCombiner(char *key, char *value)
{
  int partition_num = partitioner(key, all_partition);
  struct partition *thisPartition = partitions[partition_num];
  char *bufferKey = malloc(strlen(key) + 1);
  strcpy(bufferKey, key);
  char *bufferValue = malloc(strlen(value) + 1);
  strcpy(bufferValue, value);
  add(&(thisPartition->list), bufferKey, bufferValue);

  return;
}

void MR_EmitToReducer(char *key, char *value)
{
  int partition_num = partitioner(key, all_partition);
  struct partition *thisPartition = partitions[partition_num];
  char *bufferKey = malloc(strlen(key) + 1);
  strcpy(bufferKey, key);
  char *bufferValue = malloc(strlen(value) + 1);
  strcpy(bufferValue, value);
  add(&(thisPartition->list), bufferKey, bufferValue);

  return;
}

char *get_next(char *key, int partition_number)
{
  struct List *list = &(partitions[partition_number]->list);
  struct Entry *curr_etr = list->etrs;
  int pos = list->pos;
  if (pos >= list->occupied)
  {
    return NULL;
  }
  if (strcmp(key, curr_etr[pos].key) != 0)
    return NULL;
  else
    return curr_etr[(list->pos)++].value;
  return NULL;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
  unsigned long hash = 5381;
  int c;
  while ((c = *key++) != '\0')
    hash = hash * 33 + c;
  return hash % num_partitions;
}

unsigned long MR_SortedPartition(char *key, int num_partitions)
{
  unsigned long num = (unsigned long)atoi(key);
  num = num & 0x0FFFFFFFF;
  unsigned int numbit = 0;
  while (num_partitions >>= 1)
  {
    numbit++;
  }
  return num >> (32 - numbit);
}

int comparator(const void *v1, const void *v2)
{
  struct Entry x = *(struct Entry *)v1;
  struct Entry y = *(struct Entry *)v2;
  return strcmp(x.key, y.key);
}

void sort_partitions(int partition_num)
{
  struct List *list = &(partitions[partition_num]->list);
  qsort(&list->etrs[0], list->occupied, sizeof(struct Entry), comparator);
}

void *reduce_wrapper()
{
  int curPartition;
  for (;;)
  {
    pthread_mutex_lock(&fileLock);
    if (count_partitions < all_partition)
    {
      struct partition *thisPartition = partitions[count_partitions];
      curPartition = count_partitions;
      count_partitions++;
      pthread_mutex_unlock(&fileLock);
      sort_partitions(curPartition);
      struct List *list = &(thisPartition->list);
      if (list->occupied <= 0)
        continue;
      struct Entry *element = &(list->etrs[0]);
      list->pos = 0;
      for (;;)
      {
        reducer(element->key, NULL, get_next, curPartition);
        if (list->pos >= list->occupied)
          break;
        element = &(list->etrs[list->pos]);
      }
    }
    else
    {
      pthread_mutex_unlock(&fileLock);
      return NULL;
    }
  }

  return NULL;
}

char *extract_file()
{
  pthread_mutex_lock(&fileLock);
  char *file;
  if (count_files < total_files)
  {
      file = fileNames[count_files++];
  }
  else
  {
    pthread_mutex_unlock(&fileLock);
    file = NULL;
  }
  pthread_mutex_unlock(&fileLock);
  return file;
}

void *map_wrapper()
{
  char *file;
  while ((file = extract_file()) != NULL)
  {
    mapper(file);
  }
  return NULL;
}

void create(struct List *list)
{
  pthread_mutex_init(&list->list_lock, NULL);
  list->capacity = 1073;
  list->occupied = 0;
  list->etrs = malloc(list->capacity * sizeof(struct Entry));
}

void free_all(struct List *list)
{
  for (int i = 0; i < list->occupied; i++)
  {
    free(list->etrs[i].key);
    free(list->etrs[i].value);
  }
  free(list->etrs);
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
            Reducer reduce, int num_reducers, Combiner combine, Partitioner partition)
{
  pthread_t mappers[num_mappers];
  pthread_t reducers[num_reducers];
  count_files = 0;
  total_files = argc - 1;
  fileNames = &argv[1];
  count_partitions = 0;
  all_partition = num_reducers;
  partitions = calloc(num_reducers, sizeof(struct partition *));
  int i;
  for (i = 0; i < num_reducers; i++)
  {
    partitions[i] = malloc(sizeof(struct partition));
    create(&(partitions[i]->list));
  }
  partitioner = partition;
  reducer = reduce;
  //map
  mapper = map;
  for (i = 0; i < num_mappers; i++)
  {
    pthread_create(&mappers[i], NULL, map_wrapper, NULL);
  }
  for (i = 0; i < num_mappers; i++)
  {
    pthread_join(mappers[i], NULL);
  }
  //reduce(with sort)
  for (i = 0; i < num_reducers; i++)
  {
    pthread_create(&reducers[i], NULL, reduce_wrapper, NULL);
  }
  for (i = 0; i < num_reducers; i++)
  {
    pthread_join(reducers[i], NULL);
  }
  for (i = 0; i < num_reducers; i++)
  {
    free_all(&partitions[i]->list);
  }
  for (i = 0; i < num_reducers; i++)
  {
    free(partitions[i]);
  }
  free(partitions);
}
