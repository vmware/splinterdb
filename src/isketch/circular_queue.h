#pragma once

#include "../platform_linux/platform.h"
#include "splinterdb/public_util.h"
#include <string.h>

#define MAX_SIZE 1000

typedef struct {
   slice key;
} entry;

typedef struct {
   entry entries[MAX_SIZE];
   int   head;
   int   tail;
   int   size;
} circular_queue;

typedef struct partitioned_circular_queue {
   circular_queue *queues;
} partitioned_circular_queue;

void
partitioned_circular_queue_init(partitioned_circular_queue *pcq, int nthreads);
void
partitioned_circular_queue_deinit(partitioned_circular_queue *pcq,
                                  int                         nthreads);
void
pcq_push_back(partitioned_circular_queue *pcq, slice key, int tid);
void
pcq_pop_front(partitioned_circular_queue *pcq, int tid);
slice
pcq_front(partitioned_circular_queue *pcq, int tid);
slice
pcq_back(partitioned_circular_queue *pcq, int tid);
void
pcq_clear(partitioned_circular_queue *pcq, int tid);
bool
pcq_empty(partitioned_circular_queue *pcq, int tid);
bool
pcq_full(partitioned_circular_queue *pcq, int tid);
