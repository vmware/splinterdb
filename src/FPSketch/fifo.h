#pragma once

#include "platform.h"
#include "lock.h"
#include <stdlib.h>
#include <stdatomic.h>

typedef struct iceberg_lock {
   void *ptr;
   int   level;
} iceberg_lock;

typedef struct fifo_data {
   void        *kv;
   iceberg_lock lock;
} fifo_data;

typedef struct fifo_node {
   fifo_data         data;
   struct fifo_node *next;
} fifo_node;

typedef struct fifo_queue {
   fifo_node       *head;
   fifo_node       *tail;
   int              head_lock;
   int              tail_lock;
   _Atomic uint64_t size;
} fifo_queue;

fifo_queue *
fifo_queue_create();
fifo_node *
fifo_node_create(void *kv, void *lock_ptr, int level);
void
fifo_node_destroy(fifo_node *node);
void
fifo_enqueue(fifo_queue *q, fifo_node *new_node);
fifo_node *
fifo_dequeue(fifo_queue *q);
void
fifo_queue_destroy(fifo_queue *q);
uint64_t
fifo_queue_size(fifo_queue *q);
