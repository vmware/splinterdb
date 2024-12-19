#pragma once

#include "platform.h"
#include "lock.h"
#include <stdlib.h>
#include <stdatomic.h>

typedef struct iceberg_lock {
   void *ptr;
   int   level;
} iceberg_lock;

typedef struct iceberg_block {
   uint64_t bindex;
   uint64_t boffset;
   uint8_t  slot;
} iceberg_block;

typedef struct fifo_data {
   void         *kv;
   iceberg_block block;
   iceberg_lock  lock;
} fifo_data;

typedef struct fifo_queue {
   fifo_data       *data;
   uint32_t         capacity;
   uint32_t         head;
   uint32_t         tail;
   int              head_lock;
   int              tail_lock;
   _Atomic uint64_t size;
} fifo_queue;

fifo_queue *
fifo_queue_create(uint32_t capacity);

void
fifo_queue_destroy(fifo_queue *q);

// Return: The size of queue after enqueueing
uint64_t
fifo_enqueue(fifo_queue *q, fifo_data data);

// Return 1: queue is not empty
int
fifo_dequeue(fifo_queue *q, fifo_data *data);

uint64_t
fifo_queue_size(fifo_queue *q);
