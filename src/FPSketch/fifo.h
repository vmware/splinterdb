#pragma once

#include "platform.h"
#include "lock.h"
#include "types.h"
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

typedef struct iceberg_data {
   kv_pair      *kv;
   iceberg_block block;
   iceberg_lock  lock;
} iceberg_data;

typedef struct entry {
   iceberg_data data;
   int          lock;
   bool         filled;
} entry;

typedef struct fifo_queue {
   entry   *entries;
   uint64_t max_num_entries;
   uint64_t hand;
} fifo_queue;

fifo_queue *
fifo_queue_create(uint64_t max_num_entries);

void
fifo_queue_destroy(fifo_queue *q);

uint64_t
fifo_queue_hand(fifo_queue *q);