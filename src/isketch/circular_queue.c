#include "circular_queue.h"

static void
circular_queue_init(circular_queue *list)
{
   memset(list->entries, 0, sizeof(entry) * MAX_SIZE);
   list->head = 0;
   list->tail = 0;
   list->size = 0;
}

static void
circular_queue_push_back(circular_queue *list, slice key)
{
   if (list->size == MAX_SIZE) {
      // Queue is full
      return;
   }
   list->entries[list->tail].key = key;
   list->tail                    = (list->tail + 1) % MAX_SIZE;
   list->size++;
}

static void
circular_queue_pop_front(circular_queue *list)
{
   if (list->size == 0) {
      // Queue is empty
      return;
   }
   list->head = (list->head + 1) % MAX_SIZE;
   list->size--;
}

static slice
circular_queue_front(circular_queue *list)
{
   return list->entries[list->head].key;
}

static slice
circular_queue_back(circular_queue *list)
{
   return list->entries[(list->tail - 1 + MAX_SIZE) % MAX_SIZE].key;
}

static void
circular_queue_clear(circular_queue *list)
{
   list->head = 0;
   list->tail = 0;
   list->size = 0;
}

static bool
circular_queue_empty(circular_queue *list)
{
   return list->size == 0;
}

static bool
circular_queue_full(circular_queue *list)
{
   return list->size == MAX_SIZE;
}

void
partitioned_circular_queue_init(partitioned_circular_queue *pcq, int nthreads)
{
   pcq->queues = TYPED_ARRAY_ZALLOC(0, pcq->queues, nthreads);
   for (int i = 0; i < nthreads; i++) {
      circular_queue_init(&pcq->queues[i]);
   }
}

void
partitioned_circular_queue_deinit(partitioned_circular_queue *pcq, int nthreads)
{
   platform_free(0, pcq->queues);
}

void
pcq_push_back(partitioned_circular_queue *pcq, slice key, int tid)
{
   circular_queue_push_back(&pcq->queues[tid], key);
}

void
pcq_pop_front(partitioned_circular_queue *pcq, int tid)
{
   circular_queue_pop_front(&pcq->queues[tid]);
}

slice
pcq_front(partitioned_circular_queue *pcq, int tid)
{
   return circular_queue_front(&pcq->queues[tid]);
}

slice
pcq_back(partitioned_circular_queue *pcq, int tid)
{
   return circular_queue_back(&pcq->queues[tid]);
}

void
pcq_clear(partitioned_circular_queue *pcq, int tid)
{
   circular_queue_clear(&pcq->queues[tid]);
}

bool
pcq_empty(partitioned_circular_queue *pcq, int tid)
{
   return circular_queue_empty(&pcq->queues[tid]);
}

bool
pcq_full(partitioned_circular_queue *pcq, int tid)
{
   return circular_queue_full(&pcq->queues[tid]);
}
