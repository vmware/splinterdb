#include "fifo.h"

fifo_queue *
fifo_queue_create(uint32_t capacity)
{
   fifo_queue *q;
   q = TYPED_ZALLOC(0, q);
   if (q == NULL)
      return NULL;

   q->data = TYPED_ARRAY_ZALLOC(0, q->data, capacity);
   if (q->data == NULL) {
      platform_free(0, q);
      return NULL;
   }

   q->capacity = capacity;
   q->head = q->tail = 0;
   q->head_lock = q->tail_lock = 0;
   atomic_store(&q->size, 0);
   return q;
}

uint64_t
fifo_enqueue(fifo_queue *q, fifo_data data)
{
   lock(&q->tail_lock, WAIT_FOR_LOCK);

   // Check if queue is full
   if (((q->tail + 1) % q->capacity) == q->head) {
      platform_assert(false, "invalid path for now\n");
      unlock(&q->tail_lock);
      return atomic_load(&q->size); // Queue is full
   }

   // Add the element
   q->data[q->tail] = data;

   // Update tail
   q->tail = (q->tail + 1) % q->capacity;

   uint64_t size = atomic_fetch_add(&q->size, 1) + 1;
   unlock(&q->tail_lock);
   return size;
}

int
fifo_dequeue(fifo_queue *q, fifo_data *data)
{
   lock(&q->head_lock, WAIT_FOR_LOCK);

   // Check if queue is empty
   if (q->head == q->tail) {
      unlock(&q->head_lock);
      return 0;
   }

   // Get the element
   *data = q->data[q->head];

   // Update head
   q->head = (q->head + 1) % q->capacity;

   atomic_fetch_sub(&q->size, 1);
   unlock(&q->head_lock);
   return 1;
}

void
fifo_queue_destroy(fifo_queue *q)
{
   platform_free(0, q->data); // Free the data array
   platform_free(0, q);
}

uint64_t
fifo_queue_size(fifo_queue *q)
{
   return atomic_load(&q->size);
}
