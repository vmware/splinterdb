#include "fifo.h"

fifo_queue *
fifo_queue_create()
{
   fifo_queue *q;
   q = TYPED_ZALLOC(0, q);
   if (q == NULL)
      return NULL;

   q->head = fifo_node_create(NULL, NULL, 0);
   if (q->head == NULL) {
      platform_free(0, q);
      return NULL;
   }

   q->tail      = q->head;
   q->head_lock = q->tail_lock = 0;
   return q;
}

fifo_node *
fifo_node_create(void *kv, void *lock_ptr, int level)
{
   fifo_node *node;
   node = TYPED_ZALLOC(0, node);
   if (node == NULL)
      return NULL;
   node->data.kv         = kv;
   node->data.lock.ptr   = lock_ptr;
   node->data.lock.level = level;
   node->next            = NULL;
   return node;
}

void
fifo_node_destroy(fifo_node *node)
{
   platform_free(0, node);
}

uint64_t
fifo_enqueue(fifo_queue *q, fifo_node *new_node)
{
   lock(&q->tail_lock, WAIT_FOR_LOCK);
   new_node->next = NULL;
   q->tail->next  = new_node;
   q->tail        = new_node;
   uint64_t size  = atomic_fetch_add(&q->size, 1) + 1;
   unlock(&q->tail_lock);
   return size;
}

fifo_node *
fifo_dequeue(fifo_queue *q)
{
   fifo_node *to_return;

   lock(&q->head_lock, WAIT_FOR_LOCK);

   to_return = q->head->next;
   if (to_return == NULL) {
      unlock(&q->head_lock);
      return NULL;
   }

   q->head->next = to_return->next;

   if (to_return->next == NULL) {
      lock(&q->tail_lock, WAIT_FOR_LOCK);
      q->tail = q->head;
      unlock(&q->tail_lock);
   }

   atomic_fetch_sub(&q->size, 1);
   unlock(&q->head_lock);
   return to_return;
}

void
fifo_queue_destroy(fifo_queue *q)
{
   fifo_node *current;
   while ((current = fifo_dequeue(q)) != NULL) {
      fifo_node_destroy(current);
   }
   fifo_node_destroy(q->head);
   platform_free(0, q);
}

uint64_t
fifo_queue_size(fifo_queue *q)
{
   return atomic_load(&q->size);
}
