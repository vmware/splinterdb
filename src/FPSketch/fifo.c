#include "fifo.h"

fifo_queue *
fifo_queue_create(uint64_t max_num_entries)
{
   fifo_queue *q;
   q = TYPED_ZALLOC(0, q);
   if (q == NULL)
      return NULL;

   q->entries = TYPED_ARRAY_ZALLOC(0, q->entries, max_num_entries);
   if (q->entries == NULL) {
      platform_free(0, q);
      return NULL;
   }

   q->max_num_entries = max_num_entries;
   q->hand            = 0;
   return q;
}

void
fifo_queue_destroy(fifo_queue *q)
{
   platform_free(0, q->entries); // Free the entries array
   platform_free(0, q);
}

uint64_t
fifo_queue_next_hand(fifo_queue *q)
{
   uint64_t old_hand, new_hand;
   do {
      old_hand = __atomic_load_n(&q->hand, __ATOMIC_SEQ_CST);
      new_hand = (old_hand + 1) % q->max_num_entries;
   } while (!__atomic_compare_exchange_n(
      &q->hand, &old_hand, new_hand, true, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST));
   return old_hand;
}