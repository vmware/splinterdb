#include "transaction_table_queue.h"

#include <stdlib.h>
#include <string.h>

// This configuration could be in a user setting
#define TRANSACTION_QUEUE_SIZE_LIMIT 100

typedef struct transaction_table_queue_entry {
   transaction_table_tuple tuple;

   struct transaction_table_queue_entry *prev;
   struct transaction_table_queue_entry *next;
} transaction_table_queue_entry;

typedef struct transaction_table_queue {
   transaction_table super;

   transaction_table_queue_entry *head;
   transaction_table_queue_entry *tail;
} transaction_table_queue;

transaction_table *
transaction_table_queue_init()
{
   // FIXME: use the allocator in SplinterDB
   transaction_table_queue *queue = malloc(sizeof(transaction_table_queue));

   transaction_table *txn_tbl = &queue->super;
   txn_tbl->insert            = transaction_table_queue_insert;
   txn_tbl->delete            = transaction_table_queue_delete;
   txn_tbl->lookup            = transaction_table_queue_lookup;
   txn_tbl->update            = transaction_table_queue_update;
   txn_tbl->deinit            = transaction_table_queue_deinit;

   queue->head = 0;
   queue->tail = 0;

   return txn_tbl;
}

void
transaction_table_queue_deinit(transaction_table *txn_tbl)
{
   transaction_table_queue *queue = (transaction_table_queue *)txn_tbl;

   transaction_table_queue_entry *entry = queue->head;
   while (entry) {
      transaction_table_queue_entry *entry_to_be_deleted = entry;
      entry                                              = entry->next;
      free(entry_to_be_deleted);
   }
   queue->head = 0;
   queue->tail = 0;

   // FIXME: use the allocator in SplinterDB
   free(queue);
}

int
transaction_table_queue_insert(transaction_table *txn_tbl,
                               transaction_id     txn_id)
{
   transaction_table_queue *queue = (transaction_table_queue *)txn_tbl;
   txn_tbl                        = &queue->super;

   // FIXME: use the allocator in SplinterDB
   transaction_table_queue_entry *new_entry =
      malloc(sizeof(transaction_table_queue_entry));

   transaction_table_tuple_init(&new_entry->tuple, txn_id);

   new_entry->prev = queue->tail;
   new_entry->next = 0;

   if (txn_tbl->size == 0) {
      queue->head = new_entry;
      queue->tail = new_entry;

      ++txn_tbl->size;

      return 0;
   }

   /* if (txn_tbl->size + 1 == TRANSACTION_QUEUE_SIZE_LIMIT) { */
   /*    transaction_table_queue_delete((transaction_table *)queue, */
   /*                                   &queue->head->tuple.txn_id); */
   /* } */

   queue->tail->next = new_entry;
   queue->tail       = new_entry;

   ++txn_tbl->size;

   return 0;
}

int
transaction_table_queue_delete(transaction_table *txn_tbl,
                               transaction_id     txn_id)
{
   transaction_table_queue *queue = (transaction_table_queue *)txn_tbl;
   txn_tbl                        = &queue->super;

   if (txn_tbl->size == 0) {
      return 1;
   }

   if (txn_tbl->size == 1) {
      // FIXME: use the allocator in SplinterDB
      free(queue->head);

      queue->head   = 0;
      queue->tail   = 0;
      txn_tbl->size = 0;

      return 0;
   }

   transaction_table_queue_entry *entry = queue->head;

   while (entry) {
      if (entry->tuple.txn_id == txn_id) {
         if (entry == queue->head) {
            queue->head       = entry->next;
            queue->head->prev = 0;
         } else if (entry == queue->tail) {
            queue->tail       = entry->prev;
            queue->tail->next = 0;
         } else {
            entry->prev->next = entry->next;
            entry->next->prev = entry->prev;
         }

         // FIXME: use the allocator in SplinterDB
         free(entry);

         --txn_tbl->size;

         return 0;
      }

      entry = entry->next;
   }

   return 1;
}

transaction_table_tuple *
transaction_table_queue_lookup(transaction_table *txn_tbl,
                               transaction_id     txn_id)
{
   transaction_table_queue *queue = (transaction_table_queue *)txn_tbl;

   transaction_table_queue_entry *entry = queue->head;

   while (entry) {
      if (entry->tuple.txn_id == txn_id) {
         return &entry->tuple;
      }

      entry = entry->next;
   }

   return 0;
}

int
transaction_table_queue_update(transaction_table            *txn_tbl,
                               transaction_id                txn_id,
                               transaction_table_update_func do_update)
{
   transaction_table_queue *queue = (transaction_table_queue *)txn_tbl;

   transaction_table_queue_entry *entry = queue->head;

   while (entry) {
      if (entry->tuple.txn_id == txn_id) {
         do_update(&entry->tuple);
         return 0;
      }

      entry = entry->next;
   }

   return 1;
}
