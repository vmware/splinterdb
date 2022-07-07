#include "transaction_table_queue.h"
#include "platform.h"

void
transaction_table_tuple_init(transaction_table_tuple *tuple,
                             transaction_id           txn_id)
{
   tuple->txn_id = txn_id;
   tuple->state  = TRANSACTION_STATE_PROVISIONAL;
   simple_set_init(&tuple->read_set);
   simple_set_init(&tuple->write_set);
}

void
transaction_table_tuple_state_partially_committed(
   transaction_table_tuple *tuple)
{
   platform_assert(tuple->state == TRANSACTION_STATE_PROVISIONAL);
   tuple->state = TRANSACTION_STATE_PARTIALLY_COMMITTED;
}

void
transaction_table_tuple_state_committed(transaction_table_tuple *tuple)
{
   platform_assert(tuple->state == TRANSACTION_STATE_PARTIALLY_COMMITTED);
   tuple->state = TRANSACTION_STATE_COMMITTED;
}

void
transaction_table_tuple_state_failed(transaction_table_tuple *tuple)
{
   platform_assert(tuple->state == TRANSACTION_STATE_PROVISIONAL);
   tuple->state = TRANSACTION_STATE_FAILED;
}

void
transaction_table_tuple_state_aborted(transaction_table_tuple *tuple)
{
   platform_assert(tuple->state == TRANSACTION_STATE_FAILED);
   tuple->state = TRANSACTION_STATE_ABORTED;
}

transaction_table *
transaction_table_create(transaction_table_type tbl_type)
{
   transaction_table *out_txn_tbl = 0;

   switch (tbl_type) {
      case TRANSACTION_TABLE_TYPE_QUEUE:
      {
         out_txn_tbl = transaction_table_queue_init();
         break;
      }
      case TRANSACTION_TABLE_TYPE_INVALID:
      default:
         break;
   }

   return out_txn_tbl;
}

void
transaction_table_destroy(transaction_table *txn_tbl)
{
   txn_tbl->deinit(txn_tbl);
}

int
transaction_table_insert(transaction_table *txn_tbl, transaction_id txn_id)
{
   return txn_tbl->insert(txn_tbl, txn_id);
}

int
transaction_table_delete(transaction_table *txn_tbl, transaction_id txn_id)
{
   return txn_tbl->delete (txn_tbl, txn_id);
}

transaction_table_tuple *
transaction_table_lookup(transaction_table *txn_tbl, transaction_id txn_id)
{
   return txn_tbl->lookup(txn_tbl, txn_id);
}

int
transaction_table_update(transaction_table            *txn_tbl,
                         transaction_id                txn_id,
                         transaction_table_update_func do_update)
{
   return txn_tbl->update(txn_tbl, txn_id, do_update);
}

transaction_table_tuple *
transaction_table_first(transaction_table *txn_tbl)
{
   return txn_tbl->first(txn_tbl);
}
