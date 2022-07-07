#include "splinterdb/transaction.h"
#include "transaction_table.h"
#include "lock_table.h"
#include "atomic_counter.h"
#include "mvcc_data.h"

// TODO: implement these functions

typedef struct transaction_handle {
   splinterdb        *kvsb;
   transaction_table *txn_tbl;
   lock_table        *lock_tbl;
   atomic_counter    *g_counter;
   pthread_mutex_t   *lock;
} transaction_handle;

transaction_handle *
splinterdb_transaction_init(const splinterdb *kvsb)
{
   transaction_handle *txn_hdl =
      (transaction_handle *)malloc(sizeof(transaction_handle));
   txn_hdl->txn_tbl = transaction_table_create(TRANSACTION_TABLE_TYPE_QUEUE);

   txn_hdl->lock_tbl = lock_table_create();

   txn_hdl->g_counter = (atomic_counter *)malloc(sizeof(atomic_counter));
   atomic_counter_init(txn_hdl->g_counter);

   txn_hdl->lock = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
   pthread_mutex_init(txn_hdl->lock, 0);

   return txn_hdl;
}

void
splinterdb_transaction_deinit(transaction_handle *txn_hdl)
{
   if (!txn_hdl) {
      return;
   }

   pthread_mutex_destroy(txn_hdl->lock);
   free(txn_hdl->lock);

   atomic_counter_deinit(txn_hdl->g_counter);
   free(txn_hdl->g_counter);

   lock_table_destroy(txn_hdl->lock_tbl);

   transaction_table_destroy(txn_hdl->txn_tbl);
}


transaction_id
splinterdb_transaction_begin(transaction_handle *txn_hdl)
{
   transaction_id txn_id = atomic_counter_get_next(txn_hdl->g_counter);
   transaction_table_insert(txn_hdl->txn_tbl, txn_id);

   return txn_id;
}

inline static bool
_can_commit(transaction_handle      *txn_hdl,
            transaction_table_tuple *curr_txn_tuple)
{
   transaction_table_tuple *tp = transaction_table_first(txn_hdl->txn_tbl);
   while (tp) {
      // FIXME: filter transactions that committed during the execution of this
      // transaction
      if (tp->state == TRANSACTION_STATE_COMMITTED) {
         if (tp->txn_id < curr_txn_tuple->txn_id
             && simple_set_is_overlap(&curr_txn_tuple->read_set,
                                      &tp->write_set))
            return FALSE;

         if (curr_txn_tuple->txn_id < tp->txn_id
             && (simple_set_is_overlap(&curr_txn_tuple->write_set,
                                       &tp->read_set)
                 || simple_set_is_overlap(&curr_txn_tuple->write_set,
                                          &tp->write_set)))
            return FALSE;
      }

      tp = tp->next;
   }

   transaction_table_tuple_state_partially_committed(curr_txn_tuple);

   return TRUE;
}

inline static int
_abort(transaction_handle *txn_hdl, transaction_table_tuple *tuple)
{
   // TODO: Log this abort and make it durable here

   // TODO: iterate all keys in T[txn_id].write_set and send an abort msg using
   // upsert
   simple_set_iter it = simple_set_first(&tuple->write_set);
   while (simple_set_iter_is_valid(it)) {
      transaction_op_meta *meta = simple_set_iter_data(it);
      // How to call upsert?

      lock_table_delete(txn_hdl->lock_tbl, meta->key, meta->key);
   }

   // TODO: remove the tuple from txn table? Or just mark it as aborted
   transaction_table_tuple_state_aborted(tuple);

   return 0;
}

int
splinterdb_transaction_commit(transaction_handle *txn_hdl,
                              transaction_id      txn_id)
{
   pthread_mutex_lock(txn_hdl->lock);

   transaction_table_tuple *tuple =
      transaction_table_lookup(txn_hdl->txn_tbl, txn_id);

   if (!_can_commit(txn_hdl, tuple)) {
      _abort(txn_hdl, tuple);
      return -1;
   }

   // TODO: log this commit here

   transaction_table_tuple_state_committed(tuple);

   pthread_mutex_unlock(txn_hdl->lock);

   return 0;
}

int
splinterdb_transaction_abort(transaction_handle *txn_hdl, transaction_id txn_id)
{
   return _abort(txn_hdl, transaction_table_lookup(txn_hdl->txn_tbl, txn_id));
}

static platform_status
singleton_mvcc_message(transaction_id txn_id, message msg, writable_buffer *wb)
{
   slice           value = message_slice(msg);
   platform_status rc =
      writable_buffer_resize(wb, sizeof(mvcc_message) + mvcc_entry_size(value));
   if (!SUCCESS(rc)) {
      return rc;
   }

   mvcc_message *msg_data = writable_buffer_data(wb);
   msg_data->num_values   = 1;

   mvcc_entry *entry = msg_data->entries;
   entry->txn_id     = txn_id;
   entry->op         = message_class(msg);
   entry->len        = slice_length(value);
   memcpy(entry->data, slice_data(value), entry->len);

   return STATUS_OK;
}

static int
splinterdb_transaction_insert_message(transaction_handle *txn_hdl,
                                      transaction_id      txn_id,
                                      slice               key,
                                      message             msg)
{
   writable_buffer wb;
   writable_buffer_init(&wb, 0); // FIXME: use a valid heap_id

   platform_status rc = singleton_mvcc_message(txn_id, msg, &wb);
   if (!SUCCESS(rc)) {
      return -1;
   }

   transaction_op_meta *meta =
      transaction_op_meta_create(txn_id, key, message_class(msg));

   lock_table_insert(txn_hdl->lock_tbl, key, key, meta);
   transaction_table_tuple *tuple =
      transaction_table_lookup(txn_hdl->txn_tbl, txn_id);
   simple_set_insert(&tuple->write_set, meta);

   splinterdb_update(txn_hdl->kvsb, key, writable_buffer_to_slice(&wb));

   writable_buffer_deinit(&wb);

   return 0;
}

// TODO: make transaction_handle

int
splinterdb_transaction_insert(transaction_handle *txn_hdl,
                              transaction_id      txn_id,
                              slice               key,
                              slice               value)
{
   message msg = message_create(MESSAGE_TYPE_INSERT, value);
   return splinterdb_transaction_insert_message(txn_hdl, txn_id, key, msg);
}

int
splinterdb_transaction_delete(transaction_handle *txn_hdl,
                              transaction_id      txn_id,
                              slice               key)
{
   return splinterdb_transaction_insert_message(
      txn_hdl, txn_id, key, DELETE_MESSAGE);
}


int
splinterdb_transaction_update(transaction_handle *txn_hdl,
                              transaction_id      txn_id,
                              slice               key,
                              slice               delta)
{
   message msg = message_create(MESSAGE_TYPE_UPDATE, delta);
   return splinterdb_transaction_insert_message(txn_hdl, txn_id, key, msg);
}

/* int */
/* tid2_wants_to_see_tid1(transaction_id tid1, transaction_id tid2) */
/* { */
/*    // TODO: implement this */
/*    return 0; */
/* } */

int
splinterdb_transaction_lookup(transaction_handle       *txn_hdl,
                              transaction_id            txn_id,
                              slice                     key,
                              splinterdb_lookup_result *result)
{
   transaction_op_meta *meta = transaction_op_meta_create(
      txn_id, key, MESSAGE_TYPE_PIVOT_DATA); // FIXME: use a correct type

   lock_table_insert(txn_hdl->lock_tbl, key, key, meta);

   transaction_table_tuple *tuple =
      transaction_table_lookup(txn_hdl->txn_tbl, txn_id);
   simple_set_insert(&tuple->read_set, meta);

   splinterdb_lookup(
      txn_hdl->kvsb,
      key,
      result); // return a single mvcc_message, which contains multiple entries

   if (!splinterdb_lookup_found(result)) {
      return 0;
   }

   // Do not allocate a buffer on a stack
   writable_buffer values;
   writable_buffer_init(&values, 0); // FIXME: use a valid heap_id

   char  value_buf[SPLINTERDB_LOOKUP_BUFSIZE];
   slice value = slice_create(SPLINTERDB_LOOKUP_BUFSIZE, value_buf);
   splinterdb_lookup_result_value(txn_hdl->kvsb, result, &value);

   // FIXME: Not implemented yet

   // TODO: choose one from results based on isolation lavel (READ_COMMITTED)
   // TODO: do the same thing like the merge function


   return 0;
}
