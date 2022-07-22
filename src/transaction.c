#include "transaction.h"
#include "platform_linux/platform.h"
#include "data_internal.h"
#include "tictoc_data.h"
#include "lock_table.h"
#include "transaction_data_config.h"
#include <stdlib.h>

TS_word ZERO_TS_WORD = {.rts = 0, .wts = 0};

typedef struct transaction_handle {
   const splinterdb        *kvsb;
   transaction_data_config *tcfg;
   lock_table              *lock_tbl;
   pthread_mutex_t         *g_lock;
} transaction_handle;

typedef struct transaction {
   tictoc_transaction tictoc;
} transaction;

static entry *
tictoc_get_new_read_set_entry(tictoc_transaction *tt_txn)
{
   return &tt_txn->read_set[tt_txn->read_cnt++];
}

static entry *
tictoc_get_new_write_set_entry(tictoc_transaction *tt_txn)
{
   return &tt_txn->write_set[tt_txn->write_cnt++];
}

/*
 * Algorithm 1: Read Phase
 */

static int
tictoc_read(transaction_handle       *txn_hdl,
            tictoc_transaction       *tt_txn,
            slice                     key,
            splinterdb_lookup_result *result)
{
   entry *r = tictoc_get_new_read_set_entry(tt_txn);

   splinterdb_lookup_result tuple_result;
   splinterdb_lookup_result_init(
      txn_hdl->kvsb, &tuple_result, SPLINTERDB_LOOKUP_BUFSIZE, 0);

   int rc = splinterdb_lookup(txn_hdl->kvsb, key, &tuple_result);

   slice value;

   if (splinterdb_lookup_found(&tuple_result)) {
      splinterdb_lookup_result_value(txn_hdl->kvsb, &tuple_result, &value);
      writable_buffer_init_from_slice(
         &r->tuple, 0, value); // FIXME: use a correct heap_id
      r->key = slice_create(slice_length(key), slice_data(key));

      tictoc_tuple *tuple = writable_buffer_data(&r->tuple);
      // TODO: deinit and init again can make users confused?
      splinterdb_lookup_result_deinit(result);
      splinterdb_lookup_result_init(
         txn_hdl->kvsb, result, strlen(tuple->value), tuple->value);
   }

   splinterdb_lookup_result_deinit(&tuple_result);

   return rc;
}

static int
get_ts_from_splinterdb(const splinterdb *kvsb, slice key, TS_word *ts)
{
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(kvsb, &result, SPLINTERDB_LOOKUP_BUFSIZE, 0);

   splinterdb_lookup(kvsb, key, &result);

   slice value;

   if (splinterdb_lookup_found(&result)) {
      splinterdb_lookup_result_value(kvsb, &result, &value);
      memcpy(ts, slice_data(value), sizeof(TS_word));
   } else {
      ts->wts = 0;
      ts->rts = 0;
   }

   splinterdb_lookup_result_deinit(&result);

   return 0;
}

static TS_word
get_ts_from_entry(entry *entry)
{
   TS_word ts;
   memcpy(&ts, writable_buffer_data(&entry->tuple), sizeof(ts));
   return ts;
}

static inline bool
entry_is_not_in_write_set(tictoc_transaction *tt_txn, entry *e)
{
   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      entry *we = &tt_txn->write_set[i];
      if (slices_equal(writable_buffer_to_slice(&we->tuple),
                       writable_buffer_to_slice(&e->tuple)))
      {
         return FALSE;
      }
   }

   return TRUE;
}

static int
entry_key_comp(const void *elem1, const void *elem2)
{
   entry *a = (entry *)elem1;
   entry *b = (entry *)elem2;

   return slice_lex_cmp(a->key, b->key);
}

/*
 * Algorithm 2: Validation Phase
 */
char
tictoc_validation(transaction_handle *txn_hdl, tictoc_transaction *tt_txn)
{
   // Step 1: Lock Write Set
   qsort(tt_txn->write_set,
         tt_txn->write_cnt * sizeof(entry),
         sizeof(entry),
         entry_key_comp);

   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      entry *we = &tt_txn->write_set[i];
      we->latch = lock_table_lock_range(txn_hdl->lock_tbl, we->key, we->key);
   }

   // Step 2: Compute the Commit Timestamp
   tt_txn->commit_ts = 0;

   uint64 total_size = tt_txn->write_cnt + tt_txn->read_cnt;
   for (uint64 i = 0; i < total_size; ++i) {
      entry *e = &tt_txn->entries[i];

      TS_word tuple_ts;
      get_ts_from_splinterdb(txn_hdl->kvsb, e->key, &tuple_ts);

      if (e->type == ENTRY_TYPE_WRITE) {
         tt_txn->commit_ts = MAX(tt_txn->commit_ts, tuple_ts.rts + 1);
      } else if (e->type == ENTRY_TYPE_READ) {
         tt_txn->commit_ts = MAX(tt_txn->commit_ts, tuple_ts.wts);
      } else {
         // invaild
      }
   }

   // Step 3: Validate the Read Set
   for (uint64 i = 0; i < tt_txn->read_cnt; ++i) {
      entry *r = &tt_txn->read_set[i];

      TS_word read_entry_ts = get_ts_from_entry(r);

      if (read_entry_ts.rts < tt_txn->commit_ts) {
         pthread_mutex_lock(txn_hdl->g_lock);
         TS_word tuple_ts;
         get_ts_from_splinterdb(txn_hdl->kvsb, r->key, &tuple_ts);

         if ((read_entry_ts.wts != tuple_ts.wts)
             || (tuple_ts.rts <= tt_txn->commit_ts
                 && lock_table_is_range_locked(
                    txn_hdl->lock_tbl, r->key, r->key)
                 && entry_is_not_in_write_set(tt_txn, r)))
         {
            return 0;
         } else {
            uint32 new_rts = MAX(tt_txn->commit_ts, tuple_ts.rts);

            if (new_rts != tuple_ts.rts) {
               writable_buffer ts;
               writable_buffer_init(&ts, 0); // FIXME: use a correct heap_id
               writable_buffer_append(&ts, TIMESTAMP_SIZE, &new_rts);
               splinterdb_update(
                  txn_hdl->kvsb, r->key, writable_buffer_to_slice(&ts));
               writable_buffer_deinit(&ts);
            }
         }
         pthread_mutex_unlock(txn_hdl->g_lock);
      }
   }

   return 1;
}

/*
 * Algorithm 3: Write Phase
 */
static void
tictoc_write(transaction_handle *txn_hdl, tictoc_transaction *tt_txn)
{
   const splinterdb *kvsb = txn_hdl->kvsb;

   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      entry  *w              = &tt_txn->write_set[i];
      TS_word write_entry_ts = get_ts_from_entry(w);
      if (write_entry_ts.wts != tt_txn->commit_ts
          || write_entry_ts.rts != tt_txn->commit_ts)
      {
         write_entry_ts.wts = tt_txn->commit_ts;
         write_entry_ts.rts = tt_txn->commit_ts;

         tictoc_tuple *tuple = writable_buffer_data(&w->tuple);
         memcpy(&tuple->ts_word, &write_entry_ts, sizeof(TS_word));
      }

      // TODO: merge messages in the write set and write to splinterdb
      switch (w->op) {
         case MESSAGE_TYPE_INSERT:
            splinterdb_insert(
               kvsb, w->key, writable_buffer_to_slice(&w->tuple));
            break;
         case MESSAGE_TYPE_UPDATE:
            splinterdb_update(
               kvsb, w->key, writable_buffer_to_slice(&w->tuple));
            break;
         case MESSAGE_TYPE_DELETE:
            // TODO: Mark the tuple as absent and GC later
            splinterdb_delete(kvsb, w->key);
            break;
         default:
            break;
      }

      writable_buffer_deinit(&w->tuple);

      lock_table_unlock_latch(txn_hdl->lock_tbl, w->latch);
   }
}

static void
tictoc_local_write(tictoc_transaction *txn,
                   TS_word             ts_word,
                   slice               key,
                   message             msg)
{
   entry *w = tictoc_get_new_write_set_entry(txn);

   w->op  = message_class(msg);
   w->key = slice_create(slice_length(key), slice_data(key));

   slice value = message_slice(msg);

   writable_buffer_init(&w->tuple, 0); // FIXME: use a correct heap_id
   writable_buffer_resize(&w->tuple, sizeof(TS_word) + slice_length(value));

   tictoc_tuple *tuple = writable_buffer_data(&w->tuple);

   memcpy(&tuple->ts_word, &ts_word, sizeof(TS_word));
   memcpy(tuple->value, slice_data(value), slice_length(value));
}

static inline char
ts_word_is_nonzero(TS_word ts_word)
{
   return ts_word.rts == 0 && ts_word.wts == 0;
}

transaction_handle *
splinterdb_transaction_init(const splinterdb *kvsb, data_config *cfg)
{
   transaction_handle *txn_hdl =
      (transaction_handle *)malloc(sizeof(transaction_handle));

   txn_hdl->kvsb = kvsb;

   txn_hdl->tcfg =
      (transaction_data_config *)malloc(sizeof(transaction_data_config));
   transaction_data_config_init(cfg, txn_hdl->tcfg);

   txn_hdl->lock_tbl = lock_table_create();

   txn_hdl->g_lock = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
   pthread_mutex_init(txn_hdl->g_lock, 0);

   return txn_hdl;
}

void
splinterdb_transaction_deinit(transaction_handle *txn_hdl)
{
   if (!txn_hdl) {
      return;
   }

   free(txn_hdl->g_lock);

   lock_table_destroy(txn_hdl->lock_tbl);

   free(txn_hdl);
}

transaction *
splinterdb_transaction_begin(transaction_handle *txn_hdl)
{
   transaction *txn = (transaction *)malloc(sizeof(transaction));

   tictoc_transaction *tt_txn = &txn->tictoc;

   tt_txn->read_set  = &tt_txn->entries[0];
   tt_txn->write_set = &tt_txn->entries[SET_SIZE_LIMIT];
   tt_txn->read_cnt  = 0;
   tt_txn->write_cnt = 0;
   tt_txn->commit_ts = 0;

   return txn;
}

int
splinterdb_transaction_commit(transaction_handle *txn_hdl, transaction *txn)
{
   int rc = 0;
   if (tictoc_validation(txn_hdl, &txn->tictoc)) {
      tictoc_write(txn_hdl, &txn->tictoc);
   } else {
      rc = -1;
      splinterdb_transaction_abort(txn_hdl, txn);
   }

   return rc;
}

int
splinterdb_transaction_abort(transaction_handle *txn_hdl, transaction *txn)
{
   // TODO: Implement
   return 0;
}

int
splinterdb_transaction_insert(transaction_handle *txn_hdl,
                              transaction        *txn,
                              slice               key,
                              slice               value)
{
   tictoc_local_write(&txn->tictoc,
                      ZERO_TS_WORD,
                      key,
                      message_create(MESSAGE_TYPE_INSERT, value));
   return 0;
}

int
splinterdb_transaction_delete(transaction_handle *txn_hdl,
                              transaction        *txn,
                              slice               key)
{
   TS_word ts_word = ZERO_TS_WORD;
   get_ts_from_splinterdb(txn_hdl->kvsb, key, &ts_word);

   if (ts_word_is_nonzero(ts_word)) {
      tictoc_local_write(&txn->tictoc, ts_word, key, DELETE_MESSAGE);
   }
   return 0;
}

int
splinterdb_transaction_update(transaction_handle *txn_hdl,
                              transaction        *txn,
                              slice               key,
                              slice               delta)
{
   TS_word ts_word = ZERO_TS_WORD;
   get_ts_from_splinterdb(txn_hdl->kvsb, key, &ts_word);

   tictoc_local_write(
      &txn->tictoc, ts_word, key, message_create(MESSAGE_TYPE_UPDATE, delta));
   return 0;
}

int
splinterdb_transaction_lookup(transaction_handle       *txn_hdl,
                              transaction              *txn,
                              slice                     key,
                              splinterdb_lookup_result *result)
{
   return tictoc_read(txn_hdl, &txn->tictoc, key, result);
}
