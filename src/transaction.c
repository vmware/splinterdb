#include "transaction.h"
#include "platform_linux/platform.h"
#include "data_internal.h"
#include "tictoc_data.h"
#include "lock_table.h"
#include "transaction_data_config.h"
#include <stdlib.h>

typedef struct transaction_handle {
   const splinterdb        *kvsb;
   transaction_data_config *tcfg;
   lock_table              *lock_tbl;
} transaction_handle;

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

   return txn_hdl;
}

void
splinterdb_transaction_deinit(transaction_handle *txn_hdl)
{
   if (!txn_hdl) {
      return;
   }

   lock_table_destroy(txn_hdl->lock_tbl);

   free(txn_hdl);
}

typedef struct transaction {
   tictoc_transaction tictoc;
} transaction;

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
   } else {
      // TODO: handle this case: NOT FOUND
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
      lock_table_lock(txn_hdl->lock_tbl, we->key, we->key);
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
         // TODO: Begin atomic section
         TS_word tuple_ts;
         get_ts_from_splinterdb(txn_hdl->kvsb, r->key, &tuple_ts);

         if ((read_entry_ts.wts != tuple_ts.wts)
             || (tuple_ts.rts <= tt_txn->commit_ts
                 && lock_table_is_locked(txn_hdl->lock_tbl, r->key, r->key)
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
         // TODO: End atomic section
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

      splinterdb_update(kvsb, w->key, writable_buffer_to_slice(&w->tuple));
      writable_buffer_deinit(&w->tuple);
      // TODO: merge messages in the write set and write to splinterdb

      lock_table_unlock(txn_hdl->lock_tbl, w->key, w->key);
   }
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
   return 0;
}


static void
tictoc_local_write(tictoc_transaction *txn,
                   uint32              rts,
                   uint32              wts,
                   slice               key,
                   slice               value)
{
   entry *w = tictoc_get_new_write_set_entry(txn);

   w->key = slice_create(slice_length(key), slice_data(key));

   writable_buffer_init(&w->tuple, 0); // FIXME: use a correct heap_id
   writable_buffer_resize(&w->tuple, sizeof(TS_word) + slice_length(value));

   tictoc_tuple *tuple = writable_buffer_data(&w->tuple);

   TS_word write_entry_ts = {.rts = rts, .wts = wts};

   memcpy(&tuple->ts_word, &write_entry_ts, sizeof(TS_word));
   memcpy(tuple->value, slice_data(value), slice_length(value));
}

int
splinterdb_transaction_insert(transaction_handle *txn_hdl,
                              transaction        *txn,
                              slice               key,
                              slice               value)
{
#if 0
  // Lock by inserting an empty record
  writable_buffer wb;
  writable_buffer_init(&wb, 0); // FIXME: use a correct heap id
  writable_buffer_resize(&wb, sizeof(tictoc_tuple));
  
  // TODO: It seems that I need a seperate lock table
  tictoc_tuple *tuple = writable_buffer_data(&wb);
  const uint8 locked = 1;
  tictoc_tuple_init(tuple, locked);

  slice tictoc_value = writable_buffer_to_slice(&wb);

  // TODO: Can we get the address of this record so that we can access
  // it directly later?
  int rc = splinterdb_insert(txn->hdl->kvsb, key, tictoc_value);
  if (rc != 0) {
    // TODO: How to know if there is existing key in splinterdb?
    return rc;
  }

  tictoc_local_write(&txn->tictoc, tuple, value);

  writable_buffer_deinit(&wb);
  
  return rc;
#else
   tictoc_local_write(&txn->tictoc, 0, 0, key, value);

   return 0;
#endif
}

int
splinterdb_transaction_delete(transaction_handle *txn_hdl,
                              transaction        *txn,
                              slice               key)
{
   // TODO: implement this
   return 0;
}

int
splinterdb_transaction_update(transaction_handle *txn_hdl,
                              transaction        *txn,
                              slice               key,
                              slice               delta)
{
   // TODO: implement this
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
