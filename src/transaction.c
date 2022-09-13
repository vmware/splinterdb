#include "splinterdb/transaction.h"
#include "transaction_private.h"
#include "splinterdb_private.h"
#include "platform_linux/platform.h"
#include "data_internal.h"
#include "poison.h"
#include <stdlib.h>

tictoc_timestamp_set ZERO_TICTOC_TIMESTAMP_SET = {.rts = 0, .wts = 0};

static int
get_ts_from_splinterdb(const splinterdb     *kvsb,
                       slice                 key,
                       tictoc_timestamp_set *ts)
{
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(kvsb, &result, 0, NULL);

   splinterdb_lookup(kvsb, key, &result);

   slice value;

   if (splinterdb_lookup_found(&result)) {
      splinterdb_lookup_result_value(&result, &value);
      memcpy(ts, slice_data(value), sizeof(tictoc_timestamp_set));
   } else {
      ts->wts = 0;
      ts->rts = 0;
   }

   splinterdb_lookup_result_deinit(&result);

   return 0;
}

/*
 * Algorithm 1: Read Phase
 */

static int
tictoc_read(transactional_splinterdb *txn_kvsb,
            tictoc_transaction       *tt_txn,
            slice                     key,
            splinterdb_lookup_result *result)
{
   tictoc_rw_entry *r = tictoc_get_new_read_set_entry(tt_txn);
   platform_assert(!tictoc_rw_entry_is_invalid(r));

   int rc = splinterdb_lookup(txn_kvsb->kvsb, key, result);

   if (splinterdb_lookup_found(result)) {
      slice value;
      splinterdb_lookup_result_value(result, &value);
      writable_buffer_init_from_slice(&r->tuple,
                                      0,
                                      value); // FIXME: use a correct heap_id
      writable_buffer_init_from_slice(
         &r->key, 0, key); // FIXME: use a correct heap_id

      tictoc_tuple_header       *tuple   = writable_buffer_data(&r->tuple);
      _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
      uint64 app_value_size = merge_accumulator_length(&_result->value)
                              - sizeof(tictoc_tuple_header);
      memmove(
         merge_accumulator_data(&_result->value), tuple->value, app_value_size);
      merge_accumulator_resize(&_result->value, app_value_size);
   } else {
      tictoc_delete_last_read_set_entry(tt_txn);
   }

   return rc;
}

/*
 * Algorithm 2: Validation Phase
 */
static bool
tictoc_validation(transactional_splinterdb *txn_kvsb,
                  tictoc_transaction       *tt_txn)
{
   // Step 2: Compute the Commit Timestamp
   tt_txn->commit_ts = 0;

   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      tictoc_rw_entry     *w = tictoc_get_write_set_entry(tt_txn, i);
      tictoc_timestamp_set tuple_ts;
      get_ts_from_splinterdb(
         txn_kvsb->kvsb, writable_buffer_to_slice(&w->key), &tuple_ts);
      tt_txn->commit_ts = MAX(tt_txn->commit_ts, tuple_ts.rts + 1);
   }

   for (uint64 i = 0; i < tt_txn->read_cnt; ++i) {
      tictoc_rw_entry     *r        = tictoc_get_read_set_entry(tt_txn, i);
      tictoc_timestamp_set entry_ts = get_ts_from_tictoc_rw_entry(r);
      tt_txn->commit_ts             = MAX(tt_txn->commit_ts, entry_ts.wts);
   }

   // Step 3: Validate the Read Set
   for (uint64 i = 0; i < tt_txn->read_cnt; ++i) {
      tictoc_rw_entry *r = tictoc_get_read_set_entry(tt_txn, i);

      slice rkey = writable_buffer_to_slice(&r->key);

      tictoc_timestamp_set read_entry_ts = get_ts_from_tictoc_rw_entry(r);

      bool is_read_entry_invalid = read_entry_ts.rts < tt_txn->commit_ts;
      if (is_read_entry_invalid) {
         platform_mutex_lock(&txn_kvsb->g_lock);

         tictoc_timestamp_set tuple_ts;
         get_ts_from_splinterdb(txn_kvsb->kvsb, rkey, &tuple_ts);

         bool is_read_entry_written_by_another =
            read_entry_ts.wts != tuple_ts.wts;
         bool is_read_entry_locked_by_another =
            tuple_ts.rts <= tt_txn->commit_ts
            && lock_table_is_range_locked(txn_kvsb->lock_tbl, rkey, rkey)
            && tictoc_rw_entry_is_not_in_write_set(
               tt_txn,
               r,
               txn_kvsb->tcfg->txn_data_cfg->application_data_config);

         bool need_to_abort =
            is_read_entry_written_by_another || is_read_entry_locked_by_another;
         if (need_to_abort) {
            platform_mutex_unlock(&txn_kvsb->g_lock);
            return FALSE;
         }

         uint32 new_rts            = MAX(tt_txn->commit_ts, tuple_ts.rts);
         bool   need_to_update_rts = new_rts != tuple_ts.rts;
         if (need_to_update_rts) {
            writable_buffer ts;
            writable_buffer_init(&ts, 0); // FIXME: use a correct heap_id
            writable_buffer_append(&ts, sizeof(tictoc_timestamp), &new_rts);
            splinterdb_update(
               txn_kvsb->kvsb, rkey, writable_buffer_to_slice(&ts));
            writable_buffer_deinit(&ts);
         }

         platform_mutex_unlock(&txn_kvsb->g_lock);
      }
   }

   return TRUE;
}

/*
 * Algorithm 3: Write Phase
 */
static void
tictoc_write(transactional_splinterdb *txn_kvsb, tictoc_transaction *tt_txn)
{
   const splinterdb *kvsb = txn_kvsb->kvsb;

   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      tictoc_rw_entry     *w              = &tt_txn->write_set[i];
      tictoc_timestamp_set write_entry_ts = get_ts_from_tictoc_rw_entry(w);
      if (write_entry_ts.wts != tt_txn->commit_ts
          || write_entry_ts.rts != tt_txn->commit_ts)
      {
         write_entry_ts.wts = tt_txn->commit_ts;
         write_entry_ts.rts = tt_txn->commit_ts;

         tictoc_tuple_header *tuple = writable_buffer_data(&w->tuple);
         memcpy(&tuple->ts_set, &write_entry_ts, sizeof(tictoc_timestamp_set));
      }

      slice wkey = writable_buffer_to_slice(&w->key);

      int rc = 0;

      // TODO: merge messages in the write set and write to splinterdb
      switch (w->op) {
         case MESSAGE_TYPE_INSERT:
            rc = splinterdb_insert(
               kvsb, wkey, writable_buffer_to_slice(&w->tuple));
            break;
         case MESSAGE_TYPE_UPDATE:
            rc = splinterdb_update(
               kvsb, wkey, writable_buffer_to_slice(&w->tuple));
            break;
         case MESSAGE_TYPE_DELETE:
            rc = splinterdb_delete(kvsb, wkey);
            break;
         default:
            break;
      }

      platform_assert(rc == 0, "Error from SplinterDB: %d\n", rc);

      writable_buffer_deinit(&w->tuple);
   }
}

static int
tictoc_local_write(transactional_splinterdb *txn_kvsb,
                   tictoc_transaction       *txn,
                   tictoc_timestamp_set      ts_set,
                   slice                     key,
                   message                   msg)
{
   const data_config *cfg =
      txn_kvsb->tcfg->txn_data_cfg->application_data_config;

   // TODO: this part can be done by binary search if the write_set is sorted
   for (uint64 i = 0; i < txn->write_cnt; ++i) {
      tictoc_rw_entry *w    = tictoc_get_write_set_entry(txn, i);
      slice            wkey = writable_buffer_to_slice(&w->key);
      if (data_key_compare(cfg, wkey, key) == 0) {
         if (message_is_definitive(msg)) {
            w->op = message_class(msg);

            slice value = message_slice(msg);
            writable_buffer_resize(
               &w->tuple, sizeof(tictoc_timestamp_set) + slice_length(value));

            tictoc_tuple_header *tuple = writable_buffer_data(&w->tuple);

            memcpy(&tuple->ts_set, &ts_set, sizeof(tictoc_timestamp_set));
            memcpy(tuple->value, slice_data(value), slice_length(value));
         } else {
            platform_assert(w->op != MESSAGE_TYPE_DELETE);

            merge_accumulator new_message;
            merge_accumulator_init_from_message(&new_message, 0, msg);

            tictoc_tuple_header *tuple = writable_buffer_data(&w->tuple);
            slice   old_value   = slice_create(writable_buffer_length(&w->tuple)
                                              - sizeof(tictoc_tuple_header),
                                           tuple->value);
            message old_message = message_create(w->op, old_value);

            data_merge_tuples(cfg, key, old_message, &new_message);

            writable_buffer_resize(&w->tuple,
                                   sizeof(tictoc_timestamp_set)
                                      + merge_accumulator_length(&new_message));

            tuple = writable_buffer_data(&w->tuple);

            memcpy(&tuple->ts_set, &ts_set, sizeof(tictoc_timestamp_set));
            memcpy(tuple->value,
                   merge_accumulator_data(&new_message),
                   merge_accumulator_length(&new_message));

            merge_accumulator_deinit(&new_message);
         }

         return 0;
      }
   }

   tictoc_rw_entry *w = tictoc_get_new_write_set_entry(txn);
   platform_assert(!tictoc_rw_entry_is_invalid(w));

   w->op = message_class(msg);
   writable_buffer_init_from_slice(&w->key,
                                   0,
                                   key); // FIXME: Use a correct heap id

   slice value = message_slice(msg);

   writable_buffer_init(&w->tuple, 0); // FIXME: use a correct heap_id
   writable_buffer_resize(&w->tuple,
                          sizeof(tictoc_timestamp_set) + slice_length(value));

   tictoc_tuple_header *tuple = writable_buffer_data(&w->tuple);

   memcpy(&tuple->ts_set, &ts_set, sizeof(tictoc_timestamp_set));
   memcpy(tuple->value, slice_data(value), slice_length(value));

   return 0;
}

static int
transactional_splinterdb_create_or_open(const splinterdb_config   *kvsb_cfg,
                                        transactional_splinterdb **txn_kvsb,
                                        bool open_existing)
{
   transactional_splinterdb_config *txn_splinterdb_cfg;
   txn_splinterdb_cfg = TYPED_ZALLOC(0, txn_splinterdb_cfg);
   memcpy(txn_splinterdb_cfg, kvsb_cfg, sizeof(txn_splinterdb_cfg->kvsb_cfg));

   txn_splinterdb_cfg->txn_data_cfg =
      TYPED_ZALLOC(0, txn_splinterdb_cfg->txn_data_cfg);
   transactional_data_config_init(kvsb_cfg->data_cfg,
                                  txn_splinterdb_cfg->txn_data_cfg);

   txn_splinterdb_cfg->kvsb_cfg.data_cfg =
      (data_config *)txn_splinterdb_cfg->txn_data_cfg;

   transactional_splinterdb *_txn_kvsb;
   _txn_kvsb       = TYPED_ZALLOC(0, _txn_kvsb);
   _txn_kvsb->tcfg = txn_splinterdb_cfg;

   int rc = splinterdb_create_or_open(
      &txn_splinterdb_cfg->kvsb_cfg, &_txn_kvsb->kvsb, open_existing);
   bool fail_to_create_splinterdb = (rc != 0);
   if (fail_to_create_splinterdb) {
      platform_free(0, _txn_kvsb);
      platform_free(0, txn_splinterdb_cfg->txn_data_cfg);
      platform_free(0, txn_splinterdb_cfg);
      return rc;
   }

   _txn_kvsb->lock_tbl = lock_table_create(kvsb_cfg->data_cfg);

   platform_mutex_init(&_txn_kvsb->g_lock, 0, 0);

   *txn_kvsb = _txn_kvsb;

   return 0;
}

int
transactional_splinterdb_create(const splinterdb_config   *kvsb_cfg,
                                transactional_splinterdb **txn_kvsb)
{
   return transactional_splinterdb_create_or_open(kvsb_cfg, txn_kvsb, FALSE);
}


int
transactional_splinterdb_open(const splinterdb_config   *kvsb_cfg,
                              transactional_splinterdb **txn_kvsb)
{
   return transactional_splinterdb_create_or_open(kvsb_cfg, txn_kvsb, TRUE);
}

void
transactional_splinterdb_close(transactional_splinterdb **txn_kvsb)
{
   transactional_splinterdb *_txn_kvsb = *txn_kvsb;
   splinterdb_close(&_txn_kvsb->kvsb);

   platform_mutex_destroy(&_txn_kvsb->g_lock);

   lock_table_destroy(_txn_kvsb->lock_tbl);

   platform_free(0, _txn_kvsb->tcfg->txn_data_cfg);
   platform_free(0, _txn_kvsb->tcfg);
   platform_free(0, _txn_kvsb);

   *txn_kvsb = NULL;
}

void
transactional_splinterdb_register_thread(transactional_splinterdb *kvs)
{
   splinterdb_register_thread(kvs->kvsb);
}

void
transactional_splinterdb_deregister_thread(transactional_splinterdb *kvs)
{
   splinterdb_deregister_thread(kvs->kvsb);
}

int
transactional_splinterdb_begin(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   tictoc_transaction_init(&txn->tictoc);
   return 0;
}

int
transactional_splinterdb_commit(transactional_splinterdb *txn_kvsb,
                                transaction              *txn)
{
   tictoc_transaction *tt_txn = &txn->tictoc;

   bool write_successfully = FALSE;

   // Step 1: Lock Write Set
   tictoc_transaction_sort_write_set(
      tt_txn, txn_kvsb->tcfg->txn_data_cfg->application_data_config);

   while (tictoc_transaction_lock_all_write_set(tt_txn, txn_kvsb->lock_tbl)
          == FALSE)
   {
      platform_sleep(1000); // 1us is the value that is mentioned in the paper
   }

   if (tictoc_validation(txn_kvsb, &txn->tictoc)) {
      tictoc_write(txn_kvsb, &txn->tictoc);
      write_successfully = TRUE;
   }

   tictoc_transaction_unlock_all_write_set(tt_txn, txn_kvsb->lock_tbl);
   tictoc_transaction_deinit(tt_txn, txn_kvsb->lock_tbl);

   return write_successfully ? 0 : -1;
}

int
transactional_splinterdb_abort(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   tictoc_transaction_deinit(&txn->tictoc, txn_kvsb->lock_tbl);

   return 0;
}

int
transactional_splinterdb_insert(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key,
                                slice                     value)
{
   return tictoc_local_write(txn_kvsb,
                             &txn->tictoc,
                             ZERO_TICTOC_TIMESTAMP_SET,
                             key,
                             message_create(MESSAGE_TYPE_INSERT, value));
}

int
transactional_splinterdb_delete(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key)
{
   return tictoc_local_write(
      txn_kvsb, &txn->tictoc, ZERO_TICTOC_TIMESTAMP_SET, key, DELETE_MESSAGE);
}

int
transactional_splinterdb_update(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key,
                                slice                     delta)
{
   return tictoc_local_write(txn_kvsb,
                             &txn->tictoc,
                             ZERO_TICTOC_TIMESTAMP_SET,
                             key,
                             message_create(MESSAGE_TYPE_UPDATE, delta));
}

int
transactional_splinterdb_lookup(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key,
                                splinterdb_lookup_result *result)
{
   return tictoc_read(txn_kvsb, &txn->tictoc, key, result);
}

void
transactional_splinterdb_lookup_result_init(
   transactional_splinterdb *txn_kvsb,   // IN
   splinterdb_lookup_result *result,     // IN/OUT
   uint64                    buffer_len, // IN
   char                     *buffer      // IN
)
{
   return splinterdb_lookup_result_init(
      txn_kvsb->kvsb, result, buffer_len, buffer);
}

uint64
transactional_splinterdb_key_size(transactional_splinterdb *txn_kvsb)
{
   return txn_kvsb->tcfg->kvsb_cfg.data_cfg->key_size;
}
