#pragma once

#include "splinterdb/data.h"
#include "platform.h"
#include "data_internal.h"
#include "util.h"
#include "sto_disk_internal.h"
#include "poison.h"


/*
 * Implementation of the classic Strict Timestamp Ordering.
 * The implementation uses a dirty bit to prevent transactions
 * from reading uncommitted data.
 *
 * The dirty bit is also used a latch to protect against race
 * conditions that may appear from running multiple threads.
 */

enum sto_access_rc { STO_ACCESS_OK, STO_ACCESS_BUSY, STO_ACCESS_ABORT };

uint64 global_ts = 0;

static inline txn_timestamp
get_next_global_ts()
{
   return __atomic_add_fetch(&global_ts, 1, __ATOMIC_RELAXED);
}

static void
get_global_timestamps(transactional_splinterdb *txn_kvsb,
                      rw_entry                 *entry,
                      txn_timestamp            *wts,
                      txn_timestamp            *rts)
{
   const splinterdb *kvsb = txn_kvsb->kvsb;

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(kvsb, &result, 0, NULL);

   splinterdb_lookup(kvsb, entry->key, &result);

   if (splinterdb_lookup_found(&result)) {
      _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)&result;
      tuple_header *tuple = merge_accumulator_data(&_result->value);
      if (wts) {
         *wts = tuple->ts.wts;
      }
      if (rts) {
         *rts = tuple->ts.rts;
      }
   }
   splinterdb_lookup_result_deinit(&result);
}

static rw_entry *
rw_entry_create()
{
   rw_entry *new_entry;
   new_entry = TYPED_ZALLOC(0, new_entry);
   platform_assert(new_entry != NULL);
   return new_entry;
}

static inline void
rw_entry_deinit(rw_entry *entry)
{
   if (!slice_is_null(entry->key)) {
      platform_free_from_heap(0, (void *)slice_data(entry->key));
   }

   if (!message_is_null(entry->msg)) {
      platform_free_from_heap(0, (void *)message_data(entry->msg));
   }
}

static inline void
rw_entry_set_key(rw_entry *e, slice key, const data_config *cfg)
{
   char *key_buf;
   key_buf = TYPED_ARRAY_ZALLOC(0, key_buf, slice_length(key));
   memcpy(key_buf, slice_data(key), slice_length(key));
   e->key = slice_create(slice_length(key), key_buf);
}

static inline void
rw_entry_set_msg(rw_entry *e, message msg)
{
   uint64 msg_len = sizeof(tuple_header) + message_length(msg);
   char  *msg_buf;
   msg_buf = TYPED_ARRAY_ZALLOC(0, msg_buf, msg_len);
   memcpy(msg_buf + sizeof(tuple_header), message_data(msg), msg_len);
   e->msg = message_create(message_class(msg), slice_create(msg_len, msg_buf));
}

static inline bool
rw_entry_is_read(const rw_entry *entry)
{
   return entry->is_read;
}

static inline bool
rw_entry_is_write(const rw_entry *entry)
{
   return !message_is_null(entry->msg);
}

/*
 * Will Set timestamps in entry later
 */
static inline rw_entry *
rw_entry_get(transactional_splinterdb *txn_kvsb,
             transaction              *txn,
             slice                     user_key,
             const data_config        *cfg,
             const bool                is_read)
{
   bool      need_to_create_new_entry = TRUE;
   rw_entry *entry                    = NULL;
   const key ukey                     = key_create_from_slice(user_key);
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      entry = txn->rw_entries[i];

      if (data_key_compare(cfg, ukey, key_create_from_slice(entry->key)) == 0) {
         need_to_create_new_entry = FALSE;
         break;
      }
   }

   if (need_to_create_new_entry) {
      entry = rw_entry_create();
      rw_entry_set_key(entry, user_key, cfg);
      txn->rw_entries[txn->num_rw_entries++] = entry;
   }

   entry->is_read = entry->is_read || is_read;
   return entry;
}

static inline void
rw_entry_unlock(lock_table *lock_tbl,
                rw_entry   *entry,
                uint64      UNUSED_PARAM(txn_ts))
{
   // platform_default_log("Unlock key = %s, %lu\n", (char*)entry->key.data,
   // txn_ts);
   lock_table_release_entry_lock(lock_tbl, entry);
}

static inline enum sto_access_rc
rw_entry_try_read_lock(transactional_splinterdb *txn_spl,
                       rw_entry                 *entry,
                       uint64                    txn_ts)
{
   get_global_timestamps(txn_spl, entry, &entry->wts, &entry->rts);
   if (txn_ts < entry->wts) {
      // platform_default_log(
      //    "transaction abort at line %d: txn_ts=%lu, entry->wts=%d\n",
      //    __LINE__,
      //    txn_ts,
      //    entry->wts);
      return STO_ACCESS_ABORT;
   }
   if (lock_table_get_entry_lock_state(txn_spl->lock_tbl, entry)
       == LOCK_TABLE_RC_BUSY)
   {
      return STO_ACCESS_BUSY;
   }
   if (lock_table_try_acquire_entry_lock(txn_spl->lock_tbl, entry)
       == LOCK_TABLE_RC_OK)
   {
      get_global_timestamps(txn_spl, entry, &entry->wts, &entry->rts);
      if (txn_ts < entry->wts) {
         // TO rule would be violated so we need to abort
         rw_entry_unlock(txn_spl->lock_tbl, entry, txn_ts);
         // platform_default_log(
         //    "transaction abort at line %d: txn_ts=%lu, entry->wts=%d\n",
         //    __LINE__,
         //    txn_ts,
         //    entry->wts);
         return STO_ACCESS_ABORT;
      }
   } else {
      return STO_ACCESS_BUSY;
   }
   return STO_ACCESS_OK;
}

static inline enum sto_access_rc
rw_entry_try_write_lock(transactional_splinterdb *txn_spl,
                        rw_entry                 *entry,
                        uint64                    txn_ts)
{
   get_global_timestamps(txn_spl, entry, &entry->wts, &entry->rts);
   // platform_default_log("rw_entry_try_write_lock at line %d: txn_ts=%lu,
   // entry->wts=%d, entry->rts=%d\n", __LINE__, txn_ts, entry->wts,
   // entry->rts);

   if (txn_ts < entry->wts || txn_ts < entry->rts) {
      // TO rule would be violated so we need to abort
      return STO_ACCESS_ABORT;
   }
   if (lock_table_get_entry_lock_state(txn_spl->lock_tbl, entry)
       == LOCK_TABLE_RC_BUSY)
   {
      return STO_ACCESS_BUSY;
   }
   if (lock_table_try_acquire_entry_lock(txn_spl->lock_tbl, entry)
       == LOCK_TABLE_RC_OK)
   {
      get_global_timestamps(txn_spl, entry, &entry->wts, &entry->rts);
      if (txn_ts < entry->wts || txn_ts < entry->rts) {
         // TO rule would be violated so we need to abort
         rw_entry_unlock(txn_spl->lock_tbl, entry, txn_ts);
         return STO_ACCESS_ABORT;
      }
   } else {
      return STO_ACCESS_BUSY;
   }
   return STO_ACCESS_OK;
}

static inline enum sto_access_rc
rw_entry_read_lock(transactional_splinterdb *txn_spl,
                   rw_entry                 *entry,
                   uint64                    txn_ts)
{
   // platform_default_log("Trying to lock key for read at ts = %s, %lu\n",
   // (char*)entry->key.data, txn_ts);
   enum sto_access_rc rc;
   do {
      rc = rw_entry_try_read_lock(txn_spl, entry, txn_ts);
      if (rc == STO_ACCESS_BUSY) {
         // 1us is the value that is mentioned in the paper
         platform_sleep_ns(1000);
      }
   } while (rc == STO_ACCESS_BUSY);
   return rc;
}

static inline enum sto_access_rc
rw_entry_write_lock(transactional_splinterdb *txn_spl,
                    rw_entry                 *entry,
                    uint64                    txn_ts)
{
   // platform_default_log("Trying to lock key for write = %s, %lu\n",
   // (char*)entry->key.data, txn_ts);
   enum sto_access_rc rc;
   do {
      rc = rw_entry_try_write_lock(txn_spl, entry, txn_ts);
      if (rc == STO_ACCESS_BUSY) {
         // 1us is the value that is mentioned in the paper
         platform_sleep_ns(1000);
      }
   } while (rc == STO_ACCESS_BUSY);
   return rc;
}

static void
transactional_splinterdb_config_init(
   transactional_splinterdb_config *txn_splinterdb_cfg,
   const splinterdb_config         *kvsb_cfg)
{
   memcpy(&txn_splinterdb_cfg->kvsb_cfg,
          kvsb_cfg,
          sizeof(txn_splinterdb_cfg->kvsb_cfg));

   txn_splinterdb_cfg->txn_data_cfg =
      TYPED_ZALLOC(0, txn_splinterdb_cfg->txn_data_cfg);
   transactional_data_config_init(kvsb_cfg->data_cfg,
                                  txn_splinterdb_cfg->txn_data_cfg);
   txn_splinterdb_cfg->kvsb_cfg.data_cfg =
      (data_config *)txn_splinterdb_cfg->txn_data_cfg;

   // TODO things like filename, logfile, or data_cfg would need a
   // deep-copy
   txn_splinterdb_cfg->isol_level = TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE;
}

static int
transactional_splinterdb_create_or_open(const splinterdb_config   *kvsb_cfg,
                                        transactional_splinterdb **txn_kvsb,
                                        bool open_existing)
{
   check_experimental_mode_is_valid();
   print_current_experimental_modes();

   transactional_splinterdb_config *txn_splinterdb_cfg;
   txn_splinterdb_cfg = TYPED_ZALLOC(0, txn_splinterdb_cfg);
   transactional_splinterdb_config_init(txn_splinterdb_cfg, kvsb_cfg);

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
   *txn_kvsb           = _txn_kvsb;

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
   platform_assert(txn);
   memset(txn, 0, sizeof(*txn));
   txn->ts = get_next_global_ts();
   // platform_default_log("Starting transaction, ts = %lu\n", txn->ts);
   return 0;
}


static inline void
transaction_deinit(transactional_splinterdb *txn_kvsb, transaction *txn)
{
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      rw_entry_deinit(txn->rw_entries[i]);
      platform_free(0, txn->rw_entries[i]);
   }
}
int
transactional_splinterdb_commit(transactional_splinterdb *txn_kvsb,
                                transaction              *txn)
{
   // unlock all writes and update the DB
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      rw_entry *w = txn->rw_entries[i];
      if (rw_entry_is_write(w)) {
#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
         if (0) {
#endif
            timestamp_set ts = {
               .magic = 0,
               .type = 0,
               .wts = w->wts,
               .rts = w->rts,
            };
            tuple_header *msg = (tuple_header *)message_data(w->msg);
            memcpy(&msg->ts, &ts, sizeof(ts));

            int rc = 0;
            switch (message_class(w->msg)) {
               case MESSAGE_TYPE_INSERT:
                  rc = splinterdb_insert(
                     txn_kvsb->kvsb, w->key, message_slice(w->msg));
                  break;
               case MESSAGE_TYPE_UPDATE:
                  rc = splinterdb_update(
                     txn_kvsb->kvsb, w->key, message_slice(w->msg));
                  break;
               case MESSAGE_TYPE_DELETE:
                  rc = splinterdb_delete(txn_kvsb->kvsb, w->key);
                  break;
               default:
                  break;
            }
            platform_assert(rc == 0, "Error from SplinterDB: %d\n", rc);
#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
         }
#endif
         // w->ts->wts = txn->ts;
         rw_entry_unlock(txn_kvsb->lock_tbl, w, txn->ts);
      }
   }

   transaction_deinit(txn_kvsb, txn);

   return 0;
}

int
transactional_splinterdb_abort(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   // unlock all writes
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      rw_entry *w = txn->rw_entries[i];
      if (rw_entry_is_write(w)) {
         rw_entry_unlock(txn_kvsb->lock_tbl, w, txn->ts);
      }
   }

   transaction_deinit(txn_kvsb, txn);

   return 0;
}

static int
local_write(transactional_splinterdb *txn_kvsb,
            transaction              *txn,
            slice                     user_key,
            message                   msg)
{
   const data_config *cfg   = txn_kvsb->tcfg->kvsb_cfg.data_cfg;
   rw_entry          *entry = rw_entry_get(txn_kvsb, txn, user_key, cfg, FALSE);
   /* if (message_class(msg) == MESSAGE_TYPE_UPDATE */
   /*     || message_class(msg) == MESSAGE_TYPE_DELETE) */
   /* { */
   /*    get_global_timestamps(txn_kvsb, entry, &entry->wts, &entry->rts); */
   /* } */

   if (!rw_entry_is_write(entry)) {
      if (rw_entry_write_lock(txn_kvsb, entry, txn->ts) == STO_ACCESS_ABORT) {
         transactional_splinterdb_abort(txn_kvsb, txn);
         return 1;
      }
      // To prevent deadlocks, we have to update the wts
      entry->wts = txn->ts;
      
      timestamp_set ts = {.magic = TIMESTAMP_UPDATE_MAGIC, .type = TIMESTAMP_UPDATE_WTS, .rts = entry->rts, .wts = entry->wts};
      splinterdb_update(
         txn_kvsb->kvsb, entry->key, slice_create(sizeof(ts), &ts));
      // platform_default_log("Locked key for write = %s, %lu\n",
      // (char*)entry->key.data, (uint64)txn->ts);
      rw_entry_set_msg(entry, msg);
   } else {
      // TODO it needs to be checked later for upsert
      key       wkey = key_create_from_slice(entry->key);
      const key ukey = key_create_from_slice(user_key);
      if (data_key_compare(cfg, wkey, ukey) == 0) {
         if (message_is_definitive(msg)) {
            platform_free_from_heap(0, (void *)message_data(entry->msg));
            rw_entry_set_msg(entry, msg);
         } else {
            platform_assert(message_class(entry->msg) != MESSAGE_TYPE_DELETE);
            merge_accumulator new_message;
            merge_accumulator_init_from_message(&new_message, 0, msg);
            data_merge_tuples(cfg, ukey, entry->msg, &new_message);
            platform_free_from_heap(0, (void *)message_data(entry->msg));
            entry->msg = merge_accumulator_to_message(&new_message);
         }
      }
   }
   return 0;
}

static int
non_transactional_splinterdb_insert(const splinterdb *kvsb,
                                    slice             key,
                                    slice             value)
{
   uint64 value_len = sizeof(tuple_header) + slice_length(value);
   char  *value_buf;
   value_buf = TYPED_ARRAY_ZALLOC(0, value_buf, value_len);
   memcpy(value_buf + sizeof(tuple_header), slice_data(value), value_len);
   int rc = splinterdb_insert(kvsb, key, slice_create(value_len, value_buf));
   platform_free(0, value_buf);
   return rc;
}

int
transactional_splinterdb_insert(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                slice                     value)
{
   if (!txn) {
      return non_transactional_splinterdb_insert(
         txn_kvsb->kvsb, user_key, value);
   }

   return local_write(
      txn_kvsb, txn, user_key, message_create(MESSAGE_TYPE_INSERT, value));
}

int
transactional_splinterdb_delete(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key)
{
   return local_write(txn_kvsb, txn, user_key, DELETE_MESSAGE);
}

int
transactional_splinterdb_update(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                slice                     delta)
{
   return local_write(
      txn_kvsb, txn, user_key, message_create(MESSAGE_TYPE_UPDATE, delta));
}

int
transactional_splinterdb_lookup(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                splinterdb_lookup_result *result)
{
   const data_config *cfg   = txn_kvsb->tcfg->kvsb_cfg.data_cfg;
   rw_entry          *entry = rw_entry_get(txn_kvsb, txn, user_key, cfg, TRUE);

   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;

   if (rw_entry_is_write(entry)) {
      // read my write
      // TODO This works for simple insert/update. However, it doesn't work
      // for upsert.
      // TODO if it succeeded, this read should not be considered for
      // validation. entry->is_read should be false.
      tuple_header *tuple = (tuple_header *)message_data(entry->msg);
      const size_t  value_len =
         message_length(entry->msg) - sizeof(tuple_header);
      merge_accumulator_resize(&_result->value, value_len);
      memcpy(merge_accumulator_data(&_result->value),
             tuple->value,
             merge_accumulator_length(&_result->value));

      return 0;
   }

   int rc = 0;

   if (rw_entry_read_lock(txn_kvsb, entry, txn->ts) == STO_ACCESS_ABORT) {
      transactional_splinterdb_abort(txn_kvsb, txn);
      return 1;
   }
   // platform_default_log("Locked key for read = %s, %lu\n",
   // (char*)entry->key.data, txn->ts);
   rc = splinterdb_lookup(txn_kvsb->kvsb, entry->key, result);

   if (splinterdb_lookup_found(result)) {
      _result = (_splinterdb_lookup_result *)result;
      tuple_header *tuple =
         (tuple_header *)merge_accumulator_data(&_result->value);

      const size_t value_len =
         merge_accumulator_length(&_result->value) - sizeof(tuple_header);
      memmove(merge_accumulator_data(&_result->value), tuple->value, value_len);
      merge_accumulator_resize(&_result->value, value_len);

      if (txn->ts > tuple->ts.wts) {
         // This is hacky to distinguish between read and write timestamps
         // updates by the update size.
         entry->wts       = tuple->ts.wts;
         entry->rts       = txn->ts;
         timestamp_set ts = {.magic = TIMESTAMP_UPDATE_MAGIC, .type = TIMESTAMP_UPDATE_RTS, .rts = entry->rts, .wts = entry->wts};
         splinterdb_update(
            txn_kvsb->kvsb, entry->key, slice_create(sizeof(ts), &ts));
      }
      rw_entry_unlock(txn_kvsb->lock_tbl, entry, txn->ts);
   } else {
      --txn->num_rw_entries;
      rw_entry_unlock(txn_kvsb->lock_tbl, entry, txn->ts);
      rw_entry_deinit(entry);
   }

   return rc;
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

void
transactional_splinterdb_set_isolation_level(
   transactional_splinterdb   *txn_kvsb,
   transaction_isolation_level isol_level)
{
   platform_assert(isol_level > TRANSACTION_ISOLATION_LEVEL_INVALID);
   platform_assert(isol_level < TRANSACTION_ISOLATION_LEVEL_MAX_VALID);

   txn_kvsb->tcfg->isol_level = isol_level;
}
