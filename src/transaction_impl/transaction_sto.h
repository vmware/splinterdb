#pragma once

#include "splinterdb/data.h"
#include "platform.h"
#include "data_internal.h"
#include "splinterdb/transaction.h"
#include "util.h"
#include "experimental_mode.h"
#include "splinterdb_internal.h"
#include "FPSketch/iceberg_table.h"
#include <math.h>
#include "poison.h"


/*
 * Implementation of the classic Strict Timestamp Ordering.
 * The implementation uses a dirty bit to prevent transactions
 * from reading uncommitted data.
 *
 * The dirty bit is also used a latch to protect against race
 * conditions that may appear from running multiple threads.
 */

uint64 global_ts = 0;

typedef struct transactional_splinterdb_config {
   splinterdb_config           kvsb_cfg;
   transaction_isolation_level isol_level;
   iceberg_config              iceberght_config;
   sketch_config               sktch_config;
   bool                        is_upsert_disabled;
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   iceberg_table                   *tscache;
} transactional_splinterdb;

typedef struct {
   txn_timestamp dirty_bit : 1;
   txn_timestamp delete_bit : 1;
   txn_timestamp wts : 62;
   txn_timestamp rts : 64;
} timestamp_set __attribute__((aligned(sizeof(txn_timestamp))));

typedef struct rw_entry {
   slice          key;
   message        msg; // value + op
   timestamp_set *ts;
   bool           is_read;
} rw_entry;

enum sto_access_rc { STO_ACCESS_OK, STO_ACCESS_BUSY, STO_ACCESS_ABORT };

static inline txn_timestamp
get_next_global_ts()
{
   return __atomic_add_fetch(&global_ts, 1, __ATOMIC_RELAXED);
}

static inline bool
timestamp_set_compare_and_swap(timestamp_set *ts,
                               timestamp_set *v1,
                               timestamp_set *v2)
{
   return __atomic_compare_exchange((volatile txn_timestamp *)ts,
                                    (txn_timestamp *)v1,
                                    (txn_timestamp *)v2,
                                    TRUE,
                                    __ATOMIC_RELAXED,
                                    __ATOMIC_RELAXED);
}

static inline void
timestamp_set_load(timestamp_set *ts, timestamp_set *v)
{
   __atomic_load(
      (volatile txn_timestamp *)ts, (txn_timestamp *)v, __ATOMIC_RELAXED);
}

static inline void
timestamp_set_set_timestamps(txn_timestamp  wts,
                             txn_timestamp  rts,
                             timestamp_set *ts)
{
   ts->wts = wts;
   ts->rts = rts;
}

static void
sketch_insert_timestamp_set(ValueType *current_value, ValueType new_value)
{
   timestamp_set *current_ts = (timestamp_set *)current_value;
   timestamp_set *new_ts     = (timestamp_set *)&new_value;

   timestamp_set_set_timestamps(MAX(current_ts->wts, new_ts->wts),
                                MAX(current_ts->rts, new_ts->rts),
                                current_ts);
}

static void
sketch_get_timestamp_set(ValueType current_value, ValueType *new_value)
{
   timestamp_set *current_ts = (timestamp_set *)&current_value;
   timestamp_set *new_ts     = (timestamp_set *)new_value;

   timestamp_set_set_timestamps(MIN(current_ts->wts, new_ts->wts),
                                MIN(current_ts->rts, new_ts->rts),
                                new_ts);
}

/*
 * This function has the following effects:
 * A. If entry key is not in the cache, it inserts the key in the cache with
 * refcount=1 and value=0. B. If the key is already in the cache, it just
 * increases the refcount. C. returns the pointer to the value.
 */
static inline bool
rw_entry_iceberg_insert(transactional_splinterdb *txn_kvsb, rw_entry *entry)
{
   // Make sure increasing the refcount only once
   if (entry->ts) {
      return FALSE;
   }

   // increase refcount for key
   timestamp_set ts = {0};
   entry->ts        = &ts;
   return iceberg_insert_and_get(txn_kvsb->tscache,
                                 &entry->key,
                                 (ValueType **)&entry->ts,
                                 platform_get_tid());
}

static inline void
rw_entry_iceberg_remove(transactional_splinterdb *txn_kvsb, rw_entry *entry)
{
   if (!entry->ts) {
      return;
   }

   entry->ts = NULL;

   iceberg_remove(txn_kvsb->tscache, entry->key, platform_get_tid());
}

static rw_entry *
rw_entry_create()
{
   rw_entry *new_entry;
   new_entry = TYPED_ZALLOC(0, new_entry);
   platform_assert(new_entry != NULL);
   new_entry->ts = NULL;
   return new_entry;
}

static inline void
rw_entry_deinit(rw_entry *entry)
{
   if (!message_is_null(entry->msg)) {
      void *ptr = (void *)message_data(entry->msg);
      platform_free(0, ptr);
   }
}

/*
 * The msg is the msg from app.
 * In EXPERIMENTAL_MODE_TICTOC_DISK, this function adds timestamps at the begin
 * of the msg
 */
static inline void
rw_entry_set_msg(rw_entry *e, message msg)
{
   char *msg_buf;
   msg_buf = TYPED_ARRAY_ZALLOC(0, msg_buf, message_length(msg));
   memcpy(msg_buf, message_data(msg), message_length(msg));
   e->msg = message_create(message_class(msg),
                           slice_create(message_length(msg), msg_buf));
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
      entry                                  = rw_entry_create();
      entry->key                             = user_key;
      txn->rw_entries[txn->num_rw_entries++] = entry;
   }

   entry->is_read = entry->is_read || is_read;
   return entry;
}

static inline void
rw_entry_unlock(rw_entry *entry, uint64 txn_ts)
{
   // platform_default_log("Unlock key = %s, %lu\n", (char*)entry->key.data,
   // txn_ts);
   timestamp_set v1, v2;
   do {
      timestamp_set_load(entry->ts, &v1);
      v2           = v1;
      v2.dirty_bit = 0;
   } while (!timestamp_set_compare_and_swap(entry->ts, &v1, &v2));
}

static inline enum sto_access_rc
rw_entry_try_read_lock(rw_entry *entry, uint64 txn_ts)
{
   timestamp_set v1, v2;
   timestamp_set_load(entry->ts, &v1);
   if (txn_ts < v1.wts) {
      return STO_ACCESS_ABORT;
   }
   v2 = v1;
   if (v1.dirty_bit) {
      return STO_ACCESS_BUSY;
   }
   v2.dirty_bit = 1;
   if (timestamp_set_compare_and_swap(entry->ts, &v1, &v2)) {
      if (txn_ts < v1.wts) {
         // TO rule would be violated so we need to abort
         rw_entry_unlock(entry, txn_ts);
         return STO_ACCESS_ABORT;
      }
   } else {
      return STO_ACCESS_BUSY;
   }
   return STO_ACCESS_OK;
}

static inline enum sto_access_rc
rw_entry_try_write_lock(rw_entry *entry, uint64 txn_ts)
{
   timestamp_set v1, v2;
   timestamp_set_load(entry->ts, &v1);
   if (txn_ts < v1.wts || txn_ts < v1.rts) {
      // TO rule would be violated so we need to abort
      return STO_ACCESS_ABORT;
   }
   v2 = v1;
   if (v1.dirty_bit) {
      return STO_ACCESS_BUSY;
   }
   v2.dirty_bit = 1;
   if (timestamp_set_compare_and_swap(entry->ts, &v1, &v2)) {
      if (txn_ts < v1.wts || txn_ts < v1.rts) {
         rw_entry_unlock(entry, txn_ts);
         return STO_ACCESS_ABORT;
      }
   } else {
      return STO_ACCESS_BUSY;
   }
   return STO_ACCESS_OK;
}

static inline enum sto_access_rc
rw_entry_read_lock(rw_entry *entry, uint64 txn_ts)
{
   // platform_default_log("Trying to lock key for read at ts = %s, %lu\n",
   // (char*)entry->key.data, txn_ts);
   enum sto_access_rc rc;
   do {
      rc = rw_entry_try_read_lock(entry, txn_ts);
      if (rc == STO_ACCESS_BUSY) {
         // 1us is the value that is mentioned in the paper
         platform_sleep_ns(1000);
      }
   } while (rc == STO_ACCESS_BUSY);
   return rc;
}

static inline enum sto_access_rc
rw_entry_write_lock(rw_entry *entry, uint64 txn_ts)
{
   // platform_default_log("Trying to lock key for write = %s, %lu\n",
   // (char*)entry->key.data, txn_ts);
   enum sto_access_rc rc;
   do {
      rc = rw_entry_try_write_lock(entry, txn_ts);
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

   iceberg_config_default_init(&txn_splinterdb_cfg->iceberght_config);
   txn_splinterdb_cfg->iceberght_config.log_slots = 12;
   txn_splinterdb_cfg->iceberght_config.merge_value_from_sketch =
      &sketch_insert_timestamp_set;

   // TODO things like filename, logfile, or data_cfg would need a
   // deep-copy
   txn_splinterdb_cfg->isol_level = TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE;
   txn_splinterdb_cfg->is_upsert_disabled = FALSE;

   sketch_config_default_init(&txn_splinterdb_cfg->sktch_config);

   txn_splinterdb_cfg->sktch_config.insert_value_fn =
      &sketch_insert_timestamp_set;
   txn_splinterdb_cfg->sktch_config.get_value_fn = &sketch_get_timestamp_set;

   txn_splinterdb_cfg->iceberght_config.max_num_keys = 1000;
#if EXPERIMENTAL_MODE_STO_COUNTER
   txn_splinterdb_cfg->sktch_config.rows = 1;
   txn_splinterdb_cfg->sktch_config.cols = 1;
#elif EXPERIMENTAL_MODE_STO_COUNTER_LAZY
   txn_splinterdb_cfg->iceberght_config.max_num_keys += 820;
   txn_splinterdb_cfg->sktch_config.rows                     = 1;
   txn_splinterdb_cfg->sktch_config.cols                     = 1;
   txn_splinterdb_cfg->iceberght_config.enable_lazy_eviction = TRUE;
#elif EXPERIMENTAL_MODE_STO_SKETCH
   txn_splinterdb_cfg->sktch_config.rows = 2;
   txn_splinterdb_cfg->sktch_config.cols = 1024; // 131072;
#elif EXPERIMENTAL_MODE_STO_SKETCH_LAZY
   txn_splinterdb_cfg->iceberght_config.max_num_keys += 410;
   txn_splinterdb_cfg->sktch_config.rows                     = 2;
   txn_splinterdb_cfg->sktch_config.cols                     = 512; // 131072;
   txn_splinterdb_cfg->iceberght_config.enable_lazy_eviction = TRUE;
#else
#   error "Invalid experimental mode"
#endif
   txn_splinterdb_cfg->iceberght_config.log_slots = (int)ceil(
      log2(5 * (double)txn_splinterdb_cfg->iceberght_config.max_num_keys));
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
      platform_free(0, txn_splinterdb_cfg);
      return rc;
   }

   iceberg_table *tscache;
   tscache = TYPED_ZALLOC(0, tscache);
   platform_assert(
      iceberg_init_with_sketch(tscache,
                               &txn_splinterdb_cfg->iceberght_config,
                               kvsb_cfg->data_cfg,
                               &txn_splinterdb_cfg->sktch_config)
      == 0);

   _txn_kvsb->tscache = tscache;

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

   iceberg_print_state(_txn_kvsb->tscache);

   splinterdb_close(&_txn_kvsb->kvsb);

   platform_free(0, _txn_kvsb->tscache);
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
      rw_entry_iceberg_remove(txn_kvsb, txn->rw_entries[i]);
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
         rw_entry_unlock(w, txn->ts);
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
         rw_entry_unlock(w, txn->ts);
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
   /*    rw_entry_iceberg_insert(txn_kvsb, entry); */
   /*    timestamp_set v = *entry->tuple_ts; */
   /*    entry->wts      = v.wts; */
   /*    entry->rts      = timestamp_set_get_rts(&v); */
   /* } */

   if (!rw_entry_is_write(entry)) {
      rw_entry_iceberg_insert(txn_kvsb, entry);
      if (rw_entry_write_lock(entry, txn->ts) == STO_ACCESS_ABORT) {
         transactional_splinterdb_abort(txn_kvsb, txn);
         return 1;
      }
      // To prevent deadlocks, we have to update the wts
      entry->ts->wts = txn->ts;
      // platform_default_log("Locked key for write = %s, %lu\n",
      // (char*)entry->key.data, txn->ts);
      rw_entry_set_msg(entry, msg);
   } else {
      // TODO it needs to be checked later for upsert
      key       wkey = key_create_from_slice(entry->key);
      const key ukey = key_create_from_slice(user_key);
      if (data_key_compare(cfg, wkey, ukey) == 0) {
         if (message_is_definitive(msg)) {
            void *ptr = (void *)message_data(entry->msg);
            platform_free(0, ptr);
            rw_entry_set_msg(entry, msg);
         } else {
            platform_assert(message_class(entry->msg) != MESSAGE_TYPE_DELETE);
            merge_accumulator new_message;
            merge_accumulator_init_from_message(&new_message, 0, msg);
            data_merge_tuples(cfg, ukey, entry->msg, &new_message);
            void *ptr = (void *)message_data(entry->msg);
            platform_free(0, ptr);
            entry->msg = merge_accumulator_to_message(&new_message);
         }
      }
   }
   return 0;
}

int
transactional_splinterdb_insert(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                slice                     value)
{
   if (!txn) {
      return splinterdb_insert(txn_kvsb->kvsb, user_key, value);
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
   message_type msg_type = txn_kvsb->tcfg->is_upsert_disabled
                              ? MESSAGE_TYPE_INSERT
                              : MESSAGE_TYPE_UPDATE;
   return local_write(txn_kvsb, txn, user_key, message_create(msg_type, delta));
}

int
transactional_splinterdb_lookup(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                splinterdb_lookup_result *result)
{
   const data_config *cfg   = txn_kvsb->tcfg->kvsb_cfg.data_cfg;
   rw_entry          *entry = rw_entry_get(txn_kvsb, txn, user_key, cfg, TRUE);

   int rc = 0;

   // TODO: do not insert if already inserted for this transaction
   rw_entry_iceberg_insert(txn_kvsb, entry);

#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 0
   if (rw_entry_is_write(entry)) {
      // read my write
      // TODO This works for simple insert/update. However, it doesn't work
      // for upsert.
      // TODO if it succeeded, this read should not be considered for
      // validation. entry->is_read should be false.
      _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
      merge_accumulator_resize(&_result->value, message_length(entry->msg));
      memcpy(merge_accumulator_data(&_result->value),
             message_data(entry->msg),
             message_length(entry->msg));
   } else {
      if (rw_entry_read_lock(entry, txn->ts) == STO_ACCESS_ABORT) {
         transactional_splinterdb_abort(txn_kvsb, txn);
         return 1;
      }
      // platform_default_log("Locked key for read = %s, %lu\n",
      // (char*)entry->key.data, txn->ts);
      rc = splinterdb_lookup(txn_kvsb->kvsb, entry->key, result);
      if (txn->ts > entry->ts->rts) {
         entry->ts->rts = txn->ts;
      }
      rw_entry_unlock(entry, txn->ts);
   }
#endif
   return rc;
}
