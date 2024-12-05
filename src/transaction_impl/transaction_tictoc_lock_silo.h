#pragma once

#include "data_internal.h"
#include "splinterdb/data.h"
#include "platform.h"
#include "data_internal.h"
#include "splinterdb/transaction.h"
#include "util.h"
#include "experimental_mode.h"
#include "splinterdb_internal.h"
#include "FPSketch/iceberg_table.h"
#include "lock_table.h"
#include "poison.h"

typedef struct transactional_splinterdb_config {
   splinterdb_config           kvsb_cfg;
   transaction_isolation_level isol_level;
   iceberg_config              iceberght_config;
   bool                        is_upsert_disabled;
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   lock_table                      *lock_tbl;
   iceberg_table                   *tscache;
} transactional_splinterdb;

// This causes a lot of delta overflow with tictoc
/* typedef struct timestamp_set { */
/*    txn_timestamp delta : 15; // rts = wts + delta */
/*    txn_timestamp wts : 49; */
/* } timestamp_set __attribute__((aligned(sizeof(txn_timestamp)))); */


// TODO we don't need to use delta, use rts
typedef struct {
   txn_timestamp delta : 64;
   txn_timestamp wts : 64;
} timestamp_set __attribute__((aligned(sizeof(txn_timestamp))));

// read_set and write_set entry stored locally
typedef struct rw_entry {
   slice            key;
   message          msg; // value + op
   txn_timestamp    wts;
   txn_timestamp    rts;
   timestamp_set   *tuple_ts;
   char             is_read;
   lock_table_entry lock;
} rw_entry;

static inline txn_timestamp
timestamp_set_get_rts(timestamp_set *ts)
{
#if EXPERIMENTAL_MODE_SILO == 1
   return ts->wts;
#else
   return ts->wts + ts->delta;
#endif
}

static inline void
timestamp_set_load(timestamp_set *tuple_ts, timestamp_set *v)
{
   __atomic_load(
      (txn_timestamp *)tuple_ts, (txn_timestamp *)v, __ATOMIC_RELAXED);
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
   if (entry->tuple_ts) {
      return FALSE;
   }

   // ValueType value_ht = {0};
   // bool is_new_item = iceberg_insert_without_increasing_refcount(
   //    txn_kvsb->tscache, key_ht, value_ht, platform_get_tid());

   timestamp_set ts = {0};
   entry->tuple_ts  = &ts;
   return iceberg_insert_and_get_without_increasing_refcount(
      txn_kvsb->tscache,
      &entry->key,
      (ValueType **)&entry->tuple_ts,
      platform_get_tid());
}

static inline void
rw_entry_iceberg_remove(transactional_splinterdb *txn_kvsb, rw_entry *entry)
{
   if (!entry->tuple_ts) {
      return;
   }

   entry->tuple_ts = NULL;

   //    KeyType   key_ht   = (KeyType)slice_data(entry->key);
   //    ValueType value_ht = {0};
   //    if (iceberg_get_and_remove(
   //           txn_kvsb->tscache, &key_ht, &value_ht, platform_get_tid()))
   //    {
   //       if (slice_data(entry->key) != key_ht) {
   //          platform_free(0, key_ht);
   //       } else {
   //          entry->need_to_keep_key = 0;
   //       }
   //    }
}

static rw_entry *
rw_entry_create()
{
   rw_entry *new_entry;
   new_entry = TYPED_ZALLOC(0, new_entry);
   platform_assert(new_entry != NULL);
   new_entry->tuple_ts = NULL;
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
      entry = rw_entry_create();
      // The entry->key will be replaced by the key from the hash table.
      entry->key                             = user_key;
      txn->rw_entries[txn->num_rw_entries++] = entry;
   }

   entry->is_read = entry->is_read || is_read;
   return entry;
}

static int
rw_entry_key_compare(const void *elem1, const void *elem2, void *args)
{
   const data_config *cfg = (const data_config *)args;

   rw_entry *e1 = *((rw_entry **)elem1);
   rw_entry *e2 = *((rw_entry **)elem2);

   key akey = key_create_from_slice(e1->key);
   key bkey = key_create_from_slice(e2->key);

   return data_key_compare(cfg, akey, bkey);
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
   txn_splinterdb_cfg->iceberght_config.log_slots = 29;

   // TODO things like filename, logfile, or data_cfg would need a
   // deep-copy
   txn_splinterdb_cfg->isol_level = TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE;
   txn_splinterdb_cfg->is_upsert_disabled = FALSE;
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

   _txn_kvsb->lock_tbl = lock_table_create(kvsb_cfg->data_cfg);

   iceberg_table *tscache;
   tscache = TYPED_ZALLOC(0, tscache);
   platform_assert(iceberg_init(tscache,
                                &txn_splinterdb_cfg->iceberght_config,
                                kvsb_cfg->data_cfg)
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

   lock_table_destroy(_txn_kvsb->lock_tbl);
   /* iceberg_print_state(_txn_kvsb->tscache); */
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
   // platform_error_log("[%lu] begin\n", ((unsigned long)txn) % 100);

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
   txn_timestamp commit_ts = 0;

   int       num_reads                    = 0;
   int       num_writes                   = 0;
   rw_entry *read_set[RW_SET_SIZE_LIMIT]  = {0};
   rw_entry *write_set[RW_SET_SIZE_LIMIT] = {0};

   for (int i = 0; i < txn->num_rw_entries; i++) {
      rw_entry *entry = txn->rw_entries[i];
      if (rw_entry_is_write(entry)) {
         write_set[num_writes++] = entry;
      }

      if (rw_entry_is_read(entry)) {
         read_set[num_reads++] = entry;

         txn_timestamp wts = entry->wts;
#if EXPERIMENTAL_MODE_SILO == 1
         wts += 1;
#endif
         commit_ts = MAX(commit_ts, wts);
      }
   }

   platform_sort_slow(write_set,
                      num_writes,
                      sizeof(rw_entry *),
                      rw_entry_key_compare,
                      (void *)txn_kvsb->tcfg->kvsb_cfg.data_cfg,
                      NULL);

RETRY_LOCK_WRITE_SET:
{
   for (int lock_num = 0; lock_num < num_writes; ++lock_num) {
      lock_table_rc lock_rc = lock_table_try_acquire_entry_lock(
         txn_kvsb->lock_tbl, &write_set[lock_num]->lock);
      platform_assert(lock_rc != LOCK_TABLE_RC_DEADLK);
      if (lock_rc == LOCK_TABLE_RC_BUSY) {
         for (int i = 0; i < lock_num; ++i) {
            lock_table_release_entry_lock(txn_kvsb->lock_tbl,
                                          &write_set[i]->lock);
         }

         // 1us is the value that is mentioned in the paper
         platform_sleep_ns(1000);

         goto RETRY_LOCK_WRITE_SET;
      }
   }
}

   for (uint64 i = 0; i < num_writes; ++i) {
      rw_entry *w = write_set[i];
      if (!w->tuple_ts) {
         rw_entry_iceberg_insert(txn_kvsb, w);
      }
      commit_ts = MAX(commit_ts, timestamp_set_get_rts(w->tuple_ts) + 1);
   }

   bool is_abort = FALSE;
   for (uint64 i = 0; i < num_reads; ++i) {
      rw_entry *r = read_set[i];
      platform_assert(rw_entry_is_read(r));

      bool is_read_entry_invalid = r->rts < commit_ts;

#if EXPERIMENTAL_MODE_SILO == 1
      is_read_entry_invalid = true;
#endif
      // platform_error_log("[%lu] key %s r->rts %lu commit_ts %lu\n",
      //                    ((unsigned long)txn) % 100,
      //                    (char *)slice_data(r->key),
      //                    r->rts,
      //                    commit_ts);

      if (is_read_entry_invalid) {
         lock_table_rc lock_rc =
            lock_table_try_acquire_entry_lock(txn_kvsb->lock_tbl, &r->lock);

         // platform_error_log("[%lu] key %s rts %lu commit_ts %lu lock_rc == "
         //                    "LOCK_TABLE_RC_BUSY %d\n",
         //                    ((unsigned long)txn) % 100,
         //                    (char *)slice_data(r->key),
         //                    rts,
         //                    commit_ts,
         //                    (lock_rc == LOCK_TABLE_RC_BUSY));

         if (lock_rc == LOCK_TABLE_RC_BUSY) {
            if (timestamp_set_get_rts(r->tuple_ts) <= commit_ts) {
               is_abort = TRUE;
               break;
            }
         }

         // platform_error_log("[%lu] key %s wts %lu r->wts %lu\n",
         //                    ((unsigned long)txn) % 100,
         //                    (char *)slice_data(r->key),
         //                    wts,
         //                    r->wts);

         if (r->tuple_ts->wts != r->wts) {
            if (lock_rc == LOCK_TABLE_RC_OK) {
               lock_table_release_entry_lock(txn_kvsb->lock_tbl, &r->lock);
            }
            is_abort = TRUE;
            break;
         }

#if !EXPERIMENTAL_MODE_SILO
         if (timestamp_set_get_rts(r->tuple_ts) < commit_ts) {
            platform_assert(commit_ts > r->tuple_ts->wts);
            // Handle delta overflow
            timestamp_set v     = *r->tuple_ts;
            txn_timestamp delta = commit_ts - v.wts;
            /* txn_timestamp shift = delta - (delta & 0x7fff); */
            txn_timestamp shift = delta - (delta & UINT64_MAX);
            v.wts += shift;
            v.delta      = delta - shift;
            *r->tuple_ts = v;
         }
#endif
         if (lock_rc == LOCK_TABLE_RC_OK) {
            lock_table_release_entry_lock(txn_kvsb->lock_tbl, &r->lock);
         }
      }
   }

   if (!is_abort) {
      int rc = 0;

      for (uint64 i = 0; i < num_writes; ++i) {
         rw_entry *w = write_set[i];
         platform_assert(rw_entry_is_write(w));
#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
         platform_sleep_ns(100);

         if (0) {
#endif
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

         w->tuple_ts->wts   = commit_ts;
         w->tuple_ts->delta = 0;

         lock_table_release_entry_lock(txn_kvsb->lock_tbl, &w->lock);
      }
   } else {
      // Transaction abort
      for (int i = 0; i < num_writes; ++i) {
         lock_table_release_entry_lock(txn_kvsb->lock_tbl, &write_set[i]->lock);
      }
   }

   transaction_deinit(txn_kvsb, txn);

   return (-1 * is_abort);
}

int
transactional_splinterdb_abort(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
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
   const key          ukey  = key_create_from_slice(user_key);
   rw_entry          *entry = rw_entry_get(txn_kvsb, txn, user_key, cfg, FALSE);
   /* if (message_class(msg) == MESSAGE_TYPE_UPDATE */
   /*     || message_class(msg) == MESSAGE_TYPE_DELETE) */
   /* { */
   /*    rw_entry_iceberg_insert(txn_kvsb, entry); */
   /*    timestamp_set v = *entry->tuple_ts; */
   /*    entry->wts      = v.wts; */
   /*    entry->rts      = timestamp_set_get_rts(&v); */
   /* } */

   if (message_is_null(entry->msg)) {
      rw_entry_set_msg(entry, msg);
   } else {
      // TODO it needs to be checked later for upsert
      key wkey = key_create_from_slice(entry->key);
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
   // platform_error_log("[%lu] lookup %s\n",
   //                    ((unsigned long)txn) % 100,
   //                    (char *)slice_data(user_key));

   const data_config *cfg   = txn_kvsb->tcfg->kvsb_cfg.data_cfg;
   rw_entry          *entry = rw_entry_get(txn_kvsb, txn, user_key, cfg, TRUE);

   int rc = 0;

   rw_entry_iceberg_insert(txn_kvsb, entry);

   timestamp_set v1, v2;
   do {
      timestamp_set_load(entry->tuple_ts, &v1);

#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
      platform_sleep_ns(100);
#else
      if (rw_entry_is_write(entry)) {
         // read my write
         // TODO This works for simple insert/update. However, it doesn't work
         // for upsert.
         // TODO if it succeeded, this read should not be considered for
         // validation. entry->is_read should be false.
         _splinterdb_lookup_result *_result =
            (_splinterdb_lookup_result *)result;
         merge_accumulator_resize(&_result->value, message_length(entry->msg));
         memcpy(merge_accumulator_data(&_result->value),
                message_data(entry->msg),
                message_length(entry->msg));
      } else {
         rc = splinterdb_lookup(txn_kvsb->kvsb, entry->key, result);
      }
#endif

      timestamp_set_load(entry->tuple_ts, &v2);
   } while (memcmp(&v1, &v2, sizeof(v1)) != 0
            || lock_table_get_entry_lock_state(txn_kvsb->lock_tbl, &entry->lock)
                  == LOCK_TABLE_RC_BUSY);

   entry->wts = v1.wts;
   entry->rts = timestamp_set_get_rts(&v1);

   return rc;
}
