#include "data_internal.h"
#include "splinterdb/data.h"
#include "transaction_internal.h"
#include "poison.h"

#if EXPERIMENTAL_MODE_TICTOC_DISK
timestamp_set ZERO_TIMESTAMP_SET = {0};
#else
timestamp_set ZERO_TIMESTAMP_SET = {.refcount = 1, .wts = 0, .delta = 0};
#endif

#if EXPERIMENTAL_MODE_TICTOC_DISK == 0
/*
 * Returns the the value is inserted or not
 */
static inline bool
rw_entry_increase_refcount(transactional_splinterdb *txn_kvsb, rw_entry *entry)
{
   // Make sure increasing the refcount only once
   if (entry->need_to_decrease_refcount) {
      return FALSE;
   }

   entry->need_to_decrease_refcount = TRUE;

   KeyType    key_ht   = (KeyType)slice_data(entry->key);
   ValueType *value_ht = (ValueType *)&ZERO_TIMESTAMP_SET;
   if (iceberg_insert(txn_kvsb->tscache, key_ht, *value_ht, platform_get_tid()))
   {
      entry->need_to_keep_key = TRUE;
      return TRUE;
   }

   return FALSE;
}

static inline void
rw_entry_decrease_refcount(transactional_splinterdb *txn_kvsb, rw_entry *entry)
{
   if (!entry->need_to_decrease_refcount) {
      return;
   }

   entry->need_to_decrease_refcount = FALSE;

   KeyType key_ht = (KeyType)slice_data(entry->key);
#   if EXPERIMENTAL_MODE_KEEP_ALL_KEYS
   iceberg_decrease_refcount(txn_kvsb->tscache, key_ht, platform_get_tid());
#   else
   ValueType value_ht = {0};
   if (iceberg_get_and_remove(
          txn_kvsb->tscache, &key_ht, &value_ht, platform_get_tid())) {
      if (slice_data(entry->key) != key_ht) {
         platform_free_from_heap(0, key_ht);
      } else {
         entry->need_to_keep_key = FALSE;
      }
      
#   if EXPERIMENTAL_MODE_SKETCH
      timestamp_set *ts = (timestamp_set *)&value_ht;
      count_min_sketch_max_insert(
         txn_kvsb->sket, entry->key, timestamp_set_get_rts(ts));
#   endif
   }
#   endif
}
#endif

/*
 * This function will update only rts for EXPERIMENTAL_MODE_TICTOC_DISK
 */
static inline void
update_global_timestamps(transactional_splinterdb *txn_kvsb,
                         rw_entry                 *entry,
                         txn_timestamp             wts,
                         txn_timestamp             rts)
{
#if EXPERIMENTAL_MODE_TICTOC_DISK
   platform_assert(sizeof(rts) == sizeof(uint32));
   splinterdb_update(
      txn_kvsb->kvsb, entry->key, slice_create(sizeof(rts), &rts));
#else
   KeyType       key_ht     = (KeyType)slice_data(entry->key);
   timestamp_set new_ts_set = {
      .refcount = 1, .wts = wts, .delta = timestamp_set_get_delta(wts, rts)};
   ValueType *new_value_ht = (ValueType *)&new_ts_set;
   platform_assert(iceberg_update(
      txn_kvsb->tscache, key_ht, *new_value_ht, platform_get_tid()));
#endif
}

static bool
get_global_timestamps(transactional_splinterdb *txn_kvsb,
                      rw_entry                 *entry,
                      txn_timestamp            *wts,
                      txn_timestamp            *rts)
{
#if EXPERIMENTAL_MODE_TICTOC_DISK
   const splinterdb *kvsb  = txn_kvsb->kvsb;
   bool              found = FALSE;

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(kvsb, &result, 0, NULL);

   splinterdb_lookup(kvsb, entry->key, &result);

   slice value;

   if (splinterdb_lookup_found(&result)) {
      splinterdb_lookup_result_value(&result, &value);
      timestamp_set ts_set;
      memcpy(&ts_set, slice_data(value), sizeof(ts_set));
      if (wts) {
         *wts = ts_set.wts;
      }
      if (rts) {
         *rts = ts_set.rts;
      }
      found = TRUE;
   }
   splinterdb_lookup_result_deinit(&result);

   return found;
#else
   KeyType    key_ht   = (KeyType)slice_data(entry->key);
   ValueType *value_ht = NULL;

   if (!rw_entry_increase_refcount(txn_kvsb, entry)) {
      if (iceberg_get_value(
            txn_kvsb->tscache, key_ht, &value_ht, platform_get_tid()))
      {
         timestamp_set *ts_set = (timestamp_set *)value_ht;
         if (wts) {
            *wts = ts_set->wts;
         }
         if (rts) {
            *rts = timestamp_set_get_rts(ts_set);
         }
         // platform_error_log("Key %s get rts from cache: %lu\n", key_ht, *rts);
         return TRUE;
      }
   }

#   if EXPERIMENTAL_MODE_SKETCH
   if (rts) {
      *rts = count_min_sketch_query(txn_kvsb->sket, entry->key);
      // platform_error_log("Key %s get rts from sketch: %lu\n", key_ht, *rts);
      update_global_timestamps(txn_kvsb, entry, 0, *rts);
   }
#   endif

   return FALSE;
#endif
}


static rw_entry *
rw_entry_create()
{
   rw_entry *new_entry;
   new_entry = TYPED_ZALLOC(0, new_entry);
   platform_assert(new_entry != NULL);

   new_entry->owner = platform_get_tid();
   return new_entry;
}

static inline void
rw_entry_deinit(rw_entry *entry)
{
   bool can_key_free = !slice_is_null(entry->key) && !entry->need_to_keep_key;
   if (can_key_free) {
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

/*
 * The msg is the msg from app.
 * In EXPERIMENTAL_MODE_TICTOC_DISK, this function adds timestamps at the begin
 * of the msg
 */
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

#if EXPERIMENTAL_MODE_TICTOC_DISK
   txn_splinterdb_cfg->txn_data_cfg =
      TYPED_ZALLOC(0, txn_splinterdb_cfg->txn_data_cfg);
   transactional_data_config_init(kvsb_cfg->data_cfg,
                                  txn_splinterdb_cfg->txn_data_cfg);
   txn_splinterdb_cfg->kvsb_cfg.data_cfg =
      (data_config *)txn_splinterdb_cfg->txn_data_cfg;
#else
   txn_splinterdb_cfg->tscache_log_slots = 28;
#endif

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
#if EXPERIMENTAL_MODE_TICTOC_DISK
      platform_free(0, txn_splinterdb_cfg->txn_data_cfg);
#endif
      platform_free(0, txn_splinterdb_cfg);
      return rc;
   }

   _txn_kvsb->lock_tbl = lock_table_create();

#if EXPERIMENTAL_MODE_TICTOC_DISK == 0
   iceberg_table *tscache;
   tscache = TYPED_ZALLOC(0, tscache);
   platform_assert(iceberg_init(tscache, txn_splinterdb_cfg->tscache_log_slots)
                   == 0);
   _txn_kvsb->tscache = tscache;
#endif

#if EXPERIMENTAL_MODE_SKETCH
   _txn_kvsb->sket = TYPED_ZALLOC(0, _txn_kvsb->sket);
   count_min_sketch_init(_txn_kvsb->sket, 10, 1000000);
#endif

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

   lock_table_destroy(_txn_kvsb->lock_tbl);

#if EXPERIMENTAL_MODE_SKETCH
   // count_min_sketch_print(_txn_kvsb->sket);
   count_min_sketch_deinit(_txn_kvsb->sket);
   platform_free(0, _txn_kvsb->sket);
#endif

#if EXPERIMENTAL_MODE_TICTOC_DISK
   platform_free(0, _txn_kvsb->tcfg->txn_data_cfg);
#else
   /* iceberg_print_state(_txn_kvsb->tscache); */
   platform_free(0, _txn_kvsb->tscache);
#endif
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
#if EXPERIMENTAL_MODE_TICTOC_DISK == 0
      rw_entry_decrease_refcount(txn_kvsb, txn->rw_entries[i]);
#endif
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
         /* txn->commit_rts = MAX(txn->commit_rts, wts); */
         commit_ts = MAX(commit_ts, wts);
      }
   }

   /* if (is_serializable(txn_kvsb->tcfg) || is_repeatable_read(txn_kvsb->tcfg))
    * { */
   /*    txn->commit_rts = txn->commit_wts = MAX(txn->commit_rts,
    * txn->commit_wts); */
   /* } */

   platform_sort_slow(write_set,
                      num_writes,
                      sizeof(rw_entry *),
                      rw_entry_key_compare,
                      (void *)txn_kvsb->tcfg->kvsb_cfg.data_cfg,
                      NULL);

RETRY_LOCK_WRITE_SET:
{
   int lock_num = 0;
   while (lock_num < num_writes) {
      lock_table_rc lock_rc = lock_table_try_acquire_entry_lock(
         txn_kvsb->lock_tbl, write_set[lock_num]);
      platform_assert(lock_rc != LOCK_TABLE_RC_DEADLK);
      if (lock_rc == LOCK_TABLE_RC_BUSY) {
         while (lock_num-- > 0) {
            lock_table_release_entry_lock(txn_kvsb->lock_tbl,
                                          write_set[lock_num]);
         }

         // 1us is the value that is mentioned in the paper
         platform_sleep_ns(1000);

         goto RETRY_LOCK_WRITE_SET;
      }

      ++lock_num;
   }
}

   for (uint64 i = 0; i < num_writes; ++i) {
      txn_timestamp rts = 0;
      get_global_timestamps(txn_kvsb, write_set[i], NULL, &rts);
      /* txn->commit_wts = MAX(txn->commit_wts, rts + 1); */
      commit_ts = MAX(commit_ts, rts + 1);
   }

   /* uint64 commit_ts = */
   /*    is_snapshot_isolation(txn_kvsb->tcfg) ? txn->commit_rts :
    * txn->commit_wts; */

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
            lock_table_try_acquire_entry_lock(txn_kvsb->lock_tbl, r);

         txn_timestamp wts = 0;
         txn_timestamp rts = 0;
         get_global_timestamps(txn_kvsb, r, &wts, &rts);

         // platform_error_log("[%lu] key %s wts %lu r->wts %lu\n",
         //                    ((unsigned long)txn) % 100,
         //                    (char *)slice_data(r->key),
         //                    wts,
         //                    r->wts);

         if (wts != r->wts) {
            if (lock_rc == LOCK_TABLE_RC_OK) {
               lock_table_release_entry_lock(txn_kvsb->lock_tbl, r);
            }
            is_abort = TRUE;
            break;
         }
         // platform_error_log("[%lu] key %s rts %lu commit_ts %lu lock_rc == "
         //                    "LOCK_TABLE_RC_BUSY %d\n",
         //                    ((unsigned long)txn) % 100,
         //                    (char *)slice_data(r->key),
         //                    rts,
         //                    commit_ts,
         //                    (lock_rc == LOCK_TABLE_RC_BUSY));


         if (rts <= commit_ts && lock_rc == LOCK_TABLE_RC_BUSY) {
            is_abort = TRUE;
            break;
         }

         platform_assert(wts <= commit_ts);

#if EXPERIMENTAL_MODE_SILO == 0
         if (rts < commit_ts) {
            update_global_timestamps(txn_kvsb, r, wts, commit_ts);
         }
#endif
         if (lock_rc == LOCK_TABLE_RC_OK) {
            lock_table_release_entry_lock(txn_kvsb->lock_tbl, r);
         }
      }
   }

   if (!is_abort) {
      int rc = 0;

      for (uint64 i = 0; i < num_writes; ++i) {
         rw_entry *w = write_set[i];
         platform_assert(rw_entry_is_write(w));

#if EXPERIMENTAL_MODE_TICTOC_DISK
         timestamp_set ts = {
            .wts = commit_ts,
            .rts = commit_ts,
         };
         tuple_header *msg = (tuple_header *)message_data(w->msg);
         memcpy(&msg->ts, &ts, sizeof(ts));
#else
         update_global_timestamps(txn_kvsb, w, commit_ts, commit_ts);
#endif

         //    lock_table_release_entry_lock(txn_kvsb->lock_tbl, w);
         // }

         // for (uint64 i = 0; i < num_writes; ++i) {
         //    rw_entry *w = write_set[i];
         //    platform_assert(rw_entry_is_write(w));

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

         lock_table_release_entry_lock(txn_kvsb->lock_tbl, w);
      }
   } else {
      for (uint64 i = 0; i < num_writes; ++i) {
         lock_table_release_entry_lock(txn_kvsb->lock_tbl, write_set[i]);
      }
   }

   // platform_error_log("[%lu] %s at commit_ts: %lu\n",
   //                    ((unsigned long)txn) % 100,
   //                    (is_abort ? "abort" : "commit"),
   //                    commit_ts);
   // for (uint64 i = 0; i < txn->num_rw_entries; ++i) {
      // char         *key = (char *)slice_data(txn->rw_entries[i]->key);
      // txn_timestamp rts = 0;
      // txn_timestamp wts = 0;
      // get_global_timestamps(txn_kvsb, txn->rw_entries[i], &rts, &wts);
      // platform_error_log("[%lu] key %s rts %lu wts %lu\n",
      //                    ((unsigned long)txn) % 100,
      //                    key,
      //                    rts,
      //                    wts);
   // }

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
   if (message_class(msg) == MESSAGE_TYPE_UPDATE
       || message_class(msg) == MESSAGE_TYPE_DELETE)
   {
      get_global_timestamps(txn_kvsb, entry, &entry->wts, &entry->rts);
   }

   if (message_is_null(entry->msg)) {
      rw_entry_set_msg(entry, msg);
   } else {
      key wkey = key_create_from_slice(entry->key);
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

int
transactional_splinterdb_insert(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                slice                     value)
{
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
   // platform_error_log("[%lu] update %s\n",
   //                    ((unsigned long)txn) % 100,
   //                    (char *)slice_data(user_key));

   return local_write(
      txn_kvsb, txn, user_key, message_create(MESSAGE_TYPE_UPDATE, delta));
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

#if EXPERIMENTAL_MODE_TICTOC_DISK
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;

   if (!message_is_null(entry->msg)) {
      get_global_timestamps(txn_kvsb, entry, &entry->wts, &entry->rts);

      tuple_header *tuple = (tuple_header *)message_data(entry->msg);
      const size_t  value_len =
         message_length(entry->msg) - sizeof(tuple_header);
      merge_accumulator_resize(&_result->value, value_len);
      memcpy(merge_accumulator_data(&_result->value),
             tuple->value,
             merge_accumulator_length(&_result->value));
      return 0;
   }

   int rc = splinterdb_lookup(txn_kvsb->kvsb, user_key, result);
   if (splinterdb_lookup_found(result)) {
      tuple_header *tuple =
         (tuple_header *)merge_accumulator_data(&_result->value);
      entry->wts = tuple->ts.wts;
      entry->rts = tuple->ts.rts;
      const size_t value_len =
         merge_accumulator_length(&_result->value) - sizeof(tuple_header);
      memmove(merge_accumulator_data(&_result->value), tuple->value, value_len);
      merge_accumulator_resize(&_result->value, value_len);
   }
   return rc;
#else
   get_global_timestamps(txn_kvsb, entry, &entry->wts, &entry->rts);

#   if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
   platform_sleep_ns(100);
   return 0;
#   endif

   if (!message_is_null(entry->msg)) {
      _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
      merge_accumulator_resize(&_result->value, message_length(entry->msg));
      memcpy(merge_accumulator_data(&_result->value),
             message_data(entry->msg),
             message_length(entry->msg));
      return 0;
   }

   return splinterdb_lookup(txn_kvsb->kvsb, user_key, result);
#endif
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
