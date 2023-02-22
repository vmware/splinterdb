#include "data_internal.h"
#include "splinterdb/data.h"
#include "transaction_internal.h"
#include "poison.h"

timestamp_set ZERO_TIMESTAMP_SET = {.refcount = 1, .wts = 0, .delta = 0};

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
   key_buf = TYPED_ARRAY_ZALLOC(0, key_buf, KEY_SIZE);
   memmove(key_buf, slice_data(key), slice_length(key));
   e->key = slice_create(KEY_SIZE, key_buf);
}

static inline void
rw_entry_set_msg(rw_entry *e, message msg)
{
   char *msg_buf;
   msg_buf = TYPED_ARRAY_ZALLOC(0, msg_buf, message_length(msg));
   memmove(msg_buf, message_data(msg), message_length(msg));
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

static inline void
rw_entry_increase_refcount(transactional_splinterdb *txn_kvsb, rw_entry *entry)
{
   // Each transaction can increase the refcount of the entry only one time
   if (entry->need_to_decrease_refcount) {
      return;
   }

   KeyType    key_ht   = (KeyType)slice_data(entry->key);
   ValueType *value_ht = (ValueType *)&ZERO_TIMESTAMP_SET;

   if (iceberg_insert(txn_kvsb->tscache, key_ht, *value_ht, platform_get_tid()))
   {
      entry->need_to_keep_key = TRUE;
   }
   entry->need_to_decrease_refcount = (EXPERIMENTAL_MODE_KEEP_ALL_KEYS == 0);
}

static inline void
rw_entry_decrease_refcount(transactional_splinterdb *txn_kvsb, rw_entry *entry)
{
   if (!entry->need_to_decrease_refcount) {
      return;
   }
   KeyType key_ht = (KeyType)slice_data(entry->key);
   if (iceberg_remove_and_get_key(
          txn_kvsb->tscache, &key_ht, platform_get_tid())) {
      if (slice_data(entry->key) != key_ht) {
         platform_free_from_heap(0, key_ht);
      } else {
         entry->need_to_keep_key = FALSE;
      }
   }
}

static inline void
rw_entry_set_timestamps(transactional_splinterdb *txn_kvsb, rw_entry *entry)
{
   KeyType    key_ht   = (KeyType)slice_data(entry->key);
   ValueType *value_ht = NULL;
   if (iceberg_get_value(
          txn_kvsb->tscache, key_ht, &value_ht, platform_get_tid())
       != TRUE)
   {
      value_ht = (ValueType *)&ZERO_TIMESTAMP_SET;
   }

   timestamp_set *ts_set = (timestamp_set *)value_ht;
   entry->wts            = ts_set->wts;
   entry->rts            = timestamp_set_get_rts(ts_set);
}

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

   bool need_to_increase_refcount = (EXPERIMENTAL_MODE_KEEP_ALL_KEYS == 1);
   if (!need_to_increase_refcount) {
      need_to_increase_refcount = is_read && !entry->need_to_decrease_refcount;
   }

   if (need_to_increase_refcount) {
      rw_entry_increase_refcount(txn_kvsb, entry);
   }

   entry->is_read = entry->is_read || is_read;
   if (is_read) {
      rw_entry_set_timestamps(txn_kvsb, entry);
   }
   return entry;
}

static int
rw_entry_key_compare(const void *elem1, const void *elem2, void *args)
{
   rw_entry         **a   = (rw_entry **)elem1;
   rw_entry         **b   = (rw_entry **)elem2;
   const data_config *cfg = (const data_config *)args;

   key akey = key_create_from_slice((*a)->key);
   key bkey = key_create_from_slice((*b)->key);

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
   // TODO things like filename, logfile, or data_cfg would need a
   // deep-copy
   txn_splinterdb_cfg->isol_level = TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE;
   txn_splinterdb_cfg->tscache_log_slots = 20;
}

static int
transactional_splinterdb_create_or_open(const splinterdb_config   *kvsb_cfg,
                                        transactional_splinterdb **txn_kvsb,
                                        bool open_existing)
{
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

   _txn_kvsb->lock_tbl = lock_table_create();

   iceberg_table *tscache;
   tscache = TYPED_ZALLOC(0, tscache);
   platform_assert(iceberg_init(tscache, txn_splinterdb_cfg->tscache_log_slots)
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
   splinterdb_close(&_txn_kvsb->kvsb);

   lock_table_destroy(_txn_kvsb->lock_tbl);

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
   return 0;
}

static inline void
transaction_deinit(transaction *txn)
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
   txn->commit_wts = 0;
   txn->commit_rts = 0;

   int       num_reads                    = 0;
   int       num_writes                   = 0;
   rw_entry *read_set[RW_SET_SIZE_LIMIT]  = {0};
   rw_entry *write_set[RW_SET_SIZE_LIMIT] = {0};

   for (int i = 0; i < txn->num_rw_entries; i++) {
      rw_entry *entry = txn->rw_entries[i];
      if (rw_entry_is_read(entry)) {
         read_set[num_reads++] = entry;

         timestamp wts = entry->wts;
#if EXPERIMENTAL_MODE_SILO == 1
         wts += 1;
#endif
         txn->commit_rts = MAX(txn->commit_rts, wts);
      }

      if (rw_entry_is_write(entry)) {
         write_set[num_writes++] = entry;
         txn->commit_wts         = MAX(txn->commit_wts, entry->rts + 1);
      }
   }

   if (is_serializable(txn_kvsb->tcfg) || is_repeatable_read(txn_kvsb->tcfg)) {
      txn->commit_rts = txn->commit_wts = MAX(txn->commit_rts, txn->commit_wts);
   }

   platform_sort_slow(write_set,
                      num_writes,
                      sizeof(rw_entry *),
                      rw_entry_key_compare,
                      (void *)txn_kvsb->tcfg->kvsb_cfg.data_cfg,
                      NULL);

RETRY_LOCK_WRITE_SET:
{
   int locked_cnt = 0;
   while (locked_cnt < num_writes) {
      lock_table_rc lock_rc = lock_table_try_acquire_entry_lock(
         txn_kvsb->lock_tbl, write_set[locked_cnt]);
      platform_assert(lock_rc != LOCK_TABLE_RC_DEADLK);
      if (lock_rc == LOCK_TABLE_RC_BUSY) {
         while (locked_cnt-- > 0) {
            lock_table_release_entry_lock(txn_kvsb->lock_tbl,
                                          write_set[locked_cnt]);
         }

         // 1us is the value that is mentioned in the paper
         platform_sleep_ns(1000);

         goto RETRY_LOCK_WRITE_SET;
      }

      ++locked_cnt;
   }
}

   for (uint64 i = 0; i < num_writes; ++i) {
      rw_entry  *w                   = write_set[i];
      KeyType    key_ht              = (KeyType)slice_data(w->key);
      ValueType *value_ht            = NULL;
      const bool key_exists_in_cache = iceberg_get_value(
         txn_kvsb->tscache, key_ht, &value_ht, platform_get_tid());
      if (key_exists_in_cache) {
         timestamp_set *ts_set = (timestamp_set *)value_ht;
         txn->commit_wts =
            MAX(txn->commit_wts, timestamp_set_get_rts(ts_set) + 1);
      } else {
         txn->commit_wts = MAX(txn->commit_wts, 1);
      }
   }

   uint64 commit_ts =
      is_snapshot_isolation(txn_kvsb->tcfg) ? txn->commit_rts : txn->commit_wts;

   bool is_abort = FALSE;
   for (uint64 i = 0; i < num_reads; ++i) {
      rw_entry *r = read_set[i];
      platform_assert(rw_entry_is_read(r));

      bool is_read_entry_invalid = r->rts < commit_ts;
      // platform_error_log("r->rts = %lu, commit_ts = %lu\n", r->rts,
      // commit_ts);

#if EXPERIMENTAL_MODE_SILO == 1
      is_read_entry_invalid = true;
#endif

      if (is_read_entry_invalid) {
         KeyType    key_ht   = (KeyType)slice_data(r->key);
         ValueType *value_ht = NULL;
         platform_assert(iceberg_get_value(
            txn_kvsb->tscache, key_ht, &value_ht, platform_get_tid()));

         timestamp_set *ts_set = (timestamp_set *)value_ht;

         if (ts_set->wts != r->wts) {
            is_abort = TRUE;
            break;
         }

         lock_table_rc lock_rc =
            lock_table_try_acquire_entry_lock(txn_kvsb->lock_tbl, r);
         // platform_error_log("lock_rc = %d", lock_rc);

         if (lock_rc == LOCK_TABLE_RC_BUSY) {
            is_abort = TRUE;
            break;
         }

         if (lock_rc != LOCK_TABLE_RC_DEADLK) {
            platform_assert(iceberg_get_value(
               txn_kvsb->tscache, key_ht, &value_ht, platform_get_tid()));
            ts_set = (timestamp_set *)value_ht;
         }

         if (ts_set->wts != r->wts) {
            if (lock_rc == LOCK_TABLE_RC_OK) {
               lock_table_release_entry_lock(txn_kvsb->lock_tbl, r);
            }
            is_abort = TRUE;
            break;
         }

#if EXPERIMENTAL_MODE_SILO == 0
         const uint32 new_rts = MAX(commit_ts, timestamp_set_get_rts(ts_set));
         const bool   need_to_update_rts =
            (new_rts != timestamp_set_get_rts(ts_set))
            && !is_repeatable_read(txn_kvsb->tcfg);
         if (need_to_update_rts) {
            timestamp_set new_ts_set = {
               .refcount = 0,
               .wts      = ts_set->wts,
               .delta    = timestamp_set_get_delta(ts_set->wts, new_rts)};
            ValueType *new_value_ht = (ValueType *)&new_ts_set;
            platform_assert(iceberg_update(
               txn_kvsb->tscache, key_ht, *new_value_ht, platform_get_tid()));
         }
#endif

         if (lock_rc == LOCK_TABLE_RC_OK) {
            lock_table_release_entry_lock(txn_kvsb->lock_tbl, r);
         }
      }
   }

   if (!is_abort) {
      for (uint64 i = 0; i < num_writes; ++i) {
         rw_entry *w = write_set[i];
         platform_assert(rw_entry_is_write(w));

         int rc = 0;

#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
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
         // platform_error_log("wts: %ld\n", txn->commit_wts);
         // platform_error_log("rts: %ld\n", txn->commit_rts);
         timestamp_set ts_set = {.wts = txn->commit_wts, .delta = 0};
         // platform_error_log("delta: %d\n", ts_set.delta);

         KeyType    key_ht   = (KeyType)slice_data(w->key);
         ValueType *value_ht = (ValueType *)&ts_set;

         iceberg_update(
            txn_kvsb->tscache, key_ht, *value_ht, platform_get_tid());
      }
   }

   for (uint64 i = 0; i < num_writes; ++i) {
      lock_table_release_entry_lock(txn_kvsb->lock_tbl, write_set[i]);
   }

   for (int i = 0; i < txn->num_rw_entries; ++i) {
      rw_entry_decrease_refcount(txn_kvsb, txn->rw_entries[i]);
   }

   transaction_deinit(txn);

   return (-1 * is_abort);
}

int
transactional_splinterdb_abort(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   transaction_deinit(txn);

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

#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
   return 0;
#endif

   if (!message_is_null(entry->msg)) {
      _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
      merge_accumulator_resize(&_result->value, message_length(entry->msg));
      memcpy(merge_accumulator_data(&_result->value),
             message_data(entry->msg),
             message_length(entry->msg));
      return 0;
   }

   return splinterdb_lookup(txn_kvsb->kvsb, user_key, result);
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
