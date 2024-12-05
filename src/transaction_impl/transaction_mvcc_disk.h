#include "splinterdb/data.h"
#include "platform.h"
#include "data_internal.h"
#include "splinterdb/transaction.h"
#include "util.h"
#include "experimental_mode.h"
#include "splinterdb_internal.h"
#include "transaction_stats.h"
#include "FPSketch/iceberg_table.h"
#include "poison.h"

typedef struct transactional_data_config {
   data_config        super;
   const data_config *application_data_config;
} transactional_data_config;

typedef struct transactional_splinterdb_config {
   splinterdb_config           kvsb_cfg;
   transaction_isolation_level isol_level;
   transactional_data_config  *txn_data_cfg;
   iceberg_config              lock_tbl_cfg;
   bool                        is_upsert_disabled;
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   iceberg_table                   *lock_tbl;

#if USE_TRANSACTION_STATS
   // For experimental purpose
   transaction_stats txn_stats;
#endif
} transactional_splinterdb;

// MVCC Key and Value definitions and compare and merge functions for
// them.

#define MVCC_VERSION_NODATA 0
#define MVCC_VERSION_START  1
#define MVCC_VERSION_INF    UINT32_MAX
#define MVCC_TIMESTAMP_INF  ((txn_timestamp)-1)

typedef struct ONDISK mvcc_key_header {
   uint32 version;
} mvcc_key_header;

typedef struct ONDISK mvcc_key {
   mvcc_key_header header;
   char            key[];
} mvcc_key;

static char *
mvcc_key_get_user_key_from_slice(slice s)
{
   return ((mvcc_key *)slice_data(s))->key;
}

static uint64
mvcc_key_get_user_key_length_from_slice(slice s)
{
   return slice_length(s) - sizeof(mvcc_key_header);
}

static uint32
mvcc_key_get_version_from_slice(slice s)
{
   return ((mvcc_key *)slice_data(s))->header.version;
}

static key
mvcc_user_key(slice mk)
{
   return key_create(mvcc_key_get_user_key_length_from_slice(mk),
                     mvcc_key_get_user_key_from_slice(mk));
}


static int
mvcc_key_compare(const data_config *cfg, slice key1, slice key2)
{
   platform_assert(slice_length(key1) >= sizeof(mvcc_key_header));
   platform_assert(slice_length(key2) >= sizeof(mvcc_key_header));

   // user_keys are increasingly ordered, but versions are ordered in decreasing
   // order.
   int ret = data_key_compare(
      ((const transactional_data_config *)cfg)->application_data_config,
      mvcc_user_key(key1),
      mvcc_user_key(key2));

   if (ret != 0) {
      return ret;
   }

   if (mvcc_key_get_version_from_slice(key1)
       < mvcc_key_get_version_from_slice(key2))
   {
      return 1;
   } else if (mvcc_key_get_version_from_slice(key1)
              > mvcc_key_get_version_from_slice(key2))
   {
      return -1;
   } else {
      return 0;
   }
}

typedef struct ONDISK mvcc_timestamp_update {
   txn_timestamp rts;
   txn_timestamp wts_max;
} mvcc_timestamp_update;


typedef struct ONDISK mvcc_value_header {
   mvcc_timestamp_update update; // Must be first field
   txn_timestamp         wts_min;
} mvcc_value_header;

typedef struct ONDISK mvcc_value {
   mvcc_value_header header;
   char              value[];
} mvcc_value;

static inline message
get_app_value_from_message(message msg)
{
   return message_create(
      message_class(msg),
      slice_create(message_length(msg) - sizeof(mvcc_value_header),
                   message_data(msg) + sizeof(mvcc_value_header)));
}

static inline message
get_app_value_from_merge_accumulator(merge_accumulator *ma)
{
   return message_create(
      merge_accumulator_message_class(ma),
      slice_create(merge_accumulator_length(ma) - sizeof(mvcc_value_header),
                   merge_accumulator_data(ma) + sizeof(mvcc_value_header)));
}

static int
merge_mvcc_tuple(const data_config *cfg,
                 slice              key,         // IN
                 message            old_message, // IN
                 merge_accumulator *new_message) // IN/OUT
{
   mvcc_timestamp_update update;

   // The only thing we use updates for is to update timestamps.  All
   // application-level updates get converted into inserts.

   platform_assert(slice_length(key) >= sizeof(mvcc_key_header));
   platform_assert(message_length(old_message)
                   >= sizeof(mvcc_timestamp_update));

   platform_assert(merge_accumulator_message_class(new_message)
                   == MESSAGE_TYPE_UPDATE);
   platform_assert(merge_accumulator_length(new_message) == sizeof(update));

   memcpy(&update, merge_accumulator_data(new_message), sizeof(update));

   bool success = merge_accumulator_copy_message(new_message, old_message);
   platform_assert(success, "Failed to copy old_message\n");

   mvcc_timestamp_update *result_header =
      (mvcc_timestamp_update *)merge_accumulator_data(new_message);

   platform_assert(
      update.wts_max == MVCC_TIMESTAMP_INF
         || result_header->wts_max == MVCC_TIMESTAMP_INF,
      "Two updates to wts_max for the same key/version\n"
      "old wts_max: %u\n"
      "new wts_max: %u\n"
      "key: version: %u string: \"%s\" (%s)",
      result_header->wts_max,
      update.wts_max,
      (uint32)mvcc_key_get_version_from_slice(key),
      mvcc_key_get_user_key_from_slice(key),
      key_string(((transactional_data_config *)cfg)->application_data_config,
                 mvcc_user_key(key)));


   if (update.rts > result_header->rts) {
      result_header->rts = update.rts;
   }
   if (update.wts_max != MVCC_TIMESTAMP_INF) {
      result_header->wts_max = update.wts_max;
   }

   return 0;
}


static int
merge_mvcc_tuple_final(const data_config *cfg,
                       slice              key,
                       merge_accumulator *oldest_message)
{
   platform_assert(FALSE, "merge_mvcc_tuple_final should not be called\n");
   return 0;
}

static void
transactional_data_config_init(data_config               *in_cfg, // IN
                               transactional_data_config *out_cfg // OUT
)
{
   memcpy(&out_cfg->super, in_cfg, sizeof(out_cfg->super));
   out_cfg->super.key_compare        = mvcc_key_compare;
   out_cfg->super.merge_tuples       = merge_mvcc_tuple;
   out_cfg->super.merge_tuples_final = merge_mvcc_tuple_final;
   // Increase the max_key_size to accommodate the mvcc_key_header
   out_cfg->super.max_key_size += sizeof(mvcc_key_header);
   out_cfg->application_data_config = in_cfg;
}

// A global timestamp allocator

uint64 global_ts = 0;

static inline txn_timestamp
get_next_global_ts()
{
   return __atomic_add_fetch(&global_ts, 1, __ATOMIC_RELAXED);
}

// rw_entry is a read-write entry in the transaction.

typedef struct {
   uint64 locked;
   uint64 lock_holder;
} mvcc_lock __attribute__((aligned(8)));

static inline bool
mvcc_lock_compare_and_swap(mvcc_lock *target,
                           mvcc_lock *expected,
                           mvcc_lock *desired)
{
   return __atomic_compare_exchange((volatile ValueType *)target,
                                    (ValueType *)expected,
                                    (ValueType *)desired,
                                    TRUE,
                                    __ATOMIC_RELAXED,
                                    __ATOMIC_RELAXED);
}

static inline void
mvcc_lock_load(mvcc_lock *src, mvcc_lock *dst)
{
   __atomic_load((volatile ValueType *)src, (ValueType *)dst, __ATOMIC_RELAXED);
}

typedef struct rw_entry {
   slice         key;
   message       msg; // value + op
   txn_timestamp rts;
   txn_timestamp wts_min;
   txn_timestamp wts_max;
   mvcc_lock    *lock;
} rw_entry;

static void
rw_entry_load_timestamps_from_splinter(transactional_splinterdb *txn_kvsb,
                                       rw_entry                 *entry)
{
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(txn_kvsb->kvsb, &result, 0, NULL);
   int rc = splinterdb_lookup(txn_kvsb->kvsb, entry->key, &result);
   platform_assert(rc == 0);
   if (splinterdb_lookup_found(&result)) {
      _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)&result;
      mvcc_value_header         *header =
         (mvcc_value_header *)merge_accumulator_data(&_result->value);
      entry->rts     = header->update.rts;
      entry->wts_min = header->wts_min;
      entry->wts_max = header->update.wts_max;
   } else {
      entry->rts     = 0;
      entry->wts_min = 0;
      entry->wts_max = MVCC_TIMESTAMP_INF;
   }
   splinterdb_lookup_result_deinit(&result);
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
   // if (entry->lock) {
   //    return FALSE;
   // }
   platform_assert(entry->lock == NULL);

   // increase refcount for key
   mvcc_lock l         = {0};
   slice     entry_key = entry->key;
   entry->lock         = &l;
   return iceberg_insert_and_get(txn_kvsb->lock_tbl,
                                 &entry_key,
                                 (ValueType **)&entry->lock,
                                 platform_get_tid());
}

static inline void
rw_entry_iceberg_remove(transactional_splinterdb *txn_kvsb, rw_entry *entry)
{
   // if (!entry->lock) {
   //    return;
   // }
   platform_assert(entry->lock != NULL);

   entry->lock = NULL;

   iceberg_remove(txn_kvsb->lock_tbl, entry->key, platform_get_tid());
}


typedef enum {
   VERSION_LOCK_STATUS_INVALID = 0,
   VERSION_LOCK_STATUS_OK,
   VERSION_LOCK_STATUS_BUSY,
   VERSION_LOCK_STATUS_RETRY_VERSION,
   VERSION_LOCK_STATUS_ABORT
} version_lock_status;


static void
rw_entry_unlock(transactional_splinterdb *txn_kvsb, rw_entry *entry)
{
   platform_assert(entry->lock);
   mvcc_lock v1, v2 = {0};
   // platform_default_log("[%lu] unlock %s(version: %u)\n",
   //                      platform_get_tid(),
   //                      key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
   //                                 mvcc_user_key(entry->key))
   //                                 ,
   //                                 mvcc_key_get_version_from_slice(entry->key));
   mvcc_lock_load(entry->lock, &v1);
   platform_assert(
      mvcc_lock_compare_and_swap(entry->lock, &v1, &v2),
      "%lu (key: %s, version: %u)\n",
      v1.locked,
      key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
                 mvcc_user_key(entry->key)),
      mvcc_key_get_version_from_slice(entry->key));
}


static void
rw_entry_unlock_and_remove(transactional_splinterdb *txn_kvsb, rw_entry *entry)
{
   rw_entry_unlock(txn_kvsb, entry);
   rw_entry_iceberg_remove(txn_kvsb, entry);
}

static version_lock_status
rw_entry_try_wrlock(transactional_splinterdb *txn_kvsb,
                    rw_entry                 *entry,
                    txn_timestamp             ts)
{
   rw_entry_load_timestamps_from_splinter(txn_kvsb, entry);
   if (entry->wts_max != MVCC_TIMESTAMP_INF) {
      if (ts < entry->wts_max) {
         return VERSION_LOCK_STATUS_ABORT;
      } else {
         return VERSION_LOCK_STATUS_RETRY_VERSION;
      }
   }

   if (ts < entry->rts) {
      return VERSION_LOCK_STATUS_ABORT;
   }

   mvcc_lock l;
   mvcc_lock_load(entry->lock, &l);
   if (l.locked) {
      // a transaction with higher timestamp already holds this lock
      // so we abort to prevent deadlocks
      if (ts < l.lock_holder) {
         // platform_default_log("[%lu] %u (key: %s, version: %u) is locked by
         // %lu(locked: %lu) --> ABORT\n",
         //                      platform_get_tid(),
         //                      ts,
         //                      key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
         //                                 mvcc_user_key(entry->key)),
         //                      mvcc_key_get_version_from_slice(entry->key),
         //                      l.lock_holder, l.locked);
         return VERSION_LOCK_STATUS_ABORT;
      } else {
         // platform_default_log("[%lu] %u (key: %s, version: %u) is locked by
         // %lu(locked: %lu) --> BUSY\n",
         //                      platform_get_tid(),
         //                      ts,
         //                      key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
         //                                 mvcc_user_key(entry->key)),
         //                      mvcc_key_get_version_from_slice(entry->key),
         //                      l.lock_holder, l.locked);
         return VERSION_LOCK_STATUS_BUSY;
      }
   }
   l.locked      = 0;
   l.lock_holder = 0;
   mvcc_lock l2;
   l2.locked      = 1;
   l2.lock_holder = ts;
   bool locked    = mvcc_lock_compare_and_swap(entry->lock, &l, &l2);
   if (locked) {
      rw_entry_load_timestamps_from_splinter(txn_kvsb, entry);
      if (entry->wts_max != MVCC_TIMESTAMP_INF) {
         if (entry->wts_max > ts) {
            rw_entry_unlock(txn_kvsb, entry);
            return VERSION_LOCK_STATUS_ABORT;
         } else {
            rw_entry_unlock(txn_kvsb, entry);
            return VERSION_LOCK_STATUS_RETRY_VERSION;
         }
      }

      if (ts < entry->rts) {
         rw_entry_unlock(txn_kvsb, entry);
         return VERSION_LOCK_STATUS_ABORT;
      }
   } else {
      return VERSION_LOCK_STATUS_BUSY;
   }
   return VERSION_LOCK_STATUS_OK;
}

static version_lock_status
rw_entry_wrlock(transactional_splinterdb *txn_kvsb,
                rw_entry                 *entry,
                txn_timestamp             ts)
{
   rw_entry_iceberg_insert(txn_kvsb, entry);
   version_lock_status rc;
   do {
      rc = rw_entry_try_wrlock(txn_kvsb, entry, ts);
      if (rc == VERSION_LOCK_STATUS_BUSY) {
         // 1us is the value that is mentioned in the paper
         platform_sleep_ns(1000);
      }
   } while (rc == VERSION_LOCK_STATUS_BUSY);
   if (rc != VERSION_LOCK_STATUS_OK) {
      rw_entry_iceberg_remove(txn_kvsb, entry);
   }
   return rc;
}

static version_lock_status
rw_entry_try_rdlock(transactional_splinterdb *txn_kvsb,
                    rw_entry                 *entry,
                    txn_timestamp             ts)
{
   rw_entry_load_timestamps_from_splinter(txn_kvsb, entry);
   if (entry->wts_max != MVCC_VERSION_INF && ts > entry->wts_max) {
      // TO rule would be violated so we need to abort
      return VERSION_LOCK_STATUS_RETRY_VERSION;
   }

   mvcc_lock l;
   mvcc_lock_load(entry->lock, &l);
   if (l.locked) {
      // a transaction with higher timestamp already holds this lock
      // so we abort to prevent deadlocks
      if (l.lock_holder > ts) {
         return VERSION_LOCK_STATUS_ABORT;
      } else {
         return VERSION_LOCK_STATUS_BUSY;
      }
   }

   l.locked      = 0;
   l.lock_holder = 0;
   mvcc_lock l2;
   l2.locked      = 1;
   l2.lock_holder = 0;
   bool locked    = mvcc_lock_compare_and_swap(entry->lock, &l, &l2);
   if (locked) {
      rw_entry_load_timestamps_from_splinter(txn_kvsb, entry);
      if (entry->wts_max != MVCC_VERSION_INF && ts > entry->wts_max) {
         rw_entry_unlock(txn_kvsb, entry);
         return VERSION_LOCK_STATUS_RETRY_VERSION;
      }
   } else {
      return VERSION_LOCK_STATUS_BUSY;
   }
   return VERSION_LOCK_STATUS_OK;
}

static version_lock_status
rw_entry_rdlock(transactional_splinterdb *txn_kvsb,
                rw_entry                 *entry,
                txn_timestamp             ts)
{
   rw_entry_iceberg_insert(txn_kvsb, entry);
   version_lock_status rc;
   do {
      rc = rw_entry_try_rdlock(txn_kvsb, entry, ts);
      if (rc == VERSION_LOCK_STATUS_BUSY) {
         // 1us is the value that is mentioned in the paper
         platform_sleep_ns(1000);
      }
   } while (rc == VERSION_LOCK_STATUS_BUSY);
   if (rc != VERSION_LOCK_STATUS_OK) {
      rw_entry_iceberg_remove(txn_kvsb, entry);
   }
   return rc;
}
static rw_entry *
rw_entry_create(txn_timestamp ts)
{
   rw_entry *new_entry;
   new_entry = TYPED_ZALLOC(0, new_entry);
   platform_assert(new_entry != NULL);
   new_entry->wts_max = MVCC_TIMESTAMP_INF;
   return new_entry;
}

static inline void
rw_entry_deinit(rw_entry *entry)
{
   if (!slice_is_null(entry->key)) {
      void *ptr = (void *)slice_data(entry->key);
      platform_free(0, ptr);
   }

   if (!message_is_null(entry->msg)) {
      void *ptr = (void *)message_data(entry->msg);
      platform_free(0, ptr);
   }
}

static inline void
rw_entry_destroy(rw_entry *entry)
{
   rw_entry_deinit(entry);
   platform_free(0, entry);
}

static inline void
rw_entry_set_key(rw_entry *e, slice key)
{
   // transform the given user key to mvcc_key
   char  *key_buf;
   uint64 key_len = slice_length(key) + sizeof(mvcc_key_header);
   key_buf        = TYPED_ARRAY_ZALLOC(0, key_buf, key_len);
   mvcc_key *mkey = (mvcc_key *)key_buf;
   memcpy(mkey->key, slice_data(key), slice_length(key));
   mkey->header.version = MVCC_VERSION_INF;
   e->key               = slice_create(key_len, key_buf);
}

static inline void
rw_entry_set_msg(rw_entry *e, txn_timestamp ts, message msg)
{
   // transform the given user key to mvcc_key
   uint64 msg_len = sizeof(mvcc_value_header) + message_length(msg);
   char  *msg_buf;
   msg_buf                      = TYPED_ARRAY_ZALLOC(0, msg_buf, msg_len);
   mvcc_value *tuple            = (mvcc_value *)msg_buf;
   tuple->header.update.rts     = ts;
   tuple->header.wts_min        = ts;
   tuple->header.update.wts_max = MVCC_TIMESTAMP_INF;
   memcpy(tuple->value, message_data(msg), message_length(msg));
   e->msg = message_create(message_class(msg), slice_create(msg_len, msg_buf));
}

static inline bool
rw_entry_is_write(const rw_entry *entry)
{
   return !message_is_null(entry->msg);
}

/*
 * Will Set timestamps in entry later.
 * In MVCC, txn maintains only write set.
 */
static inline rw_entry *
rw_entry_get(transactional_splinterdb *txn_kvsb,
             transaction              *txn,
             slice                     user_key,
             const bool                is_read)
{
   rw_entry *entry                    = NULL;
   bool      need_to_create_new_entry = TRUE;
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      entry = txn->rw_entries[i];
      if (data_key_compare(
             txn_kvsb->tcfg->txn_data_cfg->application_data_config,
             mvcc_user_key(entry->key),
             key_create_from_slice(user_key))
          == 0)
      {
         need_to_create_new_entry = FALSE;
         break;
      }
   }

   if (need_to_create_new_entry) {
      entry = rw_entry_create(txn->ts);
      rw_entry_set_key(entry, user_key);
      if (!is_read) {
         txn->rw_entries[txn->num_rw_entries++] = entry;
      }
   }

   return entry;
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

   iceberg_config_default_init(&txn_splinterdb_cfg->lock_tbl_cfg);
   txn_splinterdb_cfg->lock_tbl_cfg.log_slots               = 20;
   txn_splinterdb_cfg->lock_tbl_cfg.merge_value_from_sketch = NULL;
   txn_splinterdb_cfg->lock_tbl_cfg.transform_sketch_value  = NULL;

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
      platform_free(0, txn_splinterdb_cfg->txn_data_cfg);
      platform_free(0, txn_splinterdb_cfg);
      return rc;
   }

   iceberg_table *lock_tbl;
   lock_tbl = TYPED_ZALLOC(0, lock_tbl);
   platform_assert(
      iceberg_init(lock_tbl,
                   &txn_splinterdb_cfg->lock_tbl_cfg,
                   txn_splinterdb_cfg->txn_data_cfg->application_data_config)
      == 0);

   _txn_kvsb->lock_tbl = lock_tbl;

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

   platform_free(0, _txn_kvsb->lock_tbl);
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

   return 0;
}

static inline void
transaction_deinit(transactional_splinterdb *txn_kvsb, transaction *txn)
{
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      rw_entry_destroy(txn->rw_entries[i]);
   }
}

int
transactional_splinterdb_commit(transactional_splinterdb *txn_kvsb,
                                transaction              *txn)
{
   // unlock all writes and update the DB
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      rw_entry *w = txn->rw_entries[i];
      platform_assert(rw_entry_is_write(w));
      mvcc_key *mkey = (mvcc_key *)slice_data(w->key);

#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
      if (0) {
#endif
         // If the message type is the user-level update, then it
         // needs to merge with the previous version. We reallocate
         // the entire buffer in the rw_entry instead of reallocating
         // and updating the user key only.
         // if (message_class(w->msg) == MESSAGE_TYPE_UPDATE) {
         //    if (mkey->header.version == MVCC_VERSION_NODATA) {
         //       merge_accumulator new_message;
         //       merge_accumulator_init_from_message(
         //          &new_message, 0, get_app_value_from_message(w->msg));
         //       data_merge_tuples_final(
         //          txn_kvsb->tcfg->txn_data_cfg->application_data_config,
         //          mvcc_user_key(w->key),
         //          &new_message);
         //       void *ptr = (void *)message_data(w->msg);
         //       platform_free(0, ptr);
         //       rw_entry_set_msg(
         //          w, txn->ts, merge_accumulator_to_message(&new_message));
         //       merge_accumulator_deinit(&new_message);
         //    } else {
         //       splinterdb_lookup_result result;
         //       splinterdb_lookup_result_init(txn_kvsb->kvsb, &result, 0,
         //       NULL); int rc = splinterdb_lookup(txn_kvsb->kvsb, w->key,
         //       &result); platform_assert(rc == 0);
         //       platform_assert(splinterdb_lookup_found(&result));
         //       _splinterdb_lookup_result *_result =
         //          (_splinterdb_lookup_result *)&result;
         //       merge_accumulator new_message;
         //       merge_accumulator_init_from_message(
         //          &new_message, 0, get_app_value_from_message(w->msg));
         //       data_merge_tuples(
         //          txn_kvsb->tcfg->txn_data_cfg->application_data_config,
         //          mvcc_user_key(w->key),
         //          get_app_value_from_merge_accumulator(&_result->value),
         //          &new_message);
         //       void *ptr = (void *)message_data(w->msg);
         //       platform_free(0, ptr);
         //       rw_entry_set_msg(
         //          w, txn->ts, merge_accumulator_to_message(&new_message));
         //       merge_accumulator_deinit(&new_message);
         //       splinterdb_lookup_result_deinit(&result);
         //    }
         // }

         // Bump the version number of w->key(alias to mkey) to insert a new key
         ++mkey->header.version;

         // platform_default_log(
         //    "[%lu] %lu commit insert a new version(%u): %s\n",
         //    platform_get_tid(),
         //    (uint64)txn->ts,
         //    mkey->header.version,
         //    key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
         //               mvcc_user_key(w->key)));

         int rc =
            splinterdb_insert(txn_kvsb->kvsb, w->key, message_slice(w->msg));
         platform_assert(rc == 0, "Error from SplinterDB: %d\n", rc);

         // Get the version number back to update the timestamps of
         // the current version afterward.
         --mkey->header.version;

#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
      }
#endif
      /* platform_default_log("[%ld] commit %s and version %d\n",
       * (int64)txn->ts, */
      /* 			   mvcc_key_get_user_key_from_slice(w->lock.key),
       */
      /* 			   mvcc_key_get_version_from_slice(w->lock.key));
       */
      if (mkey->header.version != MVCC_VERSION_NODATA) {
         mvcc_timestamp_update update = {
            .rts     = 0,
            .wts_max = txn->ts,
         };
         splinterdb_update(
            txn_kvsb->kvsb, w->key, slice_create(sizeof(update), &update));
      } else {
         mvcc_value version_head;
         version_head.header.wts_min        = 0;
         version_head.header.update.rts     = 0;
         version_head.header.update.wts_max = txn->ts;
         int rc                             = splinterdb_insert(
            txn_kvsb->kvsb,
            w->key,
            slice_create(sizeof(version_head), &version_head));
         platform_assert(rc == 0, "SplinterDB Error: %d\n", rc);
      }

      rw_entry_unlock_and_remove(txn_kvsb, w);
   }

   transaction_deinit(txn_kvsb, txn);

   return 0;
}

int
transactional_splinterdb_abort(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      if (rw_entry_is_write(txn->rw_entries[i])) {
         mvcc_lock l;
         mvcc_lock_load(txn->rw_entries[i]->lock, &l);
         if (l.lock_holder == txn->ts) {
            rw_entry_unlock_and_remove(txn_kvsb, txn->rw_entries[i]);
         }
      }
   }
   transaction_deinit(txn_kvsb, txn);

   return 0;
}


static void
find_key_in_splinterdb(transactional_splinterdb *txn_kvsb,
                       slice                     target_user_key)
{
   splinterdb_iterator *it;
   int rc = splinterdb_iterator_init(txn_kvsb->kvsb, &it, NULL_SLICE);
   if (rc != 0) {
      platform_error_log("Error from SplinterDB: %d\n", rc);
   }

   slice range_mvcc_key = NULL_SLICE;
   slice range_value    = NULL_SLICE;

   int cnt = 0;

   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      splinterdb_iterator_get_current(it, &range_mvcc_key, &range_value);
      // Insert the latest version data into the hash table.
      if (data_key_compare(
             txn_kvsb->tcfg->txn_data_cfg->application_data_config,
             mvcc_user_key(target_user_key),
             mvcc_user_key(range_mvcc_key))
          == 0)
      {
         // platform_default_log(
         //    "[%lu] range query attempt to find \"%s\" (%s), v: %u, (actual, "
         //    "\"%s\" "
         //    "(%s), v: %u) "
         //    "(key_len: %lu) exists in db, but cannot be found\n",
         //    platform_get_tid(),
         //    (char *)key_data(mvcc_user_key(target_user_key)),
         //    key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
         //               mvcc_user_key(target_user_key)),
         //    mvcc_key_get_version_from_slice(target_user_key),
         //    (char *)key_data(mvcc_user_key(range_mvcc_key)),
         //    key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
         //               mvcc_user_key(range_mvcc_key)),
         //    mvcc_key_get_version_from_slice(range_mvcc_key),
         //    slice_length(range_mvcc_key));
         // platform_default_log("%s:%u -> ",
         //                      (char
         //                      *)key_data(mvcc_user_key(range_mvcc_key)),
         //                      mvcc_key_get_version_from_slice(range_mvcc_key));
         cnt++;
         break;
      }
   }

   for (; 0 < cnt && cnt < 4; cnt++) {
      splinterdb_iterator_next(it);
      if (splinterdb_iterator_valid(it)) {
         splinterdb_iterator_get_current(it, &range_mvcc_key, &range_value);
         // platform_default_log(
         //    "\"%s\"(%s):%u -> ",
         //    (char *)key_data(mvcc_user_key(range_mvcc_key)),
         //    key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
         //               mvcc_user_key(range_mvcc_key)),
         //    mvcc_key_get_version_from_slice(range_mvcc_key));
      }
   }

   splinterdb_iterator_next(it);
   if (splinterdb_iterator_valid(it)) {
      splinterdb_iterator_get_current(it, &range_mvcc_key, &range_value);
      // platform_default_log("%s:%u\n",
      //                      (char *)key_data(mvcc_user_key(range_mvcc_key)),
      //                      mvcc_key_get_version_from_slice(range_mvcc_key));
   }
   // loop exit may mean error, or just that we've reached the end
   // of the range
   rc = splinterdb_iterator_status(it);
   if (rc != 0) {
      platform_error_log("Error from SplinterDB: %d\n", rc);
   }

   //    // Release resources acquired by the iterator
   //    // If you skip this, other operations, including close(), may
   //    // hang.
   splinterdb_iterator_deinit(it);
}

static int
local_write(transactional_splinterdb *txn_kvsb,
            transaction              *txn,
            slice                     user_key,
            message                   msg)
{
   rw_entry *entry = rw_entry_get(txn_kvsb, txn, user_key, FALSE);
   // Save to the local write set
   if (message_is_null(entry->msg)) {
      mvcc_key *entry_mkey = (mvcc_key *)slice_data(entry->key);
      // Versions are ordered in decreasing order of version number.
      // The version number of entry->key is initially INF.
      version_lock_status lc;
      do {
         splinterdb_iterator *it;
         entry_mkey->header.version = MVCC_VERSION_INF;
         int rc = splinterdb_iterator_init(txn_kvsb->kvsb, &it, entry->key);
         platform_assert(rc == 0, "splinterdb_iterator_init: %d\n", rc);
         slice latest_version_key   = NULL_SLICE;
         slice latest_version_tuple = NULL_SLICE;
         if (splinterdb_iterator_valid(it)) {
            splinterdb_iterator_get_current(
               it, &latest_version_key, &latest_version_tuple);
            if (data_key_compare(
                   txn_kvsb->tcfg->txn_data_cfg->application_data_config,
                   mvcc_user_key(latest_version_key),
                   key_create_from_slice(user_key))
                == 0)
            {
               mvcc_value *tuple =
                  (mvcc_value *)slice_data(latest_version_tuple);
               if ((tuple->header.update.wts_max != MVCC_TIMESTAMP_INF
                    && tuple->header.update.wts_max > txn->ts)
                   || tuple->header.update.rts > txn->ts)
               {
                  splinterdb_iterator_deinit(it);
                  transactional_splinterdb_abort(txn_kvsb, txn);
                  return -1;
               }
               entry_mkey->header.version =
                  mvcc_key_get_version_from_slice(latest_version_key);
            } else {
               // There is no version had been inserted yet
               entry_mkey->header.version = MVCC_VERSION_NODATA;
            }
         } else {
            // it may mean error, or just that we've reached the end of the
            // range
            rc = splinterdb_iterator_status(it);
            platform_assert(rc == 0, "splinterdb_iterator_status: %d\n", rc);

            // It means the database is empty now
            entry_mkey->header.version = MVCC_VERSION_NODATA;
         }
         // Release resources acquired by the iterator
         // If you skip this, other operations, including close(), may hang.
         splinterdb_iterator_deinit(it);

         lc = rw_entry_wrlock(txn_kvsb, entry, txn->ts);
         if (lc == VERSION_LOCK_STATUS_ABORT) {
            // platform_default_log("rw_entry_wrlock failed\n");
            transactional_splinterdb_abort(txn_kvsb, txn);
            return -1;
         }
      } while (lc == VERSION_LOCK_STATUS_RETRY_VERSION);

      // platform_default_log("[%lu] %u write lock %s (version: %u)\n",
      // platform_get_tid(),
      //                      (txn_timestamp)txn->ts,
      //                      key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
      //                               mvcc_user_key(entry->key)),
      //                               mvcc_key_get_version_from_slice(entry->key));
      rw_entry_set_msg(entry, txn->ts, msg);
   } else {
      // Same key is written multiple times in the same transaction
      if (message_is_definitive(msg)) {
         void *ptr = (void *)message_data(entry->msg);
         platform_free(0, ptr);
         rw_entry_set_msg(entry, txn->ts, msg);
      } else {
         platform_assert(message_class(entry->msg) == MESSAGE_TYPE_UPDATE);

         merge_accumulator new_message;
         merge_accumulator_init_from_message(&new_message, 0, msg);
         data_merge_tuples(
            txn_kvsb->tcfg->txn_data_cfg->application_data_config,
            key_create_from_slice(user_key),
            get_app_value_from_message(entry->msg),
            &new_message);
         void *ptr = (void *)message_data(entry->msg);
         platform_free(0, ptr);
         rw_entry_set_msg(
            entry, txn->ts, merge_accumulator_to_message(&new_message));
         merge_accumulator_deinit(&new_message);
      }
   }
   return 0;
}

static int
non_transactional_splinterdb_insert(const splinterdb *kvsb,
                                    slice             key,
                                    slice             value)
{
   rw_entry entry;
   rw_entry_set_key(&entry, key);
   ((mvcc_key *)slice_data(entry.key))->header.version = MVCC_VERSION_START;
   rw_entry_set_msg(&entry, 0, message_create(MESSAGE_TYPE_INSERT, value));
   int rc = splinterdb_insert(kvsb, entry.key, message_slice(entry.msg));
   platform_assert(rc == 0);
   rw_entry_deinit(&entry);
   return 0;
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

   // platform_default_log(
   //    "[%lu] %lu %s key: %s\n",
   //    platform_get_tid(),
   //    (uint64)txn->ts,
   //    __func__,
   //    key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
   //               key_create_from_slice(user_key)));

   return local_write(
      txn_kvsb, txn, user_key, message_create(MESSAGE_TYPE_INSERT, value));
}

int
transactional_splinterdb_delete(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key)
{

   // platform_default_log(
   //    "[%lu] %lu %s key: %s\n",
   //    platform_get_tid(),
   //    (uint64)txn->ts,
   //    __func__,
   //    key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
   //               key_create_from_slice(user_key)));

   return local_write(txn_kvsb, txn, user_key, DELETE_MESSAGE);
}

int
transactional_splinterdb_update(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                slice                     delta)
{

   // platform_default_log(
   //    "[%lu] %lu %s key: %s\n",
   //    platform_get_tid(),
   //    (uint64)txn->ts,
   //    __func__,
   //    key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
   //               key_create_from_slice(user_key)));

   return local_write(
      txn_kvsb, txn, user_key, message_create(MESSAGE_TYPE_UPDATE, delta));
}

int
transactional_splinterdb_lookup(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                splinterdb_lookup_result *result)
{
   // platform_default_log(
   //    "[%lu] %lu %s key: %s\n",
   //    platform_get_tid(),
   //    (uint64)txn->ts,
   //    __func__,
   //    key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
   //               key_create_from_slice(user_key)));
   rw_entry *entry = rw_entry_get(txn_kvsb, txn, user_key, TRUE);
   // Read my writes
   if (rw_entry_is_write(entry)) {
      // TODO: do only insert.
      message app_value = get_app_value_from_message(entry->msg);
      _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
      merge_accumulator_copy_message(&_result->value, app_value);
      return 0;
   }

   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
   version_lock_status        lc;
   do {
      // Versions are ordered in decreasing order of version number.
      mvcc_key *entry_mkey       = (mvcc_key *)slice_data(entry->key);
      entry_mkey->header.version = MVCC_VERSION_INF;
      splinterdb_iterator *it;
      int rc = splinterdb_iterator_init(txn_kvsb->kvsb, &it, entry->key);
      platform_assert(rc == 0, "splinterdb_iterator_init: %d\n", rc);
      int num_versions_found = 0;
      for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
         slice range_key, range_tuple;
         splinterdb_iterator_get_current(it, &range_key, &range_tuple);
         if (data_key_compare(
                txn_kvsb->tcfg->txn_data_cfg->application_data_config,
                mvcc_user_key(range_key),
                key_create_from_slice(user_key))
             == 0)
         {
            ++num_versions_found;
            mvcc_value *tuple = (mvcc_value *)slice_data(range_tuple);
            if (tuple->header.wts_min <= txn->ts) {
               if (txn->ts < tuple->header.update.wts_max) {
                  // Found a readable version
                  entry_mkey->header.version =
                     mvcc_key_get_version_from_slice(range_key);
                  // memcpy((void *)slice_data(entry->key),
                  //       slice_data(range_key),
                  //       slice_length(entry->key));
                  break;
               }
            }
         } else {
            break;
         }
      }
      // loop exit may mean error, or just that we've reached the end of the
      // range
      rc = splinterdb_iterator_status(it);
      platform_assert(rc == 0, "splinterdb_iterator_status: %d\n", rc);
      // Release resources acquired by the iterator
      // If you skip this, other operations, including close(), may hang.
      splinterdb_iterator_deinit(it);

      const bool is_no_data_with_key = (num_versions_found == 0);
      const bool is_all_versions_younger_than_me =
         (entry_mkey->header.version == MVCC_VERSION_INF);

      if (is_no_data_with_key) {
         // If the key is not found, return an empty result
         rw_entry_destroy(entry);
         return 0;
      } else if (is_all_versions_younger_than_me) {
         // All existing versions have wts > ts
         rw_entry_destroy(entry);
         transactional_splinterdb_abort(txn_kvsb, txn);
         return -1;
      }

      lc = rw_entry_rdlock(txn_kvsb, entry, txn->ts);
      if (lc == VERSION_LOCK_STATUS_ABORT) {
         rw_entry_destroy(entry);
         transactional_splinterdb_abort(txn_kvsb, txn);
         return -1;
      }
   } while (lc == VERSION_LOCK_STATUS_RETRY_VERSION);

   // platform_default_log("[%lu] %u read lock %s\n",
   //                      platform_get_tid(),
   //                      (txn_timestamp)txn->ts,
   //                      key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
   //                                 mvcc_user_key(entry->key)));

   // Lock acquired
   int rc = splinterdb_lookup(txn_kvsb->kvsb, entry->key, result);
   platform_assert(rc == 0, "SplinterDB lookup Error: %d\n", rc);
   if (!splinterdb_lookup_found(result)) {
      platform_default_log(
         "[%lu] %lu lookup NOT FOUND (version: %u): %s\n",
         platform_get_tid(),
         (uint64)txn->ts,
         mvcc_key_get_version_from_slice(entry->key),
         key_string(txn_kvsb->tcfg->txn_data_cfg->application_data_config,
                    mvcc_user_key(entry->key)));
      find_key_in_splinterdb(txn_kvsb, entry->key);
      platform_assert(FALSE);
   }
   _result = (_splinterdb_lookup_result *)result;
   const size_t value_len =
      merge_accumulator_length(&_result->value) - sizeof(mvcc_value_header);
   mvcc_value *tuple = (mvcc_value *)merge_accumulator_data(&_result->value);
   memmove(merge_accumulator_data(&_result->value), tuple->value, value_len);
   merge_accumulator_resize(&_result->value, value_len);

   // Update the rts of the readable version
   mvcc_timestamp_update update = {.rts     = txn->ts,
                                   .wts_max = MVCC_TIMESTAMP_INF};
   rc                           = splinterdb_update(
      txn_kvsb->kvsb, entry->key, slice_create(sizeof(update), &update));
   platform_assert(rc == 0, "splinterdb_update: %d\n", rc);

   rw_entry_unlock_and_remove(txn_kvsb, entry);
   platform_assert(rw_entry_is_write(entry) == FALSE);
   rw_entry_destroy(entry);

   return 0;
}
