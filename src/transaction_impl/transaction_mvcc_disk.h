#include "splinterdb/data.h"
#include "platform.h"
#include "data_internal.h"
#include "splinterdb/transaction.h"
#include "util.h"
#include "experimental_mode.h"
#include "splinterdb_internal.h"
#include "../lock_table.h"
#include "transaction_stats.h"
#include "poison.h"

typedef struct transactional_data_config {
   data_config        super;
   const data_config *application_data_config;
} transactional_data_config;

typedef struct transactional_splinterdb_config {
   splinterdb_config           kvsb_cfg;
   transaction_isolation_level isol_level;
   transactional_data_config  *txn_data_cfg;
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   lock_table                      *lock_tbl;

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
#define MVCC_TIMESTAMP_INF  UINT32_MAX

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

typedef struct ONDISK mvcc_value_header {
   txn_timestamp rts;
   txn_timestamp wts_min;
   txn_timestamp wts_max;
} mvcc_value_header;

typedef struct ONDISK mvcc_timestamp_update {
   txn_timestamp rts;
   txn_timestamp wts_max;
} mvcc_timestamp_update;

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

   platform_assert(merge_accumulator_message_class(new_message)
                   == MESSAGE_TYPE_UPDATE);
   platform_assert(merge_accumulator_length(new_message) == sizeof(update));
   memcpy(&update, merge_accumulator_data(new_message), sizeof(update));

   bool success = merge_accumulator_copy_message(new_message, old_message);
   platform_assert(success, "Failed to copy old_message\n");

   mvcc_value_header *result_header =
      (mvcc_value_header *)merge_accumulator_data(new_message);

   if (update.rts > result_header->rts) {
      result_header->rts = update.rts;
   }
   if (update.wts_max != MVCC_TIMESTAMP_INF) {
      platform_assert(result_header->wts_max == MVCC_TIMESTAMP_INF);
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

typedef struct rw_entry {
   slice            key;
   message          msg; // value + op
   lock_table_entry lock;
} rw_entry;

static rw_entry *
rw_entry_create(txn_timestamp ts)
{
   rw_entry *new_entry;
   new_entry = TYPED_ZALLOC(0, new_entry);
   platform_assert(new_entry != NULL);
   new_entry->lock.id = ts;
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
   e->lock.key          = e->key;
}

static inline void
rw_entry_set_msg(rw_entry *e, txn_timestamp ts, message msg)
{
   // transform the given user key to mvcc_key
   uint64 msg_len = sizeof(mvcc_value_header) + message_length(msg);
   char  *msg_buf;
   msg_buf               = TYPED_ARRAY_ZALLOC(0, msg_buf, msg_len);
   mvcc_value *tuple     = (mvcc_value *)msg_buf;
   tuple->header.rts     = ts;
   tuple->header.wts_min = ts;
   tuple->header.wts_max = MVCC_TIMESTAMP_INF;
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
   _txn_kvsb->lock_tbl = lock_table_create_with_rwlock(
      (const data_config *)txn_splinterdb_cfg->txn_data_cfg);
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
      // insert the new version with increased version number (x+1)
      mkey->header.version++;
#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
      if (0) {
#endif
         int rc =
            splinterdb_insert(txn_kvsb->kvsb, w->key, message_slice(w->msg));
         platform_assert(rc == 0, "Error from SplinterDB: %d\n", rc);

#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
      }
#endif
      /* platform_default_log("[%ld] commit %s and version %d\n",
       * (int64)txn->ts, */
      /* 			   mvcc_key_get_user_key_from_slice(w->lock.key),
       */
      /* 			   mvcc_key_get_version_from_slice(w->lock.key));
       */
      // Update the wts_max of the previous version and unlock the previous
      // version (x)
      mkey->header.version--;
      if (mkey->header.version != MVCC_VERSION_NODATA) {
         mvcc_timestamp_update update = {
            .rts     = 0,
            .wts_max = txn->ts,
         };
         splinterdb_update(
            txn_kvsb->kvsb, w->key, slice_create(sizeof(update), &update));
      }

      /* platform_default_log("[%ld] release write lock for %s and version
       * %d\n", (int64)txn->ts, */
      /* 			   mvcc_key_get_user_key_from_slice(w->lock.key),
       */
      /* 			   mvcc_key_get_version_from_slice(w->lock.key));
       */

      lock_table_release_entry_wrlock(txn_kvsb->lock_tbl, &w->lock);
   }

   transaction_deinit(txn_kvsb, txn);

   return 0;
}

int
transactional_splinterdb_abort(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      rw_entry *w = txn->rw_entries[i];
      if (w->lock.shared_lock && w->lock.shared_lock->id == txn->ts) {
         /* platform_default_log("[%ld] release write lock for %s and version
          * %d\n", (int64)txn->ts, */
         /* 		     mvcc_key_get_user_key_from_slice(w->lock.key), */
         /* 		     mvcc_key_get_version_from_slice(w->lock.key)); */
         lock_table_release_entry_wrlock(txn_kvsb->lock_tbl, &w->lock);
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
   rw_entry *entry;
local_write_begin:
   entry = rw_entry_get(txn_kvsb, txn, user_key, FALSE);
   // Save to the local write set
   if (message_is_null(entry->msg)) {
      mvcc_key *entry_mkey = (mvcc_key *)slice_data(entry->key);
      // Versions are ordered in decreasing order of version number.
      splinterdb_iterator *it;
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
            mvcc_value *tuple = (mvcc_value *)slice_data(latest_version_tuple);
            if (tuple->header.wts_min > txn->ts || tuple->header.rts > txn->ts)
            {
               // Need to abort because the latest version is younger than me
               /* if (tuple->header.wts_min > txn->ts) { */
               /*    platform_default_log("abort because the latest version
                * wts_min (%u) is younger than me(%u)\n", */
               /* 			       tuple->header.wts_min, */
               /*                         (uint32)txn->ts); */
               /* } */
               /* if (tuple->header.rts > txn->ts) { */
               /*    platform_default_log("abort because the latest version
                * rts(%u) is younger than me (%u)\n", */
               /*                         tuple->header.rts, */
               /*                         (uint32)txn->ts); */
               /* } */
               transactional_splinterdb_abort(txn_kvsb, txn);
               splinterdb_iterator_deinit(it);
               return 1;
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

      while (
         lock_table_try_acquire_entry_wrlock(txn_kvsb->lock_tbl, &entry->lock)
         == LOCK_TABLE_RC_BUSY)
      {
         // The lock is held by readers or another writer
         if (entry->lock.shared_lock->id == -1) {
            // The lock is held by readers. (It is possible that the same
            // thread already holds the reader lock. But, it should never
            // happen in our mvcc implementation.)
            platform_sleep_ns(1000);
            // } else if (entry->lock.shared_lock->id < entry->lock.id) {
            //    // The lock is held by another writer, but it is older
            //    // than me.
            //    platform_sleep_ns(1000);
         } else {
            platform_assert(entry->lock.shared_lock->id != entry->lock.id,
                            "Try acquire a write lock twice.\n");
            /* platform_default_log( */
            /*    "[%u] abort due to write lock conflict (current: %u) at
             * %lu\n", */
            /*    (uint32)txn->ts, */
            /*    (uint32)entry->lock.shared_lock->id, */
            /*    platform_get_timestamp()); */
            // The lock is held by another writer
            lock_table_entry_deinit(txn_kvsb->lock_tbl, &entry->lock);
            transactional_splinterdb_abort(txn_kvsb, txn);
            return 1;
         }
      }
      /* platform_default_log("[%ld] acquired write lock for %s and version
       * %d\n", (int64)txn->ts, */
      /* 			   mvcc_key_get_user_key_from_slice(entry->lock.key),
       */
      /* 			   mvcc_key_get_version_from_slice(entry->lock.key));
       */
      // Lock is acquired
      splinterdb_lookup_result result;
      splinterdb_lookup_result_init(txn_kvsb->kvsb, &result, 0, NULL);
      splinterdb_lookup(txn_kvsb->kvsb, entry->key, &result);
      if (splinterdb_lookup_found(&result)) {
         splinterdb_lookup_result_value(&result, &latest_version_tuple);
         mvcc_value *tuple = (mvcc_value *)slice_data(latest_version_tuple);
         if (tuple->header.rts > (uint32)txn->ts) {
            /* platform_default_log( */
            /*    "[%u] abort due to tuple->header.rts (%u) >= txn->ts (%u)\n",
             */
            /*    (uint32)txn->ts, */
            /*    tuple->header.rts, */
            /*    (uint32)txn->ts); */
            splinterdb_lookup_result_deinit(&result);
            lock_table_release_entry_wrlock(txn_kvsb->lock_tbl, &entry->lock);
            transactional_splinterdb_abort(txn_kvsb, txn);
            return 1;
         }
         if (tuple->header.wts_max != MVCC_TIMESTAMP_INF) {
            if (tuple->header.wts_max > txn->ts) {
               // Need to abort because the latest version is younger than me
               /* platform_default_log("[%u] abort due to tuple->header.wts_max
                * (%u) >= txn->ts(%u)\n", */
               /* 			    (uint32)txn->ts, */
               /* 	  tuple->header.wts_max, (uint32)txn->ts); */
               splinterdb_lookup_result_deinit(&result);
               lock_table_release_entry_wrlock(txn_kvsb->lock_tbl,
                                               &entry->lock);
               transactional_splinterdb_abort(txn_kvsb, txn);
               return 1;
            } else {
               splinterdb_lookup_result_deinit(&result);
               lock_table_release_entry_wrlock(txn_kvsb->lock_tbl,
                                               &entry->lock);
               // It is safe to deinit the current entry and retry the
               // write. It can prevent from retrying invalid values
               // in the entry. But it cause a little overhead by
               // freeing and allocating the key.
               rw_entry_destroy(entry);
               txn->num_rw_entries--;
               goto local_write_begin;
            }
         }
      }
      splinterdb_lookup_result_deinit(&result);
      rw_entry_set_msg(entry, txn->ts, msg);
   } else {
      // Same key is written multiple times in the same transaction
      if (message_is_definitive(msg)) {
         platform_free_from_heap(0, (void *)message_data(entry->msg));
         rw_entry_set_msg(entry, txn->ts, msg);
      } else {
         platform_assert(message_class(entry->msg) != MESSAGE_TYPE_DELETE);

         merge_accumulator new_message;
         merge_accumulator_init_from_message(&new_message, 0, msg);
         data_merge_tuples((const data_config *)txn_kvsb->tcfg->txn_data_cfg,
                           key_create_from_slice(entry->key),
                           get_app_value_from_message(entry->msg),
                           &new_message);
         platform_free_from_heap(0, (void *)message_data(entry->msg));
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
   uint64 key_len = sizeof(mvcc_key_header) + slice_length(key);
   char  *key_buf;
   key_buf        = TYPED_ARRAY_ZALLOC(0, key_buf, key_len);
   mvcc_key *mkey = (mvcc_key *)key_buf;
   memcpy(mkey->key, slice_data(key), slice_length(key));
   mkey->header.version = MVCC_VERSION_START;
   uint64 value_len     = sizeof(mvcc_value_header) + slice_length(value);
   char  *value_buf;
   value_buf          = TYPED_ARRAY_ZALLOC(0, value_buf, value_len);
   mvcc_value *mvalue = (mvcc_value *)value_buf;
   memcpy(mvalue->value, slice_data(value), slice_length(value));
   mvalue->header.rts     = 0;
   mvalue->header.wts_min = 0;
   mvalue->header.wts_max = MVCC_TIMESTAMP_INF;
   int rc                 = splinterdb_insert(
      kvsb, slice_create(key_len, key_buf), slice_create(value_len, value_buf));
   platform_free(0, key_buf);
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
   rw_entry *entry;
find_readable_version:
   /* platform_default_log("[%ld] lookup start for %s at %lu\n", (int64)txn->ts,
    */
   /* 			(char *)slice_data(user_key), platform_get_timestamp());
    */
   entry = rw_entry_get(txn_kvsb, txn, user_key, TRUE);

   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;

   // Read my writes
   if (rw_entry_is_write(entry)) {
      // TODO: do only insert.
      message app_value = get_app_value_from_message(entry->msg);
      merge_accumulator_copy_message(&_result->value, app_value);
      return 0;
   }

   // Versions are ordered in decreasing order of version number.
   mvcc_key            *entry_mkey = (mvcc_key *)slice_data(entry->key);
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
         if (tuple->header.wts_min < txn->ts) {
            // Found a readable version
            entry_mkey->header.version =
               mvcc_key_get_version_from_slice(range_key);

            /* const size_t value_len = */
            /*    slice_length(range_tuple) - sizeof(mvcc_value_header); */
            /* merge_accumulator_resize(&_result->value, value_len); */
            /* memcpy(merge_accumulator_data(&_result->value), */
            /*        tuple->value, */
            /*        value_len); */
            break;
         }
      } else {
         break;
      }
   }
   // loop exit may mean error, or just that we've reached the end of the range
   rc = splinterdb_iterator_status(it);
   platform_assert(rc == 0, "splinterdb_iterator_status: %d\n", rc);
   // Release resources acquired by the iterator
   // If you skip this, other operations, including close(), may hang.
   splinterdb_iterator_deinit(it);

   /* platform_default_log("[%ld] range query done for %s at %lu\n",
    * (int64)txn->ts, */
   /* 			(char *)slice_data(user_key), platform_get_timestamp());
    */

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
      /* platform_default_log("All existing versions are younger than me
       * (%u)\n", (uint32)txn->ts); */
      transactional_splinterdb_abort(txn_kvsb, txn);
      return 1;
   }


   lock_table_rc lock_rc;
   while ((lock_rc = lock_table_try_acquire_entry_rdlock(txn_kvsb->lock_tbl,
                                                         &entry->lock))
          == LOCK_TABLE_RC_BUSY)
   {
      // There is a writer holding the lock
      if (entry->lock.shared_lock->id == txn->ts) {
         // That is me. I can read this version.
         break;
      } else if (entry->lock.shared_lock->id > txn->ts) {
         // The writer is younger than me. I can read this version.
         platform_sleep_ns(1000);
      } else {
         // The writer is older than me. I need to abort because there might be
         // a newer version.

         /* platform_default_log("[%u] abort due to read lock conflict (current:
          * %ld) at %lu\n", */
         /* 		     (uint32)txn->ts, */
         /* 		     (int64)entry->lock.shared_lock->id, */
         /* 		     platform_get_timestamp()); */
         lock_table_entry_deinit(txn_kvsb->lock_tbl, &entry->lock);
         rw_entry_destroy(entry);
         transactional_splinterdb_abort(txn_kvsb, txn);
         return 1;
      }
   }
   /* platform_default_log("[%ld] acquired read lock for %s and version %d at
    * %lu\n", (int64)txn->ts, */
   /* 			mvcc_key_get_user_key_from_slice(entry->lock.key), */
   /* 			mvcc_key_get_version_from_slice(entry->lock.key), */
   /* 			platform_get_timestamp()); */
   // Lock acquired
   splinterdb_lookup(txn_kvsb->kvsb, entry->key, result);
   _result           = (_splinterdb_lookup_result *)result;
   mvcc_value *tuple = (mvcc_value *)merge_accumulator_data(&_result->value);
   if (tuple->header.wts_max < txn->ts) {
      /* platform_default_log( */
      /*    "goto find_readable_version wts_max(%u) < txn->ts(%u)\n", */
      /*    (uint32)tuple->header.wts_max, */
      /*    (uint32)txn->ts); */
      if (lock_rc == LOCK_TABLE_RC_OK) {
         /* platform_default_log("[%ld] release read lock for %s and version %d
          * at %lu\n", (int64)txn->ts, */
         /* 		     mvcc_key_get_user_key_from_slice(entry->lock.key),
          */
         /* 		     mvcc_key_get_version_from_slice(entry->lock.key),
          */
         /* 		     platform_get_timestamp()); */
         lock_table_release_entry_rdlock(txn_kvsb->lock_tbl, &entry->lock);
      }
      rw_entry_destroy(entry);
      goto find_readable_version;
   }

   const size_t value_len =
      merge_accumulator_length(&_result->value) - sizeof(mvcc_value_header);
   memmove(merge_accumulator_data(&_result->value), tuple->value, value_len);
   merge_accumulator_resize(&_result->value, value_len);

   // Update the rts of the readable version
   mvcc_timestamp_update update = {.rts     = txn->ts,
                                   .wts_max = MVCC_TIMESTAMP_INF};
   rc                           = splinterdb_update(
      txn_kvsb->kvsb, entry->key, slice_create(sizeof(update), &update));
   platform_assert(rc == 0, "splinterdb_update: %d\n", rc);
   if (lock_rc == LOCK_TABLE_RC_OK) {
      /* platform_default_log("[%ld] release read lock for %s and version %d at
       * %lu\n", (int64)txn->ts, */
      /* 			  mvcc_key_get_user_key_from_slice(entry->lock.key),
       */
      /* 			  mvcc_key_get_version_from_slice(entry->lock.key),
       */
      /* 			  platform_get_timestamp()); */
      lock_table_release_entry_rdlock(txn_kvsb->lock_tbl, &entry->lock);
   }
   platform_assert(rw_entry_is_write(entry) == FALSE);
   rw_entry_destroy(entry);

   return 0;
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
