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

#define MVCC_VERSION_INF   UINT32_MAX
#define MVCC_TIMESTAMP_INF UINT32_MAX

typedef struct mvcc_key_header {
   uint32 version;
} mvcc_key_header;

typedef struct mvcc_key {
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

static int
mvcc_key_compare(const data_config *cfg, slice key1, slice key2)
{
   int order = -1; // 1: increasing order, -1: decreasing order

   int ret = data_key_compare(
      ((const transactional_data_config *)cfg)->application_data_config,
      key_create(mvcc_key_get_user_key_length_from_slice(key1),
                 mvcc_key_get_user_key_from_slice(key1)),
      key_create(mvcc_key_get_user_key_length_from_slice(key2),
                 mvcc_key_get_user_key_from_slice(key2)));

   if (ret != 0) {
      return order * ret;
   }

   // Keys are sorted in increasing order of wts
   return order
          * (mvcc_key_get_version_from_slice(key1)
             - mvcc_key_get_version_from_slice(key2));
}

typedef struct ONDISK mvcc_value_header {
   txn_timestamp rts;
   txn_timestamp wts_min;
   txn_timestamp wts_max;
} mvcc_value_header;

typedef struct ONDISK mvcc_value {
   mvcc_value_header header;
   char              value[];
} mvcc_value;

#define TIMESTAMP_UPDATE_MAGIC 0x4938723

typedef enum timestamp_update_type {
   TIMESTAMP_UPDATE_TYPE_INVALID = 0,
   TIMESTAMP_UPDATE_TYPE_RTS,
   TIMESTAMP_UPDATE_TYPE_WTS_MAX
} timestamp_update_type;

typedef struct ONDISK timestamp_value_update {
   uint32                magic;
   timestamp_update_type type;
   txn_timestamp         ts;
} timestamp_value_update;

static inline bool
is_message_rts_update(message msg)
{
   timestamp_value_update *update_msg =
      (timestamp_value_update *)message_data(msg);
   if (update_msg->magic != TIMESTAMP_UPDATE_MAGIC) {
      return FALSE;
   }
   platform_assert(update_msg->type != TIMESTAMP_UPDATE_TYPE_INVALID,
                   "Invalid timestamp update type\n");
   return update_msg->type == TIMESTAMP_UPDATE_TYPE_RTS;
}

static inline bool
is_merge_accumulator_rts_update(merge_accumulator *ma)
{
   timestamp_value_update *update_msg =
      (timestamp_value_update *)merge_accumulator_data(ma);
   if (update_msg->magic != TIMESTAMP_UPDATE_MAGIC) {
      return FALSE;
   }
   platform_assert(update_msg->type != TIMESTAMP_UPDATE_TYPE_INVALID,
                   "Invalid timestamp update type\n");
   return update_msg->type == TIMESTAMP_UPDATE_TYPE_RTS;
}

static inline bool
is_message_wts_max_update(message msg)
{
   timestamp_value_update *update_msg =
      (timestamp_value_update *)message_data(msg);
   if (update_msg->magic != TIMESTAMP_UPDATE_MAGIC) {
      return FALSE;
   }
   platform_assert(update_msg->type != TIMESTAMP_UPDATE_TYPE_INVALID,
                   "Invalid timestamp update type\n");
   return update_msg->type == TIMESTAMP_UPDATE_TYPE_WTS_MAX;
}

static inline bool
is_merge_accumulator_wts_max_update(merge_accumulator *ma)
{
   timestamp_value_update *update_msg =
      (timestamp_value_update *)merge_accumulator_data(ma);
   if (update_msg->magic != TIMESTAMP_UPDATE_MAGIC) {
      return FALSE;
   }
   platform_assert(update_msg->type != TIMESTAMP_UPDATE_TYPE_INVALID,
                   "Invalid timestamp update type\n");
   return update_msg->type == TIMESTAMP_UPDATE_TYPE_WTS_MAX;
}

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
   if (is_message_rts_update(old_message)
       || is_message_wts_max_update(old_message))
   {
      // Just discard
      return 0;
   }

   if (is_merge_accumulator_rts_update(new_message)) {
      timestamp_value_update *update_msg =
         (timestamp_value_update *)merge_accumulator_data(new_message);
      merge_accumulator_copy_message(new_message, old_message);
      mvcc_value_header *new_header =
         (mvcc_value_header *)merge_accumulator_data(new_message);
      new_header->rts = MAX(new_header->rts, update_msg->ts);

      return 0;
   } else if (is_merge_accumulator_wts_max_update(new_message)) {
      timestamp_value_update *update_msg =
         (timestamp_value_update *)merge_accumulator_data(new_message);
      merge_accumulator_copy_message(new_message, old_message);
      mvcc_value_header *new_header =
         (mvcc_value_header *)merge_accumulator_data(new_message);
      new_header->wts_max = update_msg->ts;

      return 0;
   }

   message old_value_message = get_app_value_from_message(old_message);
   message new_value_message =
      get_app_value_from_merge_accumulator(new_message);

   merge_accumulator new_value_ma;
   merge_accumulator_init_from_message(
      &new_value_ma,
      new_message->data.heap_id,
      new_value_message); // FIXME: use a correct heap_id

   data_merge_tuples(
      ((const transactional_data_config *)cfg)->application_data_config,
      key_create_from_slice(key),
      old_value_message,
      &new_value_ma);

   merge_accumulator_resize(new_message,
                            sizeof(mvcc_value_header)
                               + merge_accumulator_length(&new_value_ma));

   mvcc_value *new_tuple = merge_accumulator_data(new_message);
   memcpy(&new_tuple->value,
          merge_accumulator_data(&new_value_ma),
          merge_accumulator_length(&new_value_ma));

   merge_accumulator_deinit(&new_value_ma);

   merge_accumulator_set_class(new_message, message_class(old_message));

   return 0;
}


static int
merge_mvcc_tuple_final(const data_config *cfg,
                       slice              key,
                       merge_accumulator *oldest_message)
{
   platform_assert(!is_merge_accumulator_rts_update(oldest_message),
                   "oldest_message shouldn't be a rts update\n");

   platform_assert(!is_merge_accumulator_wts_max_update(oldest_message),
                   "oldest_message shouldn't be a wts_max update\n");

   message oldest_message_value =
      get_app_value_from_merge_accumulator(oldest_message);
   merge_accumulator app_oldest_message;
   merge_accumulator_init_from_message(
      &app_oldest_message,
      app_oldest_message.data.heap_id, // FIXME: use a correct heap id
      oldest_message_value);

   data_merge_tuples_final(
      ((const transactional_data_config *)cfg)->application_data_config,
      key_create_from_slice(key),
      &app_oldest_message);

   merge_accumulator_resize(oldest_message,
                            sizeof(mvcc_value_header)
                               + merge_accumulator_length(&app_oldest_message));
   mvcc_value *tuple = merge_accumulator_data(oldest_message);
   memcpy(&tuple->value,
          merge_accumulator_data(&app_oldest_message),
          merge_accumulator_length(&app_oldest_message));

   merge_accumulator_deinit(&app_oldest_message);

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
   bool             is_read;
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
rw_entry_set_key(rw_entry *e, slice key, const data_config *cfg)
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
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      entry = txn->rw_entries[i];
      if (data_key_compare(
             cfg,
             key_create(mvcc_key_get_user_key_length_from_slice(entry->key),
                        mvcc_key_get_user_key_from_slice(entry->key)),
             key_create_from_slice(user_key))
          == 0)
      {
         need_to_create_new_entry = FALSE;
         break;
      }
   }

   if (need_to_create_new_entry) {
      entry = rw_entry_create(txn->ts);
      rw_entry_set_key(entry, user_key, cfg);
      txn->rw_entries[txn->num_rw_entries++] = entry;
   }

   entry->is_read = entry->is_read || is_read;
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
   _txn_kvsb->lock_tbl = lock_table_create_with_rwlock(kvsb_cfg->data_cfg);
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
         // Update the wts_max of the previous version and unlock the previous
         // version (x)
         mkey->header.version--;
         if (mkey->header.version > 0) {
            timestamp_value_update wts_max_update = {
               .magic = TIMESTAMP_UPDATE_MAGIC,
               .type  = TIMESTAMP_UPDATE_TYPE_WTS_MAX,
               .ts    = txn->ts};
            splinterdb_update(
               txn_kvsb->kvsb,
               w->key,
               slice_create(sizeof(wts_max_update), &wts_max_update));
         }
         lock_table_release_entry_rwlock(txn_kvsb->lock_tbl, &w->lock);
      }
   }

   transaction_deinit(txn_kvsb, txn);

   return 0;
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
   rw_entry          *entry = rw_entry_get(txn_kvsb, txn, user_key, cfg, FALSE);
   // Save to the local write set
   if (message_is_null(entry->msg)) {
      rw_entry_set_msg(entry, txn->ts, msg);
   local_write_begin:;
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
                cfg,
                key_create(
                   mvcc_key_get_user_key_length_from_slice(latest_version_key),
                   mvcc_key_get_user_key_from_slice(latest_version_tuple)),
                key_create_from_slice(user_key))
             == 0)
         {
            mvcc_value *tuple = (mvcc_value *)slice_data(latest_version_tuple);
            if (tuple->header.wts_min >= txn->ts
                && tuple->header.rts >= txn->ts) {
               // Need to abort because the latest version is younger than me
               transactional_splinterdb_abort(txn_kvsb, txn);
               return -1;
            }
            entry_mkey->header.version =
               mvcc_key_get_version_from_slice(latest_version_key);
         } else {
            // There is no version had been inserted yet
            entry_mkey->header.version = 0;
         }
      } else {
         // loop exit may mean error, or just that we've reached the end of the
         // range
         rc = splinterdb_iterator_status(it);
         platform_assert(rc == 0, "splinterdb_iterator_status: %d\n", rc);

         // It means the database is empty now
         entry_mkey->header.version = 0;
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
         } else if (entry->lock.shared_lock->id < entry->lock.id) {
            // The lock is held by another writer, but it is older than me.
            platform_sleep_ns(1000);
         } else {
            platform_assert(entry->lock.shared_lock->id != entry->lock.id,
                            "Try acquire a write lock twice.\n");
            // The lock is held by another writer
            transactional_splinterdb_abort(txn_kvsb, txn);
            return -1;
         }
      }
      // Lock is acquired
      splinterdb_lookup_result result;
      splinterdb_lookup_result_init(txn_kvsb->kvsb, &result, 0, NULL);
      splinterdb_lookup(txn_kvsb->kvsb, entry->key, &result);
      if (splinterdb_lookup_found(&result)) {
         splinterdb_lookup_result_value(&result, &latest_version_tuple);
         mvcc_value *tuple = (mvcc_value *)slice_data(latest_version_tuple);
         if (tuple->header.rts >= txn->ts) {
            lock_table_release_entry_rwlock(txn_kvsb->lock_tbl, &entry->lock);
            transactional_splinterdb_abort(txn_kvsb, txn);
            return -1;
         }
         if (tuple->header.wts_max > txn->ts) {
            // Need to abort because the latest version is younger than me
            lock_table_release_entry_rwlock(txn_kvsb->lock_tbl, &entry->lock);
            transactional_splinterdb_abort(txn_kvsb, txn);
            return -1;
         } else {
            lock_table_release_entry_rwlock(txn_kvsb->lock_tbl, &entry->lock);
            goto local_write_begin;
         }
      }
      splinterdb_lookup_result_deinit(&result);

   } else {
      // Same key is written multiple times in the same transaction
      if (message_is_definitive(msg)) {
         platform_free_from_heap(0, (void *)message_data(entry->msg));
         rw_entry_set_msg(entry, txn->ts, msg);
      } else {
         platform_assert(message_class(entry->msg) != MESSAGE_TYPE_DELETE);

         merge_accumulator new_message;
         merge_accumulator_init_from_message(&new_message, 0, msg);
         data_merge_tuples(cfg,
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
   uint64 value_len = sizeof(mvcc_value_header) + slice_length(value);
   char  *value_buf;
   value_buf          = TYPED_ARRAY_ZALLOC(0, value_buf, value_len);
   mvcc_value *mvalue = (mvcc_value *)value_buf;
   memcpy(mvalue->value, slice_data(value), slice_length(value));
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

   // Read my writes
   if (rw_entry_is_write(entry)) {
      mvcc_value  *tuple = (mvcc_value *)message_data(entry->msg);
      const size_t value_len =
         message_length(entry->msg) - sizeof(mvcc_value_header);
      merge_accumulator_resize(&_result->value, value_len);
      memcpy(merge_accumulator_data(&_result->value),
             tuple->value,
             merge_accumulator_length(&_result->value));

      return 0;
   }

   // Versions are ordered in decreasing order of version number.
   mvcc_key            *entry_mkey = (mvcc_key *)slice_data(entry->key);
   splinterdb_iterator *it;
   int rc = splinterdb_iterator_init(txn_kvsb->kvsb, &it, entry->key);
   platform_assert(rc == 0, "splinterdb_iterator_init: %d\n", rc);
   slice       readable_version_key   = NULL_SLICE;
   slice       readable_version_tuple = NULL_SLICE;
   int         num_versions_found     = 0;
   mvcc_value *tuple                  = NULL;
   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      slice range_key;
      splinterdb_iterator_get_current(it, &range_key, &readable_version_tuple);
      // do something with key and value...
      if (data_key_compare(
             cfg,
             key_create(mvcc_key_get_user_key_length_from_slice(range_key),
                        mvcc_key_get_user_key_from_slice(range_key)),
             key_create_from_slice(user_key))
          == 0)
      {
         ++num_versions_found;
         tuple = (mvcc_value *)slice_data(readable_version_tuple);
         if (tuple->header.wts_min < txn->ts) {
            readable_version_key = range_key;
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

   // Transaction aborts if there is no version with wts(B_x) <= ts
   if (slice_is_null(readable_version_key)) {
      if (num_versions_found == 0) {
         // If the key is not found, return an empty result
         --txn->num_rw_entries;
         rw_entry_deinit(entry);
         return 0;
      } else {
         // All existing versions have wts > ts
         transactional_splinterdb_abort(txn_kvsb, txn);
         return -1;
      }
   }

   entry_mkey->header.version =
      mvcc_key_get_version_from_slice(readable_version_key);

   lock_table_rc lock_rc;

   while ((lock_rc = lock_table_try_acquire_entry_rdlock(txn_kvsb->lock_tbl,
                                                         &entry->lock))
          == LOCK_TABLE_RC_BUSY)
   {
      // There is a writer holding the lock
      if (entry->lock.shared_lock->id == entry->lock.id) {
         // That is me. I can read this version.
         break;
      } else if (entry->lock.shared_lock->id > entry->lock.id) {
         // The writer is younger than me. I can read this version.
         platform_sleep_ns(1000);
      } else {
         // The writer is older than me. I need to abort because there might be
         // a newer version.
         transactional_splinterdb_abort(txn_kvsb, txn);
         return -1;
      }
   }

   // Lock acquired
   // Update the rts of the readable version
   timestamp_value_update rts_update = {.magic = TIMESTAMP_UPDATE_MAGIC,
                                        .type  = TIMESTAMP_UPDATE_TYPE_RTS,
                                        .ts    = txn->ts};
   splinterdb_update(txn_kvsb->kvsb,
                     readable_version_key,
                     slice_create(sizeof(rts_update), &rts_update));

   if (lock_rc == LOCK_TABLE_RC_OK) {
      lock_table_release_entry_rwlock(txn_kvsb->lock_tbl, &entry->lock);
   }

   const size_t value_len =
      merge_accumulator_length(&_result->value) - sizeof(mvcc_value_header);
   memmove(merge_accumulator_data(&_result->value), tuple->value, value_len);
   merge_accumulator_resize(&_result->value, value_len);

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
