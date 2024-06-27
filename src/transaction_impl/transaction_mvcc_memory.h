#include "splinterdb/data.h"
#include "platform.h"
#include "data_internal.h"
#include "splinterdb/transaction.h"
#include "util.h"
#include "experimental_mode.h"
#include "splinterdb_internal.h"
#include "isketch/iceberg_table.h"
#include "transaction_stats.h"
#include "poison.h"

typedef struct {
   data_config        super;
   const data_config *application_data_cfg;
} transactional_data_config;

typedef struct transactional_splinterdb_config {
   splinterdb_config           kvsb_cfg;
   transactional_data_config   txn_data_cfg;
   transaction_isolation_level isol_level;
   iceberg_config              iceberght_config;
   sketch_config               sktch_config;
   bool                        is_upsert_disabled;
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   iceberg_table                   *tscache;

#if USE_TRANSACTION_STATS
   // For experimental purpose
   transaction_stats txn_stats;
#endif
} transactional_splinterdb;

#define MVCC_VERSION_NODATA 0
#define MVCC_VERSION_START  1
#define MVCC_VERSION_INF    UINT32_MAX
#define MVCC_TIMESTAMP_INF  ((txn_timestamp)-1)

typedef struct ONDISK mvcc_key_header {
   uint32 version_number;
} mvcc_key_header;

typedef struct ONDISK mvcc_key {
   mvcc_key_header header;
   char            key[];
} mvcc_key;

static slice
mvcc_key_create_slice(slice user_key, uint32 version_number)
{
   char  *mvcc_key_buf;
   uint64 mvcc_key_buf_length =
      slice_length(user_key) + sizeof(mvcc_key_header);
   mvcc_key_buf   = TYPED_ARRAY_ZALLOC(0, mvcc_key_buf, mvcc_key_buf_length);
   mvcc_key *mkey = (mvcc_key *)mvcc_key_buf;
   memcpy(mkey->key, slice_data(user_key), slice_length(user_key));
   mkey->header.version_number = version_number;
   return slice_create(mvcc_key_buf_length, mvcc_key_buf);
}

static void
mvcc_key_destroy_slice(slice s)
{
   platform_free(0, (void *)slice_data(s));
}

static key
mvcc_user_key(slice s)
{
   return key_create(slice_length(s) - sizeof(mvcc_key_header),
                     ((mvcc_key *)slice_data(s))->key);
}

static uint32
mvcc_version_number(slice s)
{
   return ((mvcc_key *)slice_data(s))->header.version_number;
}

static int
mvcc_key_compare(const data_config *cfg, slice key1, slice key2)
{
   platform_assert(slice_length(key1) >= sizeof(mvcc_key_header));
   platform_assert(slice_length(key2) >= sizeof(mvcc_key_header));

   // user_keys are increasingly ordered, but versions are ordered in decreasing
   // order.
   int ret = data_key_compare(
      ((const transactional_data_config *)cfg)->application_data_cfg,
      mvcc_user_key(key1),
      mvcc_user_key(key2));

   if (ret != 0) {
      return ret;
   }

   if (mvcc_version_number(key1) < mvcc_version_number(key2)) {
      return 1;
   } else if (mvcc_version_number(key1) > mvcc_version_number(key2)) {
      return -1;
   } else {
      return 0;
   }
}

typedef struct {
   platform_rwlock rwlock;
   int32           wrlock_holder_ts;
   uint32          version_number;
   txn_timestamp   rts;
   txn_timestamp   wts_min;
   txn_timestamp   wts_max;
} version_meta PLATFORM_CACHELINE_ALIGNED;

typedef enum {
   VERSION_LOCK_STATUS_INVALID = 0,
   VERSION_LOCK_STATUS_OK,
   VERSION_LOCK_STATUS_BUSY,
   VERSION_LOCK_STATUS_DEADLK,
} version_lock_status;

static version_lock_status
version_meta_wrlock(version_meta *meta, txn_timestamp ts)
{
   while (1) {
      platform_status rc = platform_rwlock_trywrlock(&meta->rwlock);
      if (rc.r == EBUSY) {
         if (meta->wrlock_holder_ts == -1) {
            platform_sleep_ns(1000);
         } else {
            // platform_default_log("write lock holder = %u(me: %u)\n",
            // (int32)meta->wrlock_holder_ts, (int32)ts);
            return VERSION_LOCK_STATUS_BUSY;
         }
      } else if (rc.r == 0) {
         meta->wrlock_holder_ts = ts;
         return VERSION_LOCK_STATUS_OK;
      } else {
         platform_assert(0, "platform_rwlock_trywrlock: %d\n", rc.r);
      }
   }
}

static version_lock_status
version_meta_rdlock(version_meta *meta, txn_timestamp ts)
{
   while (1) {
      platform_status rc = platform_rwlock_tryrdlock(&meta->rwlock);
      if (rc.r == EBUSY) {
         if (meta->wrlock_holder_ts == ts) {
            return VERSION_LOCK_STATUS_DEADLK;
         } else if (meta->wrlock_holder_ts > ts) {
            platform_sleep_ns(1000);
         } else {
            return VERSION_LOCK_STATUS_BUSY;
         }
      } else if (rc.r == 0) {
         return VERSION_LOCK_STATUS_OK;
      } else {
         platform_assert(0, "platform_rwlock_tryrdlock: %d\n", rc.r);
      }
   }
}

static void
version_meta_unlock(version_meta *meta)
{
   meta->wrlock_holder_ts = -1;
   platform_rwlock_unlock(&meta->rwlock);
}

// This list node will be stored in the iceberg table.
typedef struct list_node {
   version_meta     *meta;
   struct list_node *next;
} list_node __attribute__((aligned(sizeof(struct list_node))));

static list_node *
list_node_create(txn_timestamp rts,
                 txn_timestamp wts_min,
                 txn_timestamp wts_max)
{
   list_node *new_node;
   new_node = TYPED_ZALLOC(0, new_node);
   platform_assert(new_node != NULL);
   version_meta *meta;
   meta = TYPED_ZALLOC(0, meta);
   platform_assert(meta != NULL);
   platform_rwlock_init(&meta->rwlock, 0, 0);
   meta->wrlock_holder_ts = -1;
   meta->version_number   = MVCC_VERSION_NODATA;
   meta->rts              = rts;
   meta->wts_min          = wts_min;
   meta->wts_max          = wts_max;
   new_node->meta         = meta;
   return new_node;
}

static void
list_node_destroy(list_node *node)
{
   platform_rwlock_destroy(&node->meta->rwlock);
   platform_free(0, node->meta);
   platform_free(0, node);
}

// static void
// list_node_dump(list_node *head)
// {
//    list_node *node = head;
//    while (node != NULL) {
//       platform_default_log("node: %p, wts_min: %u, next: %p\n",
//                            node,
//                            (int32)node->meta->wts_min,
//                            node->next);
//       node = node->next;
//    }
// }

// A global timestamp allocator

uint64 global_ts = 0;

static inline txn_timestamp
get_next_global_ts()
{
   return __atomic_add_fetch(&global_ts, 1, __ATOMIC_RELAXED);
}

// rw_entry is a read-write entry in the transaction.

typedef struct rw_entry {
   slice      key;
   message    msg; // value + op
   list_node *version_list_head;
   list_node *version;
} rw_entry;

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
   if (entry->version_list_head) {
      return FALSE;
   }

   // increase refcount for key
   list_node *head          = list_node_create(0, 0, MVCC_TIMESTAMP_INF);
   entry->version_list_head = head;
   bool is_first_item =
      iceberg_insert_and_get(txn_kvsb->tscache,
                             &entry->key,
                             (ValueType **)&entry->version_list_head,
                             platform_get_tid());
   if (!is_first_item) {
      list_node_destroy(head);
   }

   // platform_default_log("insert key: %s, head: %p\n",
   // (char *)slice_data(entry->key), entry->version_list_head);

   return is_first_item;
}

static inline void
rw_entry_iceberg_remove(transactional_splinterdb *txn_kvsb, rw_entry *entry)
{
   if (!entry->version_list_head) {
      return;
   }

   entry->version_list_head = NULL;

   // KeyType   key_ht   = (KeyType)slice_data(entry->key);
   // ValueType value_ht = {0};
   // if (iceberg_get_and_remove(
   //        txn_kvsb->tscache, &key_ht, &value_ht, platform_get_tid()))
   // {
   //    if (slice_data(entry->key) != key_ht) {
   //       platform_free(0, key_ht);
   //    } else {
   //       entry->need_to_keep_key = 0;
   //    }
   // }
}

static rw_entry *
rw_entry_create()
{
   rw_entry *new_entry;
   new_entry = TYPED_ZALLOC(0, new_entry);
   platform_assert(new_entry != NULL);
   new_entry->version_list_head = NULL;
   new_entry->version           = NULL;
   return new_entry;
}

static inline void
rw_entry_deinit(rw_entry *entry)
{
   if (!message_is_null(entry->msg)) {
      platform_free(0, (void *)message_data(entry->msg));
   }
}

static inline void
rw_entry_destroy(rw_entry *entry)
{
   rw_entry_deinit(entry);
   platform_free(0, entry);
}

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
      if (data_key_compare(txn_kvsb->tcfg->txn_data_cfg.application_data_cfg,
                           key_create_from_slice(entry->key),
                           key_create_from_slice(user_key))
          == 0)
      {
         need_to_create_new_entry = FALSE;
         break;
      }
   }

   if (need_to_create_new_entry) {
      entry      = rw_entry_create();
      entry->key = user_key;
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

   txn_splinterdb_cfg->txn_data_cfg.application_data_cfg = kvsb_cfg->data_cfg;
   memcpy(&txn_splinterdb_cfg->txn_data_cfg.super,
          kvsb_cfg->data_cfg,
          sizeof(txn_splinterdb_cfg->txn_data_cfg.super));
   txn_splinterdb_cfg->txn_data_cfg.super.key_compare = mvcc_key_compare;
   txn_splinterdb_cfg->txn_data_cfg.super.max_key_size +=
      sizeof(mvcc_key_header);
   txn_splinterdb_cfg->kvsb_cfg.data_cfg =
      (data_config *)&txn_splinterdb_cfg->txn_data_cfg;

   iceberg_config_default_init(&txn_splinterdb_cfg->iceberght_config);
   txn_splinterdb_cfg->iceberght_config.log_slots               = 29;
   txn_splinterdb_cfg->iceberght_config.merge_value_from_sketch = NULL;
   txn_splinterdb_cfg->iceberght_config.transform_sketch_value  = NULL;

   // TODO things like filename, logfile, or data_cfg would need a
   // deep-copy
   txn_splinterdb_cfg->isol_level = TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE;
   txn_splinterdb_cfg->is_upsert_disabled = FALSE;

   sketch_config_default_init(&txn_splinterdb_cfg->sktch_config);
}

static void
init_versions(transactional_splinterdb *txn_kvsb)
{
   timestamp start = platform_get_timestamp();
   platform_default_log("Initialize versions by scanning the database\n");
   splinterdb_iterator *it;
   int rc = splinterdb_iterator_init(txn_kvsb->kvsb, &it, NULL_SLICE);
   if (rc != 0) {
      platform_error_log("Error from SplinterDB: %d\n", rc);
   }

   slice key          = NULL_SLICE;
   slice value        = NULL_SLICE;
   slice previous_key = NULL_SLICE;

   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      splinterdb_iterator_get_current(it, &key, &value);
      // Insert the latest version data into the hash table.
      if (slice_is_null(previous_key)
          || data_key_compare(txn_kvsb->tcfg->txn_data_cfg.application_data_cfg,
                              mvcc_user_key(previous_key),
                              mvcc_user_key(key))
                != 0)
      {
         rw_entry entry;
         memset(&entry, 0, sizeof(entry));
         entry.key = key_slice(mvcc_user_key(key));
         rw_entry_iceberg_insert(txn_kvsb, &entry);
         // platform_default_log("%.*s\n", (int)slice_length(entry.key), (char
         // *)slice_data(entry.key));
         list_node *version = list_node_create(0, 0, MVCC_TIMESTAMP_INF);
         version->meta->version_number = mvcc_version_number(key);
         platform_assert(entry.version_list_head->next == NULL);
         entry.version_list_head->next = version;
         previous_key                  = key;
      }
   }

   // loop exit may mean error, or just that we've reached the end of the range
   rc = splinterdb_iterator_status(it);
   if (rc != 0) {
      platform_error_log("Error from SplinterDB: %d\n", rc);
   }

   // Release resources acquired by the iterator
   // If you skip this, other operations, including close(), may hang.
   splinterdb_iterator_deinit(it);
   timestamp elapsed = platform_timestamp_elapsed(start);
   platform_default_log("Done. (%.6f s)\n", (double)elapsed / 1e9);
   platform_default_log("----- Iceberg stats -----\n");
   iceberg_print_state(txn_kvsb->tscache);
   platform_default_log("-------------------------\n");
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
      iceberg_init(tscache,
                   &txn_splinterdb_cfg->iceberght_config,
                   txn_splinterdb_cfg->txn_data_cfg.application_data_cfg)
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
   int rc = transactional_splinterdb_create_or_open(kvsb_cfg, txn_kvsb, TRUE);
   init_versions(*txn_kvsb);
   return rc;
}

void
transactional_splinterdb_close(transactional_splinterdb **txn_kvsb)
{
   transactional_splinterdb *_txn_kvsb = *txn_kvsb;
   splinterdb_close(&_txn_kvsb->kvsb);
   iceberg_print_state(_txn_kvsb->tscache);

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

   return 0;
}

static inline void
transaction_deinit(transactional_splinterdb *txn_kvsb, transaction *txn)
{
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      rw_entry_iceberg_remove(txn_kvsb, txn->rw_entries[i]);
      rw_entry_destroy(txn->rw_entries[i]);
   }
}

// static void
// find_key_in_splinterdb(transactional_splinterdb *txn_kvsb, key
// target_mvcc_key)
// {
//    splinterdb_iterator *it;
//    int                  rc =
//       splinterdb_iterator_init(txn_kvsb->kvsb, &it, NULL_SLICE);
//    if (rc != 0) {
//       platform_error_log("Error from SplinterDB: %d\n", rc);
//    }

//    slice range_mvcc_key   = NULL_SLICE;
//    slice range_value = NULL_SLICE;

//    for (; splinterdb_iterator_valid(it);
//          splinterdb_iterator_next(it)) {
//       splinterdb_iterator_get_current(it, &range_mvcc_key, &range_value);
//       // Insert the latest version data into the hash table.
//       if (data_key_compare(
//                (const data_config *)&txn_kvsb->tcfg->txn_data_cfg,
//                target_mvcc_key,
//                key_create_from_slice(range_mvcc_key))
//             == 0)
//       {
//          {
//             splinterdb_lookup_result result;
//             splinterdb_lookup_result_init(txn_kvsb->kvsb, &result, 0, NULL);
//             rc = splinterdb_lookup(txn_kvsb->kvsb,
//             key_slice(target_mvcc_key), &result); platform_assert(rc == 0);
//             if(splinterdb_lookup_found(&result))
//             {
//                platform_default_log("target_mvcc_key found\n");
//             } else {
//                platform_default_log("target_mvcc_key not found\n");
//             }
//             splinterdb_lookup_result_deinit(&result);
//          }
//          {
//             splinterdb_lookup_result result;
//             splinterdb_lookup_result_init(txn_kvsb->kvsb, &result, 0, NULL);
//             rc = splinterdb_lookup(txn_kvsb->kvsb, range_mvcc_key, &result);
//             platform_assert(rc == 0);
//             if(splinterdb_lookup_found(&result))
//             {
//                platform_default_log("range_mvcc_key found\n");
//             } else {
//                platform_default_log("range_mvcc_key not found\n");
//             }
//             splinterdb_lookup_result_deinit(&result);
//          }
//          platform_default_log(
//             "attempt to find %s, v: %u, (actual, %s, v: %u) "
//             "(key_len: %lu) exists in db, but cannot be found\n",
//             (char *)key_data(mvcc_user_key(key_slice(target_mvcc_key))),
//             mvcc_version_number(key_slice(target_mvcc_key)),
//             (char *)key_data(mvcc_user_key(range_mvcc_key)),
//             mvcc_version_number(range_mvcc_key),
//             slice_length(range_mvcc_key));
//          break;
//       }
//    }

//    // loop exit may mean error, or just that we've reached the end
//    // of the range
//    rc = splinterdb_iterator_status(it);
//    if (rc != 0) {
//       platform_error_log("Error from SplinterDB: %d\n", rc);
//    }

//    // Release resources acquired by the iterator
//    // If you skip this, other operations, including close(), may
//    // hang.
//    splinterdb_iterator_deinit(it);
// }

int
transactional_splinterdb_commit(transactional_splinterdb *txn_kvsb,
                                transaction              *txn)
{
   // unlock all writes and update the DB
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      rw_entry *w = txn->rw_entries[i];
      platform_assert(rw_entry_is_write(w));

      const uint32 new_version_number = w->version->meta->version_number + 1;
      // platform_default_log("insert key: %s\n", (char *)slice_data(w->key));
      // list_node_dump(w->version_list_head);

#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
      if (0) {
#endif

         // If the message type is the user-level update, then it needs
         // to merge with the previous version. The buffer of the merge
         // accumulator will be simply freed by platform_free later, not
         // merge_accumulator_deinit.
         if (message_class(w->msg) == MESSAGE_TYPE_UPDATE) {
            const bool is_key_not_inserted =
               (w->version == w->version_list_head);
            if (is_key_not_inserted) {
               merge_accumulator new_message;
               merge_accumulator_init_from_message(&new_message, 0, w->msg);
               data_merge_tuples_final(
                  txn_kvsb->tcfg->txn_data_cfg.application_data_cfg,
                  key_create_from_slice(w->key),
                  &new_message);
               platform_free(0, (void *)message_data(w->msg));
               w->msg = merge_accumulator_to_message(&new_message);
            } else {
               splinterdb_lookup_result result;
               splinterdb_lookup_result_init(txn_kvsb->kvsb, &result, 0, NULL);
               slice spl_key = mvcc_key_create_slice(
                  w->key, w->version->meta->version_number);
               int rc = splinterdb_lookup(txn_kvsb->kvsb, spl_key, &result);
               platform_assert(rc == 0);

               // // For debugging BEGIN
               // if (!splinterdb_lookup_found(&result)) {
               //    find_key_in_splinterdb(txn_kvsb,
               //    key_create_from_slice(spl_key));
               // }
               // // For debugging END
               mvcc_key_destroy_slice(spl_key);
               platform_assert(splinterdb_lookup_found(&result));
               _splinterdb_lookup_result *_result =
                  (_splinterdb_lookup_result *)&result;
               merge_accumulator new_message;
               merge_accumulator_init_from_message(&new_message, 0, w->msg);
               data_merge_tuples(
                  txn_kvsb->tcfg->txn_data_cfg.application_data_cfg,
                  key_create_from_slice(w->key),
                  merge_accumulator_to_message(&_result->value),
                  &new_message);
               platform_free(0, (void *)message_data(w->msg));
               w->msg = merge_accumulator_to_message(&new_message);
               splinterdb_lookup_result_deinit(&result);
            }
         }

         slice new_key = mvcc_key_create_slice(w->key, new_version_number);
         int   rc =
            splinterdb_insert(txn_kvsb->kvsb, new_key, message_slice(w->msg));
         platform_assert(rc == 0, "Error from SplinterDB: %d\n", rc);
         mvcc_key_destroy_slice(new_key);
#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
      }
#endif

      // Update the wts_max of the previous version. Atomic update
      // (currently implemented by using 64 bits variable)
      w->version->meta->wts_max = txn->ts;

      // Make the new version visible by inserting it into the list.
      list_node *new_version =
         list_node_create(txn->ts, txn->ts, MVCC_TIMESTAMP_INF);
      new_version->meta->version_number = new_version_number;
      new_version->next                 = w->version_list_head->next;
      w->version_list_head->next        = new_version;

      // Unlock the previous version (x)
      version_meta_unlock(w->version->meta);
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
      if (w->version->meta->wrlock_holder_ts == txn->ts) {
         // platform_default_log("[%s] lock release and abort = %u\n",
         //                      (char *)slice_data(w->key),
         //                      (int32)w->version->meta->wrlock_holder_ts);

         version_meta_unlock(w->version->meta);
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
   entry = rw_entry_get(txn_kvsb, txn, user_key, FALSE);
   // Save to the local write set
   if (message_is_null(entry->msg)) {
      // This function will get version_list_head from the iceberg table.
      rw_entry_iceberg_insert(txn_kvsb, entry);

      // Find the latest version
      bool should_retry;
      do {
         should_retry   = FALSE;
         entry->version = entry->version_list_head->next;
         if (entry->version) {
            // platform_default_log("entry->version found: %p\n",
            // entry->version);

            if ((entry->version->meta->wts_max != MVCC_TIMESTAMP_INF
                 && entry->version->meta->wts_max > txn->ts)
                || entry->version->meta->rts > txn->ts)
            {
               // if (entry->version->meta->wts_min > txn->ts)
               //    platform_default_log(
               //       "entry->version->meta->wts_min(%u) > txn->ts (%u)\n",
               //       (uint32)entry->version->meta->wts_min,
               //       (uint32)txn->ts);

               // if (entry->version->meta->rts > txn->ts)
               //    platform_default_log(
               //       "entry->version->meta->rts(%u) > txn->ts (%u)\n",
               //       (uint32)entry->version->meta->rts,
               //       (uint32)txn->ts);

               transactional_splinterdb_abort(txn_kvsb, txn);
               return -1;
            }
         } else {
            // There is no data with the key, it becomes the first version.
            entry->version = entry->version_list_head;
         }

         if (version_meta_wrlock(entry->version->meta, txn->ts)
             == VERSION_LOCK_STATUS_BUSY)
         {
            // platform_default_log("version_meta_wrlock failed\n");
            transactional_splinterdb_abort(txn_kvsb, txn);
            return -1;
         }
         // platform_default_log("[%s] version: %p, write lock holder = %u\n",
         //                      (char *)slice_data(user_key),
         //                      entry->version,
         //                      (int32)entry->version->meta->wrlock_holder_ts);

         // Lock is acquired
         if (entry->version->meta->rts > txn->ts) {
            // platform_default_log(
            //    "[lock acquired] entry->version->meta->rts(%u) > txn->ts
            //    (%u)\n", (uint32)entry->version->meta->rts, (uint32)txn->ts);
            transactional_splinterdb_abort(txn_kvsb, txn);
            return -1;
         }
         // Read wts_max atomically (currently, it is implemented as a 64bits
         // variable.)
         if (entry->version->meta->wts_max != MVCC_TIMESTAMP_INF) {
            if (entry->version->meta->wts_max > txn->ts) {
               // platform_default_log(
               //    "[lock acquired] entry->version->meta->wts_max(%u) >
               //    txn->ts "
               //    "(%u)\n",
               //    (uint32)entry->version->meta->wts_max,
               //    (uint32)txn->ts);

               transactional_splinterdb_abort(txn_kvsb, txn);
               return -1;
            } else {
               // platform_default_log(
               //    "[%s] lock release and retry = %u\n",
               //    (char *)slice_data(user_key),
               //    (int32)entry->version->meta->wrlock_holder_ts);

               version_meta_unlock(entry->version->meta);
               should_retry = TRUE;
            }
         }
      } while (should_retry);
      rw_entry_set_msg(entry, msg);
   } else {
      // Same key is written multiple times in the same transaction
      if (message_is_definitive(msg)) {
         platform_free(0, (void *)message_data(entry->msg));
         rw_entry_set_msg(entry, msg);
      } else {
         // TODO it needs to be checked later for upsert
         key       wkey = key_create_from_slice(entry->key);
         const key ukey = key_create_from_slice(user_key);
         if (data_key_compare(
                txn_kvsb->tcfg->txn_data_cfg.application_data_cfg, wkey, ukey)
             == 0)
         {
            if (message_is_definitive(msg)) {
               platform_free(0, (void *)message_data(entry->msg));
               rw_entry_set_msg(entry, msg);
            } else {
               platform_assert(message_class(entry->msg)
                               != MESSAGE_TYPE_DELETE);
               merge_accumulator new_message;
               merge_accumulator_init_from_message(&new_message, 0, msg);
               data_merge_tuples(
                  txn_kvsb->tcfg->txn_data_cfg.application_data_cfg,
                  ukey,
                  entry->msg,
                  &new_message);
               platform_free(0, (void *)message_data(entry->msg));
               entry->msg = merge_accumulator_to_message(&new_message);
            }
         }
      }
   }
   return 0;
}

static int
non_transactional_splinterdb_insert(const splinterdb *kvsb,
                                    slice             user_key,
                                    slice             value)
{
   slice spl_key = mvcc_key_create_slice(user_key, MVCC_VERSION_START);
   int   rc      = splinterdb_insert(kvsb, spl_key, value);
   platform_assert(rc == 0, "Error from SplinterDB: %d\n", rc);
   mvcc_key_destroy_slice(spl_key);
   return rc;
}

int
transactional_splinterdb_insert(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                slice                     value)
{
   if (!txn) {
      rw_entry entry;
      memset(&entry, 0, sizeof(entry));
      entry.key = user_key;
      rw_entry_iceberg_insert(txn_kvsb, &entry);
      list_node *version = list_node_create(0, 0, MVCC_TIMESTAMP_INF);
      version->meta->version_number = MVCC_VERSION_START;
      platform_assert(entry.version_list_head->next == NULL);
      entry.version_list_head->next = version;

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
   rw_entry *entry = rw_entry_get(txn_kvsb, txn, user_key, TRUE);

   // Read my writes
   if (rw_entry_is_write(entry)) {
      // TODO: do only insert.
      _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
      merge_accumulator_copy_message(&_result->value, entry->msg);
      return 0;
   }

   rw_entry_iceberg_insert(txn_kvsb, entry);

   // Find the latest readable version in the list. If there is no
   // version in the list but the key is in the database, it fills the
   // version metadata on demand. However, it causes a higher abort
   // rate by the r-w lock conflict on the list head.
   list_node          *readable_version;
   bool                should_retry;
   version_lock_status lc;
   do {
      should_retry     = FALSE;
      readable_version = entry->version_list_head->next;
      while (readable_version != NULL) {
         if (readable_version->meta->wts_min <= txn->ts) {
            break;
         }
         readable_version = readable_version->next;
         if (readable_version == NULL) {
            // platform_default_log(
            //    "abort -- all versions' timestamps are higher than me(%u) \n",
            //    (int32)txn->ts);
            rw_entry_destroy(entry);
            transactional_splinterdb_abort(txn_kvsb, txn);
            return -1;
         }
      }
      if (readable_version == NULL) {
         readable_version = entry->version_list_head;
      }
      lc = version_meta_rdlock(readable_version->meta, txn->ts);
      if (lc == VERSION_LOCK_STATUS_BUSY) {
         rw_entry_destroy(entry);
         // platform_default_log(
         //    "[%s] abort %u failed to acquire read lock(holder: %u)\n",
         //    (char *)slice_data(user_key),
         //    (int32)txn->ts,
         //    (int32)readable_version->meta->wrlock_holder_ts);
         transactional_splinterdb_abort(txn_kvsb, txn);
         return -1;
      }
      // Lock acquired
      // Read wts_max atomically (currently, it is implemented as a 64bits
      // variable.)
      if (readable_version->meta->wts_max < txn->ts) {
         if (lc == VERSION_LOCK_STATUS_OK) {
            version_meta_unlock(readable_version->meta);
         }
         should_retry = TRUE;
      }
   } while (should_retry);

   const bool is_key_not_inserted =
      (readable_version == entry->version_list_head);
   if (is_key_not_inserted) {
      if (lc == VERSION_LOCK_STATUS_OK) {
         version_meta_unlock(readable_version->meta);
      }
      rw_entry_destroy(entry);
      return 0;
   }


   slice spl_key =
      mvcc_key_create_slice(user_key, readable_version->meta->version_number);
   int rc = splinterdb_lookup(txn_kvsb->kvsb, spl_key, result);

   // // For debugging
   // if (!splinterdb_lookup_found(result)) {
   //    platform_default_log("find_key_in_splinterdb in lookup\n");
   //    find_key_in_splinterdb(txn_kvsb, key_create_from_slice(spl_key));
   // }

   platform_assert(rc == 0);
   mvcc_key_destroy_slice(spl_key);

   txn_timestamp expected;
   do {
      should_retry = TRUE;
      expected =
         __atomic_load_n(&readable_version->meta->rts, __ATOMIC_RELAXED);
      if (expected < txn->ts) {
         // atomic update to txn->ts
         should_retry =
            !__atomic_compare_exchange_n(&readable_version->meta->rts,
                                         &expected,
                                         txn->ts,
                                         TRUE,
                                         __ATOMIC_RELAXED,
                                         __ATOMIC_RELAXED);
      } else {
         should_retry = FALSE;
      }
   } while (should_retry);

   if (lc == VERSION_LOCK_STATUS_OK) {
      version_meta_unlock(readable_version->meta);
   }

   platform_assert(rw_entry_is_write(entry) == FALSE);
   rw_entry_destroy(entry);
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
