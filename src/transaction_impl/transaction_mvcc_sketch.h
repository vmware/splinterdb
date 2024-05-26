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
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   iceberg_table                   *tscache;

   // For debugging
   iceberg_table *version_numbers;

#if USE_TRANSACTION_STATS
   // For experimental purpose
   transaction_stats txn_stats;
#endif
} transactional_splinterdb;

#define MVCC_VERSION_LATEST 0
#define MVCC_VERSION_START  1
#define MVCC_VERSION_NODATA UINT32_MAX
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
   platform_free_from_heap(0, (void *)slice_data(s));
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

static void
list_node_init(list_node    *node,
               txn_timestamp rts,
               txn_timestamp wts_min,
               txn_timestamp wts_max,
               uint32        version_number)
{
   version_meta *meta;
   meta = TYPED_ZALLOC(0, meta);
   platform_assert(meta != NULL);
   platform_rwlock_init(&meta->rwlock, 0, 0);
   meta->wrlock_holder_ts = -1;
   meta->version_number   = version_number;
   meta->rts              = rts;
   meta->wts_min          = wts_min;
   meta->wts_max          = wts_max;
   node->meta             = meta;
}

static list_node *
list_node_create(txn_timestamp rts,
                 txn_timestamp wts_min,
                 txn_timestamp wts_max,
                 uint32        version_number)
{
   list_node *new_node;
   new_node = TYPED_ZALLOC(0, new_node);
   platform_assert(new_node != NULL);
   list_node_init(new_node, rts, wts_min, wts_max, version_number);
   return new_node;
}

static void
list_node_deinit(list_node *node)
{
   platform_rwlock_destroy(&node->meta->rwlock);
   platform_free(0, node->meta);
   node->next = NULL;
}

static void
list_node_destroy(list_node *node)
{
   list_node_deinit(node);
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

   list_node *head = list_node_create(0, 0, 0, 0);
   platform_rwlock_wrlock(&head->meta->rwlock);

   entry->version_list_head = head;
   bool is_first_item =
      iceberg_insert_and_get(txn_kvsb->tscache,
                             &entry->key,
                             (ValueType **)&entry->version_list_head,
                             platform_get_tid());
   if (!is_first_item) {
      platform_rwlock_rdlock(&entry->version_list_head->meta->rwlock);
      platform_rwlock_unlock(&entry->version_list_head->meta->rwlock);
      list_node_destroy(head);
   } else {
      platform_assert(head->meta == entry->version_list_head->meta,
                      "head->meta: %p, entry->version_list_head->meta: %p\n",
                      head->meta,
                      entry->version_list_head->meta);
      platform_free(0, head);

      {
         splinterdb_lookup_result v0_result;
         splinterdb_lookup_result_init(txn_kvsb->kvsb, &v0_result, 0, NULL);
         slice v0_key = mvcc_key_create_slice(entry->key, MVCC_VERSION_LATEST);
         splinterdb_lookup(txn_kvsb->kvsb, v0_key, &v0_result);
         _splinterdb_lookup_result *_v0_result =
            (_splinterdb_lookup_result *)&v0_result;
         slice v1_key = mvcc_key_create_slice(entry->key, MVCC_VERSION_START);
         splinterdb_insert(txn_kvsb->kvsb,
                           v1_key,
                           merge_accumulator_to_slice(&_v0_result->value));
         mvcc_key_destroy_slice(v0_key);
         mvcc_key_destroy_slice(v1_key);
         splinterdb_lookup_result_deinit(&v0_result);
      }

      list_node *first_version =
         list_node_create(entry->version_list_head->meta->rts,
                          entry->version_list_head->meta->wts_min,
                          MVCC_TIMESTAMP_INF,
                          MVCC_VERSION_START);
      entry->version_list_head->next = first_version;

      platform_rwlock_unlock(&entry->version_list_head->meta->rwlock);
   }

   platform_assert(entry->version_list_head->next != NULL);

   // platform_default_log("insert key: %s, head: %p\n",
   // (char *)slice_data(entry->key), entry->version_list_head);

   return is_first_item;
}

static inline void
rw_entry_iceberg_remove(transactional_splinterdb *txn_kvsb, rw_entry *entry)
{
   if (entry->version_list_head) {
      iceberg_remove(txn_kvsb->tscache, entry->key, platform_get_tid());
      entry->version_list_head = NULL;
   }
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
      entry                                  = rw_entry_create();
      entry->key                             = user_key;
      txn->rw_entries[txn->num_rw_entries++] = entry;
   }

   return entry;
}

typedef struct sketch_value {
   txn_timestamp wts;
   txn_timestamp rts;
} sketch_value;

static void
sketch_insert_timestamps(ValueType *current_value, ValueType new_value)
{
   sketch_value *current_timestamps = (sketch_value *)current_value;
   list_node    *latest_version     = ((list_node *)&new_value)->next;
   // platform_default_log("%s latest_version->meta->rts %lu,
   // latest_version->meta->wts_min %lu\n", __func__,
   // (uint64)latest_version->meta->rts, (uint64)latest_version->meta->wts_min);
   current_timestamps->wts =
      MAX(current_timestamps->wts, latest_version->meta->wts_min);
   current_timestamps->rts =
      MAX(current_timestamps->rts, latest_version->meta->rts);
   // platform_default_log("insert timestamps rts %lu wts %lu to sketch\n",
   // (uint64)current_timestamps->rts, (uint64)current_timestamps->wts);
}

static void
sketch_merge_timestamps_to_cache(ValueType *hash_table_item,
                                 ValueType  sketch_item)
{
   list_node    *head     = (list_node *)hash_table_item;
   sketch_value *new_node = (sketch_value *)&sketch_item;
   head->meta->wts_min    = MAX(head->meta->wts_min, new_node->wts);
   head->meta->rts        = MAX(head->meta->rts, new_node->rts);
   // platform_default_log("insert timestamps rts %lu wts %lu to cache\n",
   // (uint64)hash_table_node->meta->rts,
   // (uint64)hash_table_node->meta->wts_min);
}

static void
sketch_get_timestamps(ValueType current_value, ValueType *new_value)
{
   sketch_value *current_timestamps = (sketch_value *)&current_value;
   sketch_value *new_timestamps     = (sketch_value *)new_value;
   new_timestamps->wts = MIN(current_timestamps->wts, new_timestamps->wts);
   new_timestamps->rts = MIN(current_timestamps->rts, new_timestamps->rts);
}

static void
sketch_item_to_hash_table_item(ValueType *hash_table_item,
                               ValueType  sketch_item)
{
   // list_node *hash_table_node = (list_node *)hash_table_item;
   // sketch_value *sketch_timestamps     = (sketch_value *)&sketch_item;
   // if (hash_table_node->meta == NULL) {
   //    list_node_init(hash_table_node, sketch_timestamps->rts,
   //    sketch_timestamps->wts, MVCC_TIMESTAMP_INF);
   //    // platform_default_log("Timestamps from sketch to cache: rts %lu wts
   //    %lu\n",
   //    // (uint64)sketch_timestamps->rts, (uint64)sketch_timestamps->wts);
   // } else {
   platform_assert(FALSE, "not used for now");
   // }
}

// It deallocates the version list in the critical section of the
// iceberg_remove.
static void
free_version_list(ValueType *hash_table_item)
{
   // Clean up all nodes in the list when evicting the key from the cache.
   list_node *head = (list_node *)hash_table_item;
   list_node *node = head->next;
   while (node != NULL) {
      list_node *next = node->next;
      list_node_destroy(node);
      node = next;
   }
   memset(head, 0, sizeof(*head));
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
   txn_splinterdb_cfg->iceberght_config.log_slots = 29;
   txn_splinterdb_cfg->iceberght_config.merge_value_from_sketch =
      &sketch_merge_timestamps_to_cache;
   txn_splinterdb_cfg->iceberght_config.transform_sketch_value =
      &sketch_item_to_hash_table_item;
   txn_splinterdb_cfg->iceberght_config.post_remove = &free_version_list;

   // TODO things like filename, logfile, or data_cfg would need a
   // deep-copy
   txn_splinterdb_cfg->isol_level = TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE;

   sketch_config_default_init(&txn_splinterdb_cfg->sktch_config);
   txn_splinterdb_cfg->sktch_config.insert_value_fn = &sketch_insert_timestamps;
   txn_splinterdb_cfg->sktch_config.get_value_fn    = &sketch_get_timestamps;
#if EXPERIMENTAL_MODE_MVCC_COUNTER
   txn_splinterdb_cfg->sktch_config.rows = 1;
   txn_splinterdb_cfg->sktch_config.cols = 1;
#elif EXPERIMENTAL_MODE_MVCC_SKETCH
   txn_splinterdb_cfg->sktch_config.rows = 2;
   txn_splinterdb_cfg->sktch_config.cols = 131072;
#else
#   error "Invalid experimental mode"
#endif
}

// static void
// init_versions(transactional_splinterdb *txn_kvsb)
// {
//    timestamp start = platform_get_timestamp();
//    platform_default_log("Initialize versions by scanning the database\n");
//    splinterdb_iterator *it;
//    int rc = splinterdb_iterator_init(txn_kvsb->kvsb, &it, NULL_SLICE);
//    if (rc != 0) {
//       platform_error_log("Error from SplinterDB: %d\n", rc);
//    }

//    slice key          = NULL_SLICE;
//    slice value        = NULL_SLICE;
//    slice previous_key = NULL_SLICE;

//    for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
//       splinterdb_iterator_get_current(it, &key, &value);
//       // Insert the latest version data into the hash table.
//       if (slice_is_null(previous_key)
//           ||
//           data_key_compare(txn_kvsb->tcfg->txn_data_cfg.application_data_cfg,
//                               mvcc_user_key(previous_key),
//                               mvcc_user_key(key))
//                 != 0)
//       {
//          rw_entry entry;
//          memset(&entry, 0, sizeof(entry));
//          entry.key = key_slice(mvcc_user_key(key));
//          rw_entry_iceberg_insert(txn_kvsb, &entry);
//          // platform_default_log("%.*s\n", (int)slice_length(entry.key),
//          (char
//          // *)slice_data(entry.key));
//          list_node *version = list_node_create(0, 0, MVCC_TIMESTAMP_INF);
//          version->meta->version_number = mvcc_version_number(key);
//          platform_assert(entry.version_list_head->next == NULL);
//          entry.version_list_head->next = version;
//          previous_key                  = key;
//       }
//    }

//    // loop exit may mean error, or just that we've reached the end of the
//    range rc = splinterdb_iterator_status(it); if (rc != 0) {
//       platform_error_log("Error from SplinterDB: %d\n", rc);
//    }

//    // Release resources acquired by the iterator
//    // If you skip this, other operations, including close(), may hang.
//    splinterdb_iterator_deinit(it);
//    timestamp elapsed = platform_timestamp_elapsed(start);
//    platform_default_log("Done. (%.6f s)\n", (double)elapsed / 1e9);
//    platform_default_log("----- Iceberg stats -----\n");
//    iceberg_print_state(txn_kvsb->tscache);
//    platform_default_log("-------------------------\n");
// }

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

   // For debugging
   {
      _txn_kvsb->version_numbers = TYPED_ZALLOC(0, _txn_kvsb->version_numbers);
      platform_assert(iceberg_init(_txn_kvsb->version_numbers,
                                   &txn_splinterdb_cfg->iceberght_config,
                                   kvsb_cfg->data_cfg)
                      == 0);
   }

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
   // init_versions(*txn_kvsb);
   return rc;
}

void
transactional_splinterdb_close(transactional_splinterdb **txn_kvsb)
{
   transactional_splinterdb *_txn_kvsb = *txn_kvsb;
   splinterdb_close(&_txn_kvsb->kvsb);
   iceberg_print_state(_txn_kvsb->tscache);

   // For debugging
   {
      platform_free(0, _txn_kvsb->version_numbers);
   }
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


// static uint32
// find_version_number_in_splinterdb(transactional_splinterdb *txn_kvsb, slice
// user_key)
// {
//    splinterdb_iterator *it;
//    slice start_key = mvcc_key_create_slice(user_key, MVCC_VERSION_NODATA);
//    int                  rc =
//       splinterdb_iterator_init(txn_kvsb->kvsb, &it, start_key);
//    if (rc != 0) {
//       platform_error_log("Error from SplinterDB: %d\n", rc);
//    }

//    slice range_mvcc_key   = NULL_SLICE;
//    slice range_value = NULL_SLICE;

//    uint32 ret_version_number = MVCC_VERSION_NODATA;
//    if (splinterdb_iterator_valid(it)) {
//       splinterdb_iterator_get_current(it, &range_mvcc_key, &range_value);
//       // Insert the latest version data into the hash table.
//       if (data_key_compare(
//                txn_kvsb->tcfg->txn_data_cfg.application_data_cfg,
//                key_create_from_slice(user_key),
//                mvcc_user_key(range_mvcc_key))
//             == 0)
//       {
//          ret_version_number = mvcc_version_number(range_mvcc_key);
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
//    mvcc_key_destroy_slice(start_key);

//    return ret_version_number;
// }

int
transactional_splinterdb_commit(transactional_splinterdb *txn_kvsb,
                                transaction              *txn)
{
   // unlock all writes and update the DB
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      rw_entry *w = txn->rw_entries[i];
      if (!rw_entry_is_write(w)) {
         continue;
      }

      // if (w->version->meta->version_number == MVCC_VERSION_NODATA) {
      //    w->version->meta->version_number =
      //    find_version_number_in_splinterdb(txn_kvsb, w->key);

      //    // // For debugging
      //    // (void)find_version_number_in_splinterdb;

      //    // ValueType vn = 0;
      //    // ValueType *version_number = &vn;
      //    // iceberg_insert_and_get(txn_kvsb->version_numbers,
      //    //                        &w->key,
      //    //                        &version_number,
      //    //                        platform_get_tid());
      //    // w->version->meta->version_number = *version_number;
      //    // *version_number = *version_number + 1;
      // }

      const uint32 new_version_number = w->version->meta->version_number + 1;
      // platform_default_log("[tid %d] insert key: %s\n",
      // (int)platform_get_tid(), (char *)slice_data(w->key));
      // list_node_dump(w->version_list_head);

#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
      if (0) {
#endif

         // If the message type is the user-level update, then it needs
         // to merge with the previous version. The buffer of the merge
         // accumulator will be simply freed by platform_free later, not
         // merge_accumulator_deinit.
         if (message_class(w->msg) == MESSAGE_TYPE_UPDATE) {
            splinterdb_lookup_result result;
            splinterdb_lookup_result_init(txn_kvsb->kvsb, &result, 0, NULL);
            slice spl_key =
               mvcc_key_create_slice(w->key, w->version->meta->version_number);
            int rc = splinterdb_lookup(txn_kvsb->kvsb, spl_key, &result);
            platform_assert(rc == 0);
            mvcc_key_destroy_slice(spl_key);
            if (!splinterdb_lookup_found(&result)) {
               merge_accumulator new_message;
               merge_accumulator_init_from_message(&new_message, 0, w->msg);
               data_merge_tuples_final(
                  txn_kvsb->tcfg->txn_data_cfg.application_data_cfg,
                  key_create_from_slice(w->key),
                  &new_message);
               platform_free_from_heap(0, (void *)message_data(w->msg));
               w->msg = merge_accumulator_to_message(&new_message);
            } else {
               _splinterdb_lookup_result *_result =
                  (_splinterdb_lookup_result *)&result;
               merge_accumulator new_message;
               merge_accumulator_init_from_message(&new_message, 0, w->msg);
               data_merge_tuples(
                  txn_kvsb->tcfg->txn_data_cfg.application_data_cfg,
                  key_create_from_slice(w->key),
                  merge_accumulator_to_message(&_result->value),
                  &new_message);
               platform_free_from_heap(0, (void *)message_data(w->msg));
               w->msg = merge_accumulator_to_message(&new_message);
               splinterdb_lookup_result_deinit(&result);
            }
         }

         slice new_key = mvcc_key_create_slice(w->key, new_version_number);
         int   rc =
            splinterdb_insert(txn_kvsb->kvsb, new_key, message_slice(w->msg));
         platform_assert(rc == 0, "Error from SplinterDB: %d\n", rc);
         mvcc_key_destroy_slice(new_key);

         // maintain another version for the latest
         slice latest_key = mvcc_key_create_slice(w->key, MVCC_VERSION_LATEST);
         rc               = splinterdb_insert(
            txn_kvsb->kvsb, latest_key, message_slice(w->msg));
         platform_assert(rc == 0, "Error from SplinterDB: %d\n", rc);
         mvcc_key_destroy_slice(latest_key);
#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
      }
#endif

      // Update the wts_max of the previous version. Atomic update
      // (currently implemented by using 64 bits variable)
      w->version->meta->wts_max = txn->ts;

      // Make the new version visible by inserting it into the list.
      list_node *new_version = list_node_create(
         txn->ts, txn->ts, MVCC_TIMESTAMP_INF, new_version_number);
      new_version->next          = w->version_list_head->next;
      w->version_list_head->next = new_version;


      // platform_default_log("%lu commit key: %s, rts %lu, wts_min %lu, wts_max
      // %lu\n",
      //    (uint64)txn->ts,
      //    (char *)slice_data(w->key),
      //    w->version->meta->rts,
      //    w->version->meta->wts_min,
      //    w->version->meta->wts_max);

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
      if (w->version) { // Transaction gets a corresponding version.
         if (w->version->meta->wrlock_holder_ts == txn->ts) {
            // platform_default_log("[%lu] key: %s, lock release and abort = %u
            // (instance: %p)\n",
            //                      (uint64)txn->ts,
            //                      (char *)slice_data(w->key),
            //                      (int32)w->version->meta->wrlock_holder_ts,
            //                      w->version->meta);

            version_meta_unlock(w->version->meta);
         }
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
      // This function will get version_list_head from the iceberg
      // table. The version_list_head could have the timestamps of the
      // latest version from sketch when the list is empty.
      rw_entry_iceberg_insert(txn_kvsb, entry);

      // Find the latest version
      bool should_retry;
      do {
         should_retry   = FALSE;
         entry->version = entry->version_list_head->next;
         if (entry->version->meta->wts_min > txn->ts
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

         if (version_meta_wrlock(entry->version->meta, txn->ts)
             == VERSION_LOCK_STATUS_BUSY)
         {
            // platform_default_log("%lu version_meta_wrlock failed key: %s, rts
            // %lu, wts_min %lu, wts_max %lu\n",
            //    (uint64)txn->ts,
            //    (char *)slice_data(user_key),
            //    entry->version->meta->rts,
            //    entry->version->meta->wts_min,
            //    entry->version->meta->wts_max);
            transactional_splinterdb_abort(txn_kvsb, txn);
            return -1;
         }
         // platform_default_log("[%u] wrlock acquired version: %s, write lock
         // holder = %u\n",
         //                      (uint32)txn->ts,
         //                      (char *)slice_data(user_key),
         //                      (int32)entry->version->meta->wrlock_holder_ts);

         // Lock is acquired
         if (entry->version->meta->rts > txn->ts) {
            // platform_default_log("[%u] %s abort after wrlock acquired due to
            // entry->version->meta->rts(%u) > txn->ts(%u)\n", (uint32)txn->ts,
            // (char *)slice_data(user_key), (uint32)entry->version->meta->rts,
            // (uint32)txn->ts);
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

               // platform_default_log(
               //    "[%u] abort %s (wts_max: %lu > ts: %lu)\n",
               //    (uint32)txn->ts,
               //    (char *)slice_data(user_key),
               //    entry->version->meta->wts_max,
               //    (uint64)txn->ts);

               transactional_splinterdb_abort(txn_kvsb, txn);
               return -1;
            } else {
               // platform_default_log(
               //    "[%u] %s lock release and retry = %u (wts_max: %lu, ts:
               //    %lu)\n", (uint32)txn->ts, (char *)slice_data(user_key),
               //    (int32)entry->version->meta->wrlock_holder_ts,
               //    entry->version->meta->wts_max,
               //    (uint64)txn->ts);

               version_meta_unlock(entry->version->meta);
               should_retry = TRUE;
            }
         }
      } while (should_retry);
      // platform_default_log("%lu insert key: %s, rts %lu, wts_min %lu, wts_max
      // %lu\n",
      //    (uint64)txn->ts,
      //    (char *)slice_data(user_key),
      //    entry->version->meta->rts,
      //    entry->version->meta->wts_min,
      //    entry->version->meta->wts_max);
      rw_entry_set_msg(entry, msg);
   } else {
      // Same key is written multiple times in the same transaction
      if (message_is_definitive(msg)) {
         platform_free_from_heap(0, (void *)message_data(entry->msg));
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
               platform_free_from_heap(0, (void *)message_data(entry->msg));
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
               platform_free_from_heap(0, (void *)message_data(entry->msg));
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
   slice spl_key = mvcc_key_create_slice(user_key, MVCC_VERSION_LATEST);
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
      // rw_entry entry;
      // memset(&entry, 0, sizeof(entry));
      // entry.key = user_key;
      // rw_entry_iceberg_insert(txn_kvsb, &entry);
      // list_node *version = list_node_create(0, 0, MVCC_TIMESTAMP_INF);
      // version->meta->version_number = MVCC_VERSION_START;
      // platform_assert(entry.version_list_head->next == NULL);
      // entry.version_list_head->next = version;

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
   platform_assert(FALSE, "Not supported yet");
   return local_write(txn_kvsb, txn, user_key, DELETE_MESSAGE);
}

int
transactional_splinterdb_update(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                slice                     delta)
{
   // MVCC does not support user-level upsert.
   return local_write(
      txn_kvsb, txn, user_key, message_create(MESSAGE_TYPE_INSERT, delta));
   // txn_kvsb, txn, user_key, message_create(MESSAGE_TYPE_UPDATE, delta));
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
      }
      if (readable_version == NULL) {
         // No versions in the list.
         transactional_splinterdb_abort(txn_kvsb, txn);
         return -1;
      }

      // platform_default_log(
      //    "[%u] read %s try to acquire read lock(holder: %u)\n",
      //    (int32)txn->ts,
      //    (char *)slice_data(user_key),
      //    (int32)readable_version->meta->wrlock_holder_ts);
      lc = version_meta_rdlock(readable_version->meta, txn->ts);
      if (lc == VERSION_LOCK_STATUS_BUSY) {
         // platform_default_log(
         //    "[%s] abort %u failed to acquire read lock(holder: %u)\n",
         //    (char *)slice_data(user_key),
         //    (int32)txn->ts,
         //    (int32)readable_version->meta->wrlock_holder_ts);
         transactional_splinterdb_abort(txn_kvsb, txn);
         return -1;
      }
      //  else if (lc == VERSION_LOCK_STATUS_INVALID) {
      //    platform_assert(FALSE,
      //       "[%u] %s failed to acquire read lock due to infinite loop(holder:
      //       %u)\n", (int32)txn->ts, (char *)slice_data(user_key),
      //       (int32)readable_version->meta->wrlock_holder_ts);
      // }

      // Lock acquired
      // Read wts_max atomically (currently, it is implemented as a 64bits
      // variable.)
      if (readable_version->meta->wts_max < txn->ts) {
         // platform_default_log("[%lu] retry read lock wts_max: %lu\n",
         //                            (uint64)txn->ts,
         //                            readable_version->meta->wts_max);
         if (lc == VERSION_LOCK_STATUS_OK) {
            version_meta_unlock(readable_version->meta);
         }
         should_retry = TRUE;
      }
   } while (should_retry);

   // const bool is_key_not_inserted =
   //    (readable_version == entry->version_list_head);
   // if (is_key_not_inserted) {


   //    platform_default_log("%lu lookup failed key: %s, rts %lu, wts_min %lu,
   //    wts_max %lu\n",
   //       (uint64)txn->ts,
   //       (char *)slice_data(user_key),
   //       readable_version->meta->rts,
   //       readable_version->meta->wts_min,
   //       readable_version->meta->wts_max);


   //    if (lc == VERSION_LOCK_STATUS_OK) {
   //       version_meta_unlock(readable_version->meta);
   //    }
   //    return 0;
   // }


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

   if (splinterdb_lookup_found(result)) {
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
   }

   // platform_default_log("%lu lookup key: %s, rts %lu, wts_min %lu, wts_max
   // %lu\n",
   //    (uint64)txn->ts,
   //    (char *)slice_data(user_key),
   //    readable_version->meta->rts,
   //    readable_version->meta->wts_min,
   //    readable_version->meta->wts_max);


   if (lc == VERSION_LOCK_STATUS_OK) {
      version_meta_unlock(readable_version->meta);
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
