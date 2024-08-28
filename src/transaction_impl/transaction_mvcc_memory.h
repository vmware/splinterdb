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

#define MVCC_VERSION_START 1
#define MVCC_TIMESTAMP_INF ((txn_timestamp)-1)

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
   void *ptr = (void *)slice_data(s);
   platform_free(0, ptr);
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
   uint64_t lock_bit : 1;
   uint64_t lock_holder : 63;
} mvcc_lock;

typedef struct {
   txn_timestamp wts_max;
   txn_timestamp rts;
   txn_timestamp version_number;
   txn_timestamp wts_min;
   mvcc_lock     lock;
   uint64_t      head_lock; // A special lock used for the head insertion
} version_meta __attribute__((aligned(sizeof(txn_timestamp))));

typedef enum {
   VERSION_LOCK_STATUS_INVALID = 0,
   VERSION_LOCK_STATUS_OK,
   VERSION_LOCK_STATUS_BUSY,
   VERSION_LOCK_STATUS_RETRY_VERSION,
   VERSION_LOCK_STATUS_ABORT
} version_lock_status;

static void
version_meta_unlock(version_meta *meta)
{
   *(uint64_t *)&meta->lock = 0;
}

static version_lock_status
version_meta_try_wrlock(version_meta *meta, txn_timestamp ts)
{
   if (meta->wts_max != MVCC_TIMESTAMP_INF) {
      if (meta->wts_max > ts) {
         return VERSION_LOCK_STATUS_ABORT;
      } else {
         return VERSION_LOCK_STATUS_RETRY_VERSION;
      }
   }

   if (ts < meta->rts) {
      return VERSION_LOCK_STATUS_ABORT;
   }

   mvcc_lock l = meta->lock; // atomically read the lock's lock_bit and holder
                             // (64-bit aligned)
   if (l.lock_bit) {
      // a transaction with higher timestamp already holds this lock
      // so we abort to prevent deadlocks
      if (l.lock_holder > ts) {
         return VERSION_LOCK_STATUS_ABORT;
      } else {
         return VERSION_LOCK_STATUS_BUSY;
      }
   }

   mvcc_lock v1;
   v1.lock_bit    = 0;
   v1.lock_holder = 0;
   mvcc_lock v2;
   v2.lock_bit    = 1;
   v2.lock_holder = ts;
   bool locked    = __atomic_compare_exchange((volatile uint64_t *)&meta->lock,
                                           (txn_timestamp *)&v1,
                                           (txn_timestamp *)&v2,
                                           TRUE,
                                           __ATOMIC_RELAXED,
                                           __ATOMIC_RELAXED);

   if (locked) {
      if (meta->wts_max != MVCC_TIMESTAMP_INF) {
         if (meta->wts_max > ts) {
            version_meta_unlock(meta);
            return VERSION_LOCK_STATUS_ABORT;
         } else {
            version_meta_unlock(meta);
            return VERSION_LOCK_STATUS_RETRY_VERSION;
         }
      }

      if (ts < meta->rts) {
         version_meta_unlock(meta);
         return VERSION_LOCK_STATUS_ABORT;
      }
   } else {
      return VERSION_LOCK_STATUS_BUSY;
   }
   // platform_default_log("ts %lu lock\n", ts);
   return VERSION_LOCK_STATUS_OK;
}

static version_lock_status
version_meta_wrlock(version_meta *meta, txn_timestamp ts)
{
   version_lock_status rc;
   do {
      rc = version_meta_try_wrlock(meta, ts);
      if (rc == VERSION_LOCK_STATUS_BUSY) {
         // 1us is the value that is mentioned in the paper
         platform_sleep_ns(1000);
      }
   } while (rc == VERSION_LOCK_STATUS_BUSY);
   return rc;
}

static version_lock_status
version_meta_try_rdlock(version_meta *meta, txn_timestamp ts)
{
   if (meta->wts_max != MVCC_TIMESTAMP_INF && ts > meta->wts_max) {
      // TO rule would be violated so we need to abort
      return VERSION_LOCK_STATUS_RETRY_VERSION;
   }

   mvcc_lock l = meta->lock; // TODO: does this atomically read the lock's
                             // lock_bit and holder (64-bit aligned)?
   if (l.lock_bit) {
      // a transaction with higher timestamp already holds this lock
      // so we abort to prevent deadlocks
      if (l.lock_holder > ts) {
         return VERSION_LOCK_STATUS_ABORT;
      } else {
         return VERSION_LOCK_STATUS_BUSY;
      }
   }

   mvcc_lock v1;
   v1.lock_bit    = 0;
   v1.lock_holder = 0;
   mvcc_lock v2;
   v2.lock_bit    = 1;
   v2.lock_holder = 0;
   bool locked    = __atomic_compare_exchange((volatile uint64_t *)&meta->lock,
                                           (txn_timestamp *)&v1,
                                           (txn_timestamp *)&v2,
                                           TRUE,
                                           __ATOMIC_RELAXED,
                                           __ATOMIC_RELAXED);

   if (locked) {
      if (meta->wts_max != MVCC_TIMESTAMP_INF && ts > meta->wts_max) {
         version_meta_unlock(meta);
         return VERSION_LOCK_STATUS_RETRY_VERSION;
      }
   } else {
      return VERSION_LOCK_STATUS_BUSY;
   }
   return VERSION_LOCK_STATUS_OK;
}

static version_lock_status
version_meta_rdlock(version_meta *meta, txn_timestamp ts)
{
   version_lock_status rc;
   do {
      rc = version_meta_try_rdlock(meta, ts);
      if (rc == VERSION_LOCK_STATUS_BUSY) {
         // 1us is the value that is mentioned in the paper
         platform_sleep_ns(1000);
      }
   } while (rc == VERSION_LOCK_STATUS_BUSY);
   return rc;
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
   meta->version_number = version_number;
   meta->rts            = rts;
   meta->wts_min        = wts_min;
   meta->wts_max        = wts_max;
   node->meta           = meta;
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
   rw_entry *entry = NULL;
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      entry = txn->rw_entries[i];
      if (data_key_compare(txn_kvsb->tcfg->txn_data_cfg.application_data_cfg,
                           key_create_from_slice(entry->key),
                           key_create_from_slice(user_key))
          == 0)
      {
         return entry;
      }
   }

   entry                                  = rw_entry_create();
   entry->key                             = user_key;
   txn->rw_entries[txn->num_rw_entries++] = entry;
   return entry;
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
   if (entry->version_list_head) {
      return FALSE;
   }

   // increase refcount for key

   list_node *head       = list_node_create(0, 0, 0, 0);
   head->meta->head_lock = 1;

   entry->version_list_head = head;
   bool is_first_item =
      iceberg_insert_and_get(txn_kvsb->tscache,
                             &entry->key,
                             (ValueType **)&entry->version_list_head,
                             platform_get_tid());
   if (!is_first_item) {
      while (1) {
         uint64_t zero   = 0;
         uint64_t one    = 1;
         bool     locked = __atomic_compare_exchange(
            (volatile uint64_t *)&entry->version_list_head->meta->head_lock,
            &zero,
            &one,
            TRUE,
            __ATOMIC_RELAXED,
            __ATOMIC_RELAXED);
         if (locked) {
            break;
         }
      }
      entry->version_list_head->meta->head_lock = 0;
      list_node_destroy(head);
   } else {
      platform_assert(head->meta == entry->version_list_head->meta,
                      "head->meta: %p, entry->version_list_head->meta: %p\n",
                      head->meta,
                      entry->version_list_head->meta);
      platform_free(0, head);

      list_node *first_version =
         list_node_create(0, 0, MVCC_TIMESTAMP_INF, MVCC_VERSION_START);
      entry->version_list_head->next            = first_version;
      entry->version_list_head->meta->head_lock = 0;
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
      entry->version_list_head = NULL;
   }
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
   // init_versions(*txn_kvsb);
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

      const uint32 new_version_number = w->version->meta->version_number + 1;
      // platform_default_log("[tid %d] insert key: %s\n",
      // (int)platform_get_tid(), (char *)slice_data(w->key));
      // list_node_dump(w->version_list_head);

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
            void *ptr = (void *)message_data(w->msg);
            platform_free(0, ptr);
            w->msg = merge_accumulator_to_message(&new_message);
         } else {
            _splinterdb_lookup_result *_result =
               (_splinterdb_lookup_result *)&result;
            merge_accumulator new_message;
            merge_accumulator_init_from_message(&new_message, 0, w->msg);
            data_merge_tuples(txn_kvsb->tcfg->txn_data_cfg.application_data_cfg,
                              key_create_from_slice(w->key),
                              merge_accumulator_to_message(&_result->value),
                              &new_message);
            void *ptr = (void *)message_data(w->msg);
            platform_free(0, ptr);
            w->msg = merge_accumulator_to_message(&new_message);
            splinterdb_lookup_result_deinit(&result);
         }
      }

      {
         slice new_key = mvcc_key_create_slice(w->key, new_version_number);
         int   rc =
            splinterdb_insert(txn_kvsb->kvsb, new_key, message_slice(w->msg));
         platform_assert(rc == 0, "Error from SplinterDB: %d\n", rc);
         mvcc_key_destroy_slice(new_key);
      }

      // Make the new version visible by inserting it into the list.
      list_node *new_version = list_node_create(
         txn->ts, txn->ts, MVCC_TIMESTAMP_INF, new_version_number);
      new_version->next          = w->version_list_head->next;
      w->version_list_head->next = new_version;

      // Update the wts_max of the previous version. Atomic update
      // (currently implemented by using 64 bits variable)
      w->version->meta->wts_max = txn->ts;

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
      if (rw_entry_is_write(txn->rw_entries[i])) {
         if (txn->rw_entries[i]->version->meta->lock.lock_holder == txn->ts) {
            version_meta_unlock(txn->rw_entries[i]->version->meta);
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
      version_lock_status lc;
      do {
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

         lc = version_meta_wrlock(entry->version->meta, txn->ts);
         if (lc == VERSION_LOCK_STATUS_ABORT) {
            // platform_default_log("version_meta_wrlock failed\n");
            transactional_splinterdb_abort(txn_kvsb, txn);
            return -1;
         }
      } while (lc == VERSION_LOCK_STATUS_RETRY_VERSION);
      rw_entry_set_msg(entry, msg);
   } else {
      // Same key is written multiple times in the same transaction
      if (message_is_definitive(msg)) {
         void *ptr = (void *)message_data(entry->msg);
         platform_free(0, ptr);
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
               void *ptr = (void *)message_data(entry->msg);
               platform_free(0, ptr);
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
               void *ptr = (void *)message_data(entry->msg);
               platform_free(0, ptr);
               entry->msg = merge_accumulator_to_message(&new_message);
            }
         }
      }
   }
   return 0;
}

// This function is used to initialize benchmarks.
static int
non_transactional_splinterdb_insert(const splinterdb *kvsb,
                                    slice             user_key,
                                    slice             value)
{
   int rc;
   {
      slice spl_key = mvcc_key_create_slice(user_key, MVCC_VERSION_START);
      rc            = splinterdb_insert(kvsb, spl_key, value);
      platform_assert(rc == 0, "Error from SplinterDB: %d\n", rc);
      mvcc_key_destroy_slice(spl_key);
   }
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

   list_node          *readable_version;
   version_lock_status lc;
   do {
      readable_version = entry->version_list_head->next;
      while (readable_version != NULL) {
         if (readable_version->meta->wts_min <= txn->ts) {
            if (txn->ts < readable_version->meta->wts_max) {
               break;
            }
         }
         readable_version = readable_version->next;
         if (readable_version == NULL) {
            transactional_splinterdb_abort(txn_kvsb, txn);
            return -1;
         }
      }
      lc = version_meta_rdlock(readable_version->meta, txn->ts);
      if (lc == VERSION_LOCK_STATUS_ABORT) {
         transactional_splinterdb_abort(txn_kvsb, txn);
         return -1;
      }
   } while (lc == VERSION_LOCK_STATUS_RETRY_VERSION);

   slice spl_key =
      mvcc_key_create_slice(user_key, readable_version->meta->version_number);
   int rc = splinterdb_lookup(txn_kvsb->kvsb, spl_key, result);
   platform_assert(rc == 0);
   platform_assert(splinterdb_lookup_found(result));
   readable_version->meta->rts = txn->ts;
   mvcc_key_destroy_slice(spl_key);

   version_meta_unlock(readable_version->meta);
   return rc;
}
