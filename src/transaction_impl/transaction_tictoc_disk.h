#include "data_internal.h"
#include "splinterdb/data.h"
#include "platform.h"
#include "splinterdb/transaction.h"
#include "util.h"
#include "experimental_mode.h"
#include "splinterdb_internal.h"
#include "isketch/iceberg_table.h"
#include "lock_table.h"
#include "poison.h"

typedef struct transactional_data_config {
   data_config        super;
   const data_config *application_data_config;
} transactional_data_config;

typedef struct transactional_splinterdb_config {
   splinterdb_config           kvsb_cfg;
   transaction_isolation_level isol_level;
   transactional_data_config  *txn_data_cfg;
   bool                        is_upsert_disabled;
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   lock_table                      *lock_tbl;
} transactional_splinterdb;

typedef struct ONDISK timestamp_set {
   txn_timestamp wts;
   txn_timestamp rts;
} timestamp_set __attribute__((aligned(64)));

// read_set and write_set entry stored locally
typedef struct rw_entry {
   slice            key;
   message          msg; // value + op
   txn_timestamp    wts;
   txn_timestamp    rts;
   char             is_read;
   lock_table_entry lock;
} rw_entry;

typedef struct ONDISK tuple_header {
   timestamp_set ts;
   char          value[];
} tuple_header;

static inline bool
is_message_rts_update(message msg)
{
   return message_length(msg) == sizeof(txn_timestamp);
}

static inline bool
is_merge_accumulator_rts_update(merge_accumulator *ma)
{
   return merge_accumulator_length(ma) == sizeof(txn_timestamp);
}

static inline message
get_app_value_from_message(message msg)
{
   return message_create(
      message_class(msg),
      slice_create(message_length(msg) - sizeof(tuple_header),
                   message_data(msg) + sizeof(tuple_header)));
}

static inline message
get_app_value_from_merge_accumulator(merge_accumulator *ma)
{
   return message_create(
      merge_accumulator_message_class(ma),
      slice_create(merge_accumulator_length(ma) - sizeof(tuple_header),
                   merge_accumulator_data(ma) + sizeof(tuple_header)));
}

static int
merge_tictoc_tuple(const data_config *cfg,
                   slice              key,         // IN
                   message            old_message, // IN
                   merge_accumulator *new_message) // IN/OUT
{
   if (is_message_rts_update(old_message)) {
      // Just discard
      return 0;
   }

   if (is_merge_accumulator_rts_update(new_message)) {
      txn_timestamp new_rts =
         *((txn_timestamp *)merge_accumulator_data(new_message));
      merge_accumulator_copy_message(new_message, old_message);
      tuple_header *new_tuple =
         (tuple_header *)merge_accumulator_data(new_message);
      new_tuple->ts.rts = new_rts;

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
                            sizeof(tuple_header)
                               + merge_accumulator_length(&new_value_ma));

   tuple_header *new_tuple = merge_accumulator_data(new_message);
   memcpy(&new_tuple->value,
          merge_accumulator_data(&new_value_ma),
          merge_accumulator_length(&new_value_ma));

   merge_accumulator_deinit(&new_value_ma);

   merge_accumulator_set_class(new_message, message_class(old_message));

   return 0;
}

static int
merge_tictoc_tuple_final(const data_config *cfg,
                         slice              key,
                         merge_accumulator *oldest_message)
{
   platform_assert(!is_merge_accumulator_rts_update(oldest_message),
                   "oldest_message shouldn't be a rts update\n");

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
                            sizeof(tuple_header)
                               + merge_accumulator_length(&app_oldest_message));
   tuple_header *tuple = merge_accumulator_data(oldest_message);
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
   out_cfg->super.merge_tuples       = merge_tictoc_tuple;
   out_cfg->super.merge_tuples_final = merge_tictoc_tuple_final;
   out_cfg->application_data_config  = in_cfg;
}

static void
get_global_timestamps(transactional_splinterdb *txn_kvsb,
                      rw_entry                 *entry,
                      txn_timestamp            *wts,
                      txn_timestamp            *rts)
{
   const splinterdb *kvsb = txn_kvsb->kvsb;

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(kvsb, &result, 0, NULL);

   splinterdb_lookup(kvsb, entry->key, &result);

   if (splinterdb_lookup_found(&result)) {
      _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)&result;
      tuple_header *tuple = merge_accumulator_data(&_result->value);
      if (wts) {
         *wts = tuple->ts.wts;
      }
      if (rts) {
         *rts = tuple->ts.rts;
      }
   }
   splinterdb_lookup_result_deinit(&result);
}

static rw_entry *
rw_entry_create()
{
   rw_entry *new_entry;
   new_entry = TYPED_ZALLOC(0, new_entry);
   platform_assert(new_entry != NULL);
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
rw_entry_set_key(rw_entry *e, slice key, const data_config *cfg)
{
   char *key_buf;
   key_buf = TYPED_ARRAY_ZALLOC(0, key_buf, slice_length(key));
   memcpy(key_buf, slice_data(key), slice_length(key));
   e->key      = slice_create(slice_length(key), key_buf);
   e->lock.key = e->key;
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
   memcpy(
      msg_buf + sizeof(tuple_header), message_data(msg), message_length(msg));
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

   txn_splinterdb_cfg->txn_data_cfg =
      TYPED_ZALLOC(0, txn_splinterdb_cfg->txn_data_cfg);
   transactional_data_config_init(kvsb_cfg->data_cfg,
                                  txn_splinterdb_cfg->txn_data_cfg);
   txn_splinterdb_cfg->kvsb_cfg.data_cfg =
      (data_config *)txn_splinterdb_cfg->txn_data_cfg;

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
   _txn_kvsb->lock_tbl = lock_table_create(kvsb_cfg->data_cfg);
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
         commit_ts             = MAX(commit_ts, entry->wts);
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
      txn_timestamp rts = 0;
      get_global_timestamps(txn_kvsb, write_set[i], NULL, &rts);
      commit_ts = MAX(commit_ts, rts + 1);
   }

   bool is_abort = FALSE;
   for (uint64 i = 0; i < num_reads; ++i) {
      rw_entry *r = read_set[i];
      platform_assert(rw_entry_is_read(r));

      if (r->rts < commit_ts) {
         lock_table_rc lock_rc =
            lock_table_try_acquire_entry_lock(txn_kvsb->lock_tbl, &r->lock);

         txn_timestamp rts = 0;
         txn_timestamp wts = 0;
         get_global_timestamps(txn_kvsb, r, &wts, &rts);

         if (wts != r->wts) {
            if (lock_rc == LOCK_TABLE_RC_OK) {
               lock_table_release_entry_lock(txn_kvsb->lock_tbl, &r->lock);
            }
            is_abort = TRUE;
            break;
         }

         if (rts <= commit_ts && lock_rc == LOCK_TABLE_RC_BUSY) {
            is_abort = TRUE;
            break;
         }
         if (rts < commit_ts) {
            platform_assert(sizeof(commit_ts) == sizeof(uint32));
            splinterdb_update(txn_kvsb->kvsb,
                              r->key,
                              slice_create(sizeof(commit_ts), &commit_ts));
         }

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

         timestamp_set ts = {
            .wts = commit_ts,
            .rts = commit_ts,
         };
         tuple_header *msg = (tuple_header *)message_data(w->msg);
         memcpy(&msg->ts, &ts, sizeof(ts));

         //    lock_table_release_entry_lock(txn_kvsb->lock_tbl, w);
         // }

         // for (uint64 i = 0; i < num_writes; ++i) {
         //    rw_entry *w = write_set[i];
         //    platform_assert(rw_entry_is_write(w));

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
         lock_table_release_entry_lock(txn_kvsb->lock_tbl, &w->lock);
      }
   } else {
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
   /*    get_global_timestamps(txn_kvsb, entry, &entry->wts, &entry->rts); */
   /* } */

   if (message_is_null(entry->msg)) {
      rw_entry_set_msg(entry, msg);
   } else {
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

static int
non_transactional_splinterdb_insert(const splinterdb *kvsb,
                                    slice             key,
                                    slice             value)
{
   uint64 value_len = sizeof(tuple_header) + slice_length(value);
   char  *value_buf;
   value_buf = TYPED_ARRAY_ZALLOC(0, value_buf, value_len);
   memcpy(
      value_buf + sizeof(tuple_header), slice_data(value), slice_length(value));
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

   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;

   if (rw_entry_is_write(entry)) {
      get_global_timestamps(txn_kvsb, entry, &entry->wts, &entry->rts);

      // read my write
      // TODO This works for simple insert/update. However, it doesn't work
      // for upsert.
      // TODO if it succeeded, this read should not be considered for
      // validation. entry->is_read should be false.

      tuple_header *tuple = (tuple_header *)message_data(entry->msg);
      const size_t  value_len =
         message_length(entry->msg) - sizeof(tuple_header);
      merge_accumulator_resize(&_result->value, value_len);
      memcpy(merge_accumulator_data(&_result->value),
             tuple->value,
             merge_accumulator_length(&_result->value));

      return 0;
   }

   int rc = 0;
   /* timestamp_set v1, v2; */

   /* do { */
   /*    get_global_timestamps(txn_kvsb, entry, &v1.wts, &v1.rts); */
   /*    rc = splinterdb_lookup(txn_kvsb->kvsb, user_key, result); */
   /*    get_global_timestamps(txn_kvsb, entry, &v2.wts, &v2.rts); */
   /* } while (memcmp(&v1, &v2, sizeof(v1)) != 0 */
   /*          || lock_table_get_entry_lock_state(txn_kvsb->lock_tbl, entry) */
   /*                == LOCK_TABLE_RC_BUSY); */

   do {
      rc = splinterdb_lookup(txn_kvsb->kvsb, entry->key, result);
   } while (lock_table_get_entry_lock_state(txn_kvsb->lock_tbl, &entry->lock)
            == LOCK_TABLE_RC_BUSY);

   if (splinterdb_lookup_found(result)) {
      _result = (_splinterdb_lookup_result *)result;
      tuple_header *tuple =
         (tuple_header *)merge_accumulator_data(&_result->value);

      entry->wts = tuple->ts.wts;
      entry->rts = tuple->ts.rts;

      const size_t value_len =
         merge_accumulator_length(&_result->value) - sizeof(tuple_header);
      memmove(merge_accumulator_data(&_result->value), tuple->value, value_len);
      merge_accumulator_resize(&_result->value, value_len);
   } else {
      --txn->num_rw_entries;
      rw_entry_deinit(entry);
   }

   return rc;
}
