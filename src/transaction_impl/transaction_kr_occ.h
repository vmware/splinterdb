#pragma once

#include "splinterdb/transaction.h"
#include "splinterdb/public_platform.h"
#include "splinterdb/data.h"
#include "splinterdb_internal.h"
#include "data_internal.h"
#include "platform.h"
#include "platform_linux/poison.h"

#define TRANSACTION_RW_SET_MAX 64

typedef struct transaction_internal transaction_internal;

typedef struct transaction_rw_set_entry {
   slice   key;
   message msg;
} transaction_rw_set_entry;

typedef struct transaction_table {
   transaction_internal *head;
   transaction_internal *tail;
} transaction_table;

typedef struct transaction_internal {
   timestamp tn;

   transaction_rw_set_entry rs[TRANSACTION_RW_SET_MAX];
   transaction_rw_set_entry ws[TRANSACTION_RW_SET_MAX];

   uint64 rs_size;
   uint64 ws_size;

#ifdef EXPERIMENTAL_MODE_KR_OCC_PARALLEL
   transaction_table finish_active_transactions;
#endif

   transaction_internal *next;

   transaction_internal *start_txn;

   int64 ref_count;
} transaction_internal;

typedef struct transactional_splinterdb_config {
   splinterdb_config           kvsb_cfg;
   transaction_isolation_level isol_level;
   bool                        is_upsert_disabled;
} transactional_splinterdb_config;

typedef struct atomic_counter {
   uint64 num;
} atomic_counter;

void
atomic_counter_init(atomic_counter *counter)
{
   counter->num = 1;
}

void
atomic_counter_deinit(atomic_counter *counter)
{
}

uint64
atomic_counter_get_next(atomic_counter *counter)
{
   return __sync_add_and_fetch(&counter->num, 1);
}

uint64
atomic_counter_get_current(atomic_counter *counter)
{
   return counter->num;
}

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   atomic_counter                   ts_allocator;
   platform_mutex                   lock;
   transaction_table                all_transactions;
#ifdef EXPERIMENTAL_MODE_KR_OCC_PARALLEL
   transaction_table active_transactions;
#endif
} transactional_splinterdb;


static uint64 gc_count = 0;

void
transaction_internal_create(transaction_internal **new_internal)
{
   transaction_internal *txn_internal;
   txn_internal          = TYPED_ZALLOC(0, txn_internal);
   txn_internal->tn      = 0;
   txn_internal->ws_size = txn_internal->rs_size = 0;
   txn_internal->next                            = NULL;
   txn_internal->start_txn                       = NULL;
   txn_internal->ref_count                       = 0;

   *new_internal = txn_internal;
}

void
transaction_internal_create_by_copy(transaction_internal      **new_internal,
                                    const transaction_internal *other)
{
   transaction_internal *txn_internal;
   txn_internal            = TYPED_ZALLOC(0, txn_internal);
   txn_internal->tn        = other->tn;
   txn_internal->ws_size   = other->ws_size;
   txn_internal->rs_size   = other->rs_size;
   txn_internal->next      = NULL;
   txn_internal->start_txn = other->start_txn;
   txn_internal->ref_count = other->ref_count;

   for (uint64 i = 0; i < other->ws_size; ++i) {
      void *key;
      key = TYPED_ALIGNED_ZALLOC(0, 64, key, slice_length(other->ws[i].key));
      memmove(
         key, slice_data(other->ws[i].key), slice_length(other->ws[i].key));
      txn_internal->ws[i].key =
         slice_create(slice_length(other->ws[i].key), key);

      void *msg;
      msg = TYPED_ALIGNED_ZALLOC(0, 64, msg, message_length(other->ws[i].msg));
      memmove(
         msg, message_data(other->ws[i].msg), message_length(other->ws[i].msg));
      txn_internal->ws[i].msg =
         message_create(message_class(other->ws[i].msg),
                        slice_create(message_length(other->ws[i].msg), msg));
   }

   for (uint64 i = 0; i < other->rs_size; ++i) {
      void *key;
      key = TYPED_ALIGNED_ZALLOC(0, 64, key, slice_length(other->rs[i].key));
      memmove(
         key, slice_data(other->rs[i].key), slice_length(other->rs[i].key));
      txn_internal->rs[i].key =
         slice_create(slice_length(other->rs[i].key), key);
   }

   *new_internal = txn_internal;
}

void
transaction_internal_destroy(transaction_internal **internal_to_delete)
{
   transaction_internal *txn_internal = *internal_to_delete;

   for (uint64 i = 0; i < txn_internal->ws_size; ++i) {
      void *to_delete = (void *)slice_data(txn_internal->ws[i].key);
      platform_free(0, to_delete);
      to_delete = (void *)message_data(txn_internal->ws[i].msg);
      platform_free(0, to_delete);
   }

   for (uint64 i = 0; i < txn_internal->rs_size; ++i) {
      void *to_delete = (void *)slice_data(txn_internal->rs[i].key);
      platform_free(0, to_delete);
   }

   platform_free(0, *internal_to_delete);
   *internal_to_delete = NULL;
}

void
transaction_table_init(transaction_table *transactions)
{
   transaction_internal_create(&transactions->head);
   transactions->tail = transactions->head;
}

void
transaction_table_insert(transaction_table    *transactions,
                         transaction_internal *txn)
{
   transactions->tail->next = txn;
   transactions->tail       = txn;
   transactions->tail->next = NULL;
}

void *
transaction_table_insert_by_copy(transaction_table          *transactions,
                                 const transaction_internal *original)
{
   transaction_internal *copy;
   transaction_internal_create_by_copy(&copy, original);
   transaction_table_insert(transactions, copy);

   return copy;
}

void
transaction_table_copy(transaction_table       *transactions,
                       const transaction_table *other)
{
   transaction_internal *other_curr = other->head->next;
   while (other_curr) {
      transaction_table_insert_by_copy(transactions, other_curr);
      other_curr = other_curr->next;
   }
}

void
transaction_table_init_from_table(transaction_table       *transactions,
                                  const transaction_table *other)
{
   transaction_table_init(transactions);
   transaction_table_copy(transactions, other);
}

void
transaction_table_deinit(transaction_table *transactions)
{
   transaction_internal *curr = transactions->head->next;
   while (curr) {
      transactions->head->next = curr->next;
      transaction_internal_destroy(&curr);
      curr = transactions->head->next;
   }
   transaction_internal_destroy(&transactions->head);
   transactions->tail = NULL;
}

void
transaction_table_delete(transaction_table    *transactions,
                         transaction_internal *txn,
                         bool                  should_free)
{
   transaction_internal *curr = transactions->head->next;
   transaction_internal *prev = transactions->head;

   while (curr) {
      if (curr == txn) {
         prev->next = curr->next;
         if (prev->next == NULL) {
            transactions->tail = prev;
         }

         if (should_free) {
            transaction_internal_destroy(&txn);
         }

         return;
      }

      prev = curr;
      curr = curr->next;
   }
}


// For debug purpose
void
transaction_internal_print(transaction_internal *txn_internal)
{
   platform_error_log("[%p] ---\n", txn_internal);
   platform_error_log("tn: %lu\n", txn_internal->tn);
   platform_error_log("ws_size: %lu\n", txn_internal->ws_size);
   platform_error_log("rs_size: %lu\n", txn_internal->rs_size);
   platform_error_log("start_txn: %p\n", txn_internal->start_txn);
   platform_error_log("ref_count: %lu\n", txn_internal->ref_count);
   platform_error_log("next: %p\n", txn_internal->next);
}

// For debug purpose
void
transaction_table_dump(transaction_table *transactions)
{
   transaction_internal *curr = transactions->head->next;
   while (curr) {
      transaction_internal_print(curr);
      curr = curr->next;
   }
}

bool
transaction_check_for_conflict(transaction_table    *transactions,
                               transaction_internal *txn,
                               const data_config    *cfg)
{
   transaction_internal *txn_committed = txn->start_txn->next;
   while (txn_committed) {
      for (uint64 i = 0; i < txn->rs_size; ++i) {
         for (uint64 j = 0; j < txn_committed->ws_size; ++j) {
            key rkey = key_create_from_slice(txn->rs[i].key);
            key wkey = key_create_from_slice(txn_committed->ws[j].key);
            if (data_key_compare(cfg, rkey, wkey) == 0) {
               return FALSE;
            }
         }
      }
      txn_committed = txn_committed->next;
   }

   return TRUE;
}

#ifdef EXPERIMENTAL_MODE_KR_OCC_PARALLEL
// TODO: rename to the general
bool
transaction_check_for_conflict_with_active_transactions(
   transaction_internal *txn,
   const data_config    *cfg)
{
   transaction_internal *txn_i = txn->finish_active_transactions.head->next;
   while (txn_i) {
      for (uint64 i = 0; i < txn_i->ws_size; ++i) {
         key wkey = key_create_from_slice(txn_i->ws[i].key);
         for (uint64 j = 0; j < txn->rs_size; ++j) {
            key txn_rkey = key_create_from_slice(txn->rs[j].key);
            if (data_key_compare(cfg, wkey, txn_rkey) == 0) {
               return FALSE;
            }
         }

         for (uint64 j = 0; j < txn->ws_size; ++j) {
            key txn_wkey = key_create_from_slice(txn->ws[j].key);
            if (data_key_compare(cfg, wkey, txn_wkey) == 0) {
               return FALSE;
            }
         }
      }

      txn_i = txn_i->next;
   }

   return TRUE;
}

#endif

static void
transaction_table_delete_after(transaction_table    *transactions,
                               transaction_internal *txn)
{
   transaction_internal *curr = txn->next;
   if (curr == NULL) {
      return;
   }

   txn->next = curr->next;
   if (curr == transactions->tail) {
      transactions->tail = txn;
   }
   transaction_internal_destroy(&curr);
}

void
transaction_gc(transactional_splinterdb *txn_kvsb)
{
   if (txn_kvsb->all_transactions.head->ref_count > 0) {
      return;
   }

   transaction_internal *head = txn_kvsb->all_transactions.head;
   transaction_internal *curr = head->next;
   while (curr) {
      platform_assert(curr->ref_count >= 0);

      // transaction_table_dump(&txn_kvsb->all_transactions);

      if (curr->ref_count == 0) {
         transaction_table_delete_after(&txn_kvsb->all_transactions, head);

         ++gc_count;
      } else {
         break;
      }

      curr = head->next;
   }

   // platform_default_log("current gc_count: %lu\n", gc_count);
}


static int
transactional_splinterdb_create_or_open(const splinterdb_config   *kvsb_cfg,
                                        transactional_splinterdb **txn_kvsb,
                                        bool open_existing)
{
   transactional_splinterdb_config *txn_splinterdb_cfg;
   txn_splinterdb_cfg = TYPED_ZALLOC(0, txn_splinterdb_cfg);
   memcpy(txn_splinterdb_cfg, kvsb_cfg, sizeof(txn_splinterdb_cfg->kvsb_cfg));
   txn_splinterdb_cfg->is_upsert_disabled = FALSE;

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

   atomic_counter_init(&_txn_kvsb->ts_allocator);
   platform_mutex_init(&_txn_kvsb->lock, 0, 0);
   transaction_table_init(&_txn_kvsb->all_transactions);
#ifdef EXPERIMENTAL_MODE_KR_OCC_PARALLEL
   transaction_table_init(&_txn_kvsb->active_transactions);
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

   platform_mutex_destroy(&_txn_kvsb->lock);
#ifdef EXPERIMENTAL_MODE_KR_OCC_PARALLEL
   transaction_table_deinit(&_txn_kvsb->active_transactions);
#endif
   transaction_table_deinit(&_txn_kvsb->all_transactions);
   atomic_counter_deinit(&_txn_kvsb->ts_allocator);

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
   // Initialize the given transaction
   transaction_internal *txn_internal;
   transaction_internal_create(&txn_internal);

   platform_mutex_lock(&txn_kvsb->lock);
   txn_internal->start_txn = txn_kvsb->all_transactions.tail;
   ++txn_internal->start_txn->ref_count;
   platform_mutex_unlock(&txn_kvsb->lock);

   txn->internal = txn_internal;

   return 0;
}

static void
write_into_splinterdb(transactional_splinterdb *txn_kvsb,
                      transaction_internal     *txn_internal)
{
   int rc = 0;

   // Write all elements in txn->ws
   for (int i = 0; i < txn_internal->ws_size; ++i) {
      switch (message_class(txn_internal->ws[i].msg)) {
         case MESSAGE_TYPE_INSERT:
            rc = splinterdb_insert(txn_kvsb->kvsb,
                                   txn_internal->ws[i].key,
                                   message_slice(txn_internal->ws[i].msg));
            platform_assert(rc == 0);
            break;
         case MESSAGE_TYPE_UPDATE:
            rc = splinterdb_update(txn_kvsb->kvsb,
                                   txn_internal->ws[i].key,
                                   message_slice(txn_internal->ws[i].msg));
            platform_assert(rc == 0);
            break;
         case MESSAGE_TYPE_DELETE:
            rc = splinterdb_delete(txn_kvsb->kvsb, txn_internal->ws[i].key);
            platform_assert(rc == 0);
            break;
         default:
            platform_assert(0, "invalid operation");
      }
   }
}

int
transactional_splinterdb_commit(transactional_splinterdb *txn_kvsb,
                                transaction              *txn)
{
   transaction_internal *txn_internal = txn->internal;
   platform_assert(txn_internal != NULL);

#ifdef EXPERIMENTAL_MODE_KR_OCC_PARALLEL
   platform_mutex_lock(&txn_kvsb->lock);

   transaction_table_init_from_table(&txn_internal->finish_active_transactions,
                                     &txn_kvsb->active_transactions);
   void *txn_copy = transaction_table_insert_by_copy(
      &txn_kvsb->active_transactions, txn_internal);

   platform_mutex_unlock(&txn_kvsb->lock);

   bool valid =
      transaction_check_for_conflict(&txn_kvsb->all_transactions,
                                     txn_internal,
                                     txn_kvsb->tcfg->kvsb_cfg.data_cfg);
   if (valid) {
      valid = transaction_check_for_conflict_with_active_transactions(
         txn_internal, txn_kvsb->tcfg->kvsb_cfg.data_cfg);
   }

   transaction_table_deinit(&txn_internal->finish_active_transactions);

   if (valid) {
      // The new transaction finally becomes visible globally
      write_into_splinterdb(txn_kvsb, txn_internal);

      platform_mutex_lock(&txn_kvsb->lock);

      txn_internal->tn = atomic_counter_get_next(&txn_kvsb->ts_allocator);
      transaction_table_insert(&txn_kvsb->all_transactions, txn_internal);
      transaction_table_delete(&txn_kvsb->active_transactions, txn_copy, TRUE);

      // FIXME: use atomic operation
      --txn_internal->start_txn->ref_count;

      transaction_gc(txn_kvsb);

      platform_mutex_unlock(&txn_kvsb->lock);
   } else {
      platform_mutex_lock(&txn_kvsb->lock);

      transaction_table_delete(&txn_kvsb->active_transactions, txn_copy, TRUE);

      // FIXME: use atomic operation
      --txn_internal->start_txn->ref_count;

      platform_mutex_unlock(&txn_kvsb->lock);

      transaction_internal_destroy((transaction_internal **)&txn->internal);

      return -1;
   }

   return 0;
#else
   platform_mutex_lock(&txn_kvsb->lock);

   bool valid =
      transaction_check_for_conflict(&txn_kvsb->all_transactions,
                                     txn_internal,
                                     txn_kvsb->tcfg->kvsb_cfg.data_cfg);

   if (valid) {
      // The new transaction finally becomes visible globally
      write_into_splinterdb(txn_kvsb, txn_internal);

      txn_internal->tn = atomic_counter_get_next(&txn_kvsb->ts_allocator);
      transaction_table_insert(&txn_kvsb->all_transactions, txn_internal);
   }

   // FIXME: use atomic operation
   --txn_internal->start_txn->ref_count;

   transaction_gc(txn_kvsb);

   // transaction_table_delete(&txn_kvsb->active_transactions, txn_internal);

   platform_mutex_unlock(&txn_kvsb->lock);

   if (!valid) {
      transaction_internal_destroy((transaction_internal **)&txn->internal);
      return -1;
   }

   // TODO: Garbage collection

   return 0;
#endif
}

int
transactional_splinterdb_abort(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   platform_assert(txn->internal != NULL);

   transaction_internal_destroy((transaction_internal **)&txn->internal);

   return 0;
}

static void
insert_into_write_set(transaction_internal *txn_internal,
                      slice                 user_key,
                      message_type          op,
                      slice                 value,
                      const data_config    *cfg)
{
   key ukey = key_create_from_slice(user_key);

   // check if there is the same key in its write set
   for (uint64 i = 0; i < txn_internal->ws_size; ++i) {
      key wkey = key_create_from_slice(txn_internal->ws[i].key);
      if (data_key_compare(cfg, ukey, wkey) == 0) {
         if (op == MESSAGE_TYPE_INSERT) {
            void *old_msg_data = (void *)message_data(txn_internal->ws[i].msg);
            platform_free(0, old_msg_data);

            char *new_msg_data;
            new_msg_data =
               TYPED_ARRAY_ZALLOC(0, new_msg_data, slice_length(value));
            memcpy(new_msg_data, slice_data(value), slice_length(value));

            txn_internal->ws[i].msg = message_create(
               op, slice_create(slice_length(value), new_msg_data));
         } else if (op == MESSAGE_TYPE_DELETE) {
            txn_internal->ws[i].msg = DELETE_MESSAGE;
         } else if (op == MESSAGE_TYPE_UPDATE) {
            merge_accumulator new_msg;
            merge_accumulator_init_from_message(
               &new_msg, 0, message_create(op, value));

            data_merge_tuples(cfg, ukey, txn_internal->ws[i].msg, &new_msg);

            void *old_msg_data = (void *)message_data(txn_internal->ws[i].msg);
            platform_free(0, old_msg_data);

            char *new_msg_data;
            new_msg_data = TYPED_ARRAY_ZALLOC(
               0, new_msg_data, merge_accumulator_length(&new_msg));
            memcpy(new_msg_data,
                   merge_accumulator_data(&new_msg),
                   merge_accumulator_length(&new_msg));

            txn_internal->ws[i].msg = message_create(
               op,
               slice_create(merge_accumulator_length(&new_msg), new_msg_data));

            merge_accumulator_deinit(&new_msg);
         }

         return;
      }
   }


   char *key_buf;
   key_buf = TYPED_ARRAY_ZALLOC(0, key_buf, slice_length(user_key));
   memcpy(key_buf, slice_data(user_key), slice_length(user_key));
   txn_internal->ws[txn_internal->ws_size].key =
      slice_create(slice_length(user_key), key_buf);

   if (op == MESSAGE_TYPE_DELETE) {
      txn_internal->ws[txn_internal->ws_size].msg = DELETE_MESSAGE;
   } else {
      char *value_buf;
      value_buf = TYPED_ARRAY_ZALLOC(0, value_buf, slice_length(value));
      memcpy(value_buf, slice_data(value), slice_length(value));
      txn_internal->ws[txn_internal->ws_size].msg =
         message_create(op, slice_create(slice_length(value), value_buf));
   }

   ++txn_internal->ws_size;
}

static void
insert_into_read_set(transaction_internal *txn_internal, slice key)
{
   char *key_buf;
   key_buf = TYPED_ARRAY_ZALLOC(0, key_buf, slice_length(key));
   memcpy(key_buf, slice_data(key), slice_length(key));
   txn_internal->rs[txn_internal->rs_size].key =
      slice_create(slice_length(key), key_buf);

   ++txn_internal->rs_size;
}

int
transactional_splinterdb_insert(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                slice                     value)
{
   // Call non-transactional insertion for YCSB loading..
   if (txn == NULL) {
      return splinterdb_insert(txn_kvsb->kvsb, user_key, value);
   }

   transaction_internal *txn_internal = txn->internal;
   platform_assert(txn_internal != NULL);

   insert_into_write_set(txn_internal,
                         user_key,
                         MESSAGE_TYPE_INSERT,
                         value,
                         txn_kvsb->tcfg->kvsb_cfg.data_cfg);

   return 0;
}

int
transactional_splinterdb_delete(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key)
{
   transaction_internal *txn_internal = txn->internal;
   platform_assert(txn_internal != NULL);

   insert_into_write_set(txn_internal,
                         user_key,
                         MESSAGE_TYPE_DELETE,
                         NULL_SLICE,
                         txn_kvsb->tcfg->kvsb_cfg.data_cfg);

   return 0;
}

int
transactional_splinterdb_update(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                slice                     delta)
{
   transaction_internal *txn_internal = txn->internal;
   platform_assert(txn_internal != NULL);

   message_type op = MESSAGE_TYPE_UPDATE;
   if (txn_kvsb->tcfg->is_upsert_disabled) {
      op = MESSAGE_TYPE_INSERT;
   }

   insert_into_write_set(
      txn_internal, user_key, op, delta, txn_kvsb->tcfg->kvsb_cfg.data_cfg);

   return 0;
}

int
transactional_splinterdb_lookup(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                splinterdb_lookup_result *result)
{
   transaction_internal *txn_internal = txn->internal;
   platform_assert(txn_internal != NULL);

   key ukey = key_create_from_slice(user_key);

   // Support read a value within its write set, which may not be committed
   for (int i = 0; i < txn_internal->ws_size; ++i) {
      key wkey = key_create_from_slice(txn_internal->ws[i].key);
      if (data_key_compare(txn_kvsb->tcfg->kvsb_cfg.data_cfg, ukey, wkey) == 0)
      {
         _splinterdb_lookup_result *_result =
            (_splinterdb_lookup_result *)result;
         merge_accumulator_copy_message(&_result->value,
                                        txn_internal->ws[i].msg);

         insert_into_read_set(txn_internal, user_key);

         return 0;
      }
   }

   int rc = splinterdb_lookup(txn_kvsb->kvsb, user_key, result);

   if (splinterdb_lookup_found(result)) {
      insert_into_read_set(txn_internal, user_key);
   }

   return rc;
}
