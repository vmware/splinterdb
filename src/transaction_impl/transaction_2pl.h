#pragma once

#include "splinterdb/data.h"
#include "platform.h"
#include "data_internal.h"
#include "splinterdb/transaction.h"
#include "util.h"
#include "experimental_mode.h"
#include "splinterdb_internal.h"
#include "isketch/iceberg_table.h"
#include "poison.h"


/*
 * Implements a lock table that uses READ/WRITE locks and 3 locking policies:
 * NO_WAIT, WAIT-DIE, and WOUND-WAIT
 */

#define LOCK_TABLE_DEBUG   0
#define WOUND_WAIT_TIMEOUT 10

// The lock table is just a hash map
typedef struct lock_table_2pl {
   iceberg_table table;
} lock_table_2pl;

typedef enum lock_type {
   READ_LOCK = 0, // shared lock
   WRITE_LOCK     // exclusive lock
} lock_type;

typedef struct lock_req {
   lock_type        lt;
   transaction     *txn;  // access to transaction ts as well
   struct lock_req *next; // to form a linked list
} lock_req;

// Each lock_entry in this lock table contains some certain state required to
// implement the chosen locking policy
typedef struct lock_entry {
#if EXPERIMENTAL_MODE_2PL_NO_WAIT == 1
   platform_mutex latch;
#endif
   lock_req *owners;
#if EXPERIMENTAL_MODE_2PL_WOUND_WAIT == 1 || EXPERIMENTAL_MODE_2PL_WAIT_DIE == 1
   platform_condvar condvar;
#endif
} lock_entry;

typedef struct rw_entry {
   slice       key;
   message     msg; // value + op
   lock_entry *le;
} rw_entry;

typedef enum lock_table_2pl_rc {
   LOCK_TABLE_2PL_RC_INVALID = 0,
   LOCK_TABLE_2PL_RC_OK,
   LOCK_TABLE_2PL_RC_BUSY,
   LOCK_TABLE_2PL_RC_DEADLK,
   LOCK_TABLE_2PL_RC_NODATA
} lock_table_2pl_rc;


lock_table_2pl *
lock_table_2pl_create(const data_config *spl_data_config)
{
   lock_table_2pl *lt;
   lt                  = TYPED_ZALLOC(0, lt);
   iceberg_config icfg = {0};
   iceberg_config_default_init(&icfg);
   icfg.log_slots = 20;
   iceberg_init(&lt->table, &icfg, spl_data_config);
   return lt;
}

void
lock_table_2pl_destroy(lock_table_2pl *lock_tbl)
{
   platform_free(0, lock_tbl);
}

static inline threadid
get_tid()
{
   return platform_get_tid();
}

#if EXPERIMENTAL_MODE_2PL_NO_WAIT == 1 || EXPERIMENTAL_MODE_2PL_WAIT_DIE == 1  \
   || EXPERIMENTAL_MODE_2PL_WOUND_WAIT == 1

static inline lock_req *
get_lock_req(lock_type lt, transaction *txn)
{
   lock_req *lreq;
   lreq       = TYPED_ZALLOC(0, lreq);
   lreq->next = NULL;
   lreq->lt   = lt;
   lreq->txn  = txn;
   return lreq;
}
#endif

//*******************************************
#if EXPERIMENTAL_MODE_2PL_NO_WAIT == 1
//*******************************************
// we're not using pthread_rw_lock because it does not
// support upgrading from shared to exclusive
lock_entry *
lock_entry_init()
{
   lock_entry *le;
   le = TYPED_ZALLOC(0, le);
   platform_mutex_init(&le->latch, 0, 0);
   return le;
}

void
lock_entry_destroy(lock_entry *le)
{
   platform_mutex_destroy(&le->latch);
   platform_free(0, le);
}

lock_table_2pl_rc
_lock(lock_entry *le, lock_type lt, transaction *txn)
{

   platform_mutex_lock(&le->latch);

   if (le->owners == NULL) {
      // we need to create a new lock_req and obtain the lock
      le->owners = get_lock_req(lt, txn);
      platform_mutex_unlock(&le->latch);
      return LOCK_TABLE_2PL_RC_OK;
   }

   lock_req *iter = le->owners;

   if (iter->lt == WRITE_LOCK) {
      platform_assert(iter->next == NULL,
                      "More than one owners holding an exclusive lock");
      if (iter->txn->ts != txn->ts) {
         // another writer holding the lock
         platform_mutex_unlock(&le->latch);
         return LOCK_TABLE_2PL_RC_BUSY;
      } else {
         // we already hold an exclusive lock
         platform_mutex_unlock(&le->latch);
         return LOCK_TABLE_2PL_RC_OK;
      }
   } else if (lt == WRITE_LOCK) {
      if (iter->txn->ts == txn->ts && iter->next == NULL) {
         // we can upgrade the shared lock which we are
         // already exclusively holding
         iter->lt = WRITE_LOCK;
         platform_mutex_unlock(&le->latch);
         return LOCK_TABLE_2PL_RC_OK;
      } else {
         // some reader is holding the lock,
         // but we want exclusive access
         platform_mutex_unlock(&le->latch);
         return LOCK_TABLE_2PL_RC_BUSY;
      }
   } else if (lt == READ_LOCK) {
      // we keep owners sorted in ts descending order
      lock_req *prev = NULL;
      while (iter && iter->txn->ts > txn->ts) {
         prev = iter;
         iter = iter->next;
      }
      if (iter && iter->txn->ts == txn->ts) {
         // we already hold the lock
         platform_mutex_unlock(&le->latch);
         return LOCK_TABLE_2PL_RC_OK;
      }
      lock_req *lr = get_lock_req(lt, txn);
      lr->next     = iter;
      if (prev != NULL)
         prev->next = lr;
      else
         le->owners = lr;
      platform_mutex_unlock(&le->latch);
      return LOCK_TABLE_2PL_RC_OK;
   }

   // Should not get here
   platform_assert(false, "Dead code branch");
   platform_mutex_unlock(&le->latch);
   return LOCK_TABLE_2PL_RC_OK;
}

lock_table_2pl_rc
_unlock(lock_entry *le, lock_type lt, transaction *txn)
{

   platform_mutex_lock(&le->latch);
   lock_req *iter = le->owners;
   lock_req *prev = NULL;

   while (iter != NULL) {
      if (iter->txn->ts == txn->ts) {
         if (iter->lt == lt) {
            // request is valid, release the lock
            if (prev != NULL) {
               prev->next = iter->next;
            } else {
               le->owners = iter->next;
            }
            platform_free(0, iter);
            platform_mutex_unlock(&le->latch);
            return LOCK_TABLE_2PL_RC_OK;
         } else {
            platform_mutex_unlock(&le->latch);
            return LOCK_TABLE_2PL_RC_INVALID;
         }
      }
      prev = iter;
      iter = iter->next;
   }

   platform_mutex_unlock(&le->latch);
   return LOCK_TABLE_2PL_RC_NODATA;
}

//*******************************************
#elif EXPERIMENTAL_MODE_2PL_WAIT_DIE == 1
//*******************************************
lock_entry *
lock_entry_init()
{
   lock_entry *le;
   le = TYPED_ZALLOC(0, le);
   platform_condvar_init(&le->condvar, 0);
   return le;
}

void
lock_entry_destroy(lock_entry *le)
{
   platform_condvar_destroy(&le->condvar);
   platform_free(0, le);
}

lock_table_2pl_rc
_lock(lock_entry *le, lock_type lt, transaction *txn)
{
   // owners are sorted by ts such that the
   // oldest owner (with the smallest ts) is the first
   platform_condvar_lock(&le->condvar);
   while (true) {
      if (le->owners == NULL) {
         // we need to create a new lock_req and obtain the lock
         le->owners = get_lock_req(lt, txn);
         platform_condvar_unlock(&le->condvar);
         return LOCK_TABLE_2PL_RC_OK;
      }

      lock_req *iter = le->owners;

      if (iter->lt == WRITE_LOCK) {
         platform_assert(iter->next == NULL,
                         "More than one owners holding an exclusive lock");
         if (iter->txn->ts != txn->ts) {
            // another writer holding the lock
            if (iter->txn->ts < txn->ts) {
               platform_condvar_unlock(&le->condvar);
               return LOCK_TABLE_2PL_RC_BUSY;
            }
         } else {
            // we already hold an exclusive lock
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_2PL_RC_OK;
         }
      } else if (lt == WRITE_LOCK) {
         if (iter->txn->ts == txn->ts && iter->next == NULL) {
            // we can upgrade the shared lock which we are
            // already exclusively holding
            iter->lt = WRITE_LOCK;
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_2PL_RC_OK;
         } else {
            // some reader is holding the lock,
            // but we want exclusive access
            // if all readers are older, we die
            if (iter->txn->ts < txn->ts) {
               platform_condvar_unlock(&le->condvar);
               return LOCK_TABLE_2PL_RC_BUSY;
            }
         }
      } else if (lt == READ_LOCK) {
         // we keep owners sorted in ts ascending order
         lock_req *prev = NULL;
         while (iter && iter->txn->ts < txn->ts) {
            prev = iter;
            iter = iter->next;
         }
         if (iter && iter->txn->ts == txn->ts) {
            // we already hold the lock
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_2PL_RC_OK;
         }
         lock_req *lr = get_lock_req(lt, txn);
         lr->next     = iter;
         if (prev != NULL)
            prev->next = lr;
         else
            le->owners = lr;
         platform_condvar_unlock(&le->condvar);
         return LOCK_TABLE_2PL_RC_OK;
      }

      platform_condvar_wait(&le->condvar);
   }

   // Should not get here
   platform_assert(false, "Dead code branch");
   platform_condvar_unlock(&le->condvar);
   return LOCK_TABLE_2PL_RC_OK;
}

lock_table_2pl_rc
_unlock(lock_entry *le, lock_type lt, transaction *txn)
{

   platform_condvar_lock(&le->condvar);
   lock_req *iter = le->owners;
   lock_req *prev = NULL;

   while (iter != NULL) {
      if (iter->txn->ts == txn->ts) {
         if (iter->lt == lt) {
            // request is valid, release the lock
            if (prev != NULL) {
               prev->next = iter->next;
            } else {
               le->owners = iter->next;
            }
            platform_free(0, iter);
            platform_condvar_broadcast(&le->condvar);
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_2PL_RC_OK;
         } else {
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_2PL_RC_INVALID;
         }
      }
      prev = iter;
      iter = iter->next;
   }

   platform_condvar_unlock(&le->condvar);
   return LOCK_TABLE_2PL_RC_NODATA;
}


#elif EXPERIMENTAL_MODE_2PL_WOUND_WAIT == 1
lock_entry *
lock_entry_init()
{
   lock_entry *le;
   le = TYPED_ZALLOC(0, le);
   platform_condvar_init(&le->condvar, 0);
   return le;
}

void
lock_entry_destroy(lock_entry *le)
{
   platform_condvar_destroy(&le->condvar);
   platform_free(0, le);
}

lock_table_2pl_rc
_lock(lock_entry *le, lock_type lt, transaction *txn)
{
   platform_condvar_lock(&le->condvar);
   while (true) {
      if (txn->wounded) {
         platform_condvar_unlock(&le->condvar);
         return LOCK_TABLE_2PL_RC_BUSY;
      }

      if (le->owners == NULL) {
         // we need to create a new lock_req and obtain the lock
         le->owners = get_lock_req(lt, txn);
         platform_condvar_unlock(&le->condvar);
         return LOCK_TABLE_2PL_RC_OK;
      }

      lock_req *iter = le->owners;

      if (iter->lt == WRITE_LOCK) {
         platform_assert(iter->next == NULL,
                         "More than one owners holding an exclusive lock");
         if (iter->txn->ts != txn->ts) {
            // another writer holding the lock
            if (iter->txn->ts > txn->ts) {
               // wound the exclusive owner
               iter->txn->wounded = true;
            }
         } else {
            // we already hold an exclusive lock
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_2PL_RC_OK;
         }
      } else if (lt == WRITE_LOCK) {
         if (iter->txn->ts == txn->ts && iter->next == NULL) {
            // we can upgrade the shared lock which we are
            // already exclusively holding
            iter->lt = WRITE_LOCK;
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_2PL_RC_OK;
         } else {
            // wound all younger readers (i.e., with ts > txn->ts)
            while (iter && iter->txn->ts > txn->ts) {
               // lazy wound; txn aborts on the next lock attempt
               iter->txn->wounded = true;
               iter               = iter->next;
            }
         }
      } else if (lt == READ_LOCK) {
         // we keep owners sorted in ts descending order
         lock_req *prev = NULL;
         while (iter && iter->txn->ts > txn->ts) {
            prev = iter;
            iter = iter->next;
         }
         if (iter && iter->txn->ts == txn->ts) {
            // we already hold the lock
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_2PL_RC_OK;
         }
         lock_req *lr = get_lock_req(lt, txn);
         lr->next     = iter;
         if (prev != NULL)
            prev->next = lr;
         else
            le->owners = lr;
         platform_condvar_unlock(&le->condvar);
         return LOCK_TABLE_2PL_RC_OK;
      }
      platform_condvar_timedwait(&le->condvar, WOUND_WAIT_TIMEOUT);
   }

   // Should not get here
   platform_assert(false, "Dead code branch");
   platform_condvar_unlock(&le->condvar);
   return LOCK_TABLE_2PL_RC_OK;
}

lock_table_2pl_rc
_unlock(lock_entry *le, lock_type lt, transaction *txn)
{
   platform_condvar_lock(&le->condvar);
   lock_req *iter = le->owners;
   lock_req *prev = NULL;

   while (iter != NULL) {
      if (iter->txn->ts == txn->ts) {
         if (iter->lt == lt) {
            // request is valid, release the lock
            if (prev != NULL) {
               prev->next = iter->next;
            } else {
               le->owners = iter->next;
            }
            platform_free(0, iter);
            platform_condvar_broadcast(&le->condvar);
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_2PL_RC_OK;
         } else {
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_2PL_RC_INVALID;
         }
      }
      prev = iter;
      iter = iter->next;
   }

   platform_condvar_unlock(&le->condvar);
   return LOCK_TABLE_2PL_RC_NODATA;
}
#else

lock_entry *
lock_entry_init()
{
   platform_assert(FALSE, "Not implemented");
   return NULL;
}

void
lock_entry_destroy(lock_entry *le)
{
   platform_assert(FALSE, "Not implemented");
}

lock_table_2pl_rc
_lock(lock_entry *le, lock_type lt, transaction *txn)
{
   platform_assert(FALSE, "Not implemented");
   return 0;
}

lock_table_2pl_rc
_unlock(lock_entry *le, lock_type lt, transaction *txn)
{
   platform_assert(FALSE, "Not implemented");
   return 0;
}

#endif

lock_table_2pl_rc
lock_table_2pl_try_acquire_entry_lock(lock_table_2pl *lock_tbl,
                                      rw_entry       *entry,
                                      lock_type       lt,
                                      transaction    *txn)
{
   if (entry->le) {
      // we already have a pointer to the lock status
      return _lock(entry->le, lt, txn);
   }

   // else we either get a pointer to an existing lock status
   // or create a new one
   entry->le = lock_entry_init();

   ValueType  value_to_be_inserted     = (ValueType)entry->le;
   ValueType *pointer_of_iceberg_value = &value_to_be_inserted;
   bool       is_newly_inserted =
      iceberg_insert_and_get(&lock_tbl->table,
                             &entry->key,
                             (ValueType **)&pointer_of_iceberg_value,
                             get_tid());
   if (!is_newly_inserted) {
      // there's already a lock_entry for this key in the lock_table
      lock_entry_destroy(entry->le);
      entry->le = (lock_entry *)*pointer_of_iceberg_value;
   }

   // get the latch then update the lock status
   return _lock(entry->le, lt, txn);
}

lock_table_2pl_rc
lock_table_2pl_release_entry_lock(lock_table_2pl *lock_tbl,
                                  rw_entry       *entry,
                                  lock_type       lt,
                                  transaction    *txn)
{
   platform_assert(entry->le != NULL,
                   "Trying to release a lock using NULL lock entry");

   if (_unlock(entry->le, lt, txn) == LOCK_TABLE_2PL_RC_OK) {
      // platform_assert(iceberg_force_remove(&lock_tbl->table, key,
      // get_tid()));
      if (iceberg_remove(&lock_tbl->table, entry->key, get_tid())) {
         lock_entry_destroy(entry->le);
         entry->le = NULL;
      }
   }

#if LOCK_TABLE_DEBUG
   platform_default_log("[Thread %d] Release lock on key %s\n",
                        get_tid(),
                        (char *)slice_data(entry->key));
#endif

   return LOCK_TABLE_2PL_RC_OK;
}

typedef struct transactional_splinterdb_config {
   splinterdb_config           kvsb_cfg;
   transaction_isolation_level isol_level;
   bool                        is_upsert_disabled;
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   lock_table_2pl                  *lock_tbl;
} transactional_splinterdb;

/*
 * Implementation of the 2Phase-Locking(2PL). It uses a lock_table that
 * implements three deadlock prevention mechanisms (see lock_table_2pl.h).
 */
txn_timestamp global_ts = 0;

static inline txn_timestamp
get_next_global_ts()
{
   return __atomic_add_fetch(&global_ts, 1, __ATOMIC_RELAXED);
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
   if (!message_is_null(entry->msg)) {
      void *ptr = (void *)message_data(entry->msg);
      platform_free(0, ptr);
   }
}

/*
 * The msg is the msg from app.
 * In EXPERIMENTAL_MODE_TICTOC_DISK, this function adds timestamps at the begin
 * of the msg
 */
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
      entry                                  = rw_entry_create();
      entry->key                             = user_key;
      txn->rw_entries[txn->num_rw_entries++] = entry;
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
      platform_free(0, txn_splinterdb_cfg);
      return rc;
   }

   _txn_kvsb->lock_tbl = lock_table_2pl_create(kvsb_cfg->data_cfg);

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
   lock_table_2pl_destroy(_txn_kvsb->lock_tbl);

   splinterdb_close(&_txn_kvsb->kvsb);

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
   // update the DB and unlock all entries
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      rw_entry *entry = txn->rw_entries[i];
      if (rw_entry_is_write(entry)) {
#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
         if (0) {
#endif
            int rc = 0;
            switch (message_class(entry->msg)) {
               case MESSAGE_TYPE_INSERT:
                  rc = splinterdb_insert(
                     txn_kvsb->kvsb, entry->key, message_slice(entry->msg));
                  break;
               case MESSAGE_TYPE_UPDATE:
                  rc = splinterdb_update(
                     txn_kvsb->kvsb, entry->key, message_slice(entry->msg));
                  break;
               case MESSAGE_TYPE_DELETE:
                  rc = splinterdb_delete(txn_kvsb->kvsb, entry->key);
                  break;
               default:
                  break;
            }
            platform_assert(rc == 0, "Error from SplinterDB: %d\n", rc);
#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 1
         }
#endif
         lock_table_2pl_release_entry_lock(
            txn_kvsb->lock_tbl, entry, WRITE_LOCK, txn);
      } else {
         lock_table_2pl_release_entry_lock(
            txn_kvsb->lock_tbl, entry, READ_LOCK, txn);
      }
   }

   transaction_deinit(txn_kvsb, txn);

   return 0;
}

int
transactional_splinterdb_abort(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   // unlock all entries that are locked so far
   for (int i = 0; i < txn->num_rw_entries; ++i) {
      rw_entry *entry = txn->rw_entries[i];
      if (rw_entry_is_write(entry)) {
         lock_table_2pl_release_entry_lock(
            txn_kvsb->lock_tbl, entry, WRITE_LOCK, txn);
      } else {
         lock_table_2pl_release_entry_lock(
            txn_kvsb->lock_tbl, entry, READ_LOCK, txn);
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
   const data_config *cfg = txn_kvsb->tcfg->kvsb_cfg.data_cfg;
   char              *user_key_copy;
   user_key_copy = TYPED_ARRAY_ZALLOC(0, user_key_copy, slice_length(user_key));
   rw_entry *entry = rw_entry_get(
      txn_kvsb, txn, slice_copy_contents(user_key_copy, user_key), cfg, FALSE);
   /* if (message_class(msg) == MESSAGE_TYPE_UPDATE */
   /*     || message_class(msg) == MESSAGE_TYPE_DELETE) */
   /* { */
   /*    rw_entry_iceberg_insert(txn_kvsb, entry); */
   /*    timestamp_set v = *entry->tuple_ts; */
   /*    entry->wts      = v.wts; */
   /*    entry->rts      = timestamp_set_get_rts(&v); */
   /* } */

   if (!rw_entry_is_write(entry)) {
      // TODO: generate a transaction id to use as the unique lock request id
      if (lock_table_2pl_try_acquire_entry_lock(
             txn_kvsb->lock_tbl, entry, WRITE_LOCK, txn)
          == LOCK_TABLE_2PL_RC_BUSY)
      {
         transactional_splinterdb_abort(txn_kvsb, txn);
         return 1;
      }
      rw_entry_set_msg(entry, msg);
   } else {
      // TODO it needs to be checked later for upsert
      key       wkey = key_create_from_slice(entry->key);
      const key ukey = key_create_from_slice(user_key);
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

int
transactional_splinterdb_insert(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                slice                     value)
{
   if (!txn) {
      return splinterdb_insert(txn_kvsb->kvsb, user_key, value);
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

   int rc = 0;

#if EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 0
   if (rw_entry_is_write(entry)) {
      // read my write
      // TODO This works for simple insert/update. However, it doesn't work
      // for upsert.
      // TODO if it succeeded, this read should not be considered for
      // validation. entry->is_read should be false.
      _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
      merge_accumulator_resize(&_result->value, message_length(entry->msg));
      memcpy(merge_accumulator_data(&_result->value),
             message_data(entry->msg),
             message_length(entry->msg));
   } else {
      // TODO: generate a transaction id to use as the unique lock request id
      if (lock_table_2pl_try_acquire_entry_lock(
             txn_kvsb->lock_tbl, entry, READ_LOCK, txn)
          == LOCK_TABLE_2PL_RC_BUSY)
      {
         transactional_splinterdb_abort(txn_kvsb, txn);
         return 1;
      }
      rc = splinterdb_lookup(txn_kvsb->kvsb, entry->key, result);
   }
#endif
   return rc;
}
