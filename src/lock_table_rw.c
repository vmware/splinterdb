#include "lock_table_rw.h"
#include "experimental_mode.h"
#include "transaction_impl/2pl_internal.h"
#include "poison.h"

lock_table_rw *
lock_table_rw_create(const data_config *spl_data_config)
{
   lock_table_rw *lt;
   lt = TYPED_ZALLOC(0, lt);
   iceberg_init(&lt->table, 20, spl_data_config);
   return lt;
}

void
lock_table_rw_destroy(lock_table_rw *lock_tbl)
{
   platform_free(0, lock_tbl);
}

static inline uint8_t
get_tid()
{
   return platform_get_tid() - 1;
}

static inline lock_req *
get_lock_req(lock_type lt, lock_req_id lid)
{
   lock_req *lreq = TYPED_MALLOC(0, lreq);
   lreq->next     = NULL;
   lreq->lt       = lt;
   lreq->id       = lid;
   return lreq;
}

//*******************************************
#if NO_WAIT == 1
//*******************************************
// we're not using pthread_rw_lock because it does not
// support upgrading from shared to exclusive
lock_entry *
lock_entry_init()
{
   lock_entry *le = TYPED_MALLOC(0, le);
   platform_mutex_init(&le->latch, 0, 0);
   return le;
}

void
lock_entry_destroy(lock_entry *le)
{
   platform_mutex_destroy(&le->latch);
   platform_free(0, le);
}

lock_table_rw_rc
_lock(lock_entry *le, lock_type lt, lock_req_id lid)
{

   platform_mutex_lock(&le->latch);

   if (le->owners == NULL) {
      // we need to create a new lock_req and obtain the lock
      le->owners = get_lock_req(lt, lid);
      platform_mutex_unlock(&le->latch);
      return LOCK_TABLE_RW_RC_OK;
   }

   lock_req *iter = le->owners;

   while (iter != NULL) {
      if (iter->lt == WRITE_LOCK) {
         platform_assert(iter->next == NULL,
                         "More than one owners holding an exclusive lock");
         if (iter->id != lid) {
            // another writer holding the lock
            platform_mutex_unlock(&le->latch);
            return LOCK_TABLE_RW_RC_BUSY;
         } else {
            // we already hold an exclusive lock
            platform_mutex_unlock(&le->latch);
            return LOCK_TABLE_RW_RC_OK;
         }
      }

      if (lt == WRITE_LOCK) {
         if (iter->id != lid) {
            // another reader is holding the lock,
            // but we want exclusive access
            platform_mutex_unlock(&le->latch);
            return LOCK_TABLE_RW_RC_BUSY;
         } else if (iter->next) {
            // there's still another reader besides
            // us holding the lock,
            // but we want exclusive access
            platform_mutex_unlock(&le->latch);
            return LOCK_TABLE_RW_RC_BUSY;
         } else {
            // we can upgrade the shared lock which we are
            // ealready xclusively holding
            iter->lt = WRITE_LOCK;
            platform_mutex_unlock(&le->latch);
            return LOCK_TABLE_RW_RC_OK;
         }
      }

      if (iter->id == lid) {
         // we already have been granted the read lock
         platform_mutex_unlock(&le->latch);
         return LOCK_TABLE_RW_RC_OK;
      }
      if (iter->next == NULL) {
         // we need to create a new lock_req and obtain the read lock
         iter->next = get_lock_req(lt, lid);
         platform_mutex_unlock(&le->latch);
         return LOCK_TABLE_RW_RC_OK;
      }
      iter = iter->next;
   }

   // Should not get here
   platform_assert(false, "Dead code branch");
   platform_mutex_unlock(&le->latch);
   return LOCK_TABLE_RW_RC_OK;
}

lock_table_rw_rc
_unlock(lock_entry *le, lock_type lt, lock_req_id lid)
{

   platform_mutex_lock(&le->latch);
   lock_req *iter = le->owners;
   lock_req *prev = NULL;

   while (iter != NULL) {
      if (iter->id == lid) {
         if (iter->lt == lt) {
            // request is valid, release the lock
            if (prev != NULL) {
               prev->next = iter->next;
            }
            platform_free(0, iter);
            platform_mutex_unlock(&le->latch);
            return LOCK_TABLE_RW_RC_OK;
         } else {
            platform_mutex_unlock(&le->latch);
            return LOCK_TABLE_RW_RC_INVALID;
         }
      }
      prev = iter;
      iter = iter->next;
   }

   platform_mutex_unlock(&le->latch);
   return LOCK_TABLE_RW_RC_NODATA;
}

//*******************************************
#elif WAIT_DIE == 1
//*******************************************
lock_entry *
lock_entry_init()
{
   lock_entry *le = TYPED_MALLOC(0, le);
   platform_condvar_init(&le->condvar, 0);
   return le;
}

void
lock_entry_destroy(lock_entry *le)
{
   platform_condvar_destroy(&le->condvar);
   platform_free(0, le);
}

lock_table_rw_rc
_lock(lock_entry *le, lock_type lt, lock_req_id lid)
{
   // owners are sorted by ts such that the
   // oldest owner (with the smallest ts) is the first
   platform_condvar_lock(&le->condvar);
   while (true) {
      if (le->owners == NULL) {
         // we need to create a new lock_req and obtain the lock
         le->owners = get_lock_req(lt, lid);
         platform_condvar_unlock(&le->condvar);
         return LOCK_TABLE_RW_RC_OK;
      }

      lock_req *iter = le->owners;

      while (iter != NULL) {
         if (iter->lt == WRITE_LOCK) {
            platform_assert(iter->next == NULL,
                            "More than one owners holding an exclusive lock");
            if (iter->id != lid) {
               // another writer holding the lock
               if (iter->id < lid) {
                  platform_condvar_unlock(&le->condvar);
                  return LOCK_TABLE_RW_RC_BUSY;
               } else {
                  platform_condvar_wait(&le->condvar);
                  break;
               }
            } else {
               // we already hold an exclusive lock
               platform_condvar_unlock(&le->condvar);
               return LOCK_TABLE_RW_RC_OK;
            }
         }

         if (lt == WRITE_LOCK) {
            if (iter->id != lid) {
               // another reader is holding the lock,
               // but we want exclusive access
               if (iter->id < lid) {
                  platform_condvar_unlock(&le->condvar);
                  return LOCK_TABLE_RW_RC_BUSY;
               } else {
                  platform_condvar_wait(&le->condvar);
                  break;
               }
            } else if (iter->next) {
               // there's still another reader besides
               // us holding the lock,
               // but we want exclusive access
               if (iter->id < lid) {
                  platform_condvar_unlock(&le->condvar);
                  return LOCK_TABLE_RW_RC_BUSY;
               } else {
                  platform_condvar_wait(&le->condvar);
                  break;
               }
            } else {
               // we can upgrade the shared lock which we are
               // ealready exclusively holding
               iter->lt = WRITE_LOCK;
               return LOCK_TABLE_RW_RC_OK;
            }
         }

         if (iter->id == lid) {
            // we already have been granted the read lock
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_RW_RC_OK;
         }

         if (iter->next == NULL) {
            // we need to create a new lock_req and obtain the read lock
            iter->next = get_lock_req(lt, lid);
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_RW_RC_OK;
         } else if (iter->next->id > lid) {
            lock_req *lr = get_lock_req(lt, lid);
            lr->next     = iter->next;
            iter->next   = lr;
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_RW_RC_OK;
         }

         iter = iter->next;
      }
   }

   // Should not get here
   platform_assert(false, "Dead code branch");
   platform_condvar_unlock(&le->condvar);
   return LOCK_TABLE_RW_RC_OK;
}

lock_table_rw_rc
_unlock(lock_entry *le, lock_type lt, lock_req_id lid)
{

   platform_condvar_lock(&le->condvar);
   lock_req *iter = le->owners;
   lock_req *prev = NULL;

   while (iter != NULL) {
      if (iter->id == lid) {
         if (iter->lt == lt) {
            // request is valid, release the lock
            if (prev != NULL) {
               prev->next = iter->next;
            }
            platform_free(0, iter);
            platform_condvar_broadcast(&le->condvar);
            platform_condvar_unlock(&le->condvar);
            return LOCK_TABLE_RW_RC_OK;
         } else {
            return LOCK_TABLE_RW_RC_INVALID;
         }
      }
      prev = iter;
      iter = iter->next;
   }

   return LOCK_TABLE_RW_RC_NODATA;
}


#elif WOUND_WAIT == 1
#else
#   error("No locking policy selected")
#endif

lock_table_rw_rc
lock_table_rw_try_acquire_entry_lock(lock_table_rw *lock_tbl,
                                     rw_entry      *entry,
                                     lock_type      lt,
                                     lock_req_id    lid)
{
   lock_table_rw_rc ret;
   if (entry->le) {
      // we already have a pointer to the lock status
      ret = _lock(entry->le, lt, lid);
      return ret;
   }

   // else we either get a pointer to an existing lock status
   // or create a new one
   lock_entry *le = lock_entry_init();

   slice entry_key = entry->key;
   iceberg_insert_and_get(
      &lock_tbl->table, &entry_key, (ValueType **)&entry->le, get_tid());

   if (le != entry->le) {
      // there's already a lock_entry for this key in the lock_table
      lock_entry_destroy(le);
   }

   // get the latch then update the lock status
   ret = _lock(entry->le, lt, lid);
   return ret;
}

lock_table_rw_rc
lock_table_rw_release_entry_lock(lock_table_rw *lock_tbl,
                                 rw_entry      *entry,
                                 lock_type      lt,
                                 lock_req_id    lid)
{
   platform_assert(entry->le != NULL,
                   "Trying to release a lock using NULL lock entry");

   if (_unlock(entry->le, lt, lid) == LOCK_TABLE_RW_RC_OK) {
      // platform_assert(iceberg_force_remove(&lock_tbl->table, key,
      // get_tid()));
      ValueType value = {0};
      if (iceberg_get_and_remove(
             &lock_tbl->table, entry->key, &value, get_tid())) {
         lock_entry_destroy(entry->le);
         return LOCK_TABLE_RW_RC_OK;
      }
   }

#if LOCK_TABLE_DEBUG
   platform_default_log("[Thread %d] Release lock on key %s\n",
                        get_tid(),
                        (char *)slice_data(entry->key));
#endif

   return LOCK_TABLE_RW_RC_OK;
}

// lock_table_rw_rc
// lock_table_rw_get_entry_lock_state(lock_table_rw *lock_tbl, rw_entry *entry)
// {
//    KeyType    key   = (KeyType)slice_data(entry->key);
//    ValueType *value = NULL;
//    if (iceberg_get_value(&lock_tbl->table, key, &value, get_tid())) {
//       return LOCK_TABLE_RW_RC_BUSY;
//    }
//    return LOCK_TABLE_RW_RC_OK;
// }
