#include "lock_table_rw.h"
#include "experimental_mode.h"
#include "transaction_impl/2pl_internal.h"
#include "poison.h"

lock_table_rw *
lock_table_rw_create()
{
   lock_table_rw *lt;
   lt = TYPED_ZALLOC(0, lt);
   iceberg_init(&lt->table, 20);
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

lock_table_rw_rc
_lock(lock_entry *le,
      lock_type lt,
      lock_req_id lid) {

   lock_req* iter = le->owners;

   while (iter != NULL) {
      if (iter->lt == WRITE_LOCK) {
         platform_assert(iter->next == NULL, "More than one owners holding an exclusive lock");
         if (iter->id != lid)
            // another writer holding the lock
            return LOCK_TABLE_RW_RC_BUSY;
         else
            // we already hold an exclusive lock
            return LOCK_TABLE_RW_RC_OK;
      }

      if (lt == WRITE_LOCK) {
         if (iter->id != lid ) {
            // another reader is holding the lock,
            // but we want exclusive access
            return LOCK_TABLE_RW_RC_BUSY;
         } else if (iter->next) {
            // there's still another reader besides
            // us holding the lock,
            // but we want exclusive access
            return LOCK_TABLE_RW_RC_BUSY;
         } else {
            // we can upgrade the shared lock which we are
            // ealready xclusively holding
            iter->lt = WRITE_LOCK;
            return LOCK_TABLE_RW_RC_OK;
         }
      }
   
      if (iter->id == lid )
         // we already have been granted the read lock
         return LOCK_TABLE_RW_RC_OK;

      if (iter->next == NULL) {
         // we need to create a new lock_req and obtain the read lock
         lock_req* lreq = TYPED_MALLOC(0, lreq);
         lreq->next = NULL;
         lreq->lt = lt;
         lreq->id = lid;
         iter->next = lreq;
         return LOCK_TABLE_RW_RC_OK;
      }
      iter = iter->next;   
   }

   return LOCK_TABLE_RW_RC_OK;
}

lock_table_rw_rc
_unlock(lock_entry *le,
        lock_type lt,
        lock_req_id lid) {

   //lock_req* iter = le->owners;
   
   return 0;
}

lock_table_rw_rc
lock_table_rw_try_acquire_entry_lock(lock_table_rw *lock_tbl,
                                     rw_entry *entry,
                                     lock_type lt,
                                     lock_req_id lid)
{
   lock_table_rw_rc ret;
   if (entry->le) {
      // we already have a pointer to the lock status
      platform_mutex_lock(&entry->le->latch);
      ret = _lock(entry->le, lt, lid);
      platform_mutex_unlock(&entry->le->latch);
      return ret;
   }

   // else we either get a pointer to an existing lock status
   // or create a new one
   lock_entry *le = TYPED_MALLOC (0, le);
   platform_mutex_init(&le->latch, 0, 0);

   KeyType key = (KeyType)slice_data(entry->key);
   iceberg_insert_and_get(&lock_tbl->table,
                           key,
                           (ValueType **)&entry->le,
                           get_tid());

   if (le != entry->le) {
      // there's already a lock_entry for this key in the lock_table
      platform_mutex_destroy(&le->latch);
      platform_free(0, le);
   }

   // get the latch then update the lock status
   platform_mutex_lock(&entry->le->latch);
   ret = _lock(entry->le, lt, lid);
   platform_mutex_unlock(&entry->le->latch);
   return ret;
}

lock_table_rw_rc
lock_table_rw_release_entry_lock(lock_table_rw *lock_tbl,
                                 rw_entry *entry,
                                 lock_type lt,
                                 lock_req_id lid)
{

   KeyType key = (KeyType)slice_data(entry->key);
   platform_assert(iceberg_force_remove(&lock_tbl->table, key, get_tid()));
   entry->is_locked = 0;

#   if LOCK_TABLE_DEBUG
   platform_default_log("[Thread %d] Release lock on key %s\n",
                        get_tid(),
                        (char *)slice_data(entry->key));
#   endif

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