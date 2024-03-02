#include "lock_table.h"
#include "isketch/iceberg_table.h"
#include "poison.h"

#define LOCK_TABLE_DEBUG 0

typedef enum lock_table_attr {
   LOCK_TABLE_ATTR_INVALID = 0,
   LOCK_TABLE_ATTR_RWLOCK,
} lock_table_attr;

typedef struct lock_table {
   iceberg_table   table;
   lock_table_attr attr;
} lock_table;

static inline char
is_lock_table_with_rwlock(lock_table *lock_tbl)
{
   return lock_tbl->attr == LOCK_TABLE_ATTR_RWLOCK;
}

lock_table *
lock_table_create(const data_config *spl_data_config)
{
   lock_table *lt;
   lt = TYPED_ZALLOC(0, lt);
   iceberg_init(&lt->table, 20, spl_data_config);
   lt->attr = LOCK_TABLE_ATTR_INVALID;
   return lt;
}

void
lock_table_destroy(lock_table *lock_tbl)
{
   platform_free(0, lock_tbl);
}

static inline threadid
get_tid()
{
   return platform_get_tid();
}

lock_table_rc
lock_table_try_acquire_entry_lock(lock_table *lock_tbl, lock_table_entry *entry)
{
   if (entry->is_locked) {
#if LOCK_TABLE_DEBUG
      platform_default_log("[Thread %lu] Already acquired lock on key %s\n",
                           get_tid(),
                           (char *)slice_data(entry->key));
#endif
      return LOCK_TABLE_RC_DEADLK;
   }

   ValueType lock_owner = get_tid();
   slice     entry_key  = entry->key;
   if (iceberg_insert_without_increasing_refcount(
          &lock_tbl->table, &entry_key, lock_owner, get_tid()))
   {
#if LOCK_TABLE_DEBUG
      platform_default_log("[Thread %lu] Acquired lock on key %s(%p)\n",
                           get_tid(),
                           (char *)slice_data(entry->key),
                           slice_data(entry->key));
#endif
      entry->is_locked = 1;
      return LOCK_TABLE_RC_OK;
   }
#if LOCK_TABLE_DEBUG
   platform_default_log("[Thread %lu] Fail to acquire lock on key %s\n",
                        get_tid(),
                        (char *)slice_data(entry->key));
#endif
   return LOCK_TABLE_RC_BUSY;
}

lock_table_rc
lock_table_try_acquire_entry_lock_timeouts(lock_table       *lock_tbl,
                                           lock_table_entry *entry,
                                           timestamp         timeout_ns)
{
   if (entry->is_locked) {
#if LOCK_TABLE_DEBUG
      platform_default_log("[Thread %lu] Already acquired lock on key %s\n",
                           get_tid(),
                           (char *)slice_data(entry->key));
#endif
      return LOCK_TABLE_RC_DEADLK;
   }

   if (timeout_ns == 0) {
      return lock_table_try_acquire_entry_lock(lock_tbl, entry);
   }

   ValueType lock_owner = get_tid();
   timestamp start_ns   = platform_get_timestamp();
   while (TRUE) {
      slice entry_key = entry->key;
      if (iceberg_insert_without_increasing_refcount(
             &lock_tbl->table, &entry_key, lock_owner, get_tid()))
      {
#if LOCK_TABLE_DEBUG
         platform_default_log("[Thread %lu] Acquired lock on key %s\n",
                              get_tid(),
                              (char *)slice_data(entry->key));
#endif
         entry->is_locked = 1;
         return LOCK_TABLE_RC_OK;
      }

      if (platform_timestamp_diff(start_ns, platform_get_timestamp())
          > timeout_ns) {
         break;
      }
   }

#if LOCK_TABLE_DEBUG
   platform_default_log("[Thread %lu] Fail to acquire lock on key %s\n",
                        get_tid(),
                        (char *)slice_data(entry->key));
#endif

   return LOCK_TABLE_RC_BUSY;
}


lock_table_rc
lock_table_release_entry_lock(lock_table *lock_tbl, lock_table_entry *entry)
{
   platform_assert(entry->is_locked,
                   "[Thread %lu] Trying to release lock that is not locked by "
                   "this thread (key: %s)",
                   get_tid(),
                   (char *)slice_data(entry->key));

#if LOCK_TABLE_DEBUG
   platform_default_log("[Thread %lu] Release lock on key %s(%p)\n",
                        get_tid(),
                        (char *)slice_data(entry->key),
                        slice_data(entry->key));
#endif

   platform_assert(
      iceberg_force_remove(&lock_tbl->table, entry->key, get_tid()));
   entry->is_locked = 0;

   return LOCK_TABLE_RC_OK;
}

lock_table_rc
lock_table_get_entry_lock_state(lock_table *lock_tbl, lock_table_entry *entry)
{

   // #   if LOCK_TABLE_DEBUG
   //    platform_default_log("[Thread %lu] Get a lock state on key %s(%p)\n",
   //                         get_tid(),
   //                         (char *)slice_data(entry->key),
   //                         slice_data(entry->key));
   // #   endif

   ValueType *value = NULL;
   if (iceberg_get_value(&lock_tbl->table, entry->key, &value, get_tid())) {
      return LOCK_TABLE_RC_BUSY;
   }
   return LOCK_TABLE_RC_OK;
}

/*
 * Read-Writer lock
 */

// super->id represents the writer's id.
typedef struct {
   lock_table_shared_entry super;
   platform_rwlock         rwlock;
} lock_table_shared_rwlock_entry;

lock_table *
lock_table_create_with_rwlock(const data_config *spl_data_config)
{
   lock_table *lt = lock_table_create(spl_data_config);
   lt->attr       = LOCK_TABLE_ATTR_RWLOCK;
   return lt;
}

static inline int
platform_status_to_int(platform_status status)
{
   return status.r;
}

static inline void
get_shared_rwlock(lock_table *lock_tbl, lock_table_entry *entry)
{
   lock_table_shared_rwlock_entry *shared_lock;
   shared_lock            = TYPED_ZALLOC(0, shared_lock);
   entry->shared_lock     = (lock_table_shared_entry *)shared_lock;
   entry->shared_lock->id = -1;
   platform_rwlock_init(&shared_lock->rwlock, 0, 0);
   ValueType  value_to_be_inserted     = (ValueType)entry->shared_lock;
   ValueType *pointer_of_iceberg_value = &value_to_be_inserted;
   slice      entry_key                = entry->key;
   if (iceberg_insert_and_get(&lock_tbl->table,
                              &entry_key,
                              (ValueType **)&pointer_of_iceberg_value,
                              get_tid()))
   {
      // The rwlock is newly inserted.
      platform_assert((lock_table_shared_entry *)*pointer_of_iceberg_value
                         == entry->shared_lock,
                      "Unexpected rwlock");
   } else {
      // The rwlock is already inserted.
      platform_rwlock_destroy(&shared_lock->rwlock);
      platform_free(0, entry->shared_lock);
      entry->shared_lock = (lock_table_shared_entry *)*pointer_of_iceberg_value;
   }
}

lock_table_rc
lock_table_try_acquire_entry_wrlock(lock_table       *lock_tbl,
                                    lock_table_entry *entry)
{
   platform_assert(
      is_lock_table_with_rwlock(lock_tbl),
      "lock_table_try_acquire_entry_wrlock() called on non-rwlock lock table");

   if (entry->shared_lock == NULL) {
      get_shared_rwlock(lock_tbl, entry);
   }

   lock_table_shared_rwlock_entry *shared_lock =
      (lock_table_shared_rwlock_entry *)entry->shared_lock;
   platform_status rc = platform_rwlock_trywrlock(&shared_lock->rwlock);
   if (platform_status_to_int(rc) == EBUSY) {
#if LOCK_TABLE_DEBUG
      platform_default_log("[Thread %lu] Fail to acquire wrlock on key %s "
                           "(shared_id: %lu, local_id: %lu)\n",
                           get_tid(),
                           (char *)slice_data(entry->key),
                           entry->shared_lock->id,
                           entry->id);
#endif
      return LOCK_TABLE_RC_BUSY;
   } else if (platform_status_to_int(rc) == EDEADLK) {
      return LOCK_TABLE_RC_DEADLK;
   } else if (platform_status_to_int(rc) == EINVAL) {
      platform_assert(
         FALSE,
         "rwlock does not refer to an initialized read-write lock object.");
      return LOCK_TABLE_RC_INVALID;
   } else if (platform_status_to_int(rc) == 0) {
#if LOCK_TABLE_DEBUG
      platform_default_log("[Thread %lu] Acquired wrlock on key %s(%p)\n",
                           get_tid(),
                           (char *)slice_data(entry->key),
                           slice_data(entry->key));
#endif
      platform_assert(entry->shared_lock->id == -1,
                      "Unexpected shared_lock->id: %lu",
                      entry->shared_lock->id);
      platform_assert(entry->id != -1, "Unexpected entry->id: %lu", entry->id);
      entry->shared_lock->id = entry->id;
      return LOCK_TABLE_RC_OK;
   } else {
      platform_assert(FALSE,
                      "Unknown return code from platform_rwlock_trywrlock()");
      return LOCK_TABLE_RC_INVALID;
   }
}

lock_table_rc
lock_table_try_acquire_entry_rdlock(lock_table       *lock_tbl,
                                    lock_table_entry *entry)
{
   platform_assert(
      is_lock_table_with_rwlock(lock_tbl),
      "lock_table_try_acquire_entry_rdlock() called on non-rwlock lock table");

   if (entry->shared_lock == NULL) {
      get_shared_rwlock(lock_tbl, entry);
   }

   lock_table_shared_rwlock_entry *shared_lock =
      (lock_table_shared_rwlock_entry *)entry->shared_lock;
   platform_status rc = platform_rwlock_tryrdlock(&shared_lock->rwlock);
   if (platform_status_to_int(rc) == EBUSY) {
#if LOCK_TABLE_DEBUG
      platform_default_log("[Thread %lu] Fail to acquire rdlock on key %s\n",
                           get_tid(),
                           (char *)slice_data(entry->key));
#endif
      return LOCK_TABLE_RC_BUSY;
   } else if (platform_status_to_int(rc) == EDEADLK) {
      return LOCK_TABLE_RC_DEADLK;
   } else if (platform_status_to_int(rc) == EINVAL) {
      platform_assert(
         FALSE,
         "rwlock does not refer to an initialized read-write lock object.");
      return LOCK_TABLE_RC_INVALID;
   } else if (platform_status_to_int(rc) == 0) {
#if LOCK_TABLE_DEBUG
      platform_default_log("[Thread %lu] Acquired rdlock on key %s(%p)\n",
                           get_tid(),
                           (char *)slice_data(entry->key),
                           slice_data(entry->key));
#endif
      return LOCK_TABLE_RC_OK;
   } else {
      platform_assert(FALSE,
                      "Unknown return code from platform_rwlock_tryrdlock()");
      return LOCK_TABLE_RC_INVALID;
   }
}

lock_table_rc
lock_table_acquire_entry_wrlock(lock_table *lock_tbl, lock_table_entry *entry)
{
   platform_assert(
      is_lock_table_with_rwlock(lock_tbl),
      "lock_table_acquire_entry_wrlock() called on non-rwlock lock table");
   lock_table_rc rc;
   while ((rc = lock_table_try_acquire_entry_wrlock(lock_tbl, entry))
          == LOCK_TABLE_RC_BUSY)
   {
      platform_sleep_ns(1000);
   }
   return rc;
}

lock_table_rc
lock_table_acquire_entry_rdlock(lock_table *lock_tbl, lock_table_entry *entry)
{
   platform_assert(
      is_lock_table_with_rwlock(lock_tbl),
      "lock_table_acquire_entry_wrlock() called on non-rwlock lock table");
   lock_table_rc rc;
   while ((rc = lock_table_try_acquire_entry_rdlock(lock_tbl, entry))
          == LOCK_TABLE_RC_BUSY)
   {
      platform_sleep_ns(1000);
   }
   return rc;
}

lock_table_rc
lock_table_release_entry_rwlock(lock_table *lock_tbl, lock_table_entry *entry)
{
   platform_assert(
      is_lock_table_with_rwlock(lock_tbl),
      "lock_table_release_entry_rwlock() called on non-rwlock lock table");
   platform_assert(entry->shared_lock != NULL, "shared_rwlock is NULL");
   entry->shared_lock->id = -1;
   lock_table_shared_rwlock_entry *shared_lock =
      (lock_table_shared_rwlock_entry *)entry->shared_lock;
   platform_rwlock_unlock(&shared_lock->rwlock);
   if (iceberg_remove(&lock_tbl->table, entry->key, get_tid())) {
      platform_rwlock_destroy(&shared_lock->rwlock);
      platform_free(0, entry->shared_lock);
   }
   return LOCK_TABLE_RC_OK;
}