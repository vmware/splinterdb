#include "lock_table.h"
#include "experimental_mode.h"

#define USE_LOCK_TABLE 1

#if EXPERIMENTAL_MODE_TICTOC_DISK
#   include "transaction_impl/tictoc_disk_internal.h"
#elif EXPERIMENTAL_MODE_STO_DISK
#   include "transaction_impl/sto_disk_internal.h"
#elif EXPERIMENTAL_MODE_SILO_MEMORY
#   include "transaction_impl/fantasticc_internal.h"
#else
#   undef USE_LOCK_TABLE
#   define USE_LOCK_TABLE 0
#endif

#if USE_LOCK_TABLE
#   include "isketch/iceberg_table.h"
#   include "poison.h"

#   define LOCK_TABLE_DEBUG 0

typedef struct lock_table {
   iceberg_table table;
} lock_table;

lock_table *
lock_table_create(const data_config *spl_data_config)
{
   lock_table *lt;
   lt = TYPED_ZALLOC(0, lt);
   iceberg_init(&lt->table, 20, spl_data_config);
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
lock_table_try_acquire_entry_lock(lock_table *lock_tbl, rw_entry *entry)
{
   if (entry->is_locked) {
#   if LOCK_TABLE_DEBUG
      platform_default_log("[Thread %lu] Already acquired lock on key %s\n",
                           get_tid(),
                           (char *)slice_data(entry->key));
#   endif
      return LOCK_TABLE_RC_DEADLK;
   }

   ValueType lock_owner = get_tid();
   slice     entry_key  = entry->key;
   if (iceberg_insert_without_increasing_refcount(
          &lock_tbl->table, &entry_key, lock_owner, get_tid()))
   {
#   if LOCK_TABLE_DEBUG
      platform_default_log("[Thread %lu] Acquired lock on key %s(%p)\n",
                           get_tid(),
                           (char *)slice_data(entry->key),
                           slice_data(entry->key));
#   endif
      entry->is_locked = 1;
      return LOCK_TABLE_RC_OK;
   }
#   if LOCK_TABLE_DEBUG
   platform_default_log("[Thread %lu] Fail to acquire lock on key %s\n",
                        get_tid(),
                        (char *)slice_data(entry->key));
#   endif
   return LOCK_TABLE_RC_BUSY;
}

lock_table_rc
lock_table_try_acquire_entry_lock_timeouts(lock_table *lock_tbl,
                                           rw_entry   *entry,
                                           timestamp   timeout_ns)
{
   if (entry->is_locked) {
#   if LOCK_TABLE_DEBUG
      platform_default_log("[Thread %lu] Already acquired lock on key %s\n",
                           get_tid(),
                           (char *)slice_data(entry->key));
#   endif
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
#   if LOCK_TABLE_DEBUG
         platform_default_log("[Thread %lu] Acquired lock on key %s\n",
                              get_tid(),
                              (char *)slice_data(entry->key));
#   endif
         entry->is_locked = 1;
         return LOCK_TABLE_RC_OK;
      }

      if (platform_timestamp_diff(start_ns, platform_get_timestamp())
          > timeout_ns) {
         break;
      }
   }

#   if LOCK_TABLE_DEBUG
   platform_default_log("[Thread %lu] Fail to acquire lock on key %s\n",
                        get_tid(),
                        (char *)slice_data(entry->key));
#   endif

   return LOCK_TABLE_RC_BUSY;
}


lock_table_rc
lock_table_release_entry_lock(lock_table *lock_tbl, rw_entry *entry)
{
   platform_assert(entry->is_locked,
                   "[Thread %lu] Trying to release lock that is not locked by "
                   "this thread (key: %s)",
                   get_tid(),
                   (char *)slice_data(entry->key));

#   if LOCK_TABLE_DEBUG
   platform_default_log("[Thread %lu] Release lock on key %s(%p)\n",
                        get_tid(),
                        (char *)slice_data(entry->key),
                        slice_data(entry->key));
#   endif

   platform_assert(
      iceberg_force_remove(&lock_tbl->table, entry->key, get_tid()));
   entry->is_locked = 0;

   return LOCK_TABLE_RC_OK;
}

lock_table_rc
lock_table_get_entry_lock_state(lock_table *lock_tbl, rw_entry *entry)
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
#else
lock_table *
lock_table_create(const data_config *spl_data_config)
{
   platform_assert(FALSE, "Not implemented");
   return NULL;
}
void
lock_table_destroy(lock_table *lock_tbl)
{
   platform_assert(FALSE, "Not implemented");
}

lock_table_rc
lock_table_try_acquire_entry_lock(lock_table *lock_tbl, rw_entry *entry)
{
   platform_assert(FALSE, "Not implemented");
   return LOCK_TABLE_RC_INVALID;
}
lock_table_rc
lock_table_try_acquire_entry_lock_timeouts(lock_table *lock_tbl,
                                           rw_entry   *entry,
                                           timestamp   timeout_ns)
{
   platform_assert(FALSE, "Not implemented");
   return LOCK_TABLE_RC_INVALID;
}
lock_table_rc
lock_table_release_entry_lock(lock_table *lock_tbl, rw_entry *entry)
{
   platform_assert(FALSE, "Not implemented");
   return LOCK_TABLE_RC_INVALID;
}
lock_table_rc
lock_table_get_entry_lock_state(lock_table *lock_tbl, rw_entry *entry)
{
   platform_assert(FALSE, "Not implemented");
   return LOCK_TABLE_RC_INVALID;
}
#endif