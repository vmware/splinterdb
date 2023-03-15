#include "lock_table.h"
#include "experimental_mode.h"

#if !EXPERIMENTAL_MODE_ATOMIC_WORD

#   if EXPERIMENTAL_MODE_TICTOC_DISK
#      include "transaction_impl/tictoc_disk_internal.h"
#   else
#      include "transaction_impl/fantasticc_internal.h"
#   endif

#   include "poison.h"

typedef struct lock_table {
   iceberg_table table;
} lock_table;

lock_table *
lock_table_create()
{
   lock_table *lt;
   lt = TYPED_ZALLOC(0, lt);
   iceberg_init(&lt->table, 20);
   return lt;
}

void
lock_table_destroy(lock_table *lock_tbl)
{
   platform_free(0, lock_tbl);
}

lock_table_rc
lock_table_try_acquire_entry_lock(lock_table *lock_tbl, rw_entry *entry)
{
   if (entry->is_locked) {
      return LOCK_TABLE_RC_DEADLK;
   }

   KeyType   key        = (KeyType)slice_data(entry->key);
   ValueType lock_owner = platform_get_tid();
   if (iceberg_insert_without_increasing_refcount(
          &lock_tbl->table, key, lock_owner, platform_get_tid() - 1))
   {
      entry->is_locked = 1;
      return LOCK_TABLE_RC_OK;
   }
   return LOCK_TABLE_RC_BUSY;
}

void
lock_table_release_entry_lock(lock_table *lock_tbl, rw_entry *entry)
{
   platform_assert(entry->is_locked,
                   "Trying to release lock that is not locked by this thread");

   KeyType key = (KeyType)slice_data(entry->key);
   platform_assert(
      iceberg_force_remove(&lock_tbl->table, key, platform_get_tid() - 1));
   entry->is_locked = 0;
}

lock_table_rc
lock_table_get_entry_lock_state(lock_table *lock_tbl, rw_entry *entry)
{
   KeyType    key   = (KeyType)slice_data(entry->key);
   ValueType *value = NULL;
   if (iceberg_get_value(&lock_tbl->table, key, &value, platform_get_tid() - 1))
   {
      return LOCK_TABLE_RC_BUSY;
   }
   return LOCK_TABLE_RC_OK;
}
#endif