#include "lock_table.h"

#include "platform.h"
#include "data_internal.h"
#include "iceberg_table.h"

#include "poison.h"

typedef struct lock_table {
   iceberg_table table;
} lock_table;

lock_table *
lock_table_create()
{
   lock_table *lt;
   lt = TYPED_ZALLOC(0, lt);
   iceberg_init(&lt->table, 10);
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
   KeyType   key        = (KeyType)slice_data(entry->key);
   ValueType lock_owner = {.refcount = 1, .value = entry->owner};
RETRY:
   if (iceberg_insert(&lock_tbl->table, key, lock_owner, platform_get_tid())) {
      return LOCK_TABLE_RC_OK;
   }

   ValueType *current_lock_owner = NULL;
   if (iceberg_get_value(
          &lock_tbl->table, key, &current_lock_owner, platform_get_tid()))
   {
      if (current_lock_owner->value == entry->owner) {
         return LOCK_TABLE_RC_DEADLK;
      }
      return LOCK_TABLE_RC_BUSY;
   }

   goto RETRY;
}

void
lock_table_release_entry_lock(lock_table *lock_tbl, rw_entry *entry)
{
   KeyType    key                = (KeyType)slice_data(entry->key);
   ValueType *current_lock_owner = NULL;
   if (iceberg_get_value(
          &lock_tbl->table, key, &current_lock_owner, platform_get_tid()))
   {
      if (current_lock_owner->value == entry->owner) {
         iceberg_force_remove(&lock_tbl->table, key, platform_get_tid());
         return;
      }
   }
   platform_assert(FALSE,
                   "Trying to release lock that is not locked by this thread");
}
