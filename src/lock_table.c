#include "lock_table.h"

#include "platform.h"
#include "poison.h"
#include "data_internal.h"

#define GET_ITSTART(n) (n->start)
#define GET_ITLAST(n)  (n->last)

static int
interval_tree_key_compare(interval_tree_key key1, interval_tree_key key2)
{
   platform_assert(key1.app_data_cfg == key2.app_data_cfg);

   return data_key_compare(key1.app_data_cfg,
                           key_create_from_slice(key1.data),
                           key_create_from_slice(key2.data));
}

INTERVAL_TREE_DEFINE(tictoc_rw_entry,
                     rb,
                     interval_tree_key,
                     __subtree_last,
                     GET_ITSTART,
                     GET_ITLAST,
                     static,
                     interval_tree,
                     interval_tree_key_compare);

// To make a compiler quiet
#define SUPPRESS_UNUSED_WARN(var)                                              \
   void _dummy_tmp_##var(void)                                                 \
   {                                                                           \
      (void)(var);                                                             \
   }

SUPPRESS_UNUSED_WARN(interval_tree_iter_next);

typedef struct lock_table {
   struct rb_root root;
   platform_mutex lock;
} lock_table;

lock_table *
lock_table_create()
{
   lock_table *lt;
   lt       = TYPED_ZALLOC(0, lt);
   lt->root = RB_ROOT;
   platform_mutex_init(&lt->lock, 0, 0);
   return lt;
}

void
lock_table_destroy(lock_table *lock_tbl)
{
   // TODO: destroy all elements
   platform_mutex_destroy(&lock_tbl->lock);
   platform_free(0, lock_tbl);
}

lock_table_rc
lock_table_try_acquire_entry_lock(lock_table *lock_tbl, tictoc_rw_entry *entry)
{
   platform_mutex_lock(&lock_tbl->lock);
   tictoc_rw_entry *node = interval_tree_iter_first(
      &lock_tbl->root, GET_ITSTART(entry), GET_ITLAST(entry));
   if (node) {
      if (node->owner != entry->owner) {
         platform_mutex_unlock(&lock_tbl->lock);
         return LOCK_TABLE_RC_BUSY;
      }

      platform_mutex_unlock(&lock_tbl->lock);
      return LOCK_TABLE_RC_DEADLK;
   }

   interval_tree_insert(entry, &lock_tbl->root);

   platform_mutex_unlock(&lock_tbl->lock);
   return LOCK_TABLE_RC_OK;
}

void
lock_table_release_entry_lock(lock_table *lock_tbl, tictoc_rw_entry *entry)
{
   platform_mutex_lock(&lock_tbl->lock);
   tictoc_rw_entry *node = interval_tree_iter_first(
      &lock_tbl->root, GET_ITSTART(entry), GET_ITLAST(entry));
   if (node) {
      if (node->owner == entry->owner) {
         interval_tree_remove(node, &lock_tbl->root);
      }
   }
   platform_mutex_unlock(&lock_tbl->lock);
}
