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

   return data_key_compare(key1.app_data_cfg, key1.data, key2.data);
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

static inline void
lock_table_insert(lock_table *lock_tbl, tictoc_rw_entry *entry)
{
   platform_mutex_lock(&lock_tbl->lock);
   interval_tree_insert(entry, &lock_tbl->root);
   platform_mutex_unlock(&lock_tbl->lock);
}

static inline bool
lock_table_exist_overlap(lock_table *lock_tbl, tictoc_rw_entry *entry)
{
   platform_mutex_lock(&lock_tbl->lock);
   bool is_exist = interval_tree_iter_first(
                      &lock_tbl->root, GET_ITSTART(entry), GET_ITLAST(entry))
                      ? TRUE
                      : FALSE;
   platform_mutex_unlock(&lock_tbl->lock);
   return is_exist;
}

static inline void
lock_table_delete(lock_table *lock_tbl, tictoc_rw_entry *entry)
{
   platform_mutex_lock(&lock_tbl->lock);
   interval_tree_remove(entry, &lock_tbl->root);
   platform_mutex_unlock(&lock_tbl->lock);
}

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

// Lock returns the interval tree node pointer, and the pointer will
// be used on deletion
void
lock_table_acquire_entry_lock(lock_table *lock_tbl, tictoc_rw_entry *entry)
{
   while (lock_table_exist_overlap(lock_tbl, entry)) {
      platform_pause();
   }

   lock_table_insert(lock_tbl, entry);
}

// If there is a lock owner, it returns NULL
bool
lock_table_try_acquire_entry_lock(lock_table *lock_tbl, tictoc_rw_entry *entry)
{
   if (lock_table_exist_overlap(lock_tbl, entry)) {
      return NULL;
   }

   lock_table_insert(lock_tbl, entry);

   return TRUE;
}

void
lock_table_release_entry_lock(lock_table *lock_tbl, tictoc_rw_entry *entry)
{
   lock_table_delete(lock_tbl, entry);
}

bool
lock_table_is_entry_locked(lock_table *lock_tbl, tictoc_rw_entry *entry)
{
   return lock_table_exist_overlap(lock_tbl, entry);
}
