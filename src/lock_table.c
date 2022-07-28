#include "lock_table.h"

#include "interval_tree/interval_tree_generic.h"
#include "platform.h"
#include "util.h"
#include "poison.h"
#include "data_internal.h"

typedef struct interval_tree_node_key {
   slice              data;
   const data_config *app_data_cfg;
} interval_tree_node_key;

static interval_tree_node_key
interval_tree_node_key_create(slice data, const data_config *app_data_cfg)
{
   return (interval_tree_node_key){.data = data, .app_data_cfg = app_data_cfg};
}

typedef struct interval_tree_node {
   struct rb_node         rb;
   interval_tree_node_key start;
   interval_tree_node_key last;
   interval_tree_node_key __subtree_last;
} interval_tree_node;

static inline bool
is_point_key(slice start, slice last)
{
   return slices_equal(start, last);
}

static interval_tree_node *
interval_tree_node_create(slice              start,
                          slice              last,
                          const data_config *app_data_cfg)
{
  interval_tree_node *node;
  node = TYPED_ZALLOC(0, node);
   RB_CLEAR_NODE(&node->rb);

   if (is_point_key(start, last)) {
      void *key        = platform_aligned_malloc(0, 64, slice_length(start));
      node->start.data = node->last.data = slice_copy_contents(key, start);
   } else {
      void *start_key  = platform_aligned_malloc(0, 64, slice_length(start));
      void *last_key   = platform_aligned_malloc(0, 64, slice_length(last));
      node->start.data = slice_copy_contents(start_key, start);
      node->last.data  = slice_copy_contents(last_key, last);
   }

   node->start.app_data_cfg = node->last.app_data_cfg = app_data_cfg;

   return node;
}

static void
interval_tree_node_destroy(interval_tree_node *node)
{
   if (is_point_key(node->start.data, node->last.data)) {
      platform_free_from_heap(0, (void *)slice_data(node->start.data));
   } else {
      platform_free_from_heap(0, (void *)slice_data(node->start.data));
      platform_free_from_heap(0, (void *)slice_data(node->last.data));
   }

   platform_free_from_heap(0, node);
}

#define GET_ITSTART(n) (n->start)
#define GET_ITLAST(n)  (n->last)

static int
interval_tree_node_compare(interval_tree_node_key key1,
                           interval_tree_node_key key2)
{
   platform_assert(key1.app_data_cfg == key2.app_data_cfg);

   return data_key_compare(key1.app_data_cfg, key1.data, key2.data);
}

INTERVAL_TREE_DEFINE(interval_tree_node,
                     rb,
                     interval_tree_node_key,
                     __subtree_last,
                     GET_ITSTART,
                     GET_ITLAST,
                     static,
                     interval_tree,
                     interval_tree_node_compare);

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

   const data_config *app_data_cfg;
} lock_table;

static interval_tree_node *
lock_table_insert(lock_table *lock_tbl, slice start, slice last)
{
   interval_tree_node *new_node =
      interval_tree_node_create(start, last, lock_tbl->app_data_cfg);
   platform_mutex_lock(&lock_tbl->lock);
   interval_tree_insert(new_node, &lock_tbl->root);
   platform_mutex_unlock(&lock_tbl->lock);
   return new_node;
}

static bool
lock_table_exist_overlap(lock_table *lock_tbl, slice start, slice last)
{
   platform_mutex_lock(&lock_tbl->lock);
   interval_tree_node_key start_key =
      interval_tree_node_key_create(start, lock_tbl->app_data_cfg);
   interval_tree_node_key last_key =
      interval_tree_node_key_create(last, lock_tbl->app_data_cfg);
   bool exist = interval_tree_iter_first(&lock_tbl->root, start_key, last_key)
                   ? TRUE
                   : FALSE;
   platform_mutex_unlock(&lock_tbl->lock);
   return exist;
}

static void
lock_table_delete(lock_table *lock_tbl, interval_tree_node *node_to_be_deleted)
{
   platform_mutex_lock(&lock_tbl->lock);
   interval_tree_remove(node_to_be_deleted, &lock_tbl->root);
   interval_tree_node_destroy(node_to_be_deleted);
   platform_mutex_unlock(&lock_tbl->lock);
}

lock_table *
lock_table_create(const data_config *app_data_cfg)
{
  lock_table *lt;
  lt = TYPED_ZALLOC(0, lt);
   lt->root = RB_ROOT;
   platform_mutex_init(&lt->lock, 0, 0);
   lt->app_data_cfg = app_data_cfg;
   return lt;
}

void
lock_table_destroy(lock_table *lock_tbl)
{
   // TODO: destroy all elements
   platform_mutex_destroy(&lock_tbl->lock);
   platform_free_from_heap(0, lock_tbl);
}

// Lock returns the interval tree node pointer, and the pointer will
// be used on deletion
range_lock
lock_table_acquire_range_lock(lock_table *lock_tbl, slice start, slice last)
{
   while (lock_table_exist_overlap(lock_tbl, start, last)) {
      platform_pause();
   }

   return (void *)lock_table_insert(lock_tbl, start, last);
}

void
lock_table_release_range_lock(lock_table *lock_tbl, range_lock rng_lock)
{
   lock_table_delete(lock_tbl, (interval_tree_node *)rng_lock);
}

int
lock_table_is_range_locked(lock_table *lock_tbl, slice start, slice last)
{
   return lock_table_exist_overlap(lock_tbl, start, last);
}
