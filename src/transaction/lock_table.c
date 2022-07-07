#include "lock_table.h"

#include "interval_tree/interval_tree_generic.h"

typedef struct interval_tree_node {
   struct rb_node       rb;
   slice                start;
   slice                last;
   slice                __subtree_last;
   transaction_op_meta *meta;
} interval_tree_node;

interval_tree_node *
interval_tree_node_create(slice start, slice last, transaction_op_meta *meta)
{
   interval_tree_node *node =
      (interval_tree_node *)malloc(sizeof(interval_tree_node));
   RB_CLEAR_NODE(&node->rb);
   node->start = start;
   node->last  = last;
   node->meta  = meta;

   return node;
}

#define GET_ITSTART(n) (n->start)
#define GET_ITLAST(n)  (n->last)

INTERVAL_TREE_DEFINE(interval_tree_node,
                     rb,
                     slice,
                     __subtree_last,
                     GET_ITSTART,
                     GET_ITLAST,
                     static,
                     interval_tree,
                     slice_lex_cmp);


typedef struct lock_table {
   struct rb_root root;
} lock_table;

lock_table *
lock_table_create()
{
   lock_table *lt = (lock_table *)malloc(sizeof(lock_table));
   lt->root       = RB_ROOT;
   return lt;
}

void
lock_table_destroy(lock_table *lock_tbl)
{
   // TODO: destroy all elements

   free(lock_tbl);
}

void
lock_table_insert(lock_table          *lock_tbl,
                  slice                start,
                  slice                last,
                  transaction_op_meta *meta)
{
   interval_tree_node *new_node = interval_tree_node_create(start, last, meta);
   interval_tree_insert(new_node, &lock_tbl->root);
   transaction_op_meta_inc_ref(meta);
}

transaction_op_meta *
lock_table_lookup(lock_table *lock_tbl, slice start, slice last)
{
   interval_tree_node *node =
      interval_tree_iter_first(&lock_tbl->root, start, last);
   while (node) {
      // TODO: do something to find what we want
      node = interval_tree_iter_next(node, start, last);
   }

   return (node ? node->meta : 0);
}

void
lock_table_delete(lock_table *lock_tbl, slice start, slice last)
{
   interval_tree_node *node =
      interval_tree_iter_first(&lock_tbl->root, start, last);
   while (node) {
      node = interval_tree_iter_next(node, start, last);
   }

   if (node) {
      interval_tree_remove(node, &lock_tbl->root);
      transaction_op_meta_dec_ref(node->meta);
   }
}
