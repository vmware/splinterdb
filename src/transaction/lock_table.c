#include "lock_table.h"

typedef struct interval_tree_node {
   struct rb_node   rb;
   uint64           start;
   uint64           last;
   uint64           __subtree_last;
   lock_table_entry entry;
} interval_tree_node;

interval_tree_node *
interval_tree_node_create(uint64         start,
                          uint64         last,
                          transaction_id txn_id,
                          message_type   op)
{
   interval_tree_node *node =
      (interval_tree_node *)malloc(sizeof(interval_tree_node));
   RB_CLEAR_NODE(&node->rb);
   node->start        = start;
   node->last         = last;
   node->entry.txn_id = txn_id;
   node->entry.op     = op;

   return node;
}

#define GET_ITSTART(n) (n->start)
#define GET_ITLAST(n)  (n->last)

INTERVAL_TREE_DEFINE(interval_tree_node,
                     rb,
                     uint64,
                     __subtree_last,
                     GET_ITSTART,
                     GET_ITLAST,
                     static,
                     interval_tree);


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
lock_table_insert(lock_table    *lock_tbl,
                  uint64         start,
                  uint64         last,
                  transaction_id txn_id,
                  message_type   op)
{
   interval_tree_node *new_node =
      interval_tree_node_create(start, last, txn_id, op);
   interval_tree_insert(new_node, &lock_tbl->root);
}

lock_table_entry *
lock_table_lookup(lock_table *lock_tbl, uint64 start, uint64 last)
{
   interval_tree_node *node =
      interval_tree_iter_first(&lock_tbl->root, start, last);
   while (node) {

      node = interval_tree_iter_next(node, start, last);
   }

   return (node ? &node->entry : 0);
}

void
lock_table_delete(lock_table *lock_tbl, uint64 start, uint64 last)
{
   interval_tree_node *node =
      interval_tree_iter_first(&lock_tbl->root, start, last);
   while (node)
      node = interval_tree_iter_next(node, start, last);

   if (node)
      interval_tree_remove(node, &lock_tbl->root);
}
