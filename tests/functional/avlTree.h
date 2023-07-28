// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * avlTree.h --
 *
 * A generic implementation of Adelson-Velskii&Landis trees.
 *
 */

#ifndef AVLTREE_H
#define AVLTREE_H

#include "platform.h"

/*
 * Per-node structure used to store the tree.
 *
 * This structure should be embedded in a larger structure that contains the
 * appropriate per-node data that the comparator callback implementations need.
 */
typedef struct AvlTreeLinks {
   struct AvlTreeLinks *left;
   struct AvlTreeLinks *right;
   uint32               height;
} AvlTreeLinks;

/*
 * Tree lookups are performed using an opaque key, which is
 * passed to the key comparator.
 */
typedef void *AvlTreeKey;

struct AvlTree;

/*
 * A function that compares two nodes.  Return value is:
 *    0   if the nodes are equal,
 *    < 0 if node a is less than b
 *    > 0 if node a is greater than b
 *
 * The tree can not contain two nodes that are equal.
 */
typedef int (*AvlTreeNodeComparator)(const AvlTreeLinks *a,
                                     const AvlTreeLinks *b);

/*
 * A function that compares a node with a key.
 * Return value is:
 *    0   if the nodes and key are equal,
 *    < 0 if node is less than key
 *    > 0 if node is greater than key
 *
 * No two nodes in the tree should be equal to the same key.
 */
typedef int (*AvlTreeKeyComparator)(const AvlTreeLinks *node, AvlTreeKey key);

/*
 * AvlTree structure.
 *
 * This can be embedded in a larger structure that contains more
 * per-tree data that the comparator callbacks can use. A notable
 * example is storing an offset that can be used to map an
 * AvlTreeLinks pointer to the enclosing node structure.
 */
typedef struct AvlTree {
   AvlTreeLinks         *root;
   AvlTreeNodeComparator nodeCmp;
   AvlTreeKeyComparator  keyCmp;
} AvlTree;

void
AvlTree_Init(AvlTree              *tree,
             AvlTreeLinks         *root,
             AvlTreeNodeComparator nodeCmp,
             AvlTreeKeyComparator  keyCmp);

void
AvlTree_InitNode(AvlTreeLinks *node);

bool32
AvlTree_IsUnlinked(AvlTreeLinks *node);

/* Insertion / deletion */

void
AvlTree_Insert(AvlTree *tree, AvlTreeLinks *node);

void
AvlTree_Delete(AvlTree *tree, AvlTreeLinks *node);

/* Lookups by key */

AvlTreeLinks *
AvlTree_FindNode(const AvlTree *tree, AvlTreeKey key);

AvlTreeLinks *
AvlTree_FindNodeGeq(const AvlTree *tree, AvlTreeKey key);

AvlTreeLinks *
AvlTree_FindNodeLeq(const AvlTree *tree, AvlTreeKey key);

/* Iterating */

AvlTreeLinks *
AvlTree_Min(const AvlTree *tree);

AvlTreeLinks *
AvlTree_Max(const AvlTree *tree);

AvlTreeLinks *
AvlTree_Successor(const AvlTree *tree, const AvlTreeLinks *node);

AvlTreeLinks *
AvlTree_Predecessor(const AvlTree *tree, const AvlTreeLinks *node);

/*
 * Faster iterator that uses a stack and doesn't require traversing
 * root-to-leaf path on every advance.
 */

typedef struct {
   AvlTree      *tree;
   unsigned      num;
   unsigned      max;
   AvlTreeLinks *cur;
   AvlTreeLinks *stack[0];
} AvlTreeIter;


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeIter_AllocSize --
 *
 *      Calculate the size of memory needed for an avl tree given it's
 *      maximum height.
 *
 * Results:
 *      Memory size
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

static inline unsigned
AvlTreeIter_AllocSize(unsigned max) // IN height of the avl tree
{
   platform_assert(max > 0);
   return sizeof(AvlTreeIter) + max * sizeof(AvlTreeLinks *);
}

void
AvlTreeIter_Init(AvlTreeIter *iter, unsigned max, AvlTree *tree);

bool32
AvlTreeIter_IsAtEnd(AvlTreeIter *iter);

void
AvlTreeIter_Advance(AvlTreeIter *iter);

AvlTreeLinks *
AvlTreeIter_GetCurrent(AvlTreeIter *iter);

#endif // AVLTREE_H
