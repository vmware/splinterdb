// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * avlTree.c -- implementation of a generic AVL tree
 */

#ifndef AVL_TREE_TEST
#   include "platform.h"
#endif

#include "avlTree.h"

#include "poison.h"


static inline void
AvlTreeSetNode(AvlTreeLinks *n,
               AvlTreeLinks *left,
               AvlTreeLinks *right,
               uint32        height)
{
   n->left   = left;
   n->right  = right;
   n->height = height;
}


static inline uint32
AvlTreeGetHeight(AvlTreeLinks *n)
{
   if (UNLIKELY(n == NULL)) {
      return 0;
   }

   return n->height;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTree_Init --
 *
 *    Init an AvlTree structure.
 *
 * Results:
 *    None.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

void
AvlTree_Init(AvlTree              *tree,    // IN/OUT
             AvlTreeLinks         *root,    // IN
             AvlTreeNodeComparator nodeCmp, // IN
             AvlTreeKeyComparator  keyCmp)   // IN
{
   tree->nodeCmp = nodeCmp;
   tree->keyCmp  = keyCmp;
   tree->root    = root;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTree_InitNode --
 *
 *    Initializes an AvlTreeLinks.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

void
AvlTree_InitNode(AvlTreeLinks *node)
{
   AvlTreeSetNode(node, NULL, NULL, 0);
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTree_IsUnlinked --
 *
 *    Checks if a node is part of a tree. Nodes are assumed
 *    to be initialized using InitNode.
 *
 * Results:
 *    TRUE if the node is not part of a tree.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

bool32
AvlTree_IsUnlinked(AvlTreeLinks *node)
{
   return node->left == NULL && node->right == NULL && node->height == 0;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeUpdateHeight --
 *
 *    Updates the height of a subtree based on the heights of
 *    the left and right subtrees.
 *
 * Results:
 *    None.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

static void
AvlTreeUpdateHeight(const AvlTree *tree, // IN
                    AvlTreeLinks  *n,    // IN/OUT
                    AvlTreeLinks  *l,    // IN
                    AvlTreeLinks  *r)     // IN
{
   uint32 lHeight = 0;
   uint32 rHeight = 0;

   if (l != NULL) {
      lHeight = AvlTreeGetHeight(l);
   }
   if (r != NULL) {
      rHeight = AvlTreeGetHeight(r);
   }

   n->height = (lHeight > rHeight ? lHeight : rHeight) + 1;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeRotateLeft --
 *
 *    Performs a "left rotation", where a node's left child is moved up:
 *
 *       n1                n2
 *      /  \              /  \
 *     n2   C     ->     A    n1
 *    /  \                   /  \
 *   A    B                 B    C
 *
 * Results:
 *    New subtree root.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

static AvlTreeLinks *
AvlTreeRotateLeft(const AvlTree *tree, // IN
                  AvlTreeLinks  *n1,   // IN/OUT
                  AvlTreeLinks  *n2)    // IN/OUT
{
   n1->left  = n2->right;
   n2->right = n1;

   AvlTreeUpdateHeight(tree, n1, n1->left, n1->right);
   AvlTreeUpdateHeight(tree, n2, n2->left, n1);
   return n2;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeRotateRight --
 *
 *    Performs a "right rotation", where a node's right child is moved up:
 *
 *     n2                   n1
 *    /  \                 /  \
 *   A    n1      ->      n2   C
 *       /  \            /  \
 *      B    C          A    B
 *
 * Results:
 *    New subtree root.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

static AvlTreeLinks *
AvlTreeRotateRight(const AvlTree *tree, // IN
                   AvlTreeLinks  *n2,   // IN/OUT
                   AvlTreeLinks  *n1)    // IN/OUT
{
   n2->right = n1->left;
   n1->left  = n2;

   AvlTreeUpdateHeight(tree, n2, n2->left, n2->right);
   AvlTreeUpdateHeight(tree, n1, n2, n1->right);
   return n1;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeRebalanceNode --
 *
 *    Rebalances the subtree rooted at the given node, if necessary.
 *
 * Results:
 *    New subtree root.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

static AvlTreeLinks *
AvlTreeRebalanceNode(const AvlTree *tree, // IN/OUT
                     AvlTreeLinks  *n,    // IN/OUT
                     AvlTreeLinks  *l,    // IN/OUT
                     AvlTreeLinks  *r)     // IN/OUT
{
   uint32 lHeight = AvlTreeGetHeight(l);
   uint32 rHeight = AvlTreeGetHeight(r);

   if (lHeight > rHeight + 1) {
      AvlTreeLinks *ll = l->left;
      AvlTreeLinks *lr = l->right;

      if (AvlTreeGetHeight(lr) > AvlTreeGetHeight(ll)) {
         /* Left child is right-heavy, do a double rotation. */
         l       = AvlTreeRotateRight(tree, l, lr);
         n->left = l;
      }
      n = AvlTreeRotateLeft(tree, n, l);
   } else if (rHeight > lHeight + 1) {
      AvlTreeLinks *rl = r->left;
      AvlTreeLinks *rr = r->right;

      if (AvlTreeGetHeight(rl) > AvlTreeGetHeight(rr)) {
         /* Right child is left-heavy, do a double rotation. */
         r        = AvlTreeRotateLeft(tree, r, rl);
         n->right = r;
      }
      n = AvlTreeRotateRight(tree, n, r);
   }

   platform_assert(AvlTreeGetHeight(n->left) <= AvlTreeGetHeight(n->right) + 1);

   platform_assert(AvlTreeGetHeight(n->right) <= AvlTreeGetHeight(n->left) + 1);

   return n;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeInsertInt --
 *
 *    Inserts newNode in the subtree rooted at n.
 *
 * Results:
 *    New subtree root.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

static AvlTreeLinks *
AvlTreeInsertInt(const AvlTree *tree,    // IN
                 AvlTreeLinks  *newNode, // IN/OUT
                 AvlTreeLinks  *n)        // IN/OUT
{
   AvlTreeLinks *l, *r;
   int           cmp;

   if (n == NULL) {
      AvlTreeSetNode(newNode, NULL, NULL, 1);
      return newNode;
   }

   cmp = tree->nodeCmp(newNode, n);
   if (cmp == 0) {
      platform_assert(0);
      /* Duplicate node, ignore the insert. */
      return n;
   }

   l = n->left;
   r = n->right;

   if (cmp < 0) {
      l       = AvlTreeInsertInt(tree, newNode, l);
      n->left = l;
   } else {
      r        = AvlTreeInsertInt(tree, newNode, r);
      n->right = r;
   }
   AvlTreeUpdateHeight(tree, n, l, r);
   return AvlTreeRebalanceNode(tree, n, l, r);
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTree_Insert --
 *
 *    Insert a preallocated node in the tree.
 *
 *    The node must not be equal to any other node in the tree.
 *
 * Results:
 *    None.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

void
AvlTree_Insert(AvlTree      *tree, // IN/OUT
               AvlTreeLinks *node) // IN/OUT
{
   AvlTreeLinks *root = tree->root;
   platform_assert(node != NULL);

   root       = AvlTreeInsertInt(tree, node, root);
   tree->root = root;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeDeleteInt --
 *
 *    Deletes nodeToDel from the subtree rooted at n.
 *
 * Results:
 *    New subtree root.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

static AvlTreeLinks *
AvlTreeDeleteInt(const AvlTree *tree,      // IN
                 AvlTreeLinks  *nodeToDel, // IN/OUT
                 AvlTreeLinks  *n)          // IN/OUT
{
   AvlTreeLinks *l, *r;
   int           cmp;

   if (n == NULL) {
      /* We did not find the node. */
      platform_assert(0);
      return n;
   }

   l   = n->left;
   r   = n->right;
   cmp = tree->nodeCmp(nodeToDel, n);
   if (cmp == 0) {
      AvlTreeLinks *succ;

      platform_assert(n == nodeToDel);
      /* If node has a single child, just replace with the child. */

      if (l == NULL || r == NULL) {
         return l == NULL ? r : l;
      }

      /* Find successor in subtree (keep going left from the right child). */
      succ = r;
      for (succ = r; succ->left != NULL; succ = succ->left)
         ;

      /*
       * Delete succ from the right subtree (note: we know that next time we
       * will end up in the "easy" case above since succ has no left child).
       */
      r = AvlTreeDeleteInt(tree, succ, r);

      /* Now we reuse the succ node as the new root of our subtree. */
      n        = succ;
      n->left  = l;
      n->right = r;
   } else if (cmp < 0) {
      l       = AvlTreeDeleteInt(tree, nodeToDel, l);
      n->left = l;
   } else {
      r        = AvlTreeDeleteInt(tree, nodeToDel, r);
      n->right = r;
   }
   AvlTreeUpdateHeight(tree, n, l, r);
   return AvlTreeRebalanceNode(tree, n, l, r);
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTree_Delete --
 *
 *    Deletes the given node from the tree.
 *
 * Results:
 *    None.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

void
AvlTree_Delete(AvlTree      *tree, // IN/OUT
               AvlTreeLinks *node) // IN/OUT
{
   AvlTreeLinks *root = tree->root;

   root       = AvlTreeDeleteInt(tree, node, root);
   tree->root = root;

   /*
    * We must reset links to NIL to prevent any loops in concurrent queries,
    * and for AvlTree_IsUnlinked.
    */
   AvlTree_InitNode(node);
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTree_FindNode --
 *
 *    Finds the node equal to the given key.
 *
 * Results:
 *    Node or NULL.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

AvlTreeLinks *
AvlTree_FindNode(const AvlTree *tree, // IN
                 AvlTreeKey     key)      // IN
{
   AvlTreeLinks *n = tree->root;

   while (n != NULL) {
      int cmp = tree->keyCmp(n, key);
      if (cmp == 0) {
         return n;
      }
      if (cmp > 0) {
         n = n->left;
      } else {
         n = n->right;
      }
   }
   return NULL;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTree_FindNodeGeq --
 *
 *    Finds the smallest node greater than or equal to the given key.
 *
 * Results:
 *    Node or NULL.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

AvlTreeLinks *
AvlTree_FindNodeGeq(const AvlTree *tree, // IN
                    AvlTreeKey     key)      // IN
{
   AvlTreeLinks *res     = NULL;
   AvlTreeLinks *current = tree->root;

   while (current != NULL) {
      int cmp = tree->keyCmp(current, key);
      if (cmp == 0) {
         return current;
      }
      if (cmp > 0) {
         res     = current;
         current = current->left;
      } else {
         current = current->right;
      }
   }
   return res;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTree_FindNodeLeq --
 *
 *    Finds the greatest node less than or equal to the given key.
 *
 * Results:
 *    Node or NULL.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

AvlTreeLinks *
AvlTree_FindNodeLeq(const AvlTree *tree, // IN
                    AvlTreeKey     key)      // IN
{
   AvlTreeLinks *res     = NULL;
   AvlTreeLinks *current = tree->root;

   while (current != NULL) {
      int cmp = tree->keyCmp(current, key);
      if (cmp == 0) {
         return current;
      }
      if (cmp < 0) {
         res     = current;
         current = current->right;
      } else {
         current = current->left;
      }
   }
   return res;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTree_Min --
 *
 *    Returns the smallest node in the tree.
 *
 * Results:
 *    Node or NULL if the tree is empty.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

AvlTreeLinks *
AvlTree_Min(const AvlTree *tree) // IN
{
   AvlTreeLinks *res = NULL;
   AvlTreeLinks *n   = tree->root;
   while (n != NULL) {
      res = n;
      n   = n->left;
   }
   return res;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTree_Max --
 *
 *    Returns the largest node in the tree.
 *
 * Results:
 *    Node or NULL if the tree is empty.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

AvlTreeLinks *
AvlTree_Max(const AvlTree *tree) // IN
{
   AvlTreeLinks *res = NULL;
   AvlTreeLinks *n   = tree->root;
   while (n != NULL) {
      res = n;
      n   = n->right;
   }
   return res;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTree_Successor --
 *
 *    Returns the successor of a given node.
 *
 * Results:
 *    Node or NULL if the node was the maximum node.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

AvlTreeLinks *
AvlTree_Successor(const AvlTree      *tree, // IN
                  const AvlTreeLinks *node) // IN
{
   AvlTreeLinks *res     = NULL;
   AvlTreeLinks *current = tree->root;

   while (current != NULL) {
      if (tree->nodeCmp(node, current) < 0) {
         res     = current;
         current = current->left;
      } else {
         current = current->right;
      }
   }
   return res;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTree_Predecessor --
 *
 *    Returns the predecessor of a given node.
 *
 * Results:
 *    Node or NULL if the node was the minimum node.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

AvlTreeLinks *
AvlTree_Predecessor(const AvlTree      *tree, // IN
                    const AvlTreeLinks *node) // IN
{
   AvlTreeLinks *res     = NULL;
   AvlTreeLinks *current = tree->root;

   while (current != NULL) {
      if (tree->nodeCmp(node, current) > 0) {
         res     = current;
         current = current->right;
      } else {
         current = current->left;
      }
   }
   return res;
}

/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeStackInit --
 *
 *      Initialize a stack of avl tree nodes
 *
 * Results:
 *      None
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

static void
AvlTreeStackInit(AvlTreeIter *iter, unsigned max)
{
   iter->num = 0;
   iter->max = max;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeStackPush --
 *
 *      Push a node onto a stack
 *
 * Results:
 *      None
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

static void
AvlTreeStackPush(AvlTreeIter *iter, AvlTreeLinks *cur)
{
   platform_assert(iter->num < iter->max);
   iter->stack[iter->num++] = cur;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeStackPop --
 *
 *      Pop a node from a stack
 *
 * Results:
 *      Avl tree node pointer.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

static AvlTreeLinks *
AvlTreeStackPop(AvlTreeIter *iter)
{
   platform_assert(iter->num > 0);
   return iter->stack[--iter->num];
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeStackIsEmpty --
 *
 *      Find if a stack is empty
 *
 * Results:
 *      TRUE if empty, FALSE otherwise
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

static bool32
AvlTreeStackIsEmpty(AvlTreeIter *iter)
{
   return iter->num == 0;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeIterFindSmallest --
 *
 *      Find the smallest node descending the tree from cur node, pushing
 *      nodes seen to the stack. Point cur to the smallest node.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

static void
AvlTreeIterFindSmallest(AvlTreeIter  *iter, // IN iterator
                        AvlTreeLinks *cur)  // IN node to start search
{
   while (cur != NULL) {
      AvlTreeStackPush(iter, cur);
      cur = cur->left;
   }
   if (AvlTreeStackIsEmpty(iter)) {
      iter->cur = NULL;
   } else {
      iter->cur = AvlTreeStackPop(iter);
   }
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeIter_Init --
 *
 *      Initialize the avl tree iterator.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

void
AvlTreeIter_Init(AvlTreeIter *iter, // IN iterator
                 unsigned     max,  // IN maximum height of the tree
                 AvlTree     *tree)     // IN tree
{
   iter->tree = tree;
   AvlTreeStackInit(iter, max);
   AvlTreeIterFindSmallest(iter, tree->root);
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeIter_IsAtEnd --
 *
 *      Find if avl tree iterator reached the end.
 *
 * Results:
 *      TRUE if at end, FALSE otherwise.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

bool32
AvlTreeIter_IsAtEnd(AvlTreeIter *iter)
{
   return iter->cur == NULL;
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeIter_Advance --
 *
 *      Advance the avl tree iterator. The iterator is based upon a non-
 *      recursive in-order traversal of the tree. The iterator is initialized
 *      by traversing from root till the left-most leaf, pushing nodes on
 *      the stack. Then pop from the stack and that's where the iterator
 *      points. The next advance will do the same procedure by starting from
 *      the right child of the popped node instead of the root.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

void
AvlTreeIter_Advance(AvlTreeIter *iter)
{
   platform_assert(!AvlTreeIter_IsAtEnd(iter));
   AvlTreeIterFindSmallest(iter, iter->cur->right);
}


/*
 *-----------------------------------------------------------------------------
 *
 * AvlTreeIter_GetCurrent --
 *
 *      Get the node pointed to by the avl tree iterator.
 *
 * Results:
 *      Avl tree node.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

AvlTreeLinks *
AvlTreeIter_GetCurrent(AvlTreeIter *iter)
{
   platform_assert(!AvlTreeIter_IsAtEnd(iter));

   return iter->cur;
}
