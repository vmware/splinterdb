// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinter_shadow_test.c --
 *
 * This file implements an in-memory shadow KV Store functionality for
 * splinterDB. This is used during functionality tests to validate
 * against splinterDB.  This uses a generic AVL Tree as the backing data
 * structure. The usage of this is expectd to be in cases where
 * performance is not of paramount importance. Also this implementation
 * is not thread safe.
 *
 *
 * Note that some methods are implemented using recursion.  However,
 * the recursion is bounded to 64 levels for all practical purposes,
 * and the stack frames are quite small, so we don't expect this to be
 * a problem even in environments with constrained stack sizes.
 */
#include "platform.h"

#include "test_splinter_shadow.h"

#include "poison.h"

/*
 *-----------------------------------------------------------------------------
 *
 *  test_splinter_shadow_cmp_node --
 *
 *       Key Comparator for a node against a key in the shadow tree.
 *
 *  Results:
 *       0 if the node and key are equal, -1 if node < key, 1 otherwise.
 *
 *  Side Effects:
 *       None
 *-----------------------------------------------------------------------------
 */

static int
test_splinter_shadow_cmp_node(const AvlTreeLinks *k1, AvlTreeKey key)
{
   uint64 val1 = ((test_splinter_shadow_node *)k1)->key;
   uint64 val2 = *(uint64 *)key;

   if (val1 == val2) {
      return 0;
   }

   if (val1 < val2) {
      return -1;
   }
   return 1;
}


/*
 *-----------------------------------------------------------------------------
 *
 *  test_splinter_shadow_cmp_keys --
 *
 *       Key Comparator for two keys in the shadow tree.
 *
 *  Results:
 *       0 if the keys are equal, -1 if k1 < k2, 1 otherwise.
 *
 *  Side Effects:
 *       None
 *-----------------------------------------------------------------------------
 */

static int
test_splinter_shadow_cmp_keys(const AvlTreeLinks *k1, const AvlTreeLinks *k2)
{
   test_splinter_shadow_node *node2 = (test_splinter_shadow_node *)k2;

   return test_splinter_shadow_cmp_node(k1, &node2->key);
}


/*
 *-----------------------------------------------------------------------------
 *  test_splinter_shadow_init --
 *       Function to initialize memory and avlTree for the shadow tree.
 *
 *   Results:
 *       STATUS_OK if successful, appropriate error code otherwise.
 *
 *   Side Effects:
 *       Will allocate memory for the shadow structure.
 *-----------------------------------------------------------------------------
 */
platform_status
test_splinter_shadow_create(test_splinter_shadow_tree **tree,
                            platform_heap_id            hid,
                            uint64                      max_operations)
{
   platform_status rc = STATUS_NO_MEMORY;

   test_splinter_shadow_tree *shadow = TYPED_ZALLOC(hid, shadow);
   if (shadow == NULL) {
      platform_default_log("Failed to allocate memory for shadow init");
      return rc;
   }

   /*
    * XXX : We are allocating for the worst case here. In the future, if need
    * be we can probably optimize this to only allocate for the number of
    * insert operations, since only those operations will require new node
    * allocations.
    */
   rc =
      platform_buffer_init(&shadow->nodes_buffer,
                           sizeof(test_splinter_shadow_node) * max_operations);
   if (!SUCCESS(rc)) {
      platform_default_log("Failed to pre allocate nodes for shadow tree\n");
      platform_free(hid, shadow);
      return rc;
   }
   shadow->nodes = platform_buffer_getaddr(&shadow->nodes_buffer);
   shadow->numPreAllocatedNodes = max_operations;

   AvlTree_Init(&shadow->tree,
                NULL,
                test_splinter_shadow_cmp_keys,
                test_splinter_shadow_cmp_node);
   *tree = shadow;
   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 *
 *  test_splinter_shadow_lookup --
 *
 *       Function to lookup a key in the shadow tree.
 *
 *   Results:
 *       Returns TRUE if the key is found and updates the value, FALSE
 *       otherwise.
 *
 *   Side Effects:
 *       None.
 *-----------------------------------------------------------------------------
 */

bool32
test_splinter_shadow_lookup(test_splinter_shadow_tree *tree,
                            uint64                    *key,
                            uint64                    *val)
{
   AvlTreeLinks *link = AvlTree_FindNode(&tree->tree, key);
   if (link == NULL) {
      return FALSE;
   } else {
      _Static_assert(
         offsetof(test_splinter_shadow_node, treeLink) == 0,
         "Code relies on shadow node and treeLink pointer being equal");

      test_splinter_shadow_node *node = (test_splinter_shadow_node *)link;
      *val                            = node->value;
      return TRUE;
   }
}


/*
 *-----------------------------------------------------------------------------
 *
 *  test_splinter_shadow_get_node --
 *
 *       Function to get a node for the shadow tree. If there is an available
 *       pre allocated node, it returns that else will try to malloc and
 *       return a valid node.
 *
 *   Results:
 *       Valid pointer if successful, NULL otherwise.
 *
 *   Side Effects:
 *       Will allocate memory if out of pre allocated nodes.
 *-----------------------------------------------------------------------------
 */

static inline test_splinter_shadow_node *
test_splinter_shadow_get_node(test_splinter_shadow_tree *tree)
{
   /*
    * We pre-allocate memory for all the nodes during init, so we should
    * never run out of pre-allocated nodes.
    */

   platform_assert(tree->currentAllocIdx < tree->numPreAllocatedNodes);
   return &tree->nodes[tree->currentAllocIdx++];
}


/*
 *-----------------------------------------------------------------------------
 *
 *  test_splinter_shadow_add --
 *
 *       Function to add a (key, value) tuple in the shadow tree.
 *
 *   Results:
 *       Returns STATUS_OK if the tuple is added successfully, appropriate
 *       error code otherwise.
 *
 *   Side Effects:
 *       Will allocate memory for the node in the shadow tree,
 *-----------------------------------------------------------------------------
 */

platform_status
test_splinter_shadow_add(test_splinter_shadow_tree *tree,
                         uint64                    *key,
                         uint64                     value)
{
   /*
    * Since the tree doesnt handle duplicates, if the key already exists we
    * just need to update the value in place.
    */
   AvlTreeLinks *link = AvlTree_FindNode(&tree->tree, key);

   if (link != NULL) {
      test_splinter_shadow_node *node = (test_splinter_shadow_node *)link;
      node->value                     = value;
      return STATUS_OK;
   }

   test_splinter_shadow_node *node = test_splinter_shadow_get_node(tree);

   if (node == NULL) {
      return STATUS_NO_MEMORY;
   }

   node->key   = *key;
   node->value = value;
   AvlTree_InitNode(&node->treeLink);

   AvlTree_Insert(&tree->tree, &node->treeLink);
   tree->numKeys++;
   return STATUS_OK;
}


/*
 *-----------------------------------------------------------------------------
 *  test_splinter_shadow_destroy_tree --
 *
 *       Function to cleanup the shadow tree structure.
 *
 *   Results:
 *       None.
 *
 *   Side Effects:
 *       Will free memory for the for shadow tree structure itself.
 *-----------------------------------------------------------------------------
 */

void
test_splinter_shadow_destroy(platform_heap_id           hid,
                             test_splinter_shadow_tree *tree)
{
   platform_buffer_deinit(&tree->nodes_buffer);
   tree->numKeys = 0;
   platform_free(hid, tree);
}

/*
 *-----------------------------------------------------------------------------
 *
 *  test_splinter_shadow_iterate_tree --
 *
 *       Function to recurse over the tree and copy the key,value into an
 *       array using an inorder traversal.
 *
 *   Results:
 *       None.
 *
 *   Side Effects:
 *       Noe.
 *-----------------------------------------------------------------------------
 */

static void
test_splinter_shadow_iterate_tree(AvlTreeLinks               *root,
                                  test_splinter_shadow_array *array,
                                  uint64                     *idx)
{
   if (root == NULL) {
      return;
   }
   test_splinter_shadow_iterate_tree(root->left, array, idx);
   test_splinter_shadow_node *node = (test_splinter_shadow_node *)root;
   platform_assert(*idx < array->nkeys);
   array->keys[*idx]       = node->key;
   array->ref_counts[*idx] = node->value;
   (*idx)++;
   test_splinter_shadow_iterate_tree(root->right, array, idx);
}


/*
 *-----------------------------------------------------------------------------
 *  test_splinter_build_shadow_array --
 *
 *       Function to recurse over the tree and copy the key.value into an
 *       array.
 *
 *   Results:
 *       STATUS_OK if successful, appropriate error code otherwise.
 *
 *   Side Effects:
 *       Will allocate memory for the key and value arrays.
 *-----------------------------------------------------------------------------
 */
platform_status
test_splinter_build_shadow_array(test_splinter_shadow_tree  *tree,
                                 test_splinter_shadow_array *shadow_array)
{
   uint64 idx          = 0;
   shadow_array->nkeys = tree->numKeys;
   uint64 totalBufferSize =
      (sizeof(*shadow_array->keys) + sizeof(*shadow_array->ref_counts))
      * tree->numKeys;

   platform_status rc = STATUS_NO_MEMORY;

   rc = platform_buffer_init(&shadow_array->buffer, totalBufferSize);
   if (!SUCCESS(rc)) {
      platform_default_log("Failed to allocate memory for shadow array\n");
      return rc;
   }
   shadow_array->keys = platform_buffer_getaddr(&shadow_array->buffer);
   // Memory for ref count array starts at base memory + memory for keys array.
   shadow_array->ref_counts =
      (void *)((uint64)shadow_array->keys
               + sizeof(*shadow_array->keys) * tree->numKeys);

   test_splinter_shadow_iterate_tree(tree->tree.root, shadow_array, &idx);
   platform_assert(idx == shadow_array->nkeys);
   return STATUS_OK;
}
