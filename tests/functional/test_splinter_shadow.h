// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinter_shadow_test.h --
 *
 * This file implements a in memory shadow KV Store functionality for splinter
 * db.
 */

#ifndef TEST_SPLINTER_SHADOW_H
#define TEST_SPLINTER_SHADOW_H

#include "platform.h"
#include "avlTree.h"

typedef struct test_splinter_shadow_node {
   AvlTreeLinks treeLink;
   uint64       key;
   uint64       value;
} test_splinter_shadow_node;

typedef struct test_splinter_shadow_tree {
   uint64                     numKeys;
   AvlTree                    tree;
   uint64                     numPreAllocatedNodes;
   uint64                     currentAllocIdx;
   buffer_handle              nodes_buffer;
   test_splinter_shadow_node *nodes;
} test_splinter_shadow_tree;


typedef struct test_splinter_shadow_array {
   uint64        nkeys;
   buffer_handle buffer;
   uint64       *keys;
   int8         *ref_counts;
} test_splinter_shadow_array;


platform_status
test_splinter_shadow_create(test_splinter_shadow_tree **tree,
                            platform_heap_id            hid,
                            uint64                      max_operations);

/*
 *-----------------------------------------------------------------------------
 *
 *  test_splinter_shadow_count --
 *
 *       Function to return the number of keys in the shadow tree.
 *
 *   Results:
 *       Returns the number of keys in the shadow tree.
 *
 *   Side Effects:
 *       None.
 *-----------------------------------------------------------------------------
 */

static inline uint64
test_splinter_shadow_count(test_splinter_shadow_tree *tree)
{
   return tree->numKeys;
}


bool32
test_splinter_shadow_lookup(test_splinter_shadow_tree *tree,
                            uint64                    *key,
                            uint64                    *val);

platform_status
test_splinter_shadow_add(test_splinter_shadow_tree *tree,
                         uint64                    *key,
                         uint64                     value);
void
test_splinter_shadow_destroy(platform_heap_id           hid,
                             test_splinter_shadow_tree *tree);

platform_status
test_splinter_build_shadow_array(test_splinter_shadow_tree  *tree,
                                 test_splinter_shadow_array *shadow_array);

#endif
