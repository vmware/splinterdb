// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * merge.h --
 *
 *    Merging functionality, notably merge iterators.
 */

#ifndef __MERGE_H
#define __MERGE_H

#include "btree.h"
#include "iterator.h"
#include "platform.h"

// Hard limit tall tree range query?
#define MAX_MERGE_ARITY (1024)

typedef struct ordered_iterator {
   iterator *itor;
   int seq;
   char *key;
   char *data;
   bool next_key_equal;
} ordered_iterator;


typedef struct merge_iterator {
   iterator               super;           // handle for iterator.h API
   int                    num_trees;       // number of trees in the forest
   bool                   discard_deletes; // Whether to emit delete messages
   bool                   resolve_updates; // Whether to merge updates with NULL
   bool                   has_data;        // Whether to look at data at all
   bool                   at_end;
   int                    num_remaining;   // number of ritors not at end
   data_config           *cfg;             // point message tree data config
   char                  *key;             // next key
   char                  *data;            // next data

   // FIXME: [nsarmicanic 2020-07-02] Maybe preallocate one of these sections
   // pre thread?

   // Padding so ordered_iterators[-1] is valid
   ordered_iterator       ordered_iterator_stored_pad;
   ordered_iterator       ordered_iterator_stored[MAX_MERGE_ARITY];
   // Padding so ordered_iterator_ptr_arr[-1] is valid
   // Must be immediately before ordered_iterator_ptr_arr
   ordered_iterator      *ordered_iterators_pad;
   ordered_iterator      *ordered_iterators[MAX_MERGE_ARITY];

   // Stats
   uint64                 discarded_deletes;

   // space for merging data together
   char                   merge_buffer[MAX_MESSAGE_SIZE];
} merge_iterator;

// Statically enforce that the padding variables act as index -1 for both arrays
_Static_assert(offsetof(merge_iterator, ordered_iterator_stored_pad) ==
               offsetof(merge_iterator, ordered_iterator_stored[-1]), "");
_Static_assert(offsetof(merge_iterator, ordered_iterators_pad) ==
               offsetof(merge_iterator, ordered_iterators[-1]), "");

platform_status
merge_iterator_create(platform_heap_id  hid,
                      data_config      *cfg,
                      int               num_trees,
                      iterator        **itor_arr,
                      bool              discard_deletes,
                      bool              resolve_updates,
                      bool              has_data,
                      merge_iterator  **out_itor);

platform_status
merge_iterator_destroy(platform_heap_id hid, merge_iterator **merge_itor);

void
merge_iterator_print(merge_iterator *merge_itor);

#endif // __BTREE_MERGE_H
