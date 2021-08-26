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

// FIXME: [nsarmicanic 2020-07-02] There is an inconsistency
// with range iterator limit 128. What is that limit?
// Should 128 increase?
// Should 1024 increate or decrease?
// Hard limit tall tree range query?
#define MAX_MERGE_ARITY (1024)

typedef struct ordered_iterator {
   iterator *itor;
   int seq;
   slice key;
   slice data;
   data_type type;
   bool next_key_equal;
} ordered_iterator;

/*
 * range_stack structure is used by merge iterator to keep track
 * of range deletes.
 * For invariants of range stack take a look at: range_stack_check_invariants
 *
 */
// FIXME: [nsarmicanic 2020-07-29] maybe move range_stack to scratch
typedef struct range_stack {
   uint32     size;
   char       limits_bufs[MAX_MERGE_ARITY][MAX_KEY_SIZE];
   slice limits[MAX_MERGE_ARITY];
   char       start_key_buffer[MAX_KEY_SIZE];
   slice start_key;
   int        end_seq;
   int        seq[MAX_MERGE_ARITY];
   int        num_sequences;
   bool       has_start_key;
} range_stack;

typedef struct merge_iterator {
   iterator               super;           // handle for iterator.h API
   int                    num_trees;       // number of trees in the forest
   bool                   discard_deletes; // Whether to emit delete messages
   bool                   resolve_updates; // Whether to merge updates with NULL
   bool                   has_data;        // Whether to look at data at all
   bool                   at_end;
   int                    num_remaining;   // number of ritors not at end
   // FIXME: [yfogel 2020-07-01] rename cfg to point_cfg/delayed for paralelizem
   data_config           *cfg;             // point message tree data config
   data_config           *range_cfg;       // range delete tree data config
   slice             key;             // next key
   slice             data;            // next data
   data_type              type;            // next data type

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

   // FIXME: [yfogel 2020-07-01] these two are for use for merge_iterator
   //       range delete changes.  Feel free to rename (not used at all yet)
   char                   start_key_buffer[MAX_KEY_SIZE];
   char                   end_key_buffer[MAX_KEY_SIZE];

   range_stack stack;

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
                      data_config      *range_cfg,
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
