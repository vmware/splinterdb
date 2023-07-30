// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * merge.h --
 *
 *    Merging functionality, notably merge iterators.
 */

#pragma once

#include "data_internal.h"
#include "iterator.h"
#include "platform.h"

// Hard limit tall tree range query?
#define MAX_MERGE_ARITY (1024)

typedef struct ordered_iterator {
   iterator *itor;
   int       seq;
   key       curr_key;
   message   curr_data;
   bool32    next_key_equal;
} ordered_iterator;

/*
 * Merge iterators support 3 modes:
 *
 * - RAW.  Each key-value pair from the input iterators is output by
 *   the merge iterator.  The merge iterator does not merge messages
 *   for the same key.  The merge iterator emits delete messages.
 *   This is appropriate for, e.g. iterating over branch pivots in the
 *   splinter leaf splitting algorithm.
 *
 * - INTERMEDIATE.  The merge iterator merges messages for the same
 *   key but does not finalize updates or discard deletes.  Thus the
 *   resulting set of messages is semantically equivalent to the input
 *   sequence of messages.  This is appropriate for compactions that
 *   are not at the leaf of the splinter tree.
 *
 * - FULL.  Messages for the same key are merged, updates are
 *   finalized, and delete messages are discarded.  This is
 *   appropriate for compactions at trunk leaves and for splinter
 *   range iterators.
 *
 * This defines a type-safe flag that cannot be accidentally
 * converted from integers or other types and hence cannot be accidentally
 * reordered at call sites.  (enums can be mixed up or converted from other
 * types; ORed-together flags can be omitted).
 */
typedef struct merge_behavior *merge_behavior;
extern struct merge_behavior   merge_full, merge_intermediate, merge_raw;
#define MERGE_RAW          (&merge_raw)
#define MERGE_INTERMEDIATE (&merge_intermediate)
#define MERGE_FULL         (&merge_full)


typedef struct merge_iterator {
   iterator     super;     // handle for iterator.h API
   int          num_trees; // number of trees in the forest
   bool32       merge_messages;
   bool32       finalize_updates;
   bool32       emit_deletes;
   bool32       can_prev;
   bool32       can_next;
   int          num_remaining; // number of ritors not at end
   data_config *cfg;           // point message tree data config
   key          curr_key;      // current key
   message      curr_data;     // current data
   bool32       forwards;

   // Padding so ordered_iterators[-1] is valid
   ordered_iterator ordered_iterator_stored_pad;
   ordered_iterator ordered_iterator_stored[MAX_MERGE_ARITY];
   // Padding so ordered_iterator_ptr_arr[-1] is valid
   // Must be immediately before ordered_iterator_ptr_arr
   ordered_iterator *ordered_iterators_pad;
   ordered_iterator *ordered_iterators[MAX_MERGE_ARITY];

   // Stats
   uint64 discarded_deletes;

   // space for merging data together
   merge_accumulator merge_buffer;
} merge_iterator;

// Statically enforce that the padding variables act as index -1 for both arrays
_Static_assert(offsetof(merge_iterator, ordered_iterator_stored_pad)
                  == offsetof(merge_iterator, ordered_iterator_stored[-1]),
               "");
_Static_assert(offsetof(merge_iterator, ordered_iterators_pad)
                  == offsetof(merge_iterator, ordered_iterators[-1]),
               "");

platform_status
merge_iterator_create(platform_heap_id hid,
                      data_config     *cfg,
                      int              num_trees,
                      iterator       **itor_arr,
                      merge_behavior   merge_mode,
                      merge_iterator **out_itor);

platform_status
merge_iterator_destroy(platform_heap_id hid, merge_iterator **merge_itor);

void
merge_iterator_print(merge_iterator *merge_itor);
