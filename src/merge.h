// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * merge.h --
 *
 *    Merging functionality, notably merge iterators.
 */

#pragma once

#include "data_internal.h"
#include "iterator.h"

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


typedef bool32 (*merge_key_filter)(void *context, key tuple_key);

typedef struct merge_iterator {
   iterator           super;     // handle for iterator.h API
   int                num_trees; // number of trees in the forest
   bool32             merge_messages;
   bool32             finalize_updates;
   bool32             emit_deletes;
   bool32             can_prev;
   bool32             can_next;
   int                num_remaining; // number of ritors not at end
   const data_config *cfg;           // point message tree data config
   key                curr_key;      // current key
   message            curr_data;     // current data
   bool32             forwards;

   // Sentinel used as the predecessor of ordered_iterators[0].
   ordered_iterator  ordered_iterator_stored_pad;
   ordered_iterator  ordered_iterator_stored[MAX_MERGE_ARITY];
   ordered_iterator *ordered_iterators_pad;
   ordered_iterator *ordered_iterators[MAX_MERGE_ARITY];

   // Stats
   uint64 discarded_deletes;

   // space for merging data together
   merge_accumulator merge_buffer;
} merge_iterator;

typedef struct merge_iterator_options {
   /* Reject keys before any message merge/finalization. The predicate and its
    * context must remain valid until destruction and be stable for both scan
    * directions. NULL (the default) retains the ordinary iterator path. */
   merge_key_filter keep_key;
   void            *filter_context;
} merge_iterator_options;

/* Usage: merge_iterator_create(hid, cfg, n, itors, MERGE_FULL, TRUE, &out);
 *        merge_iterator_create(hid, cfg, n, itors, MERGE_FULL, TRUE, &out,
 *                              .keep_key = pred, .filter_context = ctx); */
#define merge_iterator_create(                                                 \
   hid, cfg, num_trees, itor_arr, merge_mode, forwards, out_itor, ...)         \
   merge_iterator_create_full((hid),                                           \
                              (cfg),                                           \
                              (num_trees),                                     \
                              (itor_arr),                                      \
                              (merge_mode),                                    \
                              (forwards),                                      \
                              (out_itor),                                      \
                              &(const merge_iterator_options){__VA_ARGS__})

platform_status
merge_iterator_create_full(platform_heap_id              hid,
                           const data_config            *cfg,
                           int                           num_trees,
                           iterator                    **itor_arr,
                           merge_behavior                merge_mode,
                           bool32                        forwards,
                           merge_iterator              **out_itor,
                           const merge_iterator_options *options);

platform_status
merge_iterator_destroy(platform_heap_id hid, merge_iterator **merge_itor);

void
merge_iterator_print(merge_iterator *merge_itor);
