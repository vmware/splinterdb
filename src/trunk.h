// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * trunk.h --
 *
 *     This file contains the interface for SplinterDB.
 */

#pragma once

#include "splinterdb/data.h"
#include "btree.h"
#include "memtable.h"
#include "routing_filter.h"
#include "cache.h"
#include "iterator.h"
#include "merge.h"
#include "allocator.h"
#include "log.h"
#include "srq.h"
#include "shmem.h"

/*
 * Max height of the Trunk Tree; Limited for convenience to allow for static
 * allocation of various nested arrays. (Should be possible to increase this, if
 * ever needed, in future w/o perf impacts.) This limit is quite large enough
 * for most expected installations.
 */
#define TRUNK_MAX_HEIGHT 8

/*
 * Mini-allocator uses separate batches for each height of the Trunk tree.
 * Therefore, the max # of mini-batches that the mini-allocator can track
 * is limited by the max height of the SplinterDB trunk.
 */
_Static_assert(TRUNK_MAX_HEIGHT == MINI_MAX_BATCHES,
               "TRUNK_MAX_HEIGHT should be == MINI_MAX_BATCHES");

/*
 * Upper-bound on most number of branches that we can find our lookup-key in.
 * (Used in the range iterator context.) A convenience limit, used mostly to
 * size statically defined arrays.
 */
#define TRUNK_RANGE_ITOR_MAX_BRANCHES 256


/*
 *----------------------------------------------------------------------
 * Splinter Configuration structure
 *----------------------------------------------------------------------
 */
typedef struct trunk_config {
   cache_config *cache_cfg;

   // parameters
   uint64 fanout;              // children to trigger split
   uint64 max_pivot_keys;      // hard limit on number of pivot keys
   uint64 max_tuples_per_node; // deprecated
   uint64 max_kv_bytes_per_node;
   uint64 max_branches_per_node;
   uint64 hard_max_branches_per_node;
   uint64 target_leaf_kv_bytes; // make leaves this big when splitting
   uint64 reclaim_threshold;    // start reclaming space when
                                // free space < threshold
   uint64 queue_scale_percent;  // Governs when inserters perform bg tasks.  See
                                // task.h
   bool            use_stats;   // stats
   memtable_config mt_cfg;
   btree_config    btree_cfg;
   routing_config  filter_cfg;
   data_config    *data_cfg;
   bool            use_log;
   log_config     *log_cfg;

   // verbose logging
   bool                 verbose_logging_enabled;
   platform_log_handle *log_handle;
} trunk_config;

typedef struct trunk_stats {
   uint64 insertions;
   uint64 updates;
   uint64 deletions;

   platform_histo_handle insert_latency_histo;
   platform_histo_handle update_latency_histo;
   platform_histo_handle delete_latency_histo;

   uint64 flush_wait_time_ns[TRUNK_MAX_HEIGHT];
   uint64 flush_time_ns[TRUNK_MAX_HEIGHT];
   uint64 flush_time_max_ns[TRUNK_MAX_HEIGHT];
   uint64 full_flushes[TRUNK_MAX_HEIGHT];
   uint64 count_flushes[TRUNK_MAX_HEIGHT];
   uint64 memtable_flushes;
   uint64 memtable_flush_time_ns;
   uint64 memtable_flush_time_max_ns;
   uint64 memtable_flush_wait_time_ns;
   uint64 memtable_flush_root_full;
   uint64 root_full_flushes;
   uint64 root_count_flushes;
   uint64 root_flush_time_ns;
   uint64 root_flush_time_max_ns;
   uint64 root_flush_wait_time_ns;
   uint64 failed_flushes[TRUNK_MAX_HEIGHT];
   uint64 root_failed_flushes;
   uint64 memtable_failed_flushes;

   uint64 compactions[TRUNK_MAX_HEIGHT];
   uint64 compactions_aborted_flushed[TRUNK_MAX_HEIGHT];
   uint64 compactions_aborted_leaf_split[TRUNK_MAX_HEIGHT];
   uint64 compactions_discarded_flushed[TRUNK_MAX_HEIGHT];
   uint64 compactions_discarded_leaf_split[TRUNK_MAX_HEIGHT];
   uint64 compactions_empty[TRUNK_MAX_HEIGHT];
   uint64 compaction_tuples[TRUNK_MAX_HEIGHT];
   uint64 compaction_max_tuples[TRUNK_MAX_HEIGHT];
   uint64 compaction_time_ns[TRUNK_MAX_HEIGHT];
   uint64 compaction_time_max_ns[TRUNK_MAX_HEIGHT];
   uint64 compaction_time_wasted_ns[TRUNK_MAX_HEIGHT];
   uint64 compaction_pack_time_ns[TRUNK_MAX_HEIGHT];

   uint64 root_compactions;
   uint64 root_compaction_pack_time_ns;
   uint64 root_compaction_tuples;
   uint64 root_compaction_max_tuples;
   uint64 root_compaction_time_ns;
   uint64 root_compaction_time_max_ns;

   uint64 discarded_deletes;
   uint64 index_splits;
   uint64 leaf_splits;
   uint64 leaf_splits_leaves_created;
   uint64 leaf_split_time_ns;
   uint64 leaf_split_max_time_ns;

   uint64 single_leaf_splits;
   uint64 single_leaf_tuples;
   uint64 single_leaf_max_tuples;

   uint64 root_filters_built;
   uint64 root_filter_tuples;
   uint64 root_filter_time_ns;
   uint64 filters_built[TRUNK_MAX_HEIGHT];
   uint64 filter_tuples[TRUNK_MAX_HEIGHT];
   uint64 filter_time_ns[TRUNK_MAX_HEIGHT];

   uint64 lookups_found;
   uint64 lookups_not_found;
   uint64 filter_lookups[TRUNK_MAX_HEIGHT];
   uint64 branch_lookups[TRUNK_MAX_HEIGHT];
   uint64 filter_false_positives[TRUNK_MAX_HEIGHT];
   uint64 filter_negatives[TRUNK_MAX_HEIGHT];

   uint64 space_recs[TRUNK_MAX_HEIGHT];
   uint64 space_rec_time_ns[TRUNK_MAX_HEIGHT];
   uint64 space_rec_tuples_reclaimed[TRUNK_MAX_HEIGHT];
   uint64 tuples_reclaimed[TRUNK_MAX_HEIGHT];
} PLATFORM_CACHELINE_ALIGNED trunk_stats;

// splinter refers to btrees as branches
typedef struct trunk_branch {
   uint64 root_addr; // root address of point btree
} trunk_branch;

typedef struct trunk_handle             trunk_handle;
typedef struct trunk_compact_bundle_req trunk_compact_bundle_req;

typedef struct trunk_memtable_args {
   trunk_handle *spl;
   uint64        generation;
} trunk_memtable_args;

typedef struct trunk_compacted_memtable {
   trunk_branch              branch;
   routing_filter            filter;
   timestamp                 wait_start;
   trunk_memtable_args       mt_args;
   trunk_compact_bundle_req *req;
} trunk_compacted_memtable;

struct trunk_handle {
   uint64           root_addr;
   uint64           super_block_idx;
   trunk_config     cfg;
   platform_heap_id heap_id;

   // space reclamation
   uint64 est_tuples_in_compaction;

   // allocator/cache/log
   allocator     *al;
   cache         *cc;
   log_handle    *log;
   mini_allocator mini;

   // memtables
   allocator_root_id id;
   memtable_context *mt_ctxt;

   // task system
   task_system *ts; // ALEX: currently not durable

   // stats
   trunk_stats *stats;
   size_t       stats_size; // Of allocated memory fragment.

   // Link inside the splinter list
   List_Links links;

   /*
    * Per thread task and per splinter table task counter. Used to decide when
    * to run tasks.
    */

   struct {
      uint64 counter;
   } PLATFORM_CACHELINE_ALIGNED task_countup[MAX_THREADS];

   // space rec queue
   srq srq;

   size_t size; // of memory fragment allocated to init trunk_handle{}
   trunk_compacted_memtable compacted_memtable[/*cfg.mt_cfg.max_memtables*/];
};

typedef struct trunk_range_iterator {
   iterator        super;
   trunk_handle   *spl;
   uint64          num_tuples;
   uint64          num_branches;
   uint64          num_memtable_branches;
   uint64          memtable_start_gen;
   uint64          memtable_end_gen;
   bool            compacted[TRUNK_RANGE_ITOR_MAX_BRANCHES];
   merge_iterator *merge_itor;
   bool            at_end;
   key_buffer      min_key;
   key_buffer      max_key;
   key_buffer      local_max_key;
   key_buffer      rebuild_key;
   btree_iterator  btree_itor[TRUNK_RANGE_ITOR_MAX_BRANCHES];
   trunk_branch    branch[TRUNK_RANGE_ITOR_MAX_BRANCHES];

   // used for merge iterator construction
   iterator *itor[TRUNK_RANGE_ITOR_MAX_BRANCHES];
} trunk_range_iterator;


typedef enum {
   async_state_invalid = 0,
   async_state_start,
   async_state_lookup_memtable,
   async_state_get_root_reentrant,
   async_state_trunk_node_lookup,
   async_state_subbundle_lookup,
   async_state_pivot_lookup,
   async_state_filter_lookup_start,
   async_state_filter_lookup_reentrant,
   async_state_btree_lookup_start,
   async_state_btree_lookup_reentrant,
   async_state_next_in_node,
   async_state_trunk_node_done,
   async_state_get_child_trunk_node_reentrant,
   async_state_unget_parent_trunk_node,
   async_state_found_final_answer_early,
   async_state_end
} trunk_async_state;

typedef enum {
   async_lookup_state_invalid = 0,
   async_lookup_state_pivot,
   async_lookup_state_subbundle,
   async_lookup_state_compacted_subbundle
} trunk_async_lookup_state;

struct trunk_async_ctxt;
struct trunk_pivot_data;
struct trunk_subbundle;

typedef void (*trunk_async_cb)(struct trunk_async_ctxt *ctxt);

struct trunk_hdr;
typedef struct trunk_hdr trunk_hdr;

typedef struct trunk_node {
   uint64       addr;
   page_handle *page;
   trunk_hdr   *hdr;
} trunk_node;

typedef struct trunk_async_ctxt {
   trunk_async_cb cb; // IN: callback (requeues ctxt
                      // for dispatch)
   // These fields are internal
   trunk_async_state prev_state;   // state machine's previous state
   trunk_async_state state;        // state machine's current state
   page_handle      *mt_lock_page; // Memtable lock page
   trunk_node        trunk_node;   // Current trunk node
   uint16            height;       // height of trunk_node

   uint16 sb_no;     // subbundle number (newest)
   uint16 end_sb_no; // subbundle number (oldest,
                     // exclusive
   uint16 filter_no; // sb filter no

   trunk_async_lookup_state lookup_state; // Can be pivot or
                                          // [compacted] subbundle
   struct trunk_subbundle  *sb;           // Subbundle
   struct trunk_pivot_data *pdata;        // Pivot data for next trunk node
   routing_filter          *filter;       // Filter for subbundle or pivot
   uint64                   found_values; // values found in filter
   uint16                   value;        // Current value found in filter

   uint16 branch_no;        // branch number (newest)
   uint16 branch_no_end;    // branch number end (oldest,
                            // exclusive)
   bool          was_async; // Did an async IO for trunk ?
   trunk_branch *branch;    // Current branch
   union {
      routing_async_ctxt filter_ctxt; // Filter async context
      btree_async_ctxt   btree_ctxt;  // Btree async context
   };
   cache_async_ctxt cache_ctxt; // Async cache context
} trunk_async_ctxt;


/*
 *----------------------------------------------------------------------
 *
 * Splinter API
 *
 *----------------------------------------------------------------------
 */

platform_status
trunk_insert(trunk_handle *spl, key tuple_key, message data);

platform_status
trunk_lookup(trunk_handle *spl, key target, merge_accumulator *result);

static inline bool
trunk_lookup_found(merge_accumulator *result)
{
   return !merge_accumulator_is_null(result);
}

cache_async_result
trunk_lookup_async(trunk_handle      *spl,
                   key                target,
                   merge_accumulator *data,
                   trunk_async_ctxt  *ctxt);
platform_status
trunk_range_iterator_init(trunk_handle         *spl,
                          trunk_range_iterator *range_itor,
                          key                   min_key,
                          key                   max_key,
                          uint64                num_tuples);
void
trunk_range_iterator_deinit(trunk_range_iterator *range_itor);

typedef void (*tuple_function)(key tuple_key, message value, void *arg);
platform_status
trunk_range(trunk_handle  *spl,
            key            start_key,
            uint64         num_tuples,
            tuple_function func,
            void          *arg);

trunk_handle *
trunk_create(trunk_config     *cfg,
             allocator        *al,
             cache            *cc,
             task_system      *ts,
             allocator_root_id id,
             platform_heap_id  hid);
void
trunk_destroy(trunk_handle *spl);
trunk_handle *
trunk_mount(trunk_config     *cfg,
            allocator        *al,
            cache            *cc,
            task_system      *ts,
            allocator_root_id id,
            platform_heap_id  hid);
void
trunk_unmount(trunk_handle **spl);

void
trunk_perform_tasks(trunk_handle *spl);

void
trunk_force_flush(trunk_handle *spl);
void
trunk_print_insertion_stats(platform_log_handle *log_handle, trunk_handle *spl);
void
trunk_print_lookup_stats(platform_log_handle *log_handle, trunk_handle *spl);
void
trunk_reset_stats(trunk_handle *spl);

void
trunk_print(platform_log_handle *log_handle, trunk_handle *spl);

void
trunk_print_super_block(platform_log_handle *log_handle, trunk_handle *spl);

void
trunk_print_lookup(trunk_handle        *spl,
                   key                  target,
                   platform_log_handle *log_handle);
void
trunk_print_branches(platform_log_handle *log_handle, trunk_handle *spl);
void
trunk_print_extent_counts(platform_log_handle *log_handle, trunk_handle *spl);
void
trunk_print_space_use(platform_log_handle *log_handle, trunk_handle *spl);
bool
trunk_verify_tree(trunk_handle *spl);

static inline uint64
trunk_max_key_size(trunk_handle *spl)
{
   return spl->cfg.data_cfg->max_key_size;
}

static inline int
trunk_key_compare(trunk_handle *spl, key key1, key key2)
{
   return btree_key_compare(&spl->cfg.btree_cfg, key1, key2);
}

static inline void
trunk_key_to_string(trunk_handle *spl, key key_to_print, char str[static 128])
{
   btree_key_to_string(&spl->cfg.btree_cfg, key_to_print, str);
}

static inline void
trunk_message_to_string(trunk_handle *spl, message msg, char str[static 128])
{
   btree_message_to_string(&spl->cfg.btree_cfg, msg, str);
}

static inline void
trunk_async_ctxt_init(trunk_async_ctxt *ctxt, trunk_async_cb cb)
{
   ZERO_CONTENTS(ctxt);
   ctxt->state = async_state_start;
   ctxt->cb    = cb;
}

uint64
trunk_pivot_message_size();

uint64
trunk_hdr_size();

platform_status
trunk_config_init(trunk_config        *trunk_cfg,
                  cache_config        *cache_cfg,
                  data_config         *data_cfg,
                  log_config          *log_cfg,
                  uint64               memtable_capacity,
                  uint64               fanout,
                  uint64               max_branches_per_node,
                  uint64               btree_rough_count_height,
                  uint64               filter_remainder_size,
                  uint64               filter_index_size,
                  uint64               reclaim_threshold,
                  uint64               queue_scale_percent,
                  bool                 use_log,
                  bool                 use_stats,
                  bool                 verbose_logging,
                  platform_log_handle *log_handle);
size_t
trunk_get_scratch_size();
