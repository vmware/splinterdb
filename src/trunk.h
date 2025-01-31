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
#include "trunk_node.h"

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
   uint64 queue_scale_percent; // Governs when inserters perform bg tasks.  See
                               // task.h

   bool32             use_stats; // stats
   memtable_config    mt_cfg;
   btree_config      *btree_cfg;
   data_config       *data_cfg;
   bool32             use_log;
   log_config        *log_cfg;
   trunk_node_config *trunk_node_cfg;

   // verbose logging
   bool32               verbose_logging_enabled;
   platform_log_handle *log_handle;
} trunk_config;

typedef struct trunk_stats {
   uint64 insertions;
   uint64 updates;
   uint64 deletions;

   platform_histo_handle insert_latency_histo;
   platform_histo_handle update_latency_histo;
   platform_histo_handle delete_latency_histo;

   uint64 memtable_flushes;
   uint64 memtable_flush_time_ns;
   uint64 memtable_flush_time_max_ns;
   uint64 memtable_flush_wait_time_ns;
   uint64 memtable_flush_root_full;
   uint64 memtable_failed_flushes;

   uint64 root_compactions;
   uint64 root_compaction_pack_time_ns;
   uint64 root_compaction_tuples;
   uint64 root_compaction_max_tuples;
   uint64 root_compaction_time_ns;
   uint64 root_compaction_time_max_ns;

   uint64 discarded_deletes;

   uint64 root_filters_built;
   uint64 root_filter_tuples;
   uint64 root_filter_time_ns;

   uint64 lookups_found;
   uint64 lookups_not_found;
   uint64 filter_lookups[TRUNK_MAX_HEIGHT];
   uint64 branch_lookups[TRUNK_MAX_HEIGHT];
   uint64 filter_false_positives[TRUNK_MAX_HEIGHT];
   uint64 filter_negatives[TRUNK_MAX_HEIGHT];
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
   trunk_branch        branch;
   timestamp           wait_start;
   trunk_memtable_args mt_args;
} trunk_compacted_memtable;

struct trunk_handle {
   trunk_config     cfg;
   platform_heap_id heap_id;

   uint64            super_block_idx;
   allocator_root_id id;

   platform_batch_rwlock trunk_root_lock;

   allocator         *al;
   cache             *cc;
   task_system       *ts;
   log_handle        *log;
   trunk_node_context trunk_context;
   memtable_context  *mt_ctxt;

   trunk_stats *stats;

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
   bool32          compacted[TRUNK_RANGE_ITOR_MAX_BRANCHES];
   merge_iterator *merge_itor;
   bool32          can_prev;
   bool32          can_next;
   key_buffer      min_key;
   key_buffer      max_key;
   key_buffer      local_min_key;
   key_buffer      local_max_key;
   btree_iterator  btree_itor[TRUNK_RANGE_ITOR_MAX_BRANCHES];
   branch_info     branch[TRUNK_RANGE_ITOR_MAX_BRANCHES];

   // used for merge iterator construction
   iterator *itor[TRUNK_RANGE_ITOR_MAX_BRANCHES];
} trunk_range_iterator;


struct trunk_pivot_data;
struct trunk_subbundle;

struct trunk_hdr;
typedef struct trunk_hdr trunk_hdr;

typedef struct trunk_node {
   uint64       addr;
   page_handle *page;
   trunk_hdr   *hdr;
} trunk_node;

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

static inline bool32
trunk_lookup_found(merge_accumulator *result)
{
   return !merge_accumulator_is_null(result);
}

// clang-format off
DEFINE_ASYNC_STATE(trunk_lookup_async_state, 1,
   param, trunk_handle *,        spl,
   param, key,                   target,
   param, merge_accumulator *,   result,
   param, async_callback_fn,     callback,
   param, void *,                callback_arg,
   local, platform_status,       __async_result,
   local, ondisk_node_handle,    root_handle,
   local, trunk_merge_lookup_async_state, trunk_node_state)
// clang-format on

async_status
trunk_lookup_async(trunk_lookup_async_state *state);

platform_status
trunk_range_iterator_init(trunk_handle         *spl,
                          trunk_range_iterator *range_itor,
                          key                   min_key,
                          key                   max_key,
                          key                   start_key,
                          comparison            start_type,
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
bool32
trunk_verify_tree(trunk_handle *spl);

static inline uint64
trunk_max_key_size(trunk_handle *spl)
{
   return spl->cfg.data_cfg->max_key_size;
}

static inline int
trunk_key_compare(trunk_handle *spl, key key1, key key2)
{
   return btree_key_compare(spl->cfg.btree_cfg, key1, key2);
}

static inline void
trunk_key_to_string(trunk_handle *spl, key key_to_print, char str[static 128])
{
   btree_key_to_string(spl->cfg.btree_cfg, key_to_print, str);
}

static inline void
trunk_message_to_string(trunk_handle *spl, message msg, char str[static 128])
{
   btree_message_to_string(spl->cfg.btree_cfg, msg, str);
}

uint64
trunk_pivot_message_size();

platform_status
trunk_config_init(trunk_config        *trunk_cfg,
                  cache_config        *cache_cfg,
                  data_config         *data_cfg,
                  btree_config        *btree_cfg,
                  log_config          *log_cfg,
                  trunk_node_config   *trunk_node_cfg,
                  uint64               queue_scale_percent,
                  bool32               use_log,
                  bool32               use_stats,
                  bool32               verbose_logging,
                  platform_log_handle *log_handle);
size_t
trunk_get_scratch_size();
