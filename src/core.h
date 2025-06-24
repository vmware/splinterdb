// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * core.h --
 *
 *     This file contains the interface for SplinterDB.
 */

#pragma once

#include "splinterdb/data.h"
#include "memtable.h"
#include "log.h"
#include "trunk.h"

/*
 * Upper-bound on most number of branches that we can find our lookup-key in.
 * (Used in the range iterator context.) A convenience limit, used mostly to
 * size statically defined arrays.
 */
#define CORE_RANGE_ITOR_MAX_BRANCHES 256


/*
 *----------------------------------------------------------------------
 * Splinter Configuration structure
 *----------------------------------------------------------------------
 */
typedef struct core_config {
   cache_config *cache_cfg;

   // parameters
   uint64 queue_scale_percent; // Governs when inserters perform bg tasks.  See
                               // task.h

   bool32          use_stats; // stats
   memtable_config mt_cfg;
   btree_config   *btree_cfg;
   data_config    *data_cfg;
   bool32          use_log;
   log_config     *log_cfg;
   trunk_config   *trunk_node_cfg;

   // verbose logging
   bool32               verbose_logging_enabled;
   platform_log_handle *log_handle;
} core_config;

typedef struct core_stats {
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

   uint64 lookups_found;
   uint64 lookups_not_found;
} PLATFORM_CACHELINE_ALIGNED core_stats;

// splinter refers to btrees as branches
typedef struct core_branch {
   uint64 root_addr; // root address of point btree
} core_branch;

typedef struct core_handle core_handle;

typedef struct core_memtable_args {
   core_handle *spl;
   uint64       generation;
} core_memtable_args;

typedef struct core_compacted_memtable {
   core_branch        branch;
   timestamp          wait_start;
   core_memtable_args mt_args;
} core_compacted_memtable;

struct core_handle {
   core_config      cfg;
   platform_heap_id heap_id;

   uint64            super_block_idx;
   allocator_root_id id;

   allocator        *al;
   cache            *cc;
   task_system      *ts;
   log_handle       *log;
   trunk_context     trunk_context;
   memtable_context *mt_ctxt;

   core_stats *stats;

   core_compacted_memtable compacted_memtable[/*cfg.mt_cfg.max_memtables*/];
};

typedef struct core_range_iterator {
   iterator          super;
   core_handle      *spl;
   uint64            num_tuples;
   uint64            num_branches;
   uint64            num_memtable_branches;
   uint64            memtable_start_gen;
   uint64            memtable_end_gen;
   bool32            compacted[CORE_RANGE_ITOR_MAX_BRANCHES];
   merge_iterator   *merge_itor;
   bool32            can_prev;
   bool32            can_next;
   key_buffer        min_key;
   key_buffer        max_key;
   key_buffer        local_min_key;
   key_buffer        local_max_key;
   btree_iterator    btree_itor[CORE_RANGE_ITOR_MAX_BRANCHES];
   trunk_branch_info branch[CORE_RANGE_ITOR_MAX_BRANCHES];

   // used for merge iterator construction
   iterator *itor[CORE_RANGE_ITOR_MAX_BRANCHES];
} core_range_iterator;

/*
 *----------------------------------------------------------------------
 *
 * Splinter API
 *
 *----------------------------------------------------------------------
 */

platform_status
core_insert(core_handle *spl, key tuple_key, message data);

platform_status
core_lookup(core_handle *spl, key target, merge_accumulator *result);

static inline bool32
core_lookup_found(merge_accumulator *result)
{
   return !merge_accumulator_is_null(result);
}

// clang-format off
DEFINE_ASYNC_STATE(core_lookup_async_state, 1,
   param, core_handle *,                  spl,
   param, key,                            target,
   param, merge_accumulator *,            result,
   param, async_callback_fn,              callback,
   param, void *,                         callback_arg,
   local, platform_status,                __async_result,
   local, trunk_ondisk_node_handle,       root_handle,
   local, trunk_merge_lookup_async_state, trunk_node_state)
// clang-format on

async_status
core_lookup_async(core_lookup_async_state *state);

platform_status
core_range_iterator_init(core_handle         *spl,
                         core_range_iterator *range_itor,
                         key                  min_key,
                         key                  max_key,
                         key                  start_key,
                         comparison           start_type,
                         uint64               num_tuples);
void
core_range_iterator_deinit(core_range_iterator *range_itor);

typedef void (*tuple_function)(key tuple_key, message value, void *arg);
platform_status
core_apply_to_range(core_handle   *spl,
                    key            start_key,
                    uint64         num_tuples,
                    tuple_function func,
                    void          *arg);

core_handle *
core_create(core_config      *cfg,
            allocator        *al,
            cache            *cc,
            task_system      *ts,
            allocator_root_id id,
            platform_heap_id  hid);
void
core_destroy(core_handle *spl);
core_handle *
core_mount(core_config      *cfg,
           allocator        *al,
           cache            *cc,
           task_system      *ts,
           allocator_root_id id,
           platform_heap_id  hid);
void
core_unmount(core_handle **spl);

void
core_perform_tasks(core_handle *spl);

void
core_print_insertion_stats(platform_log_handle *log_handle, core_handle *spl);

void
core_print_lookup_stats(platform_log_handle *log_handle, core_handle *spl);

void
core_reset_stats(core_handle *spl);

void
core_print_super_block(platform_log_handle *log_handle, core_handle *spl);

void
core_print_lookup(core_handle         *spl,
                  key                  target,
                  platform_log_handle *log_handle);
void
core_print_extent_counts(platform_log_handle *log_handle, core_handle *spl);

void
core_print_space_use(platform_log_handle *log_handle, core_handle *spl);

static inline uint64
core_max_key_size(core_handle *spl)
{
   return spl->cfg.data_cfg->max_key_size;
}

static inline int
core_key_compare(core_handle *spl, key key1, key key2)
{
   return btree_key_compare(spl->cfg.btree_cfg, key1, key2);
}

static inline void
core_key_to_string(core_handle *spl, key key_to_print, char str[static 128])
{
   btree_key_to_string(spl->cfg.btree_cfg, key_to_print, str);
}

static inline void
core_message_to_string(core_handle *spl, message msg, char str[static 128])
{
   btree_message_to_string(spl->cfg.btree_cfg, msg, str);
}

uint64
trunk_pivot_message_size();

platform_status
core_config_init(core_config         *trunk_cfg,
                 cache_config        *cache_cfg,
                 data_config         *data_cfg,
                 btree_config        *btree_cfg,
                 log_config          *log_cfg,
                 trunk_config        *trunk_node_cfg,
                 uint64               queue_scale_percent,
                 bool32               use_log,
                 bool32               use_stats,
                 bool32               verbose_logging,
                 platform_log_handle *log_handle);
size_t
core_get_scratch_size();
