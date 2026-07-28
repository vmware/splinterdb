// Copyright 2018-2026 VMware, Inc.
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
#include "histogram.h"
#include "superblock.h"

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
// Default range-scan prefetch budget (total extent read-ahead kept in flight),
// ~1 MiB == 8 extents at the default 128 KiB extent size.
#define CORE_DEFAULT_PREFETCH_BUDGET (1024UL * 1024)

typedef struct core_config {
   cache_config *cache_cfg;

   // parameters
   uint64 queue_scale_percent; // Governs when inserters perform bg tasks.  See
                               // task.h

   // Soft byte budget for range-scan extent read-ahead, divided across the
   // branches being merged. Roughly the storage's bandwidth-delay product;
   // raise it for higher-latency devices.
   uint64 prefetch_budget;

   bool32          use_stats; // stats
   memtable_config mt_cfg;
   btree_config   *btree_cfg;
   data_config    *data_cfg;
   bool32          use_log;
   log_config     *log_cfg;
   // Automatic-checkpoint policy: take a checkpoint (rotate the log and advance
   // the durable root) once this many memtable generations have been finalized
   // since the last one.  0 disables automatic checkpoints.
   uint64        checkpoint_generation_interval;
   trunk_config *trunk_node_cfg;

   // verbose logging
   bool32               verbose_logging_enabled;
   platform_log_handle *log_handle;
} core_config;

typedef struct core_stats {
   uint64 insertions;
   uint64 updates;
   uint64 deletions;

   histogram *insert_latency_histo;
   histogram *update_latency_histo;
   histogram *delete_latency_histo;

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

/*
 * Incorporation-driven checkpoint (two-log protocol) state machine.
 *
 *   IDLE          no checkpoint in progress.
 *   PENDING       the next live log is pre-created; the next memtable rotation
 *                 will swap it in under the insert lock.
 *   SEALING       the rotation swapped the new live log in; the old log still
 *                 needs sealing (which will be performed just after the
 *                 rotation critical section).
 *   INCORPORATING the old log is sealed; waiting for its generations to
 *                 be incorporated into the trunk root.
 *   COMPLETING    the completion publish (advance root, clear sealed slot) is
 *                 in flight.
 *
 * The only transition that touches the shared spl->log pointer (PENDING ->
 * SEALING) runs inside the memtable rotation critical section, where the insert
 * lock is held exclusively; every log writer holds that lock shared across its
 * log_write, so no writer can be mid-write to, or newly enter, the old log once
 * it is swapped out.  All other fields are guarded by checkpoint_state_lock,
 * which is only ever held for brief, I/O-free updates.
 */
typedef enum core_checkpoint_phase {
   CORE_CHECKPOINT_IDLE = 0,
   CORE_CHECKPOINT_PENDING,
   CORE_CHECKPOINT_SEALING,
   CORE_CHECKPOINT_INCORPORATING,
   CORE_CHECKPOINT_COMPLETING,
} core_checkpoint_phase;

typedef struct core_checkpoint_state {
   core_checkpoint_phase phase;
   log_handle           *pending_log;    // next live log, pre-created (PENDING)
   log_handle           *log_to_seal;    // old live log awaiting seal (SEALING)
   log_head              sealed_head;    // identity of the sealed log (reclaim)
   log_head              live_head;      // identity of the new live log
   uint64                cut_generation; // complete once retired >= this
} core_checkpoint_state;

typedef struct core_memtable_args {
   core_handle *spl;
   uint64       generation;
   task         tsk;
} core_memtable_args;

typedef struct core_compacted_memtable {
   core_branch        branch;
   timestamp          wait_start;
   core_memtable_args mt_args;
} core_compacted_memtable;

struct core_handle {
   core_config      cfg;
   platform_heap_id heap_id;

   allocator_root_id id;

   allocator       *al;
   cache           *cc;
   task_system     *ts;
   log_handle      *log;
   trunk_context    trunk_context;
   memtable_context mt_ctxt;

   /*
    * Durable instance metadata.  core owns the in-memory superblock context
    * (allocated at mkfs/mount, torn down at unmount/destroy): it borrows the
    * geometry, reads its tree record, and publishes root advances plus
    * allocation-state transitions.  For now the instance holds a single tree;
    * when multi-tree support lands this ownership hoists to an instance level
    * that per-tree cores borrow.
    */
   superblock_context superblock;

   /* Serializes snapshot cuts and superblock publication. */
   platform_mutex checkpoint_lock;
   bool32         checkpoint_lock_initialized;

   /*
    * Incorporation-driven checkpoint state.  checkpoint_state_lock guards the
    * fields of `checkpoint` and last_checkpoint_generation; it is only ever
    * held for brief, I/O-free updates (never across a barrier), so taking it
    * inside the memtable rotation critical section cannot stall inserts on I/O.
    */
   platform_mutex        checkpoint_state_lock;
   bool32                checkpoint_state_lock_initialized;
   core_checkpoint_state checkpoint;
   uint64                last_checkpoint_generation;

   core_stats *stats;

   core_compacted_memtable compacted_memtable[MAX_MEMTABLES];
};

typedef struct core_range_iterator {
   iterator          super;
   core_handle      *spl;
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
   comparison        min_key_comparison;
   comparison        max_key_comparison;
   comparison        local_min_key_comparison;
   comparison        local_max_key_comparison;
   bool32            local_max_key_truncated;
   btree_iterator    btree_itor[CORE_RANGE_ITOR_MAX_BRANCHES];
   bool32            btree_itor_initialized[CORE_RANGE_ITOR_MAX_BRANCHES];
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
core_insert(core_handle   *spl,
            key            tuple_key,
            message        data,
            lookup_result *old_result);

platform_status
core_optimize(core_handle                    *spl,
              key                             minkey,
              key                             maxkey,
              bool32                          full_leaf_compactions,
              struct splinterdb_notification *notification);

platform_status
core_lookup(core_handle *spl, key target, lookup_result *result);

static inline bool32
core_lookup_found(merge_accumulator *result)
{
   return !merge_accumulator_is_null(result);
}

// clang-format off
DEFINE_ASYNC_STATE(core_lookup_async_state, 1,
   param, core_handle *,                  spl,
   param, key,                            target,
   param, lookup_result *,                result,
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
                         comparison           min_key_comparison,
                         key                  min_key,
                         comparison           max_key_comparison,
                         key                  max_key,
                         comparison           start_key_comparison,
                         key                  start_key);
void
core_range_iterator_deinit(core_range_iterator *range_itor);

typedef void (*tuple_function)(key tuple_key, message value, void *arg);
platform_status
core_apply_to_range(core_handle   *spl,
                    key            start_key,
                    uint64         num_tuples,
                    tuple_function func,
                    void          *arg);

/* Format the disk and mount the database */
platform_status
core_mkfs(core_handle      *spl,
          core_config      *cfg,
          allocator        *al,
          cache            *cc,
          io_handle        *io,
          task_system      *ts,
          allocator_root_id id,
          platform_heap_id  hid);

platform_status
core_mount(core_handle      *spl,
           core_config      *cfg,
           allocator        *al,
           cache            *cc,
           io_handle        *io,
           task_system      *ts,
           allocator_root_id id,
           platform_heap_id  hid);

/*
 * Take a checkpoint: advance the durable root and rotate the log (two-log
 * protocol).  Must be called at a quiescent point (no concurrent inserts).
 */
platform_status
core_checkpoint(core_handle *spl);

platform_status
core_unmount(core_handle *spl);

/* Unmount the database and erase it from the disk */
void
core_destroy(core_handle *spl);

void
core_perform_tasks(core_handle *spl);

void
core_print_insertion_stats(platform_log_handle *log_handle,
                           const core_handle   *spl);

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
                 uint64               prefetch_budget,
                 bool32               use_log,
                 bool32               use_stats,
                 bool32               verbose_logging,
                 platform_log_handle *log_handle);
