// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * trunk.h --
 *
 *     This file contains the interface of the SplinterDB trunk.
 */

#include "platform.h"
#include "vector.h"
#include "cache.h"
#include "allocator.h"
#include "task.h"
#include "btree.h"
#include "routing_filter.h"
#include "iterator.h"
#include "merge.h"
#include "data_internal.h"
#include "batch_rwlock.h"

typedef struct trunk_config {
   const data_config    *data_cfg;
   const btree_config   *btree_cfg;
   const routing_config *filter_cfg;
   uint64                incorporation_size_kv_bytes;
   uint64                target_fanout;
   uint64                branch_rough_count_height;
   bool32                use_stats;
} trunk_config;

#define TRUNK_MAX_HEIGHT             16
#define TRUNK_MAX_DISTRIBUTION_VALUE 16

typedef struct trunk_stats {
   uint64 fanout_distribution[TRUNK_MAX_DISTRIBUTION_VALUE][TRUNK_MAX_HEIGHT];
   uint64 num_inflight_bundles_distribution[TRUNK_MAX_DISTRIBUTION_VALUE]
                                           [TRUNK_MAX_HEIGHT];
   uint64 bundle_num_branches_distribution[TRUNK_MAX_DISTRIBUTION_VALUE]
                                          [TRUNK_MAX_HEIGHT];

   uint64 node_size_pages_distribution[TRUNK_MAX_DISTRIBUTION_VALUE]
                                      [TRUNK_MAX_HEIGHT];

   uint64 incorporation_footprint_distribution[TRUNK_MAX_DISTRIBUTION_VALUE];

   uint64 count_flushes[TRUNK_MAX_HEIGHT];
   uint64 flush_time_ns[TRUNK_MAX_HEIGHT];
   uint64 flush_time_max_ns[TRUNK_MAX_HEIGHT];
   uint64 full_flushes[TRUNK_MAX_HEIGHT];

   // We don't know whether a node is the root. So we can't track these stats
   // carrying around some extra information that would be useful only for
   // collecting these stats.
   // uint64 root_full_flushes;
   // uint64 root_count_flushes;
   // uint64 root_flush_time_ns;
   // uint64 root_flush_time_max_ns;
   // uint64 root_flush_wait_time_ns;

   uint64 compactions[TRUNK_MAX_HEIGHT];
   uint64 compactions_aborted[TRUNK_MAX_HEIGHT];
   uint64 compactions_discarded[TRUNK_MAX_HEIGHT];
   uint64 compactions_empty[TRUNK_MAX_HEIGHT];
   uint64 compaction_tuples[TRUNK_MAX_HEIGHT];
   uint64 compaction_max_tuples[TRUNK_MAX_HEIGHT];
   uint64 compaction_time_ns[TRUNK_MAX_HEIGHT];
   uint64 compaction_time_max_ns[TRUNK_MAX_HEIGHT];
   uint64 compaction_time_wasted_ns[TRUNK_MAX_HEIGHT];
   uint64 compaction_pack_time_ns[TRUNK_MAX_HEIGHT];

   uint64 maplet_builds[TRUNK_MAX_HEIGHT];
   uint64 maplet_builds_aborted[TRUNK_MAX_HEIGHT];
   uint64 maplet_builds_discarded[TRUNK_MAX_HEIGHT];
   uint64 maplet_build_time_ns[TRUNK_MAX_HEIGHT];
   uint64 maplet_tuples[TRUNK_MAX_HEIGHT];
   uint64 maplet_build_time_max_ns[TRUNK_MAX_HEIGHT];
   uint64 maplet_build_time_wasted_ns[TRUNK_MAX_HEIGHT];

   uint64 node_splits[TRUNK_MAX_HEIGHT];
   uint64 node_splits_nodes_created[TRUNK_MAX_HEIGHT];
   uint64 leaf_split_time_ns;
   uint64 leaf_split_time_max_ns;
   uint64 single_leaf_splits;

   // The compaction that computes these stats is donez long after the decision
   // to do a single-leaf split was made, so we can't track these stats.
   //  uint64 single_leaf_tuples;
   //  uint64 single_leaf_max_tuples;

   // These are better tracked at the level that manages the memtable/trunk
   // interaction.
   // uint64 lookups_found;
   // uint64 lookups_not_found;

   uint64 maplet_lookups[TRUNK_MAX_HEIGHT];
   uint64 maplet_false_positives[TRUNK_MAX_HEIGHT];
   uint64 branch_lookups[TRUNK_MAX_HEIGHT];

   // Not yet implemented
   // uint64 space_recs[TRUNK_MAX_HEIGHT];
   // uint64 space_rec_time_ns[TRUNK_MAX_HEIGHT];
   // uint64 space_rec_tuples_reclaimed[TRUNK_MAX_HEIGHT];
   // uint64 tuples_reclaimed[TRUNK_MAX_HEIGHT];
} PLATFORM_CACHELINE_ALIGNED trunk_stats;

#define TRUNK_PIVOT_STATE_MAP_BUCKETS 1024

typedef struct trunk_pivot_state trunk_pivot_state;

typedef struct trunk_pivot_state_map {
   uint64             num_states;
   uint64             locks[TRUNK_PIVOT_STATE_MAP_BUCKETS];
   trunk_pivot_state *buckets[TRUNK_PIVOT_STATE_MAP_BUCKETS];
} trunk_pivot_state_map;

/* An ondisk_node_ref is a pivot that has an associated bump in the refcount of
 * the child, so destroying an ondisk_node_ref will perform an
 * ondisk_node_dec_ref. */
typedef struct trunk_ondisk_node_ref {
   uint64     addr;
   ondisk_key key;
} trunk_ondisk_node_ref;

typedef struct ONDISK branch_ref {
   uint64 addr;
} branch_ref;

typedef VECTOR(branch_ref) branch_ref_vector;

typedef struct bundle {
   routing_filter maplet;
   // branches[0] is the oldest branch
   branch_ref_vector branches;
} bundle;

typedef VECTOR(bundle) bundle_vector;

typedef struct trunk_pivot trunk_pivot;
typedef VECTOR(trunk_pivot *) trunk_pivot_vector;

typedef struct trunk_node {
   uint16             height;
   trunk_pivot_vector pivots;
   bundle_vector      pivot_bundles; // indexed by child
   uint64             num_old_bundles;
   // inflight_bundles[0] is the oldest bundle
   bundle_vector inflight_bundles;
} trunk_node;

typedef VECTOR(trunk_node) trunk_node_vector;

typedef struct incorporation_tasks {
   trunk_node_vector node_compactions;
} incorporation_tasks;

typedef struct pending_gc pending_gc;

typedef struct trunk_context {
   const trunk_config    *cfg;
   platform_heap_id       hid;
   cache                 *cc;
   allocator             *al;
   task_system           *ts;
   trunk_stats           *stats;
   trunk_pivot_state_map  pivot_states;
   batch_rwlock           root_lock;
   trunk_ondisk_node_ref *root;
   trunk_ondisk_node_ref *post_incorporation_root;
   trunk_ondisk_node_ref *pre_incorporation_root;
   uint64                 pending_gcs_lock;
   pending_gc            *pending_gcs;
   pending_gc            *pending_gcs_tail;
   incorporation_tasks    tasks;
} trunk_context;

typedef struct trunk_ondisk_node_handle {
   cache       *cc;
   page_handle *header_page;
   page_handle *pivot_page;
   page_handle *inflight_bundle_page;
} trunk_ondisk_node_handle;

typedef struct trunk_branch_merger {
   platform_heap_id   hid;
   const data_config *data_cfg;
   key                min_key;
   key                max_key;
   uint64             height;
   merge_iterator    *merge_itor;
   iterator_vector    itors;
} trunk_branch_merger;

/********************************
 * Lifecycle
 ********************************/

void
trunk_config_init(trunk_config         *config,
                  const data_config    *data_cfg,
                  const btree_config   *btree_cfg,
                  const routing_config *filter_cfg,
                  uint64                incorporation_size_kv_bytes,
                  uint64                target_fanout,
                  uint64                branch_rough_count_height,
                  bool32                use_stats);

platform_status
trunk_context_init(trunk_context      *context,
                   const trunk_config *cfg,
                   platform_heap_id    hid,
                   cache              *cc,
                   allocator          *al,
                   task_system        *ts,
                   uint64              root_addr);

platform_status
trunk_inc_ref(const trunk_config *cfg,
              platform_heap_id    hid,
              cache              *cc,
              allocator          *al,
              task_system        *ts,
              uint64              root_addr);

platform_status
trunk_dec_ref(const trunk_config *cfg,
              platform_heap_id    hid,
              cache              *cc,
              allocator          *al,
              task_system        *ts,
              uint64              root_addr);

void
trunk_context_deinit(trunk_context *context);

/* Create a writable snapshot of a trunk */
platform_status
trunk_context_clone(trunk_context *dst, trunk_context *src);

/* Make a trunk durable */
platform_status
trunk_make_durable(trunk_context *context);

/********************************
 * Mutations
 ********************************/

void
trunk_modification_begin(trunk_context *context);

// Build a new trunk with the branch incorporated.  The new trunk is not yet
// visible to queriers.
platform_status
trunk_incorporate_prepare(trunk_context *context, uint64 branch);

// Must be called iff trunk_incorporate_prepare returned SUCCESS
// This switches to the new trunk with the new branch incorporated.
// This is the only step that must be done atomically with removing the
// incorporated branch from the queue of memtables.
void
trunk_incorporate_commit(trunk_context *context);

// This must be called iff trunk_incorporate_prepare returned SUCCESS
// This must be called after trunk_incorporate_commit.
// This cleans up the old trunk and enqueues background rebalancing jobs.
void
trunk_incorporate_cleanup(trunk_context *context);

void
trunk_modification_end(trunk_context *context);

/********************************
 * Queries
 ********************************/

platform_status
trunk_init_root_handle(trunk_context            *context,
                       trunk_ondisk_node_handle *handle);

uint64
trunk_ondisk_node_handle_addr(const trunk_ondisk_node_handle *handle);

void
trunk_ondisk_node_handle_deinit(trunk_ondisk_node_handle *handle);

platform_status
trunk_merge_lookup(trunk_context            *context,
                   trunk_ondisk_node_handle *handle,
                   key                       tgt,
                   merge_accumulator        *result,
                   platform_log_handle      *log);

typedef struct trunk_branch_info {
   uint64    addr;
   page_type type;
} trunk_branch_info;

platform_status
trunk_collect_branches(const trunk_context            *context,
                       const trunk_ondisk_node_handle *handle,
                       key                             tgt,
                       comparison                      start_type,
                       uint64                          capacity,
                       uint64                         *num_branches,
                       trunk_branch_info              *branches,
                       key_buffer                     *min_key,
                       key_buffer                     *max_key);

typedef struct trunk_ondisk_pivot  trunk_ondisk_pivot;
typedef struct trunk_ondisk_bundle trunk_ondisk_bundle;

// clang-format off
DEFINE_ASYNC_STATE(trunk_merge_lookup_async_state, 4,
   param, trunk_context *,            context,
   param, trunk_ondisk_node_handle *, inhandle,
   param, key,                        tgt,
   param, merge_accumulator *,        result,
   param, platform_log_handle *,      log,
   param, async_callback_fn,          callback,
   param, void *,                     callback_arg,
   local, platform_status,            __async_result,
   local, platform_status,            rc,
   local, trunk_ondisk_node_handle,   handle,
   local, trunk_ondisk_node_handle *, handlep,
   local, uint64,                     height,
   local, trunk_ondisk_pivot *,       pivot,
   local, uint64,                     inflight_bundle_num,
   local, trunk_ondisk_bundle *,      bndl,
   local, trunk_ondisk_node_handle,   child_handle,
   // ondisk_node_handle_setup_content_page
   // ondisk_node_get_pivot
   // ondisk_node_bundle_at_offset
   // ondisk_node_get_first_inflight_bundle
   local, uint64,                       offset,
   local, page_handle **,               page,
   local, uint64,                       pivot_num,
   local, page_get_async_state_buffer,  cache_get_state,   
   // ondisk_node_find_pivot
   local, uint64,                       min,
   local, uint64,                       max,
   local, uint64,                       mid,
   local, int,                          last_cmp,
   local, trunk_ondisk_pivot *,         min_pivot,
   // ondisk_bundle_merge_lookup
   local, uint64,                             found_values,
   local, uint64,                             idx,
   local, routing_filter_lookup_async_state,  filter_state,
   local, btree_lookup_async_state,           btree_state,
 )
// clang-format on

async_status
trunk_merge_lookup_async(trunk_merge_lookup_async_state *state);

/**********************************
 * Statistics
 **********************************/

void
trunk_print_insertion_stats(platform_log_handle *log_handle,
                            const trunk_context *context);

void
trunk_print_space_use(platform_log_handle *log_handle, trunk_context *context);

void
trunk_reset_stats(trunk_context *context);