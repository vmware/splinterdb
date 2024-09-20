// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * trunk_node.h --
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

typedef struct trunk_node_config {
   const data_config    *data_cfg;
   const btree_config   *btree_cfg;
   const routing_config *filter_cfg;
   uint64                leaf_split_threshold_kv_bytes;
   uint64                target_leaf_kv_bytes;
   uint64                target_fanout;
   uint64                per_child_flush_threshold_kv_bytes;
   bool32                use_stats;
} trunk_node_config;

#define TRUNK_NODE_MAX_HEIGHT             16
#define TRUNK_NODE_MAX_DISTRIBUTION_VALUE 16

typedef struct trunk_node_stats {
   uint64 fanout_distribution[TRUNK_NODE_MAX_DISTRIBUTION_VALUE]
                             [TRUNK_NODE_MAX_HEIGHT];
   uint64 num_inflight_bundles_distribution[TRUNK_NODE_MAX_DISTRIBUTION_VALUE]
                                           [TRUNK_NODE_MAX_HEIGHT];
   uint64 bundle_num_branches_distribution[TRUNK_NODE_MAX_DISTRIBUTION_VALUE]
                                          [TRUNK_NODE_MAX_HEIGHT];

   uint64 node_size_pages_distribution[TRUNK_NODE_MAX_DISTRIBUTION_VALUE]
                                      [TRUNK_NODE_MAX_HEIGHT];

   uint64
      incorporation_footprint_distribution[TRUNK_NODE_MAX_DISTRIBUTION_VALUE];

   uint64 count_flushes[TRUNK_NODE_MAX_HEIGHT];
   uint64 flush_time_ns[TRUNK_NODE_MAX_HEIGHT];
   uint64 flush_time_max_ns[TRUNK_NODE_MAX_HEIGHT];
   uint64 full_flushes[TRUNK_NODE_MAX_HEIGHT];

   // We don't know whether a node is the root. So we can't track these stats
   // carrying around some extra information that would be useful only for
   // collecting these stats.
   // uint64 root_full_flushes;
   // uint64 root_count_flushes;
   // uint64 root_flush_time_ns;
   // uint64 root_flush_time_max_ns;
   // uint64 root_flush_wait_time_ns;

   uint64 compactions[TRUNK_NODE_MAX_HEIGHT];
   uint64 compactions_aborted[TRUNK_NODE_MAX_HEIGHT];
   uint64 compactions_discarded[TRUNK_NODE_MAX_HEIGHT];
   uint64 compactions_empty[TRUNK_NODE_MAX_HEIGHT];
   uint64 compaction_tuples[TRUNK_NODE_MAX_HEIGHT];
   uint64 compaction_max_tuples[TRUNK_NODE_MAX_HEIGHT];
   uint64 compaction_time_ns[TRUNK_NODE_MAX_HEIGHT];
   uint64 compaction_time_max_ns[TRUNK_NODE_MAX_HEIGHT];
   uint64 compaction_time_wasted_ns[TRUNK_NODE_MAX_HEIGHT];
   uint64 compaction_pack_time_ns[TRUNK_NODE_MAX_HEIGHT];

   uint64 maplet_builds[TRUNK_NODE_MAX_HEIGHT];
   uint64 maplet_builds_aborted[TRUNK_NODE_MAX_HEIGHT];
   uint64 maplet_builds_discarded[TRUNK_NODE_MAX_HEIGHT];
   uint64 maplet_build_time_ns[TRUNK_NODE_MAX_HEIGHT];
   uint64 maplet_tuples[TRUNK_NODE_MAX_HEIGHT];
   uint64 maplet_build_time_max_ns[TRUNK_NODE_MAX_HEIGHT];
   uint64 maplet_build_time_wasted_ns[TRUNK_NODE_MAX_HEIGHT];

   uint64 node_splits[TRUNK_NODE_MAX_HEIGHT];
   uint64 node_splits_nodes_created[TRUNK_NODE_MAX_HEIGHT];
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

   uint64 maplet_lookups[TRUNK_NODE_MAX_HEIGHT];
   uint64 maplet_false_positives[TRUNK_NODE_MAX_HEIGHT];
   uint64 branch_lookups[TRUNK_NODE_MAX_HEIGHT];

   // Not yet implemented
   // uint64 space_recs[TRUNK_NODE_MAX_HEIGHT];
   // uint64 space_rec_time_ns[TRUNK_NODE_MAX_HEIGHT];
   // uint64 space_rec_tuples_reclaimed[TRUNK_NODE_MAX_HEIGHT];
   // uint64 tuples_reclaimed[TRUNK_NODE_MAX_HEIGHT];
} PLATFORM_CACHELINE_ALIGNED trunk_node_stats;

#define PIVOT_STATE_MAP_BUCKETS 1024

typedef struct pivot_compaction_state pivot_compaction_state;

typedef struct pivot_state_map {
   uint64                  num_states;
   uint64                  locks[PIVOT_STATE_MAP_BUCKETS];
   pivot_compaction_state *buckets[PIVOT_STATE_MAP_BUCKETS];
} pivot_state_map;

/* An ondisk_node_ref is a pivot that has an associated bump in the refcount of
 * the child, so destroying an ondisk_node_ref will perform an
 * ondisk_node_dec_ref. */
typedef struct ondisk_node_ref {
   uint64     addr;
   ondisk_key key;
} ondisk_node_ref;


typedef struct trunk_node_context {
   const trunk_node_config *cfg;
   platform_heap_id         hid;
   cache                   *cc;
   allocator               *al;
   task_system             *ts;
   trunk_node_stats        *stats;
   pivot_state_map          pivot_states;
   platform_batch_rwlock    root_lock;
   ondisk_node_ref         *root;
} trunk_node_context;

typedef struct ondisk_node_handle {
   cache       *cc;
   page_handle *header_page;
   page_handle *content_page;
} ondisk_node_handle;

typedef VECTOR(iterator *) iterator_vector;

typedef struct branch_merger {
   platform_heap_id   hid;
   const data_config *data_cfg;
   key                min_key;
   key                max_key;
   uint64             height;
   merge_iterator    *merge_itor;
   iterator_vector    itors;
} branch_merger;

/********************************
 * Lifecycle
 ********************************/

void
trunk_node_config_init(trunk_node_config    *config,
                       const data_config    *data_cfg,
                       const btree_config   *btree_cfg,
                       const routing_config *filter_cfg,
                       uint64                leaf_split_threshold_kv_bytes,
                       uint64                target_leaf_kv_bytes,
                       uint64                target_fanout,
                       uint64                per_child_flush_threshold_kv_bytes,
                       bool32                use_stats);

platform_status
trunk_node_context_init(trunk_node_context      *context,
                        const trunk_node_config *cfg,
                        platform_heap_id         hid,
                        cache                   *cc,
                        allocator               *al,
                        task_system             *ts,
                        uint64                   root_addr);


void
trunk_node_context_deinit(trunk_node_context *context);

/* Create a writable snapshot of a trunk */
platform_status
trunk_node_context_clone(trunk_node_context *dst, trunk_node_context *src);

/* Make a trunk durable */
platform_status
trunk_node_make_durable(trunk_node_context *context);

/********************************
 * Mutations
 ********************************/

void
trunk_modification_begin(trunk_node_context *context);

platform_status
trunk_incorporate(trunk_node_context *context,
                  routing_filter      filter,
                  uint64              branch);

void
trunk_modification_end(trunk_node_context *context);

/********************************
 * Queries
 ********************************/

platform_status
trunk_init_root_handle(trunk_node_context *context, ondisk_node_handle *handle);

void
trunk_ondisk_node_handle_deinit(ondisk_node_handle *handle);

platform_status
trunk_merge_lookup(trunk_node_context *context,
                   ondisk_node_handle *handle,
                   key                 tgt,
                   merge_accumulator  *result);

platform_status
trunk_collect_branches(const trunk_node_context *context,
                       const ondisk_node_handle *handle,
                       key                       tgt,
                       comparison                start_type,
                       uint64                    capacity,
                       uint64                   *num_branches,
                       uint64                   *branches,
                       key_buffer               *min_key,
                       key_buffer               *max_key);

/**********************************
 * Statistics
 **********************************/

void
trunk_node_print_insertion_stats(platform_log_handle      *log_handle,
                                 const trunk_node_context *context);

void
trunk_node_reset_stats(trunk_node_context *context);