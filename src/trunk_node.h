#include "platform.h"
#include "data_internal.h"
#include "allocator.h"
#include "cache.h"

typedef struct branch_ref branch_ref;
typedef struct maplet_ref maplet_ref;

/*
 * Bundles are used to represent groups of branches that have not yet
 * been incorporated into the per-pivot filters.
 */
typedef struct routed_bundle    routed_bundle;
typedef struct compacted_bundle compacted_bundle;
typedef struct inflight_bundle  inflight_bundle;
typedef struct pivot            pivot;

typedef struct in_memory_node {
   platform_heap_id hid;
   uint16           height;
   uint64           num_pivots;
   pivot           *pivots;
   routed_bundle  **pivot_bundles; // indexed by child
   uint64           num_inflight_bundles;
   inflight_bundle *inflight_bundles;
} in_memory_node;

/*
 * Policy functions
 */

uint64
trunk_node_flush_select_child(in_memory_node *node);

uint64
trunk_node_needs_split(in_memory_node *node);

/*
 * Incorporation and flushing-related functions
 */

routed_bundle *
trunk_node_extract_pivot_bundle(in_memory_node *node, uint64 child_num);

uint64
trunk_node_extract_inflight_bundles(in_memory_node   *node,
                                    uint64            child_num,
                                    inflight_bundle **bundles);

platform_status
trunk_node_append_pivot_bundle(in_memory_node *node, routed_bundle *bundle);

platform_status
trunk_node_append_inflight_bundles(in_memory_node  *node,
                                   uint64           num_bundles,
                                   inflight_bundle *bundles);

platform_status
trunk_node_split_leaf(in_memory_node *node,
                      uint64          num_pivots,
                      key_buffer     *pivots,
                      in_memory_node *results);

platform_status
trunk_node_split_index(in_memory_node  *node,
                       uint64           max_fanout,
                       uint64          *num_results,
                       in_memory_node **results);

platform_status
trunk_node_create_root(in_memory_node *node);

platform_status
trunk_node_add_pivots(in_memory_node *node, uint64 num_pivots, pivot *pivots);

/*
 * Branch and filter compaction-related functions
 */

platform_status
trunk_node_replace_inflight_bundles(in_memory_node  *node,
                                    uint64           num_old_bundles,
                                    inflight_bundle *old_bundles,
                                    inflight_bundle *new_bundle);

platform_status
trunk_node_replace_pivot_maplets(in_memory_node   *node,
                                 compacted_bundle *old_bundle,
                                 maplet_ref       *old_maplets,
                                 maplet_ref       *new_maplets);

uint64
trunk_node_height(in_memory_node *node);

uint64
trunk_node_child(in_memory_node *node, key target);

/*
 * Marshalling and un-marshalling functions
 */

platform_status
trunk_node_marshall(in_memory_node *node,
                    allocator      *al,
                    cache          *cc,
                    uint64         *addr);

platform_status
trunk_node_unmarshall(platform_heap_id hid,
                      cache           *cc,
                      uint64           addr,
                      in_memory_node  *result);

/*
 * Query functions
 */

platform_status
trunk_node_lookup_and_merge(cache             *cc,
                            uint64             addr,
                            key                target,
                            merge_accumulator *data,
                            uint64            *child_addr);

platform_status
trunk_node_get_range_query_info(cache           *cc,
                                uint64           addr,
                                key              target,
                                key_buffer      *lower_bound,
                                key_buffer      *upper_bound,
                                writable_buffer *branches,
                                uint64          *child_addr);
