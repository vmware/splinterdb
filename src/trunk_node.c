// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * trunk_node.c --
 *
 *     This file contains the implementation SplinterDB trunk nodes.
 */

//#include "trunk_node.h"
#include "platform.h"
#include "data_internal.h"
#include "util.h"
#include "btree.h"
#include "routing_filter.h"
#include "vector.h"
#include "merge.h"
#include "data_internal.h"
#include "task.h"
#include "poison.h"

typedef struct ONDISK branch_ref {
   uint64 addr;
} branch_ref;

#if 0 // To be moved later in file
/*
 * Routed bundles are used to represent the pivot bundles, i.e. one
 * maplet that covers some number of branches.
 */
typedef struct ONDISK routed_bundle {
   routing_filter maplet;
   uint16         num_branches;
   branch_ref     branches[];
} routed_bundle;

/*
 * A compaction produces a per-child bundle, which has one branch per
 * child of the node, plus several maplets, each of which acts like a
 * filter.
 */
typedef struct ONDISK per_child_bundle {
   uint64         num_maplets;
   routing_filter maplets[];
   /* Following the maplets is one branch per child. */
} per_child_bundle;

/*
 * When flushing a per-child bundle, only the branch for that child is
 * flushed to the child.  This results in a singleton bundle, i.e. a
 * bundle with a single branch and multiple maplets, each of which
 * acts as a filter.
 */
typedef struct ONDISK singleton_bundle {
   branch_ref     branch;
   uint64         num_maplets;
   routing_filter maplets[];
} singleton_bundle;
#endif

typedef struct ONDISK trunk_pivot_stats {
   uint64 num_kv_bytes;
   uint64 num_tuples;
} trunk_pivot_stats;

typedef struct ONDISK pivot {
   trunk_pivot_stats stats;
   uint64            child_addr;
   uint64            inflight_bundle_start;
   ondisk_key        key;
} pivot;

typedef VECTOR(routing_filter) routing_filter_vector;
typedef VECTOR(branch_ref) branch_ref_vector;

typedef struct in_memory_routed_bundle {
   routing_filter    maplet;
   branch_ref_vector branches;
} in_memory_routed_bundle;

typedef struct ONDISK in_memory_pivot {
   trunk_pivot_stats prereceive_stats;
   trunk_pivot_stats stats;
   uint64            child_addr;
   uint64            inflight_bundle_start;
   ondisk_key        key;
} in_memory_pivot;

typedef VECTOR(in_memory_pivot *) in_memory_pivot_vector;
typedef VECTOR(in_memory_routed_bundle) in_memory_routed_bundle_vector;
typedef VECTOR(trunk_pivot_stats) trunk_pivot_stats_vector;

typedef struct in_memory_node {
   uint16                         height;
   in_memory_pivot_vector         pivots;
   in_memory_routed_bundle_vector pivot_bundles; // indexed by child
   uint64                         num_old_bundles;
   in_memory_routed_bundle_vector inflight_bundles;
} in_memory_node;

typedef VECTOR(in_memory_node) in_memory_node_vector;

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

typedef enum bundle_compaction_state {
   BUNDLE_COMPACTION_NOT_STARTED = 0,
   BUNDLE_COMPACTION_IN_PROGRESS = 1,
   BUNDLE_COMPACTION_MIN_ENDED   = 2,
   BUNDLE_COMPACTION_FAILED      = 2,
   BUNDLE_COMPACTION_SUCCEEDED   = 3
} bundle_compaction_state;

typedef struct bundle_compaction {
   struct bundle_compaction *next;
   bundle_compaction_state   state;
   branch_merger             merger;
   branch_ref                branch;
   uint64                    num_fingerprints;
   uint32                   *fingerprints;
} bundle_compaction;

typedef struct trunk_node_context trunk_node_context;

typedef struct pivot_compaction_state {
   struct pivot_compaction_state *next;
   trunk_node_context            *context;
   key_buffer                     key;
   uint64                         height;
   routing_filter                 maplet;
   uint64                         num_branches;
   bool32                         maplet_compaction_failed;
   bundle_compaction             *bundle_compactions;
} pivot_compaction_state;

#define PIVOT_STATE_MAP_BUCKETS 1024

typedef struct pivot_state_map {
   uint64                  locks[PIVOT_STATE_MAP_BUCKETS];
   pivot_compaction_state *buckets[PIVOT_STATE_MAP_BUCKETS];
} pivot_state_map;

typedef struct trunk_node_config {
   const data_config    *data_cfg;
   const btree_config   *btree_cfg;
   const routing_config *filter_cfg;
   uint64                leaf_split_threshold_kv_bytes;
   uint64                target_leaf_kv_bytes;
   uint64                target_fanout;
   uint64                per_child_flush_threshold_kv_bytes;
   uint64                max_tuples_per_node;
} trunk_node_config;

struct trunk_node_context {
   const trunk_node_config *cfg;
   platform_heap_id         hid;
   cache                   *cc;
   allocator               *al;
   task_system             *ts;
   pivot_state_map          pivot_states;
   uint64                   root_height;
   uint64                   root_addr;
};

/***************************************************
 * branch_ref operations
 ***************************************************/

/* static */ inline branch_ref
create_branch_ref(uint64 addr)
{
   return (branch_ref){.addr = addr};
}

static inline uint64
branch_ref_addr(branch_ref bref)
{
   return bref.addr;
}

#define NULL_BRANCH_REF ((branch_ref){.addr = 0})

static inline bool32
branches_equal(branch_ref a, branch_ref b)
{
   return a.addr == b.addr;
}

/**************************
 * routed_bundle operations
 **************************/

static inline void
in_memory_routed_bundle_init(in_memory_routed_bundle *bundle,
                             platform_heap_id         hid)
{
   bundle->maplet = NULL_ROUTING_FILTER;
   vector_init(&bundle->branches, hid);
}

static inline platform_status
in_memory_routed_bundle_init_single(in_memory_routed_bundle *bundle,
                                    platform_heap_id         hid,
                                    routing_filter           maplet,
                                    branch_ref               branch)
{
   bundle->maplet = maplet;
   vector_init(&bundle->branches, hid);
   platform_status rc = vector_append(&bundle->branches, branch);
   if (!SUCCESS(rc)) {
      vector_deinit(&bundle->branches);
   }
   return rc;
}

static inline platform_status
in_memory_routed_bundle_init_copy(in_memory_routed_bundle       *dst,
                                  platform_heap_id               hid,
                                  const in_memory_routed_bundle *src)
{
   vector_init(&dst->branches, hid);
   platform_status rc = vector_copy(&dst->branches, &src->branches);
   if (!SUCCESS(rc)) {
      vector_deinit(&dst->branches);
      return rc;
   }
   dst->maplet = src->maplet;

   return rc;
}

static inline void
in_memory_routed_bundle_deinit(in_memory_routed_bundle *bundle)
{
   vector_deinit(&bundle->branches);
}

static inline void
in_memory_routed_bundle_reset(in_memory_routed_bundle *bundle)
{
   vector_truncate(&bundle->branches, 0);
   bundle->maplet = NULL_ROUTING_FILTER;
}

static inline platform_status
in_memory_routed_bundle_add_branches(in_memory_routed_bundle *bundle,
                                     routing_filter           new_maplet,
                                     branch_ref_vector       *new_branches)
{
   platform_status rc;
   rc = vector_append_vector(&bundle->branches, new_branches);
   if (!SUCCESS(rc)) {
      return rc;
   }
   bundle->maplet = new_maplet;

   return STATUS_OK;
}

static inline routing_filter
in_memory_routed_bundle_maplet(const in_memory_routed_bundle *bundle)
{
   return bundle->maplet;
}

static inline uint64
in_memory_routed_bundle_num_branches(const in_memory_routed_bundle *bundle)
{
   return vector_length(&bundle->branches);
}

static inline branch_ref
in_memory_routed_bundle_branch(const in_memory_routed_bundle *bundle, uint64 i)
{
   debug_assert(i < vector_length(&bundle->branches));
   return vector_get(&bundle->branches, i);
}

/********************
 * Pivot stats
 ********************/

static inline trunk_pivot_stats
trunk_pivot_stats_from_btree_pivot_stats(btree_pivot_stats stats)
{
   return (trunk_pivot_stats){.num_kv_bytes =
                                 stats.key_bytes + stats.message_bytes,
                              .num_tuples = stats.num_kvs};
}

static inline trunk_pivot_stats
trunk_pivot_stats_subtract(trunk_pivot_stats a, trunk_pivot_stats b)
{
   platform_assert(a.num_kv_bytes >= b.num_kv_bytes);
   platform_assert(a.num_tuples >= b.num_tuples);
   return (trunk_pivot_stats){.num_kv_bytes = a.num_kv_bytes - b.num_kv_bytes,
                              .num_tuples   = a.num_tuples - b.num_tuples};
}

/******************
 * pivot operations
 ******************/

#define TRUNK_STATS_ZERO                                                       \
   ((trunk_pivot_stats){.num_kv_bytes = 0, .num_tuples = 0})

static inline in_memory_pivot *
in_memory_pivot_create(platform_heap_id  hid,
                       key               k,
                       uint64            child_addr,
                       uint64            inflight_bundle_start,
                       trunk_pivot_stats prereceive_stats,
                       trunk_pivot_stats stats)
{
   in_memory_pivot *result = TYPED_FLEXIBLE_STRUCT_ZALLOC(
      hid, result, key.bytes, ondisk_key_required_data_capacity(k));
   if (result == NULL) {
      return NULL;
   }
   copy_key_to_ondisk_key(&result->key, k);
   result->child_addr            = child_addr;
   result->inflight_bundle_start = inflight_bundle_start;
   result->prereceive_stats      = prereceive_stats;
   result->stats                 = stats;
   return result;
}

static inline in_memory_pivot *
in_memory_pivot_copy(platform_heap_id hid, in_memory_pivot *src)
{
   return in_memory_pivot_create(hid,
                                 ondisk_key_to_key(&src->key),
                                 src->child_addr,
                                 src->inflight_bundle_start,
                                 src->prereceive_stats,
                                 src->stats);
}

static inline void
in_memory_pivot_destroy(in_memory_pivot *pivot, platform_heap_id hid)
{
   platform_free(hid, pivot);
}

static inline key
in_memory_pivot_key(const in_memory_pivot *pivot)
{
   return ondisk_key_to_key(&pivot->key);
}

static inline uint64
in_memory_pivot_child_addr(const in_memory_pivot *pivot)
{
   return pivot->child_addr;
}

static inline void
in_memory_pivot_set_child_addr(in_memory_pivot *pivot, uint64 new_child_addr)
{
   pivot->child_addr = new_child_addr;
}


static inline trunk_pivot_stats
in_memory_pivot_stats(const in_memory_pivot *pivot)
{
   return pivot->stats;
}

static inline uint64
in_memory_pivot_inflight_bundle_start(const in_memory_pivot *pivot)
{
   return pivot->inflight_bundle_start;
}

static inline void
in_memory_pivot_set_inflight_bundle_start(in_memory_pivot *pivot, uint64 start)
{
   pivot->inflight_bundle_start = start;
}

static inline trunk_pivot_stats
in_memory_pivot_received_bundles_stats(const in_memory_pivot *pivot)
{
   return trunk_pivot_stats_subtract(pivot->stats, pivot->prereceive_stats);
}

static inline uint64
in_memory_pivot_num_kv_bytes(const in_memory_pivot *pivot)
{
   return pivot->stats.num_kv_bytes;
}

/*
 * When new bundles get flushed to this pivot's node, you must
 * inform the pivot of the tuple counts of the new bundles.
 */
static inline void
in_memory_pivot_add_tuple_counts(in_memory_pivot  *pivot,
                                 int               coefficient,
                                 trunk_pivot_stats stats)
{
   if (coefficient == 1) {
      pivot->stats.num_tuples += stats.num_tuples;
      pivot->stats.num_kv_bytes += stats.num_kv_bytes;
   } else if (coefficient == -1) {
      platform_assert(stats.num_tuples <= pivot->stats.num_tuples);
      platform_assert(stats.num_kv_bytes <= pivot->stats.num_kv_bytes);
      pivot->stats.num_tuples -= stats.num_tuples;
      pivot->stats.num_kv_bytes -= stats.num_kv_bytes;
   } else {
      platform_assert(0);
   }
}

/***********************
 * basic node operations
 ***********************/

static inline void
in_memory_node_init(in_memory_node                *node,
                    uint16                         height,
                    in_memory_pivot_vector         pivots,
                    in_memory_routed_bundle_vector pivot_bundles,
                    uint64                         num_old_bundles,
                    in_memory_routed_bundle_vector inflight_bundles)
{
   node->height           = height;
   node->pivots           = pivots;
   node->pivot_bundles    = pivot_bundles;
   node->num_old_bundles  = num_old_bundles;
   node->inflight_bundles = inflight_bundles;
}

static platform_status
in_memory_node_init_empty_leaf(in_memory_node  *node,
                               platform_heap_id hid,
                               key              lb,
                               key              ub)
{
   in_memory_pivot_vector         pivots;
   in_memory_routed_bundle_vector pivot_bundles;
   in_memory_routed_bundle_vector inflight_bundles;
   platform_status                rc;

   vector_init(&pivots, hid);
   vector_init(&pivot_bundles, hid);
   vector_init(&inflight_bundles, hid);

   rc = vector_ensure_capacity(&pivots, 2);
   if (!SUCCESS(rc)) {
      goto cleanup_vectors;
   }

   rc = vector_ensure_capacity(&pivot_bundles, 1);
   if (!SUCCESS(rc)) {
      goto cleanup_vectors;
   }

   in_memory_pivot *lb_pivot =
      in_memory_pivot_create(hid, lb, 0, 0, TRUNK_STATS_ZERO, TRUNK_STATS_ZERO);
   in_memory_pivot *ub_pivot =
      in_memory_pivot_create(hid, ub, 0, 0, TRUNK_STATS_ZERO, TRUNK_STATS_ZERO);
   if (lb_pivot == NULL || ub_pivot == NULL) {
      rc = STATUS_NO_MEMORY;
      goto cleanup_pivots;
   }
   rc = vector_append(&pivots, lb_pivot);
   platform_assert_status_ok(rc);
   rc = vector_append(&pivots, ub_pivot);
   platform_assert_status_ok(rc);

   rc =
      VECTOR_EMPLACE_APPEND(&pivot_bundles, in_memory_routed_bundle_init, hid);
   platform_assert_status_ok(rc);

   in_memory_node_init(node, 0, pivots, pivot_bundles, 0, inflight_bundles);
   return STATUS_OK;

cleanup_pivots:
   if (lb_pivot != NULL) {
      in_memory_pivot_destroy(lb_pivot, hid);
   }
   if (ub_pivot != NULL) {
      in_memory_pivot_destroy(ub_pivot, hid);
   }
cleanup_vectors:
   VECTOR_APPLY_TO_ELTS(&pivots, in_memory_pivot_destroy, hid);
   vector_deinit(&pivots);
   VECTOR_APPLY_TO_PTRS(&pivot_bundles, in_memory_routed_bundle_deinit);
   vector_deinit(&pivot_bundles);
   vector_deinit(&inflight_bundles);
   return rc;
}

static inline uint64
in_memory_node_num_children(const in_memory_node *node)
{
   return vector_length(&node->pivots) - 1;
}

static inline in_memory_pivot *
in_memory_node_pivot(const in_memory_node *node, uint64 i)
{
   return vector_get(&node->pivots, i);
}

static inline key
in_memory_node_pivot_key(const in_memory_node *node, uint64 i)
{
   return in_memory_pivot_key(vector_get(&node->pivots, i));
}

static inline key
in_memory_node_pivot_min_key(const in_memory_node *node)
{
   return in_memory_pivot_key(vector_get(&node->pivots, 0));
}

static inline key
in_memory_node_pivot_max_key(const in_memory_node *node)
{
   return in_memory_pivot_key(
      vector_get(&node->pivots, vector_length(&node->pivots) - 1));
}

static inline in_memory_routed_bundle *
in_memory_node_pivot_bundle(in_memory_node *node, uint64 i)
{
   return vector_get_ptr(&node->pivot_bundles, i);
}

static inline uint64
in_memory_node_height(const in_memory_node *node)
{
   return node->height;
}

static inline bool32
in_memory_node_is_leaf(const in_memory_node *node)
{
   return node->height == 0;
}

static inline uint64
in_memory_leaf_num_tuples(const in_memory_node *node)
{
   trunk_pivot_stats stats =
      in_memory_pivot_stats(vector_get(&node->pivots, 0));
   return stats.num_tuples;
}

static inline uint64
in_memory_leaf_num_kv_bytes(const in_memory_node *node)
{
   trunk_pivot_stats stats =
      in_memory_pivot_stats(vector_get(&node->pivots, 0));
   return stats.num_kv_bytes;
}

static inline uint64
in_memory_node_num_old_bundles(const in_memory_node *node)
{
   return node->num_old_bundles;
}

static inline bool32
in_memory_node_pivot_has_received_bundles(const in_memory_node *node, uint64 i)
{
   in_memory_pivot *pivot = vector_get(&node->pivots, i);
   return in_memory_pivot_inflight_bundle_start(pivot) <= node->num_old_bundles;
}

static inline bool
in_memory_node_is_well_formed_leaf(const trunk_node_config *cfg,
                                   const in_memory_node    *node)
{
   bool basics =
      node->height == 0 && vector_length(&node->pivots) == 2
      && vector_length(&node->pivot_bundles) == 1
      && node->num_old_bundles <= vector_length(&node->inflight_bundles);
   if (!basics) {
      return FALSE;
   }

   in_memory_pivot *lb    = vector_get(&node->pivots, 0);
   in_memory_pivot *ub    = vector_get(&node->pivots, 1);
   key              lbkey = in_memory_pivot_key(lb);
   key              ubkey = in_memory_pivot_key(ub);
   return lb->child_addr == 0 && lb->inflight_bundle_start == 0
          && data_key_compare(cfg->data_cfg, lbkey, ubkey) < 0
          && lb->prereceive_stats.num_tuples <= lb->stats.num_tuples;
}

static bool
in_memory_node_is_well_formed_index(const data_config    *data_cfg,
                                    const in_memory_node *node)
{
   bool basics =
      0 < node->height && 1 < vector_length(&node->pivots)
      && vector_length(&node->pivot_bundles) == vector_length(&node->pivots) - 1
      && node->num_old_bundles <= vector_length(&node->inflight_bundles);
   if (!basics) {
      return FALSE;
   }

   for (uint64 i = 0; i < in_memory_node_num_children(node); i++) {
      in_memory_pivot *lb    = vector_get(&node->pivots, i);
      in_memory_pivot *ub    = vector_get(&node->pivots, i + 1);
      key              lbkey = in_memory_pivot_key(lb);
      key              ubkey = in_memory_pivot_key(ub);
      bool             valid_pivots =
         lb->child_addr != 0
         && lb->inflight_bundle_start <= vector_length(&node->inflight_bundles)
         && data_key_compare(data_cfg, lbkey, ubkey) < 0
         && lb->prereceive_stats.num_tuples <= lb->stats.num_tuples;
      if (!valid_pivots) {
         return FALSE;
      }
   }

   return TRUE;
}

static inline void
in_memory_node_deinit(in_memory_node *node, trunk_node_context *context)
{
   VECTOR_APPLY_TO_ELTS(
      &node->pivots, vector_apply_platform_free, context->hid);
   VECTOR_APPLY_TO_PTRS(&node->pivot_bundles, in_memory_routed_bundle_deinit);
   VECTOR_APPLY_TO_PTRS(&node->inflight_bundles,
                        in_memory_routed_bundle_deinit);
   vector_deinit(&node->pivots);
   vector_deinit(&node->pivot_bundles);
   vector_deinit(&node->inflight_bundles);
}

/**************************************
 * Refcounting
 **************************************/

void
on_disk_node_inc_ref(trunk_node_context *context, uint64 addr);

void
on_disk_node_dec_ref(trunk_node_context *context, uint64 addr);


/*********************************************
 * node de/serialization
 *********************************************/

in_memory_pivot *
in_memory_node_serialize(trunk_node_context *context, in_memory_node *node);

platform_status
in_memory_node_deserialize(trunk_node_context *context,
                           uint64              addr,
                           in_memory_node     *result);

static platform_status
serialize_nodes(trunk_node_context     *context,
                in_memory_node_vector  *nodes,
                in_memory_pivot_vector *result)
{
   platform_status rc;

   rc = vector_ensure_capacity(result, vector_length(nodes));
   if (!SUCCESS(rc)) {
      goto finish;
   }
   for (uint64 i = 0; i < vector_length(nodes); i++) {
      in_memory_pivot *pivot =
         in_memory_node_serialize(context, vector_get_ptr(nodes, i));
      if (pivot == NULL) {
         rc = STATUS_NO_MEMORY;
         goto finish;
      }
      rc = vector_append(result, pivot);
      platform_assert_status_ok(rc);
   }

finish:
   if (!SUCCESS(rc)) {
      for (uint64 i = 0; i < vector_length(result); i++) {
         on_disk_node_dec_ref(
            context, in_memory_pivot_child_addr(vector_get(result, i)));
      }
      VECTOR_APPLY_TO_ELTS(result, in_memory_pivot_destroy, context->hid);
      vector_truncate(result, 0);
   }

   return rc;
}

/*********************************************
 * branch_merger operations
 * (used in both leaf splits and compactions)
 *********************************************/

static inline void
branch_merger_init(branch_merger     *merger,
                   platform_heap_id   hid,
                   const data_config *data_cfg,
                   key                min_key,
                   key                max_key,
                   uint64             height)
{
   merger->hid        = hid;
   merger->data_cfg   = data_cfg;
   merger->min_key    = min_key;
   merger->max_key    = max_key;
   merger->height     = height;
   merger->merge_itor = NULL;
   vector_init(&merger->itors, hid);
}

static platform_status
branch_merger_add_routed_bundle(branch_merger           *merger,
                                cache                   *cc,
                                const btree_config      *btree_cfg,
                                in_memory_routed_bundle *routed)
{
   for (uint64 i = 0; i < in_memory_routed_bundle_num_branches(routed); i++) {
      btree_iterator *iter = TYPED_MALLOC(merger->hid, iter);
      if (iter == NULL) {
         return STATUS_NO_MEMORY;
      }
      branch_ref bref = in_memory_routed_bundle_branch(routed, i);
      btree_iterator_init(cc,
                          btree_cfg,
                          iter,
                          branch_ref_addr(bref),
                          PAGE_TYPE_BRANCH,
                          merger->min_key,
                          merger->max_key,
                          merger->min_key,
                          greater_than_or_equal,
                          TRUE,
                          merger->height);
      platform_status rc = vector_append(&merger->itors, (iterator *)iter);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   return STATUS_OK;
}

static inline platform_status
branch_merger_build_merge_itor(branch_merger *merger, merge_behavior merge_mode)
{
   platform_assert(merger == NULL);

   return merge_iterator_create(merger->hid,
                                merger->data_cfg,
                                vector_length(&merger->itors),
                                vector_data(&merger->itors),
                                merge_mode,
                                &merger->merge_itor);
}

static platform_status
branch_merger_deinit(branch_merger *merger)
{
   platform_status rc;
   if (merger->merge_itor != NULL) {
      rc = merge_iterator_destroy(merger->hid, &merger->merge_itor);
   }

   for (uint64 i = 0; i < vector_length(&merger->itors); i++) {
      btree_iterator *itor = (btree_iterator *)vector_get(&merger->itors, i);
      btree_iterator_deinit(itor);
      platform_free(merger->hid, itor);
   }
   vector_deinit(&merger->itors);

   return rc;
}

/*************************
 * generic code to apply changes to nodes in the tree.
 ************************/

typedef platform_status(apply_changes_fn)(trunk_node_context *context,
                                          uint64              addr,
                                          in_memory_node     *node,
                                          void               *arg);

void
apply_changes_begin(trunk_node_context *context);

platform_status
apply_changes_internal(trunk_node_context *context,
                       uint64              addr,
                       key                 minkey,
                       key                 maxkey,
                       uint64              height,
                       apply_changes_fn   *func,
                       void               *arg,
                       uint64             *new_addr)
{
   platform_status rc;

   in_memory_node node;
   rc = in_memory_node_deserialize(context, addr, &node);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (in_memory_node_height(&node) == height) {
      rc = func(context, addr, &node, arg);
   } else {

      for (uint64 i = 0; i < in_memory_node_num_children(&node); i++) {
         in_memory_pivot *child_pivot  = in_memory_node_pivot(&node, i);
         key              child_minkey = in_memory_pivot_key(child_pivot);
         key              child_maxkey = in_memory_node_pivot_key(&node, i + 1);
         if (data_key_compare(context->cfg->data_cfg, child_minkey, maxkey) < 0
             && data_key_compare(context->cfg->data_cfg, minkey, child_maxkey)
                   < 0)
         {
            uint64 child_addr = in_memory_pivot_child_addr(child_pivot);
            rc                = apply_changes_internal(context,
                                        child_addr,
                                        minkey,
                                        maxkey,
                                        height,
                                        func,
                                        arg,
                                        &child_addr);
            if (!SUCCESS(rc)) {
               break;
            }

            in_memory_pivot_set_child_addr(child_pivot, child_addr);
         }
      }

      if (SUCCESS(rc)) {
         in_memory_pivot *pivot = in_memory_node_serialize(context, &node);
         if (pivot == NULL) {
            rc = STATUS_NO_MEMORY;
         } else {
            *new_addr = in_memory_pivot_child_addr(pivot);
         }
      }
   }

   in_memory_node_deinit(&node, context);

   return rc;
}

platform_status
apply_changes(trunk_node_context *context,
              key                 minkey,
              key                 maxkey,
              uint64              height,
              apply_changes_fn   *func,
              void               *arg)
{
   return apply_changes_internal(context,
                                 context->root_addr,
                                 minkey,
                                 maxkey,
                                 height,
                                 func,
                                 arg,
                                 &context->root_addr);
}

void
apply_changes_end(trunk_node_context *context);

/*******************************************************************************
 * pivot state tracking
 *******************************************************************************/

static void
bundle_compaction_destroy(bundle_compaction  *compaction,
                          trunk_node_context *context)
{
   branch_merger_deinit(&compaction->merger);
   if (compaction->fingerprints) {
      platform_free(context->hid, compaction->fingerprints);
   }
   if (!branches_equal(compaction->branch, NULL_BRANCH_REF)) {
      btree_dec_ref(context->cc,
                    context->cfg->btree_cfg,
                    branch_ref_addr(compaction->branch),
                    PAGE_TYPE_BRANCH);
   }
   platform_free(context->hid, compaction);
}

static bundle_compaction *
bundle_compaction_create(in_memory_node     *node,
                         uint64              pivot_num,
                         trunk_node_context *context)
{
   platform_status    rc;
   bundle_compaction *result = TYPED_ZALLOC(context->hid, result);
   if (result == NULL) {
      return NULL;
   }
   result->state = BUNDLE_COMPACTION_NOT_STARTED;
   branch_merger_init(&result->merger,
                      context->hid,
                      context->cfg->data_cfg,
                      in_memory_node_pivot_key(node, pivot_num),
                      in_memory_node_pivot_key(node, pivot_num + 1),
                      0);
   for (uint64 i = node->num_old_bundles;
        i < vector_length(&node->inflight_bundles);
        i++)
   {
      rc = branch_merger_add_routed_bundle(
         &result->merger,
         context->cc,
         context->cfg->btree_cfg,
         vector_get_ptr(&node->inflight_bundles, i));
      if (!SUCCESS(rc)) {
         bundle_compaction_destroy(result, context);
         return NULL;
      }
   }
   return result;
}

static void
pivot_state_destroy(pivot_compaction_state *state)
{
   key_buffer_deinit(&state->key);
   routing_filter_dec_ref(state->context->cc, &state->maplet);
   bundle_compaction *bc = state->bundle_compactions;
   while (bc != NULL) {
      bundle_compaction *next = bc->next;
      bundle_compaction_destroy(bc, state->context);
      bc = next;
   }
   platform_free(state->context->hid, state);
}

static bool
pivot_compaction_state_is_done(const pivot_compaction_state *state)
{
   bool32             all_bundle_compactions_ended = TRUE;
   bundle_compaction *bc;
   for (bc = state->bundle_compactions; bc != NULL; bc = bc->next) {
      if (bc->state < BUNDLE_COMPACTION_MIN_ENDED) {
         all_bundle_compactions_ended = FALSE;
         break;
      }
   }
   bc = state->bundle_compactions;
   bool32 maplet_compaction_in_progress =
      bc != NULL && bc->state == BUNDLE_COMPACTION_SUCCEEDED
      && !state->maplet_compaction_failed;

   return all_bundle_compactions_ended && !maplet_compaction_in_progress;
}

static void
pivot_compaction_state_append_compaction(pivot_compaction_state *state,
                                         bundle_compaction      *compaction)
{
   if (state->bundle_compactions == NULL) {
      state->bundle_compactions = compaction;
   } else {
      bundle_compaction *last = state->bundle_compactions;
      while (last->next != NULL) {
         last = last->next;
      }
      last->next = compaction;
   }
}

static uint64
pivot_state_map_hash(const data_config *data_cfg, key lbkey, uint64 height)
{
   uint64 hash = data_cfg->key_hash(key_data(lbkey), key_length(lbkey), 271828);
   hash ^= height;
   return hash % PIVOT_STATE_MAP_BUCKETS;
}

typedef uint64 pivot_state_map_lock;

static void
pivot_state_map_aquire_lock(pivot_state_map_lock *lock,
                            trunk_node_context   *context,
                            pivot_state_map      *map,
                            key                   pivot,
                            uint64                height)
{
   *lock       = pivot_state_map_hash(context->cfg->data_cfg, pivot, height);
   uint64 wait = 1;
   while (__sync_val_compare_and_swap(&map->locks[*lock], 0, 1) != 0) {
      platform_sleep_ns(wait);
      wait = MIN(2 * wait, 2048);
   }
}

static void
pivot_state_map_release_lock(pivot_state_map_lock *lock, pivot_state_map *map)
{
   __sync_lock_release(&map->locks[*lock]);
}

static pivot_compaction_state *
pivot_state_map_get(trunk_node_context   *context,
                    pivot_state_map      *map,
                    pivot_state_map_lock *lock,
                    key                   pivot,
                    uint64                height)
{
   pivot_compaction_state *result = NULL;
   for (pivot_compaction_state *state = map->buckets[*lock]; state != NULL;
        state                         = state->next)
   {
      if (data_key_compare(
             context->cfg->data_cfg, key_buffer_key(&state->key), pivot)
             == 0
          && state->height == height)
      {
         result = state;
         break;
      }
   }
   return result;
}

static pivot_compaction_state *
pivot_state_map_create(trunk_node_context   *context,
                       pivot_state_map      *map,
                       pivot_state_map_lock *lock,
                       key                   pivot,
                       uint64                height)
{
   pivot_compaction_state *state = TYPED_ZALLOC(context->hid, state);
   if (state == NULL) {
      return NULL;
   }
   platform_status rc =
      key_buffer_init_from_key(&state->key, context->hid, pivot);
   if (!SUCCESS(rc)) {
      platform_free(context->hid, state);
      return NULL;
   }
   state->height       = height;
   state->next         = map->buckets[*lock];
   map->buckets[*lock] = state;
   return state;
}

static pivot_compaction_state *
pivot_state_map_get_or_create(trunk_node_context   *context,
                              pivot_state_map      *map,
                              pivot_state_map_lock *lock,
                              key                   pivot,
                              uint64                height)
{
   pivot_compaction_state *state =
      pivot_state_map_get(context, map, lock, pivot, height);
   if (state == NULL) {
      state = pivot_state_map_create(context, map, lock, pivot, height);
   }
   return state;
}

static void
pivot_state_map_remove(pivot_state_map        *map,
                       pivot_state_map_lock   *lock,
                       pivot_compaction_state *tgt)
{
   pivot_compaction_state *prev = NULL;
   for (pivot_compaction_state *state = map->buckets[*lock]; state != NULL;
        prev = state, state = state->next)
   {
      if (state == tgt) {
         if (prev == NULL) {
            map->buckets[*lock] = state->next;
         } else {
            prev->next = state->next;
         }
         break;
      }
   }
}

/*********************************************
 * maplet compaction
 *********************************************/

typedef struct maplet_compaction_apply_args {
   pivot_compaction_state *state;
   routing_filter          new_maplet;
   branch_ref_vector       branches;
} maplet_compaction_apply_args;

static platform_status
apply_changes_maplet_compaction(trunk_node_context *context,
                                uint64              addr,
                                in_memory_node     *target,
                                void               *arg)
{
   platform_status               rc;
   maplet_compaction_apply_args *args = (maplet_compaction_apply_args *)arg;

   for (uint64 i = 0; i < in_memory_node_num_children(target); i++) {
      in_memory_routed_bundle *bundle = in_memory_node_pivot_bundle(target, i);
      if (routing_filters_equal(&bundle->maplet, &args->state->maplet)) {
         rc = in_memory_routed_bundle_add_branches(
            bundle, args->new_maplet, &args->branches);
         if (!SUCCESS(rc)) {
            return rc;
         }
         in_memory_pivot *pivot = in_memory_node_pivot(target, i);
         in_memory_pivot_set_inflight_bundle_start(
            pivot,
            in_memory_pivot_inflight_bundle_start(pivot)
               + vector_length(&args->branches));
         break;
      }
   }

   return STATUS_OK;
}

static inline platform_status
enqueue_maplet_compaction(pivot_compaction_state *args);

static void
maplet_compaction_task(void *arg, void *scratch)
{
   platform_status              rc      = STATUS_OK;
   pivot_compaction_state      *state   = (pivot_compaction_state *)arg;
   trunk_node_context          *context = state->context;
   maplet_compaction_apply_args apply_args;
   apply_args.state = state;
   vector_init(&apply_args.branches, context->hid);

   routing_filter     new_maplet;
   routing_filter     old_maplet  = state->maplet;
   bundle_compaction *bc          = state->bundle_compactions;
   uint64             num_bundles = 0;
   while (bc != NULL && bc->state == BUNDLE_COMPACTION_SUCCEEDED) {
      rc = vector_append(&apply_args.branches, bc->branch);
      if (!SUCCESS(rc)) {
         goto cleanup;
      }
      bc->branch = NULL_BRANCH_REF;

      rc = routing_filter_add(context->cc,
                              context->cfg->filter_cfg,
                              context->hid,
                              &old_maplet,
                              &new_maplet,
                              bc->fingerprints,
                              bc->num_fingerprints,
                              state->num_branches + num_bundles);
      if (0 < num_bundles) {
         routing_filter_dec_ref(context->cc, &old_maplet);
      }
      if (!SUCCESS(rc)) {
         goto cleanup;
      }
      old_maplet = new_maplet;
      bc         = bc->next;
      num_bundles++;
   }

   platform_assert(0 < num_bundles);

   apply_args.new_maplet = new_maplet;

   apply_changes_begin(context);
   rc = apply_changes(context,
                      key_buffer_key(&state->key),
                      key_buffer_key(&state->key),
                      state->height,
                      apply_changes_maplet_compaction,
                      &apply_args);
   apply_changes_end(context);

cleanup:
   vector_deinit(&apply_args.branches);

   pivot_state_map_lock lock;
   pivot_state_map_aquire_lock(&lock,
                               context,
                               &context->pivot_states,
                               key_buffer_key(&state->key),
                               state->height);

   if (SUCCESS(rc)) {
      routing_filter_dec_ref(context->cc, &state->maplet);
      state->maplet = new_maplet;
      state->num_branches += num_bundles;
      while (state->bundle_compactions != bc) {
         bundle_compaction *next = state->bundle_compactions->next;
         bundle_compaction_destroy(state->bundle_compactions, context->hid);
         state->bundle_compactions = next;
      }
      if (state->bundle_compactions
          && state->bundle_compactions->state == BUNDLE_COMPACTION_SUCCEEDED)
      {
         enqueue_maplet_compaction(state);
      }
   } else {
      state->maplet_compaction_failed = TRUE;
      if (0 < num_bundles) {
         routing_filter_dec_ref(context->cc, &new_maplet);
      }
   }

   if (pivot_compaction_state_is_done(state)) {
      pivot_state_map_remove(&context->pivot_states, &lock, state);
      pivot_state_destroy(state);
   }

   pivot_state_map_release_lock(&lock, &context->pivot_states);
}

static inline platform_status
enqueue_maplet_compaction(pivot_compaction_state *args)
{
   return task_enqueue(
      args->context->ts, TASK_TYPE_NORMAL, maplet_compaction_task, args, FALSE);
}

/************************
 * bundle compaction
 ************************/

static void
bundle_compaction_task(void *arg, void *scratch)
{
   // FIXME: locking
   platform_status         rc;
   pivot_compaction_state *state   = (pivot_compaction_state *)arg;
   trunk_node_context     *context = state->context;

   // Find a bundle compaction that needs doing for this pivot
   bundle_compaction *bc = state->bundle_compactions;
   while (bc != NULL
          && !__sync_bool_compare_and_swap(&bc->state,
                                           BUNDLE_COMPACTION_NOT_STARTED,
                                           BUNDLE_COMPACTION_IN_PROGRESS))
   {
      bc = bc->next;
   }
   platform_assert(bc != NULL);

   btree_pack_req pack_req;
   btree_pack_req_init(&pack_req,
                       context->cc,
                       context->cfg->btree_cfg,
                       &bc->merger.merge_itor->super,
                       context->cfg->max_tuples_per_node,
                       context->cfg->filter_cfg->hash,
                       context->cfg->filter_cfg->seed,
                       context->hid);

   // This is just a quick shortcut to avoid wasting time on a compaction when
   // the pivot is already stuck due to an earlier maplet compaction failure.
   if (state->maplet_compaction_failed) {
      rc = STATUS_INVALID_STATE;
      goto cleanup;
   }

   rc = btree_pack(&pack_req);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   bc->num_fingerprints     = pack_req.num_tuples;
   bc->fingerprints         = pack_req.fingerprint_arr;
   pack_req.fingerprint_arr = NULL;

cleanup:
   btree_pack_req_deinit(&pack_req, context->hid);

   pivot_state_map_lock lock;
   pivot_state_map_aquire_lock(&lock,
                               context,
                               &context->pivot_states,
                               key_buffer_key(&state->key),
                               state->height);
   if (SUCCESS(rc)) {
      bc->state = BUNDLE_COMPACTION_SUCCEEDED;
   } else {
      bc->state = BUNDLE_COMPACTION_FAILED;
   }
   if (bc->state == BUNDLE_COMPACTION_SUCCEEDED
       && state->bundle_compactions == bc) {
      enqueue_maplet_compaction(state);
   } else if (pivot_compaction_state_is_done(state)) {
      pivot_state_map_remove(&context->pivot_states, &lock, state);
      pivot_state_destroy(state);
   }
   pivot_state_map_release_lock(&lock, &context->pivot_states);
}

static platform_status
enqueue_bundle_compaction(trunk_node_context *context,
                          uint64              addr,
                          in_memory_node     *node)
{
   uint64 height       = in_memory_node_height(node);
   uint64 num_children = in_memory_node_num_children(node);

   for (uint64 pivot_num = 0; pivot_num < num_children; pivot_num++) {
      if (in_memory_node_pivot_has_received_bundles(node, pivot_num)) {
         platform_status rc    = STATUS_OK;
         key             pivot = in_memory_node_pivot_key(node, pivot_num);

         pivot_state_map_lock lock;
         pivot_state_map_aquire_lock(
            &lock, context, &context->pivot_states, pivot, height);

         pivot_compaction_state *state = pivot_state_map_get_or_create(
            context, &context->pivot_states, &lock, pivot, height);
         if (state == NULL) {
            rc = STATUS_NO_MEMORY;
            goto next;
         }

         bundle_compaction *bc =
            bundle_compaction_create(node, pivot_num, context->hid);
         if (bc == NULL) {
            rc = STATUS_NO_MEMORY;
            goto next;
         }

         pivot_compaction_state_append_compaction(state, bc);

         rc = task_enqueue(context->ts,
                           TASK_TYPE_NORMAL,
                           bundle_compaction_task,
                           state,
                           FALSE);
         if (!SUCCESS(rc)) {
            goto next;
         }

      next:
         if (!SUCCESS(rc)) {
            if (bc) {
               bc->state = BUNDLE_COMPACTION_FAILED;
            }
            if (state->bundle_compactions == bc) {
               // We created this state entry but didn't enqueue a task for it,
               // so destroy it.
               pivot_state_map_remove(&context->pivot_states, &lock, state);
               pivot_state_destroy(state);
            }
         }

         pivot_state_map_release_lock(&lock, &context->pivot_states);
      }
   }

   return STATUS_OK;
}

static platform_status
enqueue_bundle_compactions(trunk_node_context     *context,
                           in_memory_pivot_vector *pivots,
                           in_memory_node_vector  *nodes)
{
   debug_assert(vector_length(pivots) == vector_length(nodes));

   for (uint64 i = 0; i < vector_length(pivots); i++) {
      platform_status  rc;
      in_memory_pivot *pivot = vector_get(pivots, i);
      in_memory_node  *node  = vector_get_ptr(nodes, i);
      rc                     = enqueue_bundle_compaction(
         context, in_memory_pivot_child_addr(pivot), node);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   return STATUS_OK;
}

static inline platform_status
serialize_nodes_and_enqueue_bundle_compactions(trunk_node_context     *context,
                                               in_memory_node_vector  *nodes,
                                               in_memory_pivot_vector *result)
{
   platform_status rc;

   rc = serialize_nodes(context, nodes, result);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = enqueue_bundle_compactions(context, result, nodes);
   if (!SUCCESS(rc)) {
      VECTOR_APPLY_TO_ELTS(result, in_memory_pivot_destroy, context->hid);
      vector_truncate(result, 0);
      return rc;
   }

   return rc;
}


/************************
 * accounting maintenance
 ************************/

static inline platform_status
accumulate_branch_tuple_counts_in_range(branch_ref          bref,
                                        trunk_node_context *context,
                                        key                 minkey,
                                        key                 maxkey,
                                        btree_pivot_stats  *acc)
{
   btree_pivot_stats stats;
   btree_count_in_range(context->cc,
                        context->cfg->btree_cfg,
                        branch_ref_addr(bref),
                        minkey,
                        maxkey,
                        &stats);
   acc->num_kvs += stats.num_kvs;
   acc->key_bytes += stats.key_bytes;
   acc->message_bytes += stats.message_bytes;

   return STATUS_OK;
}

static inline platform_status
accumulate_branches_tuple_counts_in_range(const branch_ref_vector *brefs,
                                          trunk_node_context      *context,
                                          key                      minkey,
                                          key                      maxkey,
                                          btree_pivot_stats       *acc)
{
   return VECTOR_FAILABLE_FOR_LOOP_ELTS(brefs,
                                        0,
                                        vector_length(brefs),
                                        accumulate_branch_tuple_counts_in_range,
                                        context,
                                        minkey,
                                        maxkey,
                                        acc);
}

static inline platform_status
accumulate_inflight_bundle_tuple_counts_in_range(
   in_memory_routed_bundle *bundle,
   trunk_node_context      *context,
   in_memory_pivot_vector  *pivots,
   uint64                   child_num,
   btree_pivot_stats       *acc)
{
   key minkey = in_memory_pivot_key(vector_get(pivots, child_num));
   key maxkey = in_memory_pivot_key(vector_get(pivots, child_num + 1));

   return accumulate_branches_tuple_counts_in_range(
      &bundle->branches, context, minkey, maxkey, acc);
}

/*****************************************************
 * Receive bundles -- used in flushes and leaf splits
 *****************************************************/

static platform_status
in_memory_node_receive_bundles(trunk_node_context             *context,
                               in_memory_node                 *node,
                               in_memory_routed_bundle        *routed,
                               in_memory_routed_bundle_vector *inflight,
                               uint64                          inflight_start,
                               uint64                          child_num)
{
   platform_status rc;

   rc = vector_ensure_capacity(&node->inflight_bundles,
                               (routed ? 1 : 0) + vector_length(inflight));
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (routed) {
      rc = VECTOR_EMPLACE_APPEND(&node->inflight_bundles,
                                 in_memory_routed_bundle_init_copy,
                                 context->hid,
                                 routed);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   for (uint64 i = 0; i < vector_length(inflight); i++) {
      in_memory_routed_bundle *bundle = vector_get_ptr(inflight, i);
      rc = VECTOR_EMPLACE_APPEND(&node->inflight_bundles,
                                 in_memory_routed_bundle_init_copy,
                                 context->hid,
                                 bundle);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   for (uint64 i = 0; i < in_memory_node_num_children(node); i++) {
      btree_pivot_stats btree_stats;
      ZERO_CONTENTS(&btree_stats);
      rc = accumulate_inflight_bundle_tuple_counts_in_range(
         vector_get_ptr(&node->inflight_bundles, inflight_start),
         context,
         &node->pivots,
         i,
         &btree_stats);
      if (!SUCCESS(rc)) {
         return rc;
      }
      trunk_pivot_stats trunk_stats =
         trunk_pivot_stats_from_btree_pivot_stats(btree_stats);
      in_memory_pivot *pivot = in_memory_node_pivot(node, i);
      in_memory_pivot_add_tuple_counts(pivot, 1, trunk_stats);
   }

   return rc;
}

/************************
 * leaf splits
 ************************/

static inline bool
leaf_might_need_to_split(const trunk_node_config *cfg, in_memory_node *leaf)
{
   return cfg->leaf_split_threshold_kv_bytes
          < in_memory_leaf_num_kv_bytes(leaf);
}

static platform_status
in_memory_leaf_estimate_unique_keys(trunk_node_context *context,
                                    in_memory_node     *leaf,
                                    uint64             *estimate)
{
   platform_status rc;

   debug_assert(in_memory_node_is_well_formed_leaf(context->cfg, leaf));

   routing_filter_vector maplets;
   vector_init(&maplets, context->hid);

   rc = VECTOR_MAP_PTRS(
      &maplets, in_memory_routed_bundle_maplet, &leaf->inflight_bundles);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   in_memory_routed_bundle pivot_bundle = vector_get(&leaf->pivot_bundles, 0);
   rc = vector_append(&maplets, in_memory_routed_bundle_maplet(&pivot_bundle));
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   uint64 num_sb_fp     = 0;
   uint64 num_sb_unique = 0;
   for (uint16 inflight_maplet_num = 0;
        inflight_maplet_num < vector_length(&maplets) - 1;
        inflight_maplet_num++)
   {
      routing_filter maplet = vector_get(&maplets, inflight_maplet_num);
      num_sb_fp += maplet.num_fingerprints;
      num_sb_unique += maplet.num_unique;
   }

   uint32 num_unique =
      routing_filter_estimate_unique_fp(context->cc,
                                        context->cfg->filter_cfg,
                                        context->hid,
                                        vector_data(&maplets),
                                        vector_length(&maplets));

   num_unique = routing_filter_estimate_unique_keys_from_count(
      context->cfg->filter_cfg, num_unique);

   uint64 num_leaf_sb_fp         = in_memory_leaf_num_tuples(leaf);
   uint64 est_num_leaf_sb_unique = num_sb_unique * num_leaf_sb_fp / num_sb_fp;
   uint64 est_num_non_leaf_sb_unique = num_sb_fp - est_num_leaf_sb_unique;

   uint64 est_leaf_unique = num_unique - est_num_non_leaf_sb_unique;
   *estimate              = est_leaf_unique;

cleanup:
   vector_deinit(&maplets);
   return STATUS_OK;
}

static inline platform_status
leaf_split_target_num_leaves(trunk_node_context *context,
                             in_memory_node     *leaf,
                             uint64             *target)
{
   debug_assert(in_memory_node_is_well_formed_leaf(context->cfg, leaf));

   if (!leaf_might_need_to_split(context->cfg, leaf)) {
      *target = 1;
      return STATUS_OK;
   }

   uint64          estimated_unique_keys;
   platform_status rc = in_memory_leaf_estimate_unique_keys(
      context, leaf, &estimated_unique_keys);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64 num_tuples = in_memory_leaf_num_tuples(leaf);
   if (estimated_unique_keys > num_tuples * 19 / 20) {
      estimated_unique_keys = num_tuples;
   }
   uint64 kv_bytes = in_memory_leaf_num_kv_bytes(leaf);
   uint64 estimated_unique_kv_bytes =
      estimated_unique_keys * kv_bytes / num_tuples;
   uint64 target_num_leaves =
      (estimated_unique_kv_bytes + context->cfg->target_leaf_kv_bytes / 2)
      / context->cfg->target_leaf_kv_bytes;
   if (target_num_leaves < 1) {
      target_num_leaves = 1;
   }

   *target = target_num_leaves;

   return STATUS_OK;
}

typedef VECTOR(key_buffer) key_buffer_vector;

static platform_status
leaf_split_select_pivots(trunk_node_context *context,
                         in_memory_node     *leaf,
                         uint64              target_num_leaves,
                         key_buffer_vector  *pivots)
{
   platform_status  rc;
   in_memory_pivot *first   = vector_get(&leaf->pivots, 0);
   in_memory_pivot *last    = vector_get(&leaf->pivots, 1);
   key              min_key = ondisk_key_to_key(&first->key);
   key              max_key = ondisk_key_to_key(&last->key);

   rc = VECTOR_EMPLACE_APPEND(
      pivots, key_buffer_init_from_key, context->hid, min_key);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   branch_merger merger;
   branch_merger_init(
      &merger, context->hid, context->cfg->data_cfg, min_key, max_key, 1);

   rc =
      branch_merger_add_routed_bundle(&merger,
                                      context->cc,
                                      context->cfg->btree_cfg,
                                      vector_get_ptr(&leaf->pivot_bundles, 0));
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   for (uint64 bundle_num = 0;
        bundle_num < vector_length(&leaf->inflight_bundles);
        bundle_num++)
   {
      in_memory_routed_bundle *bundle =
         vector_get_ptr(&leaf->inflight_bundles, bundle_num);
      rc = branch_merger_add_routed_bundle(
         &merger, context->cc, context->cfg->btree_cfg, bundle);
      if (!SUCCESS(rc)) {
         goto cleanup;
      }
   }

   rc = branch_merger_build_merge_itor(&merger, MERGE_RAW);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   uint64 leaf_num            = 1;
   uint64 cumulative_kv_bytes = 0;
   while (!iterator_can_next(&merger.merge_itor->super)
          && leaf_num < target_num_leaves)
   {
      key     curr_key;
      message pivot_data_message;
      iterator_curr(&merger.merge_itor->super, &curr_key, &pivot_data_message);
      const btree_pivot_data *pivot_data = message_data(pivot_data_message);
      uint64                  new_cumulative_kv_bytes = cumulative_kv_bytes
                                       + pivot_data->stats.key_bytes
                                       + pivot_data->stats.message_bytes;
      uint64 next_boundary =
         leaf_num * in_memory_leaf_num_kv_bytes(leaf) / target_num_leaves;
      if (cumulative_kv_bytes < next_boundary
          && next_boundary <= new_cumulative_kv_bytes)
      {
         rc = VECTOR_EMPLACE_APPEND(
            pivots, key_buffer_init_from_key, context->hid, curr_key);
         if (!SUCCESS(rc)) {
            goto cleanup;
         }
      }

      iterator_next(&merger.merge_itor->super);
   }

   rc = VECTOR_EMPLACE_APPEND(
      pivots, key_buffer_init_from_key, context->hid, max_key);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   platform_status deinit_rc;
cleanup:
   deinit_rc = branch_merger_deinit(&merger);
   if (!SUCCESS(rc)) {
      for (uint64 i = 0; i < vector_length(pivots); i++) {
         key_buffer_deinit(vector_get_ptr(pivots, i));
      }
      return rc;
   }
   return deinit_rc;
}

static inline platform_status
in_memory_leaf_split_init(in_memory_node     *new_leaf,
                          trunk_node_context *context,
                          in_memory_node     *leaf,
                          key                 min_key,
                          key                 max_key)
{
   platform_status rc;
   platform_assert(in_memory_node_is_leaf(leaf));

   in_memory_pivot *pivot = in_memory_node_pivot(leaf, 0);

   rc =
      in_memory_node_init_empty_leaf(new_leaf, context->hid, min_key, max_key);
   if (!SUCCESS(rc)) {
      return rc;
   }

   return in_memory_node_receive_bundles(
      context,
      new_leaf,
      in_memory_node_pivot_bundle(leaf, 0),
      &leaf->inflight_bundles,
      in_memory_pivot_inflight_bundle_start(pivot),
      0);
}

static platform_status
in_memory_leaf_split(trunk_node_context    *context,
                     in_memory_node        *leaf,
                     in_memory_node_vector *new_leaves)
{
   platform_status rc;
   uint64          target_num_leaves;

   rc = leaf_split_target_num_leaves(context, leaf, &target_num_leaves);
   if (!SUCCESS(rc)) {
      return rc;
   }

   key_buffer_vector pivots;
   vector_init(&pivots, context->hid);
   rc = leaf_split_select_pivots(context, leaf, target_num_leaves, &pivots);
   if (!SUCCESS(rc)) {
      goto cleanup_pivots;
   }

   for (uint64 i = 0; i < vector_length(&pivots) - 1; i++) {
      key min_key = key_buffer_key(vector_get_ptr(&pivots, i));
      key max_key = key_buffer_key(vector_get_ptr(&pivots, i + 1));
      rc          = VECTOR_EMPLACE_APPEND(new_leaves,
                                 in_memory_leaf_split_init,
                                 context,
                                 leaf,
                                 min_key,
                                 max_key);
      if (!SUCCESS(rc)) {
         goto cleanup_new_leaves;
      }
   }

cleanup_new_leaves:
   if (!SUCCESS(rc)) {
      for (uint64 i = 0; i < vector_length(new_leaves); i++) {
         in_memory_node_deinit(vector_get_ptr(new_leaves, i), context);
      }
      vector_truncate(new_leaves, 0);
   }

cleanup_pivots:
   VECTOR_APPLY_TO_PTRS(&pivots, key_buffer_deinit);
   vector_deinit(&pivots);
   return rc;
}

/*********************************
 * index splits
 *********************************/

static platform_status
in_memory_index_init_split(in_memory_node  *new_index,
                           platform_heap_id hid,
                           in_memory_node  *index,
                           uint64           start_child_num,
                           uint64           end_child_num)
{
   platform_status rc;

   in_memory_pivot_vector pivots;
   vector_init(&pivots, hid);
   rc = vector_ensure_capacity(&pivots, end_child_num - start_child_num + 1);
   if (!SUCCESS(rc)) {
      goto cleanup_pivots;
   }
   for (uint64 i = start_child_num; i < end_child_num + 1; i++) {
      in_memory_pivot *pivot = vector_get(&index->pivots, i);
      in_memory_pivot *copy  = in_memory_pivot_copy(hid, pivot);
      if (copy == NULL) {
         rc = STATUS_NO_MEMORY;
         goto cleanup_pivots;
      }
      rc = vector_append(&pivots, copy);
      platform_assert_status_ok(rc);
   }

   in_memory_routed_bundle_vector pivot_bundles;
   vector_init(&pivot_bundles, hid);
   rc = vector_ensure_capacity(&pivot_bundles, end_child_num - start_child_num);
   if (!SUCCESS(rc)) {
      goto cleanup_pivot_bundles;
   }
   for (uint64 i = start_child_num; i < end_child_num; i++) {
      rc = VECTOR_EMPLACE_APPEND(&pivot_bundles,
                                 in_memory_routed_bundle_init_copy,
                                 hid,
                                 vector_get_ptr(&index->pivot_bundles, i));
      if (!SUCCESS(rc)) {
         goto cleanup_pivot_bundles;
      }
   }

   in_memory_routed_bundle_vector inflight_bundles;
   vector_init(&inflight_bundles, hid);
   if (!SUCCESS(rc)) {
      goto cleanup_inflight_bundles;
   }
   rc = VECTOR_EMPLACE_MAP_PTRS(&inflight_bundles,
                                in_memory_routed_bundle_init_copy,
                                &index->inflight_bundles,
                                hid);
   if (!SUCCESS(rc)) {
      goto cleanup_inflight_bundles;
   }

   in_memory_node_init(new_index,
                       in_memory_node_height(index),
                       pivots,
                       pivot_bundles,
                       in_memory_node_num_old_bundles(index),
                       inflight_bundles);

   return rc;

cleanup_inflight_bundles:
   VECTOR_APPLY_TO_PTRS(&inflight_bundles, in_memory_routed_bundle_deinit);
   vector_deinit(&inflight_bundles);
cleanup_pivot_bundles:
   VECTOR_APPLY_TO_PTRS(&pivot_bundles, in_memory_routed_bundle_deinit);
   vector_deinit(&pivot_bundles);
cleanup_pivots:
   VECTOR_APPLY_TO_ELTS(&pivots, in_memory_pivot_destroy, hid);
   vector_deinit(&pivots);
   return rc;
}

static platform_status
in_memory_index_split(trunk_node_context    *context,
                      in_memory_node        *index,
                      in_memory_node_vector *new_indexes)
{
   debug_assert(
      in_memory_node_is_well_formed_index(context->cfg->data_cfg, index));
   platform_status rc;
   rc = vector_append(new_indexes, *index);
   if (!SUCCESS(rc)) {
      goto cleanup_new_indexes;
   }

   uint64 num_children = in_memory_node_num_children(index);
   uint64 num_nodes    = (num_children + context->cfg->target_fanout - 1)
                      / context->cfg->target_fanout;

   for (uint64 i = 0; i < num_nodes; i++) {
      rc = VECTOR_EMPLACE_APPEND(new_indexes,
                                 in_memory_index_init_split,
                                 context->hid,
                                 index,
                                 i * num_children / num_nodes,
                                 (i + 1) * num_children / num_nodes);
      if (!SUCCESS(rc)) {
         goto cleanup_new_indexes;
      }
   }

cleanup_new_indexes:
   if (!SUCCESS(rc)) {
      // We skip entry 0 because it's the original index
      for (uint64 i = 1; i < vector_length(new_indexes); i++) {
         in_memory_node_deinit(vector_get_ptr(new_indexes, i), context);
      }
      vector_truncate(new_indexes, 0);
   }

   return rc;
}

/***********************************
 * flushing
 ***********************************/

static inline platform_status
restore_balance_leaf(trunk_node_context    *context,
                     in_memory_node        *leaf,
                     in_memory_node_vector *new_leaves)
{
   platform_status rc = in_memory_leaf_split(context, leaf, new_leaves);

   if (SUCCESS(rc)) {
      pivot_state_map_lock lock;
      pivot_state_map_aquire_lock(&lock,
                                  context,
                                  &context->pivot_states,
                                  in_memory_node_pivot_min_key(leaf),
                                  in_memory_node_height(leaf));
      pivot_compaction_state *pivot_state =
         pivot_state_map_get(context,
                             &context->pivot_states,
                             &lock,
                             in_memory_node_pivot_min_key(leaf),
                             in_memory_node_height(leaf));
      if (pivot_state) {
         pivot_state_map_remove(&context->pivot_states, &lock, pivot_state);
      }
      pivot_state_map_release_lock(&lock, &context->pivot_states);
   }

   return rc;
}

static platform_status
flush_then_compact(trunk_node_context             *context,
                   in_memory_node                 *node,
                   in_memory_routed_bundle        *routed,
                   in_memory_routed_bundle_vector *inflight,
                   uint64                          inflight_start,
                   uint64                          child_num,
                   in_memory_node_vector          *new_nodes);

static platform_status
restore_balance_index(trunk_node_context    *context,
                      in_memory_node        *index,
                      in_memory_node_vector *new_indexes)
{
   platform_status rc;

   debug_assert(
      in_memory_node_is_well_formed_index(context->cfg->data_cfg, index));

   for (uint64 i = 0; i < in_memory_node_num_children(index); i++) {
      in_memory_pivot *pivot = in_memory_node_pivot(index, i);
      if (context->cfg->per_child_flush_threshold_kv_bytes
          < in_memory_pivot_num_kv_bytes(pivot))
      {
         in_memory_routed_bundle *pivot_bundle =
            in_memory_node_pivot_bundle(index, i);

         in_memory_pivot_vector new_pivots;

         { // scope for new_children
            in_memory_node_vector new_children;

            { // scope for child
               // Load the node we are flushing to.
               in_memory_node child;
               rc = in_memory_node_deserialize(
                  context, in_memory_pivot_child_addr(pivot), &child);
               if (!SUCCESS(rc)) {
                  return rc;
               }

               vector_init(&new_children, context->hid);
               rc = flush_then_compact(
                  context,
                  &child,
                  pivot_bundle,
                  &index->inflight_bundles,
                  in_memory_pivot_inflight_bundle_start(pivot),
                  i,
                  &new_children);
               if (!SUCCESS(rc)) {
                  in_memory_node_deinit(&child, context);
                  vector_deinit(&new_children);
                  return rc;
               }

               in_memory_node_deinit(&child, context);
            }

            vector_init(&new_pivots, context->hid);
            rc = serialize_nodes_and_enqueue_bundle_compactions(
               context, &new_children, &new_pivots);
            if (!SUCCESS(rc)) {
               vector_deinit(&new_children);
               vector_deinit(&new_pivots);
               return rc;
            }

            // The children in new_children were stolen by the enqueued
            // compaction tasks, so the vector is now empty.
            vector_deinit(&new_children);
         }

         {
            pivot_state_map_lock lock;
            pivot_state_map_aquire_lock(&lock,
                                        context,
                                        &context->pivot_states,
                                        in_memory_pivot_key(pivot),
                                        in_memory_node_height(index));
            pivot_compaction_state *pivot_state =
               pivot_state_map_get(context,
                                   &context->pivot_states,
                                   &lock,
                                   in_memory_pivot_key(pivot),
                                   in_memory_node_height(index));
            if (pivot_state) {
               pivot_state_map_remove(
                  &context->pivot_states, &lock, pivot_state);
            }
            pivot_state_map_release_lock(&lock, &context->pivot_states);
         }

         for (uint64 j = 0; j < vector_length(&new_pivots); j++) {
            in_memory_pivot *new_pivot = vector_get(&new_pivots, j);
            in_memory_pivot_set_inflight_bundle_start(
               new_pivot, vector_length(&index->inflight_bundles));
         }
         rc = vector_replace(
            &index->pivots, i, 1, &new_pivots, 0, vector_length(&new_pivots));
         if (!SUCCESS(rc)) {
            VECTOR_APPLY_TO_ELTS(
               &new_pivots, in_memory_pivot_destroy, context->hid);
            vector_deinit(&new_pivots);
            return rc;
         }
         in_memory_pivot_destroy(pivot, context->hid);
         vector_deinit(&new_pivots);

         in_memory_routed_bundle_reset(pivot_bundle);
      }
   }

   return in_memory_index_split(context, index, new_indexes);
}

/*
 * Flush the routed bundle and inflight bundles inflight[inflight_start...]
 * to the given node.
 *
 * num_tuples and num_kv_bytes are the stats for the incoming bundles (i.e.
 * when flushing from a parent node, they are the per-pivot stat information,
 * when performing a memtable incorporation, they are the stats for the
 * incoming memtable).
 *
 * child_num is the child number of the node addr within its parent.
 *
 * flush_then_compact may choose to split the node.  The resulting
 * node/nodes are returned in new_nodes.
 */
static platform_status
flush_then_compact(trunk_node_context             *context,
                   in_memory_node                 *node,
                   in_memory_routed_bundle        *routed,
                   in_memory_routed_bundle_vector *inflight,
                   uint64                          inflight_start,
                   uint64                          child_num,
                   in_memory_node_vector          *new_nodes)
{
   platform_status rc;

   // Add the bundles to the node
   rc = in_memory_node_receive_bundles(
      context, node, routed, inflight, inflight_start, child_num);
   if (!SUCCESS(rc)) {
      return rc;
   }

   // Perform any needed recursive flushes and node splits
   if (in_memory_node_is_leaf(node)) {
      rc = restore_balance_leaf(context, node, new_nodes);
   } else {
      rc = restore_balance_index(context, node, new_nodes);
   }

   return rc;
}

static platform_status
build_new_roots(trunk_node_context *context, in_memory_node_vector *nodes)
{
   platform_status rc;

   debug_assert(1 < vector_length(nodes));

   // Remember the height now, since we will lose ownership of the children
   // when we enqueue compactions on them.
   uint64 height = in_memory_node_height(vector_get_ptr(nodes, 0));

   // Serialize the children and enqueue their compactions. This will give us
   // back the pivots for the new root node.
   in_memory_pivot_vector pivots;
   vector_init(&pivots, context->hid);
   rc = serialize_nodes_and_enqueue_bundle_compactions(context, nodes, &pivots);
   if (!SUCCESS(rc)) {
      goto cleanup_pivots;
   }
   // The nodes in the nodes vector were stolen by the enqueued compaction
   // tasks, so we can just truncate the vector.
   vector_truncate(nodes, 0);

   // Build a new vector of empty pivot bundles.
   in_memory_routed_bundle_vector pivot_bundles;
   vector_init(&pivot_bundles, context->hid);
   rc = vector_ensure_capacity(&pivot_bundles, vector_length(&pivots));
   if (!SUCCESS(rc)) {
      goto cleanup_pivot_bundles;
   }
   for (uint64 i = 0; i < vector_length(&pivots); i++) {
      rc = VECTOR_EMPLACE_APPEND(
         &pivot_bundles, in_memory_routed_bundle_init, context->hid);
      platform_assert_status_ok(rc);
   }

   // Build a new empty inflight bundle vector
   in_memory_routed_bundle_vector inflight;
   vector_init(&inflight, context->hid);

   // Build the new root
   in_memory_node new_root;
   in_memory_node_init(
      &new_root, height + 1, pivots, pivot_bundles, 0, inflight);

   // At this point, all our resources that we've allocated have been put
   // into the new root.

   rc = in_memory_index_split(context, &new_root, nodes);
   if (!SUCCESS(rc)) {
      in_memory_node_deinit(&new_root, context);
   }

   return rc;

cleanup_pivot_bundles:
   vector_deinit(&pivot_bundles);

cleanup_pivots:
   VECTOR_APPLY_TO_ELTS(&pivots, in_memory_pivot_destroy, context->hid);
   vector_deinit(&pivots);
   return rc;
}


platform_status
incorporate(trunk_node_context *context,
            routing_filter      filter,
            branch_ref          branch,
            uint64             *new_root_addr)
{
   platform_status rc;

   in_memory_routed_bundle_vector inflight;
   vector_init(&inflight, context->hid);

   in_memory_node_vector new_nodes;
   vector_init(&new_nodes, context->hid);

   // Read the old root.
   in_memory_node root;
   rc = in_memory_node_deserialize(context, context->root_addr, &root);
   if (!SUCCESS(rc)) {
      goto cleanup_vectors;
   }

   // Construct a vector of inflight bundles with one singleton bundle for
   // the new branch.
   rc = VECTOR_EMPLACE_APPEND(&inflight,
                              in_memory_routed_bundle_init_single,
                              context->hid,
                              filter,
                              branch);
   if (!SUCCESS(rc)) {
      goto cleanup_root;
   }

   // "flush" the new bundle to the root, then do any rebalancing needed.
   rc = flush_then_compact(context, &root, NULL, &inflight, 0, 0, &new_nodes);
   in_memory_node_deinit(&root, context);
   if (!SUCCESS(rc)) {
      goto cleanup_vectors;
   }

   // Build new roots, possibly splitting them, until we get down to a single
   // root with fanout that is within spec.
   while (1 < vector_length(&new_nodes)) {
      rc = build_new_roots(context, &new_nodes);
      if (!SUCCESS(rc)) {
         goto cleanup_vectors;
      }
   }

   in_memory_pivot *new_root_pivot =
      in_memory_node_serialize(context, vector_get_ptr(&new_nodes, 0));
   if (new_root_pivot == NULL) {
      rc = STATUS_NO_MEMORY;
      goto cleanup_vectors;
   }

   *new_root_addr = in_memory_pivot_child_addr(new_root_pivot);
   in_memory_pivot_destroy(new_root_pivot, context->hid);

   return STATUS_OK;

cleanup_root:
   in_memory_node_deinit(&root, context);

cleanup_vectors:
   VECTOR_APPLY_TO_PTRS(&new_nodes, in_memory_node_deinit, context);
   vector_deinit(&new_nodes);
   VECTOR_APPLY_TO_PTRS(&inflight, in_memory_routed_bundle_deinit);
   vector_deinit(&inflight);

   return rc;
}