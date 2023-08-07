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
#include "poison.h"

typedef struct ONDISK branch_ref {
   uint64 addr;
} branch_ref;

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

typedef enum inflight_bundle_type {
   INFLIGHT_BUNDLE_TYPE_ROUTED,
   INFLIGHT_BUNDLE_TYPE_PER_CHILD,
   INFLIGHT_BUNDLE_TYPE_SINGLETON
} inflight_bundle_type;

typedef struct ONDISK inflight_bundle {
   inflight_bundle_type type;
   union {
      routed_bundle    routed;
      per_child_bundle per_child;
      singleton_bundle singleton;
   } u;
} inflight_bundle;

typedef struct ONDISK pivot {
   uint64     num_kv_bytes;
   uint64     num_tuples;
   uint64     child_addr;
   uint64     inflight_bundle_start;
   ondisk_key key;
} pivot;


typedef routed_bundle    in_memory_routed_bundle;
typedef per_child_bundle in_memory_per_child_bundle;
typedef singleton_bundle in_memory_singleton_bundle;
typedef inflight_bundle  in_memory_inflight_bundle;
typedef pivot            in_memory_pivot;

VECTOR_DEFINE(in_memory_pivot_vector, pivot *)
VECTOR_DEFINE(in_memory_routed_bundle_vector, in_memory_routed_bundle *)
VECTOR_DEFINE(in_memory_inflight_bundle_vector, in_memory_inflight_bundle *)

typedef struct in_memory_node {
   platform_heap_id                 hid;
   uint16                           height;
   uint64                           num_kv_bytes;
   uint64                           num_tuples;
   uint64                           num_pivots;
   in_memory_pivot_vector           pivots;
   in_memory_routed_bundle_vector   pivot_bundles; // indexed by child
   in_memory_inflight_bundle_vector inflight_bundles;
} in_memory_node;

branch_ref
create_branch_ref(uint64 addr)
{
   return (branch_ref){.addr = addr};
}

uint64
branch_ref_addr(branch_ref bref)
{
   return bref.addr;
}

key
in_memory_pivot_key(const in_memory_pivot *pivot)
{
   return ondisk_key_to_key(&pivot->key);
}

uint64
in_memory_pivot_num_tuples(const in_memory_pivot *pivot)
{
   return pivot->num_tuples;
}

uint64
in_memory_node_num_children(const in_memory_node *node)
{
   return node->num_pivots - 1;
}

uint64
in_memory_node_height(const in_memory_node *node)
{
   return node->height;
}

bool32
in_memory_node_is_leaf(const in_memory_node *node)
{
   return node->height == 0;
}

in_memory_routed_bundle *
in_memory_routed_bundle_create(platform_heap_id hid,
                               routing_filter   maplet,
                               uint64           num_branches,
                               branch_ref      *branches)
{
   in_memory_routed_bundle *result =
      TYPED_FLEXIBLE_STRUCT_ZALLOC(hid, result, branches, num_branches);
   if (result != NULL) {
      result->maplet       = maplet;
      result->num_branches = num_branches;
      memcpy(result->branches,
             branches,
             num_branches * sizeof(result->branches[0]));
   }
   return result;
}

in_memory_routed_bundle *
in_memory_routed_bundle_add_branch(platform_heap_id               hid,
                                   const in_memory_routed_bundle *bundle,
                                   routing_filter                 new_maplet,
                                   branch_ref                     new_branch)
{
   in_memory_routed_bundle *result = TYPED_FLEXIBLE_STRUCT_ZALLOC(
      hid, result, branches, bundle->num_branches + 1);
   if (result != NULL) {
      result->maplet       = new_maplet;
      result->num_branches = bundle->num_branches + 1;
      memcpy(result->branches,
             bundle->branches,
             result->num_branches * sizeof(result->branches[0]));
      result->branches[bundle->num_branches] = new_branch;
   }
   return result;
}

void
in_memory_routed_bundle_reset(in_memory_routed_bundle *bundle)
{
   bundle->num_branches = 0;
   bundle->maplet       = NULL_ROUTING_FILTER;
}

void
in_memory_routed_bundle_destroy(platform_heap_id         hid,
                                in_memory_routed_bundle *bundle)
{
   platform_free(hid, bundle);
}

routing_filter
in_memory_routed_bundle_maplet(const in_memory_routed_bundle *bundle)
{
   return bundle->maplet;
}

uint64
in_memory_routed_bundle_num_branches(const in_memory_routed_bundle *bundle)
{
   return bundle->num_branches;
}

const branch_ref *
in_memory_routed_bundle_branch_array(const in_memory_routed_bundle *bundle)
{
   return bundle->branches;
}

branch_ref
in_memory_routed_bundle_branch(const in_memory_routed_bundle *bundle, uint64 i)
{
   debug_assert(i < bundle->num_branches);
   return bundle->branches[i];
}

branch_ref *
in_memory_per_child_bundle_branch_array(in_memory_per_child_bundle *bundle)
{
   return (branch_ref *)(&bundle->maplets[bundle->num_maplets]);
}

void
in_memory_per_child_bundle_destroy(platform_heap_id            hid,
                                   in_memory_per_child_bundle *bundle)
{
   platform_free(hid, bundle);
}

uint64
in_memory_per_child_bundle_num_maplets(const in_memory_per_child_bundle *bundle)
{
   return bundle->num_maplets;
}

routing_filter
in_memory_per_child_bundle_maplet(const in_memory_per_child_bundle *bundle,
                                  uint64                            i)
{
   debug_assert(i < bundle->num_maplets);
   return bundle->maplets[i];
}

const routing_filter *
in_memory_per_child_bundle_maplet_array(
   const in_memory_per_child_bundle *bundle)
{
   return bundle->maplets;
}

branch_ref
in_memory_per_child_bundle_branch(in_memory_per_child_bundle *bundle, uint64 i)
{
   const branch_ref *branch_array =
      in_memory_per_child_bundle_branch_array(bundle);
   return branch_array[i];
}

void
in_memory_singleton_bundle_destroy(platform_heap_id            hid,
                                   in_memory_singleton_bundle *bundle)
{
   platform_free(hid, bundle);
}

uint64
in_memory_singleton_bundle_num_maplets(const in_memory_singleton_bundle *bundle)
{
   return bundle->num_maplets;
}

routing_filter
in_memory_singleton_bundle_maplet(const in_memory_singleton_bundle *bundle,
                                  uint64                            i)
{
   debug_assert(i < bundle->num_maplets);
   return bundle->maplets[i];
}

const routing_filter *
in_memory_singleton_bundle_maplet_array(
   const in_memory_singleton_bundle *bundle)
{
   return bundle->maplets;
}

branch_ref
in_memory_singleton_bundle_branch(const in_memory_singleton_bundle *bundle)
{
   return bundle->branch;
}

in_memory_inflight_bundle *
in_memory_inflight_bundle_create_routed(platform_heap_id               hid,
                                        const in_memory_routed_bundle *bundle)
{
   in_memory_inflight_bundle *result = TYPED_FLEXIBLE_STRUCT_ZALLOC(
      hid, result, u.routed.branches, bundle->num_branches);
   if (result != NULL) {
      result->type                  = INFLIGHT_BUNDLE_TYPE_ROUTED;
      result->u.routed.maplet       = bundle->maplet;
      result->u.routed.num_branches = bundle->num_branches;
      memcpy(result->u.routed.branches,
             bundle->branches,
             bundle->num_branches * sizeof(result->u.routed.branches[0]));
   }
   return result;
}

inflight_bundle_type
in_memory_inflight_bundle_type(const in_memory_inflight_bundle *bundle)
{
   return bundle->type;
}

uint64
in_memory_inflight_bundle_num_maplets(const in_memory_inflight_bundle *bundle)
{
   switch (in_memory_inflight_bundle_type(bundle)) {
      case INFLIGHT_BUNDLE_TYPE_ROUTED:
         return 1;
         break;
      case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
         return in_memory_per_child_bundle_num_maplets(&bundle->u.per_child);
         break;
      case INFLIGHT_BUNDLE_TYPE_SINGLETON:
         return in_memory_singleton_bundle_num_maplets(&bundle->u.singleton);
         break;
      default:
         platform_assert(0);
   }
}

uint64
in_memory_inflight_bundle_num_branches(in_memory_node                  *node,
                                       const in_memory_inflight_bundle *bundle)
{
   switch (in_memory_inflight_bundle_type(bundle)) {
      case INFLIGHT_BUNDLE_TYPE_ROUTED:
         return bundle->u.routed.num_branches;
         break;
      case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
         return in_memory_node_num_children(node);
         break;
      case INFLIGHT_BUNDLE_TYPE_SINGLETON:
         return 1;
         break;
      default:
         platform_assert(0);
   }
}

uint64
in_memory_inflight_bundles_count_maplets(
   const in_memory_inflight_bundle_vector *bundles)
{
   uint64 num_maplets = 0;
   uint64 num_bundles = vector_length(bundles);
   for (int i = 0; i < num_bundles; i++) {
      const in_memory_inflight_bundle *bundle = vector_get(bundles, i);
      num_maplets += in_memory_inflight_bundle_num_maplets(bundle);
   }

   return num_maplets;
}

void
in_memory_inflight_bundle_collect_maplets(
   const in_memory_inflight_bundle_vector *bundles,
   uint64                                  maplets_capacity,
   routing_filter                         *maplets)
{
   uint64 num_maplets = 0;
   uint64 num_bundles = vector_length(bundles);
   for (uint64 i = 0; i < num_bundles; i++) {
      const in_memory_inflight_bundle *bundle = vector_get(bundles, i);
      switch (in_memory_inflight_bundle_type(bundle)) {
         case INFLIGHT_BUNDLE_TYPE_ROUTED:
         {
            platform_assert(num_maplets < maplets_capacity);
            maplets[num_maplets++] =
               in_memory_routed_bundle_maplet(&bundle->u.routed);
            break;
         }
         case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
         {
            uint64 nbmaplets =
               in_memory_per_child_bundle_num_maplets(&bundle->u.per_child);
            platform_assert(num_maplets + nbmaplets <= maplets_capacity);
            const routing_filter *bmaplets =
               in_memory_per_child_bundle_maplet_array(&bundle->u.per_child);
            memcpy(&maplets[num_maplets],
                   bmaplets,
                   nbmaplets * sizeof(routing_filter));
            num_maplets += nbmaplets;
            break;
         }
         case INFLIGHT_BUNDLE_TYPE_SINGLETON:
         {
            uint64 nbmaplets =
               in_memory_singleton_bundle_num_maplets(&bundle->u.singleton);
            platform_assert(num_maplets + nbmaplets <= maplets_capacity);
            const routing_filter *bmaplets =
               in_memory_singleton_bundle_maplet_array(&bundle->u.singleton);
            memcpy(&maplets[num_maplets],
                   bmaplets,
                   nbmaplets * sizeof(routing_filter));
            num_maplets += nbmaplets;
            break;
         }
         default:
            platform_assert(0);
      }
   }
}

in_memory_inflight_bundle *
in_memory_inflight_bundle_create_per_child(
   platform_heap_id                        hid,
   const in_memory_inflight_bundle_vector *bundles,
   uint64                                  num_branches,
   branch_ref                             *branches)
{
   uint64 num_maplets = in_memory_inflight_bundles_count_maplets(bundles);

   in_memory_inflight_bundle *result = platform_aligned_zalloc(
      hid,
      PLATFORM_CACHELINE_SIZE,
      sizeof(in_memory_inflight_bundle) + num_maplets * sizeof(routing_filter)
         + num_branches * sizeof(branch_ref));

   if (result != NULL) {
      result->type                      = INFLIGHT_BUNDLE_TYPE_PER_CHILD;
      result->u.per_child.num_maplets   = num_maplets;
      routing_filter *new_maplets_array = result->u.per_child.maplets;
      in_memory_inflight_bundle_collect_maplets(
         bundles, num_maplets, new_maplets_array);
      branch_ref *new_branch_array =
         in_memory_per_child_bundle_branch_array(&result->u.per_child);
      memcpy(new_branch_array, branches, num_branches * sizeof(branch_ref));
   }
   return result;
}

in_memory_inflight_bundle *
in_memory_inflight_bundle_create_singleton(platform_heap_id            hid,
                                           in_memory_per_child_bundle *bundle,
                                           uint64 child_num)
{
   in_memory_inflight_bundle *result = TYPED_FLEXIBLE_STRUCT_ZALLOC(
      hid, result, u.singleton.maplets, bundle->num_maplets);

   if (result != NULL) {
      result->type = INFLIGHT_BUNDLE_TYPE_SINGLETON;
      result->u.singleton.branch =
         in_memory_per_child_bundle_branch(bundle, child_num);
      result->u.singleton.num_maplets = bundle->num_maplets;
      memcpy(result->u.singleton.maplets,
             bundle->maplets,
             bundle->num_maplets * sizeof(result->u.singleton.maplets[0]));
   }

   return result;
}


in_memory_inflight_bundle *
in_memory_inflight_bundle_copy_singleton(
   platform_heap_id                  hid,
   const in_memory_singleton_bundle *bundle)
{
   in_memory_inflight_bundle *result = TYPED_FLEXIBLE_STRUCT_ZALLOC(
      hid, result, u.singleton.maplets, bundle->num_maplets);

   if (result != NULL) {
      result->type                    = INFLIGHT_BUNDLE_TYPE_SINGLETON;
      result->u.singleton.branch      = bundle->branch;
      result->u.singleton.num_maplets = bundle->num_maplets;
      memcpy(result->u.singleton.maplets,
             bundle->maplets,
             bundle->num_maplets * sizeof(result->u.singleton.maplets[0]));
   }

   return result;
}

typedef enum branch_tuple_count_operation {
   BRANCH_TUPLE_COUNT_ADD,
   BRANCH_TUPLE_COUNT_SUB,
} branch_tuple_count_operation;

platform_status
add_branch_tuple_counts_for_child(cache                       *cc,
                                  const btree_config          *cfg,
                                  in_memory_node              *node,
                                  branch_ref                   bref,
                                  branch_tuple_count_operation operation,
                                  uint64                       child_num)
{
   int coefficient;
   switch (operation) {
      case BRANCH_TUPLE_COUNT_ADD:
         coefficient = 1;
         break;
      case BRANCH_TUPLE_COUNT_SUB:
         coefficient = -1;
         break;
      default:
         platform_assert(0);
         break;
   }

   in_memory_pivot  *lbpivot = vector_get(&node->pivots, child_num);
   in_memory_pivot  *ubpivot = vector_get(&node->pivots, child_num + 1);
   key               lb      = in_memory_pivot_key(lbpivot);
   key               ub      = in_memory_pivot_key(ubpivot);
   btree_pivot_stats stats;
   btree_count_in_range(cc, cfg, branch_ref_addr(bref), lb, ub, &stats);
   int64 num_kv_bytes = stats.key_bytes + stats.message_bytes;
   int64 num_kvs      = stats.num_kvs;
   node->num_kv_bytes += coefficient * num_kv_bytes;
   node->num_tuples += coefficient * num_kvs;
   lbpivot->num_kv_bytes += coefficient * num_kv_bytes;
   lbpivot->num_tuples += coefficient * num_kvs;

   return STATUS_OK;
}

platform_status
add_branches_tuple_counts_for_child(cache                       *cc,
                                    const btree_config          *cfg,
                                    in_memory_node              *node,
                                    uint64                       num_branches,
                                    const branch_ref            *brefs,
                                    branch_tuple_count_operation operation,
                                    uint64                       child_num)
{
   platform_status rc = STATUS_OK;
   for (uint64 branch_num = 0; branch_num < num_branches; branch_num++) {
      rc = add_branch_tuple_counts_for_child(
         cc, cfg, node, brefs[branch_num], operation, child_num);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   return rc;
}

platform_status
add_branches_tuple_counts(cache                       *cc,
                          const btree_config          *cfg,
                          in_memory_node              *node,
                          uint64                       num_branches,
                          const branch_ref            *brefs,
                          branch_tuple_count_operation operation)
{
   platform_status rc = STATUS_OK;
   for (uint64 child_num = 0; child_num < in_memory_node_num_children(node);
        child_num++)
   {
      rc = add_branches_tuple_counts_for_child(
         cc, cfg, node, num_branches, brefs, operation, child_num);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   return rc;
}

platform_status
in_memory_node_receive_routed_bundle(cache                         *cc,
                                     const btree_config            *cfg,
                                     in_memory_node                *node,
                                     const in_memory_routed_bundle *routed)
{
   in_memory_inflight_bundle *inflight =
      in_memory_inflight_bundle_create_routed(node->hid, routed);
   if (inflight == NULL) {
      return STATUS_NO_MEMORY;
   }

   platform_status rc = vector_append(&node->inflight_bundles, inflight);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64 num_branches        = in_memory_routed_bundle_num_branches(routed);
   const branch_ref *branches = in_memory_routed_bundle_branch_array(routed);
   rc                         = add_branches_tuple_counts(
      cc, cfg, node, num_branches, branches, BRANCH_TUPLE_COUNT_ADD);

   return rc;
}

platform_status
in_memory_node_receive_per_child_bundle(cache                      *cc,
                                        const btree_config         *cfg,
                                        in_memory_node             *node,
                                        in_memory_per_child_bundle *per_child,
                                        uint64                      child_num)
{
   in_memory_inflight_bundle *inflight =
      in_memory_inflight_bundle_create_singleton(
         node->hid, per_child, child_num);
   if (inflight == NULL) {
      return STATUS_NO_MEMORY;
   }

   platform_status rc = vector_append(&node->inflight_bundles, inflight);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64            num_branches = 1;
   const branch_ref *branches     = &inflight->u.singleton.branch;
   rc                             = add_branches_tuple_counts(
      cc, cfg, node, num_branches, branches, BRANCH_TUPLE_COUNT_ADD);

   return rc;
}

platform_status
in_memory_node_receive_singleton_bundle(cache                      *cc,
                                        const btree_config         *cfg,
                                        in_memory_node             *node,
                                        in_memory_singleton_bundle *singleton)
{
   in_memory_inflight_bundle *inflight =
      in_memory_inflight_bundle_copy_singleton(node->hid, singleton);
   if (inflight == NULL) {
      return STATUS_NO_MEMORY;
   }

   platform_status rc = vector_append(&node->inflight_bundles, inflight);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64            num_branches = 1;
   const branch_ref *branches     = &inflight->u.singleton.branch;
   rc                             = add_branches_tuple_counts(
      cc, cfg, node, num_branches, branches, BRANCH_TUPLE_COUNT_ADD);

   return rc;
}

routed_bundle *
in_memory_node_extract_pivot_bundle(cache              *cc,
                                    const btree_config *cfg,
                                    in_memory_node     *node,
                                    uint64              child_num)
{
   debug_assert(child_num < in_memory_node_num_children(node));
   routed_bundle *result       = vector_get(&node->pivot_bundles, child_num);
   uint64         num_branches = in_memory_routed_bundle_num_branches(result);
   const branch_ref *branches  = in_memory_routed_bundle_branch_array(result);
   platform_status   rc        = add_branches_tuple_counts_for_child(
      cc, cfg, node, num_branches, branches, BRANCH_TUPLE_COUNT_SUB, child_num);
   if (SUCCESS(rc)) {
      in_memory_routed_bundle_reset(result);
   } else {
      result = NULL;
   }
   return result;
}

platform_status
perform_flush(cache              *cc,
              const btree_config *cfg,
              in_memory_node     *parent,
              in_memory_node     *child,
              uint64              child_num)
{
   in_memory_routed_bundle *pivot_bundle =
      in_memory_node_extract_pivot_bundle(cc, cfg, parent, child_num);
   if (pivot_bundle == NULL) {
      return STATUS_IO_ERROR;
   }
   platform_status rc =
      in_memory_node_receive_routed_bundle(cc, cfg, child, pivot_bundle);
   if (!SUCCESS(rc)) {
      return rc;
   }

   in_memory_pivot *pivot       = vector_get(&parent->pivots, child_num);
   uint64           num_bundles = vector_length(&parent->inflight_bundles);
   while (pivot->inflight_bundle_start < num_bundles) {
      in_memory_inflight_bundle *bundle =
         vector_get(&parent->inflight_bundles, pivot->inflight_bundle_start);
      switch (in_memory_inflight_bundle_type(bundle)) {
         case INFLIGHT_BUNDLE_TYPE_ROUTED:
            rc = in_memory_node_receive_routed_bundle(
               cc, cfg, child, &bundle->u.routed);
            if (!SUCCESS(rc)) {
               return rc;
            }
            uint64 num_branches =
               in_memory_routed_bundle_num_branches(&bundle->u.routed);
            const branch_ref *branches =
               in_memory_routed_bundle_branch_array(&bundle->u.routed);
            rc = add_branches_tuple_counts(
               cc, cfg, parent, num_branches, branches, BRANCH_TUPLE_COUNT_SUB);
            break;
         case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
            rc = in_memory_node_receive_per_child_bundle(
               cc, cfg, child, &bundle->u.per_child, child_num);
            for (uint64 child_num = 0;
                 child_num < in_memory_node_num_children(parent);
                 child_num++)
            {
               branch_ref branch = in_memory_per_child_bundle_branch(
                  &bundle->u.per_child, child_num);
               rc = add_branches_tuple_counts_for_child(cc,
                                                        cfg,
                                                        parent,
                                                        1,
                                                        &branch,
                                                        BRANCH_TUPLE_COUNT_SUB,
                                                        child_num);
            }
            break;
         case INFLIGHT_BUNDLE_TYPE_SINGLETON:
            rc = in_memory_node_receive_singleton_bundle(
               cc, cfg, child, &bundle->u.singleton);
            if (!SUCCESS(rc)) {
               return rc;
            }
            branch_ref branch =
               in_memory_singleton_bundle_branch(&bundle->u.singleton);
            rc = add_branches_tuple_counts(
               cc, cfg, parent, 1, &branch, BRANCH_TUPLE_COUNT_SUB);
            break;
         default:
            platform_assert(0);
            break;
      }
      if (!SUCCESS(rc)) {
         return rc;
      }
      pivot->inflight_bundle_start++;
   }

   return rc;
}

platform_status
in_memory_leaf_estimate_unique_keys(cache           *cc,
                                    routing_config  *filter_cfg,
                                    platform_heap_id heap_id,
                                    in_memory_node  *leaf,
                                    uint64          *estimate)
{
   platform_assert(in_memory_node_is_leaf(leaf));

   in_memory_routed_bundle *pivot_bundle = vector_get(&leaf->pivot_bundles, 0);

   uint64 num_inflight_maplets =
      in_memory_inflight_bundles_count_maplets(&leaf->inflight_bundles);

   uint64 num_maplets = num_inflight_maplets + 1;

   routing_filter *maplets =
      TYPED_ARRAY_MALLOC(leaf->hid, maplets, num_maplets);
   if (maplets == NULL) {
      return STATUS_NO_MEMORY;
   }

   maplets[0] = in_memory_routed_bundle_maplet(pivot_bundle);

   in_memory_inflight_bundle_collect_maplets(
      &leaf->inflight_bundles, num_inflight_maplets, &maplets[1]);

   uint64 num_sb_fp     = 0;
   uint64 num_sb_unique = 0;
   for (uint16 inflight_maplet_num = 1; inflight_maplet_num < num_maplets;
        inflight_maplet_num++)
   {
      num_sb_fp += maplets[inflight_maplet_num].num_fingerprints;
      num_sb_unique += maplets[inflight_maplet_num].num_unique;
   }

   uint32 num_unique = routing_filter_estimate_unique_fp(
      cc, filter_cfg, heap_id, maplets, num_maplets);

   num_unique =
      routing_filter_estimate_unique_keys_from_count(filter_cfg, num_unique);

   uint64 num_leaf_sb_fp         = leaf->num_tuples;
   uint64 est_num_leaf_sb_unique = num_sb_unique * num_leaf_sb_fp / num_sb_fp;
   uint64 est_num_non_leaf_sb_unique = num_sb_fp - est_num_leaf_sb_unique;

   uint64 est_leaf_unique = num_unique - est_num_non_leaf_sb_unique;
   *estimate              = est_leaf_unique;
   return STATUS_OK;
}

platform_status
leaf_split_target_num_leaves(cache           *cc,
                             routing_config  *filter_cfg,
                             platform_heap_id heap_id,
                             uint64           target_leaf_kv_bytes,
                             in_memory_node  *leaf,
                             uint64          *target)
{
   platform_assert(in_memory_node_is_leaf(leaf));

   uint64          estimated_unique_keys;
   platform_status rc = in_memory_leaf_estimate_unique_keys(
      cc, filter_cfg, heap_id, leaf, &estimated_unique_keys);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64 num_tuples = leaf->num_tuples;
   if (estimated_unique_keys > num_tuples * 19 / 20) {
      estimated_unique_keys = num_tuples;
   }
   uint64 kv_bytes = leaf->num_kv_bytes;
   uint64 estimated_unique_kv_bytes =
      estimated_unique_keys * kv_bytes / num_tuples;
   uint64 target_num_leaves =
      (estimated_unique_kv_bytes + target_leaf_kv_bytes / 2)
      / target_leaf_kv_bytes;
   if (target_num_leaves < 1) {
      target_num_leaves = 1;
   }

   *target = target_num_leaves;

   return STATUS_OK;
}

uint64
in_memory_node_count_inflight_branches(in_memory_node *node,
                                       uint64          start_bundle,
                                       uint64          end_bundle)
{
   uint64 num_branches = 0;

   for (uint64 bundle_num = start_bundle; bundle_num < end_bundle; bundle_num++)
   {
      in_memory_inflight_bundle *bundle =
         vector_get(&node->inflight_bundles, bundle_num);
      num_branches += in_memory_inflight_bundle_num_branches(node, bundle);
   }

   return num_branches;
}

VECTOR_DEFINE(iterator_vector, iterator *)
typedef struct branch_merger {
   platform_heap_id hid;
   data_config     *data_cfg;
   key              min_key;
   key              max_key;
   uint64           height;
   iterator        *merge_itor;
   iterator_vector  itors;
} branch_merger;

void
branch_merger_init(branch_merger   *merger,
                   platform_heap_id hid,
                   data_config     *data_cfg,
                   key              min_key,
                   key              max_key,
                   uint64           height)
{
   merger->hid        = hid;
   merger->data_cfg   = data_cfg;
   merger->min_key    = min_key;
   merger->max_key    = max_key;
   merger->height     = height;
   merger->merge_itor = NULL;
   vector_init(&merger->itors, hid);
}

platform_status
branch_merger_add_routed_bundle(branch_merger           *merger,
                                cache                   *cc,
                                btree_config            *btree_cfg,
                                in_memory_routed_bundle *routed)
{
   for (uint64 i = 0; i < routed->num_branches; i++) {
      btree_iterator *iter = TYPED_MALLOC(merger->hid, iter);
      if (iter == NULL) {
         return STATUS_NO_MEMORY;
      }
      btree_iterator_init(cc,
                          btree_cfg,
                          iter,
                          routed->branches[i].addr,
                          PAGE_TYPE_BRANCH,
                          merger->min_key,
                          merger->max_key,
                          TRUE,
                          merger->height);
      platform_status rc = vector_append(&merger->itors, (iterator *)iter);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   return STATUS_OK;
}

platform_status
branch_merger_add_per_child_bundle(branch_merger              *merger,
                                   cache                      *cc,
                                   btree_config               *btree_cfg,
                                   uint64                      child_num,
                                   in_memory_per_child_bundle *bundle)
{
   btree_iterator *iter = TYPED_MALLOC(merger->hid, iter);
   if (iter == NULL) {
      return STATUS_NO_MEMORY;
   }
   branch_ref *branches = in_memory_per_child_bundle_branch_array(bundle);
   btree_iterator_init(cc,
                       btree_cfg,
                       iter,
                       branches[child_num].addr,
                       PAGE_TYPE_BRANCH,
                       merger->min_key,
                       merger->max_key,
                       TRUE,
                       merger->height);
   return vector_append(&merger->itors, (iterator *)iter);
}

platform_status
branch_merger_add_singleton_bundle(branch_merger              *merger,
                                   cache                      *cc,
                                   btree_config               *btree_cfg,
                                   in_memory_singleton_bundle *bundle)
{
   btree_iterator *iter = TYPED_MALLOC(merger->hid, iter);
   if (iter == NULL) {
      return STATUS_NO_MEMORY;
   }
   btree_iterator_init(cc,
                       btree_cfg,
                       iter,
                       bundle->branch.addr,
                       PAGE_TYPE_BRANCH,
                       merger->min_key,
                       merger->max_key,
                       TRUE,
                       merger->height);
   return vector_append(&merger->itors, (iterator *)iter);
}

platform_status
branch_merger_add_inflight_bundle(branch_merger             *merger,
                                  cache                     *cc,
                                  btree_config              *btree_cfg,
                                  uint64                     child_num,
                                  in_memory_inflight_bundle *bundle)
{
   switch (in_memory_inflight_bundle_type(bundle)) {
      case INFLIGHT_BUNDLE_TYPE_ROUTED:
         return branch_merger_add_routed_bundle(
            merger, cc, btree_cfg, &bundle->u.routed);
      case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
         return branch_merger_add_per_child_bundle(
            merger, cc, btree_cfg, child_num, &bundle->u.per_child);
      case INFLIGHT_BUNDLE_TYPE_SINGLETON:
         return branch_merger_add_singleton_bundle(
            merger, cc, btree_cfg, &bundle->u.singleton);
      default:
         platform_assert(0);
         break;
   }
}

platform_status
branch_merger_build_merge_itor(branch_merger *merger, merge_behavior merge_mode)
{
   platform_assert(merger == NULL);

   return merge_iterator_create(merger->hid,
                                merger->data_cfg,
                                vector_length(&merger->itors),
                                vector_data(&merger->itors),
                                merge_mode,
                                (merge_iterator **)&merger->merge_itor);
}

platform_status
branch_merger_deinit(branch_merger *merger)
{
   platform_status rc;
   if (merger->merge_itor != NULL) {
      rc = merge_iterator_destroy(merger->hid,
                                  (merge_iterator **)&merger->merge_itor);
   }

   for (uint64 i = 0; i < vector_length(&merger->itors); i++) {
      btree_iterator *itor = (btree_iterator *)vector_get(&merger->itors, i);
      btree_iterator_deinit(itor);
      platform_free(merger->hid, itor);
   }
   vector_deinit(&merger->itors);

   return rc;
}

VECTOR_DEFINE(key_buffer_vector, key_buffer)

platform_status
leaf_split_select_pivots(cache             *cc,
                         data_config       *data_cfg,
                         btree_config      *btree_cfg,
                         platform_heap_id   hid,
                         in_memory_node    *leaf,
                         uint64             target_num_leaves,
                         key_buffer_vector *pivots)
{
   platform_status  rc;
   in_memory_pivot *first   = vector_get(&leaf->pivots, 0);
   in_memory_pivot *last    = vector_get(&leaf->pivots, 1);
   key              min_key = ondisk_key_to_key(&first->key);
   key              max_key = ondisk_key_to_key(&last->key);

   rc = vector_emplace(pivots, key_buffer_init_from_key, hid, min_key);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   branch_merger merger;
   branch_merger_init(&merger, hid, data_cfg, min_key, max_key, 1);

   rc = branch_merger_add_routed_bundle(
      &merger, cc, btree_cfg, vector_get(&leaf->pivot_bundles, 0));
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   for (uint64 bundle_num = 0;
        bundle_num < vector_length(&leaf->inflight_bundles);
        bundle_num++)
   {
      in_memory_inflight_bundle *bundle =
         vector_get(&leaf->inflight_bundles, bundle_num);
      rc = branch_merger_add_inflight_bundle(&merger, cc, btree_cfg, 0, bundle);
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
   while (!iterator_at_end(merger.merge_itor) && leaf_num < target_num_leaves) {
      key     curr_key;
      message pivot_data_message;
      iterator_get_curr(merger->merge_itor, &curr_key, &pivot_data_message);
      const btree_pivot_data *pivot_data = message_data(pivot_data_message);
      uint64                  new_cumulative_kv_bytes = cumulative_kv_bytes
                                       + pivot_data->stats.key_bytes
                                       + pivot_data->stats.message_bytes;
      uint64 next_boundary = leaf_num * leaf->num_kv_bytes / target_num_leaves;
      if (cumulative_kv_bytes < next_boundary
          && next_boundary <= new_cumulative_kv_bytes)
      {
         key_buffer kb;
         key_buffer_init_from_key(kb, hid, curr_key);
         rc = vector_append(pivots, kb);
         if (!SUCCESS(rc)) {
            goto cleanup;
         }
      }
   }

   rc = vector_emplace(pivots, key_buffer_init_from_key, hid, max_key);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

cleanup:
   platform_status deinit_rc = branch_merger_deinit(&merger);
   if (!SUCCESS(rc)) {
      return rc;
   }
   return deinit_rc;
}
