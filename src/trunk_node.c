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

typedef enum inflight_bundle_type {
   INFLIGHT_BUNDLE_TYPE_ROUTED,
   INFLIGHT_BUNDLE_TYPE_PER_CHILD,
   INFLIGHT_BUNDLE_TYPE_SINGLETON
} inflight_bundle_type;

#if 0 // To be moved later in file
typedef struct ONDISK inflight_bundle {
   inflight_bundle_type type;
   union {
      routed_bundle    routed;
      per_child_bundle per_child;
      singleton_bundle singleton;
   } u;
} inflight_bundle;
#endif

typedef struct ONDISK pivot {
   uint64     num_kv_bytes;
   uint64     num_tuples;
   uint64     child_addr;
   uint64     inflight_bundle_start;
   ondisk_key key;
} pivot;

typedef VECTOR(routing_filter) routing_filter_vector;
typedef VECTOR(branch_ref) branch_ref_vector;

typedef struct in_memory_routed_bundle {
   routing_filter    maplet;
   branch_ref_vector branches;
} in_memory_routed_bundle;

typedef struct in_memory_per_child_bundle {
   routing_filter_vector maplets;
   branch_ref_vector     branches;
} in_memory_per_child_bundle;

typedef struct in_memory_singleton_bundle {
   routing_filter_vector maplets;
   branch_ref            branch;
} in_memory_singleton_bundle;

typedef struct in_memory_inflight_bundle {
   inflight_bundle_type type;
   union {
      in_memory_routed_bundle    routed;
      in_memory_per_child_bundle per_child;
      in_memory_singleton_bundle singleton;
   } u;
} in_memory_inflight_bundle;

typedef pivot in_memory_pivot;

typedef VECTOR(in_memory_pivot *) in_memory_pivot_vector;
typedef VECTOR(in_memory_routed_bundle) in_memory_routed_bundle_vector;
typedef VECTOR(in_memory_inflight_bundle) in_memory_inflight_bundle_vector;

typedef struct in_memory_node {
   platform_heap_id                 hid;
   uint16                           height;
   uint64                           num_kv_bytes;
   uint64                           num_tuples;
   in_memory_pivot_vector           pivots;
   in_memory_routed_bundle_vector   pivot_bundles; // indexed by child
   in_memory_inflight_bundle_vector inflight_bundles;
} in_memory_node;

/***************************************************
 * branch_ref operations
 ***************************************************/

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

/**************************
 * routed_bundle operations
 **************************/

void
in_memory_routed_bundle_init(in_memory_routed_bundle *bundle,
                             platform_heap_id         hid)
{
   bundle->maplet = NULL_ROUTING_FILTER;
   vector_init(&bundle->branches, hid);
}

platform_status
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

void
in_memory_routed_bundle_deinit(in_memory_routed_bundle *bundle)
{
   vector_deinit(&bundle->branches);
}

void
in_memory_routed_bundle_reset(in_memory_routed_bundle *bundle)
{
   vector_truncate(&bundle->branches, 0);
   bundle->maplet = NULL_ROUTING_FILTER;
}

platform_status
in_memory_routed_bundle_add_branch(in_memory_routed_bundle *bundle,
                                   routing_filter           new_maplet,
                                   branch_ref               new_branch)
{
   platform_status rc;
   rc = vector_append(&bundle->branches, new_branch);
   if (!SUCCESS(rc)) {
      return rc;
   }
   bundle->maplet = new_maplet;

   return STATUS_OK;
}

routing_filter
in_memory_routed_bundle_maplet(const in_memory_routed_bundle *bundle)
{
   return bundle->maplet;
}

uint64
in_memory_routed_bundle_num_branches(const in_memory_routed_bundle *bundle)
{
   return vector_length(&bundle->branches);
}

const branch_ref_vector *
in_memory_routed_bundle_branch_vector(const in_memory_routed_bundle *bundle)
{
   return &bundle->branches;
}

branch_ref
in_memory_routed_bundle_branch(const in_memory_routed_bundle *bundle, uint64 i)
{
   debug_assert(i < vector_length(&bundle->branches));
   return vector_get(&bundle->branches, i);
}

/*****************************
 * per_child_bundle operations
 *****************************/

/* Note that init moves maplets and branches into the bundle */
void
in_memory_per_child_bundle_init(in_memory_per_child_bundle *bundle,
                                routing_filter_vector      *maplets,
                                branch_ref_vector          *branches)
{
   bundle->maplets  = *maplets;
   bundle->branches = *branches;
}

platform_status
in_memory_per_child_bundle_init_from_split(
   in_memory_per_child_bundle       *bundle,
   platform_heap_id                  hid,
   const in_memory_per_child_bundle *src,
   uint64                            branches_start,
   uint64                            branches_end)
{
   vector_init(&bundle->maplets, hid);
   platform_status rc = vector_copy(&bundle->maplets, &src->maplets);
   if (!SUCCESS(rc)) {
      vector_deinit(&bundle->maplets);
      return rc;
   }

   vector_init(&bundle->branches, hid);
   for (uint64 i = branches_start; i < branches_end; i++) {
      rc = vector_append(&bundle->branches, vector_get(&src->branches, i));
      if (!SUCCESS(rc)) {
         vector_deinit(&bundle->maplets);
         vector_deinit(&bundle->branches);
         return rc;
      }
   }

   return STATUS_OK;
}

void
in_memory_per_child_bundle_deinit(in_memory_per_child_bundle *bundle)
{
   vector_deinit(&bundle->maplets);
   vector_deinit(&bundle->branches);
}

void
in_memory_per_child_bundle_truncate(in_memory_per_child_bundle *bundle,
                                    uint64 new_num_children)
{
   vector_truncate(&bundle->branches, new_num_children);
}

uint64
in_memory_per_child_bundle_num_branches(
   const in_memory_per_child_bundle *bundle)
{
   return vector_length(&bundle->branches);
}

branch_ref
in_memory_per_child_bundle_branch(const in_memory_per_child_bundle *bundle,
                                  uint64                            i)
{
   return vector_get(&bundle->branches, i);
}

uint64
in_memory_per_child_bundle_num_maplets(const in_memory_per_child_bundle *bundle)
{
   return vector_length(&bundle->maplets);
}

routing_filter
in_memory_per_child_bundle_maplet(const in_memory_per_child_bundle *bundle,
                                  uint64                            i)
{
   debug_assert(i < vector_length(&bundle->maplets));
   return vector_get(&bundle->maplets, i);
}

/*****************************
 * singleton_bundle operations
 *****************************/

platform_status
in_memory_singleton_bundle_init(in_memory_singleton_bundle *bundle,
                                platform_heap_id            hid,
                                routing_filter              maplet,
                                branch_ref                  branch)
{
   vector_init(&bundle->maplets, hid);
   platform_status rc = vector_append(&bundle->maplets, maplet);
   if (!SUCCESS(rc)) {
      vector_deinit(&bundle->maplets);
      return rc;
   }
   bundle->branch = branch;
   return STATUS_OK;
}

platform_status
in_memory_singleton_bundle_init_copy(in_memory_singleton_bundle       *dst,
                                     platform_heap_id                  hid,
                                     const in_memory_singleton_bundle *src)
{
   vector_init(&dst->maplets, hid);
   platform_status rc = vector_copy(&dst->maplets, &src->maplets);
   if (!SUCCESS(rc)) {
      vector_deinit(&dst->maplets);
      return rc;
   }
   dst->branch = src->branch;
   return STATUS_OK;
}

platform_status
in_memory_singleton_bundle_init_from_per_child(
   in_memory_singleton_bundle       *bundle,
   platform_heap_id                  hid,
   const in_memory_per_child_bundle *src,
   uint64                            child_num)
{
   vector_init(&bundle->maplets, hid);
   platform_status rc = vector_copy(&bundle->maplets, &src->maplets);
   if (!SUCCESS(rc)) {
      vector_deinit(&bundle->maplets);
      return rc;
   }
   bundle->branch = in_memory_per_child_bundle_branch(src, child_num);
   return STATUS_OK;
}

void
in_memory_singleton_bundle_deinit(in_memory_singleton_bundle *bundle)
{
   vector_deinit(&bundle->maplets);
}

uint64
in_memory_singleton_bundle_num_maplets(const in_memory_singleton_bundle *bundle)
{
   return vector_length(&bundle->maplets);
}

routing_filter
in_memory_singleton_bundle_maplet(const in_memory_singleton_bundle *bundle,
                                  uint64                            i)
{
   debug_assert(i < in_memory_singleton_bundle_num_maplets(bundle));
   return vector_get(&bundle->maplets, i);
}

branch_ref
in_memory_singleton_bundle_branch(const in_memory_singleton_bundle *bundle)
{
   return bundle->branch;
}

/****************************
 * inflight_bundle operations
 ****************************/

platform_status
in_memory_inflight_bundle_init_from_routed(
   in_memory_inflight_bundle     *bundle,
   platform_heap_id               hid,
   const in_memory_routed_bundle *routed)
{
   bundle->type = INFLIGHT_BUNDLE_TYPE_ROUTED;
   return in_memory_routed_bundle_init_copy(&bundle->u.routed, hid, routed);
}

platform_status
in_memory_inflight_bundle_init_singleton(in_memory_inflight_bundle *bundle,
                                         platform_heap_id           hid,
                                         routing_filter             maplet,
                                         branch_ref                 branch)
{
   bundle->type = INFLIGHT_BUNDLE_TYPE_SINGLETON;
   return in_memory_singleton_bundle_init(
      &bundle->u.singleton, hid, maplet, branch);
}

platform_status
in_memory_inflight_bundle_init_from_singleton(
   in_memory_inflight_bundle        *bundle,
   platform_heap_id                  hid,
   const in_memory_singleton_bundle *src)
{
   bundle->type = INFLIGHT_BUNDLE_TYPE_SINGLETON;
   return in_memory_singleton_bundle_init_copy(&bundle->u.singleton, hid, src);
}

platform_status
in_memory_inflight_bundle_init_singleton_from_per_child(
   in_memory_inflight_bundle        *bundle,
   platform_heap_id                  hid,
   const in_memory_per_child_bundle *src,
   uint64                            child_num)
{
   bundle->type = INFLIGHT_BUNDLE_TYPE_SINGLETON;
   return in_memory_singleton_bundle_init_from_per_child(
      &bundle->u.singleton, hid, src, child_num);
}

void
in_memory_inflight_bundle_init_per_child(in_memory_inflight_bundle *bundle,
                                         platform_heap_id           hid,
                                         routing_filter_vector     *maplets,
                                         branch_ref_vector         *branches)
{
   bundle->type = INFLIGHT_BUNDLE_TYPE_PER_CHILD;
   in_memory_per_child_bundle_init(&bundle->u.per_child, maplets, branches);
}

platform_status
in_memory_inflight_bundle_init_per_child_from_split(
   in_memory_inflight_bundle        *bundle,
   platform_heap_id                  hid,
   const in_memory_per_child_bundle *src,
   uint64                            branches_start,
   uint64                            branches_end)
{
   bundle->type = INFLIGHT_BUNDLE_TYPE_PER_CHILD;
   return in_memory_per_child_bundle_init_from_split(
      &bundle->u.per_child, hid, src, branches_start, branches_end);
}

platform_status
in_memory_inflight_bundle_vector_collect_maplets(
   const in_memory_inflight_bundle_vector *bundles,
   uint64                                  bundle_start,
   uint64                                  bundle_end,
   routing_filter_vector                  *maplets)
{
   platform_status rc;

   for (uint64 i = bundle_start; i < bundle_end; i++) {
      const in_memory_inflight_bundle *bundle = vector_get_ptr(bundles, i);
      switch (bundle->type) {
         case INFLIGHT_BUNDLE_TYPE_ROUTED:
         {
            rc = vector_append(
               maplets, in_memory_routed_bundle_maplet(&bundle->u.routed));
            if (!SUCCESS(rc)) {
               return rc;
            }
            break;
         }
         case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
         {
            uint64 nbmaplets =
               in_memory_per_child_bundle_num_maplets(&bundle->u.per_child);
            for (uint64 j = 0; j < nbmaplets; j++) {
               rc = vector_append(
                  maplets,
                  in_memory_per_child_bundle_maplet(&bundle->u.per_child, j));
               if (!SUCCESS(rc)) {
                  return rc;
               }
            }
            break;
         }
         case INFLIGHT_BUNDLE_TYPE_SINGLETON:
         {
            uint64 nbmaplets =
               in_memory_singleton_bundle_num_maplets(&bundle->u.singleton);
            for (uint64 j = 0; j < nbmaplets; j++) {
               rc = vector_append(
                  maplets,
                  in_memory_singleton_bundle_maplet(&bundle->u.singleton, j));
               if (!SUCCESS(rc)) {
                  return rc;
               }
            }
            break;
         }
         default:
            platform_assert(0);
      }
   }

   return STATUS_OK;
}

/* Note: steals branches vector. */
platform_status
in_memory_inflight_bundle_init_per_child_from_compaction(
   in_memory_inflight_bundle              *bundle,
   platform_heap_id                        hid,
   const in_memory_inflight_bundle_vector *bundles,
   uint64                                  bundle_start,
   uint64                                  bundle_end,
   branch_ref_vector                      *branches)
{
   platform_status       rc;
   routing_filter_vector maplets;
   vector_init(&maplets, hid);

   rc = in_memory_inflight_bundle_vector_collect_maplets(
      bundles, bundle_start, bundle_end, &maplets);
   if (!SUCCESS(rc)) {
      vector_deinit(&maplets);
      return rc;
   }

   in_memory_inflight_bundle_init_per_child(bundle, hid, &maplets, branches);
   return STATUS_OK;
}

void
in_memory_inflight_bundle_deinit(in_memory_inflight_bundle *bundle)
{
   switch (bundle->type) {
      case INFLIGHT_BUNDLE_TYPE_ROUTED:
         in_memory_routed_bundle_deinit(&bundle->u.routed);
         break;
      case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
         in_memory_per_child_bundle_deinit(&bundle->u.per_child);
         break;
      case INFLIGHT_BUNDLE_TYPE_SINGLETON:
         in_memory_singleton_bundle_deinit(&bundle->u.singleton);
         break;
      default:
         platform_assert(0);
         break;
   }
}

inflight_bundle_type
in_memory_inflight_bundle_type(const in_memory_inflight_bundle *bundle)
{
   return bundle->type;
}

/******************
 * pivot operations
 ******************/

in_memory_pivot *
in_memory_pivot_create(platform_heap_id hid, key k)
{
   in_memory_pivot *result = TYPED_FLEXIBLE_STRUCT_ZALLOC(
      hid, result, key.bytes, ondisk_key_required_data_capacity(k));
   if (result == NULL) {
      return NULL;
   }
   copy_key_to_ondisk_key(&result->key, k);
   return result;
}

void
in_memory_pivot_destroy(in_memory_pivot *pivot, platform_heap_id hid)
{
   platform_free(hid, pivot);
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
in_memory_pivot_inflight_bundle_start(const in_memory_pivot *pivot)
{
   return pivot->inflight_bundle_start;
}

/* You must inform the pivot of the tuple counts from the bundle */
void
in_memory_pivot_increment_inflight_bundle_start(in_memory_pivot *pivot,
                                                uint64           num_tuples,
                                                uint64           num_kv_bytes)
{
   platform_assert(num_tuples <= pivot->num_tuples
                   && num_kv_bytes <= pivot->num_kv_bytes);
   pivot->num_tuples -= num_tuples;
   pivot->num_kv_bytes -= num_kv_bytes;
   pivot->inflight_bundle_start++;
}

/*
 * When a new bundle gets flushed to this pivot's node, you must
 * inform the pivot of the tuple counts of the new bundle.
 */
void
in_memory_pivot_add_bundle_tuple_count(in_memory_pivot *pivot,
                                       uint64           num_tuples,
                                       uint64           num_kv_bytes)
{
   pivot->num_tuples += num_tuples;
   pivot->num_kv_bytes += num_kv_bytes;
}

/***********************
 * basic node operations
 ***********************/

uint64
in_memory_node_num_pivots(const in_memory_node *node)
{
   return vector_length(&node->pivots) - 1;
}

uint64
in_memory_node_num_children(const in_memory_node *node)
{
   return vector_length(&node->pivots) - 1;
}

pivot *
in_memory_node_pivot(const in_memory_node *node, uint64 i)
{
   return vector_get(&node->pivots, i);
}

key
in_memory_node_pivot_key(const in_memory_node *node, uint64 i)
{
   return in_memory_pivot_key(vector_get(&node->pivots, i));
}

key
in_memory_node_pivot_min_key(const in_memory_node *node)
{
   return in_memory_pivot_key(vector_get(&node->pivots, 0));
}

key
in_memory_node_pivot_max_key(const in_memory_node *node)
{
   return in_memory_pivot_key(
      vector_get(&node->pivots, vector_length(&node->pivots) - 1));
}

in_memory_routed_bundle *
in_memory_node_pivot_bundle(in_memory_node *node, uint64 i)
{
   return vector_get_ptr(&node->pivot_bundles, i);
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

bool
in_memory_node_is_well_formed_leaf(const data_config    *data_cfg,
                                   const in_memory_node *node)
{
   bool basics = node->height == 0 && vector_length(&node->pivots) == 2
                 && vector_length(&node->pivot_bundles) == 1;
   if (!basics) {
      return FALSE;
   }

   pivot *lb    = vector_get(&node->pivots, 0);
   pivot *ub    = vector_get(&node->pivots, 1);
   key    lbkey = in_memory_pivot_key(lb);
   key    ubkey = in_memory_pivot_key(ub);
   return lb->child_addr == 0 && lb->inflight_bundle_start == 0
          && data_key_compare(data_cfg, lbkey, ubkey) < 0;
}

bool
in_memory_node_is_well_formed_index(const data_config    *data_cfg,
                                    const in_memory_node *node)
{
   bool basics = 0 < node->height && 1 < vector_length(&node->pivots)
                 && vector_length(&node->pivot_bundles)
                       == vector_length(&node->pivots) - 1;
   if (!basics) {
      return FALSE;
   }

   for (uint64 i = 0; i < in_memory_node_num_children(node); i++) {
      pivot *lb    = vector_get(&node->pivots, i);
      pivot *ub    = vector_get(&node->pivots, i + 1);
      key    lbkey = in_memory_pivot_key(lb);
      key    ubkey = in_memory_pivot_key(ub);
      bool   valid_pivots =
         lb->child_addr != 0
         && lb->inflight_bundle_start <= vector_length(&node->inflight_bundles)
         && data_key_compare(data_cfg, lbkey, ubkey) < 0;
      if (!valid_pivots) {
         return FALSE;
      }
   }

   for (uint64 i = 0; i < vector_length(&node->inflight_bundles); i++) {
      const in_memory_inflight_bundle *bundle =
         vector_get_ptr(&node->inflight_bundles, i);
      switch (in_memory_inflight_bundle_type(bundle)) {
         case INFLIGHT_BUNDLE_TYPE_ROUTED:
            break;
         case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
            if (vector_length(&bundle->u.per_child.branches)
                != in_memory_node_num_children(node))
            {
               return FALSE;
            }
            break;
         case INFLIGHT_BUNDLE_TYPE_SINGLETON:
            break;
         default:
            return FALSE;
      }
   }

   return TRUE;
}

void
in_memory_node_add_tuple_count(in_memory_node *node,
                               int64           num_tuples,
                               int64           num_kv_bytes)
{
   node->num_tuples += num_tuples;
   node->num_kv_bytes += num_kv_bytes;
}

#if 0
void
in_memory_node_deinit(in_memory_node *node)
{
   vector_apply(&node->pivots, vector_apply_platform_free, node->hid);
   vector_apply_ptr(&node->pivot_bundles, in_memory_routed_bundle_deinit);
   vector_apply_ptr(&node->inflight_bundles, in_memory_inflight_bundle_deinit);
   vector_deinit(&node->pivots);
   vector_deinit(&node->pivot_bundles);
   vector_deinit(&node->inflight_bundles);
}

/************************
 * accounting maintenance
 ************************/

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

#   if 0
/*
 * flushing: bundles
 */
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

/*********************************************
 * branch_merger operations
 * (used in both leaf splits and compactions)
 *********************************************/

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
                       merger->min_key,
                       greater_than_or_equal,
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
                       merger->min_key,
                       greater_than_or_equal,
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

/************************
 * flushing: leaf splits
 ************************/

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
   while (!iterator_can_next(merger.merge_itor) && leaf_num < target_num_leaves)
   {
      key     curr_key;
      message pivot_data_message;
      iterator_curr(merger.merge_itor, &curr_key, &pivot_data_message);
      const btree_pivot_data *pivot_data = message_data(pivot_data_message);
      uint64                  new_cumulative_kv_bytes = cumulative_kv_bytes
                                       + pivot_data->stats.key_bytes
                                       + pivot_data->stats.message_bytes;
      uint64 next_boundary = leaf_num * leaf->num_kv_bytes / target_num_leaves;
      if (cumulative_kv_bytes < next_boundary
          && next_boundary <= new_cumulative_kv_bytes)
      {
         rc = vector_emplace(pivots, key_buffer_init_from_key, hid, curr_key);
         if (!SUCCESS(rc)) {
            goto cleanup;
         }
      }

      iterator_next(merger.merge_itor);
   }

   rc = vector_emplace(pivots, key_buffer_init_from_key, hid, max_key);
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

platform_status
in_memory_node_init(in_memory_node  *new_node,
                    platform_heap_id hid,
                    uint64           height,
                    key              min_key)
{
   platform_status rc;
   ZERO_CONTENTS(new_node);
   new_node->hid    = hid;
   new_node->height = height;
   vector_init(&new_node->pivots, hid);
   vector_init(&new_node->pivot_bundles, hid);
   vector_init(&new_node->inflight_bundles, hid);

   pivot *lb = pivot_create(hid, min_key);
   if (lb == NULL) {
      rc = STATUS_NO_MEMORY;
      goto deinits;
   }
   rc = vector_append(&new_node->pivots, lb);
   if (!SUCCESS(rc)) {
      goto free_lb;
   }

   return STATUS_OK;

free_lb:
   platform_free(hid, lb);
deinits:
   vector_deinit(&new_node->pivots);
   vector_deinit(&new_node->pivot_bundles);
   vector_deinit(&new_node->inflight_bundles);
   return rc;
}

platform_status
in_memory_leaf_split_init(in_memory_node  *new_leaf,
                          platform_heap_id hid,
                          cache           *cc,
                          btree_config    *btree_cfg,
                          in_memory_node  *leaf,
                          key              min_key,
                          key              max_key)
{
   platform_assert(in_memory_node_is_leaf(leaf));

   platform_status rc = in_memory_node_init(new_leaf, hid, 0, min_key);
   if (!SUCCESS(rc)) {
      return rc;
   }

   in_memory_routed_bundle *pbundle = vector_get(&leaf->pivot_bundles, 0);
   rc = in_memory_node_receive_routed_bundle(cc, btree_cfg, new_leaf, pbundle);
   if (!SUCCESS(rc)) {
      return rc;
   }

   for (uint64 i = 0; i < vector_length(&leaf->inflight_bundles); i++) {
      in_memory_inflight_bundle *bundle =
         vector_get(&leaf->inflight_bundles, i);
      switch (in_memory_inflight_bundle_type(bundle)) {
         case INFLIGHT_BUNDLE_TYPE_ROUTED:
            rc = in_memory_node_receive_routed_bundle(
               cc, btree_cfg, new_leaf, &bundle->u.routed);
            if (!SUCCESS(rc)) {
               return rc;
            }
            break;
         case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
            rc = in_memory_node_receive_per_child_bundle(
               cc, btree_cfg, new_leaf, &bundle->u.per_child, 0);
            if (!SUCCESS(rc)) {
               return rc;
            }
            break;
         case INFLIGHT_BUNDLE_TYPE_SINGLETON:
            rc = in_memory_node_receive_singleton_bundle(
               cc, btree_cfg, new_leaf, &bundle->u.singleton);
            if (!SUCCESS(rc)) {
               return rc;
            }
            break;
         default:
            platform_assert(0);
      }
   }

   return rc;
}

VECTOR_DEFINE(in_memory_node_vector, in_memory_node)

platform_status
in_memory_leaf_split(platform_heap_id       hid,
                     cache                 *cc,
                     data_config           *data_cfg,
                     btree_config          *btree_cfg,
                     routing_config        *filter_cfg,
                     uint64                 target_leaf_kv_bytes,
                     in_memory_node        *leaf,
                     in_memory_node_vector *new_leaves)
{
   platform_status rc;
   uint64          target_num_leaves;

   rc = leaf_split_target_num_leaves(
      cc, filter_cfg, hid, target_leaf_kv_bytes, leaf, &target_num_leaves);
   if (!SUCCESS(rc)) {
      return rc;
   }

   key_buffer_vector pivots;
   vector_init(&pivots, hid);

   rc = leaf_split_select_pivots(
      cc, data_cfg, btree_cfg, hid, leaf, target_num_leaves, &pivots);
   if (!SUCCESS(rc)) {
      goto pivots_deinit;
   }

   for (uint64 i = 0; i < vector_length(&pivots) - 1; i++) {
      key min_key = key_buffer_key(vector_get_ptr(&pivots, i));
      key max_key = key_buffer_key(vector_get_ptr(&pivots, i + 1));
      rc          = vector_emplace(new_leaves,
                          in_memory_leaf_split_init,
                          hid,
                          cc,
                          btree_cfg,
                          leaf,
                          min_key,
                          max_key);
      if (!SUCCESS(rc)) {
         goto empty_new_leaves;
      }
   }

empty_new_leaves:
   if (!SUCCESS(rc)) {
      vector_apply_ptr(new_leaves, in_memory_node_deinit);
      vector_truncate(new_leaves, 0);
   }

pivots_deinit:
   vector_deinit(&pivots);
   return rc;
}

/*********************************
 * flushing: index splits
 *********************************/

platform_status
in_memory_build_index_split_node(in_memory_node  *new_index,
                                 platform_heap_id hid,
                                 cache           *cc,
                                 btree_config    *btree_cfg,
                                 in_memory_node  *index,
                                 uint64           start_child_num,
                                 uint64           end_child_num)
{
   platform_assert(in_memory_node_is_leaf(leaf));

   platform_status rc = in_memory_node_init(new_leaf, hid, 0, min_key, max_key);
   if (!SUCCESS(rc)) {
      return rc;
   }


   return STATUS_OK;
}
#   endif
#endif
