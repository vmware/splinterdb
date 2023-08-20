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
   uint16                           height;
   in_memory_pivot_vector           pivots;
   in_memory_routed_bundle_vector   pivot_bundles; // indexed by child
   uint64                           num_old_bundles;
   in_memory_inflight_bundle_vector inflight_bundles;
} in_memory_node;

typedef VECTOR(in_memory_node) in_memory_node_vector;

typedef struct trunk_node_config {
   const data_config    *data_cfg;
   const btree_config   *btree_cfg;
   const routing_config *filter_cfg;
   uint64                leaf_split_threshold_kv_bytes;
   uint64                target_leaf_kv_bytes;
   uint64                target_fanout;
   uint64                per_child_flush_threshold_kv_bytes;
} trunk_node_config;

typedef struct trunk_node_context {
   const trunk_node_config *cfg;
   platform_heap_id         hid;
   cache                   *cc;
   allocator               *al;
   task_system             *ts;
} trunk_node_context;

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
   rc = vector_append_subvector(
      &bundle->branches, &src->branches, branches_start, branches_end);
   if (!SUCCESS(rc)) {
      vector_deinit(&bundle->maplets);
      vector_deinit(&bundle->branches);
   }

   return rc;
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
in_memory_inflight_bundle_init_from_split(in_memory_inflight_bundle *bundle,
                                          platform_heap_id           hid,
                                          const in_memory_inflight_bundle *src,
                                          uint64 branches_start,
                                          uint64 branches_end)
{
   switch (src->type) {
      case INFLIGHT_BUNDLE_TYPE_ROUTED:
         return in_memory_inflight_bundle_init_from_routed(
            bundle, hid, &src->u.routed);
         break;
      case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
         return in_memory_inflight_bundle_init_per_child_from_split(
            bundle, hid, &src->u.per_child, branches_start, branches_end);
         break;
      case INFLIGHT_BUNDLE_TYPE_SINGLETON:
         return in_memory_inflight_bundle_init_from_singleton(
            bundle, hid, &src->u.singleton);
         break;
      default:
         platform_assert(0);
         break;
   }
}

void
in_memory_inflight_bundle_truncate(in_memory_inflight_bundle *bundle,
                                   uint64                     num_children)
{
   switch (bundle->type) {
      case INFLIGHT_BUNDLE_TYPE_ROUTED:
         break;
      case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
         vector_truncate(&bundle->u.per_child.branches, num_children);
         break;
      case INFLIGHT_BUNDLE_TYPE_SINGLETON:
         break;
      default:
         platform_assert(0);
         break;
   }
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
            rc = vector_append_vector(maplets, &bundle->u.per_child.maplets);
            if (!SUCCESS(rc)) {
               return rc;
            }
            break;
         }
         case INFLIGHT_BUNDLE_TYPE_SINGLETON:
         {
            rc = vector_append_vector(maplets, &bundle->u.singleton.maplets);
            if (!SUCCESS(rc)) {
               return rc;
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

platform_status
in_memory_inflight_bundle_vector_init_split(
   in_memory_inflight_bundle_vector *result,
   in_memory_inflight_bundle_vector *src,
   platform_heap_id                  hid,
   uint64                            start_child_num,
   uint64                            end_child_num)
{
   vector_init(result, hid);
   return VECTOR_EMPLACE_MAP_PTRS(result,
                                  in_memory_inflight_bundle_init_from_split,
                                  src,
                                  hid,
                                  start_child_num,
                                  end_child_num);
}

platform_status
in_memory_inflight_bundle_init_from_flush(in_memory_inflight_bundle *bundle,
                                          platform_heap_id           hid,
                                          const in_memory_inflight_bundle *src,
                                          uint64 child_num)
{
   switch (src->type) {
      case INFLIGHT_BUNDLE_TYPE_ROUTED:
         return in_memory_inflight_bundle_init_from_routed(
            bundle, hid, &src->u.routed);
         break;
      case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
         return in_memory_inflight_bundle_init_singleton_from_per_child(
            bundle, hid, &src->u.per_child, child_num);
         break;
      case INFLIGHT_BUNDLE_TYPE_SINGLETON:
         return in_memory_inflight_bundle_init_from_singleton(
            bundle, hid, &src->u.singleton);
         break;
      default:
         platform_assert(0);
         break;
   }
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

in_memory_pivot *
in_memory_pivot_copy(platform_heap_id hid, in_memory_pivot *src)
{
   key              k      = ondisk_key_to_key(&src->key);
   in_memory_pivot *result = in_memory_pivot_create(hid, k);
   if (result != NULL) {
      result->num_kv_bytes          = src->num_kv_bytes;
      result->num_tuples            = src->num_tuples;
      result->child_addr            = src->child_addr;
      result->inflight_bundle_start = src->inflight_bundle_start;
   }
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
in_memory_pivot_child_addr(const in_memory_pivot *pivot)
{
   return pivot->child_addr;
}

uint64
in_memory_pivot_num_tuples(const in_memory_pivot *pivot)
{
   return pivot->num_tuples;
}

uint64
in_memory_pivot_num_kv_bytes(const in_memory_pivot *pivot)
{
   return pivot->num_kv_bytes;
}

uint64
in_memory_pivot_inflight_bundle_start(const in_memory_pivot *pivot)
{
   return pivot->inflight_bundle_start;
}

void
in_memory_pivot_set_inflight_bundle_start(in_memory_pivot *pivot, uint64 start)
{
   pivot->inflight_bundle_start = start;
}

/*
 * When new bundles get flushed to this pivot's node, you must
 * inform the pivot of the tuple counts of the new bundles.
 */
void
in_memory_pivot_add_tuple_counts(in_memory_pivot *pivot,
                                 int              coefficient,
                                 uint64           num_tuples,
                                 uint64           num_kv_bytes)
{
   if (coefficient == 1) {
      pivot->num_tuples += num_tuples;
      pivot->num_kv_bytes += num_kv_bytes;
   } else if (coefficient == -1) {
      platform_assert(num_tuples <= pivot->num_tuples);
      platform_assert(num_kv_bytes <= pivot->num_kv_bytes);
      pivot->num_tuples -= num_tuples;
      pivot->num_kv_bytes -= num_kv_bytes;
   } else {
      platform_assert(0);
   }
}

void
in_memory_pivot_reset_tuple_counts(in_memory_pivot *pivot)
{
   pivot->num_tuples   = 0;
   pivot->num_kv_bytes = 0;
}

/***********************
 * basic node operations
 ***********************/

void
in_memory_node_init(in_memory_node                  *node,
                    uint16                           height,
                    in_memory_pivot_vector           pivots,
                    in_memory_routed_bundle_vector   pivot_bundles,
                    uint64                           num_old_bundles,
                    in_memory_inflight_bundle_vector inflight_bundles)
{
   node->height           = height;
   node->pivots           = pivots;
   node->pivot_bundles    = pivot_bundles;
   node->num_old_bundles  = num_old_bundles;
   node->inflight_bundles = inflight_bundles;
}


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

uint64
in_memory_leaf_num_tuples(const in_memory_node *node)
{
   return in_memory_pivot_num_tuples(vector_get(&node->pivots, 0));
}

uint64
in_memory_leaf_num_kv_bytes(const in_memory_node *node)
{
   return in_memory_pivot_num_kv_bytes(vector_get(&node->pivots, 0));
}

bool
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

   pivot *lb    = vector_get(&node->pivots, 0);
   pivot *ub    = vector_get(&node->pivots, 1);
   key    lbkey = in_memory_pivot_key(lb);
   key    ubkey = in_memory_pivot_key(ub);
   return lb->child_addr == 0 && lb->inflight_bundle_start == 0
          && data_key_compare(cfg->data_cfg, lbkey, ubkey) < 0;
}

bool
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
in_memory_node_reset_num_old_bundles(in_memory_node *node)
{
   node->num_old_bundles = 0;
}

void
in_memory_node_deinit(in_memory_node *node, trunk_node_context *context)
{
   VECTOR_APPLY_TO_ELTS(
      &node->pivots, vector_apply_platform_free, context->hid);
   VECTOR_APPLY_TO_PTRS(&node->pivot_bundles, in_memory_routed_bundle_deinit);
   VECTOR_APPLY_TO_PTRS(&node->inflight_bundles,
                        in_memory_inflight_bundle_deinit);
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

platform_status
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

typedef VECTOR(iterator *) iterator_vector;

typedef struct branch_merger {
   platform_heap_id   hid;
   const data_config *data_cfg;
   key                min_key;
   key                max_key;
   uint64             height;
   iterator          *merge_itor;
   iterator_vector    itors;
} branch_merger;

void
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

platform_status
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

platform_status
branch_merger_add_per_child_bundle(branch_merger              *merger,
                                   cache                      *cc,
                                   const btree_config         *btree_cfg,
                                   uint64                      child_num,
                                   in_memory_per_child_bundle *bundle)
{
   btree_iterator *iter = TYPED_MALLOC(merger->hid, iter);
   if (iter == NULL) {
      return STATUS_NO_MEMORY;
   }
   branch_ref bref = in_memory_per_child_bundle_branch(bundle, child_num);
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
   return vector_append(&merger->itors, (iterator *)iter);
}

platform_status
branch_merger_add_singleton_bundle(branch_merger              *merger,
                                   cache                      *cc,
                                   const btree_config         *btree_cfg,
                                   in_memory_singleton_bundle *bundle)
{
   btree_iterator *iter = TYPED_MALLOC(merger->hid, iter);
   if (iter == NULL) {
      return STATUS_NO_MEMORY;
   }
   branch_ref bref = in_memory_singleton_bundle_branch(bundle);
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
   return vector_append(&merger->itors, (iterator *)iter);
}

platform_status
branch_merger_add_inflight_bundle(branch_merger             *merger,
                                  cache                     *cc,
                                  const btree_config        *btree_cfg,
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
 * bundle compaction
 ************************/

void
bundle_compaction_task(void *arg, void *scratch);

typedef struct bundle_compaction_args {
   trunk_node_context *context;
   uint64              addr;
   in_memory_node     *node;
} bundle_compaction_args;

platform_status
enqueue_bundle_compaction(trunk_node_context *context,
                          uint64              addr,
                          in_memory_node     *node)
{
   bundle_compaction_args *args = TYPED_ZALLOC(context->hid, args);
   if (args == NULL) {
      return STATUS_NO_MEMORY;
   }
   args->context = context;
   args->addr    = addr;
   args->node    = node;

   on_disk_node_inc_ref(context, addr);

   platform_status rc = task_enqueue(
      context->ts, TASK_TYPE_NORMAL, bundle_compaction_task, args, FALSE);
   if (!SUCCESS(rc)) {
      platform_free(context->hid, args);
   }

   return rc;
}

platform_status
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

platform_status
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

platform_status
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

platform_status
accumulate_branches_tuple_counts_in_range(const branch_ref_vector *brefs,
                                          trunk_node_context      *context,
                                          key                      minkey,
                                          key                      maxkey,
                                          btree_pivot_stats       *acc)
{
   return VECTOR_FAILABLE_FOR_LOOP_ELTS(brefs,
                                        accumulate_branch_tuple_counts_in_range,
                                        context,
                                        minkey,
                                        maxkey,
                                        acc);
}

platform_status
accumulate_routed_bundle_tuple_counts_in_range(in_memory_routed_bundle *bundle,
                                               trunk_node_context      *context,
                                               key                      minkey,
                                               key                      maxkey,
                                               btree_pivot_stats       *acc)
{
   return accumulate_branches_tuple_counts_in_range(
      &bundle->branches, context, minkey, maxkey, acc);
}

platform_status
accumulate_inflight_bundle_tuple_counts_in_range(
   in_memory_inflight_bundle *bundle,
   trunk_node_context        *context,
   in_memory_pivot_vector    *pivots,
   uint64                     child_num,
   btree_pivot_stats         *acc)
{
   key minkey = in_memory_pivot_key(vector_get(pivots, child_num));
   key maxkey = in_memory_pivot_key(vector_get(pivots, child_num + 1));

   switch (in_memory_inflight_bundle_type(bundle)) {
      case INFLIGHT_BUNDLE_TYPE_ROUTED:
         return accumulate_branches_tuple_counts_in_range(
            &bundle->u.routed.branches, context, minkey, maxkey, acc);
         break;
      case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
         return accumulate_branch_tuple_counts_in_range(
            in_memory_per_child_bundle_branch(&bundle->u.per_child, child_num),
            context,
            minkey,
            maxkey,
            acc);
         break;
      case INFLIGHT_BUNDLE_TYPE_SINGLETON:
         return accumulate_branch_tuple_counts_in_range(
            in_memory_singleton_bundle_branch(&bundle->u.singleton),
            context,
            minkey,
            maxkey,
            acc);
         break;
      default:
         platform_assert(0);
         break;
   }
}

platform_status
accumulate_inflight_bundles_tuple_counts_in_range(
   in_memory_inflight_bundle_vector *bundles,
   trunk_node_context               *context,
   in_memory_pivot_vector           *pivots,
   uint64                            child_num,
   btree_pivot_stats                *acc)
{
   return VECTOR_FAILABLE_FOR_LOOP_PTRS(
      bundles,
      accumulate_inflight_bundle_tuple_counts_in_range,
      context,
      pivots,
      child_num,
      acc);
}

platform_status
accumulate_bundles_tuple_counts_in_range(
   in_memory_routed_bundle          *routed,
   in_memory_inflight_bundle_vector *inflight,
   trunk_node_context               *context,
   in_memory_pivot_vector           *pivots,
   uint64                            child_num,
   btree_pivot_stats                *acc)
{
   platform_status rc;
   key             min_key = in_memory_pivot_key(vector_get(pivots, child_num));
   key max_key = in_memory_pivot_key(vector_get(pivots, child_num + 1));
   rc          = accumulate_routed_bundle_tuple_counts_in_range(
      routed, context, min_key, max_key, acc);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = accumulate_inflight_bundles_tuple_counts_in_range(
      inflight, context, pivots, child_num, acc);
   return rc;
}

/************************
 * leaf splits
 ************************/

platform_status
in_memory_leaf_estimate_unique_keys(trunk_node_context *context,
                                    in_memory_node     *leaf,
                                    uint64             *estimate)
{
   platform_status rc;

   debug_assert(in_memory_node_is_well_formed_leaf(context->cfg, leaf));

   routing_filter_vector maplets;
   vector_init(&maplets, context->hid);

   in_memory_routed_bundle pivot_bundle = vector_get(&leaf->pivot_bundles, 0);
   rc = vector_append(&maplets, in_memory_routed_bundle_maplet(&pivot_bundle));
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   rc = in_memory_inflight_bundle_vector_collect_maplets(
      &leaf->inflight_bundles,
      0,
      vector_length(&leaf->inflight_bundles),
      &maplets);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   uint64 num_sb_fp     = 0;
   uint64 num_sb_unique = 0;
   for (uint16 inflight_maplet_num = 1;
        inflight_maplet_num < vector_length(&maplets);
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

platform_status
leaf_split_target_num_leaves(trunk_node_context *context,
                             in_memory_node     *leaf,
                             uint64             *target)
{
   debug_assert(in_memory_node_is_well_formed_leaf(context->cfg, leaf));

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

platform_status
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
      in_memory_inflight_bundle *bundle =
         vector_get_ptr(&leaf->inflight_bundles, bundle_num);
      rc = branch_merger_add_inflight_bundle(
         &merger, context->cc, context->cfg->btree_cfg, 0, bundle);
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

      iterator_next(merger.merge_itor);
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

platform_status
in_memory_leaf_split_init(in_memory_node     *new_leaf,
                          trunk_node_context *context,
                          in_memory_node     *leaf,
                          key                 min_key,
                          key                 max_key)
{
   platform_status rc;
   platform_assert(in_memory_node_is_leaf(leaf));

   // Create the new pivots vector
   pivot *lb = in_memory_pivot_create(context->hid, min_key);
   if (lb == NULL) {
      return STATUS_NO_MEMORY;
   }
   pivot *ub = in_memory_pivot_create(context->hid, max_key);
   if (ub == NULL) {
      rc = STATUS_NO_MEMORY;
      goto cleanup_lb;
   }
   in_memory_pivot_vector pivots;
   vector_init(&pivots, context->hid);
   rc = vector_append(&pivots, lb);
   if (!SUCCESS(rc)) {
      goto cleanup_pivots;
   }
   rc = vector_append(&pivots, ub);
   if (!SUCCESS(rc)) {
      goto cleanup_pivots;
   }

   // Create the new pivot_bundles vector
   in_memory_routed_bundle_vector pivot_bundles;
   vector_init(&pivot_bundles, context->hid);
   rc = VECTOR_EMPLACE_APPEND(&pivot_bundles,
                              in_memory_routed_bundle_init_copy,
                              context->hid,
                              vector_get_ptr(&leaf->pivot_bundles, 0));
   if (!SUCCESS(rc)) {
      goto cleanup_pivot_bundles;
   }

   // Create the inflight bundles vector
   in_memory_inflight_bundle_vector inflight_bundles;
   rc = in_memory_inflight_bundle_vector_init_split(
      &inflight_bundles, &leaf->inflight_bundles, context->hid, 0, 1);
   if (!SUCCESS(rc)) {
      goto cleanup_inflight_bundles;
   }

   // Compute the tuple counts for the new leaf
   btree_pivot_stats stats;
   ZERO_CONTENTS(&stats);
   rc = accumulate_bundles_tuple_counts_in_range(
      vector_get_ptr(&pivot_bundles, 0),
      &inflight_bundles,
      context,
      &pivots,
      0,
      &stats);
   if (!SUCCESS(rc)) {
      goto cleanup_inflight_bundles;
   }

   in_memory_node_init(new_leaf, 0, pivots, pivot_bundles, 0, inflight_bundles);

   return rc;

cleanup_inflight_bundles:
   VECTOR_APPLY_TO_PTRS(&inflight_bundles, in_memory_inflight_bundle_deinit);
   vector_deinit(&inflight_bundles);
cleanup_pivot_bundles:
   vector_deinit(&pivot_bundles);
cleanup_pivots:
   vector_deinit(&pivots);
cleanup_lb:
   in_memory_pivot_destroy(lb, context->hid);
   return rc;
}

platform_status
in_memory_leaf_split_truncate(in_memory_node     *leaf,
                              trunk_node_context *context,
                              key                 new_max_key)
{
   in_memory_pivot *newub = in_memory_pivot_create(context->hid, new_max_key);
   if (newub == NULL) {
      return STATUS_NO_MEMORY;
   }
   in_memory_pivot *oldub = vector_get(&leaf->pivots, 1);
   in_memory_pivot_destroy(oldub, context->hid);
   vector_set(&leaf->pivots, 1, newub);

   // Compute the tuple counts for the new leaf
   btree_pivot_stats stats;
   ZERO_CONTENTS(&stats);
   platform_status rc = accumulate_bundles_tuple_counts_in_range(
      vector_get_ptr(&leaf->pivot_bundles, 0),
      &leaf->inflight_bundles,
      context,
      &leaf->pivots,
      0,
      &stats);
   if (SUCCESS(rc)) {
      in_memory_pivot *pivot = in_memory_node_pivot(leaf, 0);
      in_memory_pivot_reset_tuple_counts(pivot);
      in_memory_pivot_add_tuple_counts(
         pivot, 1, stats.num_kvs, stats.key_bytes + stats.message_bytes);
      in_memory_node_reset_num_old_bundles(leaf);
   }

   return rc;
}

platform_status
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

   rc = vector_append(new_leaves, *leaf);
   if (!SUCCESS(rc)) {
      goto cleanup_new_leaves;
   }

   if (target_num_leaves == 1) {
      return STATUS_OK;
   }

   key_buffer_vector pivots;
   vector_init(&pivots, context->hid);
   rc = leaf_split_select_pivots(context, leaf, target_num_leaves, &pivots);
   if (!SUCCESS(rc)) {
      goto cleanup_pivots;
   }

   for (uint64 i = 1; i < vector_length(&pivots) - 1; i++) {
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

   rc =
      in_memory_leaf_split_truncate(vector_get_ptr(new_leaves, 0),
                                    context,
                                    key_buffer_key(vector_get_ptr(&pivots, 1)));
   if (!SUCCESS(rc)) {
      goto cleanup_new_leaves;
   }

cleanup_new_leaves:
   if (!SUCCESS(rc)) {
      // We skip entry 0 because it's the original leaf
      for (uint64 i = 1; i < vector_length(new_leaves); i++) {
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

platform_status
in_memory_index_init_split(in_memory_node  *new_index,
                           platform_heap_id hid,
                           in_memory_node  *index,
                           uint64           start_child_num,
                           uint64           end_child_num)
{
   platform_status rc;

   // We copy the first and last pivots, since those will be used by other
   // nodes, but we steal the pivots in between, since those will be used by
   // only this node.
   in_memory_pivot_vector pivots;
   vector_init(&pivots, hid);
   rc = vector_ensure_capacity(&pivots, end_child_num - start_child_num + 1);
   if (!SUCCESS(rc)) {
      goto cleanup_pivots;
   }
   vector_append(
      &pivots,
      in_memory_pivot_copy(hid, vector_get(&index->pivots, start_child_num)));
   for (uint64 i = start_child_num; i < end_child_num; i++) {
      in_memory_pivot *pivot = vector_get(&index->pivots, i);
      rc                     = vector_append(&pivots, pivot);
      platform_assert_status_ok(rc);
      vector_set(&index->pivots, i, NULL);
   }
   rc = vector_append(
      &pivots,
      in_memory_pivot_copy(hid, vector_get(&index->pivots, end_child_num)));
   platform_assert_status_ok(rc);

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

   in_memory_inflight_bundle_vector inflight_bundles;
   vector_init(&inflight_bundles, hid);
   if (!SUCCESS(rc)) {
      goto cleanup_inflight_bundles;
   }
   rc = in_memory_inflight_bundle_vector_init_split(&inflight_bundles,
                                                    &index->inflight_bundles,
                                                    hid,
                                                    start_child_num,
                                                    end_child_num);
   if (!SUCCESS(rc)) {
      goto cleanup_inflight_bundles;
   }

   in_memory_node_init(new_index,
                       in_memory_node_height(index),
                       pivots,
                       pivot_bundles,
                       0,
                       inflight_bundles);

   return rc;

cleanup_inflight_bundles:
   VECTOR_APPLY_TO_PTRS(&inflight_bundles, in_memory_inflight_bundle_deinit);
   vector_deinit(&inflight_bundles);
cleanup_pivot_bundles:
   VECTOR_APPLY_TO_PTRS(&pivot_bundles, in_memory_routed_bundle_deinit);
   vector_deinit(&pivot_bundles);
cleanup_pivots:
   VECTOR_APPLY_TO_ELTS(&pivots, in_memory_pivot_destroy, hid);
   vector_deinit(&pivots);
   return rc;
}

void
in_memory_index_split_truncate(in_memory_node *index, uint64 num_children)
{
   vector_truncate(&index->pivots, num_children + 1);
   vector_truncate(&index->pivot_bundles, num_children);
   VECTOR_APPLY_TO_PTRS(&index->inflight_bundles,
                        in_memory_inflight_bundle_truncate,
                        num_children);
   in_memory_node_reset_num_old_bundles(index);
}

platform_status
in_memory_index_split(trunk_node_context    *context,
                      in_memory_node        *index,
                      in_memory_node_vector *new_indexes)
{
   platform_status rc;
   rc = vector_append(new_indexes, *index);
   if (!SUCCESS(rc)) {
      goto cleanup_new_indexes;
   }

   uint64 num_children = in_memory_node_num_children(index);
   uint64 num_nodes    = (num_children + context->cfg->target_fanout - 1)
                      / context->cfg->target_fanout;

   for (uint64 i = 1; i < num_nodes; i++) {
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

   in_memory_index_split_truncate(vector_get_ptr(new_indexes, 0),
                                  num_children / num_nodes);

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

platform_status
in_memory_node_receive_bundles(trunk_node_context               *context,
                               in_memory_node                   *node,
                               in_memory_routed_bundle          *routed,
                               in_memory_inflight_bundle_vector *inflight,
                               uint64                            inflight_start,
                               uint64                            num_tuples,
                               uint64                            num_kv_bytes,
                               uint64                            child_num)
{
   platform_status rc;

   rc = vector_ensure_capacity(&node->inflight_bundles,
                               (routed ? 1 : 0) + vector_length(inflight));
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (routed) {
      rc = VECTOR_EMPLACE_APPEND(&node->inflight_bundles,
                                 in_memory_inflight_bundle_init_from_routed,
                                 context->hid,
                                 routed);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   for (uint64 i = 0; i < vector_length(inflight); i++) {
      rc = VECTOR_EMPLACE_APPEND(&node->inflight_bundles,
                                 in_memory_inflight_bundle_init_from_flush,
                                 context->hid,
                                 vector_get_ptr(inflight, i),
                                 child_num);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   VECTOR_APPLY_TO_ELTS(&node->pivots,
                        in_memory_pivot_add_tuple_counts,
                        1,
                        num_tuples,
                        num_kv_bytes);

   return rc;
}

bool
leaf_might_need_to_split(const trunk_node_config *cfg, in_memory_node *leaf)
{
   return cfg->leaf_split_threshold_kv_bytes
          < in_memory_leaf_num_kv_bytes(leaf);
}

platform_status
restore_balance_leaf(trunk_node_context    *context,
                     in_memory_node        *leaf,
                     in_memory_node_vector *new_leaves)
{
   platform_status rc;
   if (leaf_might_need_to_split(context->cfg, leaf)) {
      rc = in_memory_leaf_split(context, leaf, new_leaves);
   } else {
      rc = vector_append(new_leaves, *leaf);
   }

   return rc;
}

platform_status
flush_then_compact(trunk_node_context               *context,
                   in_memory_node                   *node,
                   in_memory_routed_bundle          *routed,
                   in_memory_inflight_bundle_vector *inflight,
                   uint64                            inflight_start,
                   uint64                            num_tuples,
                   uint64                            num_kv_bytes,
                   uint64                            child_num,
                   in_memory_node_vector            *new_nodes);

platform_status
restore_balance_index(trunk_node_context    *context,
                      in_memory_node        *index,
                      in_memory_node_vector *new_indexes)
{
   platform_status rc;

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
                  in_memory_pivot_num_tuples(pivot),
                  in_memory_pivot_num_kv_bytes(pivot),
                  i,
                  &new_children);
               if (!SUCCESS(rc)) {
                  in_memory_node_deinit(&child, context);
                  vector_deinit(&new_children);
                  return rc;
               }

               // At this point, child has been moved into new_children, so we
               // let it go out of scope.
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
 * Flush the routed bundle and inflight bundles inflight[inflight_start...] to
 * the given node.
 *
 * num_tuples and num_kv_bytes are the stats for the incoming bundles (i.e. when
 * flushing from a parent node, they are the per-pivot stat information, when
 * performing a memtable incorporation, they are the stats for the incoming
 * memtable).
 *
 * child_num is the child number of the node addr within its parent.
 *
 * flush_then_compact may choose to split the node.  The resulting
 * node/nodes are returned in new_nodes.
 */
platform_status
flush_then_compact(trunk_node_context               *context,
                   in_memory_node                   *node,
                   in_memory_routed_bundle          *routed,
                   in_memory_inflight_bundle_vector *inflight,
                   uint64                            inflight_start,
                   uint64                            num_tuples,
                   uint64                            num_kv_bytes,
                   uint64                            child_num,
                   in_memory_node_vector            *new_nodes)
{
   platform_status rc;

   // Add the bundles to the node
   rc = in_memory_node_receive_bundles(context,
                                       node,
                                       routed,
                                       inflight,
                                       inflight_start,
                                       num_tuples,
                                       num_kv_bytes,
                                       child_num);
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

platform_status
build_new_roots(trunk_node_context *context, in_memory_node_vector *nodes)
{
   platform_status rc;

   debug_assert(1 < vector_length(nodes));

   // Remember the height now, since we will lose ownership of the children when
   // we enqueue compactions on them.
   uint64 height = in_memory_node_height(vector_get_ptr(nodes, 0));

   // Serialize the children and enqueue their compactions. This will give us
   // back the pivots for the new root node.
   in_memory_pivot_vector pivots;
   vector_init(&pivots, context->hid);
   rc = serialize_nodes_and_enqueue_bundle_compactions(context, nodes, &pivots);
   if (!SUCCESS(rc)) {
      goto cleanup_pivots;
   }
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
   in_memory_inflight_bundle_vector inflight;
   vector_init(&inflight, context->hid);

   // Build the new root
   in_memory_node new_root;
   in_memory_node_init(
      &new_root, height + 1, pivots, pivot_bundles, 0, inflight);

   // At this point, all our resources that we've allocated have been put into
   // the new root.

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
            uint64              root_addr,
            routing_filter      filter,
            branch_ref          branch,
            uint64              num_tuples,
            uint64              num_kv_bytes,
            uint64             *new_root_addr)
{
   platform_status rc;

   in_memory_inflight_bundle_vector inflight;
   vector_init(&inflight, context->hid);

   in_memory_node_vector new_nodes;
   vector_init(&new_nodes, context->hid);

   // Read the old root.
   in_memory_node root;
   rc = in_memory_node_deserialize(context, root_addr, &root);
   if (!SUCCESS(rc)) {
      goto cleanup_vectors;
   }

   // Construct a vector of inflight bundles with one singleton bundle for the
   // new branch.
   rc = VECTOR_EMPLACE_APPEND(&inflight,
                              in_memory_inflight_bundle_init_singleton,
                              context->hid,
                              filter,
                              branch);
   if (!SUCCESS(rc)) {
      goto cleanup_root;
   }

   // "flush" the new bundle to the root, then do any rebalancing needed.
   rc = flush_then_compact(context,
                           &root,
                           NULL,
                           &inflight,
                           0,
                           num_tuples,
                           num_kv_bytes,
                           0,
                           &new_nodes);
   if (!SUCCESS(rc)) {
      goto cleanup_root;
   }

   // At this point. root has been copied into new_nodes, so we should no longer
   // clean it up on failure -- it will get cleaned up when we clean up
   // new_nodes.

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
   VECTOR_APPLY_TO_PTRS(&inflight, in_memory_inflight_bundle_deinit);
   vector_deinit(&inflight);

   return rc;
}