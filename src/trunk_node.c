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
#include "poison.h"

typedef struct ONDISK branch_ref {
   uint64 addr;
} branch_ref;

typedef struct ONDISK maplet_ref {
   uint64 addr;
} maplet_ref;

/*
 * Routed bundles are used to represent the pivot bundles, i.e. one
 * maplet that covers some number of branches.
 */
typedef struct ONDISK routed_bundle {
   maplet_ref maplet;
   uint16     num_branches;
   branch_ref branches[];
} routed_bundle;

/*
 * A compaction produces a per-child bundle, which has one branch per
 * child of the node, plus several maplets, each of which acts like a
 * filter.
 */
typedef struct ONDISK per_child_bundle {
   uint64     num_maplets;
   maplet_ref maplets[];
   /* Following the maplets is one branch per child. */
} per_child_bundle;

/*
 * When flushing a per-child bundle, only the branch for that child is
 * flushed to the child.  This results in a singleton bundle, i.e. a
 * bundle with a single branch and multiple maplets, each of which
 * acts as a filter.
 */
typedef struct ONDISK singleton_bundle {
   branch_ref branch;
   uint64     num_maplets;
   maplet_ref maplets[];
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

#define VECTOR_NAME         in_memory_pivot_vector
#define VECTOR_ELEMENT_TYPE pivot *
#define VECTOR_STORAGE      static
#include "vector_method_defns.h"
#undef VECTOR_NAME
#undef VECTOR_ELEMENT_TYPE
#undef VECTOR_STORAGE

#define VECTOR_NAME         in_memory_routed_bundle_vector
#define VECTOR_ELEMENT_TYPE in_memory_routed_bundle *
#define VECTOR_STORAGE      static
#include "vector_method_defns.h"
#undef VECTOR_NAME
#undef VECTOR_ELEMENT_TYPE
#undef VECTOR_STORAGE

#define VECTOR_NAME         in_memory_inflight_bundle_vector
#define VECTOR_ELEMENT_TYPE in_memory_inflight_bundle *
#define VECTOR_STORAGE      static
#include "vector_method_defns.h"
#undef VECTOR_NAME
#undef VECTOR_ELEMENT_TYPE
#undef VECTOR_STORAGE

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

maplet_ref
create_maplet_ref(uint64 addr)
{
   return (maplet_ref){.addr = addr};
}

uint64
maplet_ref_addr(maplet_ref mref)
{
   return mref.addr;
}

key
in_memory_pivot_key(const in_memory_pivot *pivot)
{
   return ondisk_key_to_key(&pivot->key);
}

uint64
in_memory_node_num_children(const in_memory_node *node)
{
   return node->num_pivots - 1;
}

in_memory_routed_bundle *
in_memory_routed_bundle_create(platform_heap_id hid,
                               maplet_ref       maplet,
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
                                   maplet_ref                     new_maplet,
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
in_memory_routed_bundle_destroy(platform_heap_id         hid,
                                in_memory_routed_bundle *bundle)
{
   platform_free(hid, bundle);
}

maplet_ref
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

maplet_ref
in_memory_per_child_bundle_maplet(const in_memory_per_child_bundle *bundle,
                                  uint64                            i)
{
   debug_assert(i < bundle->num_maplets);
   return bundle->maplets[i];
}

const maplet_ref *
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

maplet_ref
in_memory_singleton_bundle_maplet(const in_memory_singleton_bundle *bundle,
                                  uint64                            i)
{
   debug_assert(i < bundle->num_maplets);
   return bundle->maplets[i];
}

const maplet_ref *
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

void
in_memory_inflight_bundle_collect_maplets(
   uint64                           num_bundles,
   const in_memory_inflight_bundle *bundles,
   uint64                           maplets_capacity,
   maplet_ref                      *maplets)
{
   uint64 num_maplets = 0;
   for (uint64 i = 0; i < num_bundles; i++) {
      const in_memory_inflight_bundle *bundle = &bundles[i];
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
            const maplet_ref *bmaplets =
               in_memory_per_child_bundle_maplet_array(&bundle->u.per_child);
            memcpy(
               &maplets[num_maplets], bmaplets, nbmaplets * sizeof(maplet_ref));
            num_maplets += nbmaplets;
            break;
         }
         case INFLIGHT_BUNDLE_TYPE_SINGLETON:
         {
            uint64 nbmaplets =
               in_memory_singleton_bundle_num_maplets(&bundle->u.singleton);
            platform_assert(num_maplets + nbmaplets <= maplets_capacity);
            const maplet_ref *bmaplets =
               in_memory_singleton_bundle_maplet_array(&bundle->u.singleton);
            memcpy(
               &maplets[num_maplets], bmaplets, nbmaplets * sizeof(maplet_ref));
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
   platform_heap_id                 hid,
   uint64                           num_bundles,
   const in_memory_inflight_bundle *bundles,
   uint64                           num_branches,
   branch_ref                      *branches)
{
   uint64 num_maplets = 0;
   for (int i = 0; i < num_branches; i++) {
      num_maplets += in_memory_inflight_bundle_num_maplets(&bundles[i]);
   }

   in_memory_inflight_bundle *result = platform_aligned_zalloc(
      hid,
      PLATFORM_CACHELINE_SIZE,
      sizeof(in_memory_inflight_bundle) + num_maplets * sizeof(maplet_ref)
         + num_branches * sizeof(branch_ref));

   if (result != NULL) {
      result->type                    = INFLIGHT_BUNDLE_TYPE_PER_CHILD;
      result->u.per_child.num_maplets = num_maplets;
      maplet_ref *new_maplets_array   = result->u.per_child.maplets;
      in_memory_inflight_bundle_collect_maplets(
         num_bundles, bundles, num_maplets, new_maplets_array);
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

   in_memory_pivot *lbpivot =
      in_memory_pivot_vector_get(&node->pivots, child_num);
   in_memory_pivot *ubpivot =
      in_memory_pivot_vector_get(&node->pivots, child_num + 1);
   key               lb = in_memory_pivot_key(lbpivot);
   key               ub = in_memory_pivot_key(ubpivot);
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

   platform_status rc = in_memory_inflight_bundle_vector_append(
      &node->inflight_bundles, inflight);
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

   platform_status rc = in_memory_inflight_bundle_vector_append(
      &node->inflight_bundles, inflight);
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

   platform_status rc = in_memory_inflight_bundle_vector_append(
      &node->inflight_bundles, inflight);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64            num_branches = 1;
   const branch_ref *branches     = &inflight->u.singleton.branch;
   rc                             = add_branches_tuple_counts(
      cc, cfg, node, num_branches, branches, BRANCH_TUPLE_COUNT_ADD);

   return rc;
}

static in_memory_routed_bundle empty_routed_bundle = {{0}, 0};

routed_bundle *
in_memory_node_extract_pivot_bundle(cache              *cc,
                                    const btree_config *cfg,
                                    in_memory_node     *node,
                                    uint64              child_num)
{
   debug_assert(child_num < in_memory_node_num_children(node));
   routed_bundle *result =
      in_memory_routed_bundle_vector_get(&node->pivot_bundles, child_num);
   uint64 num_branches        = in_memory_routed_bundle_num_branches(result);
   const branch_ref *branches = in_memory_routed_bundle_branch_array(result);
   platform_status   rc       = add_branches_tuple_counts_for_child(
      cc, cfg, node, num_branches, branches, BRANCH_TUPLE_COUNT_SUB, child_num);
   if (SUCCESS(rc)) {
      in_memory_routed_bundle_vector_set(
         &node->pivot_bundles, child_num, &empty_routed_bundle);
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
   if (pivot_bundle != &empty_routed_bundle) {
      platform_free(parent->hid, pivot_bundle);
   }

   in_memory_pivot *pivot =
      in_memory_pivot_vector_get(&parent->pivots, child_num);
   while (pivot->inflight_bundle_start
          < in_memory_inflight_bundle_vector_length(&parent->inflight_bundles))
   {
      in_memory_inflight_bundle *bundle = in_memory_inflight_bundle_vector_get(
         &parent->inflight_bundles, pivot->inflight_bundle_start);
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
