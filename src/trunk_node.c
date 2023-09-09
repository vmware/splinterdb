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

typedef struct ONDISK in_memory_pivot {
   trunk_pivot_stats prereceive_stats;
   trunk_pivot_stats stats;
   uint64            child_addr;
   uint64            inflight_bundle_start;
   ondisk_key        key;
} in_memory_pivot;

typedef VECTOR(in_memory_pivot *) in_memory_pivot_vector;
typedef VECTOR(in_memory_routed_bundle) in_memory_routed_bundle_vector;
typedef VECTOR(in_memory_inflight_bundle) in_memory_inflight_bundle_vector;
typedef VECTOR(trunk_pivot_stats) trunk_pivot_stats_vector;

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
   uint64                max_tuples_per_node;
} trunk_node_config;

typedef struct bundle_compaction_group {
   uint64         refcount;
   uint64         addr;
   in_memory_node node;
   uint64         next_pivot;
   uint64         completed_pivots;
   bool32         failed;
} bundle_compaction_group;

typedef enum bundle_compaction_state {
   BUNDLE_COMPACTION_NOT_STARTED,
   BUNDLE_COMPACTION_INPROGRESS,
   BUNDLE_COMPACTION_FAILED,
   BUNDLE_COMPACTION_COMPLETED
} bundle_compaction_state;

typedef struct bundle_compaction {
   struct bundle_compaction *next;
   bundle_compaction_group  *group;
   bundle_compaction_state   state;
   branch_merger             merger;
   btree_pack_req            pack_req;
} bundle_compaction;

typedef struct pivot_compaction_state {
   trunk_node_context *context;
   key_buffer          key;
   uint64              height;
   uint64              spinlock;
   bool32              maplet_compaction_failed;
   bundle_compaction  *bundle_compactions;
} pivot_compaction_state;

#if 0
typedef struct maplet_compaction_input {
   branch_ref branch;
   uint64     num_fingerprints;
   uint32    *fingerprints;
} maplet_compaction_input;

typedef VECTOR(maplet_compaction_input) maplet_compaction_input_vector;

typedef struct maplet_compaction_args {
   trunk_node_context            *context;
   key_buffer                     lbkey;
   uint64                         height;
   routing_filter                 old_maplet;
   uint64                         old_num_branches;
   branch_ref_vector              branches;
   routing_filter                 new_maplet;
   bool32                         can_delete_pivot_from_tracker;
   struct maplet_compaction_args *successor;
} maplet_compaction_args;

typedef VECTOR(maplet_compaction_args *) maplet_compaction_args_vector;
typedef VECTOR(uint64) uint64_vector;

typedef struct bundle_compaction_args {
   trunk_node_context           *context;
   uint64                        addr;
   in_memory_node                node;
   uint64                        next_child;
   uint64                        completed_compactions;
   bool32                        failed;
   branch_merger                *mergers;
   btree_pack_req               *pack_reqs;
   maplet_compaction_args_vector maplet_compaction_args;
   uint64_vector                 installed_branch_indexes;
} bundle_compaction_args;


typedef struct maplet_compaction_tracker_entry {
   struct maplet_compaction_tracker_entry *next;
   key_buffer                              pivot;
   uint64                                  height;
   maplet_compaction_args                 *args;
   maplet_compaction_input_vector          inputs;
} maplet_compaction_tracker_entry;

typedef struct maplet_compaction_tracker_bucket {
   uint64                           lock;
   maplet_compaction_tracker_entry *head;
} maplet_compaction_tracker_bucket;

#   define MAPLET_COMPACTION_TRACKER_BUCKETS 1024

typedef struct maplet_compaction_input_tracker {
   platform_heap_id                 hid;
   data_config                     *data_cfg;
   maplet_compaction_tracker_bucket buckets[MAPLET_COMPACTION_TRACKER_BUCKETS];
} maplet_compaction_input_tracker;
#endif

#define PIVOT_STATE_MAP_BUCKETS 1024

typedef struct pivot_state_map {
   uint64                  locks[PIVOT_STATE_MAP_BUCKETS];
   pivot_compaction_state *buckets[PIVOT_STATE_MAP_BUCKETS];
} pivot_state_map;

typedef struct trunk_node_context {
   const trunk_node_config *cfg;
   platform_heap_id         hid;
   cache                   *cc;
   allocator               *al;
   task_system             *ts;
   pivot_state_map          pivot_states;
   uint64                   root_height;
   uint64                   root_addr;
} trunk_node_context;

/***************************************************
 * branch_ref operations
 ***************************************************/

static inline branch_ref
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

static inline bool32
in_memory_routed_bundles_equal(const in_memory_routed_bundle *a,
                               const in_memory_routed_bundle *b)
{
   return routing_filters_equal(&a->maplet, &b->maplet)
          && VECTOR_ELTS_EQUAL(&a->branches, &b->branches, branches_equal);
}

/*****************************
 * per_child_bundle operations
 *****************************/

/* Note that init moves maplets and branches into the bundle */
static inline void
in_memory_per_child_bundle_init(in_memory_per_child_bundle *bundle,
                                routing_filter_vector      *maplets,
                                branch_ref_vector          *branches)
{
   bundle->maplets  = *maplets;
   bundle->branches = *branches;
}

static platform_status
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

static inline void
in_memory_per_child_bundle_deinit(in_memory_per_child_bundle *bundle)
{
   vector_deinit(&bundle->maplets);
   vector_deinit(&bundle->branches);
}

static inline branch_ref
in_memory_per_child_bundle_branch(const in_memory_per_child_bundle *bundle,
                                  uint64                            i)
{
   return vector_get(&bundle->branches, i);
}

static inline bool32
in_memory_per_child_bundles_equal(const in_memory_per_child_bundle *a,
                                  const in_memory_per_child_bundle *b)
{
   return VECTOR_ELTS_EQUAL_BY_PTR(
             &a->maplets, &b->maplets, routing_filters_equal)
          && VECTOR_ELTS_EQUAL(&a->branches, &b->branches, branches_equal);
}

/*****************************
 * singleton_bundle operations
 *****************************/

static inline platform_status
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

static inline platform_status
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

static inline platform_status
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

static inline void
in_memory_singleton_bundle_deinit(in_memory_singleton_bundle *bundle)
{
   vector_deinit(&bundle->maplets);
}

static inline branch_ref
in_memory_singleton_bundle_branch(const in_memory_singleton_bundle *bundle)
{
   return bundle->branch;
}

static inline bool32
in_memory_singleton_bundles_equal(const in_memory_singleton_bundle *a,
                                  const in_memory_singleton_bundle *b)
{
   return VECTOR_ELTS_EQUAL_BY_PTR(
             &a->maplets, &b->maplets, routing_filters_equal)
          && branches_equal(a->branch, b->branch);
}

/****************************
 * inflight_bundle operations
 ****************************/

static inline platform_status
in_memory_inflight_bundle_init_from_routed(
   in_memory_inflight_bundle     *bundle,
   platform_heap_id               hid,
   const in_memory_routed_bundle *routed)
{
   bundle->type = INFLIGHT_BUNDLE_TYPE_ROUTED;
   return in_memory_routed_bundle_init_copy(&bundle->u.routed, hid, routed);
}

static inline platform_status
in_memory_inflight_bundle_init_singleton(in_memory_inflight_bundle *bundle,
                                         platform_heap_id           hid,
                                         routing_filter             maplet,
                                         branch_ref                 branch)
{
   bundle->type = INFLIGHT_BUNDLE_TYPE_SINGLETON;
   return in_memory_singleton_bundle_init(
      &bundle->u.singleton, hid, maplet, branch);
}

static inline platform_status
in_memory_inflight_bundle_init_from_singleton(
   in_memory_inflight_bundle        *bundle,
   platform_heap_id                  hid,
   const in_memory_singleton_bundle *src)
{
   bundle->type = INFLIGHT_BUNDLE_TYPE_SINGLETON;
   return in_memory_singleton_bundle_init_copy(&bundle->u.singleton, hid, src);
}

static inline platform_status
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

static inline void
in_memory_inflight_bundle_init_per_child(in_memory_inflight_bundle *bundle,
                                         platform_heap_id           hid,
                                         routing_filter_vector     *maplets,
                                         branch_ref_vector         *branches)
{
   bundle->type = INFLIGHT_BUNDLE_TYPE_PER_CHILD;
   in_memory_per_child_bundle_init(&bundle->u.per_child, maplets, branches);
}

static inline platform_status
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

static inline platform_status
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

static platform_status
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
static inline platform_status
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

static inline void
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

static inline inflight_bundle_type
in_memory_inflight_bundle_type(const in_memory_inflight_bundle *bundle)
{
   return bundle->type;
}

static inline bool32
in_memory_inflight_bundles_equal(const in_memory_inflight_bundle *a,
                                 const in_memory_inflight_bundle *b)
{
   if (a->type != b->type) {
      return false;
   }

   switch (a->type) {
      case INFLIGHT_BUNDLE_TYPE_ROUTED:
         return in_memory_routed_bundles_equal(&a->u.routed, &b->u.routed);
      case INFLIGHT_BUNDLE_TYPE_PER_CHILD:
         return in_memory_per_child_bundles_equal(&a->u.per_child,
                                                  &b->u.per_child);
      case INFLIGHT_BUNDLE_TYPE_SINGLETON:
         return in_memory_singleton_bundles_equal(&a->u.singleton,
                                                  &b->u.singleton);
      default:
         platform_assert(0);
         return false;
   }
}

static inline platform_status
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

static inline platform_status
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

static platform_status
in_memory_node_init_empty_leaf(in_memory_node  *node,
                               platform_heap_id hid,
                               key              lb,
                               key              ub)
{
   in_memory_pivot_vector           pivots;
   in_memory_routed_bundle_vector   pivot_bundles;
   in_memory_inflight_bundle_vector inflight_bundles;
   platform_status                  rc;

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

static inline void
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

static inline platform_status
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

static inline platform_status
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
 * maplet compaction input tracking
 *
 * This is a quick and simple implementation.  Better would be a concurrent hash
 * table.
 *******************************************************************************/

static void
maplet_compaction_input_tracker_init(maplet_compaction_input_tracker *tracker,
                                     data_config                     *data_cfg,
                                     platform_heap_id                 hid)
{
   ZERO_CONTENTS(tracker);
   tracker->data_cfg = data_cfg;
   tracker->hid      = hid;
}

static uint64
maplet_compaction_tracker_hash(const data_config *data_cfg,
                               key                lbkey,
                               uint64             height)
{
   uint64 hash = data_cfg->key_hash(key_data(lbkey), key_length(lbkey), 271828);
   hash ^= height;
   return hash % MAPLET_COMPACTION_TRACKER_BUCKETS;
}

static void
maplet_compaction_input_tracker_unlock(maplet_compaction_input_tracker *tracker,
                                       uint64 bucketidx)
{
   maplet_compaction_tracker_bucket *bucket = &tracker->buckets[bucketidx];
   bucket->lock                             = 0;
}

static maplet_compaction_tracker_entry *
maplet_compaction_input_tracker_get_locked(
   maplet_compaction_input_tracker *tracker,
   key                              lbkey,
   uint64                           height,
   uint64                           bucketidx)
{
   maplet_compaction_tracker_bucket *bucket = &tracker->buckets[bucketidx];
   uint64                            wait   = 1;
   while (__sync_val_compare_and_swap(&bucket->lock, 0, 1) != 0) {
      platform_sleep_ns(wait);
      wait = MIN(2 * wait, 2048);
   }

   maplet_compaction_tracker_entry *entry = bucket->head;
   while (entry) {
      if (data_key_compare(
             tracker->data_cfg, key_buffer_key(&entry->pivot), lbkey)
             == 0
          && entry->height == height)
      {
         return entry;
      }
      entry = entry->next;
   }

   return NULL;
}

static int64
maplet_compaction_tracker_entry_find_input(
   const maplet_compaction_tracker_entry *entry,
   branch_ref                             bref)
{
   for (uint64 i = 0; i < vector_length(&entry->inputs); i++) {
      maplet_compaction_input existing = vector_get(&entry->inputs, i);
      if (branches_equal(existing.branch, bref)) {
         return i;
      }
   }
   return -1;
}

static maplet_compaction_tracker_entry *
maplet_compaction_tracker_entry_create(key              lbkey,
                                       uint64           height,
                                       platform_heap_id hid)
{
   maplet_compaction_tracker_entry *entry = TYPED_ZALLOC(hid, entry);
   if (entry == NULL) {
      return NULL;
   }
   key_buffer_init_from_key(&entry->pivot, hid, lbkey);
   entry->height = height;
   vector_init(&entry->inputs, hid);
   return entry;
}

static void
maplet_compaction_tracker_entry_destroy(maplet_compaction_tracker_entry *entry,
                                        platform_heap_id                 hid)
{
   for (uint64 i = 0; i < vector_length(&entry->inputs); i++) {
      maplet_compaction_input input = vector_get(&entry->inputs, i);
      platform_free(input.fingerprints, hid);
   }
   vector_deinit(&entry->inputs);
   key_buffer_deinit(&entry->pivot);
   platform_free(hid, entry);
}

static bool32
maplet_compaction_tracker_lookup_inputs(
   maplet_compaction_input_tracker *tracker,
   key                              lbkey,
   uint64                           height,
   const branch_ref_vector         *branches,
   maplet_compaction_input_vector  *inputs)
{
   platform_status rc = vector_ensure_capacity(inputs, vector_length(branches));
   if (!SUCCESS(rc)) {
      return FALSE;
   }
   vector_truncate(inputs, 0);

   uint64 bucketidx =
      maplet_compaction_tracker_hash(tracker->data_cfg, lbkey, height);
   maplet_compaction_tracker_entry *entry =
      maplet_compaction_input_tracker_get_locked(
         tracker, lbkey, height, bucketidx);
   if (entry == NULL) {
      maplet_compaction_input_tracker_unlock(tracker, bucketidx);
      return FALSE;
   }

   bool32 result = TRUE;
   for (uint64 i = 0; i < vector_length(branches); i++) {
      branch_ref bref = vector_get(branches, i);
      int64      idx  = maplet_compaction_tracker_entry_find_input(entry, bref);
      if (idx < 0) {
         result = FALSE;
         break;
      } else {
         rc = vector_append(inputs, vector_get(&entry->inputs, idx));
         platform_assert_status_ok(rc);
      }
   }

   maplet_compaction_input_tracker_unlock(tracker, bucketidx);
   return result;
}

static platform_status
maplet_compaction_tracker_add_pivot(maplet_compaction_input_tracker *tracker,
                                    key                              lbkey,
                                    uint64                           height)
{
   uint64 bucketidx =
      maplet_compaction_tracker_hash(tracker->data_cfg, lbkey, height);

   platform_status                  rc           = STATUS_OK;
   bool32                           entry_is_new = FALSE;
   maplet_compaction_tracker_entry *entry =
      maplet_compaction_input_tracker_get_locked(
         tracker, lbkey, height, bucketidx);
   if (entry == NULL) {
      entry =
         maplet_compaction_tracker_entry_create(lbkey, height, tracker->hid);
      if (entry == NULL) {
         rc = STATUS_NO_MEMORY;
         goto cleanup;
      }
      entry_is_new = TRUE;
   }

   if (entry_is_new) {
      maplet_compaction_tracker_bucket *bucket = &tracker->buckets[bucketidx];
      entry->next                              = bucket->head;
      bucket->head                             = entry;
   }

cleanup:
   if (!SUCCESS(rc) && entry_is_new) {
      maplet_compaction_tracker_entry_destroy(entry, tracker->hid);
   }
   maplet_compaction_input_tracker_unlock(tracker, bucketidx);
   return rc;
}

static platform_status
maplet_compaction_tracker_add_input(maplet_compaction_input_tracker *tracker,
                                    key                              lbkey,
                                    uint64                           height,
                                    maplet_compaction_input          input)
{
   uint64 bucketidx =
      maplet_compaction_tracker_hash(tracker->data_cfg, lbkey, height);

   platform_status                  rc = STATUS_OK;
   maplet_compaction_tracker_entry *entry =
      maplet_compaction_input_tracker_get_locked(
         tracker, lbkey, height, bucketidx);
   if (entry == NULL) {
      rc = STATUS_NOT_FOUND;
      goto cleanup;
   }

   rc = vector_append(&entry->inputs, input);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

cleanup:
   maplet_compaction_input_tracker_unlock(tracker, bucketidx);
   return rc;
}

static void
maplet_compaction_tracker_entry_remove(maplet_compaction_tracker_bucket *bucket,
                                       maplet_compaction_tracker_entry  *entry)
{
   if (bucket->head == entry) {
      bucket->head = entry->next;
   } else {
      maplet_compaction_tracker_entry *prev = bucket->head;
      while (prev && prev->next != entry) {
         prev = prev->next;
      }
      if (prev) {
         prev->next = entry->next;
      }
   }
}

static void
maplet_compaction_tracker_delete_inputs(
   maplet_compaction_input_tracker *tracker,
   key                              lbkey,
   uint64                           height,
   branch_ref_vector               *branches)
{
   uint64 bucketidx =
      maplet_compaction_tracker_hash(tracker->data_cfg, lbkey, height);
   maplet_compaction_tracker_entry *entry =
      maplet_compaction_input_tracker_get_locked(
         tracker, lbkey, height, bucketidx);
   if (entry == NULL) {
      maplet_compaction_input_tracker_unlock(tracker, bucketidx);
      return;
   }

   for (uint64 i = 0; i < vector_length(branches); i++) {
      branch_ref bref = vector_get(branches, i);
      int64      idx  = maplet_compaction_tracker_entry_find_input(entry, bref);
      if (idx >= 0) {
         uint64 length = vector_length(&entry->inputs);
         vector_set(
            &entry->inputs, idx, vector_get(&entry->inputs, length - 1));
         vector_truncate(&entry->inputs, length - 1);
      }
   }

   if (vector_length(&entry->inputs) == 0) {
      maplet_compaction_tracker_entry_remove(&tracker->buckets[bucketidx],
                                             entry);
      maplet_compaction_tracker_entry_destroy(entry, tracker->hid);
   }

   maplet_compaction_input_tracker_unlock(tracker, bucketidx);
}

static void
maplet_compaction_tracker_remove_pivot_unconditionally(
   maplet_compaction_input_tracker *tracker,
   key                              lbkey,
   uint64                           height)
{
   uint64 bucketidx =
      maplet_compaction_tracker_hash(tracker->data_cfg, lbkey, height);
   maplet_compaction_tracker_entry *entry =
      maplet_compaction_input_tracker_get_locked(
         tracker, lbkey, height, bucketidx);
   if (entry != NULL) {
      maplet_compaction_tracker_entry_remove(&tracker->buckets[bucketidx],
                                             entry);
      maplet_compaction_tracker_entry_destroy(entry, tracker->hid);
   }
   maplet_compaction_input_tracker_unlock(tracker, bucketidx);
}

static void
maplet_compaction_tracker_remove_pivot_for_compaction_args(
   maplet_compaction_input_tracker *tracker,
   key                              lbkey,
   uint64                           height,
   maplet_compaction_args          *args)
{
   uint64 bucketidx =
      maplet_compaction_tracker_hash(tracker->data_cfg, lbkey, height);
   maplet_compaction_tracker_entry *entry =
      maplet_compaction_input_tracker_get_locked(
         tracker, lbkey, height, bucketidx);
   if (entry != NULL && entry->args == args) {
      maplet_compaction_tracker_entry_remove(&tracker->buckets[bucketidx],
                                             entry);
      maplet_compaction_tracker_entry_destroy(entry, tracker->hid);
   }
   maplet_compaction_input_tracker_unlock(tracker, bucketidx);
}


/*********************************************
 * maplet compaction
 *********************************************/

static maplet_compaction_args *
maplet_compaction_args_create(trunk_node_context *context,
                              in_memory_node     *node,
                              uint64              child_num)
{
   platform_status         rc;
   maplet_compaction_args *args = TYPED_ZALLOC(context->hid, args);
   if (args == NULL) {
      return NULL;
   }
   vector_init(&args->branches, context->hid);

   args->context = context;
   rc            = key_buffer_init_from_key(
      &args->lbkey, context->hid, in_memory_node_pivot_key(node, child_num));
   if (!SUCCESS(rc)) {
      goto cleanup_inputs;
   }
   args->height = node->height;
   in_memory_routed_bundle *routed =
      in_memory_node_pivot_bundle(node, child_num);
   args->old_maplet       = routed->maplet;
   args->old_num_branches = in_memory_routed_bundle_num_branches(routed);

   in_memory_pivot *pivot      = in_memory_node_pivot(node, child_num);
   uint64           bundle_num = in_memory_pivot_inflight_bundle_start(pivot);
   while (bundle_num < vector_length(&node->inflight_bundles)) {
      in_memory_inflight_bundle *inflight =
         vector_get_ptr(&node->inflight_bundles, bundle_num);
      if (in_memory_inflight_bundle_type(inflight)
          == INFLIGHT_BUNDLE_TYPE_PER_CHILD) {
         branch_ref bref = in_memory_per_child_bundle_branch(
            &inflight->u.per_child, child_num);
         btree_inc_ref_range(context->cc,
                             context->cfg->btree_cfg,
                             bref.addr,
                             NEGATIVE_INFINITY_KEY,
                             POSITIVE_INFINITY_KEY);
         rc = vector_append(&args->branches, bref);
         if (!SUCCESS(rc)) {
            goto cleanup_lbkey;
         }
      } else {
         break;
      }
      bundle_num++;
   }

   routing_filter_inc_ref(context->cc, &args->old_maplet);

   return args;

cleanup_lbkey:
   key_buffer_deinit(&args->lbkey);
cleanup_inputs:
   vector_deinit(&args->branches);
   platform_free(context->hid, args);
   return NULL;
}

static void
maplet_compaction_args_destroy(maplet_compaction_args *args)
{
   if (!args) {
      return;
   }

   key_buffer_deinit(&args->lbkey);

   routing_filter_dec_ref(args->context->cc, &args->old_maplet);
   routing_filter_dec_ref(args->context->cc, &args->new_maplet);

   for (uint64 i = 0; i < vector_length(&args->branches); i++) {
      branch_ref bref = vector_get(&args->branches, i);
      btree_dec_ref_range(args->context->cc,
                          args->context->cfg->btree_cfg,
                          branch_ref_addr(bref),
                          NEGATIVE_INFINITY_KEY,
                          POSITIVE_INFINITY_KEY);
   }
   vector_deinit(&args->branches);

   maplet_compaction_args_destroy(args->successor);

   platform_free(args->context->hid, args);
}

static platform_status
apply_changes_maplet_compaction(trunk_node_context *context,
                                uint64              addr,
                                in_memory_node     *target,
                                void               *arg)
{
   platform_status         rc;
   maplet_compaction_args *args = (maplet_compaction_args *)arg;

   for (uint64 i = 0; i < in_memory_node_num_children(target); i++) {
      in_memory_routed_bundle *bundle = in_memory_node_pivot_bundle(target, i);
      if (routing_filters_equal(&bundle->maplet, &args->old_maplet)) {
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
         if (in_memory_pivot_inflight_bundle_start(pivot)
             < vector_length(&target->inflight_bundles))
         {
            in_memory_inflight_bundle *inflight =
               vector_get_ptr(&target->inflight_bundles,
                              in_memory_pivot_inflight_bundle_start(pivot));
            if (in_memory_inflight_bundle_type(inflight)
                == INFLIGHT_BUNDLE_TYPE_PER_CHILD) {
               args->successor =
                  maplet_compaction_args_create(context, target, i);
            }
         } else {
            args->can_delete_pivot_from_tracker = TRUE;
         }
         break;
      }
   }

   return STATUS_OK;
}

static void
maplet_compaction_task(void *arg, void *scratch)
{
   platform_status         rc   = STATUS_OK;
   maplet_compaction_args *args = (maplet_compaction_args *)arg;

   maplet_compaction_input_vector inputs;
   vector_init(&inputs, args->context->hid);

   for (maplet_compaction_args *curr = args; curr; curr = curr->successor) {
      routing_filter old_maplet = curr->old_maplet;
      bool32         found      = maplet_compaction_tracker_lookup_inputs(
         &curr->context->maplet_compaction_inputs,
         key_buffer_key(&curr->lbkey),
         curr->height,
         &curr->branches,
         &inputs);
      if (!found) {
         // This pivot got flushed or one of the bundle compactions encountered
         // an error, so nothing to do.
         goto cleanup;
      }

      for (uint64 i = 0; i < vector_length(&inputs); i++) {
         maplet_compaction_input input = vector_get(&inputs, i);

         rc = routing_filter_add(curr->context->cc,
                                 curr->context->cfg->filter_cfg,
                                 curr->context->hid,
                                 &old_maplet,
                                 &curr->new_maplet,
                                 input.fingerprints,
                                 input.num_fingerprints,
                                 curr->old_num_branches + i);
         if (0 < i) {
            routing_filter_dec_ref(curr->context->cc, &old_maplet);
         }
         if (!SUCCESS(rc)) {
            goto cleanup;
         }
         old_maplet = curr->new_maplet;
      }

      apply_changes_begin(curr->context);
      rc = apply_changes(curr->context,
                         key_buffer_key(&curr->lbkey),
                         key_buffer_key(&curr->lbkey),
                         curr->height,
                         apply_changes_maplet_compaction,
                         curr);
      if (SUCCESS(rc) && curr->can_delete_pivot_from_tracker) {
         debug_assert(curr->successor == NULL);
         maplet_compaction_tracker_remove_pivot_for_compaction_args(
            &curr->context->maplet_compaction_inputs,
            key_buffer_key(&curr->lbkey),
            curr->height,
            args);
      }
      apply_changes_end(curr->context);
      if (!SUCCESS(rc)) {
         goto cleanup;
      }
   }

cleanup:
   if (!SUCCESS(rc)) {
      maplet_compaction_tracker_remove_pivot_for_compaction_args(
         &args->context->maplet_compaction_inputs,
         key_buffer_key(&args->lbkey),
         args->height,
         args);
   }
   vector_deinit(&inputs);
   maplet_compaction_args_destroy(args);
}

static inline platform_status
enqueue_maplet_compaction(maplet_compaction_args *args)
{
   return task_enqueue(
      args->context->ts, TASK_TYPE_NORMAL, maplet_compaction_task, args, FALSE);
}

/************************
 * bundle compaction
 ************************/

static void
bundle_compaction_args_destroy(bundle_compaction_args *args)
{
   uint64 num_children = in_memory_node_num_children(&args->node);

   for (uint64 i = 0; i < num_children; i++) {
      if (!in_memory_node_pivot_has_received_bundles(&args->node, i)) {
         continue;
      }
      branch_merger_deinit(&args->mergers[i]);
   }
   for (uint64 i = 0; i < num_children; i++) {
      if (!in_memory_node_pivot_has_received_bundles(&args->node, i)) {
         continue;
      }
      btree_pack_req_deinit(&args->pack_reqs[i], args->context->hid);
   }
   if (args->mergers != NULL) {
      platform_free(args->context->hid, args->mergers);
   }
   if (args->pack_reqs != NULL) {
      platform_free(args->context->hid, args->pack_reqs);
   }

   vector_deinit(&args->installed_branch_indexes);
   VECTOR_APPLY_TO_ELTS(&args->maplet_compaction_args,
                        maplet_compaction_args_destroy);
   vector_deinit(&args->maplet_compaction_args);
   platform_free(args->context->hid, args);
}

static bundle_compaction_args *
bundle_compaction_args_create(trunk_node_context *context,
                              uint64              addr,
                              in_memory_node     *node)
{
   platform_status rc;
   uint64          merger_num   = 0;
   uint64          pack_req_num = 0;

   uint64 num_children = in_memory_node_num_children(node);


   bundle_compaction_args *args = TYPED_ZALLOC(context->hid, args);
   if (args == NULL) {
      return NULL;
   }
   args->context               = context;
   args->addr                  = addr;
   args->node                  = *node;
   args->next_child            = 0;
   args->completed_compactions = 0;
   args->failed                = FALSE;

   vector_init(&args->maplet_compaction_args, context->hid);
   vector_init(&args->installed_branch_indexes, context->hid);
   rc = vector_ensure_capacity(&args->installed_branch_indexes, num_children);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   args->mergers =
      TYPED_ARRAY_ZALLOC(context->hid, args->mergers, num_children);
   args->pack_reqs =
      TYPED_ARRAY_ZALLOC(context->hid, args->pack_reqs, num_children);
   if (args->mergers == NULL || args->pack_reqs == NULL) {
      goto cleanup;
   }

   for (uint64 merger_num = 0; merger_num < num_children; merger_num++) {
      if (!in_memory_node_pivot_has_received_bundles(node, merger_num)) {
         continue;
      }

      branch_merger_init(&args->mergers[merger_num],
                         context->hid,
                         context->cfg->data_cfg,
                         in_memory_node_pivot_key(node, merger_num),
                         in_memory_node_pivot_key(node, merger_num + 1),
                         0);

      for (uint64 i = node->num_old_bundles;
           vector_length(&node->inflight_bundles);
           i++)
      {
         in_memory_inflight_bundle *bundle =
            vector_get_ptr(&node->inflight_bundles, i);
         rc = branch_merger_add_inflight_bundle(&args->mergers[merger_num],
                                                context->cc,
                                                context->cfg->btree_cfg,
                                                merger_num,
                                                bundle);
         if (!SUCCESS(rc)) {
            goto cleanup;
         }
      }

      rc = branch_merger_build_merge_itor(
         &args->mergers[merger_num],
         in_memory_node_is_leaf(node) ? MERGE_FULL : MERGE_INTERMEDIATE);
      if (!SUCCESS(rc)) {
         goto cleanup;
      }
   }

   for (pack_req_num = 0; pack_req_num < num_children; pack_req_num++) {
      if (!in_memory_node_pivot_has_received_bundles(node, pack_req_num)) {
         continue;
      }
      btree_pack_req_init(&args->pack_reqs[pack_req_num],
                          context->cc,
                          context->cfg->btree_cfg,
                          &args->mergers[pack_req_num].merge_itor->super,
                          context->cfg->max_tuples_per_node,
                          context->cfg->filter_cfg->hash,
                          context->cfg->filter_cfg->seed,
                          context->hid);
   }

   return args;

cleanup:
   for (uint64 i = 0; i < merger_num; i++) {
      if (!in_memory_node_pivot_has_received_bundles(node, i)) {
         continue;
      }
      branch_merger_deinit(&args->mergers[i]);
   }
   for (uint64 i = 0; i < pack_req_num; i++) {
      if (!in_memory_node_pivot_has_received_bundles(node, i)) {
         continue;
      }
      btree_pack_req_deinit(&args->pack_reqs[i], context->hid);
   }
   if (args->mergers != NULL) {
      platform_free(context->hid, args->mergers);
   }
   if (args->pack_reqs != NULL) {
      platform_free(context->hid, args->pack_reqs);
   }
   vector_deinit(&args->installed_branch_indexes);
   vector_deinit(&args->maplet_compaction_args);
   platform_free(context->hid, args);
   return NULL;
}

static int64
find_matching_bundles(in_memory_node *target, in_memory_node *src)
{
   // Due to the always-flush-all-bundles rule, we need only find a match for
   // the first new bundle in src.  We are guaranteed that the rest of the new
   // bundles will be in the target, as well.

   in_memory_inflight_bundle *needle =
      vector_get_ptr(&src->inflight_bundles, src->num_old_bundles);

   for (int64 i = 0; i < vector_length(&target->inflight_bundles); i++) {
      if (in_memory_inflight_bundles_equal(
             needle, vector_get_ptr(&target->inflight_bundles, i)))
      {
         return i;
      }
   }
   return -1;
}

static platform_status
apply_bundle_compaction(trunk_node_context *context,
                        uint64              addr,
                        in_memory_node     *target,
                        void               *arg)
{
   platform_status         rc;
   bundle_compaction_args *args = (bundle_compaction_args *)arg;
   in_memory_node         *src  = &args->node;

   // If this is a leaf and it has split, bail out.
   if (in_memory_node_is_leaf(target)
       && (data_key_compare(context->cfg->data_cfg,
                            in_memory_node_pivot_min_key(target),
                            in_memory_node_pivot_min_key(src))
              != 0
           || data_key_compare(context->cfg->data_cfg,
                               in_memory_node_pivot_max_key(target),
                               in_memory_node_pivot_max_key(src))
                 != 0))
   {
      return STATUS_OK;
   }

   // Find where these compacted bundles are currently located in the target.
   uint64 bundle_match_offset = find_matching_bundles(target, src);
   if (bundle_match_offset == -1) {
      // They've already been flushed to all children.  Nothing to do.
      return STATUS_OK;
   }

   uint64 src_num_children = in_memory_node_num_children(src);
   uint64 tgt_num_children = in_memory_node_num_children(target);


   // Set up the branch vector for the per-child bundle we will be building.
   branch_ref_vector branches;
   vector_init(&branches, context->hid);
   rc = vector_ensure_capacity(&branches, tgt_num_children);
   if (!SUCCESS(rc)) {
      vector_deinit(&branches);
      return rc;
   }

   // For each child in the target, find the corresponding child in the source
   uint64 src_child_num = 0;
   for (uint64 tgt_child_num = 0; tgt_child_num < tgt_num_children;
        tgt_child_num++)
   {
      key              src_lbkey = in_memory_node_pivot_key(src, src_child_num);
      in_memory_pivot *pivot     = in_memory_node_pivot(target, tgt_child_num);
      key              tgt_lbkey = in_memory_pivot_key(pivot);
      uint64 inflight_start      = in_memory_pivot_inflight_bundle_start(pivot);

      while (src_child_num < src_num_children
             && data_key_compare(context->cfg->data_cfg, src_lbkey, tgt_lbkey)
                   < 0)
      {
         src_child_num++;
         // Note that it is safe to do the following lookup because there is
         // always one more pivot that the number of children
         src_lbkey = in_memory_node_pivot_key(src, src_child_num);
      }

      if (src_child_num < src_num_children
          && data_key_compare(context->cfg->data_cfg, src_lbkey, tgt_lbkey) == 0
          && inflight_start <= bundle_match_offset)
      {
         // We found a match.  Add this compaction result to the branch vector
         // of the per-child bundle.
         branch_ref bref =
            create_branch_ref(args->pack_reqs[src_child_num].root_addr);
         rc = vector_append(&branches, bref);
         platform_assert_status_ok(rc);

         // Remember that we installed this branch so we can add an input for it
         // to the maplet_compaction_input_tracker later
         rc = vector_append(&args->installed_branch_indexes, src_child_num);
         platform_assert_status_ok(rc);

         // Compute the tuple accounting delta that will occur when we replace
         // the input branches with the compacted branch.
         trunk_pivot_stats stats_decrease =
            in_memory_pivot_received_bundles_stats(
               in_memory_node_pivot(src, src_child_num));
         in_memory_pivot_add_tuple_counts(pivot, -1, stats_decrease);

         if (inflight_start == bundle_match_offset) {
            // After we replace the input branches with the compacted branch,
            // this pivot will be eligible for maplet compaction, so record that
            // fact so we can enqueue a maplet compaction task after we finish
            // applying the results of this bundle compaction.  All we need to
            // remember is the index of this match in the src node.
            maplet_compaction_args *mc_args;
            mc_args =
               maplet_compaction_args_create(context, target, tgt_child_num);
            if (mc_args == NULL) {
               vector_deinit(&branches);
               return STATUS_NO_MEMORY;
            }
            rc = vector_append(&args->maplet_compaction_args, mc_args);
            platform_assert_status_ok(rc);
         }
      } else {
         // No match -- the input bundles have already been flushed to the
         // child, so add a NULL branch to the per-child bundle.
         rc = vector_append(&branches, NULL_BRANCH_REF);
         platform_assert_status_ok(rc);
      }
   }

   // Build the per-child bundle from the compacted branches we've collected and
   // the maplets from the input bundles
   uint64 num_bundles =
      vector_length(&args->node.inflight_bundles) - args->node.num_old_bundles;
   in_memory_inflight_bundle result_bundle;
   rc = in_memory_inflight_bundle_init_per_child_from_compaction(
      &result_bundle,
      context->hid,
      &target->inflight_bundles,
      bundle_match_offset,
      bundle_match_offset + num_bundles,
      &branches);
   if (!SUCCESS(rc)) {
      vector_deinit(&branches);
      return rc;
   }

   // Replace the input bundles with the new per-child bundle
   for (uint64 i = bundle_match_offset; i < bundle_match_offset + num_bundles;
        i++) {
      in_memory_inflight_bundle_deinit(
         vector_get_ptr(&target->inflight_bundles, i));
   }
   rc = vector_replace(&target->inflight_bundles,
                       bundle_match_offset,
                       num_bundles,
                       &target->inflight_bundles,
                       bundle_match_offset,
                       1);
   platform_assert_status_ok(rc);
   vector_set(&target->inflight_bundles, bundle_match_offset, result_bundle);

   // Adust all the pivots' inflight bundle start offsets
   for (uint64 i = 0; i < in_memory_node_num_children(target); i++) {
      in_memory_pivot *pivot    = in_memory_node_pivot(target, i);
      uint64 pivot_bundle_start = in_memory_pivot_inflight_bundle_start(pivot);
      if (bundle_match_offset < pivot_bundle_start) {
         debug_assert(bundle_match_offset + num_bundles <= pivot_bundle_start);
         in_memory_pivot_set_inflight_bundle_start(
            pivot, pivot_bundle_start - num_bundles + 1);
      }
   }

   return STATUS_OK;
}

static void
bundle_compaction_task(void *arg, void *scratch)
{
   platform_status         rc;
   bundle_compaction_args *args = (bundle_compaction_args *)arg;

   uint64 num_children = in_memory_node_num_children(&args->node);
   uint64 my_child_num = __sync_fetch_and_add(&args->next_child, 1);

   rc = btree_pack(&args->pack_reqs[my_child_num]);
   if (!SUCCESS(rc)) {
      args->failed = TRUE;
   }

   if (__sync_add_and_fetch(&args->completed_compactions, 1) != num_children) {
      return;
   }

   // We are the last btree_pack to finish, so it is our responsibility to apply
   // the changes and enqueue maplet compactions.

   if (args->failed) {
      // Someboday failed to perform their btree_pack, so we have to abandon the
      // whole thing.
      goto cleanup;
   }

   apply_changes_begin(args->context);
   rc = apply_changes(args->context,
                      in_memory_node_pivot_min_key(&args->node),
                      in_memory_node_pivot_max_key(&args->node),
                      in_memory_node_height(&args->node),
                      apply_bundle_compaction,
                      arg);
   if (!SUCCESS(rc)) {
      apply_changes_end(args->context);
      goto cleanup;
   }

   // Add all the maplet_compaction_inputs to the global input tracker
   for (uint64 i = 0; i < vector_length(&args->installed_branch_indexes); i++) {
      maplet_compaction_input input;
      uint64 index           = vector_get(&args->installed_branch_indexes, i);
      input.fingerprints     = args->pack_reqs[index].fingerprint_arr;
      input.num_fingerprints = args->pack_reqs[index].num_tuples;
      rc                     = maplet_compaction_tracker_add_input(
         &args->context->maplet_compaction_inputs,
         args->mergers[index].min_key,
         in_memory_node_height(&args->node),
         input);
      if (!SUCCESS(rc)) {
         apply_changes_end(args->context);
         goto cleanup;
      }
      args->pack_reqs[index].fingerprint_arr = NULL;
   }

   apply_changes_end(args->context);

   // Enqueue maplet compactions
   for (uint64 compaction_num = 0;
        compaction_num < vector_length(&args->maplet_compaction_args);
        compaction_num++)
   {
      maplet_compaction_args *mc_args =
         vector_get(&args->maplet_compaction_args, compaction_num);
      rc = enqueue_maplet_compaction(mc_args);
      if (SUCCESS(rc)) {
         // Remove the maplet_compaction_args from the vector so we don't
         // destroy it in cleanup
         vector_set(&args->maplet_compaction_args, compaction_num, NULL);
      } else {
         // Remove all the maplet_compaction_inputs for maplet compactions that
         // aren't going to happen.

         for (uint64 i = 0; i < vector_length(&mc_args->branches); i++) {
            branch_ref              bref = vector_get(&mc_args->branches, i);
            maplet_compaction_input input;
            maplet_compaction_input_tracker_get(
               &args->context->maplet_compaction_inputs, bref, &input);
         }
      }
   }

cleanup:
   in_memory_node_deinit(&args->node, args->context);
   on_disk_node_dec_ref(args->context, args->addr);
   bundle_compaction_args_destroy(args);
}

static platform_status
enqueue_bundle_compaction(trunk_node_context *context,
                          uint64              addr,
                          in_memory_node     *node)
{
   bundle_compaction_args *args =
      bundle_compaction_args_create(context, addr, node);
   if (args == NULL) {
      return STATUS_NO_MEMORY;
   }

   on_disk_node_inc_ref(context, addr);

   platform_status rc           = STATUS_OK;
   uint64          num_children = in_memory_node_num_children(node);
   uint64          enqueued_compactions;
   for (enqueued_compactions = 0; enqueued_compactions < num_children;
        enqueued_compactions++)
   {
      if (!in_memory_node_pivot_has_received_bundles(node,
                                                     enqueued_compactions)) {
         uint64 num_completed =
            __sync_fetch_and_add(&args->completed_compactions, 1);
         if (num_completed == num_children) {
            goto cleanup;
         }
         continue;
      }

      rc = task_enqueue(
         context->ts, TASK_TYPE_NORMAL, bundle_compaction_task, args, FALSE);
      if (!SUCCESS(rc)) {
         break;
      }
   }

   if (!SUCCESS(rc)) {
      args->failed         = TRUE;
      uint64 num_completed = __sync_fetch_and_add(
         &args->completed_compactions, num_children - enqueued_compactions);
      if (num_completed == num_children) {
         goto cleanup;
      }
   }

   return rc;

cleanup:
   on_disk_node_dec_ref(context, addr);
   bundle_compaction_args_destroy(args);
   return rc;
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

/*****************************************************
 * Receive bundles -- used in flushes and leaf splits
 *****************************************************/

typedef struct maplet_compaction_cancellation {
   key_buffer pivot;
   uint64     height;
} maplet_compaction_cancellation;

platform_status
maplet_compaction_cancellation_init(
   maplet_compaction_cancellation *cancellation,
   trunk_node_context             *context,
   key                             pivot,
   uint64                          height)
{
   platform_status rc;

   rc = key_buffer_init_from_key(&cancellation->pivot, context->hid, pivot);
   if (!SUCCESS(rc)) {
      return rc;
   }

   cancellation->height = height;

   return STATUS_OK;
}

void
maplet_compaction_cancellation_deinit(
   maplet_compaction_cancellation *cancellation)
{
   key_buffer_deinit(&cancellation->pivot);
}

typedef VECTOR(maplet_compaction_cancellation)
   maplet_compaction_cancellation_vector;

static platform_status
in_memory_node_receive_bundles(trunk_node_context               *context,
                               in_memory_node                   *node,
                               in_memory_routed_bundle          *routed,
                               in_memory_inflight_bundle_vector *inflight,
                               uint64                            inflight_start,
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
      in_memory_inflight_bundle *bundle = vector_get_ptr(inflight, i);
      rc = VECTOR_EMPLACE_APPEND(&node->inflight_bundles,
                                 in_memory_inflight_bundle_init_from_flush,
                                 context->hid,
                                 bundle,
                                 child_num);
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
                          key                 max_key,
                          branch_ref_vector  *cancelled_maplet_compactions)
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
                     in_memory_node_vector *new_leaves,
                     branch_ref_vector     *cancelled_maplet_compactions)
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
                                 max_key,
                                 cancelled_maplet_compactions);
      if (!SUCCESS(rc)) {
         goto cleanup_new_leaves;
      }
   }

   rc = VECTOR_EMPLACE_APPEND(cancelled_maplet_compactions,
                              maplet_compaction_cancellation_init,
                              context,
                              in_memory_node_pivot_min_key(leaf),
                              in_memory_node_height(leaf));

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
                       in_memory_node_num_old_bundles(index),
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
restore_balance_leaf(
   trunk_node_context                    *context,
   in_memory_node                        *leaf,
   in_memory_node_vector                 *new_leaves,
   maplet_compaction_cancellation_vector *cancelled_maplet_compactions)
{
   return in_memory_leaf_split(
      context, leaf, new_leaves, cancelled_maplet_compactions);
}

static platform_status
flush_then_compact(
   trunk_node_context                    *context,
   in_memory_node                        *node,
   in_memory_routed_bundle               *routed,
   in_memory_inflight_bundle_vector      *inflight,
   uint64                                 inflight_start,
   uint64                                 child_num,
   in_memory_node_vector                 *new_nodes,
   maplet_compaction_cancellation_vector *cancelled_maplet_compactions);

static platform_status
restore_balance_index(
   trunk_node_context                    *context,
   in_memory_node                        *index,
   in_memory_node_vector                 *new_indexes,
   maplet_compaction_cancellation_vector *cancelled_maplet_compactions)
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
                  &new_children,
                  cancelled_maplet_compactions);
               if (!SUCCESS(rc)) {
                  in_memory_node_deinit(&child, context);
                  vector_deinit(&new_children);
                  return rc;
               }

               rc = VECTOR_EMPLACE_APPEND(cancelled_maplet_compactions,
                                          maplet_compaction_cancellation_init,
                                          context,
                                          in_memory_pivot_key(pivot),
                                          in_memory_node_height(index));
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
flush_then_compact(
   trunk_node_context                    *context,
   in_memory_node                        *node,
   in_memory_routed_bundle               *routed,
   in_memory_inflight_bundle_vector      *inflight,
   uint64                                 inflight_start,
   uint64                                 child_num,
   in_memory_node_vector                 *new_nodes,
   maplet_compaction_cancellation_vector *cancelled_maplet_compactions)
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
      rc = restore_balance_leaf(
         context, node, new_nodes, cancelled_maplet_compactions);
   } else {
      rc = restore_balance_index(
         context, node, new_nodes, cancelled_maplet_compactions);
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
   in_memory_inflight_bundle_vector inflight;
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
incorporate(trunk_node_context                    *context,
            routing_filter                         filter,
            branch_ref                             branch,
            uint64                                *new_root_addr,
            maplet_compaction_cancellation_vector *cancelled_maplet_compactions)
{
   platform_status rc;

   in_memory_inflight_bundle_vector inflight;
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
                           0,
                           &new_nodes,
                           cancelled_maplet_compactions);
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
   VECTOR_APPLY_TO_PTRS(&inflight, in_memory_inflight_bundle_deinit);
   vector_deinit(&inflight);

   return rc;
}