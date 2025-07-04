// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * trunk.c --
 *
 *     This file contains the implementation of the SplinterDB trunk.
 */

#include "trunk.h"
#include "platform.h"
#include "platform_types.h"
#include "data_internal.h"
#include "util.h"
#include "btree.h"
#include "routing_filter.h"
#include "vector.h"
#include "merge.h"
#include "data_internal.h"
#include "task.h"
#include "poison.h"

typedef VECTOR(routing_filter) routing_filter_vector;

struct ONDISK trunk_ondisk_bundle {
   routing_filter maplet;
   uint16         num_branches;
   // branches[0] is the oldest branch
   branch_ref branches[];
};

typedef struct ONDISK trunk_pivot_stats {
   int64 num_kv_bytes;
   int64 num_tuples;
} trunk_pivot_stats;

struct trunk_pivot {
   trunk_pivot_stats prereceive_stats;
   trunk_pivot_stats stats;
   uint64            child_addr;
   // Index of the oldest bundle that is live for this pivot
   uint64     inflight_bundle_start;
   ondisk_key key;
};

typedef VECTOR(trunk_ondisk_node_ref *) trunk_ondisk_node_ref_vector;

struct ONDISK trunk_ondisk_pivot {
   trunk_pivot_stats stats;
   uint64            child_addr;
   uint64            num_live_inflight_bundles;
   ondisk_key        key;
};

typedef struct ONDISK trunk_ondisk_node {
   uint16 height;
   uint16 num_pivots;
   // On disk, inflight bundles are ordered from newest to oldest.
   uint16 num_inflight_bundles;
   uint32 inflight_bundles_offset;
   uint32 pivot_offsets[];
} trunk_ondisk_node;

typedef enum bundle_compaction_state {
   BUNDLE_COMPACTION_NOT_STARTED = 0,
   BUNDLE_COMPACTION_IN_PROGRESS = 1,
   BUNDLE_COMPACTION_MIN_ENDED   = 2,
   BUNDLE_COMPACTION_FAILED      = 2,
   BUNDLE_COMPACTION_SUCCEEDED   = 3
} bundle_compaction_state;

typedef VECTOR(trunk_branch_info) trunk_branch_info_vector;

typedef struct bundle_compaction {
   struct bundle_compaction *next;
   uint64                    num_bundles;
   trunk_pivot_stats         input_stats;
   bundle_compaction_state   state;
   trunk_branch_info_vector  input_branches;
   merge_behavior            merge_mode;
   branch_ref                output_branch;
   trunk_pivot_stats         output_stats;
   uint32                   *fingerprints;
   uint64                    compaction_time_ns;
} bundle_compaction;

typedef struct trunk_context trunk_context;

struct trunk_pivot_state {
   struct trunk_pivot_state *next;
   uint64                    refcount;
   bool32                    maplet_compaction_initiated;
   bool32                    abandoned;
   trunk_context            *context;
   key_buffer                key;
   key_buffer                ubkey;
   uint64                    height;
   routing_filter            maplet;
   uint64                    num_branches;
   bool32                    maplet_compaction_failed;
   uint64                    total_bundles;
   platform_spinlock         compactions_lock;
   bundle_compaction        *bundle_compactions;
};

struct pending_gc {
   pending_gc *next;
   uint64      addr;
};

/***************************************************
 * branch_ref operations
 ***************************************************/

static branch_ref
create_branch_ref(uint64 addr)
{
   return (branch_ref){.addr = addr};
}

static uint64
branch_ref_addr(branch_ref bref)
{
   return bref.addr;
}

#define NULL_BRANCH_REF ((branch_ref){.addr = 0})

static bool32
branch_is_null(branch_ref bref)
{
   return bref.addr == 0;
}

/**************************
 * routed_bundle operations
 **************************/

static void
bundle_init(bundle *bndl, platform_heap_id hid)
{
   bndl->maplet = NULL_ROUTING_FILTER;
   vector_init(&bndl->branches, hid);
}

static platform_status
bundle_init_single(bundle          *bndl,
                   platform_heap_id hid,
                   routing_filter   maplet,
                   branch_ref       branch)
{
   bndl->maplet = maplet;
   vector_init(&bndl->branches, hid);
   platform_status rc = vector_append(&bndl->branches, branch);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: vector_append() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      vector_deinit(&bndl->branches);
   }
   return rc;
}

static platform_status
bundle_init_copy(bundle *dst, const bundle *src, platform_heap_id hid)
{
   vector_init(&dst->branches, hid);
   platform_status rc = vector_copy(&dst->branches, &src->branches);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: vector_copy() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      vector_deinit(&dst->branches);
      return rc;
   }
   dst->maplet = src->maplet;

   return rc;
}

static void
bundle_deinit(bundle *bndl)
{
   vector_deinit(&bndl->branches);
}

static platform_status
bundle_add_branches(bundle            *bndl,
                    routing_filter     new_maplet,
                    branch_ref_vector *new_branches)
{
   platform_status rc;
   rc = vector_append_vector(&bndl->branches, new_branches);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: vector_append_vector() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      return rc;
   }
   bndl->maplet = new_maplet;

   return STATUS_OK;
}

static routing_filter
bundle_maplet(const bundle *bndl)
{
   return bndl->maplet;
}

static uint64
bundle_num_branches(const bundle *bndl)
{
   return vector_length(&bndl->branches);
}

static branch_ref
bundle_branch(const bundle *bndl, uint64 i)
{
   return vector_get(&bndl->branches, i);
}

static const branch_ref *
bundle_branch_array(const bundle *bndl)
{
   return vector_data(&bndl->branches);
}

static page_type
bundle_branch_type(const bundle *bndl)
{
   platform_assert(!routing_filters_equal(&bndl->maplet, &NULL_ROUTING_FILTER)
                   || bundle_num_branches(bndl) <= 1);
   if (routing_filters_equal(&bndl->maplet, &NULL_ROUTING_FILTER)
       && bundle_num_branches(bndl) == 1)
   {
      return PAGE_TYPE_BRANCH;
   } else {
      return PAGE_TYPE_BRANCH;
   }
}

debug_only static void
bundle_print(const bundle *bndl, platform_log_handle *log, int indent)
{
   platform_log(
      log, "%*sBundle(maplet: %lu, branches: ", indent, "", bndl->maplet.addr);
   for (uint64 i = 0; i < bundle_num_branches(bndl); i++) {
      platform_log(log, "%lu ", branch_ref_addr(bundle_branch_array(bndl)[i]));
   }
   platform_log(log, ")\n");
}

debug_only static void
bundle_vector_print(const bundle_vector *bv,
                    platform_log_handle *log,
                    int                  indent)
{
   platform_log(
      log, "%*s%3s %12s    %-12s\n", indent, "", "i", "maplet", "branches");
   for (uint64 i = 0; i < vector_length(bv); i++) {
      const bundle *bndl = vector_get_ptr(bv, i);
      platform_log(
         log, "%*s%3lu %12lu    ", indent, "", i, bundle_maplet(bndl).addr);
      for (uint64 j = 0; j < bundle_num_branches(bndl); j++) {
         platform_log(
            log, "%lu ", branch_ref_addr(bundle_branch_array(bndl)[j]));
      }
      platform_log(log, "\n");
   }
}

/********************
 * Pivot stats
 ********************/

static trunk_pivot_stats
trunk_pivot_stats_from_btree_pivot_stats(btree_pivot_stats stats)
{
   return (trunk_pivot_stats){.num_kv_bytes =
                                 stats.key_bytes + stats.message_bytes,
                              .num_tuples = stats.num_kvs};
}

static trunk_pivot_stats
trunk_pivot_stats_subtract(trunk_pivot_stats a, trunk_pivot_stats b)
{
   return (trunk_pivot_stats){.num_kv_bytes = a.num_kv_bytes - b.num_kv_bytes,
                              .num_tuples   = a.num_tuples - b.num_tuples};
}

static trunk_pivot_stats
trunk_pivot_stats_add(trunk_pivot_stats a, trunk_pivot_stats b)
{
   return (trunk_pivot_stats){.num_kv_bytes = a.num_kv_bytes + b.num_kv_bytes,
                              .num_tuples   = a.num_tuples + b.num_tuples};
}

static bool32
trunk_pivot_stats_are_nonnegative(trunk_pivot_stats stats)
{
   return stats.num_kv_bytes >= 0 && stats.num_tuples >= 0;
}

/******************
 * pivot operations
 ******************/

#define TRUNK_STATS_ZERO                                                       \
   ((trunk_pivot_stats){.num_kv_bytes = 0, .num_tuples = 0})

static trunk_pivot *
trunk_pivot_create(platform_heap_id  hid,
                   key               k,
                   uint64            child_addr,
                   uint64            inflight_bundle_start,
                   trunk_pivot_stats prereceive_stats,
                   trunk_pivot_stats stats)
{
   trunk_pivot *result = TYPED_FLEXIBLE_STRUCT_ZALLOC(
      hid, result, key.bytes, ondisk_key_required_data_capacity(k));
   if (result == NULL) {
      platform_error_log(
         "%s():%d: TYPED_FLEXIBLE_STRUCT_ZALLOC() failed", __func__, __LINE__);
      return NULL;
   }
   copy_key_to_ondisk_key(&result->key, k);
   result->child_addr            = child_addr;
   result->inflight_bundle_start = inflight_bundle_start;
   platform_assert(trunk_pivot_stats_are_nonnegative(prereceive_stats));
   platform_assert(trunk_pivot_stats_are_nonnegative(stats));
   result->prereceive_stats = prereceive_stats;
   result->stats            = stats;
   return result;
}

static trunk_pivot *
trunk_pivot_copy(const trunk_pivot *src, platform_heap_id hid)
{
   return trunk_pivot_create(hid,
                             ondisk_key_to_key(&src->key),
                             src->child_addr,
                             src->inflight_bundle_start,
                             src->prereceive_stats,
                             src->stats);
}

static void
trunk_pivot_destroy(trunk_pivot *pvt, platform_heap_id hid)
{
   platform_free(hid, pvt);
}

static key
trunk_pivot_key(const trunk_pivot *pvt)
{
   return ondisk_key_to_key(&pvt->key);
}

static uint64
trunk_pivot_child_addr(const trunk_pivot *pvt)
{
   return pvt->child_addr;
}

static void
trunk_pivot_set_child_addr(trunk_pivot *pvt, uint64 new_child_addr)
{
   pvt->child_addr = new_child_addr;
}

static trunk_pivot_stats
trunk_pivot_get_stats(const trunk_pivot *pvt)
{
   return pvt->stats;
}

static uint64
trunk_pivot_inflight_bundle_start(const trunk_pivot *pvt)
{
   return pvt->inflight_bundle_start;
}

static void
trunk_pivot_set_inflight_bundle_start(trunk_pivot *pvt, uint64 start)
{
   pvt->inflight_bundle_start = start;
}

static trunk_pivot_stats
trunk_pivot_received_bundles_stats(const trunk_pivot *pvt)
{
   trunk_pivot_stats result =
      trunk_pivot_stats_subtract(pvt->stats, pvt->prereceive_stats);
   platform_assert(trunk_pivot_stats_are_nonnegative(result));
   return result;
}

static uint64
trunk_pivot_num_kv_bytes(const trunk_pivot *pvt)
{
   return pvt->stats.num_kv_bytes;
}

/*
 * When new bundles get flushed to this pivot's node, you must
 * inform the pivot of the tuple counts of the new bundles.
 */
static void
trunk_pivot_add_tuple_counts(trunk_pivot      *pvt,
                             int               coefficient,
                             trunk_pivot_stats stats)
{
   if (coefficient == 1) {
      pvt->stats.num_tuples += stats.num_tuples;
      pvt->stats.num_kv_bytes += stats.num_kv_bytes;
   } else if (coefficient == -1) {
      platform_assert(stats.num_tuples <= pvt->stats.num_tuples);
      platform_assert(stats.num_kv_bytes <= pvt->stats.num_kv_bytes);
      pvt->stats.num_tuples -= stats.num_tuples;
      pvt->stats.num_kv_bytes -= stats.num_kv_bytes;
   } else {
      platform_assert(0);
   }
   platform_assert(trunk_pivot_stats_are_nonnegative(pvt->stats));
}

debug_only static void
trunk_pivot_print(const trunk_pivot   *pvt,
                  platform_log_handle *log,
                  const data_config   *data_cfg,
                  int                  indent)
{
   platform_log(
      log,
      "%*sPivot(pr_kvbytes: %lu pr_tuples: %lu kvbytes: %lu tuples: %lu "
      "child: %lu ifstart: %lu %s)\n",
      indent,
      "",
      pvt->prereceive_stats.num_kv_bytes,
      pvt->prereceive_stats.num_tuples,
      pvt->stats.num_kv_bytes,
      pvt->stats.num_tuples,
      pvt->child_addr,
      pvt->inflight_bundle_start,
      key_string(data_cfg, trunk_pivot_key(pvt)));
}

debug_only static void
trunk_pivot_vector_print(const trunk_pivot_vector *pivots,
                         platform_log_handle      *log,
                         const data_config        *data_cfg,
                         int                       indent)
{
   platform_log(log,
                "%*s%3s %12s %12s %12s %12s %12s %12s %-24s\n",
                indent,
                "",
                "i",
                "pr_kvbytes",
                "pr_tuples",
                "kvbytes",
                "tuples",
                "child_addr",
                "if_start",
                "key");
   for (uint64 i = 0; i < vector_length(pivots); i++) {
      trunk_pivot *pvt = vector_get(pivots, i);
      platform_log(log,
                   "%*s%3lu %12lu %12lu %12lu %12lu %12lu %12lu %-24s\n",
                   indent,
                   "",
                   i,
                   pvt->prereceive_stats.num_kv_bytes,
                   pvt->prereceive_stats.num_tuples,
                   pvt->stats.num_kv_bytes,
                   pvt->stats.num_tuples,
                   pvt->child_addr,
                   pvt->inflight_bundle_start,
                   key_string(data_cfg, trunk_pivot_key(pvt)));
   }
}

/***********************
 * basic node operations
 ***********************/

/* Steals pivots, pivot_bundles, and inflight_bundles. */
static void
trunk_node_init(trunk_node        *node,
                uint16             height,
                trunk_pivot_vector pivots,
                bundle_vector      pivot_bundles,
                uint64             num_old_bundles,
                bundle_vector      inflight_bundles)
{
   node->height           = height;
   node->pivots           = pivots;
   node->pivot_bundles    = pivot_bundles;
   node->num_old_bundles  = num_old_bundles;
   node->inflight_bundles = inflight_bundles;
}

static platform_status
trunk_node_copy_init(trunk_node       *dst,
                     const trunk_node *src,
                     platform_heap_id  hid)
{
   trunk_pivot_vector pivots;
   bundle_vector      pivot_bundles;
   bundle_vector      inflight_bundles;
   platform_status    rc;

   vector_init(&pivots, hid);
   vector_init(&pivot_bundles, hid);
   vector_init(&inflight_bundles, hid);

   rc = VECTOR_MAP_ELTS(&pivots, trunk_pivot_copy, &src->pivots, hid);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: VECTOR_MAP_ELTS() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      goto cleanup_vectors;
   }
   rc = VECTOR_EMPLACE_MAP_PTRS(
      &pivot_bundles, bundle_init_copy, &src->pivot_bundles, hid);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: VECTOR_EMPLACE_MAP_PTRS() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      goto cleanup_vectors;
   }
   rc = VECTOR_EMPLACE_MAP_PTRS(
      &inflight_bundles, bundle_init_copy, &src->inflight_bundles, hid);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: VECTOR_EMPLACE_MAP_PTRS() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      goto cleanup_vectors;
   }

   trunk_node_init(dst,
                   src->height,
                   pivots,
                   pivot_bundles,
                   src->num_old_bundles,
                   inflight_bundles);
   return STATUS_OK;

cleanup_vectors:
   VECTOR_APPLY_TO_ELTS(&pivots, trunk_pivot_destroy, hid);
   vector_deinit(&pivots);
   VECTOR_APPLY_TO_PTRS(&pivot_bundles, bundle_deinit);
   vector_deinit(&pivot_bundles);
   VECTOR_APPLY_TO_PTRS(&inflight_bundles, bundle_deinit);
   vector_deinit(&inflight_bundles);
   return rc;
}

static platform_status
trunk_node_init_empty_leaf(trunk_node      *node,
                           platform_heap_id hid,
                           key              lb,
                           key              ub)
{
   trunk_pivot_vector pivots;
   bundle_vector      pivot_bundles;
   bundle_vector      inflight_bundles;
   platform_status    rc;

   vector_init(&pivots, hid);
   vector_init(&pivot_bundles, hid);
   vector_init(&inflight_bundles, hid);

   rc = vector_ensure_capacity(&pivots, 2);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: vector_ensure_capacity() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      goto cleanup_vectors;
   }

   rc = vector_ensure_capacity(&pivot_bundles, 1);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: vector_ensure_capacity() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      goto cleanup_vectors;
   }

   trunk_pivot *lb_pivot =
      trunk_pivot_create(hid, lb, 0, 0, TRUNK_STATS_ZERO, TRUNK_STATS_ZERO);
   trunk_pivot *ub_pivot =
      trunk_pivot_create(hid, ub, 0, 0, TRUNK_STATS_ZERO, TRUNK_STATS_ZERO);
   if (lb_pivot == NULL || ub_pivot == NULL) {
      platform_error_log(
         "%s():%d: pivot_create() failed. lb_pivot=%p ub_pivot=%p",
         __func__,
         __LINE__,
         lb_pivot,
         ub_pivot);
      rc = STATUS_NO_MEMORY;
      goto cleanup_pivots;
   }
   rc = vector_append(&pivots, lb_pivot);
   platform_assert_status_ok(rc);
   rc = vector_append(&pivots, ub_pivot);
   platform_assert_status_ok(rc);

   rc = VECTOR_EMPLACE_APPEND(&pivot_bundles, bundle_init, hid);
   platform_assert_status_ok(rc);

   trunk_node_init(node, 0, pivots, pivot_bundles, 0, inflight_bundles);
   return STATUS_OK;

cleanup_pivots:
   if (lb_pivot != NULL) {
      trunk_pivot_destroy(lb_pivot, hid);
   }
   if (ub_pivot != NULL) {
      trunk_pivot_destroy(ub_pivot, hid);
   }
cleanup_vectors:
   VECTOR_APPLY_TO_ELTS(&pivots, trunk_pivot_destroy, hid);
   vector_deinit(&pivots);
   VECTOR_APPLY_TO_PTRS(&pivot_bundles, bundle_deinit);
   vector_deinit(&pivot_bundles);
   vector_deinit(&inflight_bundles);
   return rc;
}

static uint64
trunk_node_num_children(const trunk_node *node)
{
   return vector_length(&node->pivots) - 1;
}

static trunk_pivot *
trunk_node_pivot(const trunk_node *node, uint64 i)
{
   return vector_get(&node->pivots, i);
}

static key
trunk_node_pivot_key(const trunk_node *node, uint64 i)
{
   return trunk_pivot_key(vector_get(&node->pivots, i));
}

static key
trunk_node_pivot_min_key(const trunk_node *node)
{
   return trunk_pivot_key(vector_get(&node->pivots, 0));
}

debug_only static key
trunk_node_pivot_max_key(const trunk_node *node)
{
   return trunk_pivot_key(
      vector_get(&node->pivots, vector_length(&node->pivots) - 1));
}

static bundle *
trunk_node_pivot_bundle(trunk_node *node, uint64 i)
{
   return vector_get_ptr(&node->pivot_bundles, i);
}

static uint64
trunk_node_height(const trunk_node *node)
{
   return node->height;
}

static bool32
trunk_node_is_leaf(const trunk_node *node)
{
   return node->height == 0;
}

static uint64
trunk_node_first_live_inflight_bundle(const trunk_node *node)
{
   uint64 result = UINT64_MAX;
   for (uint64 i = 0; i < vector_length(&node->pivots) - 1; i++) {
      trunk_pivot *pvt = vector_get(&node->pivots, i);
      result           = MIN(result, pvt->inflight_bundle_start);
   }
   return result;
}

static uint64
trunk_leaf_num_tuples(const trunk_node *node)
{
   trunk_pivot_stats stats =
      trunk_pivot_get_stats(vector_get(&node->pivots, 0));
   return stats.num_tuples;
}

static uint64
trunk_leaf_num_kv_bytes(const trunk_node *node)
{
   trunk_pivot_stats stats =
      trunk_pivot_get_stats(vector_get(&node->pivots, 0));
   return stats.num_kv_bytes;
}

static uint64
trunk_node_num_old_bundles(const trunk_node *node)
{
   return node->num_old_bundles;
}

static bool32
trunk_node_pivot_has_received_bundles(const trunk_node *node, uint64 i)
{
   trunk_pivot *pvt = vector_get(&node->pivots, i);
   return trunk_pivot_inflight_bundle_start(pvt) <= node->num_old_bundles
          && node->num_old_bundles < vector_length(&node->inflight_bundles);
}

void
trunk_node_print(const trunk_node    *node,
                 platform_log_handle *log,
                 const data_config   *data_cfg,
                 int                  indent)
{
   platform_log(
      log, "%*sNode height: %lu\n", indent, "", trunk_node_height(node));
   platform_log(
      log, "%*sNum old bundles: %lu\n", indent, "", node->num_old_bundles);

   platform_log(log, "%*s--------------Pivots-----------\n", indent, "");
   trunk_pivot_vector_print(&node->pivots, log, data_cfg, indent + 4);

   platform_log(log, "%*s--------------Pivot Bundles-----------\n", indent, "");
   bundle_vector_print(&node->pivot_bundles, log, indent + 4);

   platform_log(
      log, "%*s--------------Inflight Bundles-----------\n", indent, "");
   bundle_vector_print(&node->inflight_bundles, log, indent + 4);
}

debug_only static bool
trunk_node_is_well_formed_leaf(const data_config *data_cfg,
                               const trunk_node  *node)
{
   bool basics =
      node->height == 0 && vector_length(&node->pivots) == 2
      && vector_length(&node->pivot_bundles) == 1
      && node->num_old_bundles <= vector_length(&node->inflight_bundles);
   if (!basics) {
      platform_error_log("ILL-FORMED LEAF: basics failed\n");
      trunk_node_print(node, Platform_error_log_handle, data_cfg, 4);
      return FALSE;
   }

   trunk_pivot *lb    = vector_get(&node->pivots, 0);
   trunk_pivot *ub    = vector_get(&node->pivots, 1);
   key          lbkey = trunk_pivot_key(lb);
   key          ubkey = trunk_pivot_key(ub);
   bool32       ret =
      lb->child_addr == 0 && data_key_compare(data_cfg, lbkey, ubkey) < 0;
   if (!ret) {
      platform_error_log("ILL-FORMED LEAF:\n");
      trunk_node_print(node, Platform_error_log_handle, data_cfg, 4);
   }
   return ret;
}

debug_only static bool
trunk_node_is_well_formed_index(const data_config *data_cfg,
                                const trunk_node  *node)
{
   bool basics =
      0 < node->height && 1 < vector_length(&node->pivots)
      && vector_length(&node->pivot_bundles) == vector_length(&node->pivots) - 1
      && node->num_old_bundles <= vector_length(&node->inflight_bundles);
   if (!basics) {
      platform_error_log("ILL-FORMED INDEX: basics failed\n");
      trunk_node_print(node, Platform_error_log_handle, data_cfg, 4);
      return FALSE;
   }

   for (uint64 i = 0; i < trunk_node_num_children(node); i++) {
      trunk_pivot *lb    = vector_get(&node->pivots, i);
      trunk_pivot *ub    = vector_get(&node->pivots, i + 1);
      key          lbkey = trunk_pivot_key(lb);
      key          ubkey = trunk_pivot_key(ub);
      bool         valid_pivots =
         lb->child_addr != 0
         && lb->inflight_bundle_start <= vector_length(&node->inflight_bundles)
         && data_key_compare(data_cfg, lbkey, ubkey) < 0
         && trunk_pivot_stats_are_nonnegative(lb->prereceive_stats)
         && trunk_pivot_stats_are_nonnegative(lb->stats);
      if (!valid_pivots) {
         platform_error_log("ILL-FORMED INDEX: invalid pivots\n");
         trunk_node_print(node, Platform_error_log_handle, data_cfg, 4);
         return FALSE;
      }
   }

   return TRUE;
}

static void
trunk_node_deinit(trunk_node *node, const trunk_context *context)
{
   VECTOR_APPLY_TO_ELTS(
      &node->pivots, vector_apply_platform_free, context->hid);
   VECTOR_APPLY_TO_PTRS(&node->pivot_bundles, bundle_deinit);
   VECTOR_APPLY_TO_PTRS(&node->inflight_bundles, bundle_deinit);
   vector_deinit(&node->pivots);
   vector_deinit(&node->pivot_bundles);
   vector_deinit(&node->inflight_bundles);
}


/**************************************************
 * Basic accessors for ondisk bundles
 **************************************************/

static uint64
sizeof_trunk_ondisk_bundle(trunk_ondisk_bundle *odb)
{
   return sizeof(*odb) + sizeof(odb->branches[0]) * odb->num_branches;
}

static uint64
trunk_ondisk_bundle_size(uint64 num_branches)
{
   return sizeof(trunk_ondisk_bundle) + sizeof(branch_ref) * num_branches;
}

static page_type
trunk_ondisk_bundle_branch_type(const trunk_ondisk_bundle *odb)
{
   return routing_filters_equal(&odb->maplet, &NULL_ROUTING_FILTER)
                && odb->num_branches == 1
             ? PAGE_TYPE_BRANCH
             : PAGE_TYPE_BRANCH;
}

/****************************************************
 * Basic accessors for ondisk pivots
 ****************************************************/

static uint64
sizeof_trunk_ondisk_pivot(trunk_ondisk_pivot *odp)
{
   return sizeof(*odp) + sizeof_ondisk_key_data(&odp->key);
}

static uint64
trunk_ondisk_pivot_size(key k)
{
   return sizeof(trunk_ondisk_pivot) + ondisk_key_required_data_capacity(k);
}

static key
trunk_ondisk_pivot_key(trunk_ondisk_pivot *odp)
{
   return ondisk_key_to_key(&odp->key);
}

static trunk_ondisk_bundle *
trunk_ondisk_pivot_bundle(trunk_ondisk_pivot *odp)
{
   return (trunk_ondisk_bundle *)((char *)odp + sizeof_trunk_ondisk_pivot(odp));
}

/********************************************************
 * Node serialization/deserialization and refcounting.
 ********************************************************/

static platform_status
trunk_ondisk_node_handle_init(trunk_ondisk_node_handle *handle,
                              cache                    *cc,
                              uint64                    addr)
{
   platform_assert(addr != 0);
   handle->cc          = cc;
   handle->header_page = cache_get(cc, addr, TRUE, PAGE_TYPE_TRUNK);
   if (handle->header_page == NULL) {
      platform_error_log("%s():%d: cache_get() failed", __func__, __LINE__);
      return STATUS_IO_ERROR;
   }
   handle->pivot_page           = NULL;
   handle->inflight_bundle_page = NULL;
   return STATUS_OK;
}

/*
 * IN Parameters:
 * - state->context: the trunk_node_context
 * - state->pivot->child_addr: the address of the node
 *
 * OUT Parameters:
 * - state->child_handle: the ondisk_node_handle
 * - state->rc: the return code
 */
static async_status
trunk_ondisk_node_handle_init_async(trunk_merge_lookup_async_state *state,
                                    uint64                          depth)
{
   async_begin(state, depth);

   platform_assert(state->pivot->child_addr != 0);
   state->child_handle.cc = state->context->cc;
   cache_get_async_state_init(state->cache_get_state,
                              state->context->cc,
                              state->pivot->child_addr,
                              PAGE_TYPE_TRUNK,
                              state->callback,
                              state->callback_arg);
   while (cache_get_async(state->context->cc, state->cache_get_state)
          != ASYNC_STATUS_DONE)
   {
      async_yield(state);
   }
   state->child_handle.header_page =
      cache_get_async_state_result(state->context->cc, state->cache_get_state);
   if (state->child_handle.header_page == NULL) {
      platform_error_log("%s():%d: cache_get() failed", __func__, __LINE__);
      state->rc = STATUS_IO_ERROR;
      async_return(state);
   }
   state->child_handle.pivot_page           = NULL;
   state->child_handle.inflight_bundle_page = NULL;
   state->rc                                = STATUS_OK;
   async_return(state);
}


void
trunk_ondisk_node_handle_deinit(trunk_ondisk_node_handle *handle)
{
   if (handle->pivot_page != NULL && handle->pivot_page != handle->header_page)
   {
      cache_unget(handle->cc, handle->pivot_page);
   }
   if (handle->inflight_bundle_page != NULL
       && handle->inflight_bundle_page != handle->header_page)
   {
      cache_unget(handle->cc, handle->inflight_bundle_page);
   }
   if (handle->header_page != NULL) {
      cache_unget(handle->cc, handle->header_page);
   }
   handle->header_page          = NULL;
   handle->pivot_page           = NULL;
   handle->inflight_bundle_page = NULL;
}

static platform_status
trunk_ondisk_node_handle_clone(trunk_ondisk_node_handle       *dst,
                               const trunk_ondisk_node_handle *src)
{
   dst->cc = src->cc;
   if (src->header_page == NULL) {
      dst->header_page          = NULL;
      dst->pivot_page           = NULL;
      dst->inflight_bundle_page = NULL;
      return STATUS_OK;
   }

   dst->header_page =
      cache_get(src->cc, src->header_page->disk_addr, TRUE, PAGE_TYPE_TRUNK);
   if (dst->header_page == NULL) {
      platform_error_log("%s():%d: cache_get() failed", __func__, __LINE__);
      return STATUS_IO_ERROR;
   }
   dst->pivot_page           = NULL;
   dst->inflight_bundle_page = NULL;
   return STATUS_OK;
}

static uint64
content_page_offset(const trunk_ondisk_node_handle *handle,
                    const page_handle              *page)
{
   return page->disk_addr - handle->header_page->disk_addr;
}

static bool32
offset_is_in_content_page(const trunk_ondisk_node_handle *handle,
                          const page_handle              *page,
                          uint32                          offset)
{
   uint64 page_size = cache_page_size(handle->cc);
   return page != NULL && content_page_offset(handle, page) <= offset
          && offset < content_page_offset(handle, page) + page_size;
}

static platform_status
trunk_ondisk_node_handle_setup_content_page(trunk_ondisk_node_handle *handle,
                                            uint64                    offset,
                                            page_handle             **page)
{
   uint64 page_size = cache_page_size(handle->cc);

   if (offset_is_in_content_page(handle, *page, offset)) {
      return STATUS_OK;
   }

   if (*page != NULL && *page != handle->header_page) {
      cache_unget(handle->cc, *page);
   }

   if (offset < page_size) {
      *page = handle->header_page;
      return STATUS_OK;
   } else {
      uint64 addr = handle->header_page->disk_addr + offset;
      addr -= (addr % page_size);
      *page = cache_get(handle->cc, addr, TRUE, PAGE_TYPE_TRUNK);
      if (*page == NULL) {
         platform_error_log("%s():%d: cache_get() failed", __func__, __LINE__);
         return STATUS_IO_ERROR;
      }
      return STATUS_OK;
   }
}

/*
 * IN Parameters:
 * - state->handlep: the ondisk_node_handle
 * - state->offset: the offset of the page to get
 *
 * IN/OUT Parameters:
 * - state->page: Pointer to the page pointer in the handle to set up.
 *
 * OUT Parameters:
 * - state->rc: the return code
 *
 * LOCAL Variables:
 * - state->cache_get_state: the state of the cache_get() operation
 */
static async_status
trunk_ondisk_node_handle_setup_content_page_async(
   trunk_merge_lookup_async_state *state,
   uint64                          depth)
{
   async_begin(state, depth);

   uint64 page_size = cache_page_size(state->handlep->cc);

   if (offset_is_in_content_page(state->handlep, *state->page, state->offset)) {
      state->rc = STATUS_OK;
      async_return(state);
   }

   if (*state->page != NULL && *state->page != state->handlep->header_page) {
      cache_unget(state->handlep->cc, *state->page);
   }

   if (state->offset < page_size) {
      *state->page = state->handlep->header_page;
      state->rc    = STATUS_OK;
      async_return(state);
   } else {
      uint64 addr = state->handlep->header_page->disk_addr + state->offset;
      addr -= (addr % page_size);
      cache_get_async_state_init(state->cache_get_state,
                                 state->handlep->cc,
                                 addr,
                                 PAGE_TYPE_TRUNK,
                                 state->callback,
                                 state->callback_arg);
      while (cache_get_async(state->handlep->cc, state->cache_get_state)
             != ASYNC_STATUS_DONE)
      {
         async_yield(state);
      }
      *state->page = cache_get_async_state_result(state->handlep->cc,
                                                  state->cache_get_state);
      if (*state->page == NULL) {
         platform_error_log("%s():%d: cache_get() failed", __func__, __LINE__);
         state->rc = STATUS_IO_ERROR;
         async_return(state);
      }
      state->rc = STATUS_OK;
      async_return(state);
   }
}

static uint64
trunk_ondisk_node_height(trunk_ondisk_node_handle *handle)
{
   trunk_ondisk_node *header = (trunk_ondisk_node *)handle->header_page->data;
   return header->height;
}

static uint64
trunk_ondisk_node_num_pivots(trunk_ondisk_node_handle *handle)
{
   trunk_ondisk_node *header = (trunk_ondisk_node *)handle->header_page->data;
   return header->num_pivots;
}

static trunk_ondisk_pivot *
trunk_ondisk_node_get_pivot(trunk_ondisk_node_handle *handle, uint64 pivot_num)
{
   trunk_ondisk_node *header = (trunk_ondisk_node *)handle->header_page->data;
   uint64             offset = header->pivot_offsets[pivot_num];
   platform_status    rc     = trunk_ondisk_node_handle_setup_content_page(
      handle, offset, &handle->pivot_page);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: ondisk_node_handle_setup_content_page() "
                         "failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      return NULL;
   }
   return (
      trunk_ondisk_pivot *)(handle->pivot_page->data + offset
                            - content_page_offset(handle, handle->pivot_page));
}

/*
 * IN Parameters:
 * - state->handlep: the ondisk_node_handle
 * - state->pivot_num: the pivot number to get
 *
 * OUT Parameters:
 * - state->pivot: the pivot
 * - state->rc: the return code
 *
 * LOCAL Variables:
 * - state->offset: the offset of the pivot
 * - state->page: Pointer to the page pointer in the handle to set up.
 * - state->cache_get_state: the state of the cache_get() operation
 */
static async_status
trunk_ondisk_node_get_pivot_async(trunk_merge_lookup_async_state *state,
                                  uint64                          depth)
{
   async_begin(state, depth);

   trunk_ondisk_node *header =
      (trunk_ondisk_node *)state->handlep->header_page->data;
   state->offset = header->pivot_offsets[state->pivot_num];
   state->page   = &state->handlep->pivot_page;
   async_await_subroutine(state,
                          trunk_ondisk_node_handle_setup_content_page_async);
   if (!SUCCESS(state->rc)) {
      platform_error_log("%s():%d: ondisk_node_handle_setup_content_page() "
                         "failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(state->rc));
      state->pivot = NULL;
      async_return(state);
   }
   state->pivot =
      (trunk_ondisk_pivot *)(state->handlep->pivot_page->data + state->offset
                             - content_page_offset(state->handlep,
                                                   state->handlep->pivot_page));
   state->rc = STATUS_OK;
   async_return(state);
}


static platform_status
trunk_ondisk_node_get_pivot_key(trunk_ondisk_node_handle *handle,
                                uint64                    pivot_num,
                                key                      *k)
{
   trunk_ondisk_pivot *odp = trunk_ondisk_node_get_pivot(handle, pivot_num);
   if (odp == NULL) {
      platform_error_log(
         "%s():%d: ondisk_node_get_pivot() failed", __func__, __LINE__);
      return STATUS_IO_ERROR;
   }
   *k = ondisk_key_to_key(&odp->key);
   return STATUS_OK;
}

static trunk_ondisk_bundle *
trunk_ondisk_node_get_pivot_bundle(trunk_ondisk_node_handle *handle,
                                   uint64                    pivot_num)
{
   trunk_ondisk_pivot *pivot = trunk_ondisk_node_get_pivot(handle, pivot_num);
   if (pivot == NULL) {
      platform_error_log(
         "%s():%d: ondisk_node_get_pivot() failed", __func__, __LINE__);
      return NULL;
   }
   return (trunk_ondisk_bundle *)(((char *)pivot)
                                  + sizeof_trunk_ondisk_pivot(pivot));
}

static trunk_ondisk_bundle *
trunk_ondisk_node_bundle_at_offset(trunk_ondisk_node_handle *handle,
                                   uint64                    offset)
{
   uint64 page_size = cache_page_size(handle->cc);

   /* If there's not enough room for a bundle header, skip to the next
    * page. */
   if (page_size - (offset % page_size) < sizeof(trunk_ondisk_bundle)) {
      offset += page_size - (offset % page_size);
   }

   platform_status rc = trunk_ondisk_node_handle_setup_content_page(
      handle, offset, &handle->inflight_bundle_page);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: ondisk_node_handle_setup_content_page() "
                         "failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      return NULL;
   }
   trunk_ondisk_bundle *result =
      (trunk_ondisk_bundle *)(handle->inflight_bundle_page->data + offset
                              - content_page_offset(
                                 handle, handle->inflight_bundle_page));

   /* If there wasn't enough room for this bundle on this page, then we would
    * have zeroed the remaining bytes and put the bundle on the next page. */
   if (result->num_branches == 0) {
      offset += page_size - (offset % page_size);
      rc = trunk_ondisk_node_handle_setup_content_page(
         handle, offset, &handle->inflight_bundle_page);
      if (!SUCCESS(rc)) {
         platform_error_log("%s():%d: ondisk_node_handle_setup_content_page() "
                            "failed: %s",
                            __func__,
                            __LINE__,
                            platform_status_to_string(rc));
         return NULL;
      }
      result =
         (trunk_ondisk_bundle *)(handle->inflight_bundle_page->data + offset
                                 - content_page_offset(
                                    handle, handle->inflight_bundle_page));
   }
   return result;
}

/*
 * IN Parameters:
 * - state->handlep: the ondisk_node_handle
 * - state->offset: the offset of the bundle
 *
 * OUT Parameters:
 * - state->bndl: the bundle
 * - state->rc: the return code
 *
 * LOCAL Variables:
 * - state->page: Pointer to the page pointer in the handle to set up.
 * - state->cache_get_state: the state of the cache_get() operation
 */
static async_status
trunk_ondisk_node_bundle_at_offset_async(trunk_merge_lookup_async_state *state,
                                         uint64                          depth)
{
   uint64 page_size = cache_page_size(state->handlep->cc);

   async_begin(state, depth);

   /* If there's not enough room for a bundle header, skip to the next
    * page. */
   if (page_size - (state->offset % page_size) < sizeof(trunk_ondisk_bundle)) {
      state->offset += page_size - (state->offset % page_size);
   }

   state->page = &state->handlep->inflight_bundle_page;
   async_await_subroutine(state,
                          trunk_ondisk_node_handle_setup_content_page_async);
   if (!SUCCESS(state->rc)) {
      platform_error_log("%s():%d: ondisk_node_handle_setup_content_page() "
                         "failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(state->rc));
      state->bndl = NULL;
      async_return(state);
   }
   state->bndl =
      (trunk_ondisk_bundle *)(state->handlep->inflight_bundle_page->data
                              + state->offset
                              - content_page_offset(
                                 state->handlep,
                                 state->handlep->inflight_bundle_page));

   /* If there wasn't enough room for this bundle on this page, then we would
    * have zeroed the remaining bytes and put the bundle on the next page. */
   if (state->bndl->num_branches == 0) {
      state->offset += page_size - (state->offset % page_size);
      state->page = &state->handlep->inflight_bundle_page;
      async_await_subroutine(state,
                             trunk_ondisk_node_handle_setup_content_page_async);
      if (!SUCCESS(state->rc)) {
         platform_error_log("%s():%d: ondisk_node_handle_setup_content_page() "
                            "failed: %s",
                            __func__,
                            __LINE__,
                            platform_status_to_string(state->rc));
         state->bndl = NULL;
         async_return(state);
      }
      state->bndl =
         (trunk_ondisk_bundle *)(state->handlep->inflight_bundle_page->data
                                 + state->offset
                                 - content_page_offset(
                                    state->handlep,
                                    state->handlep->inflight_bundle_page));
   }
   async_return(state);
}

static platform_status
trunk_ondisk_node_get_first_inflight_bundle(trunk_ondisk_node_handle *handle,
                                            trunk_ondisk_bundle     **bndl)
{
   trunk_ondisk_node *header = (trunk_ondisk_node *)handle->header_page->data;
   if (header->num_inflight_bundles == 0) {
      *bndl = NULL;
      return STATUS_OK;
   }
   uint64 offset = header->inflight_bundles_offset;
   *bndl         = trunk_ondisk_node_bundle_at_offset(handle, offset);
   return *bndl == NULL ? STATUS_IO_ERROR : STATUS_OK;
}

/*
 * IN Parameters:
 * - state->handlep: the ondisk_node_handle
 *
 * OUT Parameters:
 * - state->bndl: the bundle
 * - state->rc: the return code
 *
 * LOCAL Variables:
 * - state->offset: the offset of the bundle
 * - state->page: Pointer to the page pointer in the handle to set up.
 * - state->cache_get_state: the state of the cache_get() operation
 */
static async_status
trunk_ondisk_node_get_first_inflight_bundle_async(
   trunk_merge_lookup_async_state *state,
   uint64                          depth)
{
   async_begin(state, depth);

   trunk_ondisk_node *header =
      (trunk_ondisk_node *)state->handlep->header_page->data;
   if (header->num_inflight_bundles == 0) {
      state->bndl = NULL;
      state->rc   = STATUS_OK;
      async_return(state);
   }
   state->offset = header->inflight_bundles_offset;
   async_await_subroutine(state, trunk_ondisk_node_bundle_at_offset_async);
   async_return(state);
}


static trunk_ondisk_bundle *
trunk_ondisk_node_get_next_inflight_bundle(trunk_ondisk_node_handle *handle,
                                           trunk_ondisk_bundle      *bundle)
{
   uint64 offset = ((char *)bundle) - handle->inflight_bundle_page->data
                   + content_page_offset(handle, handle->inflight_bundle_page)
                   + sizeof_trunk_ondisk_bundle(bundle);
   return trunk_ondisk_node_bundle_at_offset(handle, offset);
}

/*
 * IN Parameters:
 * - state->handlep: the ondisk_node_handle
 *
 * IN/OUT Parameters:
 * - state->bndl: the bundle
 *
 * OUT Parameters:
 * - state->rc: the return code
 *
 * LOCAL Variables:
 * - state->offset: the offset of the bundle
 * - state->page: Pointer to the page pointer in the handle to set up.
 * - state->cache_get_state: the state of the cache_get() operation
 */
static async_status
trunk_ondisk_node_get_next_inflight_bundle_async(
   trunk_merge_lookup_async_state *state,
   uint64                          depth)
{
   async_begin(state, depth);
   state->offset = ((char *)state->bndl)
                   - state->handlep->inflight_bundle_page->data
                   + content_page_offset(state->handlep,
                                         state->handlep->inflight_bundle_page)
                   + sizeof_trunk_ondisk_bundle(state->bndl);
   async_await_subroutine(state, trunk_ondisk_node_bundle_at_offset_async);
   async_return(state);
}

static trunk_pivot *
trunk_pivot_deserialize(platform_heap_id          hid,
                        trunk_ondisk_node_handle *handle,
                        uint64                    i)
{
   trunk_ondisk_node  *header = (trunk_ondisk_node *)handle->header_page->data;
   trunk_ondisk_pivot *odp    = trunk_ondisk_node_get_pivot(handle, i);
   if (odp == NULL) {
      platform_error_log(
         "%s():%d: ondisk_node_get_pivot() failed", __func__, __LINE__);
      return NULL;
   }
   uint64 inflight_bundle_start;
   if (i < header->num_pivots - 1) {
      inflight_bundle_start =
         header->num_inflight_bundles - odp->num_live_inflight_bundles;
   } else {
      inflight_bundle_start = 0;
   }
   return trunk_pivot_create(hid,
                             trunk_ondisk_pivot_key(odp),
                             odp->child_addr,
                             inflight_bundle_start,
                             odp->stats,
                             odp->stats);
}

static platform_status
bundle_deserialize(bundle *bndl, platform_heap_id hid, trunk_ondisk_bundle *odb)
{
   bundle_init(bndl, hid);
   platform_status rc =
      vector_ensure_capacity(&bndl->branches, odb->num_branches);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: vector_ensure_capacity() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      bundle_deinit(bndl);
      return rc;
   }

   bndl->maplet = odb->maplet;

   for (uint64 i = 0; i < odb->num_branches; i++) {
      rc = vector_append(&bndl->branches, odb->branches[i]);
      platform_assert_status_ok(rc);
   }

   return STATUS_OK;
}

static platform_status
trunk_node_deserialize(const trunk_context *context,
                       uint64               addr,
                       trunk_node          *result)
{
   platform_status          rc;
   trunk_ondisk_node_handle handle;

   rc = trunk_ondisk_node_handle_init(&handle, context->cc, addr);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: ondisk_node_handle_init() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      return rc;
   }
   trunk_ondisk_node *header = (trunk_ondisk_node *)handle.header_page->data;

   trunk_pivot_vector pivots;
   bundle_vector      inflight_bundles;
   bundle_vector      pivot_bundles;
   vector_init(&pivots, context->hid);
   vector_init(&inflight_bundles, context->hid);
   vector_init(&pivot_bundles, context->hid);

   rc = vector_ensure_capacity(&pivots, header->num_pivots);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: vector_ensure_capacity() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      goto cleanup;
   }
   rc = vector_ensure_capacity(&pivot_bundles, header->num_pivots - 1);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: vector_ensure_capacity() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      goto cleanup;
   }
   rc = vector_ensure_capacity(&inflight_bundles, header->num_inflight_bundles);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: vector_ensure_capacity() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      goto cleanup;
   }

   for (uint64 i = 0; i < header->num_pivots; i++) {
      trunk_pivot *imp = trunk_pivot_deserialize(context->hid, &handle, i);
      if (imp == NULL) {
         platform_error_log(
            "%s():%d: pivot_deserialize() failed", __func__, __LINE__);
         rc = STATUS_NO_MEMORY;
         goto cleanup;
      }
      rc = vector_append(&pivots, imp);
      if (!SUCCESS(rc)) {
         platform_error_log("%s():%d: vector_append() failed: %s",
                            __func__,
                            __LINE__,
                            platform_status_to_string(rc));
         trunk_pivot_destroy(imp, context->hid);
         goto cleanup;
      }
   }

   for (uint64 i = 0; i < header->num_pivots - 1; i++) {
      trunk_ondisk_bundle *odb = trunk_ondisk_node_get_pivot_bundle(&handle, i);
      if (odb == NULL) {
         platform_error_log("%s():%d: ondisk_node_get_pivot_bundle() failed",
                            __func__,
                            __LINE__);
         rc = STATUS_IO_ERROR;
         goto cleanup;
      }
      rc = VECTOR_EMPLACE_APPEND(
         &pivot_bundles, bundle_deserialize, context->hid, odb);
      if (!SUCCESS(rc)) {
         platform_error_log("%s():%d: VECTOR_EMPLACE_APPEND() failed: %s",
                            __func__,
                            __LINE__,
                            platform_status_to_string(rc));
         goto cleanup;
      }
   }

   if (0 < header->num_inflight_bundles) {
      trunk_ondisk_bundle *odb = NULL;
      // We can ignore the return code here since we will notice any error once
      // we go inside the fore loop.
      trunk_ondisk_node_get_first_inflight_bundle(&handle, &odb);
      for (uint64 i = 0; i < header->num_inflight_bundles; i++) {
         if (odb == NULL) {
            platform_error_log(
               "%s():%d: ondisk_node_get_first_inflight_bundle() failed",
               __func__,
               __LINE__);
            rc = STATUS_IO_ERROR;
            goto cleanup;
         }
         rc = VECTOR_EMPLACE_APPEND(
            &inflight_bundles, bundle_deserialize, context->hid, odb);
         if (!SUCCESS(rc)) {
            platform_error_log("%s():%d: VECTOR_EMPLACE_APPEND() failed: %s",
                               __func__,
                               __LINE__,
                               platform_status_to_string(rc));
            goto cleanup;
         }
         if (i + 1 < header->num_inflight_bundles) {
            odb = trunk_ondisk_node_get_next_inflight_bundle(&handle, odb);
         }
      }
   }

   trunk_ondisk_node_handle_deinit(&handle);

   vector_reverse(&inflight_bundles);

   trunk_node_init(result,
                   header->height,
                   pivots,
                   pivot_bundles,
                   header->num_inflight_bundles,
                   inflight_bundles);

   if (trunk_node_is_leaf(result)) {
      debug_assert(
         trunk_node_is_well_formed_leaf(context->cfg->data_cfg, result));
   } else {
      debug_assert(
         trunk_node_is_well_formed_index(context->cfg->data_cfg, result));
   }

   return STATUS_OK;

cleanup:
   VECTOR_APPLY_TO_ELTS(&pivots, trunk_pivot_destroy, context->hid);
   VECTOR_APPLY_TO_PTRS(&pivot_bundles, bundle_deinit);
   VECTOR_APPLY_TO_PTRS(&inflight_bundles, bundle_deinit);
   vector_deinit(&pivots);
   vector_deinit(&pivot_bundles);
   vector_deinit(&inflight_bundles);
   trunk_ondisk_node_handle_deinit(&handle);
   return rc;
}

static void
bundle_inc_all_branch_refs(const trunk_context *context, bundle *bndl)
{
   for (uint64 i = 0; i < vector_length(&bndl->branches); i++) {
      branch_ref bref = vector_get(&bndl->branches, i);
      btree_inc_ref(
         context->cc, context->cfg->btree_cfg, branch_ref_addr(bref));
   }
}

static void
bundle_dec_all_branch_refs(const trunk_context *context, bundle *bndl)
{
   page_type type = bundle_branch_type(bndl);
   for (uint64 i = 0; i < vector_length(&bndl->branches); i++) {
      branch_ref bref = vector_get(&bndl->branches, i);
      btree_dec_ref(
         context->cc, context->cfg->btree_cfg, branch_ref_addr(bref), type);
   }
}

static void
bundle_inc_all_refs(trunk_context *context, bundle *bndl)
{
   if (routing_filters_equal(&bndl->maplet, &NULL_ROUTING_FILTER)) {
      platform_assert(vector_length(&bndl->branches) <= 1);
   } else {
      routing_filter_inc_ref(context->cc, &bndl->maplet);
   }
   bundle_inc_all_branch_refs(context, bndl);
}

static void
bundle_dec_all_refs(trunk_context *context, bundle *bndl)
{
   if (routing_filters_equal(&bndl->maplet, &NULL_ROUTING_FILTER)) {
      platform_assert(vector_length(&bndl->branches) <= 1);
   } else {
      routing_filter_dec_ref(context->cc, &bndl->maplet);
   }
   bundle_dec_all_branch_refs(context, bndl);
}

// static void
// trunk_ondisk_node_wait_for_readers(trunk_context *context, uint64 addr)
// {
//    page_handle *page    = cache_get(context->cc, addr, TRUE,
//    PAGE_TYPE_TRUNK); bool32       success = cache_try_claim(context->cc,
//    page); platform_assert(success); cache_lock(context->cc, page);
//    cache_unlock(context->cc, page);
//    cache_unclaim(context->cc, page);
//    cache_unget(context->cc, page);
// }

static void
trunk_ondisk_node_dec_ref(trunk_context *context, uint64 addr);

/* Prerequisite: addr must be in the AL_NO_REFS state. */
static void
trunk_ondisk_node_gc(trunk_context *context, uint64 addr)
{
   trunk_node      node;
   platform_status rc = trunk_node_deserialize(context, addr, &node);
   if (SUCCESS(rc)) {
      if (!trunk_node_is_leaf(&node)) {
         for (uint64 i = 0; i < vector_length(&node.pivots) - 1; i++) {
            trunk_pivot *pvt = vector_get(&node.pivots, i);
            trunk_ondisk_node_dec_ref(context, pvt->child_addr);
         }
      }
      for (uint64 i = 0; i < vector_length(&node.pivot_bundles); i++) {
         bundle *bndl = vector_get_ptr(&node.pivot_bundles, i);
         bundle_dec_all_refs(context, bndl);
      }
      for (uint64 i = 0; i < vector_length(&node.inflight_bundles); i++) {
         bundle *bndl = vector_get_ptr(&node.inflight_bundles, i);
         bundle_dec_all_refs(context, bndl);
      }
      trunk_node_deinit(&node, context);
   } else {
      platform_error_log("%s():%d: node_deserialize() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
   }
   cache_extent_discard(context->cc, addr, PAGE_TYPE_TRUNK);
   allocator_dec_ref(context->al, addr, PAGE_TYPE_TRUNK);
}

static void
pending_gcs_lock(trunk_context *context)
{
   while (__sync_lock_test_and_set(&context->pending_gcs_lock, 1)) {
      platform_yield();
   }
}

static void
pending_gcs_unlock(trunk_context *context)
{
   __sync_lock_release(&context->pending_gcs_lock);
}


static void
trunk_ondisk_node_dec_ref(trunk_context *context, uint64 addr)
{
   refcount ref = allocator_dec_ref(context->al, addr, PAGE_TYPE_TRUNK);
   if (ref == AL_NO_REFS) {
      if (cache_in_use(context->cc, addr)) {
         pending_gc *pgc = TYPED_MALLOC(context->hid, pgc);
         if (pgc == NULL) {
            platform_error_log("%s():%d: TYPED_MALLOC() failed.  We're gonna "
                               "leak some disk space.",
                               __func__,
                               __LINE__);
            return;
         }
         pgc->addr = addr;
         pgc->next = NULL;

         pending_gcs_lock(context);
         if (context->pending_gcs_tail == NULL) {
            context->pending_gcs      = pgc;
            context->pending_gcs_tail = pgc;
         } else {
            context->pending_gcs_tail->next = pgc;
            context->pending_gcs_tail       = pgc;
         }
         pending_gcs_unlock(context);

      } else {
         trunk_ondisk_node_gc(context, addr);
      }
   }
}

static void
trunk_ondisk_node_inc_ref(trunk_context *context, uint64 addr)
{
   allocator_inc_ref(context->al, addr);
}

static void
trunk_node_inc_all_refs(trunk_context *context, trunk_node *node)
{
   if (!trunk_node_is_leaf(node)) {
      for (uint64 i = 0; i < vector_length(&node->pivots) - 1; i++) {
         trunk_pivot *pvt = vector_get(&node->pivots, i);
         trunk_ondisk_node_inc_ref(context, pvt->child_addr);
      }
   }
   for (uint64 i = 0; i < vector_length(&node->pivot_bundles); i++) {
      bundle *bndl = vector_get_ptr(&node->pivot_bundles, i);
      bundle_inc_all_refs(context, bndl);
   }
   uint64 inflight_start = trunk_node_first_live_inflight_bundle(node);
   for (uint64 i = inflight_start; i < vector_length(&node->inflight_bundles);
        i++)
   {
      bundle *bndl = vector_get_ptr(&node->inflight_bundles, i);
      bundle_inc_all_refs(context, bndl);
   }
}

static trunk_ondisk_node_ref *
trunk_ondisk_node_ref_create(platform_heap_id hid, key k, uint64 child_addr)
{
   trunk_ondisk_node_ref *result = TYPED_FLEXIBLE_STRUCT_ZALLOC(
      hid, result, key.bytes, ondisk_key_required_data_capacity(k));
   if (result == NULL) {
      platform_error_log(
         "%s():%d: TYPED_FLEXIBLE_STRUCT_ZALLOC() failed", __func__, __LINE__);
      return NULL;
   }
   result->addr = child_addr;
   copy_key_to_ondisk_key(&result->key, k);
   return result;
}

static void
trunk_ondisk_node_ref_destroy(trunk_ondisk_node_ref *odnref,
                              trunk_context         *context,
                              platform_heap_id       hid)
{
   if (odnref->addr != 0) {
      trunk_ondisk_node_dec_ref(context, odnref->addr);
   }
   platform_free(hid, odnref);
}

static trunk_pivot *
trunk_pivot_create_from_ondisk_node_ref(trunk_ondisk_node_ref *odnref,
                                        platform_heap_id       hid)
{
   return trunk_pivot_create(hid,
                             ondisk_key_to_key(&odnref->key),
                             odnref->addr,
                             0,
                             TRUNK_STATS_ZERO,
                             TRUNK_STATS_ZERO);
}

static uint64
trunk_pivot_ondisk_size(trunk_pivot *pvt)
{
   return trunk_ondisk_pivot_size(trunk_pivot_key(pvt));
}

static uint64
bundle_ondisk_size(bundle *bndl)
{
   return trunk_ondisk_bundle_size(vector_length(&bndl->branches));
}

static void
pivot_serialize(trunk_context      *context,
                trunk_node         *node,
                uint64              pivot_num,
                trunk_ondisk_pivot *dest)
{
   trunk_pivot *pvt = vector_get(&node->pivots, pivot_num);
   platform_assert(trunk_pivot_stats_are_nonnegative(pvt->stats));
   dest->stats      = pvt->stats;
   dest->child_addr = pvt->child_addr;
   if (pivot_num < vector_length(&node->pivots) - 1) {
      dest->num_live_inflight_bundles =
         vector_length(&node->inflight_bundles) - pvt->inflight_bundle_start;
   } else {
      dest->num_live_inflight_bundles = 0;
   }
   copy_key_to_ondisk_key(&dest->key, trunk_pivot_key(pvt));
}

static void
bundle_serialize(bundle *bndl, trunk_ondisk_bundle *dest)
{
   dest->maplet       = bndl->maplet;
   dest->num_branches = vector_length(&bndl->branches);
   for (uint64 i = 0; i < dest->num_branches; i++) {
      dest->branches[i] = vector_get(&bndl->branches, i);
   }
}

static platform_status
trunk_node_serialize_maybe_setup_next_page(cache        *cc,
                                           uint64        required_space,
                                           page_handle  *header_page,
                                           page_handle **current_page,
                                           uint64       *page_offset)
{
   uint64 page_size   = cache_page_size(cc);
   uint64 extent_size = cache_extent_size(cc);

   if (page_size < required_space) {
      platform_error_log(
         "%s():%d: required_space too large", __func__, __LINE__);
      return STATUS_LIMIT_EXCEEDED;
   }

   if (page_size < *page_offset + required_space) {
      memset((*current_page)->data + *page_offset, 0, page_size - *page_offset);
      if (*current_page != header_page) {
         cache_unlock(cc, *current_page);
         cache_unclaim(cc, *current_page);
         cache_unget(cc, *current_page);
      }
      uint64 addr = (*current_page)->disk_addr + page_size;
      if (extent_size <= addr - header_page->disk_addr) {
         platform_error_log(
            "%s():%d: extent_size too small", __func__, __LINE__);
         return STATUS_LIMIT_EXCEEDED;
      }
      *current_page = cache_alloc(cc, addr, PAGE_TYPE_TRUNK);
      if (*current_page == NULL) {
         platform_error_log(
            "%s():%d: cache_alloc() failed", __func__, __LINE__);
         return STATUS_NO_MEMORY;
      }
      cache_mark_dirty(cc, *current_page);
      *page_offset = 0;
   }

   return STATUS_OK;
}

// For debugging
static uint64 max_pivots                   = 0;
static uint64 max_inflight_bundles         = 0;
static uint64 max_inflight_bundle_branches = 0;
static uint64 max_inflight_branches        = 0;
static uint64 max_pivot_bundle_branches    = 0;

debug_only static bool32
record_and_report_max(const char *name, uint64 value, uint64 *max)
{
   if (value > *max) {
      *max = value;
      platform_error_log("%s: %lu\n", name, value);
      return TRUE;
   }
   return FALSE;
}

debug_only static void
print_pivot_states_for_node(trunk_context *context, trunk_node *node);

debug_only static void
trunk_node_record_and_report_maxes(trunk_context *context, trunk_node *node)
{
   bool32 big = FALSE;

   big |= record_and_report_max(
      "max_pivots", vector_length(&node->pivots), &max_pivots);

   uint64 inflight_start = trunk_node_first_live_inflight_bundle(node);
   big |= record_and_report_max("max_inflight_bundles",
                                vector_length(&node->inflight_bundles)
                                   - inflight_start,
                                &max_inflight_bundles);

   uint64 inflight_branches = 0;
   for (int i = inflight_start; i < vector_length(&node->inflight_bundles); i++)
   {
      bundle *bndl = vector_get_ptr(&node->inflight_bundles, i);
      big |= record_and_report_max("max_inflight_bundle_branches",
                                   vector_length(&bndl->branches),
                                   &max_inflight_bundle_branches);
      inflight_branches += vector_length(&bndl->branches);
   }
   big |= record_and_report_max(
      "max_inflight_branches", inflight_branches, &max_inflight_branches);

   for (uint64 i = 0; i < vector_length(&node->pivot_bundles); i++) {
      bundle *bndl = vector_get_ptr(&node->pivot_bundles, i);
      big |= record_and_report_max("max_pivot_bundle_branches",
                                   vector_length(&bndl->branches),
                                   &max_pivot_bundle_branches);
   }

   if (big) {
      trunk_node_print(
         node, Platform_error_log_handle, context->cfg->data_cfg, 4);
      print_pivot_states_for_node(context, node);
   }
}

static trunk_ondisk_node_ref *
trunk_node_serialize(trunk_context *context, trunk_node *node)
{
   platform_status        rc;
   uint64                 header_addr  = 0;
   page_handle           *header_page  = NULL;
   page_handle           *current_page = NULL;
   trunk_ondisk_node_ref *result       = NULL;
   threadid               tid          = platform_get_tid();

   // if (node_height(node) == 0) {
   //    node_print(node, Platform_error_log_handle, context->cfg->data_cfg, 4);
   // }

   // node_record_and_report_maxes(context, node);

   if (context->stats) {
      uint64 fanout = vector_length(&node->pivots) - 2;
      if (TRUNK_MAX_DISTRIBUTION_VALUE <= fanout) {
         fanout = TRUNK_MAX_DISTRIBUTION_VALUE - 1;
      }
      context->stats[tid].fanout_distribution[fanout][node->height]++;

      uint64 ifbundles = vector_length(&node->inflight_bundles)
                         - trunk_node_first_live_inflight_bundle(node);
      if (TRUNK_MAX_DISTRIBUTION_VALUE <= ifbundles) {
         ifbundles = TRUNK_MAX_DISTRIBUTION_VALUE - 1;
      }
      context->stats[tid]
         .num_inflight_bundles_distribution[ifbundles][node->height]++;
   }

   if (trunk_node_is_leaf(node)) {
      debug_assert(
         trunk_node_is_well_formed_leaf(context->cfg->data_cfg, node));
   } else {
      debug_assert(
         trunk_node_is_well_formed_index(context->cfg->data_cfg, node));
   }

   rc = allocator_alloc(context->al, &header_addr, PAGE_TYPE_TRUNK);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: allocator_alloc() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      goto cleanup;
   }

   header_page = cache_alloc(context->cc, header_addr, PAGE_TYPE_TRUNK);
   if (header_page == NULL) {
      platform_error_log("%s():%d: cache_alloc() failed", __func__, __LINE__);
      rc = STATUS_NO_MEMORY;
      goto cleanup;
   }
   cache_mark_dirty(context->cc, header_page);

   int64 min_inflight_bundle_start =
      trunk_node_first_live_inflight_bundle(node);

   trunk_ondisk_node *odnode = (trunk_ondisk_node *)header_page->data;
   odnode->height            = node->height;
   odnode->num_pivots        = vector_length(&node->pivots);
   odnode->num_inflight_bundles =
      vector_length(&node->inflight_bundles) - min_inflight_bundle_start;

   current_page = header_page;
   uint64 page_offset =
      sizeof(*odnode) + sizeof(odnode->pivot_offsets[0]) * odnode->num_pivots;

   for (uint64 i = 0; i < vector_length(&node->pivots); i++) {
      uint64 pivot_size = trunk_pivot_ondisk_size(vector_get(&node->pivots, i));
      uint64 required_space = pivot_size;

      bundle *pivot_bundle;
      uint64  bundle_size;
      if (i < vector_length(&node->pivots) - 1) {
         pivot_bundle = vector_get_ptr(&node->pivot_bundles, i);
         bundle_size  = bundle_ondisk_size(pivot_bundle);
         required_space += bundle_size;

         if (context->stats) {
            uint64 bundle_size = vector_length(&pivot_bundle->branches);
            if (TRUNK_MAX_DISTRIBUTION_VALUE <= bundle_size) {
               bundle_size = TRUNK_MAX_DISTRIBUTION_VALUE - 1;
            }
            context->stats[tid]
               .bundle_num_branches_distribution[bundle_size][node->height]++;
         }
      }

      rc = trunk_node_serialize_maybe_setup_next_page(
         context->cc, required_space, header_page, &current_page, &page_offset);
      if (!SUCCESS(rc)) {
         platform_error_log(
            "%s():%d: node_serialize_maybe_setup_next_page() failed: %s",
            __func__,
            __LINE__,
            platform_status_to_string(rc));
         goto cleanup;
      }

      odnode->pivot_offsets[i] =
         current_page->disk_addr - header_addr + page_offset;
      pivot_serialize(context,
                      node,
                      i,
                      (trunk_ondisk_pivot *)(current_page->data + page_offset));
      page_offset += pivot_size;
      if (i < vector_length(&node->pivots) - 1) {
         bundle_serialize(
            pivot_bundle,
            (trunk_ondisk_bundle *)(current_page->data + page_offset));
         page_offset += bundle_size;
      }
   }

   odnode->inflight_bundles_offset = 0;

   for (int64 i = vector_length(&node->inflight_bundles) - 1;
        i >= min_inflight_bundle_start;
        i--)
   {
      bundle *bndl        = vector_get_ptr(&node->inflight_bundles, i);
      uint64  bundle_size = bundle_ondisk_size(bndl);

      rc = trunk_node_serialize_maybe_setup_next_page(
         context->cc, bundle_size, header_page, &current_page, &page_offset);
      if (!SUCCESS(rc)) {
         platform_error_log(
            "%s():%d: node_serialize_maybe_setup_next_page() failed: %s",
            __func__,
            __LINE__,
            platform_status_to_string(rc));
         goto cleanup;
      }

      if (i == vector_length(&node->inflight_bundles) - 1) {
         odnode->inflight_bundles_offset =
            current_page->disk_addr - header_addr + page_offset;
      }
      bundle_serialize(
         bndl, (trunk_ondisk_bundle *)(current_page->data + page_offset));
      page_offset += bundle_size;
   }

   trunk_node_inc_all_refs(context, node);

   result = trunk_ondisk_node_ref_create(
      context->hid, trunk_node_pivot_key(node, 0), header_addr);
   if (result == NULL) {
      platform_error_log(
         "%s():%d: ondisk_node_ref_create() failed", __func__, __LINE__);
      goto cleanup;
   }

   if (context->stats) {
      uint64 num_pages = 1
                         + (current_page->disk_addr - header_addr)
                              / cache_page_size(context->cc);
      if (TRUNK_MAX_DISTRIBUTION_VALUE <= num_pages) {
         num_pages = TRUNK_MAX_DISTRIBUTION_VALUE - 1;
      }
      context->stats[tid]
         .node_size_pages_distribution[num_pages][node->height]++;
   }

   if (current_page != header_page) {
      cache_unlock(context->cc, current_page);
      cache_unclaim(context->cc, current_page);
      cache_unget(context->cc, current_page);
   }

   cache_unlock(context->cc, header_page);
   cache_unclaim(context->cc, header_page);
   cache_unget(context->cc, header_page);

   return result;

cleanup:
   if (current_page != NULL && current_page != header_page) {
      cache_unlock(context->cc, current_page);
      cache_unclaim(context->cc, current_page);
      cache_unget(context->cc, current_page);
   }
   if (header_page != NULL) {
      cache_unlock(context->cc, header_page);
      cache_unclaim(context->cc, header_page);
      cache_unget(context->cc, header_page);
      cache_extent_discard(context->cc, header_addr, PAGE_TYPE_TRUNK);
   }
   if (result != NULL) {
      trunk_ondisk_node_ref_destroy(result, context, context->hid);
   }
   return NULL;
}

static platform_status
serialize_nodes(trunk_context                *context,
                trunk_node_vector            *nodes,
                trunk_ondisk_node_ref_vector *result)
{
   platform_status rc;

   rc = vector_ensure_capacity(result, vector_length(nodes));
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: vector_ensure_capacity() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      goto finish;
   }
   for (uint64 i = 0; i < vector_length(nodes); i++) {
      trunk_ondisk_node_ref *odnref =
         trunk_node_serialize(context, vector_get_ptr(nodes, i));
      if (odnref == NULL) {
         platform_error_log(
            "%s():%d: node_serialize() failed", __func__, __LINE__);
         rc = STATUS_NO_MEMORY;
         goto finish;
      }
      rc = vector_append(result, odnref);
      platform_assert_status_ok(rc);
   }

finish:
   if (!SUCCESS(rc)) {
      VECTOR_APPLY_TO_ELTS(
         result, trunk_ondisk_node_ref_destroy, context, context->hid);
      vector_truncate(result, 0);
   }

   return rc;
}

/*********************************************
 * branch_merger operations
 * (used in both leaf splits and compactions)
 *********************************************/

static void
trunk_branch_merger_init(trunk_branch_merger *merger,
                         platform_heap_id     hid,
                         const data_config   *data_cfg,
                         key                  min_key,
                         key                  max_key,
                         uint64               height)
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
trunk_branch_merger_add_branch(trunk_branch_merger *merger,
                               cache               *cc,
                               const btree_config  *btree_cfg,
                               uint64               addr,
                               page_type            type)
{
   btree_iterator *iter = TYPED_MALLOC(merger->hid, iter);
   if (iter == NULL) {
      platform_error_log(
         "%s():%d: platform_malloc() failed", __func__, __LINE__);
      return STATUS_NO_MEMORY;
   }
   btree_iterator_init(cc,
                       btree_cfg,
                       iter,
                       addr,
                       type,
                       merger->min_key,
                       merger->max_key,
                       merger->min_key,
                       greater_than_or_equal,
                       TRUE,
                       merger->height);
   platform_status rc = vector_append(&merger->itors, (iterator *)iter);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: vector_append() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
   }
   return STATUS_OK;
}


static platform_status
trunk_branch_merger_add_branches(trunk_branch_merger     *merger,
                                 cache                   *cc,
                                 const btree_config      *btree_cfg,
                                 uint64                   num_branches,
                                 const trunk_branch_info *branches)
{
   platform_status rc = vector_ensure_capacity(
      &merger->itors, vector_length(&merger->itors) + num_branches);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: vector_ensure_capacity() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      return rc;
   }

   for (uint64 i = 0; i < num_branches; i++) {
      rc = trunk_branch_merger_add_branch(
         merger, cc, btree_cfg, branches[i].addr, branches[i].type);
      if (!SUCCESS(rc)) {
         platform_error_log("%s():%d: btree_merger_add_branch() failed: %s",
                            __func__,
                            __LINE__,
                            platform_status_to_string(rc));
         return rc;
      }
   }
   return STATUS_OK;
}

static platform_status
trunk_branch_merger_add_bundle(trunk_branch_merger *merger,
                               cache               *cc,
                               const btree_config  *btree_cfg,
                               const bundle        *routed)
{
   platform_status rc = vector_ensure_capacity(
      &merger->itors,
      vector_length(&merger->itors) + bundle_num_branches(routed));
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: vector_ensure_capacity() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      return rc;
   }

   for (uint64 i = 0; i < bundle_num_branches(routed); i++) {
      branch_ref bref = vector_get(&routed->branches, i);
      rc              = trunk_branch_merger_add_branch(merger,
                                          cc,
                                          btree_cfg,
                                          branch_ref_addr(bref),
                                          bundle_branch_type(routed));
      if (!SUCCESS(rc)) {
         platform_error_log("%s():%d: btree_merger_add_branch() failed: %s",
                            __func__,
                            __LINE__,
                            platform_status_to_string(rc));
         return rc;
      }
   }
   return STATUS_OK;
}

static platform_status
trunk_branch_merger_build_merge_itor(trunk_branch_merger *merger,
                                     merge_behavior       merge_mode)
{
   platform_assert(merger->merge_itor == NULL);

   return merge_iterator_create(merger->hid,
                                merger->data_cfg,
                                vector_length(&merger->itors),
                                vector_data(&merger->itors),
                                merge_mode,
                                TRUE,
                                &merger->merge_itor);
}

static platform_status
trunk_branch_merger_deinit(trunk_branch_merger *merger)
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
 * concurrency in accessing the root
 ************************/

static void
trunk_read_begin(trunk_context *context)
{
   platform_batch_rwlock_get(&context->root_lock, 0);
}

static void
trunk_read_end(trunk_context *context)
{
   platform_batch_rwlock_unget(&context->root_lock, 0);
}

platform_status
trunk_init_root_handle(trunk_context *context, trunk_ondisk_node_handle *handle)
{
   platform_status rc;
   trunk_read_begin(context);
   if (context->root == NULL) {
      handle->cc                   = context->cc;
      handle->header_page          = NULL;
      handle->pivot_page           = NULL;
      handle->inflight_bundle_page = NULL;
      rc                           = STATUS_OK;
   } else {
      rc = trunk_ondisk_node_handle_init(
         handle, context->cc, context->root->addr);
   }
   trunk_read_end(context);
   return rc;
}

uint64
trunk_ondisk_node_handle_addr(const trunk_ondisk_node_handle *handle)
{
   return handle->header_page == NULL ? 0 : handle->header_page->disk_addr;
}

void
trunk_modification_begin(trunk_context *context)
{
   platform_batch_rwlock_get(&context->root_lock, 0);
   platform_batch_rwlock_claim_loop(&context->root_lock, 0);
}

static void
trunk_set_root(trunk_context *context, trunk_ondisk_node_ref *new_root_ref)
{
   trunk_ondisk_node_ref *old_root_ref;
   platform_batch_rwlock_lock(&context->root_lock, 0);
   old_root_ref  = context->root;
   context->root = new_root_ref;
   platform_batch_rwlock_unlock(&context->root_lock, 0);
   if (old_root_ref != NULL) {
      trunk_ondisk_node_ref_destroy(old_root_ref, context, context->hid);
   }
}

static void
perform_pending_gcs(trunk_context *context)
{
   pending_gcs_lock(context);

   pending_gc *pgc = context->pending_gcs;

   while (pgc && !cache_in_use(context->cc, pgc->addr)) {
      trunk_ondisk_node_gc(context, pgc->addr);
      pending_gc *next = pgc->next;
      platform_free(context->hid, pgc);
      pgc = next;
   }

   context->pending_gcs = pgc;
   if (pgc == NULL) {
      context->pending_gcs_tail = NULL;
   }

   pending_gcs_unlock(context);
}

void
trunk_modification_end(trunk_context *context)
{
   platform_batch_rwlock_unclaim(&context->root_lock, 0);
   platform_batch_rwlock_unget(&context->root_lock, 0);
   perform_pending_gcs(context);
}

/*************************
 * generic code to apply changes to nodes in the tree.
 ************************/

typedef platform_status(trunk_apply_changes_fn)(trunk_context *context,
                                                uint64         addr,
                                                trunk_node    *node,
                                                void          *arg);

static trunk_ondisk_node_ref *
trunk_apply_changes_internal(trunk_context          *context,
                             uint64                  addr,
                             key                     minkey,
                             key                     maxkey,
                             uint64                  height,
                             trunk_apply_changes_fn *func,
                             void                   *arg)
{
   platform_status rc;

   trunk_node node;
   rc = trunk_node_deserialize(context, addr, &node);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: node_deserialize() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      return NULL;
   }

   trunk_ondisk_node_ref_vector new_child_refs;
   vector_init(&new_child_refs, context->hid);

   if (trunk_node_height(&node) == height) {
      rc = func(context, addr, &node, arg);
   } else {
      rc = vector_ensure_capacity(&new_child_refs,
                                  trunk_node_num_children(&node));
      if (SUCCESS(rc)) {
         for (uint64 i = 0; i < trunk_node_num_children(&node); i++) {
            trunk_pivot *child_pivot  = trunk_node_pivot(&node, i);
            key          child_minkey = trunk_pivot_key(child_pivot);
            key          child_maxkey = trunk_node_pivot_key(&node, i + 1);
            if (data_key_compare(context->cfg->data_cfg, child_minkey, maxkey)
                   < 0
                && data_key_compare(
                      context->cfg->data_cfg, minkey, child_maxkey)
                      < 0)
            {
               uint64 child_addr = trunk_pivot_child_addr(child_pivot);
               trunk_ondisk_node_ref *new_child_ref =
                  trunk_apply_changes_internal(
                     context, child_addr, minkey, maxkey, height, func, arg);
               if (new_child_ref == NULL) {
                  platform_error_log("%s():%d: apply_changes_internal() failed",
                                     __func__,
                                     __LINE__);
                  rc = STATUS_NO_MEMORY;
                  break;
               }
               rc = vector_append(&new_child_refs, new_child_ref);
               platform_assert_status_ok(rc);

               trunk_pivot_set_child_addr(child_pivot, new_child_ref->addr);
            }
         }
      }
   }

   trunk_ondisk_node_ref *result = NULL;
   if (SUCCESS(rc)) {
      result = trunk_node_serialize(context, &node);
   }

   trunk_node_deinit(&node, context);
   VECTOR_APPLY_TO_ELTS(
      &new_child_refs, trunk_ondisk_node_ref_destroy, context, context->hid);
   vector_deinit(&new_child_refs);

   return result;
}

static platform_status
trunk_apply_changes(trunk_context          *context,
                    key                     minkey,
                    key                     maxkey,
                    uint64                  height,
                    trunk_apply_changes_fn *func,
                    void                   *arg)
{
   trunk_ondisk_node_ref *new_root_ref = trunk_apply_changes_internal(
      context, context->root->addr, minkey, maxkey, height, func, arg);
   if (new_root_ref != NULL) {
      trunk_set_root(context, new_root_ref);
   } else {
      platform_error_log(
         "%s():%d: apply_changes_internal() failed", __func__, __LINE__);
   }
   return new_root_ref == NULL ? STATUS_NO_MEMORY : STATUS_OK;
}

/*******************************************************************************
 * pivot state tracking
 ******************************************************************************/

uint64 bc_incs = 0;
uint64 bc_decs = 0;

static void
bundle_compaction_print_table_header(platform_log_handle *log, int indent)
{
   platform_log(log,
                "%*s%10s %12s %12s %5s %12s %12s %12s %18s %s\n",
                indent,
                "",
                "nbundles",
                "in_tuples",
                "in_kvbytes",
                "state",
                "out_branch",
                "out_tuples",
                "out_kvbytes",
                "fprints",
                "in_branches");
}
static void
bundle_compaction_print_table_entry(const bundle_compaction *bc,
                                    platform_log_handle     *log,
                                    int                      indent)
{
   platform_log(log,
                "%*s%10lu %12lu %12lu %5d %12lu %12lu %12lu %18p ",
                indent,
                "",
                bc->num_bundles,
                bc->input_stats.num_tuples,
                bc->input_stats.num_kv_bytes,
                bc->state,
                branch_ref_addr(bc->output_branch),
                bc->output_stats.num_tuples,
                bc->output_stats.num_kv_bytes,
                bc->fingerprints);
   for (uint64 i = 0; i < vector_length(&bc->input_branches); i++) {
      platform_log(log, "%lu ", vector_get(&bc->input_branches, i).addr);
   }
   platform_log(log, "\n");
}

static void
bundle_compaction_destroy(bundle_compaction *compaction, trunk_context *context)
{
   // platform_default_log("bundle_compaction_destroy: %p\n", compaction);
   // bundle_compaction_print_table_header(Platform_default_log_handle, 4);
   // bundle_compaction_print_table_entry(
   //    compaction, Platform_default_log_handle, 4);

   for (uint64 i = 0; i < vector_length(&compaction->input_branches); i++) {
      trunk_branch_info bi = vector_get(&compaction->input_branches, i);
      btree_dec_ref(context->cc, context->cfg->btree_cfg, bi.addr, bi.type);
      __sync_fetch_and_add(&bc_decs, 1);
   }
   vector_deinit(&compaction->input_branches);

   if (compaction->fingerprints) {
      platform_free(context->hid, compaction->fingerprints);
   }

   if (!branch_is_null(compaction->output_branch)) {
      btree_dec_ref(context->cc,
                    context->cfg->btree_cfg,
                    branch_ref_addr(compaction->output_branch),
                    PAGE_TYPE_BRANCH);
   }

   platform_free(context->hid, compaction);
}

static bundle_compaction *
bundle_compaction_create(trunk_context     *context,
                         trunk_node        *node,
                         uint64             pivot_num,
                         trunk_pivot_state *state)
{
   platform_status rc;
   trunk_pivot    *pvt      = trunk_node_pivot(node, pivot_num);
   bundle         *pvt_bndl = vector_get_ptr(&node->pivot_bundles, pivot_num);

   bundle_compaction *result = TYPED_ZALLOC(context->hid, result);
   if (result == NULL) {
      platform_error_log(
         "%s():%d: platform_malloc() failed", __func__, __LINE__);
      return NULL;
   }
   result->state       = BUNDLE_COMPACTION_NOT_STARTED;
   result->input_stats = trunk_pivot_received_bundles_stats(pvt);

   if (trunk_node_is_leaf(node) && state->bundle_compactions == NULL
       && bundle_num_branches(pvt_bndl) == 0)
   {
      result->merge_mode = MERGE_FULL;
   } else {
      result->merge_mode = MERGE_INTERMEDIATE;
   }

   vector_init(&result->input_branches, context->hid);
   int64  num_old_bundles = state->total_bundles;
   uint64 first_new_bundle =
      trunk_pivot_inflight_bundle_start(pvt) + num_old_bundles;
   platform_assert(first_new_bundle == node->num_old_bundles);

   for (int64 i = first_new_bundle; i < vector_length(&node->inflight_bundles);
        i++)
   {
      bundle *bndl = vector_get_ptr(&node->inflight_bundles, i);
      rc           = vector_ensure_capacity(&result->input_branches,
                                  vector_length(&result->input_branches)
                                     + vector_length(&bndl->branches));
      if (!SUCCESS(rc)) {
         platform_error_log("%s():%d: vector_ensure_capacity() failed: %s",
                            __func__,
                            __LINE__,
                            platform_status_to_string(rc));
         bundle_compaction_destroy(result, context);
         return NULL;
      }
      for (int64 j = 0; j < bundle_num_branches(bndl); j++) {
         branch_ref bref = vector_get(&bndl->branches, j);
         btree_inc_ref(
            context->cc, context->cfg->btree_cfg, branch_ref_addr(bref));
         page_type         type = bundle_branch_type(bndl);
         trunk_branch_info bi   = {bref.addr, type};
         rc                     = vector_append(&result->input_branches, bi);
         platform_assert_status_ok(rc);
         __sync_fetch_and_add(&bc_incs, 1);
      }
   }
   result->num_bundles =
      vector_length(&node->inflight_bundles) - first_new_bundle;

   platform_assert(0 < result->num_bundles);

   return result;
}

static uint64
trunk_pivot_state_map_hash(const data_config *data_cfg,
                           key                lbkey,
                           uint64             height)
{
   uint64 hash = data_key_hash(data_cfg, lbkey, 271828);
   hash ^= height;
   return hash % TRUNK_PIVOT_STATE_MAP_BUCKETS;
}

typedef uint64 pivot_state_map_lock;

static void
trunk_pivot_state_map_aquire_lock(pivot_state_map_lock  *lock,
                                  trunk_context         *context,
                                  trunk_pivot_state_map *map,
                                  key                    pivot_key,
                                  uint64                 height)
{
   *lock =
      trunk_pivot_state_map_hash(context->cfg->data_cfg, pivot_key, height);
   uint64 wait = 1;
   while (__sync_val_compare_and_swap(&map->locks[*lock], 0, 1) != 0) {
      platform_sleep_ns(wait);
      wait = MIN(2 * wait, 2048);
   }
}

static void
trunk_pivot_state_map_release_lock(pivot_state_map_lock  *lock,
                                   trunk_pivot_state_map *map)
{
   __sync_lock_release(&map->locks[*lock]);
}

static void
trunk_pivot_state_incref(trunk_pivot_state *state)
{
   __sync_fetch_and_add(&state->refcount, 1);
}

static uint64
trunk_pivot_state_decref(trunk_pivot_state *state)
{
   uint64 oldrc = __sync_fetch_and_add(&state->refcount, -1);
   platform_assert(0 < oldrc);
   return oldrc - 1;
}

static void
trunk_pivot_state_lock_compactions(trunk_pivot_state *state)
{
   platform_spin_lock(&state->compactions_lock);
}

static void
trunk_pivot_state_unlock_compactions(trunk_pivot_state *state)
{
   platform_spin_unlock(&state->compactions_lock);
}

debug_only static void
trunk_pivot_state_print(trunk_pivot_state   *state,
                        platform_log_handle *log,
                        const data_config   *data_cfg,
                        int                  indent)
{
   platform_log(log, "%*sheight: %lu\n", indent, "", state->height);
   platform_log(log,
                "%*skey: %s\n",
                indent,
                "",
                key_string(data_cfg, key_buffer_key(&state->key)));
   platform_log(log,
                "%*subkey: %s\n",
                indent,
                "",
                key_string(data_cfg, key_buffer_key(&state->ubkey)));
   platform_log(log, "%*smaplet: %lu\n", indent, "", state->maplet.addr);
   platform_log(log, "%*snum_branches: %lu\n", indent, "", state->num_branches);
   platform_log(log,
                "%*smaplet_compaction_failed: %d\n",
                indent,
                "",
                state->maplet_compaction_failed);

   trunk_pivot_state_lock_compactions(state);
   bundle_compaction_print_table_header(log, indent + 4);
   for (bundle_compaction *bc = state->bundle_compactions; bc != NULL;
        bc                    = bc->next)
   {
      bundle_compaction_print_table_entry(bc, log, indent + 4);
   }
   trunk_pivot_state_unlock_compactions(state);
}

debug_only static void
trunk_pivot_state_map_print(trunk_pivot_state_map *map,
                            platform_log_handle   *log,
                            const data_config     *data_cfg)
{
   platform_log(log, "pivot_state_map: %lu states\n", map->num_states);
   for (uint64 i = 0; i < TRUNK_PIVOT_STATE_MAP_BUCKETS; i++) {
      trunk_pivot_state *state = map->buckets[i];
      while (state != NULL) {
         trunk_pivot_state_print(state, log, data_cfg, 0);
         state = state->next;
      }
   }
}

static uint64 pivot_state_destructions = 0;

static void
trunk_pivot_state_destroy(trunk_pivot_state *state)
{
   trunk_context *context = state->context;
   threadid       tid     = platform_get_tid();
   platform_assert(state->refcount == 0);
   // platform_default_log("pivot_state_destroy: %p\n", state);
   // pivot_compaction_state_print(
   //    state, Platform_default_log_handle, state->context->cfg->data_cfg, 4);
   key_buffer_deinit(&state->key);
   routing_filter_dec_ref(state->context->cc, &state->maplet);
   trunk_pivot_state_lock_compactions(state);
   bundle_compaction *bc = state->bundle_compactions;
   while (bc != NULL) {
      if (context->stats) {
         if (bc->state == BUNDLE_COMPACTION_SUCCEEDED) {
            // Any completed bundle compactions still hanging off of this state
            // were never applied.
            context->stats[tid].compactions_discarded[state->height]++;
            context->stats[tid].compaction_time_wasted_ns[state->height] +=
               bc->compaction_time_ns;
         }
      }
      bundle_compaction *next = bc->next;
      bundle_compaction_destroy(bc, state->context);
      bc = next;
   }
   trunk_pivot_state_unlock_compactions(state);
   platform_spinlock_destroy(&state->compactions_lock);
   platform_free(state->context->hid, state);
   __sync_fetch_and_add(&pivot_state_destructions, 1);
}

static void
trunk_pivot_state_append_compaction(trunk_pivot_state *state,
                                    bundle_compaction *compaction)
{
   platform_assert(compaction != NULL);
   platform_assert(0 < vector_length(&compaction->input_branches));
   trunk_pivot_state_lock_compactions(state);
   if (state->bundle_compactions == NULL) {
      state->bundle_compactions = compaction;
   } else {
      bundle_compaction *last = state->bundle_compactions;
      while (last->next != NULL) {
         last = last->next;
      }
      last->next = compaction;
   }
   state->total_bundles += compaction->num_bundles;
   trunk_pivot_state_unlock_compactions(state);
}

static void
trunk_pivot_state_map_init(trunk_pivot_state_map *map)
{
   ZERO_CONTENTS(map);
}

static void
trunk_pivot_state_map_deinit(trunk_pivot_state_map *map)
{
   ZERO_CONTENTS(map);
}

static trunk_pivot_state *
trunk_pivot_state_map_get_entry(trunk_context              *context,
                                trunk_pivot_state_map      *map,
                                const pivot_state_map_lock *lock,
                                key                         pivot_key,
                                uint64                      height)
{
   trunk_pivot_state *result = NULL;
   for (trunk_pivot_state *state = map->buckets[*lock]; state != NULL;
        state                    = state->next)
   {
      if (data_key_compare(
             context->cfg->data_cfg, key_buffer_key(&state->key), pivot_key)
             == 0
          && state->height == height)
      {
         result = state;
         break;
      }
   }
   return result;
}

static uint64 pivot_state_creations = 0;

static trunk_pivot_state *
trunk_pivot_state_map_create_entry(trunk_context              *context,
                                   trunk_pivot_state_map      *map,
                                   const pivot_state_map_lock *lock,
                                   key                         pivot_key,
                                   key                         ubkey,
                                   uint64                      height,
                                   const bundle               *pivot_bundle)
{
   trunk_pivot_state *state = TYPED_ZALLOC(context->hid, state);
   if (state == NULL) {
      platform_error_log(
         "%s():%d: platform_malloc() failed", __func__, __LINE__);
      return NULL;
   }

   state->refcount = 1;

   platform_status rc =
      key_buffer_init_from_key(&state->key, context->hid, pivot_key);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: key_buffer_init_from_key() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      platform_free(context->hid, state);
      return NULL;
   }
   rc = key_buffer_init_from_key(&state->ubkey, context->hid, ubkey);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: key_buffer_init_from_key() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      key_buffer_deinit(&state->key);
      platform_free(context->hid, state);
      return NULL;
   }
   state->context = context;
   state->height  = height;
   state->maplet  = pivot_bundle->maplet;
   routing_filter_inc_ref(context->cc, &state->maplet);
   state->num_branches = bundle_num_branches(pivot_bundle);
   platform_spinlock_init(&state->compactions_lock, NULL, context->hid);

   state->next         = map->buckets[*lock];
   map->buckets[*lock] = state;
   __sync_fetch_and_add(&map->num_states, 1);
   __sync_fetch_and_add(&pivot_state_creations, 1);

   return state;
}

static void
trunk_pivot_state_map_remove(trunk_pivot_state_map *map,
                             pivot_state_map_lock  *lock,
                             trunk_pivot_state     *tgt)
{
   trunk_pivot_state *prev = NULL;
   for (trunk_pivot_state *state = map->buckets[*lock]; state != NULL;
        prev = state, state = state->next)
   {
      if (state == tgt) {
         if (prev == NULL) {
            map->buckets[*lock] = state->next;
         } else {
            prev->next = state->next;
         }
         __sync_fetch_and_sub(&map->num_states, 1);
         break;
      }
   }
}

static trunk_pivot_state *
trunk_pivot_state_map_get_or_create_entry(trunk_context         *context,
                                          trunk_pivot_state_map *map,
                                          key                    pivot_key,
                                          key                    ubkey,
                                          uint64                 height,
                                          const bundle          *pivot_bundle)
{
   pivot_state_map_lock lock;
   trunk_pivot_state_map_aquire_lock(&lock, context, map, pivot_key, height);
   trunk_pivot_state *state =
      trunk_pivot_state_map_get_entry(context, map, &lock, pivot_key, height);
   if (state == NULL) {
      state = trunk_pivot_state_map_create_entry(
         context, map, &lock, pivot_key, ubkey, height, pivot_bundle);
   } else {
      trunk_pivot_state_incref(state);
   }
   trunk_pivot_state_map_release_lock(&lock, map);
   return state;
}

static void
trunk_pivot_state_map_release_entry(trunk_context         *context,
                                    trunk_pivot_state_map *map,
                                    trunk_pivot_state     *state)
{
   pivot_state_map_lock lock;
   trunk_pivot_state_map_aquire_lock(
      &lock, context, map, key_buffer_key(&state->key), state->height);
   if (0 == trunk_pivot_state_decref(state)) {
      trunk_pivot_state_map_remove(map, &lock, state);
      trunk_pivot_state_destroy(state);
   }
   trunk_pivot_state_map_release_lock(&lock, map);
}

static bool32
trunk_pivot_state_map_abandon_entry(trunk_context *context,
                                    key            k,
                                    uint64         height)
{
   bool32               result = FALSE;
   pivot_state_map_lock lock;
   trunk_pivot_state_map_aquire_lock(
      &lock, context, &context->pivot_states, k, height);
   trunk_pivot_state *pivot_state = trunk_pivot_state_map_get_entry(
      context, &context->pivot_states, &lock, k, height);
   if (pivot_state) {
      pivot_state->abandoned = TRUE;
      trunk_pivot_state_map_remove(&context->pivot_states, &lock, pivot_state);
      result = TRUE;
   }
   trunk_pivot_state_map_release_lock(&lock, &context->pivot_states);
   return result;
}

debug_only static void
print_pivot_states_for_node(trunk_context *context, trunk_node *node)
{
   uint64 height = trunk_node_height(node);
   for (int i = 0; i < trunk_node_num_children(node); i++) {
      key                  k = trunk_node_pivot_key(node, i);
      pivot_state_map_lock lock;
      trunk_pivot_state_map_aquire_lock(
         &lock, context, &context->pivot_states, k, height);
      trunk_pivot_state *state = trunk_pivot_state_map_get_entry(
         context, &context->pivot_states, &lock, k, height);
      if (state != NULL) {
         trunk_pivot_state_incref(state);
      }
      trunk_pivot_state_map_release_lock(&lock, &context->pivot_states);
      if (state != NULL) {
         trunk_pivot_state_print(
            state, Platform_error_log_handle, context->cfg->data_cfg, 4);
      } else {
         platform_error_log("    No pivot compaction state for pivot %d\n", i);
      }
      if (state != NULL) {
         trunk_pivot_state_decref(state);
      }
   }
}


/*********************************************
 * maplet compaction
 *********************************************/

typedef struct maplet_compaction_apply_args {
   trunk_pivot_state *state;
   uint64             num_input_bundles;
   routing_filter     new_maplet;
   branch_ref_vector  branches;
   trunk_pivot_stats  delta;
   // Outputs
   bool32 found_match;
} maplet_compaction_apply_args;

static bool32
pivot_matches_compaction(const trunk_context                *context,
                         trunk_node                         *target,
                         uint64                              pivot_num,
                         const maplet_compaction_apply_args *args)
{
   trunk_pivot *pvt        = trunk_node_pivot(target, pivot_num);
   bundle      *pivot_bndl = trunk_node_pivot_bundle(target, pivot_num);

   platform_assert(0 < args->num_input_bundles);
   platform_assert(args->state->bundle_compactions != NULL);
   platform_assert(
      0 < vector_length(&args->state->bundle_compactions->input_branches));

   bundle_compaction *oldest_bc = args->state->bundle_compactions;
   trunk_branch_info  oldest_input_branch =
      vector_get(&oldest_bc->input_branches, 0);

   uint64 ifs = trunk_pivot_inflight_bundle_start(pvt);
   if (vector_length(&target->inflight_bundles) < ifs + args->num_input_bundles)
   {
      return FALSE;
   }

   bundle    *ifbndl = vector_get_ptr(&target->inflight_bundles, ifs);
   branch_ref oldest_pivot_inflight_branch = bundle_branch(ifbndl, 0);

   bool32 result =
      data_key_compare(context->cfg->data_cfg,
                       key_buffer_key(&args->state->key),
                       trunk_pivot_key(pvt))
         == 0
      && data_key_compare(context->cfg->data_cfg,
                          key_buffer_key(&args->state->ubkey),
                          trunk_node_pivot_key(target, pivot_num + 1))
            == 0
      && routing_filters_equal(&pivot_bndl->maplet, &args->state->maplet)
      && oldest_pivot_inflight_branch.addr == oldest_input_branch.addr;
   return result;
}

static platform_status
trunk_apply_changes_maplet_compaction(trunk_context *context,
                                      uint64         addr,
                                      trunk_node    *target,
                                      void          *arg)
{
   platform_status               rc;
   maplet_compaction_apply_args *args = (maplet_compaction_apply_args *)arg;

   for (uint64 i = 0; i < trunk_node_num_children(target); i++) {
      if (trunk_node_is_leaf(target)) {
         debug_assert(
            trunk_node_is_well_formed_leaf(context->cfg->data_cfg, target));
      } else {
         debug_assert(
            trunk_node_is_well_formed_index(context->cfg->data_cfg, target));
      }

      if (pivot_matches_compaction(context, target, i, args)) {
         bundle *bndl = trunk_node_pivot_bundle(target, i);
         rc = bundle_add_branches(bndl, args->new_maplet, &args->branches);
         if (!SUCCESS(rc)) {
            platform_error_log("apply_changes_maplet_compaction: "
                               "bundle_add_branches failed: %d\n",
                               rc.r);
            return rc;
         }
         trunk_pivot *pvt = trunk_node_pivot(target, i);
         trunk_pivot_set_inflight_bundle_start(
            pvt,
            trunk_pivot_inflight_bundle_start(pvt) + args->num_input_bundles);
         trunk_pivot_add_tuple_counts(pvt, -1, args->delta);
         args->found_match = TRUE;
         break;
      }
   }

   if (trunk_node_is_leaf(target)) {
      debug_assert(
         trunk_node_is_well_formed_leaf(context->cfg->data_cfg, target));
   } else {
      debug_assert(
         trunk_node_is_well_formed_index(context->cfg->data_cfg, target));
   }


   return STATUS_OK;
}

static platform_status
enqueue_maplet_compaction(trunk_pivot_state *args);

static void
maplet_compaction_task(void *arg, void *scratch)
{
   platform_status              rc         = STATUS_OK;
   trunk_pivot_state           *state      = (trunk_pivot_state *)arg;
   trunk_context               *context    = state->context;
   routing_filter               new_maplet = state->maplet;
   maplet_compaction_apply_args apply_args;
   threadid                     tid;

   tid = platform_get_tid();

   ZERO_STRUCT(apply_args);
   apply_args.state = state;
   vector_init(&apply_args.branches, context->hid);

   if (state->abandoned) {
      if (context->stats) {
         for (bundle_compaction *bc = state->bundle_compactions; bc != NULL;
              bc                    = bc->next)
         {
            context->stats[tid].maplet_builds_aborted[state->height]++;
         }
      }
      goto cleanup;
   }

   bundle_compaction *bc                  = state->bundle_compactions;
   bundle_compaction *last                = NULL;
   uint64             num_builds          = 0;
   uint64             total_build_time_ns = 0;
   while (bc != NULL && bc->state == BUNDLE_COMPACTION_SUCCEEDED) {
      if (!branch_is_null(bc->output_branch)) {
         uint64 filter_build_start;
         filter_build_start = platform_get_timestamp();

         routing_filter tmp_maplet;
         rc = routing_filter_add(context->cc,
                                 context->cfg->filter_cfg,
                                 &new_maplet,
                                 &tmp_maplet,
                                 bc->fingerprints,
                                 bc->output_stats.num_tuples,
                                 state->num_branches
                                    + vector_length(&apply_args.branches));
         if (new_maplet.addr != state->maplet.addr) {
            routing_filter_dec_ref(context->cc, &new_maplet);
         }
         if (!SUCCESS(rc)) {
            platform_error_log(
               "maplet_compaction_task: routing_filter_add failed: %d\n", rc.r);
            goto cleanup;
         }
         new_maplet = tmp_maplet;

         rc = vector_append(&apply_args.branches, bc->output_branch);
         if (!SUCCESS(rc)) {
            platform_error_log(
               "maplet_compaction_task: vector_append failed: %d\n", rc.r);
            goto cleanup;
         }

         num_builds++;
         uint64 filter_build_time_ns =
            platform_timestamp_elapsed(filter_build_start);
         total_build_time_ns += filter_build_time_ns;
         if (context->stats) {
            context->stats[tid].maplet_builds[state->height]++;
            context->stats[tid].maplet_build_time_ns[state->height] +=
               filter_build_time_ns;
            context->stats[tid].maplet_tuples[state->height] +=
               new_maplet.num_fingerprints;
            context->stats[tid].maplet_build_time_max_ns[state->height] =
               MAX(context->stats[tid].maplet_build_time_max_ns[state->height],
                   filter_build_time_ns);
         }
      }

      trunk_pivot_stats delta =
         trunk_pivot_stats_subtract(bc->input_stats, bc->output_stats);
      apply_args.delta = trunk_pivot_stats_add(apply_args.delta, delta);
      apply_args.num_input_bundles += bc->num_bundles;

      last = bc;
      bc   = bc->next;
   }

   platform_assert(last != NULL);
   platform_assert(0 < apply_args.num_input_bundles);

   if (context->stats) {
      context->stats[tid].maplet_build_time_ns[state->height] +=
         total_build_time_ns;
   }

   apply_args.new_maplet = new_maplet;

   trunk_modification_begin(context);

   rc = trunk_apply_changes(context,
                            key_buffer_key(&state->key),
                            key_buffer_key(&state->ubkey),
                            state->height,
                            trunk_apply_changes_maplet_compaction,
                            &apply_args);
   if (!SUCCESS(rc)) {
      platform_error_log("maplet_compaction_task: apply_changes failed: %d\n",
                         rc.r);
      trunk_modification_end(context);
      goto cleanup;
   }

   if (!apply_args.found_match) {
      if (!state->abandoned) {
         platform_error_log("Failed to find matching pivot for non-abandoned "
                            "compaction state\n");
         trunk_pivot_state_print(
            state, Platform_error_log_handle, context->cfg->data_cfg, 4);
      }

      pivot_state_map_lock lock;
      trunk_pivot_state_map_aquire_lock(&lock,
                                        context,
                                        &context->pivot_states,
                                        key_buffer_key(&state->key),
                                        state->height);
      trunk_pivot_state_map_remove(
         &context->pivot_states, &lock, apply_args.state);
      trunk_pivot_state_map_release_lock(&lock, &context->pivot_states);
      trunk_modification_end(context);

      if (context->stats) {
         context->stats[tid].maplet_builds_discarded[state->height] +=
            num_builds;
         context->stats[tid].maplet_build_time_wasted_ns[state->height] +=
            total_build_time_ns;
      }

      goto cleanup;
   }

   if (new_maplet.addr != state->maplet.addr) {
      routing_filter_dec_ref(context->cc, &state->maplet);
      state->maplet = new_maplet;
   }
   state->num_branches += vector_length(&apply_args.branches);
   trunk_pivot_state_lock_compactions(state);
   while (state->bundle_compactions != last) {
      bundle_compaction *next = state->bundle_compactions->next;
      state->total_bundles -= state->bundle_compactions->num_bundles;
      bundle_compaction_destroy(state->bundle_compactions, context);
      state->bundle_compactions = next;
   }
   platform_assert(state->bundle_compactions == last);
   state->bundle_compactions = last->next;
   state->total_bundles -= last->num_bundles;
   bundle_compaction_destroy(last, context);

   __sync_lock_release(&state->maplet_compaction_initiated);

   if (state->bundle_compactions
       && state->bundle_compactions->state == BUNDLE_COMPACTION_SUCCEEDED)
   {
      enqueue_maplet_compaction(state);
   }
   trunk_pivot_state_unlock_compactions(state);

   trunk_modification_end(context);

cleanup:
   if (!SUCCESS(rc) || !apply_args.found_match) {
      state->maplet_compaction_failed = TRUE;
      if (new_maplet.addr != state->maplet.addr) {
         routing_filter_dec_ref(context->cc, &new_maplet);
      }
   }

   trunk_pivot_state_map_release_entry(context, &context->pivot_states, state);
   vector_deinit(&apply_args.branches);
}

static platform_status
enqueue_maplet_compaction(trunk_pivot_state *args)
{
   if (__sync_lock_test_and_set(&args->maplet_compaction_initiated, 1)) {
      return STATUS_OK;
   }
   trunk_pivot_state_incref(args);
   platform_status rc = task_enqueue(
      args->context->ts, TASK_TYPE_NORMAL, maplet_compaction_task, args, FALSE);
   if (!SUCCESS(rc)) {
      platform_error_log("enqueue_maplet_compaction: task_enqueue failed: %d\n",
                         rc.r);
      trunk_pivot_state_decref(args);
   }
   return rc;
}

/************************
 * bundle compaction
 ************************/

static platform_status
compute_tuple_bound(trunk_context            *context,
                    trunk_branch_info_vector *branches,
                    key                       lb,
                    key                       ub,
                    uint64                   *tuple_bound)
{
   *tuple_bound = 0;
   for (uint64 i = 0; i < vector_length(branches); i++) {
      trunk_branch_info bi = vector_get(branches, i);
      btree_pivot_stats stats;
      btree_count_in_range(
         context->cc, context->cfg->btree_cfg, bi.addr, lb, ub, &stats);
      *tuple_bound += stats.num_kvs;
   }
   return STATUS_OK;
}


static void
bundle_compaction_task(void *arg, void *scratch)
{
   platform_status    rc;
   trunk_pivot_state *state   = (trunk_pivot_state *)arg;
   trunk_context     *context = state->context;
   threadid           tid     = platform_get_tid();

   if (context->stats) {
      context->stats[tid].compactions[state->height]++;
   }

   if (state->abandoned) {
      trunk_pivot_state_map_release_entry(
         context, &context->pivot_states, state);

      if (context->stats) {
         context->stats[tid].compactions_aborted[state->height]++;
      }
      return;
   }

   uint64 compaction_start = platform_get_timestamp();

   // Find a bundle compaction that needs doing for this pivot
   trunk_pivot_state_lock_compactions(state);
   bundle_compaction *bc = state->bundle_compactions;
   while (bc != NULL
          && !__sync_bool_compare_and_swap(&bc->state,
                                           BUNDLE_COMPACTION_NOT_STARTED,
                                           BUNDLE_COMPACTION_IN_PROGRESS))
   {
      bc = bc->next;
   }
   trunk_pivot_state_unlock_compactions(state);
   platform_assert(bc != NULL);
   platform_assert(0 < vector_length(&bc->input_branches));

   trunk_branch_merger merger;
   trunk_branch_merger_init(&merger,
                            context->hid,
                            context->cfg->data_cfg,
                            key_buffer_key(&state->key),
                            key_buffer_key(&state->ubkey),
                            0);
   rc = trunk_branch_merger_add_branches(&merger,
                                         context->cc,
                                         context->cfg->btree_cfg,
                                         vector_length(&bc->input_branches),
                                         vector_data(&bc->input_branches));
   if (!SUCCESS(rc)) {
      platform_error_log(
         "branch_merger_add_branches failed for state: %p bc: %p: %s\n",
         state,
         bc,
         platform_status_to_string(rc));
      goto cleanup;
   }

   uint64 tuple_bound;
   rc = compute_tuple_bound(context,
                            &bc->input_branches,
                            key_buffer_key(&state->key),
                            key_buffer_key(&state->ubkey),
                            &tuple_bound);
   if (!SUCCESS(rc)) {
      platform_error_log(
         "compute_tuple_bound failed for state: %p bc: %p: %s\n",
         state,
         bc,
         platform_status_to_string(rc));
      goto cleanup;
   }

   rc = trunk_branch_merger_build_merge_itor(&merger, bc->merge_mode);
   if (!SUCCESS(rc)) {
      platform_error_log(
         "branch_merger_build_merge_itor failed for state: %p bc: %p: %s\n",
         state,
         bc,
         platform_status_to_string(rc));
      goto cleanup;
   }

   btree_pack_req pack_req;
   btree_pack_req_init(&pack_req,
                       context->cc,
                       context->cfg->btree_cfg,
                       &merger.merge_itor->super,
                       tuple_bound,
                       context->cfg->filter_cfg->hash,
                       context->cfg->filter_cfg->seed,
                       context->hid);

   // This is just a quick shortcut to avoid wasting time on a compaction when
   // the pivot is already stuck due to an earlier maplet compaction failure.
   if (state->maplet_compaction_failed) {
      platform_error_log("maplet compaction failed, skipping bundle compaction "
                         "for state %p\n",
                         state);
      rc = STATUS_INVALID_STATE;
      goto cleanup;
   }

   uint64 pack_start = platform_get_timestamp();
   rc                = btree_pack(&pack_req);
   if (!SUCCESS(rc)) {
      platform_error_log("btree_pack failed for state: %p bc: %p: %s\n",
                         state,
                         bc,
                         platform_status_to_string(rc));
      goto cleanup;
   }
   if (context->stats) {
      context->stats[tid].compaction_pack_time_ns[state->height] +=
         platform_timestamp_elapsed(pack_start);
   }

   bc->output_branch = create_branch_ref(pack_req.root_addr);
   bc->output_stats  = (trunk_pivot_stats){
       .num_tuples   = pack_req.num_tuples,
       .num_kv_bytes = pack_req.key_bytes + pack_req.message_bytes};
   // trunk_pivot_stats_subtract(bc->input_stats, bc->output_stats);
   bc->fingerprints         = pack_req.fingerprint_arr;
   pack_req.fingerprint_arr = NULL;

   if (context->stats) {
      context->stats[tid].compaction_tuples[state->height] +=
         pack_req.num_tuples;
      context->stats[tid].compaction_max_tuples[state->height] =
         MAX(context->stats[tid].compaction_max_tuples[state->height],
             pack_req.num_tuples);
      bc->compaction_time_ns = platform_timestamp_elapsed(compaction_start);
      context->stats[tid].compaction_time_ns[state->height] +=
         bc->compaction_time_ns;
      context->stats[tid].compaction_time_max_ns[state->height] =
         MAX(context->stats[tid].compaction_time_max_ns[state->height],
             bc->compaction_time_ns);
   }

cleanup:
   btree_pack_req_deinit(&pack_req, context->hid);
   trunk_branch_merger_deinit(&merger);

   if (SUCCESS(rc)) {
      bc->state = BUNDLE_COMPACTION_SUCCEEDED;
   } else {
      bc->state = BUNDLE_COMPACTION_FAILED;
   }
   trunk_pivot_state_lock_compactions(state);
   if (bc->state == BUNDLE_COMPACTION_SUCCEEDED
       && state->bundle_compactions == bc)
   {
      enqueue_maplet_compaction(state);
   }
   trunk_pivot_state_unlock_compactions(state);
   trunk_pivot_state_map_release_entry(context, &context->pivot_states, state);
}

static platform_status
enqueue_bundle_compaction(trunk_context *context, trunk_node *node)
{
   uint64 height       = trunk_node_height(node);
   uint64 num_children = trunk_node_num_children(node);

   for (uint64 pivot_num = 0; pivot_num < num_children; pivot_num++) {
      if (trunk_node_pivot_has_received_bundles(node, pivot_num)) {
         platform_status rc        = STATUS_OK;
         key             pivot_key = trunk_node_pivot_key(node, pivot_num);
         key             ubkey     = trunk_node_pivot_key(node, pivot_num + 1);
         bundle *pivot_bundle      = trunk_node_pivot_bundle(node, pivot_num);

         trunk_pivot_state *state =
            trunk_pivot_state_map_get_or_create_entry(context,
                                                      &context->pivot_states,
                                                      pivot_key,
                                                      ubkey,
                                                      height,
                                                      pivot_bundle);
         if (state == NULL) {
            platform_error_log("enqueue_bundle_compaction: "
                               "pivot_state_map_get_or_create failed\n");
            rc = STATUS_NO_MEMORY;
            goto next;
         }

         bundle_compaction *bc =
            bundle_compaction_create(context, node, pivot_num, state);
         if (bc == NULL) {
            platform_error_log("enqueue_bundle_compaction: "
                               "bundle_compaction_create failed\n");
            rc = STATUS_NO_MEMORY;
            goto next;
         }

         trunk_pivot_state_append_compaction(state, bc);

         trunk_pivot_state_incref(state);
         rc = task_enqueue(context->ts,
                           TASK_TYPE_NORMAL,
                           bundle_compaction_task,
                           state,
                           FALSE);
         if (!SUCCESS(rc)) {
            trunk_pivot_state_decref(state);
            platform_error_log(
               "enqueue_bundle_compaction: task_enqueue failed\n");
         }

      next:
         if (!SUCCESS(rc) && bc) {
            bc->state = BUNDLE_COMPACTION_FAILED;
         }
         if (state != NULL) {
            trunk_pivot_state_map_release_entry(
               context, &context->pivot_states, state);
         }
      }
   }

   return STATUS_OK;
}

static void
incorporation_tasks_init(incorporation_tasks *itasks, platform_heap_id hid)
{
   vector_init(&itasks->node_compactions, hid);
}

static void
incorporation_tasks_deinit(incorporation_tasks *itasks, trunk_context *context)
{
   VECTOR_APPLY_TO_PTRS(&itasks->node_compactions, trunk_node_deinit, context);
   vector_deinit(&itasks->node_compactions);
}

static void
incorporation_tasks_execute(incorporation_tasks *itasks, trunk_context *context)
{
   for (uint64 i = 0; i < vector_length(&itasks->node_compactions); i++) {
      trunk_node     *node = vector_get_ptr(&itasks->node_compactions, i);
      platform_status rc   = enqueue_bundle_compaction(context, node);
      if (!SUCCESS(rc)) {
         platform_error_log("incorporation_tasks_execute: "
                            "enqueue_bundle_compaction failed: %d\n",
                            rc.r);
      }
   }
}

static platform_status
serialize_nodes_and_save_contingent_compactions(
   trunk_context                *context,
   trunk_node_vector            *nodes,
   trunk_ondisk_node_ref_vector *result,
   incorporation_tasks          *itasks)
{
   platform_status rc;

   rc = serialize_nodes(context, nodes, result);
   if (!SUCCESS(rc)) {
      platform_error_log("serialize_nodes_and_enqueue_bundle_compactions: "
                         "serialize_nodes failed: %d\n",
                         rc.r);
      return rc;
   }

   rc = vector_append_vector(&itasks->node_compactions, nodes);
   if (!SUCCESS(rc)) {
      VECTOR_APPLY_TO_ELTS(
         result, trunk_ondisk_node_ref_destroy, context, context->hid);
      vector_truncate(result, 0);
   }

   if (SUCCESS(rc)) {
      vector_truncate(nodes, 0);
   }

   return rc;
}


/************************
 * accounting maintenance
 ************************/

static platform_status
accumulate_branch_tuple_counts_in_range(branch_ref         bref,
                                        trunk_context     *context,
                                        key                minkey,
                                        key                maxkey,
                                        btree_pivot_stats *acc)
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

static platform_status
accumulate_branches_tuple_counts_in_range(const branch_ref_vector *brefs,
                                          trunk_context           *context,
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

static platform_status
accumulate_inflight_bundle_tuple_counts_in_range(bundle             *bndl,
                                                 trunk_context      *context,
                                                 trunk_pivot_vector *pivots,
                                                 uint64              child_num,
                                                 btree_pivot_stats  *acc)
{
   key minkey = trunk_pivot_key(vector_get(pivots, child_num));
   key maxkey = trunk_pivot_key(vector_get(pivots, child_num + 1));

   return accumulate_branches_tuple_counts_in_range(
      &bndl->branches, context, minkey, maxkey, acc);
}

/*****************************************************
 * Receive bundles -- used in flushes and leaf splits
 *****************************************************/

static platform_status
trunk_node_receive_bundles(trunk_context *context,
                           trunk_node    *node,
                           bundle        *pivot_bundle,
                           bundle_vector *inflight,
                           uint64         inflight_start)
{
   platform_status rc;

   rc = vector_ensure_capacity(&node->inflight_bundles,
                               vector_length(&node->inflight_bundles)
                                  + (pivot_bundle ? 1 : 0)
                                  + vector_length(inflight));
   if (!SUCCESS(rc)) {
      platform_error_log("node_receive_bundles: vector_ensure_capacity failed: "
                         "%d\n",
                         rc.r);
      return rc;
   }

   if (pivot_bundle && 0 < bundle_num_branches(pivot_bundle)) {
      rc = VECTOR_EMPLACE_APPEND(
         &node->inflight_bundles, bundle_init_copy, pivot_bundle, context->hid);
      if (!SUCCESS(rc)) {
         platform_error_log("node_receive_bundles: bundle_init_copy failed: "
                            "%d\n",
                            rc.r);
         return rc;
      }
   }

   for (uint64 i = inflight_start; i < vector_length(inflight); i++) {
      bundle *bndl = vector_get_ptr(inflight, i);
      rc           = VECTOR_EMPLACE_APPEND(
         &node->inflight_bundles, bundle_init_copy, bndl, context->hid);
      if (!SUCCESS(rc)) {
         platform_error_log("node_receive_bundles: bundle_init_copy failed: "
                            "%d\n",
                            rc.r);
         return rc;
      }
   }

   for (uint64 i = 0; i < trunk_node_num_children(node); i++) {
      btree_pivot_stats btree_stats;
      ZERO_CONTENTS(&btree_stats);
      if (pivot_bundle) {
         rc = accumulate_inflight_bundle_tuple_counts_in_range(
            pivot_bundle, context, &node->pivots, i, &btree_stats);
         if (!SUCCESS(rc)) {
            platform_error_log(
               "node_receive_bundles: "
               "accumulate_inflight_bundle_tuple_counts_in_range "
               "failed: %d\n",
               rc.r);
            return rc;
         }
      }
      for (uint64 j = inflight_start; j < vector_length(inflight); j++) {
         bundle *bndl = vector_get_ptr(inflight, j);
         rc           = accumulate_inflight_bundle_tuple_counts_in_range(
            bndl, context, &node->pivots, i, &btree_stats);
         if (!SUCCESS(rc)) {
            platform_error_log(
               "node_receive_bundles: "
               "accumulate_inflight_bundle_tuple_counts_in_range "
               "failed: %d\n",
               rc.r);
            return rc;
         }
      }
      trunk_pivot_stats trunk_stats =
         trunk_pivot_stats_from_btree_pivot_stats(btree_stats);
      trunk_pivot *pvt = trunk_node_pivot(node, i);
      trunk_pivot_add_tuple_counts(pvt, 1, trunk_stats);
   }

   return rc;
}

/************************
 * leaf splits
 ************************/

static bool
leaf_might_need_to_split(const trunk_config *cfg,
                         uint64              routing_filter_tuple_limit,
                         trunk_node         *leaf)
{
   return routing_filter_tuple_limit < trunk_leaf_num_tuples(leaf)
          || cfg->incorporation_size_kv_bytes * cfg->target_fanout
                < trunk_leaf_num_kv_bytes(leaf);
}

static platform_status
leaf_estimate_unique_keys(trunk_context *context,
                          trunk_node    *leaf,
                          uint64        *estimate)
{
   platform_status rc;

   debug_assert(trunk_node_is_well_formed_leaf(context->cfg->data_cfg, leaf));

   routing_filter_vector maplets;
   vector_init(&maplets, context->hid);
   rc = vector_ensure_capacity(&maplets,
                               vector_length(&leaf->inflight_bundles) + 1);
   if (!SUCCESS(rc)) {
      platform_error_log("leaf_estimate_unique_keys: vector_ensure_capacity "
                         "failed: %d\n",
                         rc.r);
      goto cleanup;
   }

   // rc = VECTOR_MAP_PTRS(&maplets, bundle_maplet, &leaf->inflight_bundles);
   // if (!SUCCESS(rc)) {
   //    platform_error_log("leaf_estimate_unique_keys: VECTOR_MAP_PTRS failed:
   //    "
   //                       "%d\n",
   //                       rc.r);
   //    goto cleanup;
   // }

   // bundle pivot_bundle = vector_get(&leaf->pivot_bundles, 0);
   // rc                  = vector_append(&maplets,
   // bundle_maplet(&pivot_bundle)); if (!SUCCESS(rc)) {
   //    platform_error_log(
   //       "leaf_estimate_unique_keys: vector_append failed: %d\n", rc.r);
   //    goto cleanup;
   // }

   uint64 unfiltered_tuples = 0;
   uint64 num_fp            = 0;
   uint64 num_unique_fp     = 0;
   for (uint16 inflight_maplet_num = 0;
        inflight_maplet_num < vector_length(&leaf->inflight_bundles);
        inflight_maplet_num++)
   {
      bundle *bndl =
         vector_get_ptr(&leaf->inflight_bundles, inflight_maplet_num);
      routing_filter maplet = bundle_maplet(bndl);
      if (routing_filters_equal(&maplet, &NULL_ROUTING_FILTER)) {
         btree_pivot_stats stats;
         platform_assert(bundle_num_branches(bndl) <= 1);
         btree_count_in_range(context->cc,
                              context->cfg->btree_cfg,
                              bundle_branch(bndl, 0).addr,
                              trunk_node_pivot_min_key(leaf),
                              trunk_node_pivot_max_key(leaf),
                              &stats);
         unfiltered_tuples += stats.num_kvs;
      } else {
         rc = vector_append(&maplets, maplet);
         platform_assert_status_ok(rc);
         num_fp += maplet.num_fingerprints;
         num_unique_fp += maplet.num_unique;
      }
   }

   bundle pivot_bundle = vector_get(&leaf->pivot_bundles, 0);
   rc                  = vector_append(&maplets, bundle_maplet(&pivot_bundle));
   platform_assert_status_ok(rc);

   *estimate = unfiltered_tuples;

   if (0 < num_fp) {
      uint32 num_globally_unique_fp =
         routing_filter_estimate_unique_fp(context->cc,
                                           context->cfg->filter_cfg,
                                           context->hid,
                                           vector_data(&maplets),
                                           vector_length(&maplets));

      num_globally_unique_fp = routing_filter_estimate_unique_keys_from_count(
         context->cfg->filter_cfg, num_globally_unique_fp);

      uint64 num_tuples                 = trunk_leaf_num_tuples(leaf);
      uint64 est_num_leaf_sb_unique     = num_unique_fp * num_tuples / num_fp;
      uint64 est_num_non_leaf_sb_unique = num_fp - est_num_leaf_sb_unique;

      uint64 est_leaf_unique =
         num_globally_unique_fp - est_num_non_leaf_sb_unique;
      *estimate += est_leaf_unique;
   }

cleanup:
   vector_deinit(&maplets);
   return STATUS_OK;
}

static platform_status
leaf_split_target_num_leaves(trunk_context *context,
                             trunk_node    *leaf,
                             uint64        *target)
{
   debug_assert(trunk_node_is_well_formed_leaf(context->cfg->data_cfg, leaf));

   uint64 rflimit = routing_filter_max_fingerprints(
      cache_get_config(context->cc), context->cfg->filter_cfg);

   if (!leaf_might_need_to_split(context->cfg, rflimit, leaf)) {
      *target = 1;
      return STATUS_OK;
   }

   uint64          estimated_unique_keys;
   platform_status rc =
      leaf_estimate_unique_keys(context, leaf, &estimated_unique_keys);
   if (!SUCCESS(rc)) {
      platform_error_log("leaf_split_target_num_leaves: "
                         "leaf_estimate_unique_keys failed: %d\n",
                         rc.r);
      return rc;
   }

   uint64 num_tuples = trunk_leaf_num_tuples(leaf);
   if (estimated_unique_keys > num_tuples * 19 / 20) {
      estimated_unique_keys = num_tuples;
   }
   uint64 kv_bytes = trunk_leaf_num_kv_bytes(leaf);
   uint64 estimated_unique_kv_bytes =
      estimated_unique_keys * kv_bytes / num_tuples;
   uint64 target_num_leaves = (estimated_unique_kv_bytes
                               + context->cfg->incorporation_size_kv_bytes / 2)
                              / context->cfg->incorporation_size_kv_bytes;

   if (target_num_leaves < (num_tuples + rflimit - 1) / rflimit) {
      target_num_leaves = (num_tuples + rflimit - 1) / rflimit;
   }

   if (target_num_leaves < 1) {
      target_num_leaves = 1;
   }

   *target = target_num_leaves;

   return STATUS_OK;
}

typedef VECTOR(key_buffer) key_buffer_vector;

static platform_status
leaf_split_select_pivots(trunk_context     *context,
                         trunk_node        *leaf,
                         uint64             target_num_leaves,
                         key_buffer_vector *pivots)
{
   platform_status rc;
   trunk_pivot    *first   = vector_get(&leaf->pivots, 0);
   trunk_pivot    *last    = vector_get(&leaf->pivots, 1);
   key             min_key = ondisk_key_to_key(&first->key);
   key             max_key = ondisk_key_to_key(&last->key);

   rc = VECTOR_EMPLACE_APPEND(
      pivots, key_buffer_init_from_key, context->hid, min_key);
   if (!SUCCESS(rc)) {
      platform_error_log("leaf_split_select_pivots: "
                         "VECTOR_EMPLACE_APPEND failed: %d\n",
                         rc.r);
      goto cleanup;
   }

   trunk_branch_merger merger;
   trunk_branch_merger_init(&merger,
                            context->hid,
                            context->cfg->data_cfg,
                            min_key,
                            max_key,
                            context->cfg->branch_rough_count_height);

   rc = trunk_branch_merger_add_bundle(&merger,
                                       context->cc,
                                       context->cfg->btree_cfg,
                                       vector_get_ptr(&leaf->pivot_bundles, 0));
   if (!SUCCESS(rc)) {
      platform_error_log("leaf_split_select_pivots: "
                         "branch_merger_add_bundle failed: %d\n",
                         rc.r);
      goto cleanup;
   }

   for (uint64 bundle_num = trunk_pivot_inflight_bundle_start(first);
        bundle_num < vector_length(&leaf->inflight_bundles);
        bundle_num++)
   {
      bundle *bndl = vector_get_ptr(&leaf->inflight_bundles, bundle_num);
      rc           = trunk_branch_merger_add_bundle(
         &merger, context->cc, context->cfg->btree_cfg, bndl);
      if (!SUCCESS(rc)) {
         platform_error_log("leaf_split_select_pivots: "
                            "branch_merger_add_bundle failed: %d\n",
                            rc.r);
         goto cleanup;
      }
   }

   rc = trunk_branch_merger_build_merge_itor(&merger, MERGE_RAW);
   if (!SUCCESS(rc)) {
      platform_error_log("leaf_split_select_pivots: "
                         "branch_merger_build_merge_itor failed: %d\n",
                         rc.r);
      goto cleanup;
   }

   uint64 rflimit = routing_filter_max_fingerprints(
      cache_get_config(context->cc), context->cfg->filter_cfg);
   uint64 leaf_num            = 1;
   uint64 cumulative_kv_bytes = 0;
   uint64 current_tuples      = 0;
   while (iterator_can_next(&merger.merge_itor->super)
          && leaf_num < target_num_leaves)
   {
      key     curr_key;
      message pivot_data_message;
      iterator_curr(&merger.merge_itor->super, &curr_key, &pivot_data_message);
      const btree_pivot_data *pivot_data = message_data(pivot_data_message);
      uint64                  new_cumulative_kv_bytes = cumulative_kv_bytes
                                       + pivot_data->stats.key_bytes
                                       + pivot_data->stats.message_bytes;
      uint64 new_tuples = current_tuples + pivot_data->stats.num_kvs;
      uint64 next_boundary =
         leaf_num * trunk_leaf_num_kv_bytes(leaf) / target_num_leaves;
      if ((cumulative_kv_bytes < next_boundary
           && next_boundary <= new_cumulative_kv_bytes)
          || rflimit < new_tuples)
      {
         rc = VECTOR_EMPLACE_APPEND(
            pivots, key_buffer_init_from_key, context->hid, curr_key);
         if (!SUCCESS(rc)) {
            platform_error_log("leaf_split_select_pivots: "
                               "VECTOR_EMPLACE_APPEND failed: %d\n",
                               rc.r);
            goto cleanup;
         }
         leaf_num++;
         current_tuples = 0;
      }

      cumulative_kv_bytes = new_cumulative_kv_bytes;
      current_tuples += pivot_data->stats.num_kvs;
      iterator_next(&merger.merge_itor->super);
   }

   rc = VECTOR_EMPLACE_APPEND(
      pivots, key_buffer_init_from_key, context->hid, max_key);
   if (!SUCCESS(rc)) {
      platform_error_log("leaf_split_select_pivots: "
                         "VECTOR_EMPLACE_APPEND failed: %d\n",
                         rc.r);
      goto cleanup;
   }

   platform_status deinit_rc;
cleanup:
   deinit_rc = trunk_branch_merger_deinit(&merger);
   if (!SUCCESS(rc)) {
      for (uint64 i = 0; i < vector_length(pivots); i++) {
         key_buffer_deinit(vector_get_ptr(pivots, i));
      }
      return rc;
   }
   return deinit_rc;
}

static platform_status
leaf_split_init(trunk_node    *new_leaf,
                trunk_context *context,
                trunk_node    *leaf,
                key            min_key,
                key            max_key)
{
   platform_status rc;
   platform_assert(trunk_node_is_leaf(leaf));

   trunk_pivot *pvt = trunk_node_pivot(leaf, 0);

   rc = trunk_node_init_empty_leaf(new_leaf, context->hid, min_key, max_key);
   if (!SUCCESS(rc)) {
      platform_error_log("leaf_split_init: node_init_empty_leaf failed: %d\n",
                         rc.r);
      return rc;
   }
   debug_assert(
      trunk_node_is_well_formed_leaf(context->cfg->data_cfg, new_leaf));

   return trunk_node_receive_bundles(context,
                                     new_leaf,
                                     trunk_node_pivot_bundle(leaf, 0),
                                     &leaf->inflight_bundles,
                                     trunk_pivot_inflight_bundle_start(pvt));
}

static uint64
trunk_node_pivot_eventual_num_branches(trunk_context *context,
                                       trunk_node    *node,
                                       uint64         pivot_num)
{
   uint64 num_branches = 0;

   bundle *bndl = trunk_node_pivot_bundle(node, pivot_num);
   num_branches += bundle_num_branches(bndl);

   /* Count the branches that will be added by inflight compactions. */
   pivot_state_map_lock lock;
   trunk_pivot_state_map_aquire_lock(&lock,
                                     context,
                                     &context->pivot_states,
                                     trunk_node_pivot_key(node, pivot_num),
                                     trunk_node_height(node));
   trunk_pivot_state *state =
      trunk_pivot_state_map_get_entry(context,
                                      &context->pivot_states,
                                      &lock,
                                      trunk_node_pivot_key(node, pivot_num),
                                      trunk_node_height(node));
   if (state != NULL) {
      trunk_pivot_state_lock_compactions(state);
      bundle_compaction *bc = state->bundle_compactions;
      while (bc != NULL) {
         num_branches++;
         bc = bc->next;
      }
      trunk_pivot_state_unlock_compactions(state);
   }
   trunk_pivot_state_map_release_lock(&lock, &context->pivot_states);

   if (trunk_node_pivot_has_received_bundles(node, pivot_num)) {
      num_branches++;
   }

   return num_branches;
}

static platform_status
leaf_split(trunk_context     *context,
           trunk_node        *leaf,
           trunk_node_vector *new_leaves,
           bool32            *abandon_compactions)
{
   platform_status rc;
   uint64          target_num_leaves;
   uint64          start_time = platform_get_timestamp();
   threadid        tid        = platform_get_tid();

   rc = leaf_split_target_num_leaves(context, leaf, &target_num_leaves);
   if (!SUCCESS(rc)) {
      platform_error_log(
         "leaf_split: leaf_split_target_num_leaves failed: %d\n", rc.r);
      return rc;
   }

   if (target_num_leaves == 1
       && trunk_node_pivot_eventual_num_branches(context, leaf, 0)
             <= context->cfg->target_fanout)
   {
      if (context->stats) {
         context->stats[tid].single_leaf_splits++;
      }
      *abandon_compactions = FALSE;
      return VECTOR_EMPLACE_APPEND(
         new_leaves, trunk_node_copy_init, leaf, context->hid);
   }

   if (context->stats) {
      context->stats[tid].node_splits[leaf->height]++;
      context->stats[tid].node_splits_nodes_created[leaf->height] +=
         target_num_leaves - 1;
   }


   key_buffer_vector pivots;
   vector_init(&pivots, context->hid);
   rc = vector_ensure_capacity(&pivots, target_num_leaves + 1);
   if (!SUCCESS(rc)) {
      platform_error_log("leaf_split: vector_ensure_capacity failed: %d\n",
                         rc.r);
      goto cleanup_pivots;
   }
   rc = leaf_split_select_pivots(context, leaf, target_num_leaves, &pivots);
   if (!SUCCESS(rc)) {
      platform_error_log("leaf_split: leaf_split_select_pivots failed: %d\n",
                         rc.r);
      goto cleanup_pivots;
   }

   for (uint64 i = 0; i < vector_length(&pivots) - 1; i++) {
      key min_key = key_buffer_key(vector_get_ptr(&pivots, i));
      key max_key = key_buffer_key(vector_get_ptr(&pivots, i + 1));
      rc          = VECTOR_EMPLACE_APPEND(
         new_leaves, leaf_split_init, context, leaf, min_key, max_key);
      if (!SUCCESS(rc)) {
         platform_error_log("leaf_split: leaf_split_init failed: %d\n", rc.r);
         goto cleanup_new_leaves;
      }
      debug_assert(trunk_node_is_well_formed_leaf(
         context->cfg->data_cfg, vector_get_ptr(new_leaves, i)));
   }

   *abandon_compactions = TRUE;

   if (context->stats) {
      uint64 elapsed_time = platform_timestamp_elapsed(start_time);
      context->stats[tid].leaf_split_time_ns += elapsed_time;
      context->stats[tid].leaf_split_time_max_ns =
         MAX(context->stats[tid].leaf_split_time_max_ns, elapsed_time);
   }

cleanup_new_leaves:
   if (!SUCCESS(rc)) {
      VECTOR_APPLY_TO_PTRS(new_leaves, trunk_node_deinit, context);
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
index_init_split(trunk_node      *new_index,
                 platform_heap_id hid,
                 trunk_node      *index,
                 uint64           start_child_num,
                 uint64           end_child_num)
{
   platform_status rc;

   trunk_pivot_vector pivots;
   vector_init(&pivots, hid);
   rc = vector_ensure_capacity(&pivots, end_child_num - start_child_num + 1);
   if (!SUCCESS(rc)) {
      platform_error_log(
         "index_init_split: vector_ensure_capacity failed: %d\n", rc.r);
      goto cleanup_pivots;
   }
   for (uint64 i = start_child_num; i < end_child_num + 1; i++) {
      trunk_pivot *pvt  = vector_get(&index->pivots, i);
      trunk_pivot *copy = trunk_pivot_copy(pvt, hid);
      if (copy == NULL) {
         platform_error_log("index_init_split: pivot_copy failed\n");
         rc = STATUS_NO_MEMORY;
         goto cleanup_pivots;
      }
      rc = vector_append(&pivots, copy);
      platform_assert_status_ok(rc);
   }

   bundle_vector pivot_bundles;
   vector_init(&pivot_bundles, hid);
   rc = vector_ensure_capacity(&pivot_bundles, end_child_num - start_child_num);
   if (!SUCCESS(rc)) {
      platform_error_log(
         "index_init_split: vector_ensure_capacity failed: %d\n", rc.r);
      goto cleanup_pivot_bundles;
   }
   for (uint64 i = start_child_num; i < end_child_num; i++) {
      rc = VECTOR_EMPLACE_APPEND(&pivot_bundles,
                                 bundle_init_copy,
                                 vector_get_ptr(&index->pivot_bundles, i),
                                 hid);
      if (!SUCCESS(rc)) {
         platform_error_log("index_init_split: bundle_init_copy failed: %d\n",
                            rc.r);
         goto cleanup_pivot_bundles;
      }
   }

   bundle_vector inflight_bundles;
   vector_init(&inflight_bundles, hid);
   rc = VECTOR_EMPLACE_MAP_PTRS(
      &inflight_bundles, bundle_init_copy, &index->inflight_bundles, hid);
   if (!SUCCESS(rc)) {
      platform_error_log("index_init_split: VECTOR_EMPLACE_MAP_PTRS failed: "
                         "%d\n",
                         rc.r);
      goto cleanup_inflight_bundles;
   }

   trunk_node_init(new_index,
                   trunk_node_height(index),
                   pivots,
                   pivot_bundles,
                   trunk_node_num_old_bundles(index),
                   inflight_bundles);

   return rc;

cleanup_inflight_bundles:
   VECTOR_APPLY_TO_PTRS(&inflight_bundles, bundle_deinit);
   vector_deinit(&inflight_bundles);
cleanup_pivot_bundles:
   VECTOR_APPLY_TO_PTRS(&pivot_bundles, bundle_deinit);
   vector_deinit(&pivot_bundles);
cleanup_pivots:
   VECTOR_APPLY_TO_ELTS(&pivots, trunk_pivot_destroy, hid);
   vector_deinit(&pivots);
   return rc;
}

static platform_status
index_split(trunk_context     *context,
            trunk_node        *index,
            trunk_node_vector *new_indexes)
{
   debug_assert(trunk_node_is_well_formed_index(context->cfg->data_cfg, index));
   platform_status rc;

   uint64 num_children = trunk_node_num_children(index);
   uint64 num_nodes    = (num_children + context->cfg->target_fanout - 1)
                      / context->cfg->target_fanout;

   if (context->stats && 1 < num_nodes) {
      threadid tid = platform_get_tid();
      context->stats[tid].node_splits[index->height]++;
      context->stats[tid].node_splits_nodes_created[index->height] +=
         num_nodes - 1;
   }


   for (uint64 i = 0; i < num_nodes; i++) {
      rc = VECTOR_EMPLACE_APPEND(new_indexes,
                                 index_init_split,
                                 context->hid,
                                 index,
                                 i * num_children / num_nodes,
                                 (i + 1) * num_children / num_nodes);
      if (!SUCCESS(rc)) {
         platform_error_log("index_split: index_init_split failed: %d\n", rc.r);
         goto cleanup_new_indexes;
      }
      debug_assert(trunk_node_is_well_formed_index(
         context->cfg->data_cfg, vector_get_ptr(new_indexes, i)));
   }

cleanup_new_indexes:
   if (!SUCCESS(rc)) {
      for (uint64 i = 0; i < vector_length(new_indexes); i++) {
         trunk_node_deinit(vector_get_ptr(new_indexes, i), context);
      }
      vector_truncate(new_indexes, 0);
   }

   return rc;
}

/***********************************
 * flushing
 ***********************************/

static uint64 abandoned_leaf_compactions = 0;

static platform_status
restore_balance_leaf(trunk_context                *context,
                     trunk_node                   *leaf,
                     trunk_ondisk_node_ref_vector *new_leaf_refs,
                     incorporation_tasks          *itasks)
{
   trunk_node_vector new_nodes;
   vector_init(&new_nodes, context->hid);

   bool32          abandon_compactions = FALSE;
   platform_status rc =
      leaf_split(context, leaf, &new_nodes, &abandon_compactions);
   if (!SUCCESS(rc)) {
      platform_error_log("restore_balance_leaf: leaf_split failed: %d\n", rc.r);
      goto cleanup_new_nodes;
   }

   if (abandon_compactions) {
      trunk_pivot_state_map_abandon_entry(
         context, trunk_node_pivot_min_key(leaf), trunk_node_height(leaf));
      abandoned_leaf_compactions++;
   }

   rc = serialize_nodes_and_save_contingent_compactions(
      context, &new_nodes, new_leaf_refs, itasks);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: serialize_nodes() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      goto cleanup_new_nodes;
   }


   vector_deinit(&new_nodes);

   return rc;

cleanup_new_nodes:
   VECTOR_APPLY_TO_PTRS(&new_nodes, trunk_node_deinit, context);
   vector_deinit(&new_nodes);
   return rc;
}

static platform_status
bundle_vector_init_empty(bundle_vector   *new_bundles,
                         uint64           num_bundles,
                         platform_heap_id hid)
{
   vector_init(new_bundles, hid);
   platform_status rc = vector_ensure_capacity(new_bundles, num_bundles);
   if (!SUCCESS(rc)) {
      platform_error_log("bundle_vector_init_empty: vector_ensure_capacity "
                         "failed: %d\n",
                         rc.r);
      vector_deinit(new_bundles);
      return rc;
   }
   for (uint64 j = 0; j < num_bundles; j++) {
      rc = VECTOR_EMPLACE_APPEND(new_bundles, bundle_init, hid);
      platform_assert_status_ok(rc);
   }

   return STATUS_OK;
}

static platform_status
flush_then_compact(trunk_context                *context,
                   trunk_node                   *node,
                   bundle                       *routed,
                   bundle_vector                *inflight,
                   uint64                        inflight_start,
                   trunk_ondisk_node_ref_vector *new_node_refs,
                   incorporation_tasks          *itasks);

static platform_status
flush_to_one_child(trunk_context                *context,
                   trunk_node                   *index,
                   uint64                        pivot_num,
                   trunk_ondisk_node_ref_vector *new_childrefs_accumulator,
                   incorporation_tasks          *itasks)
{
   platform_status rc = STATUS_OK;

   // Check whether we need to flush to this child
   trunk_pivot *pvt = trunk_node_pivot(index, pivot_num);

   // Start a timer
   uint64 flush_start;
   if (context->stats) {
      flush_start = platform_get_timestamp();
   }

   // Load the child
   trunk_node child;
   rc = trunk_node_deserialize(context, trunk_pivot_child_addr(pvt), &child);
   if (!SUCCESS(rc)) {
      platform_error_log("flush_to_one_child: node_deserialize failed: %d\n",
                         rc.r);
      return rc;
   }

   // Perform the flush, getting back the new children
   trunk_ondisk_node_ref_vector new_childrefs;
   vector_init(&new_childrefs, context->hid);
   rc = flush_then_compact(context,
                           &child,
                           trunk_node_pivot_bundle(index, pivot_num),
                           &index->inflight_bundles,
                           trunk_pivot_inflight_bundle_start(pvt),
                           &new_childrefs,
                           itasks);
   trunk_node_deinit(&child, context);
   if (!SUCCESS(rc)) {
      platform_error_log("flush_to_one_child: flush_then_compact failed: %d\n",
                         rc.r);
      goto cleanup_new_childrefs;
   }

   // Construct our new pivots for the new children
   trunk_pivot_vector new_pivots;
   vector_init(&new_pivots, context->hid);
   rc = vector_ensure_capacity(&new_pivots, vector_length(&new_childrefs));
   if (!SUCCESS(rc)) {
      platform_error_log("flush_to_one_child: vector_ensure_capacity failed: "
                         "%d\n",
                         rc.r);
      goto cleanup_new_pivots;
   }
   rc = VECTOR_MAP_ELTS(&new_pivots,
                        trunk_pivot_create_from_ondisk_node_ref,
                        &new_childrefs,
                        context->hid);
   if (!SUCCESS(rc)) {
      platform_error_log("flush_to_one_child: VECTOR_MAP_ELTS failed: %d\n",
                         rc.r);
      goto cleanup_new_pivots;
   }
   for (uint64 j = 0; j < vector_length(&new_pivots); j++) {
      trunk_pivot *new_pivot = vector_get(&new_pivots, j);
      trunk_pivot_set_inflight_bundle_start(
         new_pivot, vector_length(&index->inflight_bundles));
   }

   // Construct the new empty pivot bundles for the new children
   bundle_vector new_pivot_bundles;
   rc = bundle_vector_init_empty(
      &new_pivot_bundles, vector_length(&new_pivots), context->hid);
   if (!SUCCESS(rc)) {
      platform_error_log("flush_to_one_child: bundle_vector_init_empty failed: "
                         "%d\n",
                         rc.r);
      goto cleanup_new_pivots;
   }

   // Reserve room in the node for the new pivots and pivot bundles
   rc = vector_ensure_capacity(&index->pivots,
                               vector_length(&index->pivots)
                                  + vector_length(&new_pivots) - 1);
   if (!SUCCESS(rc)) {
      platform_error_log("flush_to_one_child: vector_ensure_capacity failed: "
                         "%d\n",
                         rc.r);
      goto cleanup_new_pivot_bundles;
   }
   // Reget this since the pointer may have
   // changed due to the vector_ensure_capacity
   pvt = trunk_node_pivot(index, pivot_num);
   rc  = vector_ensure_capacity(&index->pivot_bundles,
                               vector_length(&index->pivot_bundles)
                                  + vector_length(&new_pivot_bundles) - 1);
   if (!SUCCESS(rc)) {
      platform_error_log("flush_to_one_child: vector_ensure_capacity failed: "
                         "%d\n",
                         rc.r);
      goto cleanup_new_pivot_bundles;
   }

   rc = vector_append_vector(new_childrefs_accumulator, &new_childrefs);
   if (!SUCCESS(rc)) {
      platform_error_log(
         "flush_to_one_child: vector_append_vector failed: %d\n", rc.r);
      goto cleanup_new_pivot_bundles;
   }

   // We are guaranteed to succeed from here on out, so we can start modifying
   // the index in place.

   // Abandon the enqueued compactions now, before we destroy pvt.
   trunk_pivot_state_map_abandon_entry(
      context, trunk_pivot_key(pvt), trunk_node_height(index));

   // Replace the old pivot and pivot bundles with the new ones
   trunk_pivot_destroy(pvt, context->hid);
   rc = vector_replace(
      &index->pivots, pivot_num, 1, &new_pivots, 0, vector_length(&new_pivots));
   platform_assert_status_ok(rc);
   bundle_deinit(trunk_node_pivot_bundle(index, pivot_num));
   rc = vector_replace(&index->pivot_bundles,
                       pivot_num,
                       1,
                       &new_pivot_bundles,
                       0,
                       vector_length(&new_pivot_bundles));
   platform_assert_status_ok(rc);

   if (context->stats) {
      uint64   flush_time = platform_timestamp_elapsed(flush_start);
      threadid tid        = platform_get_tid();
      context->stats[tid].count_flushes[trunk_node_height(index)]++;
      context->stats[tid].flush_time_ns[trunk_node_height(index)] += flush_time;
      context->stats[tid].flush_time_max_ns[trunk_node_height(index)] =
         MAX(context->stats[tid].flush_time_max_ns[trunk_node_height(index)],
             flush_time);
   }

cleanup_new_pivot_bundles:
   vector_deinit(&new_pivot_bundles);
cleanup_new_pivots:
   vector_deinit(&new_pivots);
cleanup_new_childrefs:
   vector_deinit(&new_childrefs);
   return rc;
}

static platform_status
restore_balance_index(trunk_context                *context,
                      trunk_node                   *index,
                      trunk_ondisk_node_ref_vector *new_index_refs,
                      incorporation_tasks          *itasks)
{
   platform_status rc;
   threadid        tid     = platform_get_tid();
   uint64          rflimit = routing_filter_max_fingerprints(
      cache_get_config(context->cc), context->cfg->filter_cfg);

   debug_assert(trunk_node_is_well_formed_index(context->cfg->data_cfg, index));

   trunk_ondisk_node_ref_vector all_new_childrefs;
   vector_init(&all_new_childrefs, context->hid);

   uint64 fullest_child    = 0;
   uint64 fullest_kv_bytes = 0;
   for (uint64 i = 0; i < trunk_node_num_children(index); i++) {
      trunk_pivot *pvt = trunk_node_pivot(index, i);

      if (context->cfg->target_fanout
             < trunk_node_pivot_eventual_num_branches(context, index, i)
          || rflimit < pvt->stats.num_tuples)
      {
         rc = flush_to_one_child(context, index, i, &all_new_childrefs, itasks);
         if (!SUCCESS(rc)) {
            platform_error_log("%s():%d: flush_to_one_child() failed: %s",
                               __func__,
                               __LINE__,
                               platform_status_to_string(rc));
            goto cleanup_all_new_children;
         }

         if (context->stats) {
            context->stats[tid].full_flushes[trunk_node_height(index)]++;
         }

      } else if (fullest_kv_bytes < trunk_pivot_num_kv_bytes(pvt)) {
         fullest_child    = i;
         fullest_kv_bytes = trunk_pivot_num_kv_bytes(pvt);
      }
   }

   if (context->cfg->incorporation_size_kv_bytes < fullest_kv_bytes) {
      rc = flush_to_one_child(
         context, index, fullest_child, &all_new_childrefs, itasks);
      if (!SUCCESS(rc)) {
         platform_error_log("%s():%d: flush_to_one_child() failed: %s",
                            __func__,
                            __LINE__,
                            platform_status_to_string(rc));
         goto cleanup_all_new_children;
      }
   }

   trunk_node_vector new_nodes;
   vector_init(&new_nodes, context->hid);
   rc = index_split(context, index, &new_nodes);
   if (!SUCCESS(rc)) {
      platform_error_log("restore_balance_index: index_split failed: %d\n",
                         rc.r);
      goto cleanup_new_nodes;
   }

   rc = serialize_nodes_and_save_contingent_compactions(
      context, &new_nodes, new_index_refs, itasks);
   if (!SUCCESS(rc)) {
      platform_error_log(
         "%s():%d: serialize_nodes_and_save_contingent_compactions() failed: "
         "%s",
         __func__,
         __LINE__,
         platform_status_to_string(rc));
      goto cleanup_new_nodes;
   }

cleanup_new_nodes:
   if (!SUCCESS(rc)) {
      VECTOR_APPLY_TO_PTRS(&new_nodes, trunk_node_deinit, context);
   }
   vector_deinit(&new_nodes);

cleanup_all_new_children:
   VECTOR_APPLY_TO_ELTS(
      &all_new_childrefs, trunk_ondisk_node_ref_destroy, context, context->hid);
   vector_deinit(&all_new_childrefs);
   return rc;
}

/*
 * Flush the routed bundle and inflight bundles inflight[inflight_start...]
 * to the given node.
 *
 * child_num is the child number of the node addr within its parent.
 *
 * flush_then_compact may choose to split the node.  The resulting
 * node/nodes are returned in new_nodes.
 */
static platform_status
flush_then_compact(trunk_context                *context,
                   trunk_node                   *node,
                   bundle                       *routed,
                   bundle_vector                *inflight,
                   uint64                        inflight_start,
                   trunk_ondisk_node_ref_vector *new_node_refs,
                   incorporation_tasks          *itasks)
{
   platform_status rc;

   // Add the bundles to the node
   rc = trunk_node_receive_bundles(
      context, node, routed, inflight, inflight_start);
   if (!SUCCESS(rc)) {
      platform_error_log("%s():%d: node_receive_bundles() failed: %s",
                         __func__,
                         __LINE__,
                         platform_status_to_string(rc));
      return rc;
   }
   if (trunk_node_is_leaf(node)) {
      debug_assert(
         trunk_node_is_well_formed_leaf(context->cfg->data_cfg, node));
   } else {
      debug_assert(
         trunk_node_is_well_formed_index(context->cfg->data_cfg, node));
   }

   // Perform any needed recursive flushes and node splits
   if (trunk_node_is_leaf(node)) {
      rc = restore_balance_leaf(context, node, new_node_refs, itasks);
   } else {
      rc = restore_balance_index(context, node, new_node_refs, itasks);
   }

   return rc;
}

static platform_status
build_new_roots(trunk_context                *context,
                uint64                        height, // height of current root
                trunk_ondisk_node_ref_vector *node_refs)
{
   platform_status rc;

   debug_assert(1 < vector_length(node_refs));

   // Create the pivots vector for the new root
   trunk_pivot_vector pivots;
   vector_init(&pivots, context->hid);
   rc = vector_ensure_capacity(&pivots, vector_length(node_refs) + 1);
   if (!SUCCESS(rc)) {
      platform_error_log("build_new_roots: vector_ensure_capacity failed: %d\n",
                         rc.r);
      goto cleanup_pivots;
   }
   rc = VECTOR_MAP_ELTS(&pivots,
                        trunk_pivot_create_from_ondisk_node_ref,
                        node_refs,
                        context->hid);
   if (!SUCCESS(rc)) {
      platform_error_log("build_new_roots: VECTOR_MAP_ELTS failed: %d\n", rc.r);
      goto cleanup_pivots;
   }
   trunk_pivot *ub_pivot = trunk_pivot_create(context->hid,
                                              POSITIVE_INFINITY_KEY,
                                              0,
                                              0,
                                              TRUNK_STATS_ZERO,
                                              TRUNK_STATS_ZERO);
   if (ub_pivot == NULL) {
      platform_error_log("build_new_roots: pivot_create failed\n");
      rc = STATUS_NO_MEMORY;
      goto cleanup_pivots;
   }
   rc = vector_append(&pivots, ub_pivot);
   platform_assert_status_ok(rc);

   // Build a new vector of empty pivot bundles.
   bundle_vector pivot_bundles;
   rc = bundle_vector_init_empty(
      &pivot_bundles, vector_length(&pivots) - 1, context->hid);
   if (!SUCCESS(rc)) {
      platform_error_log(
         "build_new_roots: bundle_vector_init_empty failed: %d\n", rc.r);
      goto cleanup_pivots;
   }

   // Build a new empty inflight bundle vector
   bundle_vector inflight;
   vector_init(&inflight, context->hid);

   // Build the new root
   trunk_node new_root;
   trunk_node_init(&new_root, height + 1, pivots, pivot_bundles, 0, inflight);
   debug_assert(
      trunk_node_is_well_formed_index(context->cfg->data_cfg, &new_root));

   // At this point, all our resources that we've allocated have been put
   // into the new root.

   trunk_node_vector new_nodes;
   vector_init(&new_nodes, context->hid);
   rc = index_split(context, &new_root, &new_nodes);
   trunk_node_deinit(&new_root, context);
   if (!SUCCESS(rc)) {
      platform_error_log("build_new_roots: index_split failed: %d\n", rc.r);
      VECTOR_APPLY_TO_PTRS(&new_nodes, trunk_node_deinit, context);
      vector_deinit(&new_nodes);
      return rc;
   }

   trunk_ondisk_node_ref_vector new_ondisk_node_refs;
   vector_init(&new_ondisk_node_refs, context->hid);
   rc = serialize_nodes(context, &new_nodes, &new_ondisk_node_refs);
   VECTOR_APPLY_TO_PTRS(&new_nodes, trunk_node_deinit, context);
   vector_deinit(&new_nodes);
   if (!SUCCESS(rc)) {
      platform_error_log("build_new_roots: serialize_nodes_and_enqueue_bundle_"
                         "compactions failed: %d\n",
                         rc.r);
      goto cleanup_new_ondisk_node_refs;
   }

   VECTOR_APPLY_TO_ELTS(
      node_refs, trunk_ondisk_node_ref_destroy, context, context->hid);
   rc = vector_copy(node_refs, &new_ondisk_node_refs);
   platform_assert_status_ok(rc);
   vector_deinit(&new_ondisk_node_refs);
   return STATUS_OK;

cleanup_new_ondisk_node_refs:
   VECTOR_APPLY_TO_ELTS(&new_ondisk_node_refs,
                        trunk_ondisk_node_ref_destroy,
                        context,
                        context->hid);
   vector_deinit(&new_ondisk_node_refs);
cleanup_pivots:
   VECTOR_APPLY_TO_ELTS(&pivots, trunk_pivot_destroy, context->hid);
   vector_deinit(&pivots);

   return rc;
}

platform_status
trunk_incorporate_prepare(trunk_context *context, uint64 branch_addr)
{
   platform_status rc;
   uint64          height;

   trunk_modification_begin(context);

   incorporation_tasks_init(&context->tasks, context->hid);

   branch_ref branch = create_branch_ref(branch_addr);

   bundle_vector inflight;
   vector_init(&inflight, context->hid);

   trunk_ondisk_node_ref_vector new_node_refs;
   vector_init(&new_node_refs, context->hid);

   trunk_pivot_vector new_pivot;
   vector_init(&new_pivot, context->hid);

   // Construct a vector of inflight bundles with one singleton bundle for
   // the new branch.
   rc = VECTOR_EMPLACE_APPEND(
      &inflight, bundle_init_single, context->hid, NULL_ROUTING_FILTER, branch);
   if (!SUCCESS(rc)) {
      platform_error_log(
         "trunk_incorporate: VECTOR_EMPLACE_APPEND failed: %d\n", rc.r);
      goto cleanup_vectors;
   }

   // Read the old root.
   trunk_node root;
   if (context->root != NULL) {
      rc = trunk_node_deserialize(context, context->root->addr, &root);
      if (!SUCCESS(rc)) {
         platform_error_log("trunk_incorporate: node_deserialize failed: %d\n",
                            rc.r);
         goto cleanup_vectors;
      }
   } else {
      // If there is no root, create an empty one.
      rc = trunk_node_init_empty_leaf(
         &root, context->hid, NEGATIVE_INFINITY_KEY, POSITIVE_INFINITY_KEY);
      if (!SUCCESS(rc)) {
         platform_error_log(
            "trunk_incorporate: node_init_empty_leaf failed: %d\n", rc.r);
         goto cleanup_vectors;
      }
      debug_assert(
         trunk_node_is_well_formed_leaf(context->cfg->data_cfg, &root));
   }

   height = trunk_node_height(&root);

   // "flush" the new bundle to the root, then do any rebalancing needed.
   rc = flush_then_compact(
      context, &root, NULL, &inflight, 0, &new_node_refs, &context->tasks);
   trunk_node_deinit(&root, context);
   if (!SUCCESS(rc)) {
      platform_error_log("trunk_incorporate: flush_then_compact failed: %d\n",
                         rc.r);
      goto cleanup_vectors;
   }

   // Build new roots, possibly splitting them, until we get down to a single
   // root with fanout that is within spec.
   while (1 < vector_length(&new_node_refs)) {
      rc = build_new_roots(context, height, &new_node_refs);
      if (!SUCCESS(rc)) {
         platform_error_log("trunk_incorporate: build_new_roots failed: %d\n",
                            rc.r);
         goto cleanup_vectors;
      }
      height++;
   }

   platform_assert(context->post_incorporation_root == NULL);
   context->post_incorporation_root = vector_get(&new_node_refs, 0);

   if (context->stats) {
      threadid tid       = platform_get_tid();
      uint64   footprint = vector_length(&context->tasks.node_compactions);
      if (TRUNK_MAX_DISTRIBUTION_VALUE < footprint) {
         footprint = TRUNK_MAX_DISTRIBUTION_VALUE - 1;
      }
      context->stats[tid].incorporation_footprint_distribution[footprint]++;
   }

cleanup_vectors:
   if (!SUCCESS(rc)) {
      VECTOR_APPLY_TO_ELTS(
         &new_node_refs, trunk_ondisk_node_ref_destroy, context, context->hid);
      incorporation_tasks_deinit(&context->tasks, context);
      trunk_modification_end(context);
   }
   vector_deinit(&new_node_refs);
   VECTOR_APPLY_TO_PTRS(&inflight, bundle_deinit);
   vector_deinit(&inflight);

   return rc;
}

void
trunk_incorporate_commit(trunk_context *context)
{
   platform_batch_rwlock_lock(&context->root_lock, 0);
   platform_assert(context->pre_incorporation_root == NULL);
   context->pre_incorporation_root  = context->root;
   context->root                    = context->post_incorporation_root;
   context->post_incorporation_root = NULL;
   platform_batch_rwlock_unlock(&context->root_lock, 0);
}

void
trunk_incorporate_cleanup(trunk_context *context)
{
   if (context->pre_incorporation_root != NULL) {
      trunk_ondisk_node_ref_destroy(
         context->pre_incorporation_root, context, context->hid);
      context->pre_incorporation_root = NULL;
   }
   incorporation_tasks_execute(&context->tasks, context);
   incorporation_tasks_deinit(&context->tasks, context);
   trunk_modification_end(context);
}

/***********************************
 * Point queries
 ***********************************/

static platform_status
trunk_ondisk_node_find_pivot(const trunk_context      *context,
                             trunk_ondisk_node_handle *handle,
                             key                       tgt,
                             comparison                cmp,
                             trunk_ondisk_pivot      **pivot)
{
   uint64 num_pivots = trunk_ondisk_node_num_pivots(handle);
   uint64 min        = 0;
   uint64 max        = num_pivots - 1;

   // invariant: pivot[min] <= tgt < pivot[max]
   int                 last_cmp;
   trunk_ondisk_pivot *min_pivot = NULL;
   while (min + 1 < max) {
      uint64              mid       = (min + max) / 2;
      trunk_ondisk_pivot *mid_pivot = trunk_ondisk_node_get_pivot(handle, mid);
      if (mid_pivot == NULL) {
         platform_error_log("ondisk_node_find_pivot: "
                            "ondisk_node_get_pivot failed\n");
         return STATUS_IO_ERROR;
      }
      key mid_key = trunk_ondisk_pivot_key(mid_pivot);
      int cmp     = data_key_compare(context->cfg->data_cfg, tgt, mid_key);
      if (cmp < 0) {
         max = mid;
      } else {
         min       = mid;
         min_pivot = mid_pivot;
         last_cmp  = cmp;
      }
   }
   /* 0 < min means we executed the loop at least once.
      last_cmp == 0 means we found an exact match at pivot[mid], and we then
      assigned mid to min, which means that pivot[min] == tgt.
   */
   if (0 < min && last_cmp == 0 && cmp == less_than) {
      min--;
      min_pivot = trunk_ondisk_node_get_pivot(handle, min);
   }

   if (min_pivot == NULL) {
      min_pivot = trunk_ondisk_node_get_pivot(handle, min);
   }

   *pivot = min_pivot;
   return STATUS_OK;
}

/*
 * IN Parameters:
 * state->context: the trunk node context
 * state->handlep: the ondisk node handle
 * state->tgt: the target key
 * //state->cmp: the comparison to use
 *
 * OUT Parameters:
 * state->pivot: the pivot found
 * state->rc: the return code
 *
 * LOCAL Variables:
 * state->min: the minimum pivot index
 * state->max: the maximum pivot index
 * state->min_pivot: the minimum pivot found
 * state->last_cmp: the last comparison result
 * state->mid: the mid pivot index
 * state->pivot_num: the pivot number
 * state->offset: the offset
 * state->page: the page
 * state->cache_get_state: the cache get state
 */
static async_status
trunk_ondisk_node_find_pivot_async(trunk_merge_lookup_async_state *state,
                                   uint64                          depth)
{
   async_begin(state, depth);

   state->min = 0;
   state->max = trunk_ondisk_node_num_pivots(state->handlep) - 1;

   // invariant: pivot[min] <= tgt < pivot[max]
   state->min_pivot = NULL;
   while (state->min + 1 < state->max) {
      state->mid       = (state->min + state->max) / 2;
      state->pivot_num = state->mid;
      async_await_subroutine(state, trunk_ondisk_node_get_pivot_async);
      if (!SUCCESS(state->rc)) {
         platform_error_log("ondisk_node_find_pivot_async: "
                            "ondisk_node_get_pivot_async failed: %d\n",
                            state->rc.r);
         async_return(state);
      }
      key mid_key = trunk_ondisk_pivot_key(state->pivot);
      int cmp =
         data_key_compare(state->context->cfg->data_cfg, state->tgt, mid_key);
      if (cmp < 0) {
         state->max = state->mid;
      } else {
         state->min       = state->mid;
         state->min_pivot = state->pivot;
         state->last_cmp  = cmp;
      }
   }
   /* 0 < min means we executed the loop at least once.
      last_cmp == 0 means we found an exact match at pivot[mid], and we then
      assigned mid to min, which means that pivot[min] == tgt.
   */
   // if (0 < state->min && state->last_cmp == 0 && state->cmp == less_than) {
   //    state->min--;
   //    state->min_pivot = ondisk_node_get_pivot(state->handlep, state->min);
   // }

   if (state->min_pivot == NULL) {
      state->min_pivot =
         trunk_ondisk_node_get_pivot(state->handlep, state->min);
   }

   state->pivot = state->min_pivot;
   state->rc    = STATUS_OK;
   async_return(state);
}

static platform_status
trunk_ondisk_bundle_merge_lookup(trunk_context       *context,
                                 uint64               height,
                                 trunk_ondisk_bundle *bndl,
                                 key                  tgt,
                                 merge_accumulator   *result,
                                 platform_log_handle *log)
{
   threadid tid = platform_get_tid();
   uint64   found_values;

   platform_status rc;

   if (routing_filters_equal(&bndl->maplet, &NULL_ROUTING_FILTER)) {
      platform_assert(bndl->num_branches <= 1);
      found_values = bndl->num_branches == 1 ? 1 : 0;
   } else {
      rc = routing_filter_lookup(context->cc,
                                 context->cfg->filter_cfg,
                                 &bndl->maplet,
                                 tgt,
                                 &found_values);
      if (!SUCCESS(rc)) {
         platform_error_log("ondisk_bundle_merge_lookup: "
                            "routing_filter_lookup failed: %d\n",
                            rc.r);
         return rc;
      }
      if (context->stats) {
         context->stats[tid].maplet_lookups[height]++;
      }
   }


   if (log) {
      platform_log(log, "maplet: %lu\n", bndl->maplet.addr);
      platform_log(log, "found_values: %lu\n", found_values);
      found_values = (1ULL << bndl->num_branches) - 1;
   }

   for (uint64 idx =
           routing_filter_get_next_value(found_values, ROUTING_NOT_FOUND);
        idx != ROUTING_NOT_FOUND;
        idx = routing_filter_get_next_value(found_values, idx))
   {
      bool32 local_found;
      rc = btree_lookup_and_merge(context->cc,
                                  context->cfg->btree_cfg,
                                  branch_ref_addr(bndl->branches[idx]),
                                  trunk_ondisk_bundle_branch_type(bndl),
                                  tgt,
                                  result,
                                  &local_found);
      if (!SUCCESS(rc)) {
         platform_error_log("ondisk_bundle_merge_lookup: "
                            "btree_lookup_and_merge failed: %d\n",
                            rc.r);
         return rc;
      }

      if (context->stats) {
         context->stats[tid].branch_lookups[height]++;
         if (!local_found) {
            context->stats[tid].maplet_false_positives[height]++;
         }
      }


      if (!log && merge_accumulator_is_definitive(result)) {
         return STATUS_OK;
      }

      if (log) {
         merge_accumulator ma;
         merge_accumulator_init(&ma, context->hid);
         rc = btree_lookup_and_merge(context->cc,
                                     context->cfg->btree_cfg,
                                     branch_ref_addr(bndl->branches[idx]),
                                     trunk_ondisk_bundle_branch_type(bndl),
                                     tgt,
                                     &ma,
                                     &local_found);
         platform_log(log,
                      "branch: %lu found: %u\n",
                      branch_ref_addr(bndl->branches[idx]),
                      local_found);
         if (local_found) {
            message msg = merge_accumulator_to_message(&ma);
            platform_log(
               log, "msg: %s\n", message_string(context->cfg->data_cfg, msg));
         }
         merge_accumulator_deinit(&ma);
      }
   }

   return STATUS_OK;
}

static async_status
trunk_ondisk_bundle_merge_lookup_async(trunk_merge_lookup_async_state *state,
                                       uint64                          depth)
{
   // Get the current thread id after every yield.
   threadid tid = platform_get_tid();

   async_begin(state, depth);

   if (routing_filters_equal(&state->bndl->maplet, &NULL_ROUTING_FILTER)) {
      platform_assert(state->bndl->num_branches <= 1);
      state->found_values = state->bndl->num_branches == 1 ? 1 : 0;
   } else {
      async_await_call(state,
                       routing_filter_lookup_async,
                       &state->filter_state,
                       state->context->cc,
                       state->context->cfg->filter_cfg,
                       state->bndl->maplet,
                       state->tgt,
                       &state->found_values,
                       state->callback,
                       state->callback_arg);
      state->rc = async_result(&state->filter_state);
      if (!SUCCESS(state->rc)) {
         platform_error_log("ondisk_bundle_merge_lookup_async: "
                            "routing_filter_lookup_async failed: %d\n",
                            state->rc.r);
         async_return(state);
      }

      if (state->context->stats) {
         state->context->stats[tid].maplet_lookups[state->height]++;
      }
   }

   if (state->log) {
      platform_log(state->log, "maplet: %lu\n", state->bndl->maplet.addr);
      platform_log(state->log, "found_values: %lu\n", state->found_values);
      state->found_values = (1ULL << state->bndl->num_branches) - 1;
   }

   for (state->idx = routing_filter_get_next_value(state->found_values,
                                                   ROUTING_NOT_FOUND);
        state->idx != ROUTING_NOT_FOUND;
        state->idx =
           routing_filter_get_next_value(state->found_values, state->idx))
   {
      async_await_call(state,
                       btree_lookup_and_merge_async,
                       &state->btree_state,
                       state->context->cc,
                       state->context->cfg->btree_cfg,
                       branch_ref_addr(state->bndl->branches[state->idx]),
                       trunk_ondisk_bundle_branch_type(state->bndl),
                       state->tgt,
                       state->result,
                       state->callback,
                       state->callback_arg);
      state->rc = async_result(&state->btree_state);
      if (!SUCCESS(state->rc)) {
         platform_error_log("ondisk_bundle_merge_lookup_async: "
                            "btree_lookup_and_merge_async failed: %d\n",
                            state->rc.r);
         async_return(state);
      }

      if (state->context->stats) {
         state->context->stats[tid].branch_lookups[state->height]++;
         if (!state->btree_state.found) {
            state->context->stats[tid].maplet_false_positives[state->height]++;
         }
      }


      if (!state->log && merge_accumulator_is_definitive(state->result)) {
         async_return(state);
      }

      if (state->log) {
         merge_accumulator ma;
         merge_accumulator_init(&ma, state->context->hid);
         // Not bothering to make the logging paths async
         platform_status rc = btree_lookup_and_merge(
            state->context->cc,
            state->context->cfg->btree_cfg,
            branch_ref_addr(state->bndl->branches[state->idx]),
            trunk_ondisk_bundle_branch_type(state->bndl),
            state->tgt,
            &ma,
            &state->btree_state.found);
         platform_assert_status_ok(rc);
         platform_log(state->log,
                      "branch: %lu found: %u\n",
                      branch_ref_addr(state->bndl->branches[state->idx]),
                      state->btree_state.found);
         if (state->btree_state.found) {
            message msg = merge_accumulator_to_message(&ma);
            platform_log(state->log,
                         "msg: %s\n",
                         message_string(state->context->cfg->data_cfg, msg));
         }
         merge_accumulator_deinit(&ma);
      }
   }

   async_return(state);
}

platform_status
trunk_merge_lookup(trunk_context            *context,
                   trunk_ondisk_node_handle *inhandle,
                   key                       tgt,
                   merge_accumulator        *result,
                   platform_log_handle      *log)
{
   platform_status rc = STATUS_OK;

   trunk_ondisk_node_handle  handle;
   trunk_ondisk_node_handle *handlep;
   handlep = inhandle;

   while (handlep && handlep->header_page) {
      uint64 height = trunk_ondisk_node_height(handlep);

      if (log) {
         trunk_node node;
         rc = trunk_node_deserialize(
            context, handlep->header_page->disk_addr, &node);
         if (!SUCCESS(rc)) {
            platform_error_log("trunk_merge_lookup: "
                               "node_deserialize failed: %d\n",
                               rc.r);
            goto cleanup;
         }
         platform_log(log, "addr: %lu\n", handlep->header_page->disk_addr);
         trunk_node_print(&node, log, context->cfg->data_cfg, 0);
         trunk_node_deinit(&node, context);
      }

      trunk_ondisk_pivot *pivot;
      rc = trunk_ondisk_node_find_pivot(
         context, handlep, tgt, less_than_or_equal, &pivot);
      if (!SUCCESS(rc)) {
         platform_error_log(
            "trunk_merge_lookup: ondisk_node_find_pivot failed: "
            "%d\n",
            rc.r);
         goto cleanup;
      }

      if (log) {
         platform_log(
            log,
            "pivot: %s\n",
            key_string(context->cfg->data_cfg, trunk_ondisk_pivot_key(pivot)));
      }

      // Search the inflight bundles
      trunk_ondisk_bundle *bndl;
      rc = trunk_ondisk_node_get_first_inflight_bundle(handlep, &bndl);
      if (!SUCCESS(rc)) {
         platform_error_log("trunk_merge_lookup: "
                            "ondisk_node_get_first_inflight_bundle failed\n");
         goto cleanup;
      }
      for (uint64 i = 0; i < pivot->num_live_inflight_bundles; i++) {
         rc = trunk_ondisk_bundle_merge_lookup(
            context, height, bndl, tgt, result, log);
         if (!SUCCESS(rc)) {
            platform_error_log("trunk_merge_lookup: "
                               "ondisk_bundle_merge_lookup failed: %d\n",
                               rc.r);
            goto cleanup;
         }
         if (merge_accumulator_is_definitive(result)) {
            goto cleanup;
         }
         if (i < pivot->num_live_inflight_bundles - 1) {
            bndl = trunk_ondisk_node_get_next_inflight_bundle(handlep, bndl);
         }
      }

      // Search the pivot bundle
      bndl = trunk_ondisk_pivot_bundle(pivot);
      rc   = trunk_ondisk_bundle_merge_lookup(
         context, height, bndl, tgt, result, log);
      if (!SUCCESS(rc)) {
         platform_error_log("trunk_merge_lookup: "
                            "ondisk_bundle_merge_lookup failed: %d\n",
                            rc.r);
         goto cleanup;
      }
      if (!log && merge_accumulator_is_definitive(result)) {
         goto cleanup;
      }

      // Search the child
      if (pivot->child_addr != 0) {
         trunk_ondisk_node_handle child_handle;
         rc = trunk_ondisk_node_handle_init(
            &child_handle, context->cc, pivot->child_addr);
         if (!SUCCESS(rc)) {
            platform_error_log("trunk_merge_lookup: "
                               "ondisk_node_handle_init failed: %d\n",
                               rc.r);
            goto cleanup;
         }
         if (handlep != inhandle) {
            trunk_ondisk_node_handle_deinit(handlep);
         }
         handle  = child_handle;
         handlep = &handle;
      } else {
         if (handlep != inhandle) {
            trunk_ondisk_node_handle_deinit(handlep);
         }
         handlep = NULL;
      }
   }

cleanup:
   if (handlep && handlep != inhandle) {
      trunk_ondisk_node_handle_deinit(handlep);
   }
   return rc;
}

async_status
trunk_merge_lookup_async(trunk_merge_lookup_async_state *state)
{
   async_begin(state, 0);

   state->rc      = STATUS_OK;
   state->handlep = state->inhandle;

   while (state->handlep && state->handlep->header_page) {
      state->height = trunk_ondisk_node_height(state->handlep);

      if (state->log) {
         // Sorry, but we're not going to perform the logging asynchronously.
         trunk_node node;
         state->rc = trunk_node_deserialize(
            state->context, state->handlep->header_page->disk_addr, &node);
         if (!SUCCESS(state->rc)) {
            platform_error_log("trunk_merge_lookup_async: "
                               "node_deserialize failed: %d\n",
                               state->rc.r);
            goto cleanup;
         }
         platform_log(
            state->log, "addr: %lu\n", state->handlep->header_page->disk_addr);
         trunk_node_print(&node, state->log, state->context->cfg->data_cfg, 0);
         trunk_node_deinit(&node, state->context);
      }

      async_await_subroutine(state, trunk_ondisk_node_find_pivot_async);
      if (!SUCCESS(state->rc)) {
         platform_error_log(
            "trunk_merge_lookup_async: ondisk_node_find_pivot_async failed: "
            "%d\n",
            state->rc.r);
         goto cleanup;
      }

      if (state->log) {
         platform_log(state->log,
                      "pivot_num: %lu pivot: %s\n",
                      state->min,
                      key_string(state->context->cfg->data_cfg,
                                 trunk_ondisk_pivot_key(state->pivot)));
      }

      // Search the inflight bundles
      async_await_subroutine(state,
                             trunk_ondisk_node_get_first_inflight_bundle_async);
      if (!SUCCESS(state->rc)) {
         platform_error_log(
            "trunk_merge_lookup_async: "
            "ondisk_node_get_first_inflight_bundle_async failed\n");
         goto cleanup;
      }

      for (state->inflight_bundle_num = 0;
           state->inflight_bundle_num < state->pivot->num_live_inflight_bundles;
           state->inflight_bundle_num++)
      {
         async_await_subroutine(state, trunk_ondisk_bundle_merge_lookup_async);
         if (!SUCCESS(state->rc)) {
            platform_error_log("trunk_merge_lookup_async: "
                               "ondisk_bundle_merge_lookup_async failed: %d\n",
                               state->rc.r);
            goto cleanup;
         }
         if (merge_accumulator_is_definitive(state->result)) {
            goto cleanup;
         }
         if (state->inflight_bundle_num
             < state->pivot->num_live_inflight_bundles - 1)
         {
            async_await_subroutine(
               state, trunk_ondisk_node_get_next_inflight_bundle_async);
            if (state->bndl == NULL) {
               platform_error_log(
                  "trunk_merge_lookup_async: "
                  "ondisk_node_get_next_inflight_bundle_async failed\n");
               state->rc = STATUS_IO_ERROR;
               goto cleanup;
            }
         }
      }

      // Search the pivot bundle
      state->bndl = trunk_ondisk_pivot_bundle(state->pivot);
      async_await_subroutine(state, trunk_ondisk_bundle_merge_lookup_async);
      if (!SUCCESS(state->rc)) {
         platform_error_log("trunk_merge_lookup_async: "
                            "ondisk_bundle_merge_lookup_async failed: %d\n",
                            state->rc.r);
         goto cleanup;
      }
      if (!state->log && merge_accumulator_is_definitive(state->result)) {
         goto cleanup;
      }

      // Search the child
      if (state->pivot->child_addr != 0) {
         async_await_subroutine(state, trunk_ondisk_node_handle_init_async);
         if (!SUCCESS(state->rc)) {
            platform_error_log("trunk_merge_lookup_async: "
                               "ondisk_node_handle_init_async failed: %d\n",
                               state->rc.r);
            goto cleanup;
         }
         if (state->handlep != state->inhandle) {
            trunk_ondisk_node_handle_deinit(state->handlep);
         }
         state->handle  = state->child_handle;
         state->handlep = &state->handle;
      } else {
         if (state->handlep != state->inhandle) {
            trunk_ondisk_node_handle_deinit(state->handlep);
         }
         state->handlep = NULL;
      }
   }

cleanup:
   if (state->handlep && state->handlep != state->inhandle) {
      trunk_ondisk_node_handle_deinit(state->handlep);
   }
   async_return(state, state->rc);
}


static platform_status
trunk_collect_bundle_branches(trunk_ondisk_bundle *bndl,
                              uint64               capacity,
                              uint64              *num_branches,
                              trunk_branch_info   *branches)
{
   for (int64 i = bndl->num_branches - 1; 0 <= i; i--) {
      if (*num_branches == capacity) {
         platform_error_log("trunk_collect_bundle_branches: "
                            "capacity exceeded\n");
         *num_branches -= i;
         return STATUS_LIMIT_EXCEEDED;
      }
      branches[*num_branches].addr = branch_ref_addr(bndl->branches[i]);
      branches[*num_branches].type = trunk_ondisk_bundle_branch_type(bndl);

      (*num_branches)++;
   }
   return STATUS_OK;
}

static void
trunk_ondisk_bundle_inc_all_branch_refs(const trunk_context *context,
                                        trunk_ondisk_bundle *bndl)
{
   for (uint64 i = 0; i < bndl->num_branches; i++) {
      branch_ref bref = bndl->branches[i];
      btree_inc_ref(
         context->cc, context->cfg->btree_cfg, branch_ref_addr(bref));
   }
}

platform_status
trunk_collect_branches(const trunk_context            *context,
                       const trunk_ondisk_node_handle *inhandle,
                       key                             tgt,
                       comparison                      start_type,
                       uint64                          capacity,
                       uint64                         *num_branches,
                       trunk_branch_info              *branches,
                       key_buffer                     *min_key,
                       key_buffer                     *max_key)
{
   platform_status rc                    = STATUS_OK;
   uint64          original_num_branches = *num_branches;

   rc = key_buffer_copy_key(min_key, NEGATIVE_INFINITY_KEY);
   platform_assert_status_ok(rc);
   rc = key_buffer_copy_key(max_key, POSITIVE_INFINITY_KEY);
   platform_assert_status_ok(rc);

   trunk_ondisk_node_handle handle;
   rc = trunk_ondisk_node_handle_clone(&handle, inhandle);
   if (!SUCCESS(rc)) {
      platform_error_log("trunk_collect_branches: "
                         "trunk_ondisk_node_handle_clone failed: %d\n",
                         rc.r);
      return rc;
   }

   while (handle.header_page) {
      trunk_ondisk_pivot *pivot;
      if (start_type != less_than) {
         rc = trunk_ondisk_node_find_pivot(
            context, &handle, tgt, less_than_or_equal, &pivot);
      } else {
         rc = trunk_ondisk_node_find_pivot(
            context, &handle, tgt, less_than, &pivot);
      }
      if (!SUCCESS(rc)) {
         platform_error_log("trunk_collect_branches: "
                            "ondisk_node_find_pivot failed: %d\n",
                            rc.r);
         goto cleanup;
      }

      uint64 child_addr;
      uint64 num_inflight_bundles;
      child_addr           = pivot->child_addr;
      num_inflight_bundles = pivot->num_live_inflight_bundles;

      // Add branches from the inflight bundles
      trunk_ondisk_bundle *bndl;
      rc = trunk_ondisk_node_get_first_inflight_bundle(&handle, &bndl);
      if (!SUCCESS(rc)) {
         platform_error_log("trunk_collect_branches: "
                            "ondisk_node_get_first_inflight_bundle failed\n");
         goto cleanup;
      }
      for (uint64 i = 0; i < num_inflight_bundles; i++) {
         rc = trunk_collect_bundle_branches(
            bndl, capacity, num_branches, branches);
         if (!SUCCESS(rc)) {
            platform_error_log("trunk_collect_branches: "
                               "trunk_collect_bundle_branches failed: %d\n",
                               rc.r);
            goto cleanup;
         }

         trunk_ondisk_bundle_inc_all_branch_refs(context, bndl);

         if (i < num_inflight_bundles - 1) {
            bndl = trunk_ondisk_node_get_next_inflight_bundle(&handle, bndl);
         }
      }

      // Add branches from the pivot bundle
      bndl = trunk_ondisk_pivot_bundle(pivot);
      rc =
         trunk_collect_bundle_branches(bndl, capacity, num_branches, branches);
      if (!SUCCESS(rc)) {
         platform_error_log("trunk_collect_branches: "
                            "trunk_collect_bundle_branches failed: %d\n",
                            rc.r);
         goto cleanup;
      }

      trunk_ondisk_bundle_inc_all_branch_refs(context, bndl);

      // Proceed to the child
      if (child_addr != 0) {
         trunk_ondisk_node_handle child_handle;
         rc = trunk_ondisk_node_handle_init(
            &child_handle, context->cc, child_addr);
         if (!SUCCESS(rc)) {
            platform_error_log("trunk_collect_branches: "
                               "ondisk_node_handle_init failed: %d\n",
                               rc.r);
            goto cleanup;
         }
         trunk_ondisk_node_handle_deinit(&handle);
         handle = child_handle;
      } else {
         key leaf_min_key;
         key leaf_max_key;
         debug_assert(trunk_ondisk_node_num_pivots(&handle) == 2);
         rc = trunk_ondisk_node_get_pivot_key(&handle, 0, &leaf_min_key);
         if (!SUCCESS(rc)) {
            platform_error_log("trunk_collect_branches: "
                               "ondisk_node_get_pivot_key failed: %d\n",
                               rc.r);
            goto cleanup;
         }
         rc = trunk_ondisk_node_get_pivot_key(&handle, 1, &leaf_max_key);
         if (!SUCCESS(rc)) {
            platform_error_log("trunk_collect_branches: "
                               "ondisk_node_get_pivot_key failed: %d\n",
                               rc.r);
            goto cleanup;
         }
         rc = key_buffer_copy_key(min_key, leaf_min_key);
         if (!SUCCESS(rc)) {
            platform_error_log("trunk_collect_branches: "
                               "key_buffer_copy_key failed: %d\n",
                               rc.r);
            goto cleanup;
         }
         rc = key_buffer_copy_key(max_key, leaf_max_key);
         if (!SUCCESS(rc)) {
            platform_error_log("trunk_collect_branches: "
                               "key_buffer_copy_key failed: %d\n",
                               rc.r);
            goto cleanup;
         }
         trunk_ondisk_node_handle_deinit(&handle);
      }
   }

cleanup:
   if (handle.header_page) {
      trunk_ondisk_node_handle_deinit(&handle);
   }
   if (!SUCCESS(rc)) {
      for (uint64 i = original_num_branches; i < *num_branches; i++) {
         btree_dec_ref(context->cc,
                       context->cfg->btree_cfg,
                       branches[i].addr,
                       branches[i].type);
      }
      *num_branches = original_num_branches;
   }

   return rc;
}

/************************************
 * Lifecycle
 ************************************/

void
trunk_config_init(trunk_config         *config,
                  const data_config    *data_cfg,
                  const btree_config   *btree_cfg,
                  const routing_config *filter_cfg,
                  uint64                incorporation_size_kv_bytes,
                  uint64                target_fanout,
                  uint64                branch_rough_count_height,
                  bool32                use_stats)
{
   config->data_cfg                    = data_cfg;
   config->btree_cfg                   = btree_cfg;
   config->filter_cfg                  = filter_cfg;
   config->incorporation_size_kv_bytes = incorporation_size_kv_bytes;
   config->target_fanout               = target_fanout;
   config->branch_rough_count_height   = branch_rough_count_height;
   config->use_stats                   = use_stats;
}


platform_status
trunk_context_init(trunk_context      *context,
                   const trunk_config *cfg,
                   platform_heap_id    hid,
                   cache              *cc,
                   allocator          *al,
                   task_system        *ts,
                   uint64              root_addr)
{
   memset(context, 0, sizeof(trunk_context));

   if (root_addr != 0) {
      context->root =
         trunk_ondisk_node_ref_create(hid, NEGATIVE_INFINITY_KEY, root_addr);
      if (context->root == NULL) {
         platform_error_log("trunk_node_context_init: "
                            "ondisk_node_ref_create failed\n");
         return STATUS_NO_MEMORY;
      }
      allocator_inc_ref(al, root_addr);
   }

   context->cfg   = cfg;
   context->hid   = hid;
   context->cc    = cc;
   context->al    = al;
   context->ts    = ts;
   context->stats = NULL;
   if (cfg->use_stats) {
      context->stats = TYPED_ARRAY_MALLOC(hid, context->stats, MAX_THREADS);
      if (context->stats == NULL) {
         platform_error_log("trunk_node_context_init: "
                            "TYPED_ARRAY_MALLOC failed\n");
         return STATUS_NO_MEMORY;
      }
      memset(context->stats, 0, sizeof(trunk_stats) * MAX_THREADS);
   }

   trunk_pivot_state_map_init(&context->pivot_states);
   platform_batch_rwlock_init(&context->root_lock);

   return STATUS_OK;
}

platform_status
trunk_inc_ref(const trunk_config *cfg,
              platform_heap_id    hid,
              cache              *cc,
              allocator          *al,
              task_system        *ts,
              uint64              root_addr)
{
   trunk_context   context;
   platform_status rc =
      trunk_context_init(&context, cfg, hid, cc, al, ts, root_addr);
   if (!SUCCESS(rc)) {
      platform_error_log("trunk_node_inc_ref: trunk_node_context_init failed: "
                         "%d\n",
                         rc.r);
      return rc;
   }
   trunk_ondisk_node_inc_ref(&context, root_addr);
   trunk_context_deinit(&context);
   return STATUS_OK;
}

platform_status
trunk_dec_ref(const trunk_config *cfg,
              platform_heap_id    hid,
              cache              *cc,
              allocator          *al,
              task_system        *ts,
              uint64              root_addr)
{
   trunk_context   context;
   platform_status rc =
      trunk_context_init(&context, cfg, hid, cc, al, ts, root_addr);
   if (!SUCCESS(rc)) {
      platform_error_log("trunk_node_dec_ref: trunk_node_context_init failed: "
                         "%d\n",
                         rc.r);
      return rc;
   }
   trunk_ondisk_node_dec_ref(&context, root_addr);
   trunk_context_deinit(&context);
   return STATUS_OK;
}

void
trunk_context_deinit(trunk_context *context)
{
   platform_assert(context->pivot_states.num_states == 0);
   if (context->root != NULL) {
      trunk_ondisk_node_ref_destroy(context->root, context, context->hid);
   }
   perform_pending_gcs(context);
   platform_assert(context->pending_gcs == NULL);
   trunk_pivot_state_map_deinit(&context->pivot_states);
   platform_batch_rwlock_deinit(&context->root_lock);
}


platform_status
trunk_context_clone(trunk_context *dst, trunk_context *src)
{
   platform_status          rc;
   trunk_ondisk_node_handle handle;
   rc = trunk_init_root_handle(src, &handle);
   if (!SUCCESS(rc)) {
      platform_error_log("trunk_node_context_clone: trunk_init_root_handle "
                         "failed: %d\n",
                         rc.r);
      return rc;
   }
   uint64 root_addr = handle.header_page->disk_addr;

   rc = trunk_context_init(
      dst, src->cfg, src->hid, src->cc, src->al, src->ts, root_addr);
   trunk_ondisk_node_handle_deinit(&handle);
   return rc;
}

platform_status
trunk_make_durable(trunk_context *context)
{
   cache_flush(context->cc);
   return STATUS_OK;
}

/************************************
 * Stats
 ************************************/

static void
trunk_stats_accumulate(trunk_stats *dst, trunk_stats *src)
{
   STATS_FIELD_ADD(dst, src, fanout_distribution);
   STATS_FIELD_ADD(dst, src, num_inflight_bundles_distribution);
   STATS_FIELD_ADD(dst, src, bundle_num_branches_distribution);
   STATS_FIELD_ADD(dst, src, node_size_pages_distribution);

   STATS_FIELD_ADD(dst, src, incorporation_footprint_distribution);

   STATS_FIELD_ADD(dst, src, count_flushes);
   STATS_FIELD_ADD(dst, src, flush_time_ns);
   STATS_FIELD_MAX(dst, src, flush_time_max_ns);
   STATS_FIELD_ADD(dst, src, full_flushes);

   STATS_FIELD_ADD(dst, src, compactions);
   STATS_FIELD_ADD(dst, src, compactions_aborted);
   STATS_FIELD_ADD(dst, src, compactions_discarded);
   STATS_FIELD_ADD(dst, src, compactions_empty);
   STATS_FIELD_ADD(dst, src, compaction_tuples);
   STATS_FIELD_MAX(dst, src, compaction_max_tuples);
   STATS_FIELD_ADD(dst, src, compaction_time_ns);
   STATS_FIELD_MAX(dst, src, compaction_time_max_ns);
   STATS_FIELD_ADD(dst, src, compaction_time_wasted_ns);
   STATS_FIELD_ADD(dst, src, compaction_pack_time_ns);

   STATS_FIELD_ADD(dst, src, maplet_builds);
   STATS_FIELD_ADD(dst, src, maplet_builds_aborted);
   STATS_FIELD_ADD(dst, src, maplet_builds_discarded);
   STATS_FIELD_ADD(dst, src, maplet_build_time_ns);
   STATS_FIELD_ADD(dst, src, maplet_tuples);
   STATS_FIELD_MAX(dst, src, maplet_build_time_max_ns);
   STATS_FIELD_ADD(dst, src, maplet_build_time_wasted_ns);

   STATS_FIELD_ADD(dst, src, node_splits);
   STATS_FIELD_ADD(dst, src, node_splits_nodes_created);
   STATS_FIELD_ADD(dst, src, leaf_split_time_ns);
   STATS_FIELD_MAX(dst, src, leaf_split_time_max_ns);

   STATS_FIELD_ADD(dst, src, single_leaf_splits);

   STATS_FIELD_ADD(dst, src, maplet_lookups);
   STATS_FIELD_ADD(dst, src, maplet_false_positives);
   STATS_FIELD_ADD(dst, src, branch_lookups);
}

#define DISTRIBUTION_COLUMNS(dist, rows)                                       \
   COLUMN("0", ((uint64 *)dist) + 0 * rows),                                   \
      COLUMN("1", ((uint64 *)dist) + 1 * rows),                                \
      COLUMN("2", ((uint64 *)dist) + 2 * rows),                                \
      COLUMN("3", ((uint64 *)dist) + 3 * rows),                                \
      COLUMN("4", ((uint64 *)dist) + 4 * rows),                                \
      COLUMN("5", ((uint64 *)dist) + 5 * rows),                                \
      COLUMN("6", ((uint64 *)dist) + 6 * rows),                                \
      COLUMN("7", ((uint64 *)dist) + 7 * rows),                                \
      COLUMN("8", ((uint64 *)dist) + 8 * rows),                                \
      COLUMN("9", ((uint64 *)dist) + 9 * rows),                                \
      COLUMN("10", ((uint64 *)dist) + 10 * rows),                              \
      COLUMN("11", ((uint64 *)dist) + 11 * rows),                              \
      COLUMN("12", ((uint64 *)dist) + 12 * rows),                              \
      COLUMN("13", ((uint64 *)dist) + 13 * rows),                              \
      COLUMN("14", ((uint64 *)dist) + 14 * rows),                              \
      COLUMN(">= 15", ((uint64 *)dist) + 15 * rows)

static void
distribution_sum_avg(uint64       rows,
                     uint64       sum[],
                     fraction     avg[],
                     const uint64 distribution[])
{
   for (uint64 i = 0; i < rows; i++) {
      uint64 count    = 0;
      uint64 sumcount = 0;
      for (uint64 j = 0; j < TRUNK_MAX_DISTRIBUTION_VALUE; j++) {
         count += distribution[i + j * rows];
         sumcount += j * distribution[i + j * rows];
      }
      sum[i] = count;
      avg[i] = fraction_init_or_zero(sumcount, count);
   }
}

void
trunk_print_insertion_stats(platform_log_handle *log_handle,
                            const trunk_context *context)
{
   const uint64 height_array[TRUNK_MAX_HEIGHT] = {
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

   if (!context->stats) {
      platform_log(log_handle, "Statistics are not enabled\n");
      return;
   }

   if (context->root == NULL) {
      platform_log(log_handle, "No root node\n");
      return;
   }

   // Get the height of the tree
   trunk_node      root;
   platform_status rc =
      trunk_node_deserialize(context, context->root->addr, &root);
   if (!SUCCESS(rc)) {
      platform_error_log("trunk_node_print_insertion_stats: "
                         "node_deserialize failed: %d\n",
                         rc.r);
      return;
   }
   uint64 height = trunk_node_height(&root);
   trunk_node_deinit(&root, context);

   // Merge all the stats
   trunk_stats global_stats;
   memcpy(&global_stats, &context->stats[0], sizeof(trunk_stats));
   for (threadid tid = 1; tid < MAX_THREADS; tid++) {
      trunk_stats_accumulate(&global_stats, &context->stats[tid]);
   }

   //
   // Overall shape
   //
   platform_log(log_handle, "Height: %lu\n", height);
   uint64   total[TRUNK_MAX_HEIGHT];
   fraction avg[TRUNK_MAX_HEIGHT];

   // Fanout
   distribution_sum_avg(
      TRUNK_MAX_HEIGHT, total, avg, &global_stats.fanout_distribution[0][0]);
   column fanout_columns[] = {
      COLUMN("height", height_array),
      COLUMN("total", total),
      COLUMN("avg", avg),
      DISTRIBUTION_COLUMNS(global_stats.fanout_distribution, TRUNK_MAX_HEIGHT),
   };
   platform_log(log_handle, "Fanout distribution\n");
   print_column_table(
      log_handle, ARRAY_SIZE(fanout_columns), fanout_columns, height + 1);

   // Inflight bundles
   distribution_sum_avg(TRUNK_MAX_HEIGHT,
                        total,
                        avg,
                        &global_stats.num_inflight_bundles_distribution[0][0]);
   column inflight_columns[] = {
      COLUMN("height", height_array),
      COLUMN("total", total),
      COLUMN("avg", avg),
      DISTRIBUTION_COLUMNS(global_stats.num_inflight_bundles_distribution,
                           TRUNK_MAX_HEIGHT),
   };
   platform_log(log_handle, "Inflight bundles distribution\n");
   print_column_table(
      log_handle, ARRAY_SIZE(inflight_columns), inflight_columns, height + 1);

   // Bundle size
   distribution_sum_avg(TRUNK_MAX_HEIGHT,
                        total,
                        avg,
                        &global_stats.bundle_num_branches_distribution[0][0]);
   column bundle_columns[] = {
      COLUMN("height", height_array),
      COLUMN("total", total),
      COLUMN("avg", avg),
      DISTRIBUTION_COLUMNS(global_stats.bundle_num_branches_distribution,
                           TRUNK_MAX_HEIGHT),
   };
   platform_log(log_handle, "Bundle size distribution\n");
   print_column_table(
      log_handle, ARRAY_SIZE(bundle_columns), bundle_columns, height + 1);

   // Node size
   distribution_sum_avg(TRUNK_MAX_HEIGHT,
                        total,
                        avg,
                        &global_stats.node_size_pages_distribution[0][0]);
   column node_columns[] = {
      COLUMN("height", height_array),
      COLUMN("total", total),
      COLUMN("avg", avg),
      DISTRIBUTION_COLUMNS(global_stats.node_size_pages_distribution,
                           TRUNK_MAX_HEIGHT),
   };
   platform_log(log_handle, "Node size distribution\n");
   print_column_table(
      log_handle, ARRAY_SIZE(node_columns), node_columns, height + 1);

   //
   // Mutations
   //

   // Incorporations
   uint64   total_incorporations;
   fraction average_incorporation_footprint;
   distribution_sum_avg(1,
                        &total_incorporations,
                        &average_incorporation_footprint,
                        global_stats.incorporation_footprint_distribution);
   column incorporation_columns[] = {
      COLUMN("total incorporations", &total_incorporations),
      COLUMN("average footprint", &average_incorporation_footprint),
      DISTRIBUTION_COLUMNS(global_stats.incorporation_footprint_distribution,
                           1),
   };
   platform_log(log_handle, "Incorporation footprint distribution\n");
   print_column_table(
      log_handle, ARRAY_SIZE(incorporation_columns), incorporation_columns, 1);

   // Flushes
   fraction avg_flush_time_ns[TRUNK_MAX_HEIGHT];
   arrays_fraction(TRUNK_MAX_HEIGHT,
                   avg_flush_time_ns,
                   global_stats.flush_time_ns,
                   global_stats.count_flushes);
   column flush_columns[] = {
      COLUMN("height", height_array),
      COLUMN("count", global_stats.count_flushes),
      COLUMN("avg time (ns)", avg_flush_time_ns),
      COLUMN("max time (ns)", global_stats.flush_time_max_ns),
      COLUMN("full flushes", global_stats.full_flushes),
   };
   platform_log(log_handle, "Flushes\n");
   print_column_table(
      log_handle, ARRAY_SIZE(flush_columns), flush_columns, height + 1);

   // Compactions
   fraction avg_compaction_time_ns[TRUNK_MAX_HEIGHT];
   arrays_fraction(TRUNK_MAX_HEIGHT,
                   avg_compaction_time_ns,
                   global_stats.compaction_time_ns,
                   global_stats.compactions);
   uint64 setup_time_ns[TRUNK_MAX_HEIGHT];
   arrays_subtract(TRUNK_MAX_HEIGHT,
                   setup_time_ns,
                   global_stats.compaction_time_ns,
                   global_stats.compaction_pack_time_ns);
   fraction avg_setup_time_ns[TRUNK_MAX_HEIGHT];
   arrays_fraction(TRUNK_MAX_HEIGHT,
                   avg_setup_time_ns,
                   setup_time_ns,
                   global_stats.compactions);
   fraction avg_pack_time_per_tuple_ns[TRUNK_MAX_HEIGHT];
   arrays_fraction(TRUNK_MAX_HEIGHT,
                   avg_pack_time_per_tuple_ns,
                   global_stats.compaction_pack_time_ns,
                   global_stats.compaction_tuples);
   fraction avg_tuples[TRUNK_MAX_HEIGHT];
   arrays_fraction(TRUNK_MAX_HEIGHT,
                   avg_tuples,
                   global_stats.compaction_tuples,
                   global_stats.compactions);
   fraction fraction_wasted_compaction_time[TRUNK_MAX_HEIGHT];
   arrays_fraction(TRUNK_MAX_HEIGHT,
                   fraction_wasted_compaction_time,
                   global_stats.compaction_time_wasted_ns,
                   global_stats.compaction_time_ns);
   column compaction_columns[] = {
      COLUMN("height", height_array),
      COLUMN("num compactions", global_stats.compactions),
      COLUMN("avg setup time (ns)", avg_setup_time_ns),
      COLUMN("avg pack time / tuple (ns)", avg_pack_time_per_tuple_ns),
      COLUMN("avg tuples", avg_tuples),
      COLUMN("max tuples", global_stats.compaction_max_tuples),
      COLUMN("max time (ns)", global_stats.compaction_time_max_ns),
      COLUMN("empty", global_stats.compactions_empty),
      COLUMN("aborted", global_stats.compactions_aborted),
      COLUMN("discarded", global_stats.compactions_discarded),
      COLUMN("fraction wasted time", fraction_wasted_compaction_time),
   };
   platform_log(log_handle, "Compactions\n");
   print_column_table(log_handle,
                      ARRAY_SIZE(compaction_columns),
                      compaction_columns,
                      height + 1);

   // Maplets
   fraction avg_maplet_build_time_per_tuple_ns[TRUNK_MAX_HEIGHT];
   arrays_fraction(TRUNK_MAX_HEIGHT,
                   avg_maplet_build_time_per_tuple_ns,
                   global_stats.maplet_build_time_ns,
                   global_stats.maplet_tuples);
   fraction fraction_wasted_maplet_time[TRUNK_MAX_HEIGHT];
   arrays_fraction(TRUNK_MAX_HEIGHT,
                   fraction_wasted_maplet_time,
                   global_stats.maplet_build_time_wasted_ns,
                   global_stats.maplet_build_time_ns);
   column maplet_columns[] = {
      COLUMN("height", height_array),
      COLUMN("num maplets", global_stats.maplet_builds),
      COLUMN("avg time / tuple (ns)", avg_maplet_build_time_per_tuple_ns),
      COLUMN("max time (ns)", global_stats.maplet_build_time_max_ns),
      COLUMN("aborted", global_stats.maplet_builds_aborted),
      COLUMN("discarded", global_stats.maplet_builds_discarded),
      COLUMN("fraction wasted time", fraction_wasted_maplet_time),
   };
   platform_log(log_handle, "Maplets\n");
   print_column_table(
      log_handle, ARRAY_SIZE(maplet_columns), maplet_columns, height + 1);

   // Splits
   column split_columns[] = {
      COLUMN("num splits", global_stats.node_splits),
      COLUMN("num nodes created", global_stats.node_splits_nodes_created),
   };
   platform_log(log_handle, "Splits\n");
   print_column_table(
      log_handle, ARRAY_SIZE(split_columns), split_columns, height + 1);
   // Leaf splits
   fraction avg_leaf_split_time_ns = fraction_init_or_zero(
      global_stats.leaf_split_time_ns, global_stats.node_splits[0]);
   column leaf_split_columns[] = {
      COLUMN("avg time (ns)", &avg_leaf_split_time_ns),
      COLUMN("max time (ns)", &global_stats.leaf_split_time_max_ns),
      COLUMN("single leaf splits", &global_stats.single_leaf_splits),
   };
   platform_log(log_handle, "Leaf splits\n");
   print_column_table(
      log_handle, ARRAY_SIZE(leaf_split_columns), leaf_split_columns, 1);

   //
   // Lookups
   //
   column lookup_columns[] = {
      COLUMN("height", height_array),
      COLUMN("maplet lookups", global_stats.maplet_lookups),
      COLUMN("maplet false positives", global_stats.maplet_false_positives),
      COLUMN("branch lookups", global_stats.branch_lookups),
   };
   platform_log(log_handle, "Lookups\n");
   print_column_table(
      log_handle, ARRAY_SIZE(lookup_columns), lookup_columns, height + 1);
}

/************************************
 * Node traversal
 ************************************/

typedef platform_status (*node_visitor)(trunk_context *context,
                                        trunk_node    *node,
                                        void          *arg);

static platform_status
visit_nodes_internal(trunk_context *context,
                     trunk_node    *node,
                     node_visitor   visitor,
                     void          *arg)
{
   platform_status rc;

   rc = visitor(context, node, arg);
   if (!SUCCESS(rc)) {
      platform_error_log("visit_nodes_internal: visitor failed: %d\n", rc.r);
      return rc;
   }

   if (trunk_node_is_leaf(node)) {
      // Leaf nodes have no children, so we are done
      return rc;
   }

   for (int i = 0; i < trunk_node_num_children(node); i++) {
      trunk_pivot *pivot;
      trunk_node   child;

      pivot = vector_get(&node->pivots, i);
      rc    = trunk_node_deserialize(context, pivot->child_addr, &child);
      if (!SUCCESS(rc)) {
         platform_error_log("visit_nodes_internal: "
                            "trunk_node_deserialize failed: %d\n",
                            rc.r);
         return rc;
      }

      rc = visit_nodes_internal(context, &child, visitor, arg);
      trunk_node_deinit(&child, context);

      if (!SUCCESS(rc)) {
         platform_error_log("visit_nodes_internal: "
                            "visit_nodes_internal failed: %d\n",
                            rc.r);
         return rc;
      }
   }

   return rc;
}

static platform_status
visit_nodes(trunk_context *context, node_visitor visitor, void *arg)
{
   trunk_ondisk_node_handle root_handle;
   platform_status          rc;

   rc = trunk_init_root_handle(context, &root_handle);
   if (!SUCCESS(rc)) {
      platform_error_log("visit_nodes: trunk_init_root_handle failed: %d\n",
                         rc.r);
      return rc;
   }

   trunk_node node;
   rc = trunk_node_deserialize(
      context, root_handle.header_page->disk_addr, &node);
   if (!SUCCESS(rc)) {
      trunk_ondisk_node_handle_deinit(&root_handle);
      platform_error_log("visit_nodes_internal: "
                         "trunk_node_deserialize failed: %d\n",
                         rc.r);
      return rc;
   }


   rc = visit_nodes_internal(context, &node, visitor, arg);
   if (!SUCCESS(rc)) {
      platform_error_log("visit_nodes: visit_nodes_internal failed: %d\n",
                         rc.r);
   }

   trunk_node_deinit(&node, context);
   trunk_ondisk_node_handle_deinit(&root_handle);
   return rc;
}

/************************************
 * Space use
 ************************************/

typedef struct space_use_stats {
   uint64 trunk_bytes[TRUNK_MAX_HEIGHT];
   uint64 maplet_bytes[TRUNK_MAX_HEIGHT];
   uint64 branch_bytes[TRUNK_MAX_HEIGHT];
} space_use_stats;

static void
accumulate_space_use_branch(const branch_ref bref,
                            trunk_context   *context,
                            space_use_stats *dst,
                            uint64           height)
{
   dst->branch_bytes[height] += btree_space_use_bytes(context->cc,
                                                      context->cfg->btree_cfg,
                                                      branch_ref_addr(bref),
                                                      PAGE_TYPE_BRANCH);
}

static void
accumulate_space_use_bundle(const bundle    *bndl,
                            trunk_context   *context,
                            space_use_stats *dst,
                            uint64           height)
{
   if (!routing_filters_equal(&bndl->maplet, &NULL_ROUTING_FILTER)) {
      dst->maplet_bytes[height] +=
         routing_filter_space_use_bytes(context->cc, &bndl->maplet);
   }
   VECTOR_APPLY_TO_ELTS(
      &bndl->branches, accumulate_space_use_branch, context, dst, height);
}


static platform_status
accumulate_space_use_node(trunk_context *context, trunk_node *src, void *arg)
{
   space_use_stats *dst = (space_use_stats *)arg;
   if (src->height >= TRUNK_MAX_HEIGHT) {
      platform_error_log("accumulate_space_use_node: "
                         "node height exceeds max levels\n");
      return STATUS_LIMIT_EXCEEDED;
   }

   dst->trunk_bytes[src->height] += cache_extent_size(context->cc);

   VECTOR_APPLY_TO_PTRS(&src->pivot_bundles,
                        accumulate_space_use_bundle,
                        context,
                        dst,
                        src->height);
   return STATUS_OK;
}

void
trunk_print_space_use(platform_log_handle *log_handle, trunk_context *context)
{
   /* Measure the space used by the tree */
   space_use_stats space_usage;
   memset(&space_usage, 0, sizeof(space_usage));
   platform_status rc;

   if (context->root == NULL) {
      platform_log(log_handle, "Trunk space usage: none\n");
      return;
   }

   rc = visit_nodes(context, accumulate_space_use_node, &space_usage);
   if (!SUCCESS(rc)) {
      platform_error_log("trunk_print_space_use: "
                         "visit_nodes failed: %d\n",
                         rc.r);
      return;
   }

   uint64 height = TRUNK_MAX_HEIGHT;
   while (height > 0 && space_usage.trunk_bytes[height - 1] == 0) {
      height--;
   }

   /* Aggregate into per-level stats */
   uint64 total_bytes_per_level[TRUNK_MAX_HEIGHT];
   memset(total_bytes_per_level, 0, sizeof(total_bytes_per_level));
   array_accumulate_add(height, total_bytes_per_level, space_usage.trunk_bytes);
   array_accumulate_add(
      height, total_bytes_per_level, space_usage.maplet_bytes);
   array_accumulate_add(
      height, total_bytes_per_level, space_usage.branch_bytes);

   /* Aggregate into per-type stats */
   uint64 total_trunk_bytes  = array_sum(height, space_usage.trunk_bytes);
   uint64 total_maplet_bytes = array_sum(height, space_usage.maplet_bytes);
   uint64 total_branch_bytes = array_sum(height, space_usage.branch_bytes);

   /* Le grand total */
   uint64 total_bytes =
      total_trunk_bytes + total_maplet_bytes + total_branch_bytes;


   platform_log(log_handle,
                "Space use: trunk %lu bytes, maplet %lu bytes, "
                "branch %lu bytes, total %lu bytes\n",
                total_trunk_bytes,
                total_maplet_bytes,
                total_branch_bytes,
                total_bytes);

   const uint64 height_array[TRUNK_MAX_HEIGHT] = {
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
   column space_use_columns[] = {
      COLUMN("height", height_array),
      COLUMN("trunk bytes", space_usage.trunk_bytes),
      COLUMN("maplet bytes", space_usage.maplet_bytes),
      COLUMN("branch bytes", space_usage.branch_bytes),
      COLUMN("total bytes", total_bytes_per_level),
   };
   print_column_table(
      log_handle, ARRAY_SIZE(space_use_columns), space_use_columns, height);
}

void
trunk_reset_stats(trunk_context *context)
{
   if (context->stats) {
      memset(context->stats, 0, sizeof(trunk_stats) * MAX_THREADS);
   }
}
