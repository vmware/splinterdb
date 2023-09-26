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

typedef VECTOR(routing_filter) routing_filter_vector;
typedef VECTOR(branch_ref) branch_ref_vector;

typedef struct bundle {
   routing_filter    maplet;
   branch_ref_vector branches;
} bundle;

typedef struct ONDISK ondisk_bundle {
   routing_filter maplet;
   uint16         num_branches;
   branch_ref     branches[];
} ondisk_bundle;

typedef struct ONDISK trunk_pivot_stats {
   uint64 num_kv_bytes;
   uint64 num_tuples;
} trunk_pivot_stats;

typedef struct pivot {
   trunk_pivot_stats prereceive_stats;
   trunk_pivot_stats stats;
   uint64            child_addr;
   uint64            inflight_bundle_start;
   ondisk_key        key;
} pivot;

typedef struct ONDISK ondisk_pivot {
   trunk_pivot_stats stats;
   uint64            child_addr;
   uint64            num_live_inflight_bundles;
   ondisk_key        key;
} ondisk_pivot;

typedef VECTOR(pivot *) pivot_vector;
typedef VECTOR(bundle) bundle_vector;

typedef struct trunk_node {
   uint16        height;
   pivot_vector  pivots;
   bundle_vector pivot_bundles; // indexed by child
   uint64        num_old_bundles;
   bundle_vector inflight_bundles;
} trunk_node;

typedef struct ONDISK ondisk_trunk_node {
   uint16 height;
   uint16 num_pivots;
   uint16 num_inflight_bundles;
   uint32 pivot_offsets[];
} ondisk_trunk_node;

typedef VECTOR(trunk_node) trunk_node_vector;

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
   uint64                    num_bundles;
   trunk_pivot_stats         input_stats;
   bundle_compaction_state   state;
   branch_merger             merger;
   branch_ref                output_branch;
   trunk_pivot_stats         output_stats;
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
   platform_batch_rwlock    root_lock;
   uint64                   root_addr;
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
branches_equal(branch_ref a, branch_ref b)
{
   return a.addr == b.addr;
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
      vector_deinit(&bndl->branches);
   }
   return rc;
}

static platform_status
bundle_init_copy(bundle *dst, platform_heap_id hid, const bundle *src)
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

static void
bundle_deinit(bundle *bndl)
{
   vector_deinit(&bndl->branches);
}

static void
bundle_reset(bundle *bndl)
{
   vector_truncate(&bndl->branches, 0);
   bndl->maplet = NULL_ROUTING_FILTER;
}

static platform_status
bundle_add_branches(bundle            *bndl,
                    routing_filter     new_maplet,
                    branch_ref_vector *new_branches)
{
   platform_status rc;
   rc = vector_append_vector(&bndl->branches, new_branches);
   if (!SUCCESS(rc)) {
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
   debug_assert(i < vector_length(&bndl->branches));
   return vector_get(&bndl->branches, i);
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
   platform_assert(a.num_kv_bytes >= b.num_kv_bytes);
   platform_assert(a.num_tuples >= b.num_tuples);
   return (trunk_pivot_stats){.num_kv_bytes = a.num_kv_bytes - b.num_kv_bytes,
                              .num_tuples   = a.num_tuples - b.num_tuples};
}

static trunk_pivot_stats
trunk_pivot_stats_add(trunk_pivot_stats a, trunk_pivot_stats b)
{
   return (trunk_pivot_stats){.num_kv_bytes = a.num_kv_bytes + b.num_kv_bytes,
                              .num_tuples   = a.num_tuples + b.num_tuples};
}

/******************
 * pivot operations
 ******************/

#define TRUNK_STATS_ZERO                                                       \
   ((trunk_pivot_stats){.num_kv_bytes = 0, .num_tuples = 0})

static pivot *
pivot_create(platform_heap_id  hid,
             key               k,
             uint64            child_addr,
             uint64            inflight_bundle_start,
             trunk_pivot_stats prereceive_stats,
             trunk_pivot_stats stats)
{
   pivot *result = TYPED_FLEXIBLE_STRUCT_ZALLOC(
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

static pivot *
pivot_copy(platform_heap_id hid, pivot *src)
{
   return pivot_create(hid,
                       ondisk_key_to_key(&src->key),
                       src->child_addr,
                       src->inflight_bundle_start,
                       src->prereceive_stats,
                       src->stats);
}

static void
pivot_destroy(pivot *pvt, platform_heap_id hid)
{
   platform_free(hid, pvt);
}

static key
pivot_key(const pivot *pvt)
{
   return ondisk_key_to_key(&pvt->key);
}

static uint64
pivot_child_addr(const pivot *pvt)
{
   return pvt->child_addr;
}

static void
pivot_set_child_addr(pivot *pvt, uint64 new_child_addr)
{
   pvt->child_addr = new_child_addr;
}


static trunk_pivot_stats
pivot_stats(const pivot *pvt)
{
   return pvt->stats;
}

static uint64
pivot_inflight_bundle_start(const pivot *pvt)
{
   return pvt->inflight_bundle_start;
}

static void
pivot_set_inflight_bundle_start(pivot *pvt, uint64 start)
{
   pvt->inflight_bundle_start = start;
}

static trunk_pivot_stats
pivot_received_bundles_stats(const pivot *pvt)
{
   return trunk_pivot_stats_subtract(pvt->stats, pvt->prereceive_stats);
}

static uint64
pivot_num_kv_bytes(const pivot *pvt)
{
   return pvt->stats.num_kv_bytes;
}

/*
 * When new bundles get flushed to this pivot's node, you must
 * inform the pivot of the tuple counts of the new bundles.
 */
static void
pivot_add_tuple_counts(pivot *pvt, int coefficient, trunk_pivot_stats stats)
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
}

/***********************
 * basic node operations
 ***********************/

static void
node_init(trunk_node   *node,
          uint16        height,
          pivot_vector  pivots,
          bundle_vector pivot_bundles,
          uint64        num_old_bundles,
          bundle_vector inflight_bundles)
{
   node->height           = height;
   node->pivots           = pivots;
   node->pivot_bundles    = pivot_bundles;
   node->num_old_bundles  = num_old_bundles;
   node->inflight_bundles = inflight_bundles;
}

static platform_status
node_init_empty_leaf(trunk_node *node, platform_heap_id hid, key lb, key ub)
{
   pivot_vector    pivots;
   bundle_vector   pivot_bundles;
   bundle_vector   inflight_bundles;
   platform_status rc;

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

   pivot *lb_pivot =
      pivot_create(hid, lb, 0, 0, TRUNK_STATS_ZERO, TRUNK_STATS_ZERO);
   pivot *ub_pivot =
      pivot_create(hid, ub, 0, 0, TRUNK_STATS_ZERO, TRUNK_STATS_ZERO);
   if (lb_pivot == NULL || ub_pivot == NULL) {
      rc = STATUS_NO_MEMORY;
      goto cleanup_pivots;
   }
   rc = vector_append(&pivots, lb_pivot);
   platform_assert_status_ok(rc);
   rc = vector_append(&pivots, ub_pivot);
   platform_assert_status_ok(rc);

   rc = VECTOR_EMPLACE_APPEND(&pivot_bundles, bundle_init, hid);
   platform_assert_status_ok(rc);

   node_init(node, 0, pivots, pivot_bundles, 0, inflight_bundles);
   return STATUS_OK;

cleanup_pivots:
   if (lb_pivot != NULL) {
      pivot_destroy(lb_pivot, hid);
   }
   if (ub_pivot != NULL) {
      pivot_destroy(ub_pivot, hid);
   }
cleanup_vectors:
   VECTOR_APPLY_TO_ELTS(&pivots, pivot_destroy, hid);
   vector_deinit(&pivots);
   VECTOR_APPLY_TO_PTRS(&pivot_bundles, bundle_deinit);
   vector_deinit(&pivot_bundles);
   vector_deinit(&inflight_bundles);
   return rc;
}

static uint64
node_num_children(const trunk_node *node)
{
   return vector_length(&node->pivots) - 1;
}

static pivot *
node_pivot(const trunk_node *node, uint64 i)
{
   return vector_get(&node->pivots, i);
}

static key
node_pivot_key(const trunk_node *node, uint64 i)
{
   return pivot_key(vector_get(&node->pivots, i));
}

static key
node_pivot_min_key(const trunk_node *node)
{
   return pivot_key(vector_get(&node->pivots, 0));
}

debug_only static key
node_pivot_max_key(const trunk_node *node)
{
   return pivot_key(
      vector_get(&node->pivots, vector_length(&node->pivots) - 1));
}

static bundle *
node_pivot_bundle(trunk_node *node, uint64 i)
{
   return vector_get_ptr(&node->pivot_bundles, i);
}

static uint64
node_height(const trunk_node *node)
{
   return node->height;
}

static bool32
node_is_leaf(const trunk_node *node)
{
   return node->height == 0;
}

static uint64
node_first_live_inflight_bundle(const trunk_node *node)
{
   uint64 result = UINT64_MAX;
   for (uint64 i = 0; i < vector_length(&node->pivots); i++) {
      pivot *pvt = vector_get(&node->pivots, i);
      result     = MIN(result, pvt->inflight_bundle_start);
   }
   return result;
}

static uint64
leaf_num_tuples(const trunk_node *node)
{
   trunk_pivot_stats stats = pivot_stats(vector_get(&node->pivots, 0));
   return stats.num_tuples;
}

static uint64
leaf_num_kv_bytes(const trunk_node *node)
{
   trunk_pivot_stats stats = pivot_stats(vector_get(&node->pivots, 0));
   return stats.num_kv_bytes;
}

static uint64
node_num_old_bundles(const trunk_node *node)
{
   return node->num_old_bundles;
}

static bool32
node_pivot_has_received_bundles(const trunk_node *node, uint64 i)
{
   pivot *pvt = vector_get(&node->pivots, i);
   return pivot_inflight_bundle_start(pvt) <= node->num_old_bundles;
}

debug_only static bool
node_is_well_formed_leaf(const trunk_node_config *cfg, const trunk_node *node)
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
   key    lbkey = pivot_key(lb);
   key    ubkey = pivot_key(ub);
   return lb->child_addr == 0 && lb->inflight_bundle_start == 0
          && data_key_compare(cfg->data_cfg, lbkey, ubkey) < 0
          && lb->prereceive_stats.num_tuples <= lb->stats.num_tuples;
}

debug_only static bool
node_is_well_formed_index(const data_config *data_cfg, const trunk_node *node)
{
   bool basics =
      0 < node->height && 1 < vector_length(&node->pivots)
      && vector_length(&node->pivot_bundles) == vector_length(&node->pivots) - 1
      && node->num_old_bundles <= vector_length(&node->inflight_bundles);
   if (!basics) {
      return FALSE;
   }

   for (uint64 i = 0; i < node_num_children(node); i++) {
      pivot *lb    = vector_get(&node->pivots, i);
      pivot *ub    = vector_get(&node->pivots, i + 1);
      key    lbkey = pivot_key(lb);
      key    ubkey = pivot_key(ub);
      bool   valid_pivots =
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

static void
node_deinit(trunk_node *node, trunk_node_context *context)
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
sizeof_ondisk_bundle(ondisk_bundle *odb)
{
   return sizeof(*odb) + sizeof(odb->branches[0]) * odb->num_branches;
}

static uint64
ondisk_bundle_size(uint64 num_branches)
{
   return sizeof(ondisk_bundle) + sizeof(branch_ref) * num_branches;
}

/****************************************************
 * Basic accessors for ondisk pivots
 ****************************************************/

static uint64
sizeof_ondisk_pivot(ondisk_pivot *odp)
{
   return sizeof(*odp) + sizeof_ondisk_key_data(&odp->key);
}

static uint64
ondisk_pivot_size(key k)
{
   return sizeof(ondisk_pivot) + ondisk_key_required_data_capacity(k);
}

static key
ondisk_pivot_key(ondisk_pivot *odp)
{
   return ondisk_key_to_key(&odp->key);
}

/********************************************************
 * Node serialization/deserialization and refcounting.
 ********************************************************/

typedef struct ondisk_node_handle {
   cache       *cc;
   page_handle *header_page;
   page_handle *content_page;
} ondisk_node_handle;

static platform_status
ondisk_node_handle_init(ondisk_node_handle *handle, cache *cc, uint64 addr)
{
   handle->cc          = cc;
   handle->header_page = cache_get(cc, addr, TRUE, PAGE_TYPE_TRUNK);
   if (handle->header_page == NULL) {
      return STATUS_IO_ERROR;
   }
   handle->content_page = NULL;
   return STATUS_OK;
}

static void
ondisk_node_handle_deinit(ondisk_node_handle *handle)
{
   if (handle->content_page != NULL
       && handle->content_page != handle->header_page) {
      cache_unget(handle->cc, handle->content_page);
   }
   cache_unget(handle->cc, handle->header_page);
}

static uint64
content_page_offset(ondisk_node_handle *handle)
{
   return handle->content_page->disk_addr - handle->header_page->disk_addr;
}

static bool32
offset_is_in_content_page(ondisk_node_handle *handle, uint32 offset)
{
   uint64 page_size = cache_page_size(handle->cc);
   return handle->content_page != NULL && content_page_offset(handle) <= offset
          && offset < content_page_offset(handle) + page_size;
}

static platform_status
ondisk_node_handle_setup_content_page(ondisk_node_handle *handle, uint64 offset)
{
   uint64 page_size = cache_page_size(handle->cc);

   if (offset_is_in_content_page(handle, offset)) {
      return STATUS_OK;
   }

   if (handle->content_page != NULL
       && handle->content_page != handle->header_page) {
      cache_unget(handle->cc, handle->content_page);
   }

   if (offset < page_size) {
      handle->content_page = handle->header_page;
      return STATUS_OK;
   } else {
      uint64 addr = handle->header_page->disk_addr + offset;
      addr -= (addr % page_size);
      handle->content_page = cache_get(handle->cc, addr, TRUE, PAGE_TYPE_TRUNK);
      return handle->content_page == NULL ? STATUS_IO_ERROR : STATUS_OK;
   }
}

static ondisk_pivot *
ondisk_node_get_pivot(ondisk_node_handle *handle, uint64 pivot_num)
{
   ondisk_trunk_node *header = (ondisk_trunk_node *)handle->header_page->data;
   uint64             offset = header->pivot_offsets[pivot_num];
   platform_status rc = ondisk_node_handle_setup_content_page(handle, offset);
   if (!SUCCESS(rc)) {
      return NULL;
   }
   return (ondisk_pivot *)(handle->content_page->data + offset
                           - content_page_offset(handle));
}

static ondisk_bundle *
ondisk_node_get_pivot_bundle(ondisk_node_handle *handle, uint64 pivot_num)
{
   ondisk_pivot *pivot = ondisk_node_get_pivot(handle, pivot_num);
   if (pivot == NULL) {
      return NULL;
   }
   return (ondisk_bundle *)(((char *)pivot) + sizeof_ondisk_pivot(pivot));
}

static ondisk_bundle *
ondisk_node_bundle_at_offset(ondisk_node_handle *handle, uint64 offset)
{
   uint64 page_size = cache_page_size(handle->cc);

   /* If there's not enough room for a bundle header, skip to the next
    * page. */
   if (page_size - (offset % page_size) < sizeof(ondisk_bundle)) {
      offset += page_size - (offset % page_size);
   }

   platform_status rc = ondisk_node_handle_setup_content_page(handle, offset);
   if (!SUCCESS(rc)) {
      return NULL;
   }
   ondisk_bundle *result = (ondisk_bundle *)(handle->content_page->data + offset
                                             - content_page_offset(handle));

   /* If there wasn't enough room for this bundle on this page, then we would
    * have zeroed the remaining bytes and put the bundle on the next page. */
   if (result->num_branches == 0) {
      offset += page_size - (offset % page_size);
      rc = ondisk_node_handle_setup_content_page(handle, offset);
      if (!SUCCESS(rc)) {
         return NULL;
      }
      result = (ondisk_bundle *)(handle->content_page->data + offset
                                 - content_page_offset(handle));
   }
   return result;
}

static ondisk_bundle *
ondisk_node_get_first_inflight_bundle(ondisk_node_handle *handle)
{
   ondisk_trunk_node *header = (ondisk_trunk_node *)handle->header_page->data;
   ondisk_pivot *pivot  = ondisk_node_get_pivot(handle, header->num_pivots - 1);
   uint64        offset = header->pivot_offsets[header->num_pivots - 1]
                   + sizeof_ondisk_pivot(pivot);
   return ondisk_node_bundle_at_offset(handle, offset);
}

static ondisk_bundle *
ondisk_node_get_next_inflight_bundle(ondisk_node_handle *handle,
                                     ondisk_bundle      *bundle)
{
   uint64 offset = ((char *)bundle) - handle->content_page->data
                   + content_page_offset(handle) + sizeof_ondisk_bundle(bundle);
   return ondisk_node_bundle_at_offset(handle, offset);
}

static pivot *
pivot_deserialize(platform_heap_id   hid,
                  ondisk_trunk_node *header,
                  ondisk_pivot      *odp)
{
   return pivot_create(hid,
                       ondisk_pivot_key(odp),
                       odp->child_addr,
                       header->num_inflight_bundles
                          - odp->num_live_inflight_bundles,
                       odp->stats,
                       odp->stats);
}

static platform_status
bundle_deserialize(bundle *bndl, platform_heap_id hid, ondisk_bundle *odb)
{
   platform_status rc =
      bundle_init_single(bndl, hid, odb->maplet, odb->branches[0]);
   if (!SUCCESS(rc)) {
      return rc;
   }
   for (uint64 i = 1; i < odb->num_branches; i++) {
      rc = vector_append(&bndl->branches, odb->branches[i]);
      if (!SUCCESS(rc)) {
         bundle_deinit(bndl);
         return rc;
      }
   }
   return STATUS_OK;
}

static platform_status
node_deserialize(trunk_node_context *context, uint64 addr, trunk_node *result)
{
   platform_status    rc;
   ondisk_node_handle handle;

   rc = ondisk_node_handle_init(&handle, context->cc, addr);
   if (!SUCCESS(rc)) {
      return rc;
   }
   ondisk_trunk_node *header = (ondisk_trunk_node *)handle.header_page->data;

   pivot_vector  pivots;
   bundle_vector inflight_bundles;
   bundle_vector pivot_bundles;
   vector_init(&pivots, context->hid);
   vector_init(&inflight_bundles, context->hid);
   vector_init(&pivot_bundles, context->hid);

   rc = vector_ensure_capacity(&pivots, header->num_pivots);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc = vector_ensure_capacity(&pivot_bundles, header->num_pivots - 1);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc = vector_ensure_capacity(&inflight_bundles, header->num_inflight_bundles);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   for (uint64 i = 0; i < header->num_pivots; i++) {
      ondisk_pivot *odp = ondisk_node_get_pivot(&handle, i);
      if (odp == NULL) {
         rc = STATUS_IO_ERROR;
         goto cleanup;
      }
      pivot *imp = pivot_deserialize(context->hid, header, odp);
      if (imp == NULL) {
         rc = STATUS_NO_MEMORY;
         goto cleanup;
      }
      rc = vector_append(&pivots, imp);
      if (!SUCCESS(rc)) {
         pivot_destroy(imp, context->hid);
         goto cleanup;
      }
   }

   for (uint64 i = 0; i < header->num_pivots - 1; i++) {
      ondisk_bundle *odb = ondisk_node_get_pivot_bundle(&handle, i);
      if (odb == NULL) {
         rc = STATUS_IO_ERROR;
         goto cleanup;
      }
      rc = VECTOR_EMPLACE_APPEND(
         &pivot_bundles, bundle_deserialize, context->hid, odb);
      if (!SUCCESS(rc)) {
         goto cleanup;
      }
   }

   ondisk_bundle *odb = ondisk_node_get_first_inflight_bundle(&handle);
   for (uint64 i = 0; i < header->num_inflight_bundles; i++) {
      if (odb == NULL) {
         rc = STATUS_IO_ERROR;
         goto cleanup;
      }
      rc = VECTOR_EMPLACE_APPEND(
         &inflight_bundles, bundle_deserialize, context->hid, odb);
      if (!SUCCESS(rc)) {
         goto cleanup;
      }
      odb = ondisk_node_get_next_inflight_bundle(&handle, odb);
   }

   vector_reverse(&inflight_bundles);

   node_init(result,
             header->height,
             pivots,
             pivot_bundles,
             header->num_inflight_bundles,
             inflight_bundles);

cleanup:
   VECTOR_APPLY_TO_ELTS(&pivots, pivot_destroy, context->hid);
   VECTOR_APPLY_TO_PTRS(&pivot_bundles, bundle_deinit);
   VECTOR_APPLY_TO_PTRS(&inflight_bundles, bundle_deinit);
   vector_deinit(&pivots);
   vector_deinit(&pivot_bundles);
   vector_deinit(&inflight_bundles);
   ondisk_node_handle_deinit(&handle);
   return rc;
}

static void
bundle_inc_all_refs(trunk_node_context *context, bundle *bndl)
{
   routing_filter_inc_ref(context->cc, &bndl->maplet);
   for (uint64 i = 0; i < vector_length(&bndl->branches); i++) {
      branch_ref bref = vector_get(&bndl->branches, i);
      btree_inc_ref_range(context->cc,
                          context->cfg->btree_cfg,
                          branch_ref_addr(bref),
                          NEGATIVE_INFINITY_KEY,
                          POSITIVE_INFINITY_KEY);
   }
}

static void
bundle_dec_all_refs(trunk_node_context *context, bundle *bndl)
{
   routing_filter_dec_ref(context->cc, &bndl->maplet);
   for (uint64 i = 0; i < vector_length(&bndl->branches); i++) {
      branch_ref bref = vector_get(&bndl->branches, i);
      btree_dec_ref(context->cc,
                    context->cfg->btree_cfg,
                    branch_ref_addr(bref),
                    PAGE_TYPE_BRANCH);
   }
}

static void
on_disk_node_dec_ref(trunk_node_context *context, uint64 addr)
{
   uint8 refcount = allocator_dec_ref(context->al, addr, PAGE_TYPE_TRUNK);
   if (refcount == AL_NO_REFS) {
      trunk_node      node;
      platform_status rc = node_deserialize(context, addr, &node);
      if (SUCCESS(rc)) {
         for (uint64 i = 0; i < vector_length(&node.pivots); i++) {
            pivot *pvt = vector_get(&node.pivots, i);
            on_disk_node_dec_ref(context, pvt->child_addr);
         }
         for (uint64 i = 0; i < vector_length(&node.pivot_bundles); i++) {
            bundle *bndl = vector_get_ptr(&node.pivot_bundles, i);
            bundle_dec_all_refs(context, bndl);
         }
         for (uint64 i = 0; i < vector_length(&node.inflight_bundles); i++) {
            bundle *bndl = vector_get_ptr(&node.inflight_bundles, i);
            bundle_dec_all_refs(context, bndl);
         }
         node_deinit(&node, context);
      }
      allocator_dec_ref(context->al, addr, PAGE_TYPE_TRUNK);
   }
}

static void
on_disk_node_inc_ref(trunk_node_context *context, uint64 addr)
{
   allocator_inc_ref(context->al, addr);
}

static void
node_inc_all_refs(trunk_node_context *context, trunk_node *node)
{
   for (uint64 i = 0; i < vector_length(&node->pivots); i++) {
      pivot *pvt = vector_get(&node->pivots, i);
      on_disk_node_inc_ref(context, pvt->child_addr);
   }
   for (uint64 i = 0; i < vector_length(&node->pivot_bundles); i++) {
      bundle *bndl = vector_get_ptr(&node->pivot_bundles, i);
      bundle_inc_all_refs(context, bndl);
   }
   uint64 inflight_start = node_first_live_inflight_bundle(node);
   for (uint64 i = inflight_start; i < vector_length(&node->inflight_bundles);
        i++) {
      bundle *bndl = vector_get_ptr(&node->inflight_bundles, i);
      bundle_inc_all_refs(context, bndl);
   }
}

static uint64
pivot_ondisk_size(pivot *pvt)
{
   return ondisk_pivot_size(pivot_key(pvt));
}

static uint64
bundle_ondisk_size(bundle *bndl)
{
   return ondisk_bundle_size(vector_length(&bndl->branches));
}

static void
pivot_serialize(trunk_node_context *context,
                trunk_node         *node,
                uint64              pivot_num,
                ondisk_pivot       *dest)
{
   pivot *pvt       = vector_get(&node->pivots, pivot_num);
   dest->stats      = pvt->stats;
   dest->child_addr = pvt->child_addr;
   dest->num_live_inflight_bundles =
      vector_length(&node->inflight_bundles) - pvt->inflight_bundle_start;
   copy_key_to_ondisk_key(&dest->key, pivot_key(pvt));
}

static void
bundle_serialize(bundle *bndl, ondisk_bundle *dest)
{
   dest->maplet       = bndl->maplet;
   dest->num_branches = vector_length(&bndl->branches);
   for (uint64 i = 0; i < dest->num_branches; i++) {
      dest->branches[i] = vector_get(&bndl->branches, i);
   }
}

static platform_status
node_serialize_maybe_setup_next_page(cache        *cc,
                                     uint64        required_space,
                                     page_handle  *header_page,
                                     page_handle **current_page,
                                     uint64       *page_offset)
{
   uint64 page_size   = cache_page_size(cc);
   uint64 extent_size = cache_extent_size(cc);

   if (page_size < required_space) {
      return STATUS_LIMIT_EXCEEDED;
   }

   if (page_size < *page_offset + required_space) {
      memset((*current_page)->data + *page_offset, 0, page_size - *page_offset);
      if (*current_page != header_page) {
         cache_unlock(cc, *current_page);
         cache_unclaim(cc, *current_page);
         cache_unget(cc, *current_page);
      }
      (*current_page)->disk_addr += page_size;
      if (extent_size
          < (*current_page)->disk_addr + page_size - header_page->disk_addr)
      {
         return STATUS_LIMIT_EXCEEDED;
      }
      *current_page =
         cache_alloc(cc, (*current_page)->disk_addr, PAGE_TYPE_TRUNK);
      if (*current_page == NULL) {
         return STATUS_NO_MEMORY;
      }
      *page_offset = 0;
   }

   return STATUS_OK;
}

static pivot *
node_serialize(trunk_node_context *context, trunk_node *node)
{
   platform_status rc;
   uint64          header_addr  = 0;
   page_handle    *header_page  = NULL;
   page_handle    *current_page = NULL;

   pivot *result = pivot_create(context->hid,
                                node_pivot_key(node, 0),
                                0,
                                0,
                                TRUNK_STATS_ZERO,
                                TRUNK_STATS_ZERO);
   if (result == NULL) {
      return NULL;
   }

   rc = allocator_alloc(context->al, &header_addr, PAGE_TYPE_TRUNK);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   result->child_addr = header_addr;

   header_page = cache_alloc(context->cc, header_addr, PAGE_TYPE_TRUNK);
   if (header_page == NULL) {
      rc = STATUS_NO_MEMORY;
      goto cleanup;
   }

   ondisk_trunk_node *odnode    = (ondisk_trunk_node *)header_page->data;
   odnode->height               = node->height;
   odnode->num_pivots           = vector_length(&node->pivots);
   odnode->num_inflight_bundles = vector_length(&node->inflight_bundles);

   current_page = header_page;
   uint64 page_offset =
      sizeof(*odnode) + sizeof(odnode->pivot_offsets[0]) * odnode->num_pivots;

   for (uint64 i = 0; i < vector_length(&node->pivots); i++) {
      uint64 pivot_size     = pivot_ondisk_size(vector_get(&node->pivots, i));
      uint64 required_space = pivot_size;

      bundle *pivot_bundle;
      uint64  bundle_size;
      if (i < vector_length(&node->pivots) - 1) {
         pivot_bundle = vector_get_ptr(&node->pivot_bundles, i);
         bundle_size  = bundle_ondisk_size(pivot_bundle);
         required_space += bundle_size;
      }

      rc = node_serialize_maybe_setup_next_page(
         context->cc, required_space, header_page, &current_page, &page_offset);
      if (!SUCCESS(rc)) {
         goto cleanup;
      }

      odnode->pivot_offsets[i] =
         current_page->disk_addr - header_addr + page_offset;
      pivot_serialize(
         context, node, i, (ondisk_pivot *)(current_page->data + page_offset));
      page_offset += pivot_size;
      if (i < vector_length(&node->pivots) - 1) {
         bundle_serialize(pivot_bundle,
                          (ondisk_bundle *)(current_page->data + page_offset));
         page_offset += bundle_size;
      }
   }

   uint64 min_inflight_bundle_start = node_first_live_inflight_bundle(node);

   for (int64 i = vector_length(&node->inflight_bundles) - 1;
        i >= min_inflight_bundle_start;
        i--)
   {
      bundle *bndl        = vector_get_ptr(&node->inflight_bundles, i);
      uint64  bundle_size = bundle_ondisk_size(bndl);

      rc = node_serialize_maybe_setup_next_page(
         context->cc, bundle_size, header_page, &current_page, &page_offset);
      if (!SUCCESS(rc)) {
         goto cleanup;
      }

      bundle_serialize(bndl,
                       (ondisk_bundle *)(current_page->data + page_offset));
      page_offset += bundle_size;
   }

   node_inc_all_refs(context, node);
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
   if (header_addr != 0) {
      allocator_dec_ref(context->al, header_addr, PAGE_TYPE_TRUNK);
      allocator_dec_ref(context->al, header_addr, PAGE_TYPE_TRUNK);
   }
   if (result != NULL) {
      pivot_destroy(result, context->hid);
   }
   return NULL;
}

static platform_status
serialize_nodes(trunk_node_context *context,
                trunk_node_vector  *nodes,
                pivot_vector       *result)
{
   platform_status rc;

   rc = vector_ensure_capacity(result, vector_length(nodes));
   if (!SUCCESS(rc)) {
      goto finish;
   }
   for (uint64 i = 0; i < vector_length(nodes); i++) {
      pivot *pvt = node_serialize(context, vector_get_ptr(nodes, i));
      if (pvt == NULL) {
         rc = STATUS_NO_MEMORY;
         goto finish;
      }
      rc = vector_append(result, pvt);
      platform_assert_status_ok(rc);
   }

finish:
   if (!SUCCESS(rc)) {
      for (uint64 i = 0; i < vector_length(result); i++) {
         on_disk_node_dec_ref(context, pivot_child_addr(vector_get(result, i)));
      }
      VECTOR_APPLY_TO_ELTS(result, pivot_destroy, context->hid);
      vector_truncate(result, 0);
   }

   return rc;
}

/*********************************************
 * branch_merger operations
 * (used in both leaf splits and compactions)
 *********************************************/

static void
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
branch_merger_add_routed_bundle(branch_merger      *merger,
                                cache              *cc,
                                const btree_config *btree_cfg,
                                bundle             *routed)
{
   for (uint64 i = 0; i < bundle_num_branches(routed); i++) {
      btree_iterator *iter = TYPED_MALLOC(merger->hid, iter);
      if (iter == NULL) {
         return STATUS_NO_MEMORY;
      }
      branch_ref bref = bundle_branch(routed, i);
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

static platform_status
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
 * concurrency in accessing the root
 ************************/

void
trunk_read_begin(trunk_node_context *context)
{
   platform_batch_rwlock_get(&context->root_lock, 0);
}

void
trunk_read_end(trunk_node_context *context)
{
   platform_batch_rwlock_unget(&context->root_lock, 0);
}

void
trunk_modification_begin(trunk_node_context *context)
{
   platform_batch_rwlock_get(&context->root_lock, 0);
   platform_batch_rwlock_claim_loop(&context->root_lock, 0);
}

void
trunk_set_root_address(trunk_node_context *context, uint64 new_root_addr)
{
   uint64 old_root_addr;
   platform_batch_rwlock_lock(&context->root_lock, 0);
   old_root_addr      = context->root_addr;
   context->root_addr = new_root_addr;
   platform_batch_rwlock_unlock(&context->root_lock, 0);
   on_disk_node_dec_ref(context, old_root_addr);
}

void
trunk_modification_end(trunk_node_context *context)
{
   platform_batch_rwlock_unclaim(&context->root_lock, 0);
   platform_batch_rwlock_unget(&context->root_lock, 0);
}

/*************************
 * generic code to apply changes to nodes in the tree.
 ************************/

typedef platform_status(apply_changes_fn)(trunk_node_context *context,
                                          uint64              addr,
                                          trunk_node         *node,
                                          void               *arg);

static platform_status
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

   trunk_node node;
   rc = node_deserialize(context, addr, &node);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (node_height(&node) == height) {
      rc = func(context, addr, &node, arg);
   } else {

      for (uint64 i = 0; i < node_num_children(&node); i++) {
         pivot *child_pivot  = node_pivot(&node, i);
         key    child_minkey = pivot_key(child_pivot);
         key    child_maxkey = node_pivot_key(&node, i + 1);
         if (data_key_compare(context->cfg->data_cfg, child_minkey, maxkey) < 0
             && data_key_compare(context->cfg->data_cfg, minkey, child_maxkey)
                   < 0)
         {
            uint64 child_addr = pivot_child_addr(child_pivot);
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

            pivot_set_child_addr(child_pivot, child_addr);
         }
      }

      if (SUCCESS(rc)) {
         pivot *pvt = node_serialize(context, &node);
         if (pvt == NULL) {
            rc = STATUS_NO_MEMORY;
         } else {
            *new_addr = pivot_child_addr(pvt);
         }
      }
   }

   node_deinit(&node, context);

   return rc;
}

static platform_status
apply_changes(trunk_node_context *context,
              key                 minkey,
              key                 maxkey,
              uint64              height,
              apply_changes_fn   *func,
              void               *arg)
{
   uint64 new_root_addr;
   trunk_modification_begin(context);
   platform_status rc = apply_changes_internal(context,
                                               context->root_addr,
                                               minkey,
                                               maxkey,
                                               height,
                                               func,
                                               arg,
                                               &new_root_addr);
   if (SUCCESS(rc)) {
      trunk_set_root_address(context, new_root_addr);
   }
   trunk_modification_end(context);
   return rc;
}

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
   if (!branches_equal(compaction->output_branch, NULL_BRANCH_REF)) {
      btree_dec_ref(context->cc,
                    context->cfg->btree_cfg,
                    branch_ref_addr(compaction->output_branch),
                    PAGE_TYPE_BRANCH);
   }
   platform_free(context->hid, compaction);
}

static bundle_compaction *
bundle_compaction_create(trunk_node         *node,
                         uint64              pivot_num,
                         trunk_node_context *context)
{
   platform_status rc;
   pivot          *pvt = node_pivot(node, pivot_num);

   bundle_compaction *result = TYPED_ZALLOC(context->hid, result);
   if (result == NULL) {
      return NULL;
   }
   result->state       = BUNDLE_COMPACTION_NOT_STARTED;
   result->input_stats = pivot_received_bundles_stats(pvt);
   branch_merger_init(&result->merger,
                      context->hid,
                      context->cfg->data_cfg,
                      pivot_key(pvt),
                      node_pivot_key(node, pivot_num + 1),
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
   result->num_bundles =
      vector_length(&node->inflight_bundles) - node->num_old_bundles;
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
                            key                   pivot_key,
                            uint64                height)
{
   *lock = pivot_state_map_hash(context->cfg->data_cfg, pivot_key, height);
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
                    key                   pivot_key,
                    uint64                height)
{
   pivot_compaction_state *result = NULL;
   for (pivot_compaction_state *state = map->buckets[*lock]; state != NULL;
        state                         = state->next)
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

static pivot_compaction_state *
pivot_state_map_create(trunk_node_context   *context,
                       pivot_state_map      *map,
                       pivot_state_map_lock *lock,
                       key                   pivot_key,
                       uint64                height)
{
   pivot_compaction_state *state = TYPED_ZALLOC(context->hid, state);
   if (state == NULL) {
      return NULL;
   }
   platform_status rc =
      key_buffer_init_from_key(&state->key, context->hid, pivot_key);
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
                              key                   pivot_key,
                              uint64                height)
{
   pivot_compaction_state *state =
      pivot_state_map_get(context, map, lock, pivot_key, height);
   if (state == NULL) {
      state = pivot_state_map_create(context, map, lock, pivot_key, height);
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
   uint64                  num_input_bundles;
   routing_filter          new_maplet;
   branch_ref_vector       branches;
   trunk_pivot_stats       delta;
} maplet_compaction_apply_args;

static platform_status
apply_changes_maplet_compaction(trunk_node_context *context,
                                uint64              addr,
                                trunk_node         *target,
                                void               *arg)
{
   platform_status               rc;
   maplet_compaction_apply_args *args = (maplet_compaction_apply_args *)arg;

   for (uint64 i = 0; i < node_num_children(target); i++) {
      bundle *bndl = node_pivot_bundle(target, i);
      if (routing_filters_equal(&bndl->maplet, &args->state->maplet)) {
         rc = bundle_add_branches(bndl, args->new_maplet, &args->branches);
         if (!SUCCESS(rc)) {
            return rc;
         }
         pivot *pvt = node_pivot(target, i);
         pivot_set_inflight_bundle_start(
            pvt, pivot_inflight_bundle_start(pvt) + args->num_input_bundles);
         pivot_add_tuple_counts(pvt, -1, args->delta);
         break;
      }
   }

   return STATUS_OK;
}

static platform_status
enqueue_maplet_compaction(pivot_compaction_state *args);

static void
maplet_compaction_task(void *arg, void *scratch)
{
   pivot_state_map_lock         lock;
   platform_status              rc      = STATUS_OK;
   pivot_compaction_state      *state   = (pivot_compaction_state *)arg;
   trunk_node_context          *context = state->context;
   maplet_compaction_apply_args apply_args;
   ZERO_STRUCT(apply_args);
   apply_args.state = state;
   vector_init(&apply_args.branches, context->hid);

   routing_filter     new_maplet;
   routing_filter     old_maplet = state->maplet;
   bundle_compaction *bc         = state->bundle_compactions;
   while (bc != NULL && bc->state == BUNDLE_COMPACTION_SUCCEEDED) {
      rc = routing_filter_add(context->cc,
                              context->cfg->filter_cfg,
                              context->hid,
                              &old_maplet,
                              &new_maplet,
                              bc->fingerprints,
                              bc->output_stats.num_tuples,
                              state->num_branches
                                 + vector_length(&apply_args.branches));
      if (0 < apply_args.num_input_bundles) {
         routing_filter_dec_ref(context->cc, &old_maplet);
      }
      if (!SUCCESS(rc)) {
         goto cleanup;
      }

      rc = vector_append(&apply_args.branches, bc->output_branch);
      if (!SUCCESS(rc)) {
         goto cleanup;
      }
      bc->output_branch = NULL_BRANCH_REF;

      trunk_pivot_stats delta =
         trunk_pivot_stats_subtract(bc->input_stats, bc->output_stats);
      apply_args.delta = trunk_pivot_stats_add(apply_args.delta, delta);

      old_maplet = new_maplet;
      apply_args.num_input_bundles += bc->num_bundles;
      bc = bc->next;
   }

   platform_assert(0 < apply_args.num_input_bundles);

   apply_args.new_maplet = new_maplet;

   rc = apply_changes(context,
                      key_buffer_key(&state->key),
                      key_buffer_key(&state->key),
                      state->height,
                      apply_changes_maplet_compaction,
                      &apply_args);

cleanup:
   pivot_state_map_aquire_lock(&lock,
                               context,
                               &context->pivot_states,
                               key_buffer_key(&state->key),
                               state->height);

   if (SUCCESS(rc)) {
      routing_filter_dec_ref(context->cc, &state->maplet);
      state->maplet = new_maplet;
      state->num_branches += vector_length(&apply_args.branches);
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
      if (0 < apply_args.num_input_bundles) {
         routing_filter_dec_ref(context->cc, &new_maplet);
      }
   }

   if (pivot_compaction_state_is_done(state)) {
      pivot_state_map_remove(&context->pivot_states, &lock, state);
      pivot_state_destroy(state);
   }

   pivot_state_map_release_lock(&lock, &context->pivot_states);
   vector_deinit(&apply_args.branches);
}

static platform_status
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

   bc->output_branch = create_branch_ref(pack_req.root_addr);
   bc->output_stats  = (trunk_pivot_stats){
       .num_tuples   = pack_req.num_tuples,
       .num_kv_bytes = pack_req.key_bytes + pack_req.message_bytes};
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
                          trunk_node         *node)
{
   uint64 height       = node_height(node);
   uint64 num_children = node_num_children(node);

   for (uint64 pivot_num = 0; pivot_num < num_children; pivot_num++) {
      if (node_pivot_has_received_bundles(node, pivot_num)) {
         platform_status rc        = STATUS_OK;
         key             pivot_key = node_pivot_key(node, pivot_num);

         pivot_state_map_lock lock;
         pivot_state_map_aquire_lock(
            &lock, context, &context->pivot_states, pivot_key, height);

         pivot_compaction_state *state = pivot_state_map_get_or_create(
            context, &context->pivot_states, &lock, pivot_key, height);
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
enqueue_bundle_compactions(trunk_node_context *context,
                           pivot_vector       *pivots,
                           trunk_node_vector  *nodes)
{
   debug_assert(vector_length(pivots) == vector_length(nodes));

   for (uint64 i = 0; i < vector_length(pivots); i++) {
      platform_status rc;
      pivot          *pvt  = vector_get(pivots, i);
      trunk_node     *node = vector_get_ptr(nodes, i);
      rc = enqueue_bundle_compaction(context, pivot_child_addr(pvt), node);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   return STATUS_OK;
}

static platform_status
serialize_nodes_and_enqueue_bundle_compactions(trunk_node_context *context,
                                               trunk_node_vector  *nodes,
                                               pivot_vector       *result)
{
   platform_status rc;

   rc = serialize_nodes(context, nodes, result);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = enqueue_bundle_compactions(context, result, nodes);
   if (!SUCCESS(rc)) {
      VECTOR_APPLY_TO_ELTS(result, pivot_destroy, context->hid);
      vector_truncate(result, 0);
      return rc;
   }

   return rc;
}


/************************
 * accounting maintenance
 ************************/

static platform_status
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

static platform_status
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

static platform_status
accumulate_inflight_bundle_tuple_counts_in_range(bundle             *bndl,
                                                 trunk_node_context *context,
                                                 pivot_vector       *pivots,
                                                 uint64              child_num,
                                                 btree_pivot_stats  *acc)
{
   key minkey = pivot_key(vector_get(pivots, child_num));
   key maxkey = pivot_key(vector_get(pivots, child_num + 1));

   return accumulate_branches_tuple_counts_in_range(
      &bndl->branches, context, minkey, maxkey, acc);
}

/*****************************************************
 * Receive bundles -- used in flushes and leaf splits
 *****************************************************/

static platform_status
node_receive_bundles(trunk_node_context *context,
                     trunk_node         *node,
                     bundle             *routed,
                     bundle_vector      *inflight,
                     uint64              inflight_start,
                     uint64              child_num)
{
   platform_status rc;

   rc = vector_ensure_capacity(&node->inflight_bundles,
                               (routed ? 1 : 0) + vector_length(inflight));
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (routed) {
      rc = VECTOR_EMPLACE_APPEND(
         &node->inflight_bundles, bundle_init_copy, context->hid, routed);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   for (uint64 i = inflight_start; i < vector_length(inflight); i++) {
      bundle *bndl = vector_get_ptr(inflight, i);
      rc           = VECTOR_EMPLACE_APPEND(
         &node->inflight_bundles, bundle_init_copy, context->hid, bndl);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   for (uint64 i = 0; i < node_num_children(node); i++) {
      btree_pivot_stats btree_stats;
      ZERO_CONTENTS(&btree_stats);
      rc = accumulate_inflight_bundle_tuple_counts_in_range(
         vector_get_ptr(inflight, inflight_start),
         context,
         &node->pivots,
         i,
         &btree_stats);
      if (!SUCCESS(rc)) {
         return rc;
      }
      trunk_pivot_stats trunk_stats =
         trunk_pivot_stats_from_btree_pivot_stats(btree_stats);
      pivot *pvt = node_pivot(node, i);
      pivot_add_tuple_counts(pvt, 1, trunk_stats);
   }

   return rc;
}

/************************
 * leaf splits
 ************************/

static bool
leaf_might_need_to_split(const trunk_node_config *cfg, trunk_node *leaf)
{
   return cfg->leaf_split_threshold_kv_bytes < leaf_num_kv_bytes(leaf);
}

static platform_status
leaf_estimate_unique_keys(trunk_node_context *context,
                          trunk_node         *leaf,
                          uint64             *estimate)
{
   platform_status rc;

   debug_assert(node_is_well_formed_leaf(context->cfg, leaf));

   routing_filter_vector maplets;
   vector_init(&maplets, context->hid);

   rc = VECTOR_MAP_PTRS(&maplets, bundle_maplet, &leaf->inflight_bundles);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   bundle pivot_bundle = vector_get(&leaf->pivot_bundles, 0);
   rc                  = vector_append(&maplets, bundle_maplet(&pivot_bundle));
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

   uint64 num_leaf_sb_fp         = leaf_num_tuples(leaf);
   uint64 est_num_leaf_sb_unique = num_sb_unique * num_leaf_sb_fp / num_sb_fp;
   uint64 est_num_non_leaf_sb_unique = num_sb_fp - est_num_leaf_sb_unique;

   uint64 est_leaf_unique = num_unique - est_num_non_leaf_sb_unique;
   *estimate              = est_leaf_unique;

cleanup:
   vector_deinit(&maplets);
   return STATUS_OK;
}

static platform_status
leaf_split_target_num_leaves(trunk_node_context *context,
                             trunk_node         *leaf,
                             uint64             *target)
{
   debug_assert(node_is_well_formed_leaf(context->cfg, leaf));

   if (!leaf_might_need_to_split(context->cfg, leaf)) {
      *target = 1;
      return STATUS_OK;
   }

   uint64          estimated_unique_keys;
   platform_status rc =
      leaf_estimate_unique_keys(context, leaf, &estimated_unique_keys);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64 num_tuples = leaf_num_tuples(leaf);
   if (estimated_unique_keys > num_tuples * 19 / 20) {
      estimated_unique_keys = num_tuples;
   }
   uint64 kv_bytes = leaf_num_kv_bytes(leaf);
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
                         trunk_node         *leaf,
                         uint64              target_num_leaves,
                         key_buffer_vector  *pivots)
{
   platform_status rc;
   pivot          *first   = vector_get(&leaf->pivots, 0);
   pivot          *last    = vector_get(&leaf->pivots, 1);
   key             min_key = ondisk_key_to_key(&first->key);
   key             max_key = ondisk_key_to_key(&last->key);

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
      bundle *bndl = vector_get_ptr(&leaf->inflight_bundles, bundle_num);
      rc           = branch_merger_add_routed_bundle(
         &merger, context->cc, context->cfg->btree_cfg, bndl);
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
         leaf_num * leaf_num_kv_bytes(leaf) / target_num_leaves;
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

static platform_status
leaf_split_init(trunk_node         *new_leaf,
                trunk_node_context *context,
                trunk_node         *leaf,
                key                 min_key,
                key                 max_key)
{
   platform_status rc;
   platform_assert(node_is_leaf(leaf));

   pivot *pvt = node_pivot(leaf, 0);

   rc = node_init_empty_leaf(new_leaf, context->hid, min_key, max_key);
   if (!SUCCESS(rc)) {
      return rc;
   }

   return node_receive_bundles(context,
                               new_leaf,
                               node_pivot_bundle(leaf, 0),
                               &leaf->inflight_bundles,
                               pivot_inflight_bundle_start(pvt),
                               0);
}

static platform_status
leaf_split(trunk_node_context *context,
           trunk_node         *leaf,
           trunk_node_vector  *new_leaves)
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
      rc          = VECTOR_EMPLACE_APPEND(
         new_leaves, leaf_split_init, context, leaf, min_key, max_key);
      if (!SUCCESS(rc)) {
         goto cleanup_new_leaves;
      }
   }

cleanup_new_leaves:
   if (!SUCCESS(rc)) {
      for (uint64 i = 0; i < vector_length(new_leaves); i++) {
         node_deinit(vector_get_ptr(new_leaves, i), context);
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
index_init_split(trunk_node      *new_index,
                 platform_heap_id hid,
                 trunk_node      *index,
                 uint64           start_child_num,
                 uint64           end_child_num)
{
   platform_status rc;

   pivot_vector pivots;
   vector_init(&pivots, hid);
   rc = vector_ensure_capacity(&pivots, end_child_num - start_child_num + 1);
   if (!SUCCESS(rc)) {
      goto cleanup_pivots;
   }
   for (uint64 i = start_child_num; i < end_child_num + 1; i++) {
      pivot *pvt  = vector_get(&index->pivots, i);
      pivot *copy = pivot_copy(hid, pvt);
      if (copy == NULL) {
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
      goto cleanup_pivot_bundles;
   }
   for (uint64 i = start_child_num; i < end_child_num; i++) {
      rc = VECTOR_EMPLACE_APPEND(&pivot_bundles,
                                 bundle_init_copy,
                                 hid,
                                 vector_get_ptr(&index->pivot_bundles, i));
      if (!SUCCESS(rc)) {
         goto cleanup_pivot_bundles;
      }
   }

   bundle_vector inflight_bundles;
   vector_init(&inflight_bundles, hid);
   if (!SUCCESS(rc)) {
      goto cleanup_inflight_bundles;
   }
   rc = VECTOR_EMPLACE_MAP_PTRS(
      &inflight_bundles, bundle_init_copy, &index->inflight_bundles, hid);
   if (!SUCCESS(rc)) {
      goto cleanup_inflight_bundles;
   }

   node_init(new_index,
             node_height(index),
             pivots,
             pivot_bundles,
             node_num_old_bundles(index),
             inflight_bundles);

   return rc;

cleanup_inflight_bundles:
   VECTOR_APPLY_TO_PTRS(&inflight_bundles, bundle_deinit);
   vector_deinit(&inflight_bundles);
cleanup_pivot_bundles:
   VECTOR_APPLY_TO_PTRS(&pivot_bundles, bundle_deinit);
   vector_deinit(&pivot_bundles);
cleanup_pivots:
   VECTOR_APPLY_TO_ELTS(&pivots, pivot_destroy, hid);
   vector_deinit(&pivots);
   return rc;
}

static platform_status
index_split(trunk_node_context *context,
            trunk_node         *index,
            trunk_node_vector  *new_indexes)
{
   debug_assert(node_is_well_formed_index(context->cfg->data_cfg, index));
   platform_status rc;
   rc = vector_append(new_indexes, *index);
   if (!SUCCESS(rc)) {
      goto cleanup_new_indexes;
   }

   uint64 num_children = node_num_children(index);
   uint64 num_nodes    = (num_children + context->cfg->target_fanout - 1)
                      / context->cfg->target_fanout;

   for (uint64 i = 0; i < num_nodes; i++) {
      rc = VECTOR_EMPLACE_APPEND(new_indexes,
                                 index_init_split,
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
         node_deinit(vector_get_ptr(new_indexes, i), context);
      }
      vector_truncate(new_indexes, 0);
   }

   return rc;
}

/***********************************
 * flushing
 ***********************************/

static platform_status
restore_balance_leaf(trunk_node_context *context,
                     trunk_node         *leaf,
                     trunk_node_vector  *new_leaves)
{
   platform_status rc = leaf_split(context, leaf, new_leaves);

   if (SUCCESS(rc)) {
      pivot_state_map_lock lock;
      pivot_state_map_aquire_lock(&lock,
                                  context,
                                  &context->pivot_states,
                                  node_pivot_min_key(leaf),
                                  node_height(leaf));
      pivot_compaction_state *pivot_state =
         pivot_state_map_get(context,
                             &context->pivot_states,
                             &lock,
                             node_pivot_min_key(leaf),
                             node_height(leaf));
      if (pivot_state) {
         pivot_state_map_remove(&context->pivot_states, &lock, pivot_state);
      }
      pivot_state_map_release_lock(&lock, &context->pivot_states);
   }

   return rc;
}

static platform_status
flush_then_compact(trunk_node_context *context,
                   trunk_node         *node,
                   bundle             *routed,
                   bundle_vector      *inflight,
                   uint64              inflight_start,
                   uint64              child_num,
                   trunk_node_vector  *new_nodes);

static platform_status
restore_balance_index(trunk_node_context *context,
                      trunk_node         *index,
                      trunk_node_vector  *new_indexes)
{
   platform_status rc;

   debug_assert(node_is_well_formed_index(context->cfg->data_cfg, index));

   for (uint64 i = 0; i < node_num_children(index); i++) {
      pivot *pvt = node_pivot(index, i);
      if (context->cfg->per_child_flush_threshold_kv_bytes
          < pivot_num_kv_bytes(pvt)) {
         bundle *pivot_bundle = node_pivot_bundle(index, i);

         pivot_vector new_pivots;

         { // scope for new_children
            trunk_node_vector new_children;

            { // scope for child
               // Load the node we are flushing to.
               trunk_node child;
               rc = node_deserialize(context, pivot_child_addr(pvt), &child);
               if (!SUCCESS(rc)) {
                  return rc;
               }

               vector_init(&new_children, context->hid);
               rc = flush_then_compact(context,
                                       &child,
                                       pivot_bundle,
                                       &index->inflight_bundles,
                                       pivot_inflight_bundle_start(pvt),
                                       i,
                                       &new_children);
               if (!SUCCESS(rc)) {
                  node_deinit(&child, context);
                  vector_deinit(&new_children);
                  return rc;
               }

               node_deinit(&child, context);
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
                                        pivot_key(pvt),
                                        node_height(index));
            pivot_compaction_state *pivot_state =
               pivot_state_map_get(context,
                                   &context->pivot_states,
                                   &lock,
                                   pivot_key(pvt),
                                   node_height(index));
            if (pivot_state) {
               pivot_state_map_remove(
                  &context->pivot_states, &lock, pivot_state);
            }
            pivot_state_map_release_lock(&lock, &context->pivot_states);
         }

         for (uint64 j = 0; j < vector_length(&new_pivots); j++) {
            pivot *new_pivot = vector_get(&new_pivots, j);
            pivot_set_inflight_bundle_start(
               new_pivot, vector_length(&index->inflight_bundles));
         }
         rc = vector_replace(
            &index->pivots, i, 1, &new_pivots, 0, vector_length(&new_pivots));
         if (!SUCCESS(rc)) {
            VECTOR_APPLY_TO_ELTS(&new_pivots, pivot_destroy, context->hid);
            vector_deinit(&new_pivots);
            return rc;
         }
         pivot_destroy(pvt, context->hid);
         vector_deinit(&new_pivots);

         bundle_reset(pivot_bundle);
      }
   }

   return index_split(context, index, new_indexes);
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
flush_then_compact(trunk_node_context *context,
                   trunk_node         *node,
                   bundle             *routed,
                   bundle_vector      *inflight,
                   uint64              inflight_start,
                   uint64              child_num,
                   trunk_node_vector  *new_nodes)
{
   platform_status rc;

   // Add the bundles to the node
   rc = node_receive_bundles(
      context, node, routed, inflight, inflight_start, child_num);
   if (!SUCCESS(rc)) {
      return rc;
   }

   // Perform any needed recursive flushes and node splits
   if (node_is_leaf(node)) {
      rc = restore_balance_leaf(context, node, new_nodes);
   } else {
      rc = restore_balance_index(context, node, new_nodes);
   }

   return rc;
}

static platform_status
build_new_roots(trunk_node_context *context, trunk_node_vector *nodes)
{
   platform_status rc;

   debug_assert(1 < vector_length(nodes));

   // Remember the height now, since we will lose ownership of the children
   // when we enqueue compactions on them.
   uint64 height = node_height(vector_get_ptr(nodes, 0));

   // Serialize the children and enqueue their compactions. This will give us
   // back the pivots for the new root node.
   pivot_vector pivots;
   vector_init(&pivots, context->hid);
   rc = serialize_nodes_and_enqueue_bundle_compactions(context, nodes, &pivots);
   if (!SUCCESS(rc)) {
      goto cleanup_pivots;
   }
   // The nodes in the nodes vector were stolen by the enqueued compaction
   // tasks, so we can just truncate the vector.
   vector_truncate(nodes, 0);

   // Build a new vector of empty pivot bundles.
   bundle_vector pivot_bundles;
   vector_init(&pivot_bundles, context->hid);
   rc = vector_ensure_capacity(&pivot_bundles, vector_length(&pivots));
   if (!SUCCESS(rc)) {
      goto cleanup_pivot_bundles;
   }
   for (uint64 i = 0; i < vector_length(&pivots); i++) {
      rc = VECTOR_EMPLACE_APPEND(&pivot_bundles, bundle_init, context->hid);
      platform_assert_status_ok(rc);
   }

   // Build a new empty inflight bundle vector
   bundle_vector inflight;
   vector_init(&inflight, context->hid);

   // Build the new root
   trunk_node new_root;
   node_init(&new_root, height + 1, pivots, pivot_bundles, 0, inflight);

   // At this point, all our resources that we've allocated have been put
   // into the new root.

   rc = index_split(context, &new_root, nodes);
   if (!SUCCESS(rc)) {
      node_deinit(&new_root, context);
   }

   return rc;

cleanup_pivot_bundles:
   vector_deinit(&pivot_bundles);

cleanup_pivots:
   VECTOR_APPLY_TO_ELTS(&pivots, pivot_destroy, context->hid);
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

   bundle_vector inflight;
   vector_init(&inflight, context->hid);

   trunk_node_vector new_nodes;
   vector_init(&new_nodes, context->hid);

   // Read the old root.
   trunk_node root;
   rc = node_deserialize(context, context->root_addr, &root);
   if (!SUCCESS(rc)) {
      goto cleanup_vectors;
   }

   // Construct a vector of inflight bundles with one singleton bundle for
   // the new branch.
   rc = VECTOR_EMPLACE_APPEND(
      &inflight, bundle_init_single, context->hid, filter, branch);
   if (!SUCCESS(rc)) {
      goto cleanup_root;
   }

   // "flush" the new bundle to the root, then do any rebalancing needed.
   rc = flush_then_compact(context, &root, NULL, &inflight, 0, 0, &new_nodes);
   node_deinit(&root, context);
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

   pivot *new_root_pivot =
      node_serialize(context, vector_get_ptr(&new_nodes, 0));
   if (new_root_pivot == NULL) {
      rc = STATUS_NO_MEMORY;
      goto cleanup_vectors;
   }

   *new_root_addr = pivot_child_addr(new_root_pivot);
   pivot_destroy(new_root_pivot, context->hid);

   return STATUS_OK;

cleanup_root:
   node_deinit(&root, context);

cleanup_vectors:
   VECTOR_APPLY_TO_PTRS(&new_nodes, node_deinit, context);
   vector_deinit(&new_nodes);
   VECTOR_APPLY_TO_PTRS(&inflight, bundle_deinit);
   vector_deinit(&inflight);

   return rc;
}