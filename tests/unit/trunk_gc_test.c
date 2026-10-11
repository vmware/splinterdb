// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "trunk.h"
#include "splinterdb/default_data_config.h"
#include "platform_threads.h"
#include "ctest.h"

#define GC_PAGE_SIZE   4096
#define GC_EXTENT_SIZE (32 * GC_PAGE_SIZE)
#define GC_NUM_NODES   3

// Internal mutation scopes drain the deferred-GC queue on exit.
extern void
trunk_modification_begin(trunk_context *context);
extern void
trunk_modification_end(trunk_context *context);

// Minimal, valid trunk images: an index with one child, its empty leaf, and
// an independent leaf. No branches or filters are needed for this GC walk.
typedef struct ONDISK gc_node_header {
   uint16 height;
   uint16 num_pivots;
   uint16 num_inflight_bundles;
   uint32 inflight_bundles_offset;
   uint32 pivot_offsets[2];
} gc_node_header;

typedef struct ONDISK gc_pivot {
   int64      num_kv_bytes;
   int64      num_tuples;
   uint64     child_addr;
   uint64     num_live_inflight_bundles;
   ondisk_key key;
} gc_pivot;

typedef struct ONDISK gc_bundle {
   routing_filter maplet;
   uint16         num_branches;
} gc_bundle;

typedef struct gc_allocator {
   allocator        super;
   allocator_config cfg;
   refcount         refs[GC_NUM_NODES];
} gc_allocator;

typedef struct gc_cache {
   cache          super;
   gc_allocator   al;
   trunk_context *context;
   page_handle    pages[GC_NUM_NODES];
   uint64         words[GC_NUM_NODES][GC_PAGE_SIZE / sizeof(uint64)];
   uint64         readers[GC_NUM_NODES];
   bool32         busy[GC_NUM_NODES];
   bool32         discarded[GC_NUM_NODES];
} gc_cache;

static uint64
node_index(uint64 addr)
{
   platform_assert(addr % GC_EXTENT_SIZE == 0);
   uint64 index = addr / GC_EXTENT_SIZE - 1;
   platform_assert(index < GC_NUM_NODES);
   return index;
}

static uint64
gc_page_size(const cache_config *cfg)
{
   return GC_PAGE_SIZE;
}

static uint64
gc_extent_size(const cache_config *cfg)
{
   return GC_EXTENT_SIZE;
}

static const cache_config_ops gc_config_ops = {
   .page_size   = gc_page_size,
   .extent_size = gc_extent_size,
};
static cache_config gc_config = {.ops = &gc_config_ops};

static cache_config *
gc_get_config(const cache *cc)
{
   return &gc_config;
}

static allocator_config *
gc_allocator_get_config(allocator *al)
{
   return &((gc_allocator *)al)->cfg;
}

static allocator *
gc_get_allocator(const cache *cc)
{
   return &((gc_cache *)cc)->al.super;
}

static refcount
gc_dec_ref(allocator *al, uint64 addr, page_type type)
{
   gc_allocator *test_al = (gc_allocator *)al;
   uint64        index   = node_index(addr);
   platform_assert(type == PAGE_TYPE_TRUNK && test_al->refs[index] > AL_FREE);
   return --test_al->refs[index];
}

static platform_status
gc_get(cache       *cc,
       uint64       addr,
       bool32       blocking,
       page_type    type,
       page_handle *page)
{
   gc_cache *test_cc = (gc_cache *)cc;
   uint64    index   = node_index(addr);
   // Fail promptly if reclamation still holds the queue lock, rather than
   // hanging when it subsequently tries to enqueue the busy child.
   platform_assert(test_cc->context->pending_gcs_lock == 0);
   platform_assert(type == PAGE_TYPE_TRUNK && !test_cc->discarded[index]);
   test_cc->readers[index]++;
   *page = test_cc->pages[index];
   return STATUS_OK;
}

static void
gc_unget(cache *cc, page_handle *page)
{
   gc_cache *test_cc = (gc_cache *)cc;
   uint64    index   = node_index(page->disk_addr);
   platform_assert(test_cc->readers[index] > 0);
   test_cc->readers[index]--;
}

static bool32
gc_in_use(cache *cc, uint64 addr)
{
   gc_cache *test_cc = (gc_cache *)cc;
   uint64    index   = node_index(addr);
   return test_cc->busy[index] || test_cc->readers[index] != 0;
}

static void
gc_discard(cache *cc, uint64 addr, page_type type)
{
   gc_cache *test_cc = (gc_cache *)cc;
   uint64    index   = node_index(addr);
   platform_assert(type == PAGE_TYPE_TRUNK && !gc_in_use(cc, addr));
   platform_assert(!test_cc->discarded[index]);
   platform_assert(test_cc->al.refs[index] == AL_NO_REFS);
   test_cc->discarded[index] = TRUE;
}

static const allocator_ops gc_allocator_ops = {
   .get_config = gc_allocator_get_config,
   .dec_ref    = gc_dec_ref,
};
static const cache_ops gc_cache_ops = {
   .get_config     = gc_get_config,
   .get_allocator  = gc_get_allocator,
   .page_get       = gc_get,
   .page_unget     = gc_unget,
   .in_use         = gc_in_use,
   .extent_discard = gc_discard,
};

static void
gc_init_node(gc_cache *cc, uint64 index, uint64 child_addr)
{
   page_handle *page        = &cc->pages[index];
   page->data               = (char *)cc->words[index];
   page->disk_addr          = (index + 1) * GC_EXTENT_SIZE;
   gc_node_header *header   = (gc_node_header *)page->data;
   header->height           = child_addr != 0;
   header->num_pivots       = 2;
   header->pivot_offsets[0] = sizeof(*header);
   header->pivot_offsets[1] =
      sizeof(*header) + sizeof(gc_pivot) + sizeof(gc_bundle);
   gc_pivot *lo   = (gc_pivot *)(page->data + header->pivot_offsets[0]);
   gc_pivot *hi   = (gc_pivot *)(page->data + header->pivot_offsets[1]);
   lo->child_addr = child_addr;
   copy_key_to_ondisk_key(&lo->key, NEGATIVE_INFINITY_KEY);
   copy_key_to_ondisk_key(&hi->key, POSITIVE_INFINITY_KEY);
   cc->al.refs[index] = AL_ONE_REF;
}

CTEST(trunk_gc, deferred_parent_can_enqueue_busy_child)
{
   platform_register_thread();
   data_config dc;
   default_data_config_init(&dc);
   trunk_config cfg = {.data_cfg = &dc};
   gc_cache     cc  = {
           .super = {.ops = &gc_cache_ops},
           .al =
         {
                 .super = {.ops = &gc_allocator_ops},
                 .cfg   = {.capacity = (GC_NUM_NODES + 1) * GC_EXTENT_SIZE},
         },
   };
   trunk_context context = {.cfg = &cfg, .cc = &cc.super, .al = &cc.al.super};
   cc.context            = &context;
   batch_rwlock_init(&context.root_lock);
   gc_init_node(&cc, 0, 2 * GC_EXTENT_SIZE);
   gc_init_node(&cc, 1, 0);
   gc_init_node(&cc, 2, 0);

   // Queue the parent and an independent root while they are still pinned.
   cc.busy[0] = cc.busy[2] = TRUE;
   trunk_snapshot parent   = {.root_addr = GC_EXTENT_SIZE};
   trunk_snapshot other    = {.root_addr = 3 * GC_EXTENT_SIZE};
   ASSERT_TRUE(SUCCESS(trunk_snapshot_release(&context, &parent)));
   ASSERT_TRUE(SUCCESS(trunk_snapshot_release(&context, &other)));
   ASSERT_TRUE(context.pending_gcs != NULL);

   // A cache eviction can briefly pin a child even after tree readers leave.
   // Keep it busy across the first drain so recursive GC must enqueue it.
   cc.busy[0] = cc.busy[2] = FALSE;
   cc.busy[1]              = TRUE;
   trunk_modification_begin(&context);
   trunk_modification_end(&context);
   ASSERT_EQUAL(AL_FREE, cc.al.refs[0]);
   ASSERT_EQUAL(AL_NO_REFS, cc.al.refs[1]);
   ASSERT_EQUAL(AL_FREE, cc.al.refs[2]);
   ASSERT_TRUE(context.pending_gcs != NULL);
   ASSERT_TRUE(context.pending_gcs_tail == context.pending_gcs);
   ASSERT_EQUAL(0, context.pending_gcs_lock);

   cc.busy[1] = FALSE;
   trunk_modification_begin(&context);
   trunk_modification_end(&context);
   ASSERT_TRUE(context.pending_gcs == NULL);
   ASSERT_TRUE(context.pending_gcs_tail == NULL);
   for (uint64 i = 0; i < GC_NUM_NODES; i++) {
      ASSERT_EQUAL(AL_FREE, cc.al.refs[i]);
      ASSERT_EQUAL(0, cc.readers[i]);
      ASSERT_TRUE(cc.discarded[i]);
   }
   ASSERT_TRUE(SUCCESS(context.allocator_cleanup_status));
   batch_rwlock_deinit(&context.root_lock);
   platform_deregister_thread();
}
