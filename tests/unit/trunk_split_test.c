// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "splinterdb/default_data_config.h"
#include "splinterdb_tests_private.h"
#include "platform_threads.h"
#include "ctest.h"

typedef struct split_test_iterator {
   iterator super;
   uint64   pos;
   uint64   count;
   char     keys[80];
   char     value[100];
} split_test_iterator;

static void
split_test_curr(iterator *base, key *k, message *msg)
{
   split_test_iterator *itor = (split_test_iterator *)base;
   *k                        = key_create(FALSE, 1, &itor->keys[itor->pos]);
   *msg =
      message_create_internal(MESSAGE_TYPE_INSERT,
                              NULL,
                              slice_create(sizeof(itor->value), itor->value));
}

static bool32
split_test_can_next(iterator *base)
{
   split_test_iterator *itor = (split_test_iterator *)base;
   return itor->pos < itor->count;
}

static bool32
split_test_can_prev(iterator *base)
{
   return TRUE;
}

static platform_status
split_test_next(iterator *base)
{
   ((split_test_iterator *)base)->pos++;
   return STATUS_OK;
}

static const iterator_ops split_test_ops = {
   .curr     = split_test_curr,
   .can_next = split_test_can_next,
   .can_prev = split_test_can_prev,
   .next     = split_test_next,
};

static uint64
split_test_pack(trunk_context *context, uint64 count)
{
   split_test_iterator itor = {
      .super = {.ops = &split_test_ops},
      .count = count,
   };
   platform_assert(count <= ARRAY_SIZE(itor.keys));
   for (uint64 i = 0; i < count; i++) {
      itor.keys[i] = '!' + i;
   }
   memset(itor.value, 'v', sizeof(itor.value));
   btree_pack_req req;
   ASSERT_TRUE(SUCCESS(btree_pack_req_init(&req,
                                           context->cc,
                                           context->cfg->btree_cfg,
                                           &itor.super,
                                           count,
                                           context->hid,
                                           .seed = 42)));
   ASSERT_TRUE(SUCCESS(btree_pack(&req)));
   uint64 addr = req.root_addr;
   btree_pack_req_deinit(&req, context->hid);
   return addr;
}

static void
split_test_incorporate(trunk_context *context, uint64 branch)
{
   ASSERT_TRUE(SUCCESS(trunk_incorporate_prepare(context, branch)));
   trunk_incorporate_commit(context);
   trunk_incorporate_cleanup(context);
}

CTEST_DATA(trunk_split)
{
   splinterdb         *db;
   data_config         data_cfg;
   trunk_context      *context;
   const trunk_config *original_cfg;
   trunk_config        cfg;
};

CTEST_SETUP(trunk_split)
{
   default_data_config_init(&data->data_cfg);
   splinterdb_config cfg = {
      .filename                = "trunk_split_test.db",
      .cache_size              = 64 * 1024 * 1024,
      .disk_size               = 128 * 1024 * 1024,
      .data_cfg                = &data->data_cfg,
      .num_memtable_bg_threads = 0,
      .num_normal_bg_threads   = 0,
   };
   for (int arg = 1; arg < Ctest_argc; arg++) {
      cfg.use_shmem |= strcmp(Ctest_argv[arg], "--use-shmem") == 0;
   }
   ASSERT_EQUAL(0, splinterdb_create(&cfg, &data->db));
   core_handle *core  = (core_handle *)splinterdb_get_trunk_handle(data->db);
   data->context      = &core->trunk_context;
   data->original_cfg = data->context->cfg;
   data->cfg          = *data->original_cfg;
   data->context->cfg = &data->cfg;
}

CTEST_TEARDOWN(trunk_split)
{
   data->context->cfg = data->original_cfg;
   ASSERT_EQUAL(0, splinterdb_close(&data->db, FALSE));
   platform_deregister_thread();
}

CTEST2(trunk_split, duplicate_keys_produce_valid_leaves)
{
   // Keep the first two branches in flight, then force a split while the raw
   // merge sees three copies of each key. There are fewer distinct keys than
   // the requested number of leaves.
   for (uint64 i = 0; i < 3; i++) {
      uint64 branch = split_test_pack(data->context, 2);
      if (i == 2) {
         data->cfg.incorporation_size_kv_bytes = 150;
         data->cfg.target_fanout               = 2;
      }
      split_test_incorporate(data->context, branch);
      btree_dec_ref(
         data->context->cc, data->cfg.btree_cfg, branch, PAGE_TYPE_BRANCH);
   }
   uint64 height;
   ASSERT_TRUE(SUCCESS(trunk_get_height(data->context, &height)));
   ASSERT_EQUAL(2, height);

   // Read the resulting tree before and after queued compactions complete.
   for (uint64 pass = 0; pass < 2; pass++) {
      for (char k = '!'; k <= '"'; k++) {
         splinterdb_lookup_result result;
         splinterdb_lookup_result_init(data->db, &result);
         ASSERT_EQUAL(
            0, splinterdb_lookup(data->db, slice_create(1, &k), &result));
         ASSERT_TRUE(splinterdb_lookup_found(&result));
         slice value;
         ASSERT_EQUAL(0, splinterdb_lookup_result_value(&result, &value));
         ASSERT_EQUAL(100, slice_length(value));
         ASSERT_EQUAL('v', *(const char *)slice_data(value));
         splinterdb_lookup_result_deinit(&result);
      }
      ASSERT_TRUE(SUCCESS(task_perform_until_quiescent(data->context->ts)));
   }
}

// No background workers run in these tests, so replacing the cache vtable
// confines each injected failure to the synchronous incorporation call.
static const cache_ops *split_test_real_ops;
static uint64           split_test_alloc_calls;
static uint64           split_test_fail_alloc_at;
static uint64           split_test_fail_get_addr;
static uint64           split_test_failures;

static page_handle *
split_test_page_alloc(cache *cc, uint64 addr, page_type type, page_handle *page)
{
   if (type == PAGE_TYPE_TRUNK
       && ++split_test_alloc_calls == split_test_fail_alloc_at)
   {
      split_test_failures++;
      return NULL;
   }
   return split_test_real_ops->page_alloc(cc, addr, type, page);
}

static platform_status
split_test_page_get(cache       *cc,
                    uint64       addr,
                    bool32       blocking,
                    page_type    type,
                    page_handle *page)
{
   if (type == PAGE_TYPE_BRANCH && addr == split_test_fail_get_addr) {
      split_test_failures++;
      *page = (page_handle){0};
      return STATUS_IO_ERROR;
   }
   return split_test_real_ops->page_get(cc, addr, blocking, type, page);
}

CTEST2(trunk_split, root_growth_allocation_failure_preserves_ownership)
{
   trunk_context *context                = data->context;
   uint64         branch                 = split_test_pack(context, 80);
   data->cfg.incorporation_size_kv_bytes = 2000;
   data->cfg.target_fanout               = 2;
   data->cfg.branch_rough_count_height   = 0;
   uint64   before                       = allocator_in_use(context->al);
   refcount branch_refs = allocator_get_refcount(context->al, branch);
   split_test_real_ops  = context->cc->ops;
   cache_ops ops        = *split_test_real_ops;
   ops.page_alloc       = split_test_page_alloc;

   // Four leaf allocations precede the first two new index nodes and their
   // parent. Fail each root-growth allocation, including after partial success.
   for (uint64 fail_at = 5; fail_at <= 7; fail_at++) {
      split_test_alloc_calls   = 0;
      split_test_fail_alloc_at = fail_at;
      split_test_failures      = 0;
      context->cc->ops         = &ops;
      platform_status rc       = trunk_incorporate_prepare(context, branch);
      context->cc->ops         = split_test_real_ops;
      ASSERT_EQUAL(1, split_test_failures);
      ASSERT_EQUAL(STATUS_NO_MEMORY.r, rc.r);
      ASSERT_NULL(context->root);
      ASSERT_NULL(context->post_incorporation_root);
      ASSERT_EQUAL(before, allocator_in_use(context->al));
      ASSERT_EQUAL(branch_refs, allocator_get_refcount(context->al, branch));
   }
   split_test_incorporate(context, branch);
   btree_dec_ref(context->cc, data->cfg.btree_cfg, branch, PAGE_TYPE_BRANCH);
}

CTEST2(trunk_split, split_scan_propagates_io_error)
{
   trunk_context *context                = data->context;
   uint64         branch                 = split_test_pack(context, 80);
   data->cfg.incorporation_size_kv_bytes = 2000;
   data->cfg.target_fanout               = 2;
   data->cfg.branch_rough_count_height   = 0;

   // Find the second leaf in a valid branch. Bounds/count lookups visit the
   // endpoints; the split scan must fetch this interior page while advancing.
   btree_iterator itor;
   ASSERT_TRUE(
      SUCCESS(btree_iterator_init(context->cc,
                                  data->cfg.btree_cfg,
                                  &itor,
                                  branch,
                                  BTREE_BRANCH_FORMAT_VERSION,
                                  PAGE_TYPE_BRANCH,
                                  &(const btree_iterator_init_options){0})));
   uint64 first_page = itor.curr.addr;
   while (itor.curr.addr == first_page && iterator_can_next(&itor.super)) {
      ASSERT_TRUE(SUCCESS(iterator_next(&itor.super)));
   }
   ASSERT_TRUE(iterator_can_curr(&itor.super));
   ASSERT_NOT_EQUAL(first_page, itor.curr.addr);
   split_test_fail_get_addr = itor.curr.addr;
   btree_iterator_deinit(&itor);

   uint64 before       = allocator_in_use(context->al);
   split_test_real_ops = context->cc->ops;
   cache_ops ops       = *split_test_real_ops;
   ops.page_get        = split_test_page_get;
   split_test_failures = 0;
   context->cc->ops    = &ops;
   platform_status rc  = trunk_incorporate_prepare(context, branch);
   context->cc->ops    = split_test_real_ops;
   ASSERT_EQUAL(1, split_test_failures);
   ASSERT_EQUAL(STATUS_IO_ERROR.r, rc.r);
   ASSERT_NULL(context->root);
   ASSERT_NULL(context->post_incorporation_root);
   ASSERT_EQUAL(before, allocator_in_use(context->al));
   split_test_incorporate(context, branch);
   btree_dec_ref(context->cc, data->cfg.btree_cfg, branch, PAGE_TYPE_BRANCH);
}

// Fail a specific allocator retain before it changes the count. Every test
// restores the vtable before cleanup so the injected failure cannot mask leaks.
static const allocator_ops *split_test_real_allocator_ops;
static uint64               split_test_retain_calls;
static uint64               split_test_fail_retain_at;
static bool32               split_test_fail_limit;

static platform_status
split_test_inc_ref(allocator *al, uint64 addr, refcount *new_count)
{
   if (++split_test_retain_calls == split_test_fail_retain_at) {
      return split_test_fail_limit ? STATUS_LIMIT_EXCEEDED : STATUS_NO_MEMORY;
   }
   return split_test_real_allocator_ops->inc_ref(al, addr, new_count);
}

CTEST2(trunk_split, root_growth_retain_failure_preserves_ownership)
{
   trunk_context *context                = data->context;
   uint64         branch                 = split_test_pack(context, 80);
   data->cfg.incorporation_size_kv_bytes = 2000;
   data->cfg.target_fanout               = 2;
   data->cfg.branch_rough_count_height   = 0;
   uint64   before                       = allocator_in_use(context->al);
   refcount branch_refs          = allocator_get_refcount(context->al, branch);
   split_test_real_allocator_ops = context->al->ops;
   allocator_ops ops             = *split_test_real_allocator_ops;
   ops.inc_ref                   = split_test_inc_ref;

   // Sweep the entire acquisition sequence through multiple serialized leaves
   // and parents. The successful attempt terminates the sweep, so additions to
   // the split tree cannot silently leave later failure points untested.
   bool32 succeeded = FALSE;
   for (uint64 fail_at = 1; fail_at <= 100; fail_at++) {
      split_test_retain_calls   = 0;
      split_test_fail_retain_at = fail_at;
      context->al->ops          = &ops;
      platform_status rc        = trunk_incorporate_prepare(context, branch);
      context->al->ops          = split_test_real_allocator_ops;
      if (SUCCESS(rc)) {
         ASSERT_TRUE(fail_at > 3);
         ASSERT_EQUAL(fail_at - 1, split_test_retain_calls);
         trunk_incorporate_commit(context);
         trunk_incorporate_cleanup(context);
         succeeded = TRUE;
         break;
      }
      ASSERT_EQUAL(STATUS_NO_MEMORY.r, rc.r);
      ASSERT_EQUAL(fail_at, split_test_retain_calls);
      ASSERT_NULL(context->root);
      ASSERT_NULL(context->post_incorporation_root);
      ASSERT_EQUAL(before, allocator_in_use(context->al));
      ASSERT_EQUAL(branch_refs, allocator_get_refcount(context->al, branch));
      ASSERT_TRUE(SUCCESS(context->allocator_cleanup_status));
   }
   ASSERT_TRUE(succeeded);
   btree_dec_ref(context->cc, data->cfg.btree_cfg, branch, PAGE_TYPE_BRANCH);
}

CTEST2(trunk_split, iterator_retain_failure_preserves_caller_prefix)
{
   trunk_context *context = data->context;
   uint64         branch[3];
   refcount       counts[3];
   for (uint64 i = 0; i < ARRAY_SIZE(branch); i++) {
      branch[i] = split_test_pack(context, 2);
      split_test_incorporate(context, branch[i]);
   }
   for (uint64 i = 0; i < ARRAY_SIZE(branch); i++) {
      counts[i] = allocator_get_refcount(context->al, branch[i]);
   }
   split_test_real_allocator_ops = context->al->ops;
   allocator_ops ops             = *split_test_real_allocator_ops;
   ops.inc_ref                   = split_test_inc_ref;
   trunk_ondisk_node_handle handle;
   ASSERT_TRUE(SUCCESS(trunk_init_root_handle(context, &handle)));
   key_buffer min_key, max_key;
   key_buffer_init(&min_key, context->hid);
   key_buffer_init(&max_key, context->hid);
   for (uint64 fail_at = 1; fail_at <= 3; fail_at++) {
      trunk_branch_info branches[8]  = {{.addr = 12345}};
      uint64            num_branches = 1;
      split_test_retain_calls        = 0;
      split_test_fail_retain_at      = fail_at;
      context->al->ops               = &ops;
      platform_status rc             = trunk_collect_branches(context,
                                                  &handle,
                                                  NEGATIVE_INFINITY_KEY,
                                                  greater_than_or_equal,
                                                  NULL,
                                                  ARRAY_SIZE(branches),
                                                  &num_branches,
                                                  branches,
                                                  &min_key,
                                                  &max_key);
      context->al->ops               = split_test_real_allocator_ops;
      ASSERT_EQUAL(STATUS_NO_MEMORY.r, rc.r);
      ASSERT_EQUAL(fail_at, split_test_retain_calls);
      ASSERT_EQUAL(1, num_branches);
      ASSERT_EQUAL(12345, branches[0].addr);
      for (uint64 i = 1; i < ARRAY_SIZE(branches); i++) {
         ASSERT_EQUAL(0, branches[i].addr);
      }
      for (uint64 i = 0; i < ARRAY_SIZE(branch); i++) {
         ASSERT_EQUAL(counts[i],
                      allocator_get_refcount(context->al, branch[i]));
      }
   }
   key_buffer_deinit(&min_key);
   key_buffer_deinit(&max_key);
   trunk_ondisk_node_handle_deinit(&handle);
   for (uint64 i = 0; i < ARRAY_SIZE(branch); i++) {
      btree_dec_ref(
         context->cc, data->cfg.btree_cfg, branch[i], PAGE_TYPE_BRANCH);
   }
}

CTEST2(trunk_split, maplet_cow_retain_failure_restores_full_reference_map)
{
   trunk_context *context = data->context;
   // Completing separate compactions leaves a maplet and multiple branches in
   // the pivot bundle, exercising failure after retaining the filter and part
   // of the branch list rather than only singleton inflight bundles.
   for (uint64 i = 0; i < 2; i++) {
      uint64 old_branch = split_test_pack(context, 2);
      split_test_incorporate(context, old_branch);
      btree_dec_ref(
         context->cc, data->cfg.btree_cfg, old_branch, PAGE_TYPE_BRANCH);
      ASSERT_TRUE(SUCCESS(task_perform_until_quiescent(context->ts)));
   }
   uint64 branch      = split_test_pack(context, 2);
   uint64 old_root    = context->root->ref.addr;
   uint64 extent_size = cache_extent_size(context->cc);
   uint64 num_extents =
      allocator_get_config(context->al)->capacity / extent_size;
   refcount *counts = TYPED_ARRAY_MALLOC(context->hid, counts, num_extents);
   ASSERT_NOT_NULL(counts);
   for (uint64 i = 0; i < num_extents; i++) {
      counts[i] = allocator_get_refcount(context->al, i * extent_size);
   }
   uint64 before                 = allocator_in_use(context->al);
   split_test_real_allocator_ops = context->al->ops;
   allocator_ops ops             = *split_test_real_allocator_ops;
   ops.inc_ref                   = split_test_inc_ref;
   bool32 succeeded              = FALSE;
   for (uint64 fail_at = 1; fail_at <= 100; fail_at++) {
      split_test_fail_limit     = fail_at % 2 == 0;
      split_test_retain_calls   = 0;
      split_test_fail_retain_at = fail_at;
      context->al->ops          = &ops;
      platform_status rc        = trunk_incorporate_prepare(context, branch);
      context->al->ops          = split_test_real_allocator_ops;
      if (SUCCESS(rc)) {
         ASSERT_TRUE(fail_at > 3);
         ASSERT_EQUAL(fail_at - 1, split_test_retain_calls);
         trunk_incorporate_commit(context);
         trunk_incorporate_cleanup(context);
         succeeded = TRUE;
         break;
      }
      ASSERT_EQUAL(split_test_fail_limit ? STATUS_LIMIT_EXCEEDED.r
                                         : STATUS_NO_MEMORY.r,
                   rc.r);
      ASSERT_EQUAL(fail_at, split_test_retain_calls);
      ASSERT_EQUAL(old_root, context->root->ref.addr);
      ASSERT_NULL(context->post_incorporation_root);
      ASSERT_EQUAL(before, allocator_in_use(context->al));
      for (uint64 i = 0; i < num_extents; i++) {
         ASSERT_EQUAL(counts[i],
                      allocator_get_refcount(context->al, i * extent_size));
      }
   }
   split_test_fail_limit = FALSE;
   ASSERT_TRUE(succeeded);
   platform_free(context->hid, counts);
   btree_dec_ref(context->cc, data->cfg.btree_cfg, branch, PAGE_TYPE_BRANCH);
}
