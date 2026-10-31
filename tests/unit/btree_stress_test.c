// Copyright 2021-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * btree_stress_test.c - Basic BTree multi-threaded stress test
 *
 * Exercises the BTree APIs, with larger data volumes, and multiple threads.
 * -----------------------------------------------------------------------------
 */
#include <string.h>
#include <fcntl.h>
#include <pthread.h>

#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

#include "functional/test.h"
#include "splinterdb/data.h"
#include "../config.h"
#include "platform_io.h"
#include "platform_threads.h"
#include "platform_units.h"
#include "rc_allocator.h"
#include "clockcache.h"
#include "btree_private.h"
#include "btree_test_common.h"

static uint64
test_btree_create(cache              *cc,
                  const btree_config *cfg,
                  mini_allocator     *mini,
                  page_type           type)
{
   uint64          root_addr;
   platform_status rc = btree_create(cc, cfg, mini, type, &root_addr);
   platform_assert_status_ok(rc);
   return root_addr;
}

typedef struct insert_thread_params {
   cache           *cc;
   btree_config    *cfg;
   platform_heap_id hid;
   btree_scratch   *scratch;
   mini_allocator  *mini;
   uint64           root_addr;
   int              start;
   int              end;
} insert_thread_params;

// Function Prototypes
static void
insert_thread(void *arg);

static void
insert_tests(cache           *cc,
             btree_config    *cfg,
             platform_heap_id hid,
             btree_scratch   *scratch,
             mini_allocator  *mini,
             uint64           root_addr,
             int              start,
             int              end);

static int
query_tests(cache           *cc,
            btree_config    *cfg,
            platform_heap_id hid,
            page_type        type,
            uint64           root_addr,
            int              nkvs);

static int
iterator_tests(cache           *cc,
               btree_config    *cfg,
               uint64           root_addr,
               page_type        type,
               int              nkvs,
               bool32           start_front,
               platform_heap_id hid);

static int
iterator_seek_tests(cache           *cc,
                    btree_config    *cfg,
                    uint64           root_addr,
                    int              nkvs,
                    platform_heap_id hid);

static uint64
pack_tests(cache           *cc,
           btree_config    *cfg,
           platform_heap_id hid,
           uint64           root_addr,
           uint64           nkvs);

static key
gen_key(btree_config *cfg, uint64 i, uint8 *buffer, size_t length);

static uint64
ungen_key(key test_key);

static message
gen_msg(btree_config *cfg, uint64 i, uint8 *buffer, size_t length);

/*
 * Global data declaration macro:
 */
CTEST_DATA(btree_stress)
{
   // This part of the data structures is common to what we need
   // to set up a Splinter instance, as is done in
   // btree_test.c
   master_config      master_cfg;
   data_config       *data_cfg;
   io_config          io_cfg;
   allocator_config   allocator_cfg;
   clockcache_config  cache_cfg;
   task_system_config task_cfg;
   btree_scratch     *test_scratch;
   btree_config       dbtree_cfg;

   // To create a heap for io, allocator, cache and splinter
   platform_heap_id hid;

   // Stuff needed to setup and exercise multiple threads.
   io_handle   *io;
   task_system  ts;
   rc_allocator al;
   clockcache   cc;
};

// Setup function for suite, called before every test in suite
CTEST_SETUP(btree_stress)
{
   platform_register_thread();
   config_set_defaults(&data->master_cfg);
   data->master_cfg.cache_capacity = GiB_TO_B(5);
   data->data_cfg                  = test_data_config;

   if (!SUCCESS(
          config_parse(&data->master_cfg, 1, Ctest_argc, (char **)Ctest_argv))
       || !init_data_config_from_master_config(data->data_cfg,
                                               &data->master_cfg)
       || !init_io_config_from_master_config(&data->io_cfg, &data->master_cfg)
       || !init_rc_allocator_config_from_master_config(
          &data->allocator_cfg, &data->master_cfg, &data->io_cfg)
       || !init_clockcache_config_from_master_config(
          &data->cache_cfg, &data->master_cfg, &data->io_cfg)
       || !init_btree_config_from_master_config(&data->dbtree_cfg,
                                                &data->master_cfg,
                                                &data->cache_cfg.super,
                                                data->data_cfg)
       || !init_task_config_from_master_config(&data->task_cfg,
                                               &data->master_cfg))
   {
      ASSERT_TRUE(FALSE, "Failed to parse args\n");
   }

   // Create a heap for io, allocator, cache and splinter
   if (!SUCCESS(platform_heap_create(platform_get_module_id(),
                                     512 * MiB,
                                     data->master_cfg.use_shmem,
                                     &data->hid)))
   {
      ASSERT_TRUE(FALSE, "Failed to init heap\n");
   }

   data->test_scratch = TYPED_MANUAL_ZALLOC(
      data->hid, data->test_scratch, btree_scratch_size(&data->dbtree_cfg));
   ASSERT_NOT_NULL(data->test_scratch);

   // Setup execution of concurrent threads
   data->io = io_handle_create(&data->io_cfg, data->hid);
   if (data->io == NULL
       || !SUCCESS(task_system_init(&data->ts, data->hid, &data->task_cfg))
       || !SUCCESS(rc_allocator_init(&data->al,
                                     &data->allocator_cfg,
                                     data->io,
                                     data->hid,
                                     platform_get_module_id()))
       || !SUCCESS(clockcache_init(&data->cc,
                                   &data->cache_cfg,
                                   data->io,
                                   (allocator *)&data->al,
                                   "test",
                                   data->hid,
                                   platform_get_module_id())))
   {
      ASSERT_TRUE(FALSE,
                  "Failed to create io or init task system or rc_allocator or "
                  "clockcache\n");
   }
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(btree_stress)
{
   clockcache_deinit(&data->cc);
   rc_allocator_deinit(&data->al);
   task_system_deinit(&data->ts);
   io_handle_destroy(data->io);
   platform_heap_destroy(&data->hid);
   platform_deregister_thread();
}

CTEST2(btree_stress, iterator_basics)
{
   uint8          keybuf[1024];
   uint8          msgbuf[1024];
   mini_allocator mini;

   uint64 root_addr = test_btree_create(
      (cache *)&data->cc, &data->dbtree_cfg, &mini, PAGE_TYPE_MEMTABLE);

   for (int i = 0; i < 1000; i++) {
      iterator_tests((cache *)&data->cc,
                     &data->dbtree_cfg,
                     root_addr,
                     PAGE_TYPE_MEMTABLE,
                     i,
                     TRUE,
                     data->hid);
      iterator_tests((cache *)&data->cc,
                     &data->dbtree_cfg,
                     root_addr,
                     PAGE_TYPE_MEMTABLE,
                     i,
                     FALSE,
                     data->hid);

      btree_insert_results results;
      btree_insert_results_init(&results, NULL);
      platform_status rc =
         btree_insert((cache *)&data->cc,
                      &data->dbtree_cfg,
                      data->hid,
                      data->test_scratch,
                      root_addr,
                      &mini,
                      gen_key(&data->dbtree_cfg, i, keybuf, sizeof(keybuf)),
                      gen_msg(&data->dbtree_cfg, i, msgbuf, sizeof(msgbuf)),
                      &results);
      btree_insert_results_deinit(&results);
      if (!SUCCESS(rc)) {
         ASSERT_TRUE(FALSE, "Failed to insert 4-byte %d\n", i);
      }
   }
}

typedef struct failing_insert_callback_context {
   uint64 calls;
} failing_insert_callback_context;

static platform_status
failing_insert_callback(void *arg)
{
   failing_insert_callback_context *ctxt = arg;
   ctxt->calls++;
   return STATUS_LIMIT_EXCEEDED;
}

CTEST2(btree_stress, insert_callback_failure_abandons_insert)
{
   uint8          keybuf[1024];
   uint8          msgbuf[1024];
   uint8          replacement_buf[1024];
   mini_allocator mini;
   uint64         root_addr = test_btree_create(
      (cache *)&data->cc, &data->dbtree_cfg, &mini, PAGE_TYPE_MEMTABLE);
   key     tuple_key = gen_key(&data->dbtree_cfg, 1, keybuf, sizeof(keybuf));
   message original  = gen_msg(&data->dbtree_cfg, 1, msgbuf, sizeof(msgbuf));
   message replacement =
      gen_msg(&data->dbtree_cfg, 2, replacement_buf, sizeof(replacement_buf));

   btree_insert_results results;
   btree_insert_results_init(&results, NULL);
   platform_status rc = btree_insert((cache *)&data->cc,
                                     &data->dbtree_cfg,
                                     data->hid,
                                     data->test_scratch,
                                     root_addr,
                                     &mini,
                                     tuple_key,
                                     original,
                                     &results);
   btree_insert_results_deinit(&results);
   ASSERT_TRUE(SUCCESS(rc));

   failing_insert_callback_context callback_ctxt = {0};
   btree_insert_results_init(&results, NULL);
   btree_insert_results_set_callback(
      &results, failing_insert_callback, &callback_ctxt);
   rc = btree_insert((cache *)&data->cc,
                     &data->dbtree_cfg,
                     data->hid,
                     data->test_scratch,
                     root_addr,
                     &mini,
                     tuple_key,
                     replacement,
                     &results);
   btree_insert_results_deinit(&results);
   ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_LIMIT_EXCEEDED));
   ASSERT_EQUAL_U(1, callback_ctxt.calls);

   lookup_result lookup;
   lookup_result_init(
      &lookup, data->dbtree_cfg.data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   rc = btree_lookup((cache *)&data->cc,
                     &data->dbtree_cfg,
                     root_addr,
                     (btree_format){0},
                     PAGE_TYPE_MEMTABLE,
                     tuple_key,
                     &lookup);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_TRUE(lookup_result_found(&lookup));
   ASSERT_EQUAL(0,
                message_lex_cmp(merge_accumulator_to_message(
                                   lookup_result_accumulator(&lookup)),
                                original));
   lookup_result_deinit(&lookup);
}

CTEST2(btree_stress, insert_callback_failure_after_root_growth)
{
   mini_allocator mini;
   uint64         root_addr = test_btree_create(
      (cache *)&data->cc, &data->dbtree_cfg, &mini, PAGE_TYPE_MEMTABLE);
   uint8 value[100] = {1};

   for (uint64 i = 0; i < 25; i++) {
      uint64               key_data = htobe64(i);
      btree_insert_results results;
      btree_insert_results_init(&results, NULL);
      platform_status rc = btree_insert(
         (cache *)&data->cc,
         &data->dbtree_cfg,
         data->hid,
         data->test_scratch,
         root_addr,
         &mini,
         key_create(FALSE, sizeof(key_data), &key_data),
         message_create_internal(
            MESSAGE_TYPE_INSERT, NULL, slice_create(sizeof(value), value)),
         &results);
      btree_insert_results_deinit(&results);
      ASSERT_TRUE(SUCCESS(rc));
   }

   uint32 height;
   ASSERT_TRUE(SUCCESS(btree_get_root_height((cache *)&data->cc,
                                             &data->dbtree_cfg,
                                             root_addr,
                                             (btree_format){0},
                                             PAGE_TYPE_MEMTABLE,
                                             &height)));
   ASSERT_EQUAL_U(0, height);

   uint8  large_value[1400] = {2};
   uint64 rejected_key_data = htobe64(UINT64_MAX);
   key    rejected_key =
      key_create(FALSE, sizeof(rejected_key_data), &rejected_key_data);
   failing_insert_callback_context callback_ctxt = {0};
   btree_insert_results            results;
   btree_insert_results_init(&results, NULL);
   btree_insert_results_set_callback(
      &results, failing_insert_callback, &callback_ctxt);
   platform_status rc = btree_insert(
      (cache *)&data->cc,
      &data->dbtree_cfg,
      data->hid,
      data->test_scratch,
      root_addr,
      &mini,
      rejected_key,
      message_create_internal(MESSAGE_TYPE_INSERT,
                              NULL,
                              slice_create(sizeof(large_value), large_value)),
      &results);
   btree_insert_results_deinit(&results);
   ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_LIMIT_EXCEEDED));
   ASSERT_EQUAL_U(1, callback_ctxt.calls);

   ASSERT_TRUE(SUCCESS(btree_get_root_height((cache *)&data->cc,
                                             &data->dbtree_cfg,
                                             root_addr,
                                             (btree_format){0},
                                             PAGE_TYPE_MEMTABLE,
                                             &height)));
   ASSERT_TRUE(height > 0);
   ASSERT_TRUE(btree_verify_tree(
      (cache *)&data->cc, &data->dbtree_cfg, root_addr, PAGE_TYPE_MEMTABLE));

   lookup_result lookup;
   lookup_result_init(
      &lookup, data->dbtree_cfg.data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   rc = btree_lookup((cache *)&data->cc,
                     &data->dbtree_cfg,
                     root_addr,
                     (btree_format){0},
                     PAGE_TYPE_MEMTABLE,
                     rejected_key,
                     &lookup);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_FALSE(lookup_result_found(&lookup));
   lookup_result_deinit(&lookup);
}

typedef struct {
   cache_ops        super;
   const cache_ops *original;
   uint64           leaf_addr;
   uint64           rejections;
} retry_leaf_claim_ops;

static bool32
retry_leaf_claim_once(cache *cc, page_handle *page)
{
   retry_leaf_claim_ops *ops = (retry_leaf_claim_ops *)cc->ops;
   if (page_handle_addr(page) == ops->leaf_addr && ops->rejections == 0) {
      ops->rejections++;
      return FALSE;
   }
   return ops->original->page_try_claim(cc, page);
}

typedef struct {
   data_config super;
   uint64      calls;
} rebuild_failure_data_config;

static int
fail_rebuilt_merge(const data_config *cfg,
                   slice              key,
                   message            older,
                   merge_accumulator *newer)
{
   rebuild_failure_data_config *test_cfg = (rebuild_failure_data_config *)cfg;
   merge_accumulator_set_class(newer, MESSAGE_TYPE_INSERT);
   return ++test_cfg->calls == 2 ? -1 : 0;
}

CTEST2(btree_stress, insert_rebuild_failure_releases_parent_claim)
{
   cache         *cc  = (cache *)&data->cc;
   btree_config  *cfg = &data->dbtree_cfg;
   mini_allocator mini;
   uint64 root_addr  = test_btree_create(cc, cfg, &mini, PAGE_TYPE_MEMTABLE);
   uint8  value[100] = {1};
   btree_insert_results results;
   for (uint64 i = 0; i < 25; i++) {
      uint64 key_data = htobe64(i);
      btree_insert_results_init(&results, NULL);
      platform_status rc = btree_insert(
         cc,
         cfg,
         data->hid,
         data->test_scratch,
         root_addr,
         &mini,
         key_create(FALSE, sizeof(key_data), &key_data),
         message_create_internal(
            MESSAGE_TYPE_INSERT, NULL, slice_create(sizeof(value), value)),
         &results);
      btree_insert_results_deinit(&results);
      ASSERT_TRUE(SUCCESS(rc));
   }

   // Grow the root, then reject the pending split, leaving a full child.
   uint64  key_data          = htobe64(UINT64_MAX);
   uint8   large_value[1400] = {2};
   message replacement =
      message_create_internal(MESSAGE_TYPE_INSERT,
                              NULL,
                              slice_create(sizeof(large_value), large_value));
   failing_insert_callback_context callback_ctxt = {0};
   btree_insert_results_init(&results, NULL);
   btree_insert_results_set_callback(
      &results, failing_insert_callback, &callback_ctxt);
   platform_status rc =
      btree_insert(cc,
                   cfg,
                   data->hid,
                   data->test_scratch,
                   root_addr,
                   &mini,
                   key_create(FALSE, sizeof(key_data), &key_data),
                   replacement,
                   &results);
   btree_insert_results_deinit(&results);
   ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_LIMIT_EXCEEDED));

   page_handle root_handle =
      test_cache_get_page(cc, root_addr, TRUE, PAGE_TYPE_MEMTABLE);
   page_handle *root = &root_handle;
   btree_hdr   *hdr  = (btree_hdr *)page_handle_data(root);
   ASSERT_EQUAL_U(1, hdr->height);
   ASSERT_EQUAL_U(1, hdr->num_entries);
   uint64 leaf_addr = btree_get_child_addr(cfg, hdr, 0);
   cache_unget(cc, root);

   // Model a lost child claim without timing dependence. Rebuilding the
   // update after that retry must release BOTH claims when the merge fails.
   retry_leaf_claim_ops ops = {
      .super = *cc->ops, .original = cc->ops, .leaf_addr = leaf_addr};
   ops.super.page_try_claim              = retry_leaf_claim_once;
   rebuild_failure_data_config merge_cfg = {.super = *cfg->data_cfg};
   merge_cfg.super.merge_tuples          = fail_rebuilt_merge;
   data_config *original_data_cfg        = cfg->data_cfg;
   cfg->data_cfg                         = &merge_cfg.super;
   cc->ops                               = &ops.super;
   key_data                              = htobe64(0);
   key target = key_create(FALSE, sizeof(key_data), &key_data);
   btree_insert_results_init(&results, NULL);
   rc = btree_insert(cc,
                     cfg,
                     data->hid,
                     data->test_scratch,
                     root_addr,
                     &mini,
                     target,
                     message_create_internal(
                        MESSAGE_TYPE_UPDATE, NULL, message_slice(replacement)),
                     &results);
   btree_insert_results_deinit(&results);
   cc->ops       = ops.original;
   cfg->data_cfg = original_data_cfg;
   ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_NO_MEMORY));
   ASSERT_EQUAL_U(1, ops.rejections);
   ASSERT_EQUAL_U(2, merge_cfg.calls);

   // Verify immediate ownership release instead of waiting for another
   // insert to hang if a claim was leaked.
   uint64 addrs[] = {root_addr, leaf_addr};
   for (uint64 i = 0; i < ARRAY_SIZE(addrs); i++) {
      page_handle acquired_page =
         test_cache_get_page(cc, addrs[i], TRUE, PAGE_TYPE_MEMTABLE);
      page_handle *page    = &acquired_page;
      bool32       claimed = cache_try_claim(cc, page);
      if (claimed) {
         cache_unclaim(cc, page);
      }
      cache_unget(cc, page);
      ASSERT_TRUE(claimed);
   }

   lookup_result lookup;
   lookup_result_init(&lookup, cfg->data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   ASSERT_TRUE(SUCCESS(btree_lookup(cc,
                                    cfg,
                                    root_addr,
                                    (btree_format){0},
                                    PAGE_TYPE_MEMTABLE,
                                    target,
                                    &lookup)));
   ASSERT_EQUAL(0,
                slice_lex_cmp(merge_accumulator_to_value(
                                 lookup_result_accumulator(&lookup)),
                              slice_create(sizeof(value), value)));
   lookup_result_deinit(&lookup);

   btree_insert_results_init(&results, NULL);
   rc = btree_insert(cc,
                     cfg,
                     data->hid,
                     data->test_scratch,
                     root_addr,
                     &mini,
                     target,
                     replacement,
                     &results);
   btree_insert_results_deinit(&results);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_TRUE(btree_verify_tree(cc, cfg, root_addr, PAGE_TYPE_MEMTABLE));
}

/*
 * -------------------------------------------------------------------------
 * Test case to exercise random inserts of large volumes of data, across
 * multiple threads. This test case verifies that registration of threads
 * to Splinter is working stably.
 */

CTEST2(btree_stress, test_random_inserts_concurrent)
{
   int nkvs     = 1000000;
   int nthreads = 8;

   mini_allocator mini;

   uint64 root_addr = test_btree_create(
      (cache *)&data->cc, &data->dbtree_cfg, &mini, PAGE_TYPE_MEMTABLE);

   platform_heap_id      hid     = data->hid;
   insert_thread_params *params  = TYPED_ARRAY_ZALLOC(hid, params, nthreads);
   platform_thread      *threads = TYPED_ARRAY_ZALLOC(hid, threads, nthreads);

   uint64 bt_sc_sz = btree_scratch_size(&data->dbtree_cfg);
   for (uint64 i = 0; i < nthreads; i++) {
      params[i].cc  = (cache *)&data->cc;
      params[i].cfg = &data->dbtree_cfg;
      params[i].hid = data->hid;
      params[i].scratch =
         TYPED_MANUAL_MALLOC(data->hid, params[i].scratch, bt_sc_sz);
      params[i].mini      = &mini;
      params[i].root_addr = root_addr;
      params[i].start     = i * (nkvs / nthreads);
      params[i].end = i < nthreads - 1 ? (i + 1) * (nkvs / nthreads) : nkvs;
   }

   for (uint64 i = 0; i < nthreads; i++) {
      platform_status ret = platform_thread_create(
         &threads[i], FALSE, insert_thread, &params[i], data->hid);
      ASSERT_TRUE(SUCCESS(ret));
      // insert_tests((cache *)&cc, &dbtree_cfg, &test_scratch, &mini,
      // root_addr, 0, nkvs);
   }

   for (uint64 thread_no = 0; thread_no < nthreads; thread_no++) {
      platform_thread_join(&threads[thread_no]);
   }

   int rc = query_tests((cache *)&data->cc,
                        &data->dbtree_cfg,
                        data->hid,
                        PAGE_TYPE_MEMTABLE,
                        root_addr,
                        nkvs);
   ASSERT_NOT_EQUAL(0, rc, "Invalid tree\n");

   if (!iterator_tests((cache *)&data->cc,
                       &data->dbtree_cfg,
                       root_addr,
                       PAGE_TYPE_MEMTABLE,
                       nkvs,
                       TRUE,
                       data->hid))
   {
      CTEST_ERR("invalid ranges in original tree, starting at front\n");
   }
   if (!iterator_tests((cache *)&data->cc,
                       &data->dbtree_cfg,
                       root_addr,
                       PAGE_TYPE_MEMTABLE,
                       nkvs,
                       FALSE,
                       data->hid))
   {
      CTEST_ERR("invalid ranges in original tree, starting at back\n");
   }

   if (!iterator_seek_tests(
          (cache *)&data->cc, &data->dbtree_cfg, root_addr, nkvs, data->hid))
   {
      CTEST_ERR("invalid ranges when seeking in original tree\n");
   }

   uint64 packed_root_addr = pack_tests(
      (cache *)&data->cc, &data->dbtree_cfg, data->hid, root_addr, nkvs);
   if (0 < nkvs && !packed_root_addr) {
      ASSERT_TRUE(FALSE, "Pack failed.\n");
   }

   rc = query_tests((cache *)&data->cc,
                    &data->dbtree_cfg,
                    data->hid,
                    PAGE_TYPE_BRANCH,
                    packed_root_addr,
                    nkvs);
   ASSERT_NOT_EQUAL(0, rc, "Invalid tree\n");

   rc = iterator_tests((cache *)&data->cc,
                       &data->dbtree_cfg,
                       packed_root_addr,
                       PAGE_TYPE_BRANCH,
                       nkvs,
                       TRUE,
                       data->hid);
   ASSERT_NOT_EQUAL(0, rc, "Invalid ranges in packed tree\n");

   // Exercise print method to verify that it basically continues to work.
   set_log_streams_for_tests(MSG_LEVEL_DEBUG);

   btree_print_tree(Platform_default_log_handle,
                    (cache *)&data->cc,
                    &data->dbtree_cfg,
                    packed_root_addr,
                    PAGE_TYPE_BRANCH);

   set_log_streams_for_tests(MSG_LEVEL_INFO);

   // Release memory allocated in this test case
   for (uint64 i = 0; i < nthreads; i++) {
      platform_free(data->hid, params[i].scratch);
   }
   platform_free(hid, params);
   platform_free(hid, threads);
}

CTEST2(btree_stress, overwrite_returns_old_value_after_tree_growth)
{
   btree_scratch *scratch = TYPED_MANUAL_ZALLOC(
      data->hid, scratch, btree_scratch_size(&data->dbtree_cfg));
   ASSERT_NOT_NULL(scratch);

   mini_allocator mini;
   uint64         root_addr = test_btree_create(
      (cache *)&data->cc, &data->dbtree_cfg, &mini, PAGE_TYPE_MEMTABLE);

   uint64 bt_page_size = btree_page_size(&data->dbtree_cfg);
   uint8 *keybuf       = TYPED_MANUAL_MALLOC(data->hid, keybuf, bt_page_size);
   uint8 *msgbuf       = TYPED_MANUAL_MALLOC(data->hid, msgbuf, bt_page_size);
   ASSERT_NOT_NULL(keybuf);
   ASSERT_NOT_NULL(msgbuf);

   lookup_result uniqueness_result;
   lookup_result_init(&uniqueness_result,
                      data->dbtree_cfg.data_cfg,
                      SPLINTERDB_LOOKUP_MIGHT_EXIST,
                      0,
                      NULL);

   for (uint64 i = 0; i < 2048; i++) {
      btree_insert_results results;
      btree_insert_results_init(&results, &uniqueness_result);

      platform_status rc =
         btree_insert((cache *)&data->cc,
                      &data->dbtree_cfg,
                      data->hid,
                      scratch,
                      root_addr,
                      &mini,
                      gen_key(&data->dbtree_cfg, i, keybuf, bt_page_size),
                      gen_msg(&data->dbtree_cfg, i, msgbuf, bt_page_size),
                      &results);
      btree_insert_results_deinit(&results);
      ASSERT_TRUE(SUCCESS(rc));
      ASSERT_FALSE(lookup_result_found(&uniqueness_result));
   }

   lookup_result old_result;
   lookup_result verify_result;
   lookup_result_init(
      &old_result, data->dbtree_cfg.data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   lookup_result_init(&verify_result,
                      data->dbtree_cfg.data_cfg,
                      SPLINTERDB_LOOKUP_VALUE,
                      0,
                      NULL);

   merge_accumulator update_msg;
   merge_accumulator expected_new_msg;
   merge_accumulator probe_msg;
   merge_accumulator_init(&update_msg, PROCESS_PRIVATE_HEAP_ID);
   merge_accumulator_init(&expected_new_msg, PROCESS_PRIVATE_HEAP_ID);
   merge_accumulator_init(&probe_msg, PROCESS_PRIVATE_HEAP_ID);

   uint8  merged_ref_count = 2;
   uint64 merged_msg_len   = 0;
   for (uint8 ref_count = 2; ref_count < 127; ref_count++) {
      test_data_generate_message(
         data->dbtree_cfg.data_cfg, MESSAGE_TYPE_INSERT, ref_count, &probe_msg);
      if (merged_msg_len < merge_accumulator_length(&probe_msg)) {
         merged_msg_len   = merge_accumulator_length(&probe_msg);
         merged_ref_count = ref_count;
      }
   }
   ASSERT_TRUE(merged_msg_len > sizeof(data_handle));

   test_data_generate_message(data->dbtree_cfg.data_cfg,
                              MESSAGE_TYPE_UPDATE,
                              merged_ref_count - 1,
                              &update_msg);
   test_data_generate_message(data->dbtree_cfg.data_cfg,
                              MESSAGE_TYPE_INSERT,
                              merged_ref_count,
                              &expected_new_msg);

   for (uint64 i = 0; i < 2048; i += 257) {
      btree_insert_results results;
      btree_insert_results_init(&results, &old_result);

      platform_status rc =
         btree_insert((cache *)&data->cc,
                      &data->dbtree_cfg,
                      data->hid,
                      scratch,
                      root_addr,
                      &mini,
                      gen_key(&data->dbtree_cfg, i, keybuf, bt_page_size),
                      merge_accumulator_to_message(&update_msg),
                      &results);
      btree_insert_results_deinit(&results);
      ASSERT_TRUE(SUCCESS(rc));
      ASSERT_TRUE(lookup_result_found(&old_result));
      ASSERT_EQUAL(
         0,
         message_lex_cmp(merge_accumulator_to_message(
                            lookup_result_accumulator(&old_result)),
                         gen_msg(&data->dbtree_cfg, i, msgbuf, bt_page_size)));

      rc = btree_lookup((cache *)&data->cc,
                        &data->dbtree_cfg,
                        root_addr,
                        (btree_format){0},
                        PAGE_TYPE_MEMTABLE,
                        gen_key(&data->dbtree_cfg, i, keybuf, bt_page_size),
                        &verify_result);
      ASSERT_TRUE(SUCCESS(rc));
      ASSERT_TRUE(lookup_result_found(&verify_result));
      ASSERT_EQUAL(
         0,
         message_lex_cmp(merge_accumulator_to_message(
                            lookup_result_accumulator(&verify_result)),
                         merge_accumulator_to_message(&expected_new_msg)));
   }

   merge_accumulator_deinit(&probe_msg);
   merge_accumulator_deinit(&expected_new_msg);
   merge_accumulator_deinit(&update_msg);
   lookup_result_deinit(&verify_result);
   lookup_result_deinit(&old_result);
   lookup_result_deinit(&uniqueness_result);
   platform_free(data->hid, msgbuf);
   platform_free(data->hid, keybuf);
   platform_free(data->hid, scratch);
}

/*
 * ********************************************************************************
 * Define minions and helper functions used by this test suite.
 * ********************************************************************************
 */
static void
insert_thread(void *arg)
{
   insert_thread_params *params = (insert_thread_params *)arg;
   insert_tests(params->cc,
                params->cfg,
                params->hid,
                params->scratch,
                params->mini,
                params->root_addr,
                params->start,
                params->end);
}

static void
insert_tests(cache           *cc,
             btree_config    *cfg,
             platform_heap_id hid,
             btree_scratch   *scratch,
             mini_allocator  *mini,
             uint64           root_addr,
             int              start,
             int              end)
{
   uint64 bt_page_size = btree_page_size(cfg);
   int    keybuf_size  = bt_page_size;
   int    msgbuf_size  = bt_page_size;
   uint8 *keybuf       = TYPED_MANUAL_MALLOC(hid, keybuf, keybuf_size);
   uint8 *msgbuf       = TYPED_MANUAL_MALLOC(hid, msgbuf, msgbuf_size);

   for (uint64 i = start; i < end; i++) {
      btree_insert_results results;
      btree_insert_results_init(&results, NULL);
      platform_status rc = btree_insert(cc,
                                        cfg,
                                        hid,
                                        scratch,
                                        root_addr,
                                        mini,
                                        gen_key(cfg, i, keybuf, keybuf_size),
                                        gen_msg(cfg, i, msgbuf, msgbuf_size),
                                        &results);
      btree_insert_results_deinit(&results);
      if (!SUCCESS(rc)) {
         ASSERT_TRUE(FALSE, "Failed to insert 4-byte %ld\n", i);
      }
   }
   platform_free(hid, keybuf);
   platform_free(hid, msgbuf);
}

static key
gen_key(btree_config *cfg, uint64 i, uint8 *buffer, size_t length)
{
   uint64 keylen = sizeof(i) + (i % 100);
   platform_assert(keylen + sizeof(i) <= length);
   memset(buffer, 0, keylen);
   uint64 j = i * 23232323731ULL + 99382474567ULL;
   memcpy(buffer, &j, sizeof(j));
   return key_create(FALSE, keylen, buffer);
}

static uint64
ungen_key(key test_key)
{
   if (key_length(test_key) < sizeof(uint64)) {
      return 0;
   }

   uint64 k;
   memcpy(&k, key_data(test_key), sizeof(k));
   return (k - 99382474567ULL) * 14122572041603317147ULL;
}

static message
gen_msg(btree_config *cfg, uint64 i, uint8 *buffer, size_t length)
{
   data_handle *dh      = (data_handle *)buffer;
   uint64       datalen = sizeof(i) + (i % (btree_page_size(cfg) / 3));

   platform_assert(datalen + sizeof(i) <= length);
   dh->ref_count = 1;
   memset(dh->data, 0, datalen);
   memcpy(dh->data, &i, sizeof(i));
   return message_create_internal(
      MESSAGE_TYPE_INSERT,
      NULL,
      slice_create(sizeof(data_handle) + datalen, buffer));
}

static int
query_tests(cache           *cc,
            btree_config    *cfg,
            platform_heap_id hid,
            page_type        type,
            uint64           root_addr,
            int              nkvs)
{
   uint64 bt_page_size = btree_page_size(cfg);
   uint8 *keybuf       = TYPED_MANUAL_MALLOC(hid, keybuf, bt_page_size);
   uint8 *msgbuf       = TYPED_MANUAL_MALLOC(hid, msgbuf, bt_page_size);
   memset(msgbuf, 0, bt_page_size);

   lookup_result result;
   lookup_result_init(&result, cfg->data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   for (uint64 i = 0; i < nkvs; i++) {
      btree_lookup(cc,
                   cfg,
                   root_addr,
                   BTREE_DEFAULT_FORMAT,
                   type,
                   gen_key(cfg, i, keybuf, bt_page_size),
                   &result);
      if (!btree_found(&result)
          || message_lex_cmp(
             merge_accumulator_to_message(lookup_result_accumulator(&result)),
             gen_msg(cfg, i, msgbuf, bt_page_size)))
      {
         ASSERT_TRUE(FALSE, "Failure on lookup %lu\n", i);
      }
   }

   lookup_result_deinit(&result);
   platform_free(hid, keybuf);
   platform_free(hid, msgbuf);
   return 1;
}

static uint64
iterator_test(platform_heap_id hid,
              btree_config    *cfg,
              uint64           nkvs,
              iterator        *iter,
              bool32           forwards)
{
   uint64 seen         = 0;
   uint64 bt_page_size = btree_page_size(cfg);
   uint8 *prevbuf      = TYPED_MANUAL_MALLOC(hid, prevbuf, bt_page_size);
   key    prev         = NULL_KEY;
   uint8 *keybuf       = TYPED_MANUAL_MALLOC(hid, keybuf, bt_page_size);
   uint8 *msgbuf       = TYPED_MANUAL_MALLOC(hid, msgbuf, bt_page_size);

   while (iterator_can_curr(iter)) {
      key     curr_key;
      message msg;

      iterator_curr(iter, &curr_key, &msg);
      uint64 k = ungen_key(curr_key);
      ASSERT_TRUE(k < nkvs);

      int rc = 0;
      rc     = data_key_compare(
         cfg->data_cfg, curr_key, gen_key(cfg, k, keybuf, bt_page_size));
      ASSERT_EQUAL(0, rc);

      rc = message_lex_cmp(msg, gen_msg(cfg, k, msgbuf, bt_page_size));
      ASSERT_EQUAL(0, rc);

      if (forwards) {
         ASSERT_TRUE(key_is_null(prev)
                     || data_key_compare(cfg->data_cfg, prev, curr_key) < 0);
      } else {
         ASSERT_TRUE(key_is_null(prev)
                     || data_key_compare(cfg->data_cfg, curr_key, prev) < 0);
      }

      seen++;
      prev = key_create(FALSE, key_length(curr_key), prevbuf);
      key_copy_contents(prevbuf, curr_key);

      if (forwards) {
         if (!SUCCESS(iterator_next(iter))) {
            break;
         }
      } else {
         if (!SUCCESS(iterator_prev(iter))) {
            break;
         }
      }
   }

   platform_free(hid, prevbuf);
   platform_free(hid, keybuf);
   platform_free(hid, msgbuf);
   return seen;
}

static int
iterator_tests(cache           *cc,
               btree_config    *cfg,
               uint64           root_addr,
               page_type        type,
               int              nkvs,
               bool32           start_front,
               platform_heap_id hid)
{
   btree_iterator dbiter;

   key start_key;
   if (start_front)
      start_key = NEGATIVE_INFINITY_KEY;
   else
      start_key = POSITIVE_INFINITY_KEY;

   platform_status rc = btree_iterator_init(
      cc,
      cfg,
      &dbiter,
      root_addr,
      BTREE_DEFAULT_FORMAT,
      type,
      &(const btree_iterator_init_options){
         .start_position = key_start_at_or_after(start_key)});
   ASSERT_TRUE(SUCCESS(rc));

   iterator *iter = (iterator *)&dbiter;

   if (0 < nkvs && !start_front) {
      iterator_prev(iter);
   }
   bool32 nonempty = iterator_can_curr(iter);

   ASSERT_EQUAL(nkvs, iterator_test(hid, cfg, nkvs, iter, start_front));
   if (nonempty) {
      if (start_front) {
         iterator_prev(iter);
      } else {
         iterator_next(iter);
      }
   }
   ASSERT_EQUAL(nkvs, iterator_test(hid, cfg, nkvs, iter, !start_front));

   btree_iterator_deinit(&dbiter);

   return 1;
}

static int
iterator_seek_tests(cache           *cc,
                    btree_config    *cfg,
                    uint64           root_addr,
                    int              nkvs,
                    platform_heap_id hid)
{
   btree_iterator dbiter;

   int    keybuf_size = btree_page_size(cfg);
   uint8 *keybuf      = TYPED_MANUAL_MALLOC(hid, keybuf, keybuf_size);

   // start in the "middle" of the range
   key start_key = gen_key(cfg, nkvs / 2, keybuf, keybuf_size);

   platform_status rc = btree_iterator_init(
      cc,
      cfg,
      &dbiter,
      root_addr,
      (btree_format){0},
      PAGE_TYPE_MEMTABLE,
      &(const btree_iterator_init_options){
         .start_position = key_start_at_or_after(start_key)});
   ASSERT_TRUE(SUCCESS(rc));
   iterator *iter = (iterator *)&dbiter;

   // go down
   uint64 found_down = iterator_test(hid, cfg, nkvs, iter, FALSE);

   // seek back to start_key
   iterator_seek(iter, greater_than_or_equal, start_key);

   // skip start_key
   iterator_next(iter);

   // go up
   uint64 found_up = iterator_test(hid, cfg, nkvs, iter, TRUE);

   ASSERT_EQUAL(nkvs, found_up + found_down);

   btree_iterator_deinit(&dbiter);

   platform_free(hid, keybuf);

   return 1;
}

static uint64
pack_tests(cache           *cc,
           btree_config    *cfg,
           platform_heap_id hid,
           uint64           root_addr,
           uint64           nkvs)
{
   btree_iterator dbiter;
   iterator      *iter = (iterator *)&dbiter;

   platform_status rc =
      btree_iterator_init(cc,
                          cfg,
                          &dbiter,
                          root_addr,
                          (btree_format){0},
                          PAGE_TYPE_MEMTABLE,
                          &(const btree_iterator_init_options){0});
   ASSERT_TRUE(SUCCESS(rc));

   rc = STATUS_TEST_FAILED;
   btree_pack_req req;
   rc = btree_pack_req_init(&req, cc, cfg, iter, nkvs, hid);
   ASSERT_TRUE(SUCCESS(rc));

   if (!SUCCESS(btree_pack(&req))) {
      ASSERT_TRUE(FALSE, "Pack failed! req.num_tuples = %d\n", req.num_tuples);
   } else {
      CTEST_LOG_INFO("Packed %lu items ", req.num_tuples);
   }

   uint64 packed_root_addr = req.root_addr;
   btree_pack_req_deinit(&req, hid);

   return packed_root_addr;
}

/* Ordered keys and deliberately large values make leaf splits predictable. */
static key
bounds_key(uint64 n, uint64 *buf)
{
   *buf = htobe64(n);
   return key_create(FALSE, sizeof(*buf), buf);
}

static uint64
bounds_key_number(key k)
{
   uint64 n;
   memcpy(&n, key_data(k), sizeof(n));
   return be64toh(n);
}

static void
bounds_insert(cache           *cc,
              btree_config    *cfg,
              platform_heap_id hid,
              btree_scratch   *scratch,
              mini_allocator  *mini,
              uint64           root,
              uint64           n)
{
   uint64               keybuf;
   uint8                msgbuf[129] = {1};
   btree_insert_results result;
   btree_insert_results_init(&result, NULL);
   platform_status rc = btree_insert(
      cc,
      cfg,
      hid,
      scratch,
      root,
      mini,
      bounds_key(n, &keybuf),
      message_create_internal(
         MESSAGE_TYPE_INSERT, NULL, slice_create(sizeof(msgbuf), msgbuf)),
      &result);
   btree_insert_results_deinit(&result);
   platform_assert_status_ok(rc);
}

static void
bounds_ready(void *arg)
{
   __atomic_store_n((bool32 *)arg, TRUE, __ATOMIC_RELEASE);
}

static platform_status
bounds_init_with_copy(cache          *cc,
                      btree_config   *cfg,
                      btree_iterator *itor,
                      uint64          root,
                      key             lo,
                      comparison      lo_cmp,
                      key             hi,
                      comparison      hi_cmp,
                      bool32          reverse,
                      bool32          async,
                      page_type       type,
                      uint32          prefetch_lookahead,
                      bool32          copy_nodes)
{
   comparison start_cmp = reverse ? hi_cmp : lo_cmp;
   key        start     = reverse ? hi : lo;
   if (!async) {
      return btree_iterator_init(
         cc,
         cfg,
         itor,
         root,
         BTREE_DEFAULT_FORMAT,
         type,
         &(const btree_iterator_init_options){
            .lower_limit        = make_key_lower_limit(lo_cmp, lo),
            .upper_limit        = make_key_upper_limit(hi_cmp, hi),
            .start_position     = make_key_start_position(start_cmp, start),
            .copy_nodes         = copy_nodes,
            .prefetch_lookahead = prefetch_lookahead});
   }
   btree_iterator_async_state state;
   bool32                     ready = FALSE;
   btree_iterator_async_state_init(&state,
                                   cc,
                                   cfg,
                                   itor,
                                   root,
                                   BTREE_DEFAULT_FORMAT,
                                   type,
                                   lo_cmp,
                                   lo,
                                   hi_cmp,
                                   hi,
                                   start_cmp,
                                   start,
                                   copy_nodes,
                                   0,
                                   prefetch_lookahead,
                                   bounds_ready,
                                   &ready);
   // A reused coroutine must not inherit the previous operation's result.
   state.__async_result = STATUS_INVALID_STATE;
   while (btree_iterator_init_async(&state) != ASYNC_STATUS_DONE) {
      while (!__atomic_exchange_n(&ready, FALSE, __ATOMIC_ACQUIRE)) {
         cache_cleanup(cc);
      }
   }
   return btree_iterator_init_async_result(&state);
}

static platform_status
bounds_init(cache          *cc,
            btree_config   *cfg,
            btree_iterator *itor,
            uint64          root,
            key             lo,
            comparison      lo_cmp,
            key             hi,
            comparison      hi_cmp,
            bool32          reverse,
            bool32          async,
            page_type       type,
            uint32          prefetch_lookahead)
{
   return bounds_init_with_copy(cc,
                                cfg,
                                itor,
                                root,
                                lo,
                                lo_cmp,
                                hi,
                                hi_cmp,
                                reverse,
                                async,
                                type,
                                prefetch_lookahead,
                                TRUE);
}

/* Fail one selected root-page read while an immutable iterator prepares its
 * upper bound. The sync and async wrappers share a counter so the same test
 * identifies the endpoint refresh in both implementations. */
typedef struct iterator_endpoint_fault {
   cache           *cc;
   const cache_ops *original;
   cache_ops        ops;
   uint64           root;
   uint64           root_gets;
   uint64           fail_at;
   uint64           failures;
} iterator_endpoint_fault;

static iterator_endpoint_fault *active_iterator_endpoint_fault;

static bool32
iterator_endpoint_fault_should_fail(uint64 addr)
{
   iterator_endpoint_fault *fault = active_iterator_endpoint_fault;
   if (addr != fault->root) {
      return FALSE;
   }
   fault->root_gets++;
   if (fault->root_gets == fault->fail_at) {
      fault->failures++;
      return TRUE;
   }
   return FALSE;
}

static platform_status
iterator_endpoint_fault_get(cache       *cc,
                            uint64       addr,
                            bool32       blocking,
                            page_type    type,
                            page_handle *page)
{
   iterator_endpoint_fault *fault = active_iterator_endpoint_fault;
   platform_assert(fault != NULL && fault->cc == cc);
   if (iterator_endpoint_fault_should_fail(addr)) {
      *page = (page_handle){0};
      return STATUS_TIMEDOUT;
   }
   return fault->original->page_get(cc, addr, blocking, type, page);
}

static platform_status
iterator_endpoint_fault_async_result(void *payload, page_handle *page)
{
   iterator_endpoint_fault *fault = active_iterator_endpoint_fault;
   platform_assert(fault != NULL);
   platform_status rc = fault->original->page_get_async_result(payload, page);
   if (SUCCESS(rc)
       && iterator_endpoint_fault_should_fail(page_handle_addr(page)))
   {
      fault->original->page_unget(fault->cc, page);
      *page = (page_handle){0};
      return STATUS_TIMEDOUT;
   }
   return rc;
}

static platform_status
iterator_endpoint_fault_init(cache          *cc,
                             btree_config   *cfg,
                             btree_iterator *itor,
                             uint64          root,
                             key             upper,
                             bool32          async)
{
   if (!async) {
      return btree_iterator_init(cc,
                                 cfg,
                                 itor,
                                 root,
                                 BTREE_DEFAULT_FORMAT,
                                 PAGE_TYPE_BRANCH,
                                 &(const btree_iterator_init_options){
                                    .upper_limit = key_less_than(upper)});
   }

   btree_iterator_async_state state;
   bool32                     ready = FALSE;
   btree_iterator_async_state_init(&state,
                                   cc,
                                   cfg,
                                   itor,
                                   root,
                                   BTREE_DEFAULT_FORMAT,
                                   PAGE_TYPE_BRANCH,
                                   greater_than_or_equal,
                                   NEGATIVE_INFINITY_KEY,
                                   less_than,
                                   upper,
                                   greater_than_or_equal,
                                   NEGATIVE_INFINITY_KEY,
                                   FALSE,
                                   0,
                                   0,
                                   bounds_ready,
                                   &ready);
   while (btree_iterator_init_async(&state) != ASYNC_STATUS_DONE) {
      while (!__atomic_exchange_n(&ready, FALSE, __ATOMIC_ACQUIRE)) {
         cache_cleanup(cc);
      }
   }
   return btree_iterator_init_async_result(&state);
}

/* Unbounded immutable scans defer endpoint traversal until the last page.
 * Observe root acquisitions to catch an eager walk, then exercise the endpoint
 * while crossing leaves in both directions and seeking away from the end. */
CTEST2(btree_stress, unbounded_branch_iterator)
{
   cache         *cc  = (cache *)&data->cc;
   btree_config  *cfg = &data->dbtree_cfg;
   mini_allocator mini;
   const uint64   count = 2048;
   uint64 source_root   = test_btree_create(cc, cfg, &mini, PAGE_TYPE_MEMTABLE);
   for (uint64 n = 0; n < count; n++) {
      bounds_insert(
         cc, cfg, data->hid, data->test_scratch, &mini, source_root, n);
   }
   btree_iterator source;
   ASSERT_TRUE(
      SUCCESS(btree_iterator_init(cc,
                                  cfg,
                                  &source,
                                  source_root,
                                  (btree_format){0},
                                  PAGE_TYPE_MEMTABLE,
                                  &(const btree_iterator_init_options){0})));
   btree_pack_req req;
   ASSERT_TRUE(SUCCESS(
      btree_pack_req_init(&req, cc, cfg, &source.super, count, data->hid)));
   ASSERT_TRUE(SUCCESS(btree_pack(&req)));
   uint64 packed_root = req.root_addr;
   btree_pack_req_deinit(&req, data->hid);
   btree_iterator_deinit(&source);

   for (unsigned async = 0; async < 2; async++) {
      for (unsigned copy = 0; copy < 2; copy++) {
         for (unsigned reverse = 0; reverse < 2; reverse++) {
            iterator_endpoint_fault observer = {
               .cc       = cc,
               .original = cc->ops,
               .ops      = *cc->ops,
               .root     = packed_root,
            };
            observer.ops.page_get = iterator_endpoint_fault_get;
            observer.ops.page_get_async_result =
               iterator_endpoint_fault_async_result;
            active_iterator_endpoint_fault = &observer;
            cc->ops                        = &observer.ops;

            uint64          lower_buf;
            const int64     first = 13;
            btree_iterator  itor;
            platform_status rc =
               bounds_init_with_copy(cc,
                                     cfg,
                                     &itor,
                                     packed_root,
                                     bounds_key(first, &lower_buf),
                                     greater_than_or_equal,
                                     POSITIVE_INFINITY_KEY,
                                     less_than,
                                     reverse,
                                     async,
                                     PAGE_TYPE_BRANCH,
                                     0,
                                     copy);

            cc->ops                        = observer.original;
            active_iterator_endpoint_fault = NULL;
            ASSERT_TRUE(SUCCESS(rc));
            ASSERT_NOT_EQUAL(packed_root, itor.curr.addr);
            ASSERT_EQUAL(reverse && !copy ? 2 : 1, observer.root_gets);
            bool32 backwards = reverse;
            for (unsigned direction = 0; direction < 2; direction++) {
               int64 expected = backwards ? count - 1 : first;
               for (uint64 n = first; n < count; n++) {
                  ASSERT_TRUE(iterator_can_curr(&itor.super));
                  key     k;
                  message msg;
                  iterator_curr(&itor.super, &k, &msg);
                  ASSERT_EQUAL(expected, bounds_key_number(k));
                  expected += backwards ? -1 : 1;
                  rc = backwards ? iterator_prev(&itor.super)
                                 : iterator_next(&itor.super);
                  ASSERT_TRUE(SUCCESS(rc));
               }
               ASSERT_FALSE(iterator_can_curr(&itor.super));
               rc = backwards ? iterator_next(&itor.super)
                              : iterator_prev(&itor.super);
               ASSERT_TRUE(SUCCESS(rc));
               backwards = !backwards;
            }

            /* Seeking back from the last leaf must discard its cached bound. */
            uint64 seek_buf;
            ASSERT_TRUE(
               SUCCESS(iterator_seek(&itor.super,
                                     greater_than_or_equal,
                                     bounds_key(count - 1, &seek_buf))));
            ASSERT_TRUE(SUCCESS(iterator_next(&itor.super)));
            ASSERT_FALSE(iterator_can_curr(&itor.super));
            ASSERT_TRUE(SUCCESS(iterator_seek(&itor.super,
                                              greater_than_or_equal,
                                              bounds_key(first, &seek_buf))));
            for (uint64 n = first; n < count; n++) {
               ASSERT_TRUE(iterator_can_curr(&itor.super));
               key     k;
               message msg;
               iterator_curr(&itor.super, &k, &msg);
               ASSERT_EQUAL(n, bounds_key_number(k));
               ASSERT_TRUE(SUCCESS(iterator_next(&itor.super)));
            }
            ASSERT_FALSE(iterator_can_curr(&itor.super));
            btree_iterator_deinit(&itor);
         }
      }
   }
   /* A forged early last-page marker must still fail instead of producing a
    * successful truncated scan. Endpoint lookup is deferred, not removed. */
   btree_iterator probe;
   ASSERT_TRUE(SUCCESS(iterator_endpoint_fault_init(
      cc, cfg, &probe, packed_root, POSITIVE_INFINITY_KEY, FALSE)));
   uint64 first_leaf = probe.curr.addr;
   btree_iterator_deinit(&probe);
   page_handle page;
   ASSERT_TRUE(
      SUCCESS(cache_get(cc, first_leaf, TRUE, PAGE_TYPE_BRANCH, &page)));
   ASSERT_TRUE(cache_try_claim(cc, &page));
   cache_lock(cc, &page);
   btree_hdr *hdr   = (btree_hdr *)page_handle_data(&page);
   uint8      flags = hdr->flags;
   hdr->flags |= BTREE_HDR_LAST_IN_LEVEL;
   cache_unlock(cc, &page);
   cache_unclaim(cc, &page);
   cache_unget(cc, &page);

   for (unsigned async = 0; async < 2; async++) {
      platform_status rc = iterator_endpoint_fault_init(
         cc, cfg, &probe, packed_root, POSITIVE_INFINITY_KEY, async);
      uint64 seen = 0;
      while (iterator_can_curr(&probe.super)) {
         seen++;
         rc = iterator_next(&probe.super);
         if (!SUCCESS(rc)) {
            break;
         }
      }
      btree_iterator_deinit(&probe);
      ASSERT_FALSE(SUCCESS(rc));
      ASSERT_TRUE(seen < count);
   }

   ASSERT_TRUE(
      SUCCESS(cache_get(cc, first_leaf, TRUE, PAGE_TYPE_BRANCH, &page)));
   ASSERT_TRUE(cache_try_claim(cc, &page));
   cache_lock(cc, &page);
   hdr        = (btree_hdr *)page_handle_data(&page);
   hdr->flags = flags;
   cache_unlock(cc, &page);
   cache_unclaim(cc, &page);
   cache_unget(cc, &page);

   btree_dec_ref(cc, cfg, packed_root, PAGE_TYPE_BRANCH);
   mini_release(&mini);
   btree_dec_ref(cc, cfg, source_root, PAGE_TYPE_MEMTABLE);
}

CTEST2(btree_stress, iterator_endpoint_error_is_returned_by_init)
{
   cache         *cc  = (cache *)&data->cc;
   btree_config  *cfg = &data->dbtree_cfg;
   mini_allocator mini;
   const uint64   count = 80;
   uint64 source_root   = test_btree_create(cc, cfg, &mini, PAGE_TYPE_MEMTABLE);
   for (uint64 n = 0; n < count; n++) {
      bounds_insert(
         cc, cfg, data->hid, data->test_scratch, &mini, source_root, 2 * n);
   }

   btree_iterator  source;
   platform_status rc =
      btree_iterator_init(cc,
                          cfg,
                          &source,
                          source_root,
                          (btree_format){0},
                          PAGE_TYPE_MEMTABLE,
                          &(const btree_iterator_init_options){0});
   ASSERT_TRUE(SUCCESS(rc));
   btree_pack_req req;
   rc = btree_pack_req_init(&req, cc, cfg, &source.super, count, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc                 = btree_pack(&req);
   uint64 packed_root = req.root_addr;
   btree_pack_req_deinit(&req, data->hid);
   btree_iterator_deinit(&source);
   ASSERT_TRUE(SUCCESS(rc));

   btree_iterator probe;
   rc = btree_iterator_init(cc,
                            cfg,
                            &probe,
                            packed_root,
                            BTREE_DEFAULT_FORMAT,
                            PAGE_TYPE_BRANCH,
                            &(const btree_iterator_init_options){0});
   ASSERT_TRUE(SUCCESS(rc));
   uint64 first_leaf = probe.curr.addr;
   uint64 upper_num  = 0;
   while (iterator_can_curr(&probe.super) && probe.curr.addr == first_leaf) {
      key     tuple_key;
      message msg;
      iterator_curr(&probe.super, &tuple_key, &msg);
      upper_num = bounds_key_number(tuple_key) + 1;
      ASSERT_TRUE(SUCCESS(iterator_next(&probe.super)));
   }
   ASSERT_TRUE(iterator_can_curr(&probe.super));
   ASSERT_NOT_EQUAL(first_leaf, probe.curr.addr);
   btree_iterator_deinit(&probe);

   uint64 upper_buf;
   key    upper = bounds_key(upper_num, &upper_buf);
   for (bool32 async = FALSE; async <= TRUE; async++) {
      iterator_endpoint_fault fault = {
         .cc       = cc,
         .original = cc->ops,
         .ops      = *cc->ops,
         .root     = packed_root,
         .fail_at  = 3,
      };
      fault.ops.page_get              = iterator_endpoint_fault_get;
      fault.ops.page_get_async_result = iterator_endpoint_fault_async_result;
      active_iterator_endpoint_fault  = &fault;
      cc->ops                         = &fault.ops;

      rc = iterator_endpoint_fault_init(
         cc, cfg, &probe, packed_root, upper, async);
      uint64 gets_before_observation = fault.root_gets;
      bool32 can_curr                = iterator_can_curr(&probe.super);
      uint64 gets_after_observation  = fault.root_gets;

      cc->ops                        = fault.original;
      active_iterator_endpoint_fault = NULL;

      ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_TIMEDOUT),
                  "async=%u returned %s",
                  async,
                  platform_status_to_string(rc));
      ASSERT_EQUAL(1, fault.failures);
      ASSERT_EQUAL(3, fault.root_gets);
      ASSERT_FALSE(can_curr);
      ASSERT_EQUAL(gets_before_observation, gets_after_observation);
   }

   btree_dec_ref(cc, cfg, packed_root, PAGE_TYPE_BRANCH);
   mini_release(&mini);
   btree_dec_ref(cc, cfg, source_root, PAGE_TYPE_MEMTABLE);
}

CTEST2(btree_stress, count_in_range_returns_lookup_error)
{
   cache         *cc  = (cache *)&data->cc;
   btree_config  *cfg = &data->dbtree_cfg;
   mini_allocator mini;
   const uint64   count = 80;
   uint64 source_root   = test_btree_create(cc, cfg, &mini, PAGE_TYPE_MEMTABLE);
   for (uint64 n = 0; n < count; n++) {
      bounds_insert(
         cc, cfg, data->hid, data->test_scratch, &mini, source_root, 2 * n);
   }

   btree_iterator  source;
   platform_status rc =
      btree_iterator_init(cc,
                          cfg,
                          &source,
                          source_root,
                          (btree_format){0},
                          PAGE_TYPE_MEMTABLE,
                          &(const btree_iterator_init_options){0});
   ASSERT_TRUE(SUCCESS(rc));
   btree_pack_req req;
   rc = btree_pack_req_init(&req, cc, cfg, &source.super, count, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc                 = btree_pack(&req);
   uint64 packed_root = req.root_addr;
   btree_pack_req_deinit(&req, data->hid);
   btree_iterator_deinit(&source);
   ASSERT_TRUE(SUCCESS(rc));

   uint64 min_buf, max_buf;
   key    min_key = bounds_key(10, &min_buf);
   key    max_key = bounds_key(20, &max_buf);
   for (uint64 fail_at = 1; fail_at <= 2; fail_at++) {
      iterator_endpoint_fault fault = {
         .cc       = cc,
         .original = cc->ops,
         .ops      = *cc->ops,
         .root     = packed_root,
         .fail_at  = fail_at,
      };
      fault.ops.page_get             = iterator_endpoint_fault_get;
      active_iterator_endpoint_fault = &fault;
      cc->ops                        = &fault.ops;

      btree_pivot_stats stats = {
         .num_kvs = 1, .key_bytes = 1, .message_bytes = 1};
      rc = btree_count_in_range(
         cc, cfg, packed_root, BTREE_DEFAULT_FORMAT, min_key, max_key, &stats);

      cc->ops                        = fault.original;
      active_iterator_endpoint_fault = NULL;

      ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_TIMEDOUT),
                  "fail_at=%lu returned %s",
                  fail_at,
                  platform_status_to_string(rc));
      ASSERT_EQUAL(1, fault.failures);
      ASSERT_EQUAL(0, stats.num_kvs);
      ASSERT_EQUAL(0, stats.key_bytes);
      ASSERT_EQUAL(0, stats.message_bytes);
   }

   btree_dec_ref(cc, cfg, packed_root, PAGE_TYPE_BRANCH);
   mini_release(&mini);
   btree_dec_ref(cc, cfg, source_root, PAGE_TYPE_MEMTABLE);
}

CTEST2(btree_stress, copied_iterator_bounds)
{
   cache         *cc  = (cache *)&data->cc;
   btree_config  *cfg = &data->dbtree_cfg;
   mini_allocator mini;
   uint64         root = test_btree_create(cc, cfg, &mini, PAGE_TYPE_MEMTABLE);
   for (unsigned async = 0; async < 2; async++) {
      btree_iterator  empty;
      platform_status rc = bounds_init(cc,
                                       cfg,
                                       &empty,
                                       root,
                                       NEGATIVE_INFINITY_KEY,
                                       greater_than_or_equal,
                                       POSITIVE_INFINITY_KEY,
                                       less_than,
                                       FALSE,
                                       async,
                                       PAGE_TYPE_MEMTABLE,
                                       0);
      ASSERT_TRUE(SUCCESS(rc), "empty async=%u rc=%d", async, rc.r);
      ASSERT_FALSE(iterator_can_curr(&empty.super));

      uint64 seek_buf;
      rc = iterator_seek(
         &empty.super, greater_than_or_equal, bounds_key(0, &seek_buf));
      ASSERT_TRUE(SUCCESS(rc), "empty seek async=%u rc=%d", async, rc.r);
      ASSERT_FALSE(iterator_can_curr(&empty.super));
      btree_iterator_deinit(&empty);
   }
   for (uint64 n = 0; n < 2048; n += 2) {
      bounds_insert(cc, cfg, data->hid, data->test_scratch, &mini, root, n);
   }
   const uint64 bounds[][2] = {{0, 0},
                               {1, 1},
                               {0, 1},
                               {0, 2},
                               {1, 2},
                               {20, 40},
                               {21, 41},
                               {100, 1999},
                               {2045, 2047},
                               {2048, 4096},
                               {0, 4096},
                               {100, 20}};
   for (uint64 c = 0; c < ARRAY_SIZE(bounds); c++) {
      for (unsigned flags = 0; flags < 16; flags++) {
         bool32          reverse      = flags & 1;
         bool32          async        = flags & 2;
         bool32          lo_exclusive = flags & 4;
         bool32          hi_exclusive = flags & 8;
         uint64          lo_buf, hi_buf;
         uint64          lo = bounds[c][0], hi = bounds[c][1];
         btree_iterator  itor;
         platform_status rc =
            bounds_init(cc,
                        cfg,
                        &itor,
                        root,
                        bounds_key(lo, &lo_buf),
                        lo_exclusive ? greater_than : greater_than_or_equal,
                        bounds_key(hi, &hi_buf),
                        hi_exclusive ? less_than : less_than_or_equal,
                        reverse,
                        async,
                        PAGE_TYPE_MEMTABLE,
                        0);
         ASSERT_TRUE(
            SUCCESS(rc), "bounds c=%lu flags=%u rc=%d", c, flags, rc.r);
         int64 first = (lo + !!lo_exclusive + 1) / 2 * 2;
         int64 last  = MIN((int64)hi - !!hi_exclusive, 2046);
         if (last >= 0)
            last = last / 2 * 2;
         int64 expected_count = first <= last ? (last - first) / 2 + 1 : 0;
         for (unsigned direction = 0; direction < 2; direction++) {
            int64 expected = reverse ? last : first;
            int64 count    = 0;
            while (iterator_can_curr(&itor.super)) {
               key     k;
               message msg;
               iterator_curr(&itor.super, &k, &msg);
               ASSERT_EQUAL(expected, bounds_key_number(k));
               expected += reverse ? -2 : 2;
               count++;
               ASSERT_TRUE(count <= expected_count);
               rc = reverse ? iterator_prev(&itor.super)
                            : iterator_next(&itor.super);
               ASSERT_TRUE(SUCCESS(rc));
            }
            ASSERT_EQUAL(expected_count, count);
            if (count) {
               rc = reverse ? iterator_next(&itor.super)
                            : iterator_prev(&itor.super);
               ASSERT_TRUE(SUCCESS(rc));
            }
            reverse = !reverse;
         }
         if (expected_count) {
            uint64 seek_buf;
            rc = iterator_seek(&itor.super,
                               greater_than_or_equal,
                               bounds_key(first, &seek_buf));
            ASSERT_TRUE(SUCCESS(rc));
            ASSERT_TRUE(iterator_can_curr(&itor.super));
            key     k;
            message msg;
            iterator_curr(&itor.super, &k, &msg);
            ASSERT_EQUAL(first, bounds_key_number(k));
         }
         btree_iterator_deinit(&itor);
      }
   }
   for (unsigned async = 0; async < 2; async++) {
      btree_iterator  itor;
      platform_status rc = bounds_init(cc,
                                       cfg,
                                       &itor,
                                       root,
                                       NEGATIVE_INFINITY_KEY,
                                       greater_than_or_equal,
                                       POSITIVE_INFINITY_KEY,
                                       less_than,
                                       FALSE,
                                       async,
                                       PAGE_TYPE_MEMTABLE,
                                       0);
      ASSERT_TRUE(SUCCESS(rc));
      for (int64 n = 0; n < 2048; n += 2) {
         ASSERT_TRUE(iterator_can_curr(&itor.super));
         key     k;
         message msg;
         iterator_curr(&itor.super, &k, &msg);
         ASSERT_EQUAL(n, bounds_key_number(k));
         ASSERT_TRUE(SUCCESS(iterator_next(&itor.super)));
      }
      ASSERT_FALSE(iterator_can_curr(&itor.super));
      ASSERT_TRUE(SUCCESS(iterator_prev(&itor.super)));
      for (int64 n = 2046; n >= 0; n -= 2) {
         ASSERT_TRUE(iterator_can_curr(&itor.super));
         key     k;
         message msg;
         iterator_curr(&itor.super, &k, &msg);
         ASSERT_EQUAL(n, bounds_key_number(k));
         ASSERT_TRUE(SUCCESS(iterator_prev(&itor.super)));
      }
      ASSERT_FALSE(iterator_can_curr(&itor.super));
      btree_iterator_deinit(&itor);
   }
}

/* Observe the real cache interface, and split the copied leaf exactly when its
 * read reference is released. Also detect the old leaf-to-root lock inversion.
 * These wrappers are installed only for this single-threaded test. */
typedef struct bounds_cache_observer {
   const cache_ops *original;
   cache_ops        ops;
   btree_config    *cfg;
   platform_heap_id hid;
   btree_scratch   *scratch;
   mini_allocator  *mini;
   uint64           root, leaf, old_next;
   uint64           leaf_refs;
   bool32           inversion, injected, split;
} bounds_cache_observer;

static bounds_cache_observer *bounds_observer;

static void
bounds_observe_get(uint64 addr)
{
   if (addr == bounds_observer->root && bounds_observer->leaf_refs != 0) {
      bounds_observer->inversion = TRUE;
   }
}

static platform_status
bounds_get(cache       *cc,
           uint64       addr,
           bool32       blocking,
           page_type    type,
           page_handle *p)
{
   bounds_observe_get(addr);
   platform_status rc =
      bounds_observer->original->page_get(cc, addr, blocking, type, p);
   if (SUCCESS(rc) && page_handle_addr(p) == bounds_observer->leaf)
      bounds_observer->leaf_refs++;
   return rc;
}

static void
bounds_async_init(void             *payload,
                  cache            *cc,
                  uint64            addr,
                  page_type         type,
                  async_callback_fn callback,
                  void             *arg)
{
   bounds_observe_get(addr);
   bounds_observer->original->page_get_async_state_init(
      payload, cc, addr, type, callback, arg);
}

static platform_status
bounds_async_result(void *payload, page_handle *p)
{
   platform_status rc =
      bounds_observer->original->page_get_async_result(payload, p);
   if (SUCCESS(rc) && page_handle_addr(p) == bounds_observer->leaf)
      bounds_observer->leaf_refs++;
   return rc;
}

static void
bounds_unget(cache *cc, page_handle *p)
{
   bounds_cache_observer *o       = bounds_observer;
   bool32                 is_leaf = page_handle_addr(p) == o->leaf;
   o->original->page_unget(cc, p);
   if (!is_leaf)
      return;
   platform_assert(o->leaf_refs != 0);
   o->leaf_refs--;
   if (o->injected)
      return;
   o->injected = TRUE;
   cc->ops     = o->original;
   for (uint64 n = 1; n < 2048; n += 2) {
      bounds_insert(cc, o->cfg, o->hid, o->scratch, o->mini, o->root, n);
   }
   page_handle observed =
      test_cache_get_page(cc, o->leaf, TRUE, PAGE_TYPE_MEMTABLE);
   o->split =
      ((btree_hdr *)page_handle_data(&observed))->next_addr != o->old_next;
   cache_unget(cc, &observed);
   cc->ops = &o->ops;
}

CTEST2(btree_stress, copied_iterator_split_on_release)
{
   cache        *cc  = (cache *)&data->cc;
   btree_config *cfg = &data->dbtree_cfg;
   for (unsigned async = 0; async < 2; async++) {
      mini_allocator mini;
      uint64 root = test_btree_create(cc, cfg, &mini, PAGE_TYPE_MEMTABLE);
      for (uint64 n = 0; n < 2048; n += 2) {
         bounds_insert(cc, cfg, data->hid, data->test_scratch, &mini, root, n);
      }
      btree_iterator  probe;
      platform_status rc =
         btree_iterator_init(cc,
                             cfg,
                             &probe,
                             root,
                             (btree_format){0},
                             PAGE_TYPE_MEMTABLE,
                             &(const btree_iterator_init_options){0});
      ASSERT_TRUE(SUCCESS(rc));
      btree_node leaf = probe.curr;
      ASSERT_TRUE(leaf.addr != root);
      uint64 last = bounds_key_number(
         btree_get_tuple_key(cfg, leaf.hdr, leaf.hdr->num_entries - 1));
      bounds_cache_observer observer = {.original = cc->ops,
                                        .ops      = *cc->ops,
                                        .cfg      = cfg,
                                        .hid      = data->hid,
                                        .scratch  = data->test_scratch,
                                        .mini     = &mini,
                                        .root     = root,
                                        .leaf     = leaf.addr,
                                        .old_next = leaf.hdr->next_addr};
      btree_iterator_deinit(&probe);
      observer.ops.page_get                  = bounds_get;
      observer.ops.page_get_async_state_init = bounds_async_init;
      observer.ops.page_get_async_result     = bounds_async_result;
      observer.ops.page_unget                = bounds_unget;
      bounds_observer                        = &observer;
      cc->ops                                = &observer.ops;
      uint64         hi_buf;
      btree_iterator itor;
      rc              = bounds_init(cc,
                       cfg,
                       &itor,
                       root,
                       NEGATIVE_INFINITY_KEY,
                       greater_than_or_equal,
                       bounds_key(last - 2, &hi_buf),
                       less_than_or_equal,
                       FALSE,
                       async,
                       PAGE_TYPE_MEMTABLE,
                       0);
      cc->ops         = observer.original;
      bounds_observer = NULL;
      ASSERT_TRUE(SUCCESS(rc));
      bool32 copy_released = page_handle_is_null(&itor.curr.page);
      uint64 seen          = 0;
      while (iterator_can_curr(&itor.super)) {
         key     k;
         message msg;
         iterator_curr(&itor.super, &k, &msg);
         ASSERT_EQUAL(2 * seen, bounds_key_number(k));
         seen++;
         ASSERT_TRUE(SUCCESS(iterator_next(&itor.super)));
      }
      btree_iterator_deinit(&itor);
      ASSERT_EQUAL(last / 2, seen);
      ASSERT_TRUE(copy_released);
      ASSERT_TRUE(observer.injected);
      ASSERT_TRUE(observer.split);
      ASSERT_FALSE(observer.inversion,
                   "Iterator traversed root while retaining a leaf reference");
   }
}

static page_prefetch_fn bounds_original_prefetch;
static uint64           bounds_prefetch_count;

static void
bounds_count_prefetch(cache *cc, uint64 addr, page_type type)
{
   bounds_prefetch_count++;
   bounds_original_prefetch(cc, addr, type);
}

CTEST2(btree_stress, copied_iterator_prefetch)
{
   cache         *cc  = (cache *)&data->cc;
   btree_config  *cfg = &data->dbtree_cfg;
   mini_allocator mini;
   uint64         root  = test_btree_create(cc, cfg, &mini, PAGE_TYPE_MEMTABLE);
   const uint64   count = 8192;
   for (uint64 n = 0; n < count; n++) {
      bounds_insert(cc, cfg, data->hid, data->test_scratch, &mini, root, n);
   }

   btree_iterator  source;
   platform_status rc = bounds_init(cc,
                                    cfg,
                                    &source,
                                    root,
                                    NEGATIVE_INFINITY_KEY,
                                    greater_than_or_equal,
                                    POSITIVE_INFINITY_KEY,
                                    less_than,
                                    FALSE,
                                    FALSE,
                                    PAGE_TYPE_MEMTABLE,
                                    0);
   ASSERT_TRUE(SUCCESS(rc));
   btree_pack_req req;
   rc = btree_pack_req_init(&req, cc, cfg, &source.super, count, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc                 = btree_pack(&req);
   uint64 packed_root = req.root_addr;
   btree_pack_req_deinit(&req, data->hid);
   btree_iterator_deinit(&source);
   ASSERT_TRUE(SUCCESS(rc));

   // Memtables have no extent-prefetch links. Exercise copied and pinned packed
   // leaves to cover both the fallback and deep-prefetch paths. Initialization
   // and seek leave prefetch dormant; scanning starts it at an extent boundary.
   const cache_ops *original = cc->ops;
   cache_ops        observed = *original;
   bounds_original_prefetch  = original->page_prefetch;
   observed.page_prefetch    = bounds_count_prefetch;
   for (unsigned async = 0; async < 2; async++) {
      for (unsigned prefetch = 0; prefetch < 3; prefetch++) {
         for (unsigned copy_nodes = 0; copy_nodes < 2; copy_nodes++) {
            for (unsigned long_range = 0; long_range < 2; long_range++) {
               for (unsigned reverse = 0; reverse < 2; reverse++) {
                  uint64         limit = long_range ? count : 1;
                  uint64         hi_buf;
                  btree_iterator itor;
                  bounds_prefetch_count = 0;
                  cc->ops               = &observed;
                  rc                    = bounds_init_with_copy(cc,
                                             cfg,
                                             &itor,
                                             packed_root,
                                             NEGATIVE_INFINITY_KEY,
                                             greater_than_or_equal,
                                             bounds_key(limit, &hi_buf),
                                             less_than,
                                             reverse,
                                             async,
                                             PAGE_TYPE_BRANCH,
                                             prefetch,
                                             copy_nodes);
                  cc->ops               = original;
                  ASSERT_TRUE(SUCCESS(rc));
                  ASSERT_EQUAL(0, bounds_prefetch_count);

                  // Seeking establishes a position, but the first subsequent
                  // movement establishes the scan direction.
                  uint64 seek_buf;
                  cc->ops = &observed;
                  rc =
                     iterator_seek(&itor.super,
                                   reverse ? less_than : greater_than_or_equal,
                                   bounds_key(reverse ? limit : 0, &seek_buf));
                  cc->ops = original;
                  ASSERT_TRUE(SUCCESS(rc));
                  ASSERT_EQUAL(0, bounds_prefetch_count);

                  bool32 scan_reverse = reverse;
                  for (unsigned direction = 0; direction < 2; direction++) {
                     for (uint64 n = 0; n < limit; n++) {
                        ASSERT_TRUE(iterator_can_curr(&itor.super));
                        key     k;
                        message msg;
                        iterator_curr(&itor.super, &k, &msg);
                        ASSERT_EQUAL(scan_reverse ? limit - n - 1 : n,
                                     bounds_key_number(k));
                        cc->ops = &observed;
                        rc      = scan_reverse ? iterator_prev(&itor.super)
                                               : iterator_next(&itor.super);
                        cc->ops = original;
                        ASSERT_TRUE(SUCCESS(rc));
                     }
                     ASSERT_FALSE(iterator_can_curr(&itor.super));

                     // Return to the range and reverse direction. This also
                     // verifies that an active or fallback prefetcher restarts
                     // in the new direction only when it reaches that boundary.
                     cc->ops = &observed;
                     rc      = scan_reverse ? iterator_next(&itor.super)
                                            : iterator_prev(&itor.super);
                     cc->ops = original;
                     ASSERT_TRUE(SUCCESS(rc));
                     scan_reverse = !scan_reverse;
                  }
                  btree_iterator_deinit(&itor);
                  if (prefetch && long_range) {
                     ASSERT_TRUE(bounds_prefetch_count > 0);
                  } else {
                     ASSERT_EQUAL(0, bounds_prefetch_count);
                  }
               }
            }
         }
      }
   }
}

/* Native keys deliberately differ after P, forcing blob reads for comparisons.
 * Inline keys surround blobs so packing must both start a new leaf when C > P
 * and restore unrestricted prefix compression after leaving the blob leaves. */
#define NATIVE_KEY_COUNT    180
#define NATIVE_INLINE_COUNT 40

static key
native_test_key(uint64 n, char *buf)
{
   bool32 inline_key =
      n < NATIVE_INLINE_COUNT || n >= NATIVE_KEY_COUNT - NATIVE_INLINE_COUNT;
   uint64 length = inline_key                 ? (n % 5 ? 400 : 512)
                   : n == NATIVE_INLINE_COUNT ? 513
                                              : (n % 11 ? 1024 : 12000);
   memset(buf, 'a', length);
   uint64 order = htobe64(n);
   memcpy(buf + 300, &order, sizeof(order));
   return key_create(FALSE, length, buf);
}


static int
native_merge(const data_config *cfg,
             slice              logical_key,
             message            older,
             merge_accumulator *newer)
{
   platform_assert(slice_length(logical_key) >= 400);
   uint64 a, b;
   memcpy(&a, message_data(older), sizeof(a));
   memcpy(&b, merge_accumulator_data(newer), sizeof(b));
   b += a;
   memcpy(merge_accumulator_data(newer), &b, sizeof(b));
   merge_accumulator_set_class(newer, message_class(older));
   return 0;
}
static int
native_reverse_compare(const data_config *cfg, user_key a, user_key b)
{
   /* A descriptor or truncated key must never reach the user comparator. */
   platform_assert(slice_length(a.key) >= 400 && slice_length(b.key) >= 400);
   return -slice_lex_cmp(a.key, b.key);
}

static void
native_check_tree(cache        *cc,
                  btree_config *cfg,
                  uint64        root,
                  btree_format  format,
                  page_type     type,
                  bool32        reverse)
{
   char          keybuf[12000];
   lookup_result result;
   lookup_result_init(&result, cfg->data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   for (uint64 i = 0; i < NATIVE_KEY_COUNT; i++) {
      key             target = native_test_key(i, keybuf);
      platform_status rc =
         btree_lookup(cc, cfg, root, format, type, target, &result);
      ASSERT_TRUE(SUCCESS(rc), "lookup %lu: %d", i, rc.r);
      ASSERT_TRUE(lookup_result_found(&result));
      uint64 value;
      memcpy(&value, merge_accumulator_data(&result.value), sizeof(value));
      ASSERT_EQUAL(i + (i >= 5 && (i - 5) % 31 == 0), value);
      if (i % 17 == 0) {
         btree_lookup_async_state state;
         bool32                   ready = FALSE;
         btree_lookup_async_state_init(&state,
                                       cc,
                                       cfg,
                                       root,
                                       format,
                                       type,
                                       target,
                                       &result,
                                       bounds_ready,
                                       &ready);
         while (btree_lookup_async(&state) != ASYNC_STATUS_DONE) {
            while (!__atomic_exchange_n(&ready, FALSE, __ATOMIC_ACQUIRE)) {
               cache_cleanup(cc);
            }
         }
         ASSERT_TRUE(SUCCESS(async_result(&state)));
         ASSERT_TRUE(lookup_result_found(&result));
      }
   }
   lookup_result_deinit(&result);
   for (bool32 copy = FALSE; copy <= TRUE; copy++) {
      btree_iterator  itor;
      platform_status rc;
      if (!copy) {
         rc = btree_iterator_init(
            cc,
            cfg,
            &itor,
            root,
            format,
            type,
            &(btree_iterator_init_options){.copy_nodes = copy});
      } else {
         btree_iterator_async_state state;
         bool32                     ready = FALSE;
         btree_iterator_async_state_init(&state,
                                         cc,
                                         cfg,
                                         &itor,
                                         root,
                                         format,
                                         type,
                                         greater_than_or_equal,
                                         NEGATIVE_INFINITY_KEY,
                                         less_than,
                                         POSITIVE_INFINITY_KEY,
                                         greater_than_or_equal,
                                         NEGATIVE_INFINITY_KEY,
                                         copy,
                                         0,
                                         0,
                                         bounds_ready,
                                         &ready);
         while (btree_iterator_init_async(&state) != ASYNC_STATUS_DONE) {
            while (!__atomic_exchange_n(&ready, FALSE, __ATOMIC_ACQUIRE)) {
               cache_cleanup(cc);
            }
         }
         rc = btree_iterator_init_async_result(&state);
      }
      ASSERT_TRUE(SUCCESS(rc));
      for (uint64 i = 0; i < NATIVE_KEY_COUNT; i++) {
         ASSERT_TRUE(iterator_can_curr(&itor.super));
         key actual,
            expected =
               native_test_key(reverse ? NATIVE_KEY_COUNT - 1 - i : i, keybuf);
         message msg;
         iterator_curr(&itor.super, &actual, &msg);
         ASSERT_FALSE(actual.is_blob);
         ASSERT_TRUE(
            (slice_lex_cmp(key_slice(expected), key_slice(actual)) == 0),
            "scan i=%lu actual_length=%lu expected_length=%lu type=%u copy=%u",
            i,
            key_length(actual),
            key_length(expected),
            type,
            copy);
         rc = iterator_next(&itor.super);
         ASSERT_TRUE(SUCCESS(rc));
      }
      ASSERT_FALSE(iterator_can_curr(&itor.super));
      for (uint64 i = NATIVE_KEY_COUNT; i > 0; i--) {
         rc = iterator_prev(&itor.super);
         ASSERT_TRUE(SUCCESS(rc));
         ASSERT_TRUE(iterator_can_curr(&itor.super));
         key actual,
            expected =
               native_test_key(reverse ? NATIVE_KEY_COUNT - i : i - 1, keybuf);
         message msg;
         iterator_curr(&itor.super, &actual, &msg);
         ASSERT_TRUE(
            (slice_lex_cmp(key_slice(expected), key_slice(actual)) == 0),
            "scan i=%lu actual_length=%lu expected_length=%lu type=%u copy=%u",
            i,
            key_length(actual),
            key_length(expected),
            type,
            copy);
      }
      key seek = native_test_key(71, keybuf);
      rc       = iterator_seek(&itor.super, greater_than_or_equal, seek);
      ASSERT_TRUE(SUCCESS(rc));
      key     actual;
      message msg;
      iterator_curr(&itor.super, &actual, &msg);
      ASSERT_TRUE((slice_lex_cmp(key_slice(seek), key_slice(actual)) == 0));
      btree_iterator_deinit(&itor);
   }
}


static const cache_ops *native_saved_ops;
static uint64           native_failed_reads;

static platform_status
native_fail_blob_read(cache       *cc,
                      uint64       addr,
                      bool32       blocking,
                      page_type    type,
                      page_handle *page)
{
   if (type == PAGE_TYPE_BLOB) {
      native_failed_reads++;
      *page = (page_handle){0};
      return STATUS_TIMEDOUT;
   }
   return native_saved_ops->page_get(cc, addr, blocking, type, page);
}

static void
native_check_read_failure(cache        *cc,
                          btree_config *cfg,
                          uint64        root,
                          btree_format  format,
                          page_type     type)
{
   cache_ops fault_ops = *cc->ops;
   native_saved_ops    = cc->ops;
   fault_ops.page_get  = native_fail_blob_read;
   native_failed_reads = 0;
   lookup_result result;
   lookup_result_init(&result, cfg->data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   char buf[12000];
   key  target = native_test_key(71, buf);
   cc->ops     = &fault_ops;
   platform_status rc =
      btree_lookup(cc, cfg, root, format, type, target, &result);
   cc->ops = native_saved_ops;
   ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_TIMEDOUT));
   ASSERT_TRUE(native_failed_reads > 0);
   lookup_result_deinit(&result);

   cc->ops = &fault_ops;
   btree_iterator itor;
   rc =
      btree_iterator_init(cc,
                          cfg,
                          &itor,
                          root,
                          format,
                          type,
                          &(btree_iterator_init_options){
                             .start_position = key_start_at_or_after(target)});
   cc->ops = native_saved_ops;
   ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_TIMEDOUT));
   btree_iterator_deinit(&itor);

   /* Prefix-only rejection must not fetch blob pages for lexical order. */
   if (cfg->data_cfg->key_compare_is_lexical
       && (type == PAGE_TYPE_BRANCH ? format.key_prefix_size
                                    : cfg->blob_key_prefix_size)
             > 0)
   {
      buf[0] = 'z';
      lookup_result_init(
         &result, cfg->data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
      native_failed_reads = 0;
      cc->ops             = &fault_ops;
      rc      = btree_lookup(cc, cfg, root, format, type, target, &result);
      cc->ops = native_saved_ops;
      ASSERT_TRUE(SUCCESS(rc));
      ASSERT_FALSE(lookup_result_found(&result));
      ASSERT_EQUAL(0, native_failed_reads);
      lookup_result_deinit(&result);
   }
}

static void
native_check_corrupt_leaf(cache        *cc,
                          btree_config *cfg,
                          uint64        root,
                          btree_format  format)
{
   char           buf[12000];
   key            target = native_test_key(71, buf);
   btree_iterator holder;
   ASSERT_TRUE(SUCCESS(btree_iterator_init(
      cc,
      cfg,
      &holder,
      root,
      format,
      PAGE_TYPE_BRANCH,
      &(btree_iterator_init_options){.start_position =
                                        key_start_at_or_after(target)})));
   uint64 leaf_addr = holder.curr.addr;
   uint64 idx       = holder.idx;
   btree_iterator_deinit(&holder);
   lookup_result result;
   lookup_result_init(&result, cfg->data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   for (uint64 fault = 0; fault < 2; fault++) {
      page_handle page;
      ASSERT_TRUE(
         SUCCESS(cache_get(cc, leaf_addr, TRUE, PAGE_TYPE_BRANCH, &page)));
      ASSERT_TRUE(cache_try_claim(cc, &page));
      cache_lock(cc, &page);
      btree_hdr  *hdr            = (btree_hdr *)page_handle_data(&page);
      leaf_entry *entry          = btree_get_leaf_entry(cfg, hdr, idx);
      uint8       flags          = entry->key_flags;
      uint16      common         = hdr->prefix_length;
      uint64      p              = format.key_prefix_size - common;
      blob       *descriptor     = (blob *)(entry->key_and_message + p);
      uint64      logical_length = descriptor->length;
      if (fault == 0) {
         entry->key_flags |= BTREE_INDEX_WIDE_STATS; /* Invalid on leaves. */
      } else {
         descriptor->length = 1; /* Inline-sized keys may not be blobbed. */
      }
      cache_unlock(cc, &page);
      cache_unclaim(cc, &page);
      cache_unget(cc, &page);

      platform_status rc =
         btree_lookup(cc, cfg, root, format, PAGE_TYPE_BRANCH, target, &result);
      ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_INVALID_STATE),
                  "corruption %lu: %d",
                  fault,
                  rc.r);

      ASSERT_TRUE(
         SUCCESS(cache_get(cc, leaf_addr, TRUE, PAGE_TYPE_BRANCH, &page)));
      ASSERT_TRUE(cache_try_claim(cc, &page));
      cache_lock(cc, &page);
      hdr                = (btree_hdr *)page_handle_data(&page);
      hdr->prefix_length = common;
      entry              = btree_get_leaf_entry(cfg, hdr, idx);
      entry->key_flags   = flags;
      descriptor         = (blob *)(entry->key_and_message + p);
      descriptor->length = logical_length;
      cache_unlock(cc, &page);
      cache_unclaim(cc, &page);
      cache_unget(cc, &page);
   }
   lookup_result_deinit(&result);
   /* A copied leaf containing blobs must reject C > P on the next entry,
    * without relying on a search that might reject at the common prefix. */
   ASSERT_TRUE(SUCCESS(btree_iterator_init(
      cc,
      cfg,
      &holder,
      root,
      format,
      PAGE_TYPE_BRANCH,
      &(btree_iterator_init_options){.copy_nodes = TRUE,
                                     .start_position =
                                        key_start_at_or_after(target)})));
   holder.curr.hdr->prefix_length = format.key_prefix_size + 1;
   platform_status rc             = iterator_next(&holder.super);
   ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_INVALID_STATE));
   ASSERT_FALSE(iterator_can_curr(&holder.super));
   btree_iterator_deinit(&holder);
}
static void
native_check_corrupt_index(cache        *cc,
                           btree_config *cfg,
                           uint64        root,
                           btree_format  format,
                           key           target)
{
   lookup_result result;
   lookup_result_init(&result, cfg->data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   for (uint64 fault = 0; fault < 3; fault++) {
      page_handle page;
      ASSERT_TRUE(SUCCESS(cache_get(cc, root, TRUE, PAGE_TYPE_BRANCH, &page)));
      ASSERT_TRUE(cache_try_claim(cc, &page));
      cache_lock(cc, &page);
      btree_hdr *hdr = (btree_hdr *)page_handle_data(&page);
      ASSERT_TRUE(hdr->height > 0);
      index_entry *entry      = btree_get_index_entry(cfg, hdr, 0);
      uint8        flags      = entry->pivot.flags;
      uint16       key_length = entry->pivot.length;
      ASSERT_EQUAL(0, flags);
      ASSERT_FALSE(flags & BTREE_INDEX_WIDE_STATS);
      ASSERT_EQUAL(cfg->page_size, hdr->offsets[0] + sizeof_index_entry(entry));
      if (fault < 2) {
         // Widening this entry makes its counters extend beyond the page.
         entry->pivot.flags |= fault == 0 ? BTREE_INDEX_WIDE_STATS : 0x80;
      } else {
         /* A longer key pushes its narrow counters past the page. The custom
          * ordering's 400-byte first key still takes the ordinary-entry fast
          * path; the lexical 512-byte key exercises the maximum-key check. */
         entry->pivot.length++;
      }
      cache_unlock(cc, &page);
      cache_unclaim(cc, &page);
      cache_unget(cc, &page);

      for (uint64 async = 0; async < 2; async++) {
         platform_status rc =
            async
               ? async_call_sync_callback(cache_cleanup(cc),
                                          btree_lookup_async,
                                          cc,
                                          cfg,
                                          root,
                                          format,
                                          PAGE_TYPE_BRANCH,
                                          target,
                                          &result)
               : btree_lookup(
                    cc, cfg, root, format, PAGE_TYPE_BRANCH, target, &result);
         ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_INVALID_STATE));
      }

      ASSERT_TRUE(SUCCESS(cache_get(cc, root, TRUE, PAGE_TYPE_BRANCH, &page)));
      ASSERT_TRUE(cache_try_claim(cc, &page));
      cache_lock(cc, &page);
      hdr                 = (btree_hdr *)page_handle_data(&page);
      entry               = pointer_byte_offset(hdr, hdr->offsets[0]);
      entry->pivot.flags  = flags;
      entry->pivot.length = key_length;
      cache_unlock(cc, &page);
      cache_unclaim(cc, &page);
      cache_unget(cc, &page);
   }
   lookup_result_deinit(&result);
}

/* Mutation searches may trust memtable entries, but key-blob reads can still
 * fail. Exercise both a root leaf and a tree with internal pivots; subsequent
 * inserts and the full-tree verification also check that locks were released.
 */
static void
native_check_insert_read_failure(struct ctest_btree_stress_data *data,
                                 btree_config                   *cfg,
                                 mini_allocator                 *mini,
                                 uint64                          root)
{
   cache    *cc        = (cache *)&data->cc;
   cache_ops fault_ops = *cc->ops;
   native_saved_ops    = cc->ops;
   fault_ops.page_get  = native_fail_blob_read;
   native_failed_reads = 0;
   char buf[12000];
   key  target = native_test_key(71, buf);
   /* Keep the incoming key inline so any blob read comes from comparison,
    * rather than constructing a new key blob. Its ordinal sorts among blobs. */
   target                     = key_create(FALSE, 512, key_data(target));
   uint64               value = 0;
   btree_insert_results results;
   btree_insert_results_init(&results, NULL);
   cc->ops            = &fault_ops;
   platform_status rc = btree_insert(
      cc,
      cfg,
      data->hid,
      data->test_scratch,
      root,
      mini,
      target,
      message_create_internal(
         MESSAGE_TYPE_INSERT, NULL, slice_create(sizeof(value), &value)),
      &results);
   cc->ops = native_saved_ops;
   btree_insert_results_deinit(&results);
   ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_TIMEDOUT));
   ASSERT_TRUE(native_failed_reads > 0);
}

static void
native_blob_keys_test(struct ctest_btree_stress_data *data,
                      bool32                          reverse,
                      uint64                          prefix)
{
   cache       *cc           = (cache *)&data->cc;
   btree_config cfg          = data->dbtree_cfg;
   data_config  dc           = *cfg.data_cfg;
   dc.key_compare_is_lexical = !reverse;
   if (reverse) {
      dc.key_compare = native_reverse_compare;
   }
   cfg.data_cfg             = &dc;
   cfg.blob_key_prefix_size = prefix;
   dc.merge_tuples          = native_merge;
   mini_allocator mini;
   uint64         root = test_btree_create(cc, &cfg, &mini, PAGE_TYPE_MEMTABLE);
   char           keybuf[12000];
   uint64         total_bytes = 0;
   for (uint64 i = 0; i < NATIVE_KEY_COUNT; i++) {
      /* Permute insertion order to exercise both sides of splits. */
      uint64 n = (i * 73) % NATIVE_KEY_COUNT;
      key    k = native_test_key(n, keybuf);
      total_bytes += key_length(k);
      uint64               value = n;
      btree_insert_results results;
      btree_insert_results_init(&results, NULL);
      platform_status rc = btree_insert(
         cc,
         &cfg,
         data->hid,
         data->test_scratch,
         root,
         &mini,
         k,
         message_create_internal(
            MESSAGE_TYPE_INSERT, NULL, slice_create(sizeof(value), &value)),
         &results);
      btree_insert_results_deinit(&results);
      ASSERT_TRUE(SUCCESS(rc), "insert %lu: %d", n, rc.r);
      if (i == 1) {
         native_check_insert_read_failure(data, &cfg, &mini, root);
      }
   }
   native_check_insert_read_failure(data, &cfg, &mini, root);
   /* Update callbacks receive materialized keys; old-result ownership survives
    * the operation-local B-tree configuration. */
   for (uint64 n = 5; n < NATIVE_KEY_COUNT; n += 31) {
      lookup_result old;
      lookup_result_init(&old, &dc, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
      btree_insert_results results;
      btree_insert_results_init(&results, &old);
      uint64 increment = 1;
      ASSERT_TRUE(SUCCESS(btree_insert(
         cc,
         &cfg,
         data->hid,
         data->test_scratch,
         root,
         &mini,
         native_test_key(n, keybuf),
         message_create_internal(
            MESSAGE_TYPE_UPDATE, NULL, slice_create(8, &increment)),
         &results)));
      uint64 old_value;
      memcpy(&old_value, merge_accumulator_data(&old.value), sizeof(old_value));
      ASSERT_EQUAL(n, old_value);
      btree_insert_results_deinit(&results);
      lookup_result_deinit(&old);
   }
   native_check_read_failure(
      cc, &cfg, root, (btree_format){0}, PAGE_TYPE_MEMTABLE);
   native_check_tree(
      cc, &cfg, root, (btree_format){0}, PAGE_TYPE_MEMTABLE, reverse);
   btree_iterator source;
   ASSERT_TRUE(SUCCESS(btree_iterator_init(cc,
                                           &cfg,
                                           &source,
                                           root,
                                           (btree_format){0},
                                           PAGE_TYPE_MEMTABLE,
                                           &(btree_iterator_init_options){0})));
   btree_pack_req req;
   ASSERT_TRUE(SUCCESS(btree_pack_req_init(
      &req, cc, &cfg, &source.super, NATIVE_KEY_COUNT, data->hid)));
   ASSERT_TRUE(SUCCESS(btree_pack(&req)));
   uint64       packed_root = req.root_addr;
   btree_format format      = req.format;
   ASSERT_EQUAL(reverse ? 0 : prefix, format.key_prefix_size);
   ASSERT_EQUAL(total_bytes, req.key_bytes);
   btree_pack_req_deinit(&req, data->hid);
   btree_iterator_deinit(&source);
   mini_release(&mini);
   btree_dec_ref(cc, &cfg, root, PAGE_TYPE_MEMTABLE);
   ASSERT_TRUE(btree_verify_tree(cc, &cfg, packed_root, PAGE_TYPE_BRANCH));

   /* Read with a different construction policy; the branch's persisted P wins.
    */
   cfg.blob_key_prefix_size = 64;
   native_check_corrupt_index(
      cc,
      &cfg,
      packed_root,
      format,
      native_test_key(reverse ? NATIVE_KEY_COUNT - 1 : 0, keybuf));
   native_check_corrupt_leaf(cc, &cfg, packed_root, format);
   native_check_read_failure(cc, &cfg, packed_root, format, PAGE_TYPE_BRANCH);
   native_check_tree(cc, &cfg, packed_root, format, PAGE_TYPE_BRANCH, reverse);
   btree_pivot_stats stats;
   ASSERT_TRUE(SUCCESS(btree_count_in_range(cc,
                                            &cfg,
                                            packed_root,
                                            format,
                                            NEGATIVE_INFINITY_KEY,
                                            POSITIVE_INFINITY_KEY,
                                            &stats)));
   ASSERT_EQUAL(NATIVE_KEY_COUNT, stats.num_kvs);
   ASSERT_EQUAL(total_bytes, stats.key_bytes);
   ASSERT_EQUAL(NATIVE_KEY_COUNT * sizeof(uint64), stats.message_bytes);
   ASSERT_TRUE(SUCCESS(btree_iterator_init(cc,
                                           &cfg,
                                           &source,
                                           packed_root,
                                           format,
                                           PAGE_TYPE_BRANCH,
                                           &(btree_iterator_init_options){0})));
   bool32 saw_wide_prefix = FALSE, saw_blob_prefix = FALSE;
   bool32 saw_blob = FALSE, saw_wide_prefix_after_blob = FALSE;
   for (uint64 i = 0; i < NATIVE_KEY_COUNT; i++) {
      const leaf_entry *entry =
         btree_get_leaf_entry(source.cfg, source.curr.hdr, source.idx);
      key     logical;
      message msg;
      iterator_curr(&source.super, &logical, &msg);
      bool32 blobbed = key_length(logical) > MAX_INLINE_KEY_SIZE(cfg.page_size);
      ASSERT_EQUAL(blobbed ? ONDISK_KEY_IS_BLOB : 0, entry->key_flags);
      if (blobbed) {
         saw_blob = TRUE;
         ASSERT_TRUE(source.curr.hdr->prefix_length <= format.key_prefix_size);
         uint64 suffix =
            format.key_prefix_size - source.curr.hdr->prefix_length;
         slice descriptor = slice_create(entry->key_length - suffix,
                                         entry->key_and_message + suffix);
         ASSERT_EQUAL(key_length(logical), blob_length(descriptor));
         ASSERT_TRUE(slice_length(descriptor) <= 256);
         saw_blob_prefix |=
            source.curr.hdr->prefix_length == format.key_prefix_size;
      } else {
         saw_wide_prefix |=
            source.curr.hdr->prefix_length > format.key_prefix_size;
         saw_wide_prefix_after_blob |=
            saw_blob && source.curr.hdr->prefix_length > format.key_prefix_size;
      }
      ASSERT_TRUE(SUCCESS(iterator_next(&source.super)));
   }
   if (!reverse) {
      ASSERT_TRUE(saw_wide_prefix);
      ASSERT_TRUE(saw_blob_prefix);
      ASSERT_TRUE(saw_wide_prefix_after_blob);
   }
   ASSERT_TRUE(SUCCESS(iterator_seek(
      &source.super, greater_than_or_equal, NEGATIVE_INFINITY_KEY)));
   ASSERT_TRUE(SUCCESS(btree_pack_req_init(
      &req, cc, &cfg, &source.super, NATIVE_KEY_COUNT, data->hid)));
   ASSERT_TRUE(SUCCESS(btree_pack(&req)));
   uint64       repacked_root   = req.root_addr;
   btree_format repacked_format = req.format;
   ASSERT_EQUAL(reverse ? 0 : 64, repacked_format.key_prefix_size);
   btree_pack_req_deinit(&req, data->hid);
   btree_iterator_deinit(&source);
   btree_dec_ref(cc, &cfg, packed_root, PAGE_TYPE_BRANCH);
   native_check_tree(
      cc, &cfg, repacked_root, repacked_format, PAGE_TYPE_BRANCH, reverse);
   btree_dec_ref(cc, &cfg, repacked_root, PAGE_TYPE_BRANCH);
}

CTEST2(btree_stress, native_blob_keys_lexical)
{
   native_blob_keys_test(data, FALSE, 256);
}

CTEST2(btree_stress, native_blob_keys_custom_order)
{
   native_blob_keys_test(data, TRUE, 256);
}

CTEST2(btree_stress, native_blob_keys_without_prefix)
{
   native_blob_keys_test(data, FALSE, 0);
}
