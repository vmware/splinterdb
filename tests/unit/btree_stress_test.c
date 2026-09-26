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

   uint64 root_addr = btree_create(
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

   uint64 root_addr = btree_create(
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
   uint64         root_addr = btree_create(
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
   return message_create(MESSAGE_TYPE_INSERT,
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

   platform_status rc = btree_iterator_init(cc,
                                            cfg,
                                            &dbiter,
                                            root_addr,
                                            type,
                                            greater_than_or_equal,
                                            NEGATIVE_INFINITY_KEY,
                                            less_than,
                                            POSITIVE_INFINITY_KEY,
                                            greater_than_or_equal,
                                            start_key,
                                            FALSE,
                                            0,
                                            0);
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

   platform_status rc = btree_iterator_init(cc,
                                            cfg,
                                            &dbiter,
                                            root_addr,
                                            PAGE_TYPE_MEMTABLE,
                                            greater_than_or_equal,
                                            NEGATIVE_INFINITY_KEY,
                                            less_than,
                                            POSITIVE_INFINITY_KEY,
                                            greater_than_or_equal,
                                            start_key,
                                            FALSE,
                                            0,
                                            0);
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

   platform_status rc = btree_iterator_init(cc,
                                            cfg,
                                            &dbiter,
                                            root_addr,
                                            PAGE_TYPE_MEMTABLE,
                                            greater_than_or_equal,
                                            NEGATIVE_INFINITY_KEY,
                                            less_than,
                                            POSITIVE_INFINITY_KEY,
                                            greater_than_or_equal,
                                            NEGATIVE_INFINITY_KEY,
                                            FALSE,
                                            0,
                                            0);
   ASSERT_TRUE(SUCCESS(rc));

   rc = STATUS_TEST_FAILED;
   btree_pack_req req;
   rc = btree_pack_req_init(&req, cc, cfg, iter, nkvs, 0, FALSE, hid);
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
      message_create(
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
   comparison start_cmp = reverse ? hi_cmp : lo_cmp;
   key        start     = reverse ? hi : lo;
   if (!async) {
      return btree_iterator_init(cc,
                                 cfg,
                                 itor,
                                 root,
                                 type,
                                 lo_cmp,
                                 lo,
                                 hi_cmp,
                                 hi,
                                 start_cmp,
                                 start,
                                 TRUE,
                                 0,
                                 prefetch_lookahead);
   }
   btree_iterator_async_state state;
   bool32                     ready = FALSE;
   btree_iterator_async_state_init(&state,
                                   cc,
                                   cfg,
                                   itor,
                                   root,
                                   type,
                                   lo_cmp,
                                   lo,
                                   hi_cmp,
                                   hi,
                                   start_cmp,
                                   start,
                                   TRUE,
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

CTEST2(btree_stress, copied_iterator_bounds)
{
   cache         *cc  = (cache *)&data->cc;
   btree_config  *cfg = &data->dbtree_cfg;
   mini_allocator mini;
   uint64         root = btree_create(cc, cfg, &mini, PAGE_TYPE_MEMTABLE);
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

static page_handle *
bounds_get(cache *cc, uint64 addr, bool32 blocking, page_type type)
{
   bounds_observe_get(addr);
   page_handle *p =
      bounds_observer->original->page_get(cc, addr, blocking, type);
   if (p && p->disk_addr == bounds_observer->leaf)
      bounds_observer->leaf_refs++;
   return p;
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

static page_handle *
bounds_async_result(void *payload)
{
   page_handle *p = bounds_observer->original->page_get_async_result(payload);
   if (p && p->disk_addr == bounds_observer->leaf)
      bounds_observer->leaf_refs++;
   return p;
}

static void
bounds_unget(cache *cc, page_handle *p)
{
   bounds_cache_observer *o       = bounds_observer;
   bool32                 is_leaf = p->disk_addr == o->leaf;
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
   p        = cache_get(cc, o->leaf, TRUE, PAGE_TYPE_MEMTABLE);
   o->split = ((btree_hdr *)p->data)->next_addr != o->old_next;
   cache_unget(cc, p);
   cc->ops = &o->ops;
}

CTEST2(btree_stress, copied_iterator_split_on_release)
{
   cache        *cc  = (cache *)&data->cc;
   btree_config *cfg = &data->dbtree_cfg;
   for (unsigned async = 0; async < 2; async++) {
      mini_allocator mini;
      uint64         root = btree_create(cc, cfg, &mini, PAGE_TYPE_MEMTABLE);
      for (uint64 n = 0; n < 2048; n += 2) {
         bounds_insert(cc, cfg, data->hid, data->test_scratch, &mini, root, n);
      }
      btree_iterator  probe;
      platform_status rc = btree_iterator_init(cc,
                                               cfg,
                                               &probe,
                                               root,
                                               PAGE_TYPE_MEMTABLE,
                                               greater_than_or_equal,
                                               NEGATIVE_INFINITY_KEY,
                                               less_than,
                                               POSITIVE_INFINITY_KEY,
                                               greater_than_or_equal,
                                               NEGATIVE_INFINITY_KEY,
                                               FALSE,
                                               0,
                                               0);
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
      bool32 copy_released = itor.curr.page == NULL;
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
   uint64         root  = btree_create(cc, cfg, &mini, PAGE_TYPE_MEMTABLE);
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
   rc = btree_pack_req_init(
      &req, cc, cfg, &source.super, count, 0, FALSE, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc                 = btree_pack(&req);
   uint64 packed_root = req.root_addr;
   btree_pack_req_deinit(&req, data->hid);
   btree_iterator_deinit(&source);
   ASSERT_TRUE(SUCCESS(rc));

   // Memtables have no extent-prefetch links. Exercise copied packed leaves
   // to verify both the initialization and leaf-advance prefetch guards.
   const cache_ops *original = cc->ops;
   cache_ops        observed = *original;
   bounds_original_prefetch  = original->page_prefetch;
   observed.page_prefetch    = bounds_count_prefetch;
   for (unsigned async = 0; async < 2; async++) {
      for (unsigned prefetch = 0; prefetch < 2; prefetch++) {
         for (unsigned long_range = 0; long_range < 2; long_range++) {
            uint64         limit = long_range ? count : 1;
            uint64         hi_buf;
            btree_iterator itor;
            bounds_prefetch_count = 0;
            cc->ops               = &observed;
            rc                    = bounds_init(cc,
                             cfg,
                             &itor,
                             packed_root,
                             NEGATIVE_INFINITY_KEY,
                             greater_than_or_equal,
                             bounds_key(limit, &hi_buf),
                             less_than,
                             FALSE,
                             async,
                             PAGE_TYPE_BRANCH,
                             prefetch);
            cc->ops               = original;
            ASSERT_TRUE(SUCCESS(rc));
            uint64 init_prefetches = bounds_prefetch_count;
            ASSERT_EQUAL(prefetch && long_range ? 1 : 0, init_prefetches);
            for (uint64 n = 0; n < limit; n++) {
               ASSERT_TRUE(iterator_can_curr(&itor.super));
               key     k;
               message msg;
               iterator_curr(&itor.super, &k, &msg);
               ASSERT_EQUAL(n, bounds_key_number(k));
               cc->ops = &observed;
               rc      = iterator_next(&itor.super);
               cc->ops = original;
               ASSERT_TRUE(SUCCESS(rc));
            }
            ASSERT_FALSE(iterator_can_curr(&itor.super));
            btree_iterator_deinit(&itor);
            if (prefetch && long_range) {
               ASSERT_TRUE(bounds_prefetch_count > init_prefetches);
            } else {
               ASSERT_EQUAL(0, bounds_prefetch_count);
            }
         }
      }
   }
}
