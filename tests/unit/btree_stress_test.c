// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * btree_stress_test.c - Basic BTree multi-threaded stress test
 *
 * Exercises the BTree APIs, with larger data volumes, and multiple threads.
 * -----------------------------------------------------------------------------
 */
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

#include "splinterdb/public_platform.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

#include "functional/test.h"
#include "splinterdb/data.h"
#include "../config.h"
#include "io.h"
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
   btree_scratch      test_scratch;
   btree_config       dbtree_cfg;

   // To create a heap for io, allocator, cache and splinter
   platform_heap_handle hh;
   platform_heap_id     hid;

   // Stuff needed to setup and exercise multiple threads.
   platform_io_handle io;
   task_system       *ts;
   rc_allocator       al;
   clockcache         cc;
};

// Setup function for suite, called before every test in suite
CTEST_SETUP(btree_stress)
{
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
       || !init_task_config_from_master_config(
          &data->task_cfg, &data->master_cfg, sizeof(btree_scratch)))
   {
      ASSERT_TRUE(FALSE, "Failed to parse args\n");
   }

   // Create a heap for io, allocator, cache and splinter
   if (!SUCCESS(platform_heap_create(
          platform_get_module_id(), 1 * GiB, &data->hh, &data->hid)))
   {
      ASSERT_TRUE(FALSE, "Failed to init heap\n");
   }
   // Setup execution of concurrent threads
   data->ts = NULL;
   if (!SUCCESS(io_handle_init(&data->io, &data->io_cfg, data->hh, data->hid))
       || !SUCCESS(
          task_system_create(data->hid, &data->io, &data->ts, &data->task_cfg))
       || !SUCCESS(rc_allocator_init(&data->al,
                                     &data->allocator_cfg,
                                     (io_handle *)&data->io,
                                     data->hid,
                                     platform_get_module_id()))
       || !SUCCESS(clockcache_init(&data->cc,
                                   &data->cache_cfg,
                                   (io_handle *)&data->io,
                                   (allocator *)&data->al,
                                   "test",
                                   data->hid,
                                   platform_get_module_id())))
   {
      ASSERT_TRUE(
         FALSE,
         "Failed to init io or task system or rc_allocator or clockcache\n");
   }
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(btree_stress)
{
   clockcache_deinit(&data->cc);
   rc_allocator_deinit(&data->al);
   task_system_destroy(data->hid, &data->ts);
   platform_heap_destroy(&data->hh);
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

   platform_heap_id      hid     = platform_get_heap_id();
   insert_thread_params *params  = TYPED_ARRAY_ZALLOC(hid, params, nthreads);
   platform_thread      *threads = TYPED_ARRAY_ZALLOC(hid, threads, nthreads);

   for (uint64 i = 0; i < nthreads; i++) {
      params[i].cc        = (cache *)&data->cc;
      params[i].cfg       = &data->dbtree_cfg;
      params[i].hid       = data->hid;
      params[i].scratch   = TYPED_MALLOC(data->hid, params[i].scratch);
      params[i].mini      = &mini;
      params[i].root_addr = root_addr;
      params[i].start     = i * (nkvs / nthreads);
      params[i].end = i < nthreads - 1 ? (i + 1) * (nkvs / nthreads) : nkvs;
   }

   for (uint64 i = 0; i < nthreads; i++) {
      platform_status ret = task_thread_create("insert thread",
                                               insert_thread,
                                               &params[i],
                                               0,
                                               data->ts,
                                               data->hid,
                                               &threads[i]);
      ASSERT_TRUE(SUCCESS(ret));
      // insert_tests((cache *)&cc, &dbtree_cfg, &test_scratch, &mini,
      // root_addr, 0, nkvs);
   }

   for (uint64 thread_no = 0; thread_no < nthreads; thread_no++) {
      platform_thread_join(threads[thread_no]);
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
                       nkvs,
                       TRUE,
                       data->hid))
   {
      CTEST_ERR("invalid ranges in original tree, starting at front\n");
   }
   if (!iterator_tests((cache *)&data->cc,
                       &data->dbtree_cfg,
                       root_addr,
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
   uint64 generation;
   bool32 was_unique;

   int    keybuf_size = btree_page_size(cfg);
   int    msgbuf_size = btree_page_size(cfg);
   uint8 *keybuf      = TYPED_MANUAL_MALLOC(hid, keybuf, keybuf_size);
   uint8 *msgbuf      = TYPED_MANUAL_MALLOC(hid, msgbuf, msgbuf_size);

   for (uint64 i = start; i < end; i++) {
      if (!SUCCESS(btree_insert(cc,
                                cfg,
                                hid,
                                scratch,
                                root_addr,
                                mini,
                                gen_key(cfg, i, keybuf, keybuf_size),
                                gen_msg(cfg, i, msgbuf, msgbuf_size),
                                &generation,
                                &was_unique)))
      {
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
   return key_create(keylen, buffer);
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
   uint8 *keybuf = TYPED_MANUAL_MALLOC(hid, keybuf, btree_page_size(cfg));
   uint8 *msgbuf = TYPED_MANUAL_MALLOC(hid, msgbuf, btree_page_size(cfg));
   memset(msgbuf, 0, btree_page_size(cfg));

   merge_accumulator result;
   merge_accumulator_init(&result, hid);

   for (uint64 i = 0; i < nkvs; i++) {
      btree_lookup(cc,
                   cfg,
                   root_addr,
                   type,
                   gen_key(cfg, i, keybuf, btree_page_size(cfg)),
                   &result);
      if (!btree_found(&result)
          || message_lex_cmp(merge_accumulator_to_message(&result),
                             gen_msg(cfg, i, msgbuf, btree_page_size(cfg))))
      {
         ASSERT_TRUE(FALSE, "Failure on lookup %lu\n", i);
      }
   }

   merge_accumulator_deinit(&result);
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
   uint64 seen    = 0;
   uint8 *prevbuf = TYPED_MANUAL_MALLOC(hid, prevbuf, btree_page_size(cfg));
   key    prev    = NULL_KEY;
   uint8 *keybuf  = TYPED_MANUAL_MALLOC(hid, keybuf, btree_page_size(cfg));
   uint8 *msgbuf  = TYPED_MANUAL_MALLOC(hid, msgbuf, btree_page_size(cfg));

   while (iterator_can_curr(iter)) {
      key     curr_key;
      message msg;

      iterator_curr(iter, &curr_key, &msg);
      uint64 k = ungen_key(curr_key);
      ASSERT_TRUE(k < nkvs);

      int rc = 0;
      rc     = data_key_compare(cfg->data_cfg,
                            curr_key,
                            gen_key(cfg, k, keybuf, btree_page_size(cfg)));
      ASSERT_EQUAL(0, rc);

      rc = message_lex_cmp(msg, gen_msg(cfg, k, msgbuf, btree_page_size(cfg)));
      ASSERT_EQUAL(0, rc);

      if (forwards) {
         ASSERT_TRUE(key_is_null(prev)
                     || data_key_compare(cfg->data_cfg, prev, curr_key) < 0);
      } else {
         ASSERT_TRUE(key_is_null(prev)
                     || data_key_compare(cfg->data_cfg, curr_key, prev) < 0);
      }

      seen++;
      prev = key_create(key_length(curr_key), prevbuf);
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

   btree_iterator_init(cc,
                       cfg,
                       &dbiter,
                       root_addr,
                       PAGE_TYPE_MEMTABLE,
                       NEGATIVE_INFINITY_KEY,
                       POSITIVE_INFINITY_KEY,
                       start_key,
                       greater_than_or_equal,
                       FALSE,
                       0);

   iterator *iter = (iterator *)&dbiter;

   if (!start_front) {
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

   btree_iterator_init(cc,
                       cfg,
                       &dbiter,
                       root_addr,
                       PAGE_TYPE_MEMTABLE,
                       NEGATIVE_INFINITY_KEY,
                       POSITIVE_INFINITY_KEY,
                       start_key,
                       greater_than_or_equal,
                       FALSE,
                       0);
   iterator *iter = (iterator *)&dbiter;

   // go down
   uint64 found_down = iterator_test(hid, cfg, nkvs, iter, FALSE);

   // seek back to start_key
   iterator_seek(iter, start_key, TRUE);

   // skip start_key
   iterator_next(iter);

   // go up
   uint64 found_up = iterator_test(hid, cfg, nkvs, iter, TRUE);

   ASSERT_EQUAL(nkvs, found_up + found_down);

   btree_iterator_deinit(&dbiter);

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

   btree_iterator_init(cc,
                       cfg,
                       &dbiter,
                       root_addr,
                       PAGE_TYPE_MEMTABLE,
                       NEGATIVE_INFINITY_KEY,
                       POSITIVE_INFINITY_KEY,
                       NEGATIVE_INFINITY_KEY,
                       greater_than_or_equal,
                       FALSE,
                       0);

   btree_pack_req req;
   btree_pack_req_init(&req, cc, cfg, iter, nkvs, NULL, 0, hid);

   if (!SUCCESS(btree_pack(&req))) {
      ASSERT_TRUE(FALSE, "Pack failed! req.num_tuples = %d\n", req.num_tuples);
   } else {
      CTEST_LOG_INFO("Packed %lu items ", req.num_tuples);
   }

   btree_pack_req_deinit(&req, hid);

   return req.root_addr;
}
