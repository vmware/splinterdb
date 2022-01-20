// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * variable_length_btree_stress_test.c - Basic BTree multi-threaded stress test
 *
 * Exercises the BTree APIs, with larger data volumes, and multiple threads.
 * -----------------------------------------------------------------------------
 */
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

#include "splinterdb/platform_public.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

#include "../functional/test.h"
#include "splinterdb/data.h"
#include "config.h"
#include "io.h"
#include "rc_allocator.h"
#include "clockcache.h"
#include "variable_length_btree.h"
#include "btree_test_common.h"

typedef struct insert_thread_params {
   cache *                        cc;
   variable_length_btree_config * cfg;
   variable_length_btree_scratch *scratch;
   mini_allocator *               mini;
   uint64                         root_addr;
   int                            start;
   int                            end;
} insert_thread_params;

// Function Prototypes
static void
insert_thread(void *arg);

static void
insert_tests(cache *                        cc,
             variable_length_btree_config * cfg,
             variable_length_btree_scratch *scratch,
             mini_allocator *               mini,
             uint64                         root_addr,
             int                            start,
             int                            end);

static int
query_tests(cache *                       cc,
            variable_length_btree_config *cfg,
            uint64                        root_addr,
            int                           nkvs);

static int
iterator_tests(cache *                       cc,
               variable_length_btree_config *cfg,
               uint64                        root_addr,
               int                           nkvs);

static uint64
pack_tests(cache *                       cc,
           variable_length_btree_config *cfg,
           platform_heap_id              hid,
           uint64                        root_addr,
           uint64                        nkvs);

static slice
gen_key(variable_length_btree_config *cfg,
        uint64                        i,
        uint8                         buffer[static cfg->page_size]);

static uint64
ungen_key(slice key);

static slice
gen_msg(variable_length_btree_config *cfg,
        uint64                        i,
        uint8                         buffer[static cfg->page_size]);

/*
 * Global data declaration macro:
 */
CTEST_DATA(variable_length_btree_stress)
{
   // This part of the data structures is common to what we need
   // to set up a Splinter instance, as is done in
   // variable_length_btree_test.c
   master_config                 master_cfg;
   data_config                   data_cfg;
   io_config                     io_cfg;
   rc_allocator_config           allocator_cfg;
   clockcache_config             cache_cfg;
   variable_length_btree_scratch test_scratch;
   variable_length_btree_config  dbtree_cfg;

   // To create a heap for io, allocator, cache and splinter
   platform_heap_handle hh;
   platform_heap_id     hid;

   // Stuff needed to setup and exercise multiple threads.
   platform_io_handle io;
   uint8              num_bg_threads[NUM_TASK_TYPES];
   task_system *      ts;
   rc_allocator       al;
   clockcache         cc;
};

// Setup function for suite, called before every test in suite
CTEST_SETUP(variable_length_btree_stress)
{
   config_set_defaults(&data->master_cfg);
   data->data_cfg = test_data_config;

   // RESOLVE: Sort this out with RobJ about cmd line args support
   // if (!SUCCESS(config_parse(&data->master_cfg, 1, argc - 1, argv + 1)) ||
   if (!SUCCESS(config_parse(&data->master_cfg, 1, 0, (char **)NULL))
       || !init_data_config_from_master_config(&data->data_cfg,
                                               &data->master_cfg)
       || !init_io_config_from_master_config(&data->io_cfg, &data->master_cfg)
       || !init_rc_allocator_config_from_master_config(&data->allocator_cfg,
                                                       &data->master_cfg)
       || !init_clockcache_config_from_master_config(&data->cache_cfg,
                                                     &data->master_cfg)
       || !init_variable_length_btree_config_from_master_config(
          &data->dbtree_cfg, &data->master_cfg, &data->data_cfg))
   {
      platform_log("Failed to parse args\n");
      ASSERT_TRUE(FALSE);
   }

   // Create a heap for io, allocator, cache and splinter
   if (!SUCCESS(platform_heap_create(
          platform_get_module_id(), 1 * GiB, &data->hh, &data->hid)))
   {
      platform_log("Failed to init heap\n");
      ASSERT_TRUE(FALSE);
   }
   // Setup execution of concurrent threads
   ZERO_ARRAY(data->num_bg_threads);
   if (!SUCCESS(io_handle_init(&data->io, &data->io_cfg, data->hh, data->hid))
       || !SUCCESS(task_system_create(data->hid,
                                      &data->io,
                                      &data->ts,
                                      data->master_cfg.use_stats,
                                      FALSE,
                                      data->num_bg_threads,
                                      sizeof(variable_length_btree_scratch)))
       || !SUCCESS(rc_allocator_init(&data->al,
                                     &data->allocator_cfg,
                                     (io_handle *)&data->io,
                                     data->hh,
                                     data->hid,
                                     platform_get_module_id()))
       || !SUCCESS(clockcache_init(&data->cc,
                                   &data->cache_cfg,
                                   (io_handle *)&data->io,
                                   (allocator *)&data->al,
                                   "test",
                                   data->ts,
                                   data->hh,
                                   data->hid,
                                   platform_get_module_id())))
   {
      platform_log(
         "Failed to init io or task system or rc_allocator or clockcache\n");
      ASSERT_TRUE(FALSE);
   }
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(variable_length_btree_stress) {}

/*
 * -------------------------------------------------------------------------
 * Test case to exercise random inserts of large volumes of data, across
 * multiple threads. This test case verifies that registration of threads
 * to Splinter is working stably.
 */
CTEST2(variable_length_btree_stress, test_random_inserts_concurrent)
{
   int nkvs     = 1000000;
   int nthreads = 8;

   mini_allocator mini;

   uint64 root_addr = variable_length_btree_create(
      (cache *)&data->cc, &data->dbtree_cfg, &mini, PAGE_TYPE_MEMTABLE);

   insert_thread_params params[nthreads];
   platform_thread      threads[nthreads];

   for (uint64 i = 0; i < nthreads; i++) {
      params[i].cc        = (cache *)&data->cc;
      params[i].cfg       = &data->dbtree_cfg;
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

   int rc = query_tests((cache *)&data->cc, &data->dbtree_cfg, root_addr, nkvs);
   if (!rc) {
      platform_log("invalid tree\n");
      ASSERT_NOT_EQUAL(0, rc);
   }

   if (!iterator_tests((cache *)&data->cc, &data->dbtree_cfg, root_addr, nkvs))
   {
      platform_log("invalid ranges in original tree\n");
   }

   /* platform_log("\n\n\n"); */
   /* variable_length_btree_print_tree((cache *)&cc, &dbtree_cfg, root_addr); */

   uint64 packed_root_addr = pack_tests(
      (cache *)&data->cc, &data->dbtree_cfg, data->hid, root_addr, nkvs);
   if (0 < nkvs && !packed_root_addr) {
      platform_log("[%s:%d] Pack failed\n", __FILE__, __LINE__);
      ASSERT_TRUE(FALSE);
   }

   /* platform_log("\n\n\n"); */
   /* variable_length_btree_print_tree((cache *)&cc, &dbtree_cfg,
    * packed_root_addr); */
   /* platform_log("\n\n\n"); */

   rc = query_tests(
      (cache *)&data->cc, &data->dbtree_cfg, packed_root_addr, nkvs);
   if (!rc) {
      platform_log("invalid tree\n");
      ASSERT_NOT_EQUAL(0, rc);
   }

   rc = iterator_tests(
      (cache *)&data->cc, &data->dbtree_cfg, packed_root_addr, nkvs);
   if (!rc) {
      platform_log("invalid ranges in packed tree\n");
      ASSERT_NOT_EQUAL(0, rc);
   }
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
                params->scratch,
                params->mini,
                params->root_addr,
                params->start,
                params->end);
}

static void
insert_tests(cache *                        cc,
             variable_length_btree_config * cfg,
             variable_length_btree_scratch *scratch,
             mini_allocator *               mini,
             uint64                         root_addr,
             int                            start,
             int                            end)
{
   uint64 generation;
   bool   was_unique;
   uint8  keybuf[cfg->page_size];
   uint8  msgbuf[cfg->page_size];

   for (uint64 i = start; i < end; i++) {
      if (!SUCCESS(variable_length_btree_insert(cc,
                                                cfg,
                                                scratch,
                                                root_addr,
                                                mini,
                                                gen_key(cfg, i, keybuf),
                                                gen_msg(cfg, i, msgbuf),
                                                &generation,
                                                &was_unique)))
      {
         platform_log(
            "[%s:%d] Failed to insert 4-byte %ld\n", __FILE__, __LINE__, i);
         ASSERT_TRUE(FALSE);
      }
   }
}

static slice
gen_key(variable_length_btree_config *cfg,
        uint64                        i,
        uint8                         buffer[static cfg->page_size])
{
   uint64 keylen = sizeof(i) + (i % 100);
   memset(buffer, 0, keylen);
   uint64 j = i * 23232323731ULL + 99382474567ULL;
   memcpy(buffer, &j, sizeof(j));
   return slice_create(keylen, buffer);
}

static uint64
ungen_key(slice key)
{
   if (slice_length(key) < sizeof(uint64)) {
      return 0;
   }

   uint64 k;
   memcpy(&k, key.data, sizeof(k));
   return (k - 99382474567ULL) * 14122572041603317147ULL;
}

static slice
gen_msg(variable_length_btree_config *cfg,
        uint64                        i,
        uint8                         buffer[static cfg->page_size])
{
   data_handle *dh      = (data_handle *)buffer;
   uint64       datalen = sizeof(i) + (i % (cfg->page_size / 3));

   dh->message_type = MESSAGE_TYPE_INSERT;
   dh->ref_count    = 1;
   memset(dh->data, 0, datalen);
   memcpy(dh->data, &i, sizeof(i));
   return slice_create(sizeof(data_handle) + datalen, buffer);
}

static int
query_tests(cache *                       cc,
            variable_length_btree_config *cfg,
            uint64                        root_addr,
            int                           nkvs)
{
   uint8 keybuf[cfg->page_size];
   uint8 msgbuf[cfg->page_size];

   memset(keybuf, 0, sizeof(keybuf));
   memset(msgbuf, 0, sizeof(msgbuf));

   slice msg = slice_create(0, msgbuf);
   for (uint64 i = 0; i < nkvs; i++) {
      bool found;
      variable_length_btree_lookup(cc,
                                   cfg,
                                   root_addr,
                                   gen_key(cfg, i, keybuf),
                                   &msg.length,
                                   msgbuf,
                                   &found);
      if (!found || slice_lex_cmp(msg, gen_msg(cfg, i, msgbuf))) {
         platform_log("[%s:%d] Failure on lookup %lu\n", __FILE__, __LINE__, i);
         variable_length_btree_print_tree(cc, cfg, root_addr);
         ASSERT_TRUE(FALSE);
      }
   }

   return 1;
}

static int
iterator_tests(cache *                       cc,
               variable_length_btree_config *cfg,
               uint64                        root_addr,
               int                           nkvs)
{
   variable_length_btree_iterator dbiter;

   variable_length_btree_iterator_init(cc,
                                       cfg,
                                       &dbiter,
                                       root_addr,
                                       PAGE_TYPE_MEMTABLE,
                                       NULL_SLICE,
                                       NULL_SLICE,
                                       FALSE,
                                       0);

   iterator *iter = (iterator *)&dbiter;

   uint64 seen = 0;
   bool   at_end;
   uint8  prevbuf[cfg->page_size];
   slice  prev = NULL_SLICE;

   while (SUCCESS(iterator_at_end(iter, &at_end)) && !at_end) {
      uint8 keybuf[cfg->page_size];
      uint8 msgbuf[cfg->page_size];
      slice key, msg;

      iterator_get_curr(iter, &key, &msg);
      uint64 k = ungen_key(key);
      ASSERT_TRUE(k < nkvs);

      int rc = 0;
      rc     = slice_lex_cmp(key, gen_key(cfg, k, keybuf));
      ASSERT_EQUAL(0, rc);

      rc = slice_lex_cmp(msg, gen_msg(cfg, k, msgbuf));
      ASSERT_EQUAL(0, rc);

      ASSERT_TRUE(slice_is_null(prev) || slice_lex_cmp(prev, key) < 0);

      seen++;
      prev.data = prevbuf;
      slice_copy_contents(prevbuf, key);
      prev.length = key.length;

      if (!SUCCESS(iterator_advance(iter))) {
         break;
      }
   }

   ASSERT_EQUAL(nkvs, seen);

   variable_length_btree_iterator_deinit(&dbiter);

   return 1;
}

static uint64
pack_tests(cache *                       cc,
           variable_length_btree_config *cfg,
           platform_heap_id              hid,
           uint64                        root_addr,
           uint64                        nkvs)
{
   variable_length_btree_iterator dbiter;
   iterator *                     iter = (iterator *)&dbiter;

   variable_length_btree_iterator_init(cc,
                                       cfg,
                                       &dbiter,
                                       root_addr,
                                       PAGE_TYPE_MEMTABLE,
                                       NULL_SLICE,
                                       NULL_SLICE,
                                       FALSE,
                                       0);

   variable_length_btree_pack_req req;
   variable_length_btree_pack_req_init(&req, cc, cfg, iter, nkvs, NULL, 0, hid);

   if (!SUCCESS(variable_length_btree_pack(&req))) {
      platform_log("[%s:%d] Pack failed!\n", __FILE__, __LINE__);
      ASSERT_TRUE(FALSE);
   } else {
      platform_log("Packed %lu items ", req.num_tuples);
   }

   variable_length_btree_pack_req_deinit(&req, hid);

   return req.root_addr;
}
