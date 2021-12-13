// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * variable_length_btree_test.c --
 *
 *  Exercises the BTree interfaces in variable_length_btree_test.c, and related
 *  files. Validates correctness of variable key-value size support in BTree.
 * -----------------------------------------------------------------------------
 */
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

// Function Prototypes
static int
init_data_config_from_master_config(data_config *  data_cfg,
                                    master_config *master_cfg);

static int
init_io_config_from_master_config(io_config *io_cfg, master_config *master_cfg);


static int
init_rc_allocator_config_from_master_config(rc_allocator_config *allocator_cfg,
                                            master_config *      master_cfg);

static int
init_clockcache_config_from_master_config(clockcache_config *cache_cfg,
                                          master_config *    master_cfg);

static int
init_variable_length_btree_config_from_master_config(
   variable_length_btree_config *dbtree_cfg,
   master_config *               master_cfg,
   data_config *                 data_cfg);

static int
leaf_hdr_tests(variable_length_btree_config * cfg,
               variable_length_btree_scratch *scratch);

static int
leaf_hdr_search_tests(variable_length_btree_config * cfg,
                      variable_length_btree_scratch *scratch);
static int
index_hdr_tests(variable_length_btree_config * cfg,
                variable_length_btree_scratch *scratch);

static int
index_hdr_search_tests(variable_length_btree_config *cfg);

static int
leaf_split_tests(variable_length_btree_config * cfg,
                 variable_length_btree_scratch *scratch,
                 int                            nkvs);

/*
 * Global data declaration macro:
 */
CTEST_DATA(variable_length_btree)
{
   master_config                 master_cfg;
   data_config                   data_cfg;
   io_config                     io_cfg;
   rc_allocator_config           allocator_cfg;
   clockcache_config             cache_cfg;
   variable_length_btree_scratch test_scratch;
   variable_length_btree_config  dbtree_cfg;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(variable_length_btree)
{
   config_set_defaults(&data->master_cfg);
   data->data_cfg = test_data_config;

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
   platform_heap_handle hh;
   platform_heap_id     hid;
   bool                 rv = FALSE;
   rv                      = SUCCESS(
      platform_heap_create(platform_get_module_id(), 1 * GiB, &hh, &hid));
   if (!rv) {
      platform_log("Failed to init heap\n");
      ASSERT_TRUE(rv);
   }

   platform_io_handle io;
   uint8              num_bg_threads[NUM_TASK_TYPES] = {0};
   task_system *      ts;
   rc_allocator       al;
   clockcache         cc;

   if (!SUCCESS(io_handle_init(&io, &data->io_cfg, hh, hid))
       || !SUCCESS(task_system_create(hid,
                                      &io,
                                      &ts,
                                      data->master_cfg.use_stats,
                                      FALSE,
                                      num_bg_threads,
                                      sizeof(variable_length_btree_scratch)))
       || !SUCCESS(rc_allocator_init(&al,
                                     &data->allocator_cfg,
                                     (io_handle *)&io,
                                     hh,
                                     hid,
                                     platform_get_module_id()))
       || !SUCCESS(clockcache_init(&cc,
                                   &data->cache_cfg,
                                   (io_handle *)&io,
                                   (allocator *)&al,
                                   "test",
                                   ts,
                                   hh,
                                   hid,
                                   platform_get_module_id())))
   {
      platform_log(
         "Failed to init io or task system or rc_allocator or clockcache\n");
      ASSERT_TRUE(FALSE);
   }
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(variable_length_btree) {}

/*
 * Test leaf_hdr APIs.
 */
CTEST2(variable_length_btree, test_leaf_hdr)
{
   int rc = leaf_hdr_tests(&data->dbtree_cfg, &data->test_scratch);
   ASSERT_EQUAL(0, rc);
}

/*
 * Test leaf_hdr search APIs.
 */
CTEST2(variable_length_btree, test_leaf_hdr_search)
{
   int rc = leaf_hdr_search_tests(&data->dbtree_cfg, &data->test_scratch);
   ASSERT_EQUAL(0, rc);
}

/*
 * Test index_hdr APIs.
 */
CTEST2(variable_length_btree, test_index_hdr)
{
   int rc = index_hdr_tests(&data->dbtree_cfg, &data->test_scratch);
   ASSERT_EQUAL(0, rc);
}

/*
 * Test index_hdr search APIs.
 */
CTEST2(variable_length_btree, test_index_hdr_search)
{
   int rc = index_hdr_search_tests(&data->dbtree_cfg);
   ASSERT_EQUAL(0, rc);
}

/*
 * Test leaf-split code
 */
CTEST2(variable_length_btree, test_leaf_split)
{
   for (int nkvs = 2; nkvs < 100; nkvs++) {
      int rc = leaf_split_tests(&data->dbtree_cfg, &data->test_scratch, nkvs);
      ASSERT_EQUAL(0, rc);
   }
}


/*
 * *****************************************************************
 * Helper functions, and actual test-case methods.
 * *****************************************************************
 */
static int
init_data_config_from_master_config(data_config *  data_cfg,
                                    master_config *master_cfg)
{
   data_cfg->key_size     = master_cfg->key_size;
   data_cfg->message_size = master_cfg->message_size;
   return 1;
}

static int
init_io_config_from_master_config(io_config *io_cfg, master_config *master_cfg)
{
   io_config_init(io_cfg,
                  master_cfg->page_size,
                  master_cfg->extent_size,
                  master_cfg->io_flags,
                  master_cfg->io_perms,
                  master_cfg->io_async_queue_depth,
                  master_cfg->io_filename);
   return 1;
}

static int
init_rc_allocator_config_from_master_config(rc_allocator_config *allocator_cfg,
                                            master_config *      master_cfg)
{
   rc_allocator_config_init(allocator_cfg,
                            master_cfg->page_size,
                            master_cfg->extent_size,
                            master_cfg->allocator_capacity);
   return 1;
}

static int
init_clockcache_config_from_master_config(clockcache_config *cache_cfg,
                                          master_config *    master_cfg)
{
   clockcache_config_init(cache_cfg,
                          master_cfg->page_size,
                          master_cfg->extent_size,
                          master_cfg->cache_capacity,
                          master_cfg->cache_logfile,
                          master_cfg->use_stats);
   return 1;
}

static int
init_variable_length_btree_config_from_master_config(
   variable_length_btree_config *dbtree_cfg,
   master_config *               master_cfg,
   data_config *                 data_cfg)
{
   variable_length_btree_config_init(dbtree_cfg,
                                     data_cfg,
                                     master_cfg->btree_rough_count_height,
                                     master_cfg->page_size,
                                     master_cfg->extent_size);
   return 1;
}

/*
 * Returns 0, when all checks pass. Otherwise, will assert with a
 * message to platform log file.
 */
static int
leaf_hdr_tests(variable_length_btree_config * cfg,
               variable_length_btree_scratch *scratch)
{
   char                       leaf_buffer[cfg->page_size];
   variable_length_btree_hdr *hdr  = (variable_length_btree_hdr *)leaf_buffer;
   int                        nkvs = 240;

   variable_length_btree_init_hdr(cfg, hdr);

   bool rv = FALSE;
   for (uint32 i = 0; i < nkvs; i++) {
      rv =
         variable_length_btree_set_leaf_entry(cfg,
                                              hdr,
                                              i,
                                              slice_create(i % sizeof(i), &i),
                                              slice_create(i % sizeof(i), &i));
      if (!rv) {
         platform_log(
            "[%s:%d] failed to insert 4-byte %d\n", __FILE__, __LINE__, i);
         ASSERT_TRUE(rv);
      }
   }

   int cmp_rv = 0;
   for (uint32 i = 0; i < nkvs; i++) {
      slice key     = variable_length_btree_get_tuple_key(cfg, hdr, i);
      slice message = variable_length_btree_get_tuple_message(cfg, hdr, i);
      cmp_rv        = slice_lex_cmp(slice_create(i % sizeof(i), &i), key);
      if (cmp_rv) {
         platform_log("[%s:%d] bad 4-byte key %d\n", __FILE__, __LINE__, i);
         ASSERT_EQUAL(0, cmp_rv);
      }
      cmp_rv = slice_lex_cmp(slice_create(i % sizeof(i), &i), message);
      if (cmp_rv) {
         platform_log("[%s:%d] bad 4-byte message %d\n", __FILE__, __LINE__, i);
         ASSERT_EQUAL(0, cmp_rv);
      }
   }

   rv = FALSE;
   for (uint64 i = 0; i < nkvs; i++) {
      rv =
         variable_length_btree_set_leaf_entry(cfg,
                                              hdr,
                                              i,
                                              slice_create(i % sizeof(i), &i),
                                              slice_create(i % sizeof(i), &i));
      if (!rv) {
         platform_log(
            "[%s:%d] failed to insert 8-byte %ld\n", __FILE__, __LINE__, i);
         ASSERT_TRUE(rv);
      }
   }

   cmp_rv = 0;
   for (uint64 i = 0; i < nkvs; i++) {
      slice key     = variable_length_btree_get_tuple_key(cfg, hdr, i);
      slice message = variable_length_btree_get_tuple_message(cfg, hdr, i);
      cmp_rv        = slice_lex_cmp(slice_create(i % sizeof(i), &i), key);
      if (cmp_rv) {
         platform_log("[%s:%d] bad 4-byte key %ld\n", __FILE__, __LINE__, i);
         ASSERT_EQUAL(0, cmp_rv);
      }
      cmp_rv = slice_lex_cmp(slice_create(i % sizeof(i), &i), message);
      if (cmp_rv) {
         platform_log(
            "[%s:%d] bad 4-byte message %ld\n", __FILE__, __LINE__, i);
         ASSERT_EQUAL(0, cmp_rv);
      }
   }

   variable_length_btree_defragment_leaf(cfg, scratch, hdr, -1);

   for (uint64 i = 0; i < nkvs; i++) {
      slice key     = variable_length_btree_get_tuple_key(cfg, hdr, i);
      slice message = variable_length_btree_get_tuple_message(cfg, hdr, i);
      cmp_rv        = slice_lex_cmp(slice_create(i % sizeof(i), &i), key);
      if (cmp_rv) {
         platform_log("[%s:%d] bad 4-byte key %ld\n", __FILE__, __LINE__, i);
         ASSERT_EQUAL(0, cmp_rv);
      }
      cmp_rv = slice_lex_cmp(slice_create(i % sizeof(i), &i), message);
      if (cmp_rv) {
         platform_log(
            "[%s:%d] bad 4-byte message %ld\n", __FILE__, __LINE__, i);
         ASSERT_EQUAL(0, cmp_rv);
      }
   }

   return 0;
}

static int
leaf_hdr_search_tests(variable_length_btree_config * cfg,
                      variable_length_btree_scratch *scratch)
{
   char                       leaf_buffer[cfg->page_size];
   variable_length_btree_hdr *hdr  = (variable_length_btree_hdr *)leaf_buffer;
   int                        nkvs = 256;

   variable_length_btree_init_hdr(cfg, hdr);

   for (int i = 0; i < nkvs; i++) {
      uint64 generation;
      uint8  keybuf[1];
      uint8  messagebuf[8];
      keybuf[0]     = 17 * i;
      messagebuf[0] = i;

      slice key     = slice_create(1, &keybuf);
      slice message = slice_create(i % 8, messagebuf);

      leaf_incorporate_spec spec;
      bool result = variable_length_btree_leaf_incorporate_tuple(
         cfg, scratch, hdr, key, message, &spec, &generation);
      if (!result) {
         platform_log(
            "[%s:%d] couldn't incorporate kv pair %d\n", __FILE__, __LINE__, i);
         ASSERT_TRUE(result);
      }
      if (generation != i) {
         platform_log("[%s:%d] bad generation %d %lu\n",
                      __FILE__,
                      __LINE__,
                      i,
                      generation);
         ASSERT_EQUAL(i, generation);
      }
   }

   for (int i = 0; i < nkvs; i++) {
      slice key    = variable_length_btree_get_tuple_key(cfg, hdr, i);
      uint8 ui     = i;
      int   cmp_rv = slice_lex_cmp(slice_create(1, &ui), key);
      if (cmp_rv) {
         platform_log("[%s:%d] bad 4-byte key %d\n", __FILE__, __LINE__, i);
         ASSERT_EQUAL(0, cmp_rv);
      }
   }

   return 0;
}

static int
index_hdr_tests(variable_length_btree_config * cfg,
                variable_length_btree_scratch *scratch)
{
   char                       index_buffer[cfg->page_size];
   variable_length_btree_hdr *hdr  = (variable_length_btree_hdr *)index_buffer;
   int                        nkvs = 100;

   variable_length_btree_init_hdr(cfg, hdr);
   hdr->height = 1;

   for (uint32 i = 0; i < nkvs; i++) {
      if (!variable_length_btree_set_index_entry(
             cfg, hdr, i, slice_create(i % sizeof(i), &i), i, 0, 0, 0))
      {
         platform_log("failed to insert 4-byte %d\n", i);
      }
   }

   for (uint32 i = 0; i < nkvs; i++) {
      slice  key       = variable_length_btree_get_pivot(cfg, hdr, i);
      uint64 childaddr = variable_length_btree_get_child_addr(cfg, hdr, i);
      if (slice_lex_cmp(slice_create(i % sizeof(i), &i), key)) {
         platform_log("bad 4-byte key %d\n", i);
      }
      if (childaddr != i) {
         platform_log("bad childaddr %d\n", i);
      }
   }

   for (uint64 i = 0; i < nkvs; i++) {
      if (!variable_length_btree_set_index_entry(
             cfg, hdr, i, slice_create(i % sizeof(i), &i), i, 0, 0, 0))
      {
         platform_log("failed to insert 8-byte %ld\n", i);
      }
   }

   for (uint64 i = 0; i < nkvs; i++) {
      slice  key       = variable_length_btree_get_pivot(cfg, hdr, i);
      uint64 childaddr = variable_length_btree_get_child_addr(cfg, hdr, i);
      if (slice_lex_cmp(slice_create(i % sizeof(i), &i), key)) {
         platform_log("bad 4-byte key %ld\n", i);
      }
      if (childaddr != i) {
         platform_log("bad childaddr %ld\n", i);
      }
   }

   variable_length_btree_defragment_index(cfg, scratch, hdr);

   for (uint64 i = 0; i < nkvs; i++) {
      slice  key       = variable_length_btree_get_pivot(cfg, hdr, i);
      uint64 childaddr = variable_length_btree_get_child_addr(cfg, hdr, i);
      if (slice_lex_cmp(slice_create(i % sizeof(i), &i), key)) {
         platform_log("bad 4-byte key %ld\n", i);
      }
      if (childaddr != i) {
         platform_log("bad childaddr %ld\n", i);
      }
   }

   return 0;
}

static int
index_hdr_search_tests(variable_length_btree_config *cfg)
{
   char                       index_buffer[cfg->page_size];
   variable_length_btree_hdr *hdr  = (variable_length_btree_hdr *)index_buffer;
   int                        nkvs = 100;

   variable_length_btree_init_hdr(cfg, hdr);
   hdr->height = 1;

   for (int i = 0; i < nkvs; i += 2) {
      uint8 keybuf[1];
      keybuf[0] = i;
      slice key = slice_create(1, &keybuf);
      if (!variable_length_btree_set_index_entry(
             cfg, hdr, i / 2, key, i, 0, 0, 0)) {
         platform_log("couldn't insert pivot %d\n", i);
      }
   }

   for (int i = 0; i < nkvs; i++) {
      bool  found;
      uint8 keybuf[1];
      keybuf[0] = i;
      slice key = slice_create(1, &keybuf);
      int64 idx = variable_length_btree_find_pivot(cfg, hdr, key, &found);
      if (idx != i / 2) {
         platform_log("bad pivot search result %ld for %d\n", idx, i);
      }
   }

   return 0;
}

static int
leaf_split_tests(variable_length_btree_config * cfg,
                 variable_length_btree_scratch *scratch,
                 int                            nkvs)
{
   char leaf_buffer[cfg->page_size];
   char msg_buffer[cfg->page_size];

   memset(msg_buffer, 0, sizeof(msg_buffer));

   variable_length_btree_hdr *hdr = (variable_length_btree_hdr *)leaf_buffer;

   variable_length_btree_init_hdr(cfg, hdr);

   int   msgsize = cfg->page_size / (nkvs + 1);
   slice msg     = slice_create(msgsize, msg_buffer);
   slice bigger_msg =
      slice_create(msgsize + sizeof(table_entry) + 1, msg_buffer);

   uint8 realnkvs = 0;
   while (realnkvs < nkvs) {
      uint8 keybuf[1];
      keybuf[0] = 2 * realnkvs + 1;
      if (!variable_length_btree_set_leaf_entry(
             cfg, hdr, realnkvs, slice_create(1, &keybuf), msg))
      {
         break;
      }
      realnkvs++;
   }

   for (uint8 i = 0; i < 2 * realnkvs + 1; i++) {
      uint64                generation;
      leaf_incorporate_spec spec;
      slice                 key = slice_create(1, &i);
      bool success              = variable_length_btree_leaf_incorporate_tuple(
         cfg, scratch, hdr, key, bigger_msg, &spec, &generation);
      if (success) {
         platform_log("Weird.  An incorporate that was supposed to fail "
                      "actually succeeded (nkvs=%d, realnkvs=%d, i=%d).\n",
                      nkvs,
                      realnkvs,
                      i);
         variable_length_btree_print_locked_node(
            cfg, 0, hdr, PLATFORM_ERR_LOG_HANDLE);
      }
      leaf_splitting_plan plan =
         variable_length_btree_build_leaf_splitting_plan(cfg, hdr, spec);
      platform_assert(realnkvs / 2 - 1 <= plan.split_idx);
      platform_assert(plan.split_idx <= realnkvs / 2 + 1);
   }

   return 0;
}
