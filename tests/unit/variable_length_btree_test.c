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
   if (!SUCCESS(config_parse(&data->master_cfg, 1, 0, (char **) NULL)) ||
       !init_data_config_from_master_config(&data->data_cfg, &data->master_cfg) ||
       !init_io_config_from_master_config(&data->io_cfg, &data->master_cfg) ||
       !init_rc_allocator_config_from_master_config(&data->allocator_cfg,
                                                    &data->master_cfg) ||
       !init_clockcache_config_from_master_config(&data->cache_cfg, &data->master_cfg) ||
       !init_variable_length_btree_config_from_master_config(
          &data->dbtree_cfg, &data->master_cfg, &data->data_cfg)) {
      platform_log("Failed to parse args\n");
      ASSERT_TRUE(FALSE);
   }

   // Create a heap for io, allocator, cache and splinter
   platform_heap_handle hh;
   platform_heap_id     hid;
   if (!SUCCESS(
          platform_heap_create(platform_get_module_id(), 1 * GiB, &hh, &hid))) {
      platform_log("Failed to init heap\n");
      // return -3;
      ASSERT_TRUE(FALSE);
   }

   platform_io_handle io;
   uint8              num_bg_threads[NUM_TASK_TYPES] = {0};
   task_system *      ts;
   rc_allocator       al;
   clockcache         cc;

   if (!SUCCESS(io_handle_init(&io, &data->io_cfg, hh, hid)) ||
       !SUCCESS(task_system_create(hid,
                                   &io,
                                   &ts,
                                   data->master_cfg.use_stats,
                                   FALSE,
                                   num_bg_threads,
                                   sizeof(variable_length_btree_scratch))) ||
       !SUCCESS(rc_allocator_init(&al,
                                  &data->allocator_cfg,
                                  (io_handle *)&io,
                                  hh,
                                  hid,
                                  platform_get_module_id())) ||
       !SUCCESS(clockcache_init(&cc,
                                &data->cache_cfg,
                                (io_handle *)&io,
                                (allocator *)&al,
                                "test",
                                ts,
                                hh,
                                hid,
                                platform_get_module_id()))) {
      platform_log(
         "Failed to init io or task system or rc_allocator or clockcache\n");
      // return -2;
      ASSERT_TRUE(FALSE);
   }
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(variable_length_btree)
{
}

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

static int
leaf_hdr_tests(variable_length_btree_config * cfg,
               variable_length_btree_scratch *scratch)
{
   char                       leaf_buffer[cfg->page_size];
   variable_length_btree_hdr *hdr  = (variable_length_btree_hdr *)leaf_buffer;
   int                        nkvs = 240;

   variable_length_btree_init_hdr(cfg, hdr);

   for (uint32 i = 0; i < nkvs; i++) {
      if (!variable_length_btree_set_leaf_entry(
             cfg,
             hdr,
             i,
             slice_create(i % sizeof(i), &i),
             slice_create(i % sizeof(i), &i))) {
         platform_log("failed to insert 4-byte %d\n", i);
      }
   }

   for (uint32 i = 0; i < nkvs; i++) {
      slice key     = variable_length_btree_get_tuple_key(cfg, hdr, i);
      slice message = variable_length_btree_get_tuple_message(cfg, hdr, i);
      if (slice_lex_cmp(slice_create(i % sizeof(i), &i), key)) {
         platform_log("bad 4-byte key %d\n", i);
      }
      if (slice_lex_cmp(slice_create(i % sizeof(i), &i), message)) {
         platform_log("bad 4-byte message %d\n", i);
      }
   }

   for (uint64 i = 0; i < nkvs; i++) {
      if (!variable_length_btree_set_leaf_entry(
             cfg,
             hdr,
             i,
             slice_create(i % sizeof(i), &i),
             slice_create(i % sizeof(i), &i))) {
         platform_log("failed to insert 8-byte %ld\n", i);
      }
   }

   for (uint64 i = 0; i < nkvs; i++) {
      slice key     = variable_length_btree_get_tuple_key(cfg, hdr, i);
      slice message = variable_length_btree_get_tuple_message(cfg, hdr, i);
      if (slice_lex_cmp(slice_create(i % sizeof(i), &i), key)) {
         platform_log("bad 4-byte key %ld\n", i);
      }
      if (slice_lex_cmp(slice_create(i % sizeof(i), &i), message)) {
         platform_log("bad 4-byte message %ld\n", i);
      }
   }

   variable_length_btree_defragment_leaf(cfg, scratch, hdr, -1);

   for (uint64 i = 0; i < nkvs; i++) {
      slice key     = variable_length_btree_get_tuple_key(cfg, hdr, i);
      slice message = variable_length_btree_get_tuple_message(cfg, hdr, i);
      if (slice_lex_cmp(slice_create(i % sizeof(i), &i), key)) {
         platform_log("bad 4-byte key %ld\n", i);
      }
      if (slice_lex_cmp(slice_create(i % sizeof(i), &i), message)) {
         platform_log("bad 4-byte message %ld\n", i);
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

      slice                 key     = slice_create(1, &keybuf);
      slice                 message = slice_create(i % 8, messagebuf);
      leaf_incorporate_spec spec;
      bool result = variable_length_btree_leaf_incorporate_tuple(
         cfg, scratch, hdr, key, message, &spec, &generation);
      if (!result) {
         platform_log("couldn't incorporate kv pair %d\n", i);
      }
      if (generation != i) {
         platform_log("bad generation %d %lu\n", i, generation);
      }
   }

   for (int i = 0; i < nkvs; i++) {
      slice key = variable_length_btree_get_tuple_key(cfg, hdr, i);
      uint8 ui  = i;
      if (slice_lex_cmp(slice_create(1, &ui), key)) {
         platform_log("bad 4-byte key %d\n", i);
      }
   }

   return 0;
}
