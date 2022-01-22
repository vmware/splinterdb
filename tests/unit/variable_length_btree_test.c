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
#include "variable_length_btree_private.h"
#include "btree_test_common.h"

// Function Prototypes

static int
leaf_hdr_tests(variable_length_btree_config * cfg,
               variable_length_btree_scratch *scratch);

static int
leaf_hdr_search_tests(variable_length_btree_config *cfg, platform_heap_id hid);
static int
index_hdr_tests(variable_length_btree_config * cfg,
                variable_length_btree_scratch *scratch);

static int
index_hdr_search_tests(variable_length_btree_config *cfg);

static int
leaf_split_tests(variable_length_btree_config * cfg,
                 variable_length_btree_scratch *scratch,
                 int                            nkvs);

static bool
variable_length_btree_leaf_incorporate_tuple(
   const variable_length_btree_config *cfg,
   platform_heap_id                    hid,
   variable_length_btree_hdr *         hdr,
   slice                               key,
   slice                               message,
   leaf_incorporate_spec *             spec,
   uint64 *                            generation)
{
   bool r = variable_length_btree_create_leaf_incorporate_spec(
      cfg, hdr, hid, key, message, spec);
   ASSERT_TRUE(r);
   return variable_length_btree_perform_leaf_incorporate_spec(
      cfg, hdr, spec, generation);
}

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
leaf_hdr_search_tests(variable_length_btree_config *cfg, platform_heap_id hid)
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
         cfg, hid, hdr, key, message, &spec, &generation);
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

   bool rv     = FALSE;
   int  cmp_rv = 0;

   variable_length_btree_init_hdr(cfg, hdr);
   hdr->height = 1;

   for (uint32 i = 0; i < nkvs; i++) {
      rv = variable_length_btree_set_index_entry(
         cfg, hdr, i, slice_create(i % sizeof(i), &i), i, 0, 0, 0);
      if (!rv) {
         platform_log(
            "[%s:%d] failed to insert 4-byte %d\n", __FILE__, __LINE__, i);
         ASSERT_TRUE(rv);
      }
   }

   for (uint32 i = 0; i < nkvs; i++) {
      slice  key       = variable_length_btree_get_pivot(cfg, hdr, i);
      uint64 childaddr = variable_length_btree_get_child_addr(cfg, hdr, i);
      cmp_rv           = slice_lex_cmp(slice_create(i % sizeof(i), &i), key);
      if (cmp_rv) {
         platform_log("[%s:%d] bad 4-byte key %d\n", __FILE__, __LINE__, i);
         ASSERT_EQUAL(0, cmp_rv);
      }
      if (childaddr != i) {
         platform_log("[%s:%d] bad childaddr %d\n", __FILE__, __LINE__, i);
         ASSERT_EQUAL(i, childaddr);
      }
   }

   for (uint64 i = 0; i < nkvs; i++) {
      rv = variable_length_btree_set_index_entry(
         cfg, hdr, i, slice_create(i % sizeof(i), &i), i, 0, 0, 0);
      if (!rv) {
         platform_log(
            "[%s:%d] failed to insert 8-byte %ld\n", __FILE__, __LINE__, i);
         ASSERT_TRUE(rv);
      }
   }

   for (uint64 i = 0; i < nkvs; i++) {
      slice  key       = variable_length_btree_get_pivot(cfg, hdr, i);
      uint64 childaddr = variable_length_btree_get_child_addr(cfg, hdr, i);
      cmp_rv           = slice_lex_cmp(slice_create(i % sizeof(i), &i), key);
      if (cmp_rv) {
         platform_log("[%s:%d] bad 4-byte key %ld\n", __FILE__, __LINE__, i);
         ASSERT_EQUAL(0, cmp_rv);
      }
      if (childaddr != i) {
         platform_log("[%s:%d] bad childaddr %ld\n", __FILE__, __LINE__, i);
         ASSERT_EQUAL(i, childaddr);
      }
   }

   variable_length_btree_defragment_index(cfg, scratch, hdr);

   for (uint64 i = 0; i < nkvs; i++) {
      slice  key       = variable_length_btree_get_pivot(cfg, hdr, i);
      uint64 childaddr = variable_length_btree_get_child_addr(cfg, hdr, i);
      cmp_rv           = slice_lex_cmp(slice_create(i % sizeof(i), &i), key);
      if (cmp_rv) {
         platform_log("[%s:%d] bad 4-byte key %ld\n", __FILE__, __LINE__, i);
         ASSERT_EQUAL(0, cmp_rv);
      }
      if (childaddr != i) {
         platform_log("[%s:%d] bad childaddr %ld\n", __FILE__, __LINE__, i);
         ASSERT_EQUAL(i, childaddr);
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

   bool rv = FALSE;
   for (int i = 0; i < nkvs; i += 2) {
      uint8 keybuf[1];
      keybuf[0] = i;
      slice key = slice_create(1, &keybuf);

      rv = variable_length_btree_set_index_entry(
         cfg, hdr, i / 2, key, i, 0, 0, 0);
      if (!rv) {
         platform_log(
            "[%s:%d] couldn't insert pivot %d\n", __FILE__, __LINE__, i);
         ASSERT_TRUE(rv);
      }
   }

   for (int i = 0; i < nkvs; i++) {
      bool  found;
      uint8 keybuf[1];
      keybuf[0] = i;
      slice key = slice_create(1, &keybuf);
      int64 idx = variable_length_btree_find_pivot(cfg, hdr, key, &found);
      if (idx != i / 2) {
         platform_log("[%s:%d] bad pivot search result %ld for %d\n",
                      __FILE__,
                      __LINE__,
                      idx,
                      i);
         ASSERT_EQUAL((i / 2), idx);
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
         ASSERT_FALSE(success);
      }
      leaf_splitting_plan plan =
         variable_length_btree_build_leaf_splitting_plan(cfg, hdr, &spec);
      ASSERT_TRUE((realnkvs / 2 - 1) <= plan.split_idx);
      ASSERT_TRUE((plan.split_idx) <= (realnkvs / 2 + 1));
   }

   return 0;
}
