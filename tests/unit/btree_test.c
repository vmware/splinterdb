// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * btree_test.c --
 *
 *  Exercises the BTree interfaces in btree_test.c, and related
 *  files. Validates correctness of variable key-value size support in BTree.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

#include "test_data.h"
#include "splinterdb/data.h"
#include "io.h"
#include "rc_allocator.h"
#include "clockcache.h"
#include "btree_private.h"
#include "btree_test_common.h"

// Function Prototypes

static int
leaf_hdr_tests(btree_config *cfg, btree_scratch *scratch, platform_heap_id hid);

static int
leaf_hdr_search_tests(btree_config *cfg, platform_heap_id hid);
static int
index_hdr_tests(btree_config    *cfg,
                btree_scratch   *scratch,
                platform_heap_id hid);

static int
index_hdr_search_tests(btree_config *cfg, platform_heap_id hid);

static int
leaf_split_tests(btree_config    *cfg,
                 btree_scratch   *scratch,
                 int              nkvs,
                 platform_heap_id hid);

static bool32
btree_leaf_incorporate_tuple(const btree_config    *cfg,
                             platform_heap_id       hid,
                             btree_hdr             *hdr,
                             key                    tuple_key,
                             message                msg,
                             leaf_incorporate_spec *spec,
                             uint64                *generation)
{
   platform_status rc =
      btree_create_leaf_incorporate_spec(cfg, hid, hdr, tuple_key, msg, spec);
   ASSERT_TRUE(SUCCESS(rc));
   return btree_try_perform_leaf_incorporate_spec(cfg, hdr, spec, generation);
}

/*
 * Global data declaration macro:
 */
CTEST_DATA(btree)
{
   master_config     master_cfg;
   data_config      *data_cfg;
   io_config         io_cfg;
   allocator_config  allocator_cfg;
   clockcache_config cache_cfg;
   btree_scratch     test_scratch;
   btree_config      dbtree_cfg;
   platform_heap_id  hid;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(btree)
{
   config_set_defaults(&data->master_cfg);
   data->data_cfg = test_data_config;
   data->hid      = platform_get_heap_id();

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
                                                data->data_cfg))
   {
      ASSERT_TRUE(FALSE, "Failed to parse args\n");
   }
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(btree) {}

/*
 * Test leaf_hdr APIs.
 */
CTEST2(btree, test_leaf_hdr)
{
   int rc = leaf_hdr_tests(&data->dbtree_cfg, &data->test_scratch, data->hid);
   ASSERT_EQUAL(0, rc);
}

/*
 * Test leaf_hdr search APIs.
 */
CTEST2(btree, test_leaf_hdr_search)
{
   int rc = leaf_hdr_search_tests(&data->dbtree_cfg, data->hid);
   ASSERT_EQUAL(0, rc);
}

/*
 * Test index_hdr APIs.
 */
CTEST2(btree, test_index_hdr)
{
   int rc = index_hdr_tests(&data->dbtree_cfg, &data->test_scratch, data->hid);
   ASSERT_EQUAL(0, rc);
}

/*
 * Test index_hdr search APIs.
 */
CTEST2(btree, test_index_hdr_search)
{
   int rc = index_hdr_search_tests(&data->dbtree_cfg, data->hid);
   ASSERT_EQUAL(0, rc);
}

/*
 * Test leaf-split code
 */
CTEST2(btree, test_leaf_split)
{
   for (int nkvs = 2; nkvs < 100; nkvs++) {
      int rc = leaf_split_tests(
         &data->dbtree_cfg, &data->test_scratch, nkvs, data->hid);
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
leaf_hdr_tests(btree_config *cfg, btree_scratch *scratch, platform_heap_id hid)
{
   char *leaf_buffer =
      TYPED_MANUAL_MALLOC(hid, leaf_buffer, btree_page_size(cfg));
   btree_hdr *hdr = (btree_hdr *)leaf_buffer;
   /*
    * The following number is empirically determined to be the most
    * entries that we could fit in a leaf.  There's nothing magical
    * about this number. If you change the size of a btree leaf header
    * or the size of a btree leafy entry, then this number will need
    * to be changed, and that's fine.
    */
   int nkvs = 208;

   btree_init_hdr(cfg, hdr);

   bool32 rv = FALSE;
   for (uint32 i = 0; i < nkvs; i++) {
      rv = btree_set_leaf_entry(
         cfg,
         hdr,
         i,
         key_create(i % sizeof(i), &i),
         message_create(MESSAGE_TYPE_INSERT, slice_create(i % sizeof(i), &i)));
      ASSERT_TRUE(rv, "Failed to insert 4-byte %d\n", i);
   }

   int cmp_rv = 0;
   for (uint32 i = 0; i < nkvs; i++) {
      key     tuple_key = btree_get_tuple_key(cfg, hdr, i);
      message msg       = btree_get_tuple_message(cfg, hdr, i);
      cmp_rv            = data_key_compare(
         cfg->data_cfg, key_create(i % sizeof(i), &i), tuple_key);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte key %d\n", i);

      cmp_rv = message_lex_cmp(
         message_create(MESSAGE_TYPE_INSERT, slice_create(i % sizeof(i), &i)),
         msg);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte message %d\n", i);
   }

   rv = FALSE;
   for (uint64 i = 0; i < nkvs; i++) {
      rv = btree_set_leaf_entry(
         cfg,
         hdr,
         i,
         key_create(i % sizeof(i), &i),
         message_create(MESSAGE_TYPE_INSERT, slice_create(i % sizeof(i), &i)));
      ASSERT_TRUE(rv, "Failed to insert 8-byte %ld\n", i);
   }

   cmp_rv = 0;
   for (uint64 i = 0; i < nkvs; i++) {
      key     tuple_key = btree_get_tuple_key(cfg, hdr, i);
      message msg       = btree_get_tuple_message(cfg, hdr, i);
      cmp_rv            = data_key_compare(
         cfg->data_cfg, key_create(i % sizeof(i), &i), tuple_key);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte key %d\n", i);

      cmp_rv = message_lex_cmp(
         message_create(MESSAGE_TYPE_INSERT, slice_create(i % sizeof(i), &i)),
         msg);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte message %d\n", i);
   }

   leaf_incorporate_spec spec = {.old_entry_state = ENTRY_DID_NOT_EXIST};
   btree_defragment_leaf(cfg, scratch, hdr, &spec);

   for (uint64 i = 0; i < nkvs; i++) {
      key     tuple_key = btree_get_tuple_key(cfg, hdr, i);
      message msg       = btree_get_tuple_message(cfg, hdr, i);
      cmp_rv            = data_key_compare(
         cfg->data_cfg, key_create(i % sizeof(i), &i), tuple_key);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte key %d\n", i);

      cmp_rv = message_lex_cmp(
         message_create(MESSAGE_TYPE_INSERT, slice_create(i % sizeof(i), &i)),
         msg);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte message %d\n", i);
   }

   platform_free(hid, leaf_buffer);
   return 0;
}

static int
leaf_hdr_search_tests(btree_config *cfg, platform_heap_id hid)
{
   char *leaf_buffer =
      TYPED_MANUAL_MALLOC(hid, leaf_buffer, btree_page_size(cfg));
   btree_hdr *hdr  = (btree_hdr *)leaf_buffer;
   int        nkvs = 256;

   btree_init_hdr(cfg, hdr);

   for (int i = 0; i < nkvs; i++) {
      uint64 generation;
      uint8  keybuf[1];
      uint8  messagebuf[8];
      keybuf[0]     = 17 * i;
      messagebuf[0] = i;

      key     tuple_key = key_create(1, &keybuf);
      message msg =
         message_create(MESSAGE_TYPE_INSERT, slice_create(i % 8, messagebuf));

      leaf_incorporate_spec spec;
      bool32                result = btree_leaf_incorporate_tuple(
         cfg, hid, hdr, tuple_key, msg, &spec, &generation);
      ASSERT_TRUE(result, "Could not incorporate kv pair %d\n", i);

      ASSERT_EQUAL(generation, i, "Bad generation=%lu, i=%d\n", generation, i);
   }

   for (int i = 0; i < nkvs; i++) {
      key   tuple_key = btree_get_tuple_key(cfg, hdr, i);
      uint8 ui        = i;
      int   cmp_rv =
         data_key_compare(cfg->data_cfg, key_create(1, &ui), tuple_key);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte key %d\n", i);
   }

   platform_free(hid, leaf_buffer);
   return 0;
}

static int
index_hdr_tests(btree_config *cfg, btree_scratch *scratch, platform_heap_id hid)
{

   char *index_buffer =
      TYPED_MANUAL_MALLOC(hid, index_buffer, btree_page_size(cfg));
   btree_hdr *hdr  = (btree_hdr *)index_buffer;
   int        nkvs = 100;


   bool32 rv     = FALSE;
   int    cmp_rv = 0;

   btree_init_hdr(cfg, hdr);
   hdr->height = 1;

   btree_pivot_stats stats;
   memset(&stats, 0, sizeof(stats));
   for (uint32 i = 0; i < nkvs; i++) {
      rv = btree_set_index_entry(
         cfg, hdr, i, key_create(i % sizeof(i), &i), i, stats);
      ASSERT_TRUE(rv, "Failed to insert 4-byte %d\n", i);
   }

   for (uint32 i = 0; i < nkvs; i++) {
      key    pivot_key = btree_get_pivot(cfg, hdr, i);
      uint64 childaddr = btree_get_child_addr(cfg, hdr, i);
      cmp_rv           = data_key_compare(
         cfg->data_cfg, key_create(i % sizeof(i), &i), pivot_key);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte key %d\n", i);

      ASSERT_EQUAL(childaddr, i, "Bad childaddr %d\n", i);
   }

   for (uint64 i = 0; i < nkvs; i++) {
      rv = btree_set_index_entry(
         cfg, hdr, i, key_create(i % sizeof(i), &i), i, stats);
      ASSERT_TRUE(rv, "Failed to insert 8-byte %ld\n", i);
   }

   for (uint64 i = 0; i < nkvs; i++) {
      key    pivot_key = btree_get_pivot(cfg, hdr, i);
      uint64 childaddr = btree_get_child_addr(cfg, hdr, i);
      cmp_rv           = data_key_compare(
         cfg->data_cfg, key_create(i % sizeof(i), &i), pivot_key);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte key %d\n", i);

      ASSERT_EQUAL(childaddr, i, "Bad childaddr %d\n", i);
   }

   btree_defragment_index(cfg, scratch, hdr);

   for (uint64 i = 0; i < nkvs; i++) {
      key    pivot_key = btree_get_pivot(cfg, hdr, i);
      uint64 childaddr = btree_get_child_addr(cfg, hdr, i);
      cmp_rv           = data_key_compare(
         cfg->data_cfg, key_create(i % sizeof(i), &i), pivot_key);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte key %d\n", i);

      ASSERT_EQUAL(childaddr, i, "Bad childaddr %d\n", i);
   }
   platform_free(hid, index_buffer);
   return 0;
}

static int
index_hdr_search_tests(btree_config *cfg, platform_heap_id hid)
{
   char *leaf_buffer =
      TYPED_MANUAL_MALLOC(hid, leaf_buffer, btree_page_size(cfg));
   btree_hdr        *hdr  = (btree_hdr *)leaf_buffer;
   int               nkvs = 256;
   btree_pivot_stats stats;
   memset(&stats, 0, sizeof(stats));

   btree_init_hdr(cfg, hdr);

   bool32 rv = FALSE;
   for (int i = 0; i < nkvs; i += 2) {
      uint8 keybuf[1];
      keybuf[0]     = i;
      key pivot_key = key_create(1, &keybuf);

      rv = btree_set_index_entry(cfg, hdr, i / 2, pivot_key, i, stats);
      ASSERT_TRUE(rv, "Could not insert pivot %d\n", i);
   }

   for (int i = 0; i < nkvs; i++) {
      bool32 found;
      uint8  keybuf[1];
      keybuf[0]    = i;
      key   target = key_create(1, &keybuf);
      int64 idx    = btree_find_pivot(cfg, hdr, target, &found);
      ASSERT_EQUAL(
         (i / 2), idx, "Bad pivot search result idx=%ld for i=%d\n", idx, i);
   }

   platform_free(hid, leaf_buffer);
   return 0;
}

static int
leaf_split_tests(btree_config    *cfg,
                 btree_scratch   *scratch,
                 int              nkvs,
                 platform_heap_id hid)
{
   char *leaf_buffer =
      TYPED_MANUAL_MALLOC(hid, leaf_buffer, btree_page_size(cfg));
   char *msg_buffer =
      TYPED_MANUAL_MALLOC(hid, msg_buffer, btree_page_size(cfg));

   memset(msg_buffer, 0, btree_page_size(cfg));

   btree_hdr *hdr = (btree_hdr *)leaf_buffer;

   btree_init_hdr(cfg, hdr);

   int     msgsize = btree_page_size(cfg) / (nkvs + 1);
   message msg =
      message_create(MESSAGE_TYPE_INSERT, slice_create(msgsize, msg_buffer));
   message bigger_msg = message_create(
      MESSAGE_TYPE_INSERT,
      slice_create(msgsize + sizeof(table_entry) + 1, msg_buffer));

   uint8 realnkvs = 0;
   while (realnkvs < nkvs) {
      uint8 keybuf[1];
      keybuf[0] = 2 * realnkvs + 1;
      if (!btree_set_leaf_entry(
             cfg, hdr, realnkvs, key_create(1, &keybuf), msg)) {
         break;
      }
      realnkvs++;
   }

   for (uint8 i = 0; i < 2 * realnkvs + 1; i++) {
      uint64                generation;
      leaf_incorporate_spec spec;

      key tuple_key = key_create(1, &i);

      bool32 success = btree_leaf_incorporate_tuple(
         cfg, hid, hdr, tuple_key, bigger_msg, &spec, &generation);
      if (success) {
         btree_print_locked_node(
            Platform_error_log_handle, cfg, 0, hdr, PAGE_TYPE_MEMTABLE);
         ASSERT_FALSE(success,
                      "Weird.  An incorporate that was supposed to fail "
                      "actually succeeded (nkvs=%d, realnkvs=%d, i=%d).\n",
                      nkvs,
                      realnkvs,
                      i);
         btree_print_locked_node(
            Platform_error_log_handle, cfg, 0, hdr, PAGE_TYPE_MEMTABLE);
         ASSERT_FALSE(success);
      }
      leaf_splitting_plan plan =
         btree_build_leaf_splitting_plan(cfg, hdr, &spec);
      ASSERT_TRUE((realnkvs / 2 - 1) <= plan.split_idx,
                  "realnkvs = %d, plan.split_idx = %d",
                  realnkvs,
                  plan.split_idx);
      ASSERT_TRUE((plan.split_idx) <= (realnkvs / 2 + 1),
                  "realnkvs = %d, plan.split_idx = %d",
                  realnkvs,
                  plan.split_idx);

      destroy_leaf_incorporate_spec(&spec);
   }

   platform_free(hid, leaf_buffer);
   platform_free(hid, msg_buffer);
   return 0;
}
