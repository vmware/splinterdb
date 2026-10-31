// Copyright 2021-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * btree_test.c --
 *
 *  Exercises the BTree interfaces in btree_test.c, and related
 *  files. Validates correctness of variable key-value size support in BTree.
 * -----------------------------------------------------------------------------
 */
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

#include "test_data.h"
#include "splinterdb/data.h"
#include "platform_io.h"
#include "platform_threads.h"
#include "platform_units.h"
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

static int
defragmented_leaf_bytes_tests(btree_config    *cfg,
                              btree_scratch   *scratch,
                              platform_heap_id hid);

static int
defragmented_index_bytes_tests(btree_config    *cfg,
                               btree_scratch   *scratch,
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
   platform_status rc = btree_create_leaf_incorporate_spec(
      cfg, NULL, NULL, hid, hdr, &tuple_key, &msg, spec);
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
   btree_scratch    *test_scratch;
   btree_config      dbtree_cfg;
   platform_heap_id  hid;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(btree)
{
   platform_register_thread();
   config_set_defaults(&data->master_cfg);
   uint64 heap_capacity = (512 * MiB);

   if (!SUCCESS(
          config_parse(&data->master_cfg, 1, Ctest_argc, (char **)Ctest_argv)))
   {
      ASSERT_TRUE(FALSE, "Failed to parse args\n");
   }

   // Create a heap for io, allocator, cache and splinter
   platform_status rc = platform_heap_create(platform_get_module_id(),
                                             heap_capacity,
                                             data->master_cfg.use_shmem,
                                             &data->hid);
   platform_assert_status_ok(rc);

   data->data_cfg = test_data_config;

   if (!init_data_config_from_master_config(data->data_cfg, &data->master_cfg)
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

   data->test_scratch = TYPED_MANUAL_ZALLOC(
      data->hid, data->test_scratch, btree_scratch_size(&data->dbtree_cfg));
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(btree)
{
   platform_heap_destroy(&data->hid);
   platform_deregister_thread();
}

CTEST2(btree, test_compressed_suffix_copy)
{
   char source[64];
   char destination[66];
   for (uint64 i = 0; i < sizeof(source); i++) {
      source[i] = (char)(i + 1);
   }
   for (uint64 length = 0; length <= sizeof(source); length++) {
      memset(destination, 0x5a, sizeof(destination));
      btree_copy_key_suffix(destination + 1, source, length);
      ASSERT_EQUAL(0, memcmp(destination + 1, source, length));
      ASSERT_EQUAL(0x5a, destination[0]);
      for (uint64 i = length + 1; i < sizeof(destination); i++) {
         ASSERT_EQUAL(0x5a, destination[i]);
      }
   }
}

/*
 * Test leaf_hdr APIs.
 */
CTEST2(btree, test_leaf_hdr)
{
   int rc = leaf_hdr_tests(&data->dbtree_cfg, data->test_scratch, data->hid);
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
   int rc = index_hdr_tests(&data->dbtree_cfg, data->test_scratch, data->hid);
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
         &data->dbtree_cfg, data->test_scratch, nkvs, data->hid);
      ASSERT_EQUAL(0, rc);
   }
}

/*
 * Defragment-or-split decisions predict the bytes a node will use once it is
 * defragmented. Check those predictions against the bytes actually used.
 */
CTEST2(btree, test_defragmented_leaf_bytes)
{
   int rc = defragmented_leaf_bytes_tests(
      &data->dbtree_cfg, data->test_scratch, data->hid);
   ASSERT_EQUAL(0, rc);
}

CTEST2(btree, test_defragmented_index_bytes)
{
   int rc = defragmented_index_bytes_tests(
      &data->dbtree_cfg, data->test_scratch, data->hid);
   ASSERT_EQUAL(0, rc);
}

/*
 * Branch references must be rejected by the btree before any page access.
 * A NULL cache makes an accidental read fail, including in the async paths.
 */
static const struct {
   uint64       addr;
   btree_format format;
} invalid_branch_refs[] = {
   {4096, {1, 256}},
   {4096, {2, 256}},
   {4096, {3, 256}},
   {4096, {BTREE_BRANCH_FORMAT_VERSION + 1, 256}},
   {0, BTREE_DEFAULT_FORMAT},
   {4096, {BTREE_BRANCH_FORMAT_VERSION, UINT16_MAX}},
};

CTEST2(btree, test_invalid_branch_point_lookup)
{
   lookup_result result;
   lookup_result_init(
      &result, data->data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   uint64 keybuf = 1;
   key    target = key_create(FALSE, sizeof(keybuf), &keybuf);

   for (uint64 i = 0; i < ARRAY_SIZE(invalid_branch_refs); i++) {
      platform_status rc = btree_lookup(NULL,
                                        &data->dbtree_cfg,
                                        invalid_branch_refs[i].addr,
                                        invalid_branch_refs[i].format,
                                        PAGE_TYPE_BRANCH,
                                        target,
                                        &result);
      ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_INVALID_STATE));
      ASSERT_FALSE(lookup_result_found(&result));

      bool32 local_found = TRUE;
      rc                 = btree_lookup_and_merge(NULL,
                                  &data->dbtree_cfg,
                                  invalid_branch_refs[i].addr,
                                  invalid_branch_refs[i].format,
                                  PAGE_TYPE_BRANCH,
                                  &target,
                                  &result,
                                  &local_found);
      ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_INVALID_STATE));
      ASSERT_FALSE(local_found);
      ASSERT_FALSE(lookup_result_found(&result));
   }
   lookup_result_deinit(&result);
}

CTEST2(btree, test_invalid_branch_async_point_lookup)
{
   lookup_result result;
   lookup_result_init(
      &result, data->data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   uint64 keybuf = 1;
   key    target = key_create(FALSE, sizeof(keybuf), &keybuf);

   for (uint64 i = 0; i < ARRAY_SIZE(invalid_branch_refs); i++) {
      btree_lookup_async_state state;
      btree_lookup_async_state_init(&state,
                                    NULL,
                                    &data->dbtree_cfg,
                                    invalid_branch_refs[i].addr,
                                    invalid_branch_refs[i].format,
                                    PAGE_TYPE_BRANCH,
                                    target,
                                    &result,
                                    NULL,
                                    NULL);
      ASSERT_EQUAL(ASYNC_STATUS_DONE, btree_lookup_async(&state));
      ASSERT_TRUE(STATUS_IS_EQ(async_result(&state), STATUS_INVALID_STATE));
      ASSERT_FALSE(lookup_result_found(&result));

      btree_lookup_and_merge_async_state_init(&state,
                                              NULL,
                                              &data->dbtree_cfg,
                                              invalid_branch_refs[i].addr,
                                              invalid_branch_refs[i].format,
                                              PAGE_TYPE_BRANCH,
                                              target,
                                              &result,
                                              NULL,
                                              NULL);
      ASSERT_EQUAL(ASYNC_STATUS_DONE, btree_lookup_and_merge_async(&state));
      ASSERT_TRUE(STATUS_IS_EQ(async_result(&state), STATUS_INVALID_STATE));
      ASSERT_FALSE(lookup_result_found(&result));
   }
   lookup_result_deinit(&result);
}

CTEST2(btree, test_invalid_branch_iterator)
{
   for (uint64 i = 0; i < ARRAY_SIZE(invalid_branch_refs); i++) {
      for (bool32 copy_nodes = FALSE; copy_nodes <= TRUE; copy_nodes++) {
         btree_iterator itor;
         memset(&itor, 0xa5, sizeof(itor));
         platform_status rc = btree_iterator_init(
            NULL,
            &data->dbtree_cfg,
            &itor,
            invalid_branch_refs[i].addr,
            invalid_branch_refs[i].format,
            PAGE_TYPE_BRANCH,
            &(const btree_iterator_init_options){.copy_nodes = copy_nodes,
                                                 .prefetch_lookahead = 1});
         ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_INVALID_STATE));
         btree_iterator_deinit(&itor);

         memset(&itor, 0xa5, sizeof(itor));
         btree_iterator_async_state state;
         btree_iterator_async_state_init(&state,
                                         NULL,
                                         &data->dbtree_cfg,
                                         &itor,
                                         invalid_branch_refs[i].addr,
                                         invalid_branch_refs[i].format,
                                         PAGE_TYPE_BRANCH,
                                         greater_than_or_equal,
                                         NEGATIVE_INFINITY_KEY,
                                         less_than,
                                         POSITIVE_INFINITY_KEY,
                                         greater_than_or_equal,
                                         NEGATIVE_INFINITY_KEY,
                                         copy_nodes,
                                         0,
                                         1,
                                         NULL,
                                         NULL);
         ASSERT_EQUAL(ASYNC_STATUS_DONE, btree_iterator_init_async(&state));
         ASSERT_TRUE(STATUS_IS_EQ(btree_iterator_init_async_result(&state),
                                  STATUS_INVALID_STATE));
         btree_iterator_deinit(&itor);
      }
   }
}

CTEST2(btree, test_invalid_branch_metadata_queries)
{
   for (uint64 i = 0; i < ARRAY_SIZE(invalid_branch_refs); i++) {
      uint32          height;
      platform_status rc = btree_get_root_height(NULL,
                                                 &data->dbtree_cfg,
                                                 invalid_branch_refs[i].addr,
                                                 invalid_branch_refs[i].format,
                                                 PAGE_TYPE_BRANCH,
                                                 &height);
      ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_INVALID_STATE));

      btree_pivot_stats stats;
      rc = btree_count_in_range(NULL,
                                &data->dbtree_cfg,
                                invalid_branch_refs[i].addr,
                                invalid_branch_refs[i].format,
                                NEGATIVE_INFINITY_KEY,
                                POSITIVE_INFINITY_KEY,
                                &stats);
      ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_INVALID_STATE));

      rc = btree_recover_allocations(NULL,
                                     &data->dbtree_cfg,
                                     invalid_branch_refs[i].addr,
                                     invalid_branch_refs[i].format,
                                     PAGE_TYPE_BRANCH);
      ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_INVALID_STATE));
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
    * (The header currently has room for 208 such entries.)
    */
   int nkvs = 208;

   btree_init_hdr(cfg, hdr);

   bool32 rv = FALSE;
   for (uint32 i = 0; i < nkvs; i++) {
      rv = btree_set_leaf_entry(
         cfg,
         hdr,
         i,
         key_create(FALSE, i % sizeof(i), &i),
         message_create_internal(
            MESSAGE_TYPE_INSERT, NULL, slice_create(i % sizeof(i), &i)));
      ASSERT_TRUE(rv, "Failed to insert 4-byte %d\n", i);
   }

   int cmp_rv = 0;
   for (uint32 i = 0; i < nkvs; i++) {
      key     tuple_key = btree_get_tuple_key(cfg, hdr, i);
      message msg       = btree_get_tuple_message(cfg, NULL, hdr, i);
      cmp_rv            = data_key_compare(
         cfg->data_cfg, key_create(FALSE, i % sizeof(i), &i), tuple_key);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte key %d\n", i);

      cmp_rv = message_lex_cmp(
         message_create_internal(
            MESSAGE_TYPE_INSERT, NULL, slice_create(i % sizeof(i), &i)),
         msg);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte message %d\n", i);
   }

   rv = FALSE;
   for (uint64 i = 0; i < nkvs; i++) {
      rv = btree_set_leaf_entry(
         cfg,
         hdr,
         i,
         key_create(FALSE, i % sizeof(i), &i),
         message_create_internal(
            MESSAGE_TYPE_INSERT, NULL, slice_create(i % sizeof(i), &i)));
      ASSERT_TRUE(rv, "Failed to insert 8-byte %ld\n", i);
   }

   cmp_rv = 0;
   for (uint64 i = 0; i < nkvs; i++) {
      key     tuple_key = btree_get_tuple_key(cfg, hdr, i);
      message msg       = btree_get_tuple_message(cfg, NULL, hdr, i);
      cmp_rv            = data_key_compare(
         cfg->data_cfg, key_create(FALSE, i % sizeof(i), &i), tuple_key);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte key %d\n", i);

      cmp_rv = message_lex_cmp(
         message_create_internal(
            MESSAGE_TYPE_INSERT, NULL, slice_create(i % sizeof(i), &i)),
         msg);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte message %d\n", i);
   }

   leaf_incorporate_spec spec = {.old_entry_state = ENTRY_DID_NOT_EXIST};
   btree_defragment_leaf(cfg, scratch, hdr, &spec);

   for (uint64 i = 0; i < nkvs; i++) {
      key     tuple_key = btree_get_tuple_key(cfg, hdr, i);
      message msg       = btree_get_tuple_message(cfg, NULL, hdr, i);
      cmp_rv            = data_key_compare(
         cfg->data_cfg, key_create(FALSE, i % sizeof(i), &i), tuple_key);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte key %d\n", i);

      cmp_rv = message_lex_cmp(
         message_create_internal(
            MESSAGE_TYPE_INSERT, NULL, slice_create(i % sizeof(i), &i)),
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

      key     tuple_key = key_create(FALSE, 1, &keybuf);
      message msg       = message_create_internal(
         MESSAGE_TYPE_INSERT, NULL, slice_create(i % 8, messagebuf));

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
         data_key_compare(cfg->data_cfg, key_create(FALSE, 1, &ui), tuple_key);
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
   int        nkvs = (btree_page_size(cfg) - sizeof(*hdr))
              / (2
                 * (sizeof(index_entry) + sizeof(btree_pivot_stats32)
                    + sizeof(uint64) + sizeof(table_entry)));

   bool32 rv     = FALSE;
   int    cmp_rv = 0;

   btree_init_hdr(cfg, hdr);
   hdr->height = 1;

   btree_pivot_stats stats;
   memset(&stats, 0, sizeof(stats));
   for (uint32 i = 0; i < nkvs; i++) {
      rv = btree_set_index_entry(
         cfg, hdr, i, key_create(FALSE, i % sizeof(i), &i), i, stats);
      ASSERT_TRUE(rv, "Failed to insert 4-byte %d\n", i);
   }

   for (uint32 i = 0; i < nkvs; i++) {
      key    pivot_key = btree_get_pivot(cfg, hdr, i);
      uint64 childaddr = btree_get_child_addr(cfg, hdr, i);
      cmp_rv           = data_key_compare(
         cfg->data_cfg, key_create(FALSE, i % sizeof(i), &i), pivot_key);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte key %d\n", i);

      ASSERT_EQUAL(childaddr, i, "Bad childaddr %d\n", i);
   }

   for (uint64 i = 0; i < nkvs; i++) {
      rv = btree_set_index_entry(
         cfg, hdr, i, key_create(FALSE, i % sizeof(i), &i), i, stats);
      ASSERT_TRUE(rv, "Failed to insert 8-byte %ld\n", i);
   }

   for (uint64 i = 0; i < nkvs; i++) {
      key    pivot_key = btree_get_pivot(cfg, hdr, i);
      uint64 childaddr = btree_get_child_addr(cfg, hdr, i);
      cmp_rv           = data_key_compare(
         cfg->data_cfg, key_create(FALSE, i % sizeof(i), &i), pivot_key);
      ASSERT_EQUAL(0, cmp_rv, "Bad 4-byte key %d\n", i);

      ASSERT_EQUAL(childaddr, i, "Bad childaddr %d\n", i);
   }

   btree_defragment_index(cfg, scratch, hdr);

   for (uint64 i = 0; i < nkvs; i++) {
      key    pivot_key = btree_get_pivot(cfg, hdr, i);
      uint64 childaddr = btree_get_child_addr(cfg, hdr, i);
      cmp_rv           = data_key_compare(
         cfg->data_cfg, key_create(FALSE, i % sizeof(i), &i), pivot_key);
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
   btree_hdr *hdr  = (btree_hdr *)leaf_buffer;
   int        nkvs = 2
              * MIN(128,
                    (btree_page_size(cfg) - sizeof(*hdr))
                       / (sizeof(index_entry) + sizeof(btree_pivot_stats32) + 1
                          + sizeof(table_entry)));
   btree_pivot_stats stats;
   memset(&stats, 0, sizeof(stats));

   btree_init_hdr(cfg, hdr);

   bool32 rv = FALSE;
   for (int i = 0; i < nkvs; i += 2) {
      uint8 keybuf[1];
      keybuf[0]     = i;
      key pivot_key = key_create(FALSE, 1, &keybuf);

      rv = btree_set_index_entry(cfg, hdr, i / 2, pivot_key, i, stats);
      ASSERT_TRUE(rv, "Could not insert pivot %d\n", i);
   }

   for (int i = 0; i < nkvs; i++) {
      bool32 found;
      uint8  keybuf[1];
      keybuf[0]    = i;
      key   target = key_create(FALSE, 1, &keybuf);
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
   uint64 bt_page_size = btree_page_size(cfg);
   char  *leaf_buffer  = TYPED_MANUAL_MALLOC(hid, leaf_buffer, bt_page_size);
   char  *msg_buffer   = TYPED_MANUAL_MALLOC(hid, msg_buffer, bt_page_size);

   memset(msg_buffer, 0, bt_page_size);

   btree_hdr *hdr = (btree_hdr *)leaf_buffer;

   btree_init_hdr(cfg, hdr);

   int     msgsize = bt_page_size / (nkvs + 1);
   message msg     = message_create_internal(
      MESSAGE_TYPE_INSERT, NULL, slice_create(msgsize, msg_buffer));
   message bigger_msg = message_create_internal(
      MESSAGE_TYPE_INSERT,
      NULL,
      slice_create(msgsize + sizeof(table_entry) + 1, msg_buffer));

   uint8 realnkvs = 0;
   while (realnkvs < nkvs) {
      uint8 keybuf[1];
      keybuf[0] = 2 * realnkvs + 1;
      if (!btree_set_leaf_entry(
             cfg, hdr, realnkvs, key_create(FALSE, 1, &keybuf), msg))
      {
         break;
      }
      realnkvs++;
   }

   for (uint8 i = 0; i < 2 * realnkvs + 1; i++) {
      uint64                generation;
      leaf_incorporate_spec spec;

      key tuple_key = key_create(FALSE, 1, &i);

      bool32 success = btree_leaf_incorporate_tuple(
         cfg, hid, hdr, tuple_key, bigger_msg, &spec, &generation);
      if (success) {
         btree_print_locked_node(
            Platform_error_log_handle, cfg, NULL, 0, hdr, PAGE_TYPE_MEMTABLE);
         ASSERT_FALSE(success,
                      "Weird.  An incorporate that was supposed to fail "
                      "actually succeeded (nkvs=%d, realnkvs=%d, i=%d).\n",
                      nkvs,
                      realnkvs,
                      i);
         btree_print_locked_node(
            Platform_error_log_handle, cfg, NULL, 0, hdr, PAGE_TYPE_MEMTABLE);
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

// Bytes used by a node's entries, including garbage, and its offset table.
static uint64
node_used_bytes(const btree_config *cfg, const btree_hdr *hdr)
{
   return btree_page_size(cfg) - hdr->next_entry
          + hdr->num_entries * sizeof(table_entry);
}

static int
defragmented_leaf_bytes_tests(btree_config    *cfg,
                              btree_scratch   *scratch,
                              platform_heap_id hid)
{
   uint64 page_size   = btree_page_size(cfg);
   char  *leaf_buffer = TYPED_MANUAL_MALLOC(hid, leaf_buffer, page_size);
   char  *copy_buffer = TYPED_MANUAL_MALLOC(hid, copy_buffer, page_size);
   uint8  msgbuf[32];
   memset(msgbuf, 'm', sizeof(msgbuf));

   // The leaf's keys are the odd numbers 1, 3, 5, ...
   const uint8 nkvs = 60;
   btree_hdr  *hdr  = (btree_hdr *)leaf_buffer;
   btree_init_hdr(cfg, hdr);
   for (uint8 i = 0; i < nkvs; i++) {
      uint8   keybuf = 2 * i + 1;
      message msg    = message_create_internal(
         MESSAGE_TYPE_INSERT, NULL, slice_create(i % 7 + 1, msgbuf));
      ASSERT_TRUE(
         btree_set_leaf_entry(cfg, hdr, i, key_create(FALSE, 1, &keybuf), msg));
   }
   // Growing every third message leaves its old copy behind as garbage.
   for (uint8 i = 0; i < nkvs; i += 3) {
      uint8   keybuf = 2 * i + 1;
      message msg    = message_create_internal(
         MESSAGE_TYPE_INSERT, NULL, slice_create(i % 7 + 11, msgbuf));
      ASSERT_TRUE(
         btree_set_leaf_entry(cfg, hdr, i, key_create(FALSE, 1, &keybuf), msg));
   }
   uint64 live_entry_bytes = 0;
   for (uint64 i = 0; i < hdr->num_entries; i++) {
      live_entry_bytes += sizeof_leaf_entry(btree_get_leaf_entry(cfg, hdr, i));
   }
   ASSERT_TRUE(live_entry_bytes < page_size - hdr->next_entry);

   // Incorporate a new key, then a replacement for an existing key.
   uint8 new_keys[] = {2, 5};
   for (uint64 k = 0; k < ARRAY_SIZE(new_keys); k++) {
      memcpy(copy_buffer, leaf_buffer, page_size);
      btree_hdr            *copy = (btree_hdr *)copy_buffer;
      leaf_incorporate_spec spec;
      message               msg = message_create_internal(
         MESSAGE_TYPE_INSERT, NULL, slice_create(9, msgbuf));
      key             tuple_key = key_create(FALSE, 1, &new_keys[k]);
      platform_status rc        = btree_create_leaf_incorporate_spec(
         cfg, NULL, NULL, hid, copy, &tuple_key, &msg, &spec);
      ASSERT_TRUE(SUCCESS(rc));
      ASSERT_EQUAL(k == 0 ? ENTRY_DID_NOT_EXIST : ENTRY_STILL_EXISTS,
                   spec.old_entry_state);

      uint64 predicted = btree_defragmented_leaf_bytes(cfg, copy, &spec);
      btree_defragment_leaf(cfg, scratch, copy, &spec);
      uint64 generation;
      ASSERT_TRUE(btree_try_perform_leaf_incorporate_spec(
         cfg, copy, &spec, &generation));
      ASSERT_EQUAL(node_used_bytes(cfg, copy), predicted);
      destroy_leaf_incorporate_spec(&spec);
   }

   platform_free(hid, leaf_buffer);
   platform_free(hid, copy_buffer);
   return 0;
}

static int
defragmented_index_bytes_tests(btree_config    *cfg,
                               btree_scratch   *scratch,
                               platform_heap_id hid)
{
   uint64 page_size    = btree_page_size(cfg);
   char  *index_buffer = TYPED_MANUAL_MALLOC(hid, index_buffer, page_size);
   uint8  keybuf[16];
   memset(keybuf, 'k', sizeof(keybuf));
   btree_pivot_stats stats;
   memset(&stats, 0, sizeof(stats));

   const uint32 nkvs = 60;
   btree_hdr   *hdr  = (btree_hdr *)index_buffer;
   btree_init_hdr(cfg, hdr);
   hdr->height = 1;
   for (uint32 i = 0; i < nkvs; i++) {
      ASSERT_TRUE(btree_set_index_entry(
         cfg, hdr, i, key_create(FALSE, i % 5 + 1, keybuf), i, stats));
   }
   // Lengthening and widening every third pivot leaves garbage behind.
   stats.message_bytes = UINT64_C(1) << 40;
   for (uint32 i = 0; i < nkvs; i += 3) {
      ASSERT_TRUE(btree_set_index_entry(
         cfg, hdr, i, key_create(FALSE, i % 5 + 8, keybuf), i, stats));
   }
   uint64 live_entry_bytes = 0;
   for (uint64 i = 0; i < hdr->num_entries; i++) {
      live_entry_bytes +=
         sizeof_index_entry(btree_get_index_entry(cfg, hdr, i));
   }
   ASSERT_TRUE(live_entry_bytes < page_size - hdr->next_entry);

   uint64 predicted = btree_defragmented_index_bytes(cfg, hdr);
   btree_defragment_index(cfg, scratch, hdr);
   ASSERT_EQUAL(node_used_bytes(cfg, hdr), predicted);

   for (uint32 i = 0; i < nkvs; i++) {
      index_entry      *entry  = btree_get_index_entry(cfg, hdr, i);
      btree_pivot_stats actual = index_entry_stats(entry);
      ASSERT_EQUAL(i % 3 == 0 ? stats.message_bytes : 0, actual.message_bytes);
      ASSERT_EQUAL(i, index_entry_child_addr(entry));
      ASSERT_EQUAL(i % 5 + (i % 3 == 0 ? 8 : 1),
                   key_length(index_entry_key(entry)));
   }

   // Narrowing an existing wide entry must clear its flag and retain its key.
   stats.message_bytes = 1;
   for (uint32 i = 0; i < nkvs; i += 3) {
      ASSERT_TRUE(btree_set_index_entry(
         cfg, hdr, i, key_create(FALSE, i % 5 + 8, keybuf), i, stats));
      index_entry *entry = btree_get_index_entry(cfg, hdr, i);
      ASSERT_FALSE(entry->pivot.flags & BTREE_INDEX_WIDE_STATS);
      ASSERT_EQUAL(1, index_entry_stats(entry).message_bytes);
   }
   predicted = btree_defragmented_index_bytes(cfg, hdr);
   btree_defragment_index(cfg, scratch, hdr);
   ASSERT_EQUAL(node_used_bytes(cfg, hdr), predicted);

   platform_free(hid, index_buffer);
   return 0;
}

CTEST2(btree, test_native_statistics_layout)
{
   ASSERT_EQUAL(3 * sizeof(uint32), sizeof(btree_pivot_stats32));
   ASSERT_EQUAL(3 * sizeof(uint64), sizeof(btree_pivot_stats));
   ASSERT_EQUAL(sizeof(uint64), offsetof(index_entry, pivot));
   const struct {
      btree_pivot_stats stats;
      bool32            wide;
   } cases[] = {
      {{0, 1, 2}, FALSE},
      {{UINT64_C(0x7fffffff), UINT32_MAX - 1, UINT64_MAX}, FALSE},
      {{UINT64_MAX, UINT64_MAX, UINT64_MAX}, FALSE},
      {{UINT32_MAX, 0, 0}, TRUE},
      {{0, UINT32_MAX, 0}, TRUE},
      {{0, 0, UINT32_MAX}, TRUE},
      {{(UINT64_C(1) << 40) + 7, (UINT64_C(1) << 53) + 9, UINT64_MAX}, TRUE},
      {{UINT64_MAX - 1, UINT64_MAX, 0}, TRUE},
   };
   // Three key bytes deliberately leave both counter representations unaligned.
   char storage[sizeof(index_entry) + 3 + sizeof(btree_pivot_stats) + 1];
   for (uint64 i = 0; i < ARRAY_SIZE(cases); i++) {
      for (uint8 blob_flag = 0; blob_flag <= ONDISK_KEY_IS_BLOB;
           blob_flag += ONDISK_KEY_IS_BLOB)
      {
         memset(storage, 0xa5, sizeof(storage));
         index_entry *entry  = (index_entry *)storage;
         entry->pivot.length = 3;
         // Start with the opposite width to exercise both flag transitions.
         entry->pivot.flags =
            blob_flag | (cases[i].wide ? 0 : BTREE_INDEX_WIDE_STATS);
         memcpy(entry->pivot.bytes, "key", 3);
         index_entry_set_stats(entry, cases[i].stats);
         btree_pivot_stats actual = index_entry_stats(entry);
         ASSERT_EQUAL(cases[i].stats.num_kvs, actual.num_kvs);
         ASSERT_EQUAL(cases[i].stats.key_bytes, actual.key_bytes);
         ASSERT_EQUAL(cases[i].stats.message_bytes, actual.message_bytes);
         ASSERT_EQUAL(blob_flag | (cases[i].wide ? BTREE_INDEX_WIDE_STATS : 0),
                      entry->pivot.flags);
         ASSERT_EQUAL(0, memcmp(entry->pivot.bytes, "key", 3));
         uint64 size = cases[i].wide ? 24 : 12;
         ASSERT_EQUAL(sizeof(index_entry) + 3 + size,
                      sizeof_index_entry(entry));
         ASSERT_EQUAL(0xa5, (uint8)storage[sizeof_index_entry(entry)]);
         if (cases[i].wide) {
            uint64 raw[3];
            memcpy(raw, entry->pivot.bytes + 3, sizeof(raw));
            ASSERT_EQUAL(cases[i].stats.num_kvs, raw[0]);
            ASSERT_EQUAL(cases[i].stats.key_bytes, raw[1]);
            ASSERT_EQUAL(cases[i].stats.message_bytes, raw[2]);
         } else {
            uint32 raw[3];
            memcpy(raw, entry->pivot.bytes + 3, sizeof(raw));
            ASSERT_EQUAL((uint32)cases[i].stats.num_kvs, raw[0]);
            ASSERT_EQUAL((uint32)cases[i].stats.key_bytes, raw[1]);
            ASSERT_EQUAL((uint32)cases[i].stats.message_bytes, raw[2]);
         }
      }
   }
}

CTEST2(btree, test_blob_key_configuration)
{
   btree_config cfg                      = data->dbtree_cfg;
   data_config  dc                       = *cfg.data_cfg;
   dc.key_compare_is_lexical             = TRUE;
   cfg.data_cfg                          = &dc;
   cfg.blob_key_prefix_size              = 256;
   cfg.blob_cfg.blob_max_descriptor_size = 256;
   ASSERT_TRUE(SUCCESS(btree_config_validate(&cfg)));
   cfg.blob_key_prefix_size = 257;
   ASSERT_TRUE(STATUS_IS_EQ(btree_config_validate(&cfg), STATUS_BAD_PARAM));
   cfg.blob_key_prefix_size              = 0;
   cfg.blob_cfg.blob_max_descriptor_size = 512;
   ASSERT_TRUE(SUCCESS(btree_config_validate(&cfg)));
   cfg.blob_key_prefix_size = UINT64_C(65536);
   ASSERT_TRUE(STATUS_IS_EQ(btree_config_validate(&cfg), STATUS_BAD_PARAM));
   dc.key_compare_is_lexical = FALSE;
   ASSERT_TRUE(SUCCESS(btree_config_validate(&cfg)));
   ASSERT_EQUAL(0, btree_config_format(&cfg).key_prefix_size);
}
