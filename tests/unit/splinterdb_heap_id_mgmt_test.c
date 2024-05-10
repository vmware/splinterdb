// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinterdb_heap_id_mgmt_test.c
 *
 * This is a very specific white-box test to exercise splinterdb_create() and
 * splinterdb_close() interfaces, to exercise the handling of platform heap
 * in the presence of error paths. Usually, the platform_heap is created when
 * splinterdb_create() is done, and the heap is destroyed upon close. There is
 * another way for the calling code to create the platform-heap externally.
 * The cases in this test verify that we can create(), close() and reopen
 * SplinterDB even while managing the externally created platform heap.
 *
 * Tests for the other code-path, where splinterdb_create() itself creates
 * the platform heap, and verifying that the heap is correctly managed in the
 * face of errors exist in other unit-tests, e.g, splinterdb_quick_test.c,
 * task_system_test.c etc.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/splinterdb.h"
#include "splinterdb/default_data_config.h"
#include "config.h"
#include "unit_tests.h"

#define TEST_MAX_KEY_SIZE 20

static void
create_default_cfg(splinterdb_config *out_kvs_cfg,
                   data_config       *default_data_cfg,
                   bool               use_shmem);

/*
 * Global data declaration macro:
 */
CTEST_DATA(splinterdb_heap_id_mgmt)
{
   data_config       default_data_cfg;
   platform_heap_id  hid;
   splinterdb_config kvs_cfg;
   splinterdb       *kvs;
   size_t            shmem_size; // for assert validation
};

CTEST_SETUP(splinterdb_heap_id_mgmt)
{
   platform_status rc = STATUS_OK;

   bool use_shmem = config_parse_use_shmem(Ctest_argc, (char **)Ctest_argv);

   uint64 heap_capacity = (256 * MiB); // small heap is sufficient.
   default_data_config_init(TEST_MAX_KEY_SIZE, &data->default_data_cfg);

   memset(&data->kvs_cfg, 0, sizeof(data->kvs_cfg));
   create_default_cfg(&data->kvs_cfg, &data->default_data_cfg, use_shmem);

   platform_module_id mid = platform_get_module_id();

   rc = platform_heap_create(
      mid, heap_capacity, use_shmem, &data->kvs_cfg.heap_id);
   platform_assert_status_ok(rc);

   // Remember the heap-ID created here; used to cross-check in assertions
   data->hid = data->kvs_cfg.heap_id;
   if (data->kvs_cfg.use_shmem) {
      data->shmem_size = platform_shmsize(data->hid);
   }
}

CTEST_TEARDOWN(splinterdb_heap_id_mgmt)
{
   platform_heap_destroy(&data->kvs_cfg.heap_id);
}

/*
 * Test create and close interfaces.
 */
CTEST2(splinterdb_heap_id_mgmt, test_create_close)
{
   int rc = splinterdb_create(&data->kvs_cfg, &data->kvs);
   ASSERT_EQUAL(0, rc);

   ASSERT_EQUAL((size_t)data->hid, (size_t)data->kvs_cfg.heap_id);

   splinterdb_close(&data->kvs);

   // As we created the heap externally, in this test, close should not
   // have destroyed the heap. Exercise interfaces which will seg-fault
   // otherwise.
   if (data->kvs_cfg.use_shmem) {
      ASSERT_EQUAL(data->shmem_size, platform_shmsize(data->hid));
   }
}

/*
 * Test create, close followed by re-open of SplinterDB. Re-use the previously
 * setup splinterdb_config{} struct, which should have a handle to the platform
 * heap. The same (shared memory) heap should be re-used.
 */
CTEST2(splinterdb_heap_id_mgmt, test_create_close_and_reopen)
{
   int rc = splinterdb_create(&data->kvs_cfg, &data->kvs);
   ASSERT_EQUAL(0, rc);

   splinterdb_close(&data->kvs);

   rc = splinterdb_open(&data->kvs_cfg, &data->kvs);

   // The heap ID should not change.
   ASSERT_EQUAL((size_t)data->hid, (size_t)data->kvs_cfg.heap_id);

   splinterdb_close(&data->kvs);
   // Shared memory heap should still be around and accessible.
   if (data->kvs_cfg.use_shmem) {
      ASSERT_EQUAL(data->shmem_size, platform_shmsize(data->hid));
   }
}

/*
 * Test error path while creating SplinterDB which can trip-up due to a bad
 * configuration. Induce such an error and verify that platform heap is not
 * messed-up due to backout code.
 */
CTEST2(splinterdb_heap_id_mgmt, test_failed_init_config)
{
   size_t save_cache_size   = data->kvs_cfg.cache_size;
   data->kvs_cfg.cache_size = 0;
   int rc                   = splinterdb_create(&data->kvs_cfg, &data->kvs);
   ASSERT_NOT_EQUAL(0, rc);

   data->kvs_cfg.cache_size = save_cache_size;

   // The heap ID should not change.
   ASSERT_EQUAL((size_t)data->hid, (size_t)data->kvs_cfg.heap_id);

   // Shared memory heap should still be around and accessible.
   if (data->kvs_cfg.use_shmem) {
      ASSERT_EQUAL(data->shmem_size, platform_shmsize(data->hid));
   }
}

/*
 * Helper routine to create a valid Splinter configuration using default
 * page- and extent-size. Shared-memory usage is OFF by default.
 */
static void
create_default_cfg(splinterdb_config *out_kvs_cfg,
                   data_config       *default_data_cfg,
                   bool               use_shmem)
{
   *out_kvs_cfg =
      (splinterdb_config){.filename    = TEST_DB_NAME,
                          .cache_size  = 64 * Mega,
                          .disk_size   = 127 * Mega,
                          .page_size   = TEST_CONFIG_DEFAULT_PAGE_SIZE,
                          .extent_size = TEST_CONFIG_DEFAULT_EXTENT_SIZE,
                          .use_shmem   = use_shmem,
                          .data_cfg    = default_data_cfg};
}
