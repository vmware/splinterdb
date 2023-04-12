// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * limitations_test.c --
 *
 * Exercises SplinterDB configuration interfaces with unsupported parameters
 * and confirm that we are not able to sneak-through. This would, otherwise,
 * lead to configuring Splinter instance with parameters that are either
 * unworkable, or currently unsupported.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "trunk.h"
#include "clockcache.h"
#include "allocator.h"
#include "task.h"
#include "functional/test.h"
#include "functional/test_async.h"
#include "splinterdb/splinterdb.h"
#include "splinterdb/default_data_config.h"
#include "test_misc_common.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

#define TEST_MAX_KEY_SIZE 13

static void
create_default_cfg(splinterdb_config *out_cfg,
                   data_config       *default_data_cfg,
                   bool               use_shmem);

static platform_status
parse_cmdline_args(void *datap, int unit_test_argc, char **unit_test_argv);

/*
 * Global data declaration macro:
 */
CTEST_DATA(limitations)
{
   // Declare head handles for io, allocator, cache and splinter allocation.
   platform_heap_handle hh;
   platform_heap_id     hid;

   // Config structs required, as per splinter_test() setup work.
   io_config          io_cfg;
   allocator_config   al_cfg;
   shard_log_config   log_cfg;
   task_system_config task_cfg;

   rc_allocator al;

   // Following get setup pointing to allocated memory
   trunk_config          *splinter_cfg;
   data_config           *data_cfg;
   clockcache_config     *cache_cfg;
   platform_io_handle    *io;
   clockcache            *clock_cache;
   task_system           *tasks;
   test_message_generator gen;

   // Test execution related configuration
   test_exec_config test_exec_cfg;
   bool             use_shmem;
};

/*
 * Setup heap memory to be used later to test Splinter configuration.
 * splinter_test().
 */
CTEST_SETUP(limitations)
{
   // All test cases in this test usually deal with error handling
   set_log_streams_for_tests(MSG_LEVEL_ERRORS);

   uint64 heap_capacity = (1 * GiB);

   data->use_shmem = test_using_shmem(Ctest_argc, (char **)Ctest_argv);

   // Create a heap for io, allocator, cache and splinter

   platform_status rc = platform_heap_create(platform_get_module_id(),
                                             heap_capacity,
                                             data->use_shmem,
                                             &data->hh,
                                             &data->hid);
   platform_assert_status_ok(rc);
}

/*
 * Tear down memory allocated for various sub-systems. Shutdown Splinter.
 */
CTEST_TEARDOWN(limitations)
{
   platform_status rc = platform_heap_destroy(&data->hh);
   ASSERT_TRUE(SUCCESS(rc));
}

/*
 * **************************************************************************
 * Basic test case to verify that an attempt to go through lower-level
 * Splinter sub-system initializtion routines will correctly trap invalid
 * page- / extent-size parameters.
 * **************************************************************************
 */
CTEST2(limitations, test_io_init_invalid_page_size)
{
   platform_status rc;
   uint64          num_tables = 1;

   // Allocate memory for global config structures
   platform_memfrag memfrag_splinter_cfg;
   data->splinter_cfg = TYPED_ARRAY_MALLOC_MF(
      data->hid, data->splinter_cfg, num_tables, &memfrag_splinter_cfg);

   platform_memfrag memfrag_cache_cfg;
   data->cache_cfg = TYPED_ARRAY_MALLOC_MF(
      data->hid, data->cache_cfg, num_tables, &memfrag_cache_cfg);

   ZERO_STRUCT(data->test_exec_cfg);

   rc = test_parse_args_n(data->splinter_cfg,
                          &data->data_cfg,
                          &data->io_cfg,
                          &data->al_cfg,
                          data->cache_cfg,
                          &data->log_cfg,
                          &data->task_cfg,
                          &data->test_exec_cfg,
                          &data->gen,
                          num_tables,
                          Ctest_argc, // argc/argv globals setup by CTests
                          (char **)Ctest_argv);
   platform_assert_status_ok(rc);

   // Allocate and initialize the IO sub-system.
   data->io = TYPED_MALLOC(data->hid, data->io);
   ASSERT_TRUE((data->io != NULL));

   // Hard-fix the configured default page-size to an illegal value
   uint64 page_size_configured = data->io_cfg.page_size;
   ASSERT_EQUAL(page_size_configured, 4096);

   data->io_cfg.page_size = 2048;

   // This should fail.
   rc = io_handle_init(data->io, &data->io_cfg, data->hh, data->hid);
   ASSERT_FALSE(SUCCESS(rc));

   // This should fail.
   data->io_cfg.page_size = (page_size_configured * 2);
   rc = io_handle_init(data->io, &data->io_cfg, data->hh, data->hid);
   ASSERT_FALSE(SUCCESS(rc));

   // Restore, and now set extent-size to invalid value
   data->io_cfg.page_size = page_size_configured;

   // This should succeed, finally!.
   rc = io_handle_init(data->io, &data->io_cfg, data->hh, data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   // Release resources acquired in this test case.
   io_handle_deinit(data->io);
   platform_free(data->hid, data->io);

   platform_memfrag *mf = NULL;
   if (data->cache_cfg) {
      mf = &memfrag_cache_cfg;
      platform_free(data->hid, mf);
   }

   if (data->splinter_cfg) {
      mf = &memfrag_splinter_cfg;
      platform_free(data->hid, mf);
   }
}

/*
 * Test case to verify that we fail to initialize the IO sub-system with
 * an invalid extent-size. Page-size is left as configured, and we diddle
 * with extent size to verify error handling.
 */
CTEST2(limitations, test_io_init_invalid_extent_size)
{
   platform_status rc;
   uint64          num_tables = 1;

   // Allocate memory for global config structures
   platform_memfrag memfrag_splinter_cfg;
   data->splinter_cfg = TYPED_ARRAY_MALLOC_MF(
      data->hid, data->splinter_cfg, num_tables, &memfrag_splinter_cfg);

   platform_memfrag memfrag_cache_cfg;
   data->cache_cfg = TYPED_ARRAY_MALLOC_MF(
      data->hid, data->cache_cfg, num_tables, &memfrag_cache_cfg);

   ZERO_STRUCT(data->test_exec_cfg);

   rc = test_parse_args_n(data->splinter_cfg,
                          &data->data_cfg,
                          &data->io_cfg,
                          &data->al_cfg,
                          data->cache_cfg,
                          &data->log_cfg,
                          &data->task_cfg,
                          &data->test_exec_cfg,
                          &data->gen,
                          num_tables,
                          Ctest_argc, // argc/argv globals setup by CTests
                          (char **)Ctest_argv);
   platform_assert_status_ok(rc);

   // Allocate and initialize the IO sub-system.
   data->io = TYPED_MALLOC(data->hid, data->io);
   ASSERT_TRUE((data->io != NULL));

   uint64 pages_per_extent =
      (data->io_cfg.extent_size / data->io_cfg.page_size);
   ASSERT_EQUAL(MAX_PAGES_PER_EXTENT,
                pages_per_extent,
                "pages_per_extent=%lu != MAX_PAGES_PER_EXTENT=%lu ",
                pages_per_extent,
                MAX_PAGES_PER_EXTENT);

   uint64 extent_size_configured = data->io_cfg.extent_size;

   // This should fail.
   data->io_cfg.extent_size = data->io_cfg.page_size;
   rc = io_handle_init(data->io, &data->io_cfg, data->hh, data->hid);
   ASSERT_FALSE(SUCCESS(rc));

   // Halving the # of pages/extent. This should fail.
   data->io_cfg.extent_size = (data->io_cfg.page_size * pages_per_extent) / 2;
   rc = io_handle_init(data->io, &data->io_cfg, data->hh, data->hid);
   ASSERT_FALSE(SUCCESS(rc));

   // Doubling the # of pages/extent. This should fail.
   data->io_cfg.extent_size = (data->io_cfg.page_size * pages_per_extent * 2);
   rc = io_handle_init(data->io, &data->io_cfg, data->hh, data->hid);
   ASSERT_FALSE(SUCCESS(rc));

   data->io_cfg.extent_size = extent_size_configured;

   // This should succeed, finally!.
   rc = io_handle_init(data->io, &data->io_cfg, data->hh, data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   // Release resources acquired in this test case.
   io_handle_deinit(data->io);

   platform_memfrag *mf = NULL;
   if (data->cache_cfg) {
      mf = &memfrag_cache_cfg;
      platform_free(data->hid, mf);
   }

   if (data->splinter_cfg) {
      mf = &memfrag_splinter_cfg;
      platform_free(data->hid, mf);
   }
}

/*
 * Test creating SplinterDB with an invalid task system configuration.
 */
CTEST2(limitations, test_splinterdb_create_invalid_task_system_config)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(TEST_MAX_KEY_SIZE, &default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   // Cannot use up all possible threads for just bg-threads.
   cfg.num_normal_bg_threads   = (MAX_THREADS - 1);
   cfg.num_memtable_bg_threads = 1;

   int rc = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);
}

/*
 * Test splinterdb_*() interfaces with invalid page- / extent-size
 * configurations, and verify that they fail correctly.
 */
CTEST2(limitations, test_splinterdb_create_invalid_page_size)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(TEST_MAX_KEY_SIZE, &default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   uint64 page_size_configured = cfg.page_size;

   // Futz around with invalid page sizes.
   cfg.page_size = (2 * KiB);
   int rc        = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);

   cfg.page_size = (2 * page_size_configured);
   rc            = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);
}

CTEST2(limitations, test_splinterdb_create_invalid_extent_size)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(TEST_MAX_KEY_SIZE, &default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   uint64 extent_size_configured = cfg.extent_size;

   // Futz around with invalid extent sizes.
   cfg.extent_size = (extent_size_configured / 2);
   int rc          = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);

   cfg.extent_size = (extent_size_configured * 2);
   rc              = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);
}

/*
 * Negative test-case to verify that we are properly detecting an
 * insufficient disk-size. (This was discovered while building C-sample
 * programs; basically a user-error.)
 */
CTEST2(limitations, test_create_zero_disk_size)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(TEST_MAX_KEY_SIZE, &default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   // Hard-fix this, to see if an error is raised.
   cfg.disk_size = 0;

   int rc = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);
}

CTEST2(limitations, test_create_zero_extent_capacity)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(TEST_MAX_KEY_SIZE, &default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   // Hard-fix this to some non-zero value, to see if an error is raised.
   cfg.disk_size = 256; // bytes

   int rc = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);
}

CTEST2(limitations, test_disk_size_not_integral_multiple_of_page_size)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(TEST_MAX_KEY_SIZE, &default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   // Hard-fix this to some non-integral multiple of configured page-size.
   // Will trip an internal check that validates that disk-capacity specified
   // can be carved up into exact # of pages.
   cfg.disk_size = (cfg.page_size * 100) + (cfg.page_size / 2);

   int rc = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);
}

CTEST2(limitations, test_disk_size_not_integral_multiple_of_extents)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(TEST_MAX_KEY_SIZE, &default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   // Hard-fix this to some non-integral multiple of configured extent-size.
   // Will trip an internal check that validates that disk-capacity specified
   // can be carved up into exact # of extents. Configure the disk-size so
   // that it _is_ a multiple of page-size, thereby, moving past the checks
   // verified by test_disk_size_not_integral_multiple_of_page_size() case.
   cfg.disk_size = (cfg.extent_size * 100) + (cfg.page_size);

   int rc = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);
}

/*
 * **************************************************************************
 * Test that an invalid configuration that makes trunk node configuration
 * impractical fails correctly with an error message. We try out few diff
 * config params that go into error checks in trunk_config_init().
 * **************************************************************************
 */
CTEST2(limitations, test_trunk_config_init_fails_for_invalid_configs)
{
   platform_status rc;
   uint64          num_tables = 1;

   // Allocate memory for global config structures
   platform_memfrag memfrag_splinter_cfg;
   data->splinter_cfg = TYPED_ARRAY_MALLOC_MF(
      data->hid, data->splinter_cfg, num_tables, &memfrag_splinter_cfg);

   platform_memfrag memfrag_cache_cfg;
   data->cache_cfg = TYPED_ARRAY_MALLOC_MF(
      data->hid, data->cache_cfg, num_tables, &memfrag_cache_cfg);

   char *unit_test_argv0[] = {"--key-size", "1000"};
   int   unit_test_argc    = ARRAY_SIZE(unit_test_argv0);

   char **unit_test_argv = unit_test_argv0;
   rc = parse_cmdline_args(data, unit_test_argc, unit_test_argv);
   ASSERT_FALSE(SUCCESS(rc));

   char *unit_test_argv1[] = {"--page-size", "4096", "--fanout", "100"};
   unit_test_argc          = ARRAY_SIZE(unit_test_argv1);

   unit_test_argv = unit_test_argv1;
   rc             = parse_cmdline_args(data, unit_test_argc, unit_test_argv);
   ASSERT_FALSE(SUCCESS(rc));

   char *unit_test_argv2[] = {"--max-branches-per-node", "200"};
   unit_test_argc          = ARRAY_SIZE(unit_test_argv2);

   unit_test_argv = unit_test_argv2;
   rc             = parse_cmdline_args(data, unit_test_argc, unit_test_argv);
   ASSERT_FALSE(SUCCESS(rc));

   // Release resources acquired in this test case.
   platform_memfrag *mf = NULL;
   if (data->cache_cfg) {
      mf = &memfrag_cache_cfg;
      platform_free(data->hid, mf);
   }

   if (data->splinter_cfg) {
      mf = &memfrag_splinter_cfg;
      platform_free(data->hid, mf);
   }
}

CTEST2(limitations, test_zero_cache_size)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(TEST_MAX_KEY_SIZE, &default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   // Hard-fix this to an illegal value.
   // We need more error checking in clockcache_init(), for totally bogus
   // configured cache sizes; like, say, 256 or some random number. Leave all
   // that for another day.
   cfg.cache_size = 0;

   int rc = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);
}
/*
 * Check that errors on file-opening are returned, not asserted.
 * Previously, a user error, e.g. bad file permissions, would
 * just crash the program.
 */
CTEST2(limitations, test_file_error_returns)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(TEST_MAX_KEY_SIZE, &default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   cfg.filename = "/dev/null/this-file-cannot-possibly-be-opened";

   // this will fail, but shouldn't crash!
   int rc = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);
   // if we've made it this far, at least the application can report
   // the error and recover!
}

/*
 * Helper routine to create a valid Splinter configuration using default
 * page- and extent-size. Shared-memory usage is OFF by default.
 */
static void
create_default_cfg(splinterdb_config *out_cfg,
                   data_config       *default_data_cfg,
                   bool               use_shmem)
{
   *out_cfg =
      (splinterdb_config){.filename    = TEST_DB_NAME,
                          .cache_size  = 64 * Mega,
                          .disk_size   = 127 * Mega,
                          .page_size   = TEST_CONFIG_DEFAULT_PAGE_SIZE,
                          .extent_size = TEST_CONFIG_DEFAULT_EXTENT_SIZE,
                          .use_shmem   = use_shmem,
                          .data_cfg    = default_data_cfg};
}

/*
 * Helper function to parse command-line arguments to setup the configuration
 * for SplinterDB.
 */
static platform_status
parse_cmdline_args(void *datap, int unit_test_argc, char **unit_test_argv)
{
   // Cast void * datap to ptr-to-CTEST_DATA() struct in use.
   struct CTEST_IMPL_DATA_SNAME(limitations) *data =
      (struct CTEST_IMPL_DATA_SNAME(limitations) *)datap;

   ZERO_STRUCT(data->test_exec_cfg);

   uint64 num_memtable_bg_threads_unused = 0;
   uint64 num_normal_bg_threads_unused   = 0;
   uint64 seed                           = 0;

   platform_status rc = test_parse_args(data->splinter_cfg,
                                        &data->data_cfg,
                                        &data->io_cfg,
                                        &data->al_cfg,
                                        data->cache_cfg,
                                        &data->log_cfg,
                                        &data->task_cfg,
                                        &seed,
                                        &data->gen,
                                        &num_memtable_bg_threads_unused,
                                        &num_normal_bg_threads_unused,
                                        unit_test_argc,
                                        unit_test_argv);
   return rc;
}
