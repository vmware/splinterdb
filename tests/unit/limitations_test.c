// Copyright 2022-2026 VMware, Inc.
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
#include "clockcache.h"
#include "data_internal.h"
#include "memtable.h"
#include "rc_allocator.h"
#include "task.h"
#include "functional/test.h"
#include "splinterdb/splinterdb.h"
#include "splinterdb/default_data_config.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.
#include "platform_threads.h"
#include "platform_units.h"

static void
create_default_cfg(splinterdb_config *out_cfg,
                   data_config       *default_data_cfg,
                   bool               use_shmem);

static int
limitations_merge_tuples(const data_config *cfg,
                         slice              key,
                         message            old_message,
                         merge_accumulator *new_message)
{
   (void)cfg;
   (void)key;
   (void)old_message;
   (void)new_message;
   return 0;
}

/*
 * Global data declaration macro:
 */
CTEST_DATA(limitations)
{
   // Declare head handles for io, allocator, cache and splinter allocation.
   platform_heap_id hid;

   // Config structs required, as per splinter_test() setup work.
   system_config *system_cfg;

   rc_allocator al;

   // Following get setup pointing to allocated memory
   io_handle             *io;
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
   platform_register_thread();
   // All test cases in this test usually deal with error handling
   set_log_streams_for_tests(MSG_LEVEL_ERRORS);

   uint64 heap_capacity = (512 * MiB);

   data->use_shmem = config_parse_use_shmem(Ctest_argc, (char **)Ctest_argv);
   // Create a heap for io, allocator, cache and splinter
   platform_status rc = platform_heap_create(
      platform_get_module_id(), heap_capacity, data->use_shmem, &data->hid);
   platform_assert_status_ok(rc);
}

/*
 * Tear down memory allocated for various sub-systems. Shutdown Splinter.
 */
CTEST_TEARDOWN(limitations)
{
   platform_heap_destroy(&data->hid);
   platform_deregister_thread();
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
   data->system_cfg =
      TYPED_ARRAY_MALLOC(data->hid, data->system_cfg, num_tables);

   ZERO_STRUCT(data->test_exec_cfg);

   rc = test_parse_args_n(data->system_cfg,
                          &data->test_exec_cfg,
                          NULL,
                          &data->gen,
                          num_tables,
                          Ctest_argc, // argc/argv globals setup by CTests
                          (char **)Ctest_argv);
   platform_assert_status_ok(rc);

   // Hard-fix the configured default page-size to an illegal value
   uint64 page_size_configured = data->system_cfg->io_cfg.page_size;
   ASSERT_EQUAL(page_size_configured, 4096);

   data->system_cfg->io_cfg.page_size = 2048;

   // This should fail.
   data->io = io_handle_create(&data->system_cfg->io_cfg, data->hid);
   ASSERT_TRUE(data->io == NULL);

   // This should fail.
   data->system_cfg->io_cfg.page_size = (page_size_configured * 2);
   data->io = io_handle_create(&data->system_cfg->io_cfg, data->hid);
   ASSERT_TRUE(data->io == NULL);

   // Restore, and now set extent-size to invalid value
   data->system_cfg->io_cfg.page_size = page_size_configured;

   // This should succeed, finally!.
   data->io = io_handle_create(&data->system_cfg->io_cfg, data->hid);
   ASSERT_TRUE(data->io != NULL);

   // Release resources acquired in this test case.
   io_handle_destroy(data->io);

   if (data->system_cfg) {
      platform_free(data->hid, data->system_cfg);
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
   data->system_cfg =
      TYPED_ARRAY_MALLOC(data->hid, data->system_cfg, num_tables);

   ZERO_STRUCT(data->test_exec_cfg);

   rc = test_parse_args_n(data->system_cfg,
                          &data->test_exec_cfg,
                          NULL,
                          &data->gen,
                          num_tables,
                          Ctest_argc, // argc/argv globals setup by CTests
                          (char **)Ctest_argv);
   platform_assert_status_ok(rc);

   uint64 pages_per_extent = (data->system_cfg->io_cfg.extent_size
                              / data->system_cfg->io_cfg.page_size);
   ASSERT_EQUAL(MAX_PAGES_PER_EXTENT,
                pages_per_extent,
                "pages_per_extent=%lu != MAX_PAGES_PER_EXTENT=%lu ",
                pages_per_extent,
                MAX_PAGES_PER_EXTENT);

   uint64 extent_size_configured = data->system_cfg->io_cfg.extent_size;

   // This should fail.
   data->system_cfg->io_cfg.extent_size = data->system_cfg->io_cfg.page_size;
   data->io = io_handle_create(&data->system_cfg->io_cfg, data->hid);
   ASSERT_TRUE(data->io == NULL);

   // Halving the # of pages/extent. This should fail.
   data->system_cfg->io_cfg.extent_size =
      (data->system_cfg->io_cfg.page_size * pages_per_extent) / 2;
   data->io = io_handle_create(&data->system_cfg->io_cfg, data->hid);
   ASSERT_TRUE(data->io == NULL);

   // Doubling the # of pages/extent. This should fail.
   data->system_cfg->io_cfg.extent_size =
      (data->system_cfg->io_cfg.page_size * pages_per_extent * 2);
   data->io = io_handle_create(&data->system_cfg->io_cfg, data->hid);
   ASSERT_TRUE(data->io == NULL);

   data->system_cfg->io_cfg.extent_size = extent_size_configured;

   // This should succeed, finally!.
   data->io = io_handle_create(&data->system_cfg->io_cfg, data->hid);
   ASSERT_TRUE(data->io != NULL);
   io_handle_destroy(data->io);

   if (data->system_cfg) {
      platform_free(data->hid, data->system_cfg);
   }
}

/*
 * Test creating SplinterDB with an invalid task system configuration.
 */
CTEST2(limitations, test_splinterdb_create_invalid_task_system_config)
{
   splinterdb       *kvsb = NULL;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(&default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   // Cannot use up all possible threads for just bg-threads.
   cfg.num_normal_bg_threads   = (MAX_THREADS - 1);
   cfg.num_memtable_bg_threads = 1;

   int rc = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);

   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.num_normal_bg_threads = UINT64_MAX;
   rc                        = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);

   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.num_memtable_bg_threads = UINT64_MAX;
   rc                          = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);
}

CTEST2(limitations, test_splinterdb_create_invalid_cache_size)
{
   splinterdb       *kvsb = NULL;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(&default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   cfg.cache_size++;
   int rc = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);

   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.cache_size = 65 * cfg.page_size;
   rc             = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);

   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.cache_size = (UINT64_C(1) << 32) * cfg.page_size;
   rc             = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);
}

CTEST2(limitations, test_splinterdb_create_invalid_fanout)
{
   splinterdb       *kvsb = NULL;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(&default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   cfg.fanout = 1;
   int rc     = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);

   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.fanout = (UINT64_C(1) << 32) + 1;
   rc         = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);

   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.fanout = trunk_max_fanout(cfg.page_size) + 1;
   rc         = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);

   /* The format-derived bound itself remains valid. */
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.fanout = trunk_max_fanout(cfg.page_size);
   rc         = splinterdb_create(&cfg, &kvsb);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(0, splinterdb_close(&kvsb, FALSE));
}

CTEST2(limitations, test_splinterdb_create_invalid_memtable_capacity)
{
   splinterdb       *kvsb = NULL;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(&default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.memtable_capacity = UINT64_MAX / MEMTABLE_SPACE_OVERHEAD_FACTOR + 1;

   int rc = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);
}

CTEST2(limitations, test_memtable_capacity_rounds_up_to_one_extent)
{
   io_config io_cfg;
   ZERO_CONTENTS(&io_cfg);
   io_cfg.page_size   = 4 * KiB;
   io_cfg.extent_size = 128 * KiB;

   clockcache_config cache_cfg;
   clockcache_config_init(
      &cache_cfg, &io_cfg, 64 * io_cfg.page_size, "", FALSE);

   data_config data_cfg;
   default_data_config_init(&data_cfg);
   btree_config btree_cfg;
   btree_config_init(&btree_cfg, &cache_cfg.super, &data_cfg);

   memtable_config mt_cfg;
   platform_status rc = memtable_config_init(&mt_cfg, &btree_cfg, 1, 1);
   ASSERT_EQUAL(STATUS_OK.r, rc.r);
   ASSERT_EQUAL(1, mt_cfg.max_extents_per_memtable);
}

CTEST2(limitations, test_splinterdb_create_invalid_data_callbacks)
{
   splinterdb       *kvsb = NULL;
   splinterdb_config cfg;
   data_config       valid;
   data_config       invalid;

   default_data_config_init(&valid);

   invalid             = valid;
   invalid.key_compare = NULL;
   create_default_cfg(&cfg, &invalid, data->use_shmem);
   ASSERT_NOT_EQUAL(0, splinterdb_create(&cfg, &kvsb));

   invalid          = valid;
   invalid.key_hash = NULL;
   create_default_cfg(&cfg, &invalid, data->use_shmem);
   ASSERT_NOT_EQUAL(0, splinterdb_create(&cfg, &kvsb));

   invalid               = valid;
   invalid.key_to_string = NULL;
   create_default_cfg(&cfg, &invalid, data->use_shmem);
   ASSERT_NOT_EQUAL(0, splinterdb_create(&cfg, &kvsb));

   invalid                   = valid;
   invalid.message_to_string = NULL;
   create_default_cfg(&cfg, &invalid, data->use_shmem);
   ASSERT_NOT_EQUAL(0, splinterdb_create(&cfg, &kvsb));

   invalid              = valid;
   invalid.merge_tuples = limitations_merge_tuples;
   create_default_cfg(&cfg, &invalid, data->use_shmem);
   ASSERT_NOT_EQUAL(0, splinterdb_create(&cfg, &kvsb));

   invalid                    = valid;
   invalid.merge_tuples       = NULL;
   invalid.merge_tuples_final = NULL;

   merge_accumulator accumulator;
   message           update =
      message_create_internal(MESSAGE_TYPE_UPDATE, NULL, slice_create(1, "u"));
   ASSERT_TRUE(merge_accumulator_init_from_message(
      &accumulator, PROCESS_PRIVATE_HEAP_ID, update));
   key     tuple_key = key_create_from_slice(FALSE, slice_create(1, "k"));
   message old_message =
      message_create_internal(MESSAGE_TYPE_INSERT, NULL, slice_create(1, "v"));
   ASSERT_NOT_EQUAL(
      0, data_merge_tuples(&invalid, tuple_key, old_message, &accumulator));
   ASSERT_NOT_EQUAL(0,
                    data_merge_tuples_final(&invalid, tuple_key, &accumulator));
   merge_accumulator_deinit(&accumulator);

   create_default_cfg(&cfg, &invalid, data->use_shmem);
   ASSERT_EQUAL(0, splinterdb_create(&cfg, &kvsb));
   ASSERT_EQUAL(STATUS_BAD_PARAM.r,
                splinterdb_update(
                   kvsb, slice_create(1, "k"), message_create(1, "v"), NULL));
   ASSERT_EQUAL(0, splinterdb_close(&kvsb, FALSE));
}

CTEST2(limitations, test_allocation_exhaustion_returns_error)
{
   splinterdb       *kvsb = NULL;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(&default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.disk_size = 4 * MiB;

   /* This fills part of the memtable ring before a later mini-allocator
    * reservation fails.  Repeating it also exercises the complete unwind. */
   for (uint64 attempt = 0; attempt < 2; attempt++) {
      int rc = splinterdb_create(&cfg, &kvsb);
      ASSERT_EQUAL(STATUS_NO_SPACE.r, rc);
      ASSERT_NULL(kvsb);
   }
}

CTEST2(limitations, test_runtime_allocation_exhaustion_returns_error)
{
   splinterdb *kvsb = NULL;
   data_config default_data_cfg;

   default_data_config_init(&default_data_cfg);
   const uint64 disk_sizes[] = {
      8 * MiB,  /* branch packing runs out of extents */
      15 * MiB, /* recycling an incorporated memtable runs out */
   };

   for (uint64 size_no = 0; size_no < ARRAY_SIZE(disk_sizes); size_no++) {
      splinterdb_config cfg;
      create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
      cfg.disk_size         = disk_sizes[size_no];
      cfg.memtable_capacity = 1 * MiB;
      ASSERT_EQUAL(0, splinterdb_create(&cfg, &kvsb));

      char key[32];
      char value[1024];
      memset(value, 'v', sizeof(value));
      int rc = 0;
      for (uint64 i = 0; i < 10000 && rc == 0; i++) {
         int key_len = snprintf(key, sizeof(key), "%020lu", i);
         ASSERT_TRUE(0 < key_len && (uint64)key_len < sizeof(key));
         rc = splinterdb_insert(kvsb,
                                slice_create((uint64)key_len, key),
                                message_create(sizeof(value), value),
                                NULL);
      }
      ASSERT_EQUAL(STATUS_NO_SPACE.r, rc);

      /* The forced close may report that an unlogged in-memory memtable was
       * not preserved, but it must release the handle without an assertion or
       * stall. */
      (void)splinterdb_close(&kvsb, TRUE);
      ASSERT_NULL(kvsb);
   }
}

/*
 * Filter widths are public uint64 values, while the routing-filter format uses
 * uint32 fields. Reject invalid values before narrowing them or using the
 * index width as a shift count.
 */
CTEST2(limitations, test_splinterdb_create_invalid_filter_config)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(&default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   cfg.filter_hash_size = 33;
   int rc               = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);

   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.filter_log_index_size = 32;
   rc                        = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);

   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.filter_hash_size      = 8;
   cfg.filter_log_index_size = 9;
   rc                        = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);

   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.filter_hash_size = (UINT64_C(1) << 32) + 26;
   rc                   = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);

   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.filter_log_index_size = (UINT64_C(1) << 32) + 9;
   rc                        = splinterdb_create(&cfg, &kvsb);
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

   default_data_config_init(&default_data_cfg);
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

   default_data_config_init(&default_data_cfg);
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

   default_data_config_init(&default_data_cfg);
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

   default_data_config_init(&default_data_cfg);
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

   default_data_config_init(&default_data_cfg);
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

   default_data_config_init(&default_data_cfg);
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

CTEST2(limitations, test_zero_cache_size)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   default_data_config_init(&default_data_cfg);
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

   default_data_config_init(&default_data_cfg);
   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);

   cfg.filename = "/dev/null/this-file-cannot-possibly-be-opened";

   // this will fail, but shouldn't crash!
   int rc = splinterdb_create(&cfg, &kvsb);
   ASSERT_NOT_EQUAL(0, rc);
   // if we've made it this far, at least the application can report
   // the error and recover!
}

CTEST2(limitations, test_oversized_paths_return_bad_param)
{
   splinterdb       *kvsb = NULL;
   splinterdb_config cfg;
   data_config       default_data_cfg;
   char              oversized_path[MAX_STRING_LENGTH + 1];

   memset(oversized_path, 'x', sizeof(oversized_path) - 1);
   oversized_path[sizeof(oversized_path) - 1] = '\0';
   default_data_config_init(&default_data_cfg);

   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.filename = oversized_path;
   ASSERT_EQUAL(STATUS_BAD_PARAM.r, splinterdb_create(&cfg, &kvsb));
   ASSERT_NULL(kvsb);

   create_default_cfg(&cfg, &default_data_cfg, data->use_shmem);
   cfg.cache_logfile = oversized_path;
   ASSERT_EQUAL(STATUS_BAD_PARAM.r, splinterdb_create(&cfg, &kvsb));
   ASSERT_NULL(kvsb);
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
      (splinterdb_config){.filename    = TEST_CONFIG_DEFAULT_IO_FILENAME,
                          .cache_size  = 64 * Mega,
                          .disk_size   = 127 * Mega,
                          .page_size   = TEST_CONFIG_DEFAULT_PAGE_SIZE,
                          .extent_size = TEST_CONFIG_DEFAULT_EXTENT_SIZE,
                          .use_shmem   = use_shmem,
                          .data_cfg    = default_data_cfg};
}
