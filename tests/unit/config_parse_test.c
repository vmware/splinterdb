// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * config_parse_test.c --
 *
 *  Exercises the interfaces to parse test-configuration parameters.
 *  Do some basic validation that parsing is returning the right result.
 *
 * NOTE:
 *  - This test case cannot be executed stand-alone. It needs specific
 *    --<config-options> that need to be supplied to it, as we will be
 *    checking for the result of the config-parsing routines.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "trunk.h"
#include "clockcache.h"
#include "allocator.h"
#include "rc_allocator.h"
#include "shard_log.h"
#include "task.h"
#include "functional/test.h"
#include "functional/test_async.h"
#include "test_common.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

/*
 * Global data declaration macro:
 */
CTEST_DATA(config_parse)
{
   // Declare head handles for io, allocator, cache and splinter allocation.
   platform_heap_handle hh;
   platform_heap_id     hid;
   test_exec_config     test_exec_cfg;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(config_parse)
{
   uint64 heap_capacity = (1024 * MiB);
   // Create a heap for io, allocator, cache and splinter
   platform_status rc = platform_heap_create(
      platform_get_module_id(), heap_capacity, FALSE, &data->hh, &data->hid);
   platform_assert_status_ok(rc);

   ZERO_STRUCT(data->test_exec_cfg);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(config_parse)
{
   platform_status rc = platform_heap_destroy(&data->hh);
   ASSERT_TRUE(SUCCESS(rc));
}

/*
 * Basic test case.
 */
CTEST2(config_parse, test_basic_parsing)
{
   // Config structs required, as per splinter_test() setup work.
   io_config          io_cfg;
   allocator_config   al_cfg;
   shard_log_config   log_cfg;
   task_system_config task_cfg;

   // Following get setup pointing to allocated memory
   trunk_config          *splinter_cfg = NULL;
   data_config           *data_cfg     = NULL;
   clockcache_config     *cache_cfg    = NULL;
   test_message_generator gen;

   int num_tables = 1;

   // Allocate memory for global config structures
   platform_memfrag memfrag_splinter_cfg;
   splinter_cfg = TYPED_ARRAY_MALLOC(data->hid, splinter_cfg, num_tables);

   platform_memfrag memfrag_cache_cfg;
   cache_cfg = TYPED_ARRAY_MALLOC(data->hid, cache_cfg, num_tables);

   platform_status rc;

   rc = test_parse_args_n(splinter_cfg,
                          &data_cfg,
                          &io_cfg,
                          &al_cfg,
                          cache_cfg,
                          &log_cfg,
                          &task_cfg,
                          &data->test_exec_cfg,
                          &gen,
                          num_tables,
                          Ctest_argc, // argc/argv globals setup by CTests
                          (char **)Ctest_argv);
   platform_assert_status_ok(rc);

   // Check parsing of some key --config-options expected by diff sub-systems
   int max_branches_per_node = 42;
   ASSERT_EQUAL(max_branches_per_node,
                splinter_cfg->max_branches_per_node,
                "Parameter '%s' expected. ",
                "--max-branches-per-node 42");

   ASSERT_TRUE(splinter_cfg->use_stats, "Parameter '%s' expected. ", "--stats");
   ASSERT_TRUE(splinter_cfg->use_log, "Parameter '%s' expected. ", "--log");
   ASSERT_TRUE(splinter_cfg->verbose_logging_enabled,
               "Parameter '%s' expected. ",
               "--verbose-logging");

   int btree_rough_count_height = 11;
   ASSERT_EQUAL(btree_rough_count_height,
                splinter_cfg->btree_cfg.rough_count_height,
                "Parameter '%s' expected. ",
                "--rough-count-height 11");

   int num_inserts = 20;
   ASSERT_EQUAL(num_inserts,
                data->test_exec_cfg.num_inserts,
                "Parameter '%s' expected. ",
                "--num-inserts 20");

   ASSERT_TRUE(test_show_verbose_progress(&data->test_exec_cfg),
               "Parameter '%s' expected. ",
               "--verbose-progress");

   platform_memfrag *mf = &memfrag_cache_cfg;
   platform_free(data->hid, mf);
   mf = &memfrag_splinter_cfg;
   platform_free(data->hid, mf);
}
