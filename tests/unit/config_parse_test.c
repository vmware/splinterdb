// Copyright 2022-2026 VMware, Inc.
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
#include "core.h"
#include "functional/test.h"
#include "test_common.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

/*
 * Global data declaration macro:
 */
CTEST_DATA(config_parse)
{
   // Declare head handles for io, allocator, cache and splinter allocation.
   platform_heap_id hid;
   test_exec_config test_exec_cfg;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(config_parse)
{
   platform_register_thread();
   uint64 heap_capacity = (1024 * MiB);
   // Create a heap for io, allocator, cache and splinter
   platform_status rc = platform_heap_create(
      platform_get_module_id(), heap_capacity, FALSE, &data->hid);
   platform_assert_status_ok(rc);

   ZERO_STRUCT(data->test_exec_cfg);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(config_parse)
{
   platform_heap_destroy(&data->hid);
   platform_deregister_thread();
}

/*
 * Basic test case.
 */
CTEST2(config_parse, test_basic_parsing)
{
   // Following get setup pointing to allocated memory
   system_config         *system_cfg = NULL;
   test_message_generator gen;

   int num_tables = 1;

   // Allocate memory for global config structures
   system_cfg = TYPED_ARRAY_MALLOC(data->hid, system_cfg, num_tables);

   platform_status rc;

   rc = test_parse_args_n(system_cfg,
                          &data->test_exec_cfg,
                          &gen,
                          num_tables,
                          Ctest_argc, // argc/argv globals setup by CTests
                          (char **)Ctest_argv);
   platform_assert_status_ok(rc);

   ASSERT_TRUE(system_cfg->splinter_cfg.use_stats,
               "Parameter '%s' expected. ",
               "--stats");
   ASSERT_TRUE(
      system_cfg->splinter_cfg.use_log, "Parameter '%s' expected. ", "--log");
   ASSERT_TRUE(system_cfg->splinter_cfg.verbose_logging_enabled,
               "Parameter '%s' expected. ",
               "--verbose-logging");

   int num_inserts = 20;
   ASSERT_EQUAL(num_inserts,
                data->test_exec_cfg.num_inserts,
                "Parameter '%s' expected. ",
                "--num-inserts 20");

   ASSERT_TRUE(test_show_verbose_progress(&data->test_exec_cfg),
               "Parameter '%s' expected. ",
               "--verbose-progress");

   platform_free(data->hid, system_cfg);
}
