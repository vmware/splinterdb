// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinterdb_forked_child_test.c --
 *
 * This test exercises several cases to 'simulate' running SplinterDB in a
 * multi-process environment. Some 'parent' main() process will instantiate
 * a Splinter instance, and one or more forked-child processes will run
 * Splinter APIs. We use this test to validate that a Splinter instance
 * configured to run using shared-memory will work correctly for basic
 * functional usages in this multi-process configuration.
 * -----------------------------------------------------------------------------
 */
#include <sys/wait.h>

#include "platform.h"
#include "splinterdb/public_platform.h"
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

// Function Prototypes
static void
create_default_cfg(splinterdb_config *out_cfg, data_config *default_data_cfg);

/*
 * Global data declaration macro:
 * Each test-case is designed to be self-contained, so we do not have any
 * global SplinterDB-related data structure declarations.
 */
CTEST_DATA(splinterdb_forked_child){};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(splinterdb_forked_child) {}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(splinterdb_forked_child) {}

/*
 * ------------------------------------------------------------------------------
 * Test case to try and repro a bug seen when we do just one insert from a
 * child process and then close SplinterDB from the parent process. During
 * shutdown we try to flush contents of BTree page while unmounting the
 * trunk, and run into an assertion that num_tuples found by BTree iterator
 * is expected to be "> 0", but we found not tuples.
 * ------------------------------------------------------------------------------
 */
CTEST2(splinterdb_forked_child, test_one_insert_then_close_bug)
{
   data_config  default_data_cfg;
   data_config *splinter_data_cfgp = &default_data_cfg;

   // Setup global data_config for Splinter's use.
   memset(splinter_data_cfgp, 0, sizeof(*splinter_data_cfgp));
   default_data_config_init(30, splinter_data_cfgp);

   splinterdb_config splinterdb_cfg;

   // Configure SplinterDB Instance
   memset(&splinterdb_cfg, 0, sizeof(splinterdb_cfg));

   create_default_cfg(&splinterdb_cfg, splinter_data_cfgp);

   splinterdb_cfg.filename = "test_bugfix.db";

   splinterdb *spl_handle; // To a running SplinterDB instance
   int         rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
   ASSERT_EQUAL(0, rc);

   int pid = getpid();

   platform_default_log(
      "Thread-ID=%lu, Parent OS-pid=%d\n", platform_get_tid(), pid);
   pid = fork();

   if (pid < 0) {
      platform_error_log("fork() of child process failed: pid=%d\n", pid);
      return;
   } else if (pid == 0) {
      // Perform some inserts through child process
      splinterdb_register_thread(spl_handle);

      platform_default_log("Thread-ID=%lu, OS-pid=%d: "
                           "Child execution started ...\n",
                           platform_get_tid(),
                           getpid());

      char   key_data[30];
      size_t key_len;

      // Basic insert of new key should succeed.
      key_len   = snprintf(key_data, sizeof(key_data), "%d", 1);
      slice key = slice_create(key_len, key_data);

      static char *to_insert_data = "some-value";
      size_t       to_insert_len  = strlen(to_insert_data);
      slice        to_insert      = slice_create(to_insert_len, to_insert_data);

      rc = splinterdb_insert(spl_handle, key, to_insert);
      ASSERT_EQUAL(0, rc);

      key_len = snprintf(key_data, sizeof(key_data), "%d", 2);
      key     = slice_create(key_len, key_data);
      rc      = splinterdb_insert(spl_handle, key, to_insert);
      ASSERT_EQUAL(0, rc);

      splinterdb_deregister_thread(spl_handle);
   }

   // Only parent can close Splinter
   if (pid) {
      platform_default_log("Thread-ID=%lu, OS-pid=%d: "
                           "Waiting for child pid=%d to complete ...\n",
                           platform_get_tid(),
                           getpid(),
                           pid);

      wait(NULL);

      platform_default_log("Thread-ID=%lu, OS-pid=%d: "
                           "Child execution wait() completed."
                           " Resuming parent ...\n",
                           platform_get_tid(),
                           getpid());
      splinterdb_close(&spl_handle);
   }
}

/*
 * Helper functions.
 */
// By default, all test cases in this file need to run with shared memory
// configured.
static void
create_default_cfg(splinterdb_config *out_cfg, data_config *default_data_cfg)
{
   *out_cfg = (splinterdb_config){.filename   = TEST_DB_NAME,
                                  .cache_size = 64 * Mega,
                                  .disk_size  = 127 * Mega,
                                  .use_shmem  = TRUE,
                                  .data_cfg   = default_data_cfg};
}
