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
#include <unistd.h>
#include <sys/wait.h>

#include "platform.h"
#include "splinterdb/public_platform.h"
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "shmem.h"
#include "config.h"
#include "test_splinterdb_apis.h"
#include "test_common.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

// Some constants to drive KV-inserts
#define TEST_KEY_SIZE   30
#define TEST_VALUE_SIZE 256

// By default, we test with 2 forked processes in most test case
#define TEST_NUM_FORKED_PROCS 2

// Function Prototypes
static void
create_default_cfg(splinterdb_config *out_cfg, data_config *default_data_cfg);

static void
do_many_inserts(splinterdb *kvsb, uint64 num_inserts);

static void
safe_wait();

/*
 * Global data declaration macro:
 * Each test-case is designed to be self-contained, so we do not have any
 * global SplinterDB-related data structure declarations.
 */
CTEST_DATA(splinterdb_forked_child)
{
   master_config master_cfg;
   uint64        num_inserts;      // per main() process or per thread
   uint64        num_forked_procs; // Passed down using --num-threads
   bool          am_parent;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(splinterdb_forked_child)
{
   data->am_parent = TRUE;

   ZERO_STRUCT(data->master_cfg);
   config_set_defaults(&data->master_cfg);

   // Expected args to parse --num-inserts, --verbose-progress.
   platform_status rc =
      config_parse(&data->master_cfg, 1, Ctest_argc, (char **)Ctest_argv);
   ASSERT_TRUE(SUCCESS(rc));

   data->num_inserts =
      (data->master_cfg.num_inserts ? data->master_cfg.num_inserts
                                    : (2 * MILLION));

   if ((data->num_inserts % MILLION) != 0) {
      platform_error_log("Test expects --num-inserts parameter to be an"
                         " integral multiple of a million.\n");
      ASSERT_EQUAL(0, (data->num_inserts % MILLION));
      return;
   }
   data->num_forked_procs = data->master_cfg.num_threads;
   if (!data->num_forked_procs) {
      data->num_forked_procs = TEST_NUM_FORKED_PROCS;
   }
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(splinterdb_forked_child) {}

/*
 * ------------------------------------------------------------------------------
 * Elementary test case to validate data structures and various handles as seen
 * from the child process. Establish that the child sees the same ptr-handles as
 * seen by the parent, and that all of them are correctly allocated from shared
 * memory.
 * ------------------------------------------------------------------------------
 */
CTEST2(splinterdb_forked_child, test_data_structures_handles)
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

   splinterdb_cfg.filename = "splinterdb_forked_child_test_db";

   splinterdb *spl_handle; // To a running SplinterDB instance
   int         rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
   ASSERT_EQUAL(0, rc);

   int pid = getpid();

   // Parent / main() process is always at tid==0.
   ASSERT_EQUAL(0, platform_get_tid());

   platform_default_log(
      "Thread-ID=%lu, Parent OS-pid=%d\n", platform_get_tid(), pid);
   pid = fork();

   if (pid < 0) {
      platform_error_log("fork() of child process failed: pid=%d\n", pid);
      return;
   } else if (pid == 0) {
      // Verify that child process sees the same handle to a running Splinter
      // as seen by the parent. (We cross-check using the copy saved off in
      // shared memory.)
      ASSERT_TRUE((void *)spl_handle
                  == platform_heap_get_splinterdb_handle(
                     splinterdb_get_heap_handle(spl_handle)));

      // Verify that the splinter handle and handles to other sub-systems are
      // all valid addresses allocated from the shared segment setup by the main
      // process.
      ASSERT_TRUE(platform_valid_addr_in_shm(
         splinterdb_get_heap_handle(spl_handle), spl_handle));

      ASSERT_TRUE(platform_valid_addr_in_shm(
         splinterdb_get_heap_handle(spl_handle),
         splinterdb_get_task_system_handle(spl_handle)));

      ASSERT_TRUE(
         platform_valid_addr_in_shm(splinterdb_get_heap_handle(spl_handle),
                                    splinterdb_get_io_handle(spl_handle)));

      ASSERT_TRUE(platform_valid_addr_in_shm(
         splinterdb_get_heap_handle(spl_handle),
         splinterdb_get_allocator_handle(spl_handle)));

      ASSERT_TRUE(
         platform_valid_addr_in_shm(splinterdb_get_heap_handle(spl_handle),
                                    splinterdb_get_cache_handle(spl_handle)));

      ASSERT_TRUE(
         platform_valid_addr_in_shm(splinterdb_get_heap_handle(spl_handle),
                                    splinterdb_get_trunk_handle(spl_handle)));

      ASSERT_TRUE(platform_valid_addr_in_shm(
         splinterdb_get_heap_handle(spl_handle),
         splinterdb_get_memtable_context_handle(spl_handle)));

      // Before registering w/Splinter, child process is still at tid==0.
      ASSERT_EQUAL(0, platform_get_tid());

      // Perform some inserts through child process
      splinterdb_register_thread(spl_handle);

      // After registering w/Splinter, child process' tid will change.
      ASSERT_EQUAL(1, platform_get_tid());

      splinterdb_deregister_thread(spl_handle);

      // After deregistering w/Splinter, child process is back to invalid value
      ASSERT_EQUAL(INVALID_TID, platform_get_tid());
   }

   // Only parent can close Splinter
   if (pid) {

      safe_wait();

      // We would get assertions tripping from BTree iterator code here,
      // if the fix in platform_buffer_create_mmap() to use MAP_SHARED
      // was not in-place.
      rc = splinterdb_close(&spl_handle);
      ASSERT_EQUAL(0, rc);
   } else {
      // Child should not attempt to run the rest of the tests
      exit(0);
   }
}

/*
 * ------------------------------------------------------------------------------
 * Test case to try and repro a bug seen when we do just one insert from a
 * child process and then close SplinterDB from the parent process. During
 * shutdown we try to flush contents of BTree page while unmounting the
 * trunk, and run into an assertion that num_tuples found by BTree iterator
 * is expected to be "> 0", but we found not tuples.
 *
 * The issue here is (was) that we need to setup mmap()-based shared structures,
 * e.g. the buffer cache, using MAP_SHARED, so that the child process(es) "see"
 * the same buffer cache, in mmap'ed-memory, as the parent process would see.
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

   splinterdb_cfg.filename = "splinterdb_forked_child_test_db";

   splinterdb *spl_handle; // To a running SplinterDB instance
   int         rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
   ASSERT_EQUAL(0, rc);

   int pid = getpid();

   platform_default_log(
      "Parent OS-pid=%d, ThreadID=%lu\n", pid, platform_get_tid());
   pid = fork();

   if (pid < 0) {
      platform_error_log("fork() of child process failed: pid=%d\n", pid);
      return;
   } else if (pid == 0) {
      // Perform some inserts through child process
      splinterdb_register_thread(spl_handle);

      platform_default_log("OS-pid=%d, ThreadID=%lu: "
                           "Child execution started ...\n",
                           getpid(),
                           platform_get_tid());

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
      platform_default_log("OS-pid=%d, ThreadID=%lu: "
                           "Waiting for child pid=%d to complete ...\n",
                           getpid(),
                           platform_get_tid(),
                           pid);

      safe_wait();

      platform_default_log("OS-pid=%d, ThreadID=%lu: "
                           "Child execution wait() completed."
                           " Resuming parent ...\n",
                           getpid(),
                           platform_get_tid());

      // We would get assertions tripping from BTree iterator code here,
      // if the fix in platform_buffer_create_mmap() to use MAP_SHARED
      // was not in-place.
      rc = splinterdb_close(&spl_handle);
      ASSERT_EQUAL(0, rc);
   } else {
      // child should not attempt to run the rest of the tests
      exit(0);
   }
}

/*
 * ------------------------------------------------------------------------------
 * Test case to perform large #s of inserts in child process, enough to trigger
 * IO. Test case ensures that all IOs performed by child process are drained and
 * completed before child deregisters from Splinter. Otherwise, we will end up
 * with hard-errors when the IO completion routine uses the IO-context for the
 * parent process rather than that of the child.
 *
 * This test case actually demonstrates couple of things:
 *
 *  1. Test that a registered process can do IOs and process completion -before-
 *     it deregisters.
 *     If there are any pending async-IOs issued by a process waiting to be
 *     completed, we can verify that invoking this completion -before- the
 *     process deregisters itself from Splinter will work correctly. We use
 *     splinterdb_cache_flush() as a testing hook, to trigger a cache-flush
 *      which will go down the path of calling io_cleanup_all(). A registered
 *     process can do IOs and process completion -before- it deregisters.
 *     An attempt to deregister with outstanding pending IOs will trigger an
 *     assert.
 *
 *  2. Test that after deregistration, a process cannot issue any IOs. The
 *     IO-context state is reset to that of the parent thread, so we will end
 *     up with hard errors from IO-system calls. (We only test this indirectly.)
 *
 *     If there are any pending async-IOs issued by a process waiting to be
 *     completed, these need to be completed before the process deregisters
 *     itself from Splinter. We need the fix on Splinter-side to ensure that
 *     all pending async-IOs from this process are taken to completion.
 * ------------------------------------------------------------------------------
 */
CTEST2(splinterdb_forked_child,
       test_completion_of_outstanding_async_IOs_from_process_bug)
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

   splinterdb_cfg.filename = "splinterdb_forked_child_test_db";

   splinterdb *spl_handle; // To a running SplinterDB instance
   int         rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
   ASSERT_EQUAL(0, rc);

   int pid = getpid();

   platform_default_log(
      "Parent OS-pid=%d, ThreadID=%lu\n", pid, platform_get_tid());
   pid = fork();

   if (pid < 0) {
      platform_error_log("fork() of child process failed: pid=%d\n", pid);
      return;
   } else if (pid == 0) {
      // Perform some inserts through child process
      splinterdb_register_thread(spl_handle);

      platform_default_log(
         "OS-pid=%d, ThreadID=%lu: "
         "Child execution started: Test cache-flush before deregister ...\n",
         getpid(),
         platform_get_tid());

      do_many_inserts(spl_handle, data->num_inserts);

      // This combination of calls tests scenario (1)
      splinterdb_cache_flush(spl_handle);
      splinterdb_deregister_thread(spl_handle);

      // Repeat the insert exercise: Perform some inserts through child process
      splinterdb_register_thread(spl_handle);

      platform_default_log(
         "OS-pid=%d, ThreadID=%lu, Test cache-flush after deregister:\n",
         getpid(),
         platform_get_tid());

      do_many_inserts(spl_handle, data->num_inserts);

      // This combination of calls tests scenario (2)
      splinterdb_deregister_thread(spl_handle);

      // **** DEAD CODE WARNING ****
      // You -cannot- enable this call. A thread is supposed to have
      // completely drained all its pending IOs, and it cannot do
      // any more IOs (which is what the flush below will try to do.)
      // splinterdb_cache_flush(spl_handle);
   }

   // Only parent can close Splinter
   if (pid) {
      platform_default_log("OS-pid=%d, ThreadID=%lu: "
                           "Waiting for child pid=%d to complete ...\n",
                           getpid(),
                           platform_get_tid(),
                           pid);

      safe_wait();

      platform_default_log("OS-pid=%d, ThreadID=%lu: "
                           "Child execution wait() completed."
                           " Resuming parent ...\n",
                           getpid(),
                           platform_get_tid());
      rc = splinterdb_close(&spl_handle);
      ASSERT_EQUAL(0, rc);
   } else {
      // child should not attempt to run the rest of the tests
      exit(0);
   }
}

/*
 * ------------------------------------------------------------------------------
 * Test case to fire-up multiple child processes, each doing concurrent inserts.
 * This test simulates the usage of multiple clients connected to a DB-server
 * where each connection is a forked child process. We verify the stability of
 * this concurrent insert workload to establish a baseline for performance
 * measurements (elsewhere). This test case verifies that the per-process/thread
 * IO-context rework works reliably for multiple child processes performing IO
 * concurrently.
 * ------------------------------------------------------------------------------
 */
CTEST2(splinterdb_forked_child, test_multiple_forked_process_doing_IOs)
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

   // We want larger cache as multiple child processes will be
   // hammering at it with large #s of inserts.
   splinterdb_cfg.cache_size = (1 * Giga);

   // Bump up disk size based on # of concurrent child processes
   splinterdb_cfg.disk_size *= data->num_forked_procs;

   splinterdb_cfg.filename = "splinterdb_forked_child_test_db";

   splinterdb *spl_handle; // To a running SplinterDB instance
   int         rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
   ASSERT_EQUAL(0, rc);

   int pid = getpid();

   platform_default_log(
      "Parent OS-pid=%d, ThreadID=%lu, fork %lu child processes.\n",
      pid,
      platform_get_tid(),
      data->num_forked_procs);

   bool wait_for_gdb = data->master_cfg.wait_for_gdb;

   int forked_pids[20] = {0};
   platform_assert(data->num_forked_procs < sizeof(forked_pids));

   // Fork n-concurrently executing child processes.
   for (int fctr = 0; data->am_parent && fctr < data->num_forked_procs; fctr++)
   {
      pid               = fork();
      forked_pids[fctr] = pid;

      if (pid < 0) {
         platform_error_log("fork() of child process failed: pid=%d\n", pid);
         return;
      } else if (pid == 0) {

         // This is now executing as a child process.
         data->am_parent = FALSE;

         if (wait_for_gdb) {
            trace_wait_for_gdb();
         }

         // Perform some inserts through child process
         splinterdb_register_thread(spl_handle);

         platform_default_log(
            "OS-pid=%d, ThreadID=%lu:"
            "Child execution started: Perform %lu (%d million) inserts ..."
            " Test cache-flush before deregister ...\n",
            getpid(),
            platform_get_tid(),
            data->num_inserts,
            (int)(data->num_inserts / MILLION));

         do_many_inserts(spl_handle, data->num_inserts);

         /*
          * **** DEAD CODE WARNING! ****
          * Although this block is similar to what's being done in
          * test_completion_of_outstanding_async_IOs ... test case,
          * we -cannot- issue this call below. As concurrent processes
          * are executing the cache will never be in a fully clean
          * state.
          *
          *   splinterdb_cache_flush(spl_handle);
          */
         splinterdb_deregister_thread(spl_handle);
      }
   }

   // Only parent can close Splinter
   if (data->am_parent && pid) {
      platform_default_log("OS-pid=%d, ThreadID=%lu: "
                           "Waiting for child pid=%d to complete ...\n",
                           getpid(),
                           platform_get_tid(),
                           pid);

      // Wait-for -ALL- children to finish; duh!
      for (int fctr = 0; fctr < data->num_forked_procs; fctr++) {
         int wstatus;
         int wr = waitpid(forked_pids[fctr], &wstatus, 0);
         platform_assert(wr != -1, "wait failure: %s", strerror(errno));
         platform_assert(WIFEXITED(wstatus),
                         "Child terminated abnormally: SIGNAL=%d",
                         WIFSIGNALED(wstatus) ? WTERMSIG(wstatus) : 0);
         platform_assert(WEXITSTATUS(wstatus) == 0);
      }

      platform_default_log("OS-pid=%d, ThreadID=%lu: "
                           "Child execution wait() completed."
                           " Resuming parent ...\n",
                           getpid(),
                           platform_get_tid());

      rc = splinterdb_close(&spl_handle);
      ASSERT_EQUAL(0, rc);
   } else {
      // child should not attempt to run the rest of the tests
      exit(0);
   }
}

/*
 * ------------------------------------------------------------------------------
 * Helper functions.
 * ------------------------------------------------------------------------------
 */
// By default, all test cases in this file need to run with shared memory
// configured. Cache is sized small as most test cases want to exercise
// heavy IOs, which will happen more easily with smaller caches.
static void
create_default_cfg(splinterdb_config *out_cfg, data_config *default_data_cfg)
{
   *out_cfg = (splinterdb_config){.filename   = TEST_DB_NAME,
                                  .cache_size = 64 * Mega,
                                  .disk_size  = 10 * Giga,
                                  .use_shmem  = TRUE,
                                  .data_cfg   = default_data_cfg};
}

/*
 * do_many_inserts() - Driver which will perform large #s of inserts, enough to
 * cause an IO. All inserts are sequential.
 */
static void
do_many_inserts(splinterdb *kvsb, uint64 num_inserts)
{
   char key_data[TEST_KEY_SIZE];
   char val_data[TEST_VALUE_SIZE];

   uint64 start_key = 0;

   uint64 start_time = platform_get_timestamp();

   threadid thread_idx = platform_get_tid();

   // Test is written to insert multiples of millions per thread.
   ASSERT_EQUAL(0, (num_inserts % MILLION));

   platform_default_log("%s()::%d:Thread-%-lu inserts %lu (%lu million)"
                        ", sequential key, sequential value, "
                        "KV-pairs starting from %lu ...\n",
                        __func__,
                        __LINE__,
                        thread_idx,
                        num_inserts,
                        (num_inserts / MILLION),
                        start_key);

   uint64 ictr = 0;
   uint64 jctr = 0;

   bool verbose_progress = TRUE;
   memset(val_data, 'V', sizeof(val_data));
   uint64 val_len = sizeof(val_data);

   for (ictr = 0; ictr < (num_inserts / MILLION); ictr++) {
      for (jctr = 0; jctr < MILLION; jctr++) {

         uint64 id = (start_key + (ictr * MILLION) + jctr);

         // Generate sequential key data
         snprintf(key_data, sizeof(key_data), "%lu", id);
         uint64 key_len = strlen(key_data);

         slice key = slice_create(key_len, key_data);
         slice val = slice_create(val_len, val_data);

         int rc = splinterdb_insert(kvsb, key, val);
         ASSERT_EQUAL(0, rc);
      }
      if (verbose_progress) {
         platform_default_log(
            "%s()::%d:Thread-%lu Inserted %lu million KV-pairs ...\n",
            __func__,
            __LINE__,
            thread_idx,
            (ictr + 1));
      }
   }
   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
   uint64 elapsed_s  = NSEC_TO_SEC(elapsed_ns);
   if (elapsed_s == 0) {
      elapsed_s = 1;
   }

   platform_default_log("%s()::%d:Thread-%lu Inserted %lu million KV-pairs in "
                        "%lu s, %lu rows/s\n",
                        __func__,
                        __LINE__,
                        thread_idx,
                        ictr, // outer-loop ends at #-of-Millions inserted
                        elapsed_s,
                        (num_inserts / elapsed_s));
}

static void
safe_wait()
{
   int wstatus;
   int wr = wait(&wstatus);
   platform_assert(wr != -1, "wait failure: %s", strerror(errno));
   platform_assert(WIFEXITED(wstatus),
                   "Child terminated abnormally: SIGNAL=%d",
                   WIFSIGNALED(wstatus) ? WTERMSIG(wstatus) : 0);
   platform_assert(WEXITSTATUS(wstatus) == 0);
}
