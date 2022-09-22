// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * task_system_test.c --
 *
 *  Exercises the interfaces in src/task.c .
 *  These unit-tests are constructed to verify basic execution of interfaces
 *  in the SplinterDB task system.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "unit_tests.h"
#include "ctest.h"  // This is required for all test-case files.
#include "config.h" // Reqd for definition of master_config{}
#include "trunk.h"  // Needed for trunk_get_scratch_size()
#include "task.h"

// Configuration for each worker thread
typedef struct {
   task_system *tasks;
   pthread_t    this_thread_id;
   threadid     exp_thread_idx;
} thread_config;

// Function prototypes
static void *
exec_one_thread(void *arg);

/*
 * Global data declaration macro:
 */
CTEST_DATA(task_system)
{
   // Declare head handles for io, allocator, cache and splinter allocation.
   platform_heap_handle hh;
   platform_heap_id     hid;

   // Config structs required, to exercise task subsystem
   io_config io_cfg;

   uint8 num_bg_threads[NUM_TASK_TYPES];

   // Following get setup pointing to allocated memory
   platform_io_handle *ioh;
   task_system        *tasks;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(task_system)
{
   // Allocate and initialize the IO sub-system.
   data->ioh = TYPED_MALLOC(data->hid, data->ioh);
   ASSERT_TRUE((data->ioh != NULL));

   // Do minimal IO config setup, using default IO values.
   master_config master_cfg;
   config_set_defaults(&master_cfg);
   io_config_init(&data->io_cfg,
                  master_cfg.page_size,
                  master_cfg.extent_size,
                  master_cfg.io_flags,
                  master_cfg.io_perms,
                  master_cfg.io_async_queue_depth,
                  master_cfg.io_filename);

   platform_status rc;
   rc = io_handle_init(data->ioh, &data->io_cfg, data->hh, data->hid);
   ASSERT_TRUE(SUCCESS(rc),
               "Failed to init IO handle: %s\n",
               platform_status_to_string(rc));

   // no background threads by default.
   for (int idx = 0; idx < ARRAY_SIZE(data->num_bg_threads); idx++) {
      data->num_bg_threads[idx] = 0;
   }

   bool use_bg_threads = data->num_bg_threads[TASK_TYPE_NORMAL] != 0;
   rc                  = task_system_create(data->hid,
                           data->ioh,
                           &data->tasks,
                           TRUE,           // Use statistics,
                           use_bg_threads, // False, currently.
                           data->num_bg_threads,
                           trunk_get_scratch_size());
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(task_system)
{
   task_system_destroy(data->hid, &data->tasks);
}

/*
 * Basic test case: This is a degenerate test case which essentially
 * invokes the create() / destroy() interfaces of the task sub-system.
 * Every other test case will also execute this pair. This test case
 * solely serves the purpose of a minimalistic exerciser of those methods.
 * While at it, report the value returned by platform_get_tid().
 */
CTEST2(task_system, test_basic_create_destroy)
{
   platform_default_log("platform_get_tid() = %lu ", platform_get_tid());
}

/*
 * Test creation of one new thread which will do the required stuff to
 * start using Splinter interfaces (in a real application code-flow).
 */
CTEST2(task_system, test_one_thread)
{
   pthread_t     new_thread;
   thread_config thread_cfg;

   threadid main_thread_idx = platform_get_tid();

   int rc = pthread_create(&new_thread, NULL, exec_one_thread, &thread_cfg);
   ASSERT_EQUAL(0, rc);

   thread_cfg.tasks          = data->tasks;
   thread_cfg.this_thread_id = new_thread;
   thread_cfg.exp_thread_idx = 1;

   void *thread_rc;
   rc = pthread_join(new_thread, &thread_rc);
   ASSERT_EQUAL(0, rc);

   // After thread exits, get_tid() should revert back to that of initial
   // thread.
   threadid get_tid_after_thread_exits = platform_get_tid();
   ASSERT_EQUAL(main_thread_idx,
                get_tid_after_thread_exits,
                "main_thread_idx=%lu != get_tid_after_thread_exits=%lu",
                main_thread_idx,
                get_tid_after_thread_exits);

   ASSERT_EQUAL(main_thread_idx, 0, "main_thread_idx=%lu", main_thread_idx);
}

static void *
exec_one_thread(void *arg)
{
   thread_config *thread_cfg = (thread_config *)arg;

   task_register_this_thread(thread_cfg->tasks, trunk_get_scratch_size());

   ASSERT_EQUAL(thread_cfg->exp_thread_idx, platform_get_tid());

   // Brain-dead cross-check, to understand what's going on with thread-IDs.
   pthread_t thread_id = pthread_self();
   ASSERT_EQUAL(thread_cfg->this_thread_id, thread_id);

   platform_default_log(
      "platform_get_tid() = %lu, new_thread_ID == pthread_self()=%lu ",
      platform_get_tid(),
      thread_id);

   task_deregister_this_thread(thread_cfg->tasks);

   // Register / de-register of thread with SplinterDB's task system is just
   // SplinterDB's jugglery to keep track of resources. get_tid() should still
   // remain the expected index into the threads[] array.
   threadid get_tid_after_deregister = platform_get_tid();
   ASSERT_EQUAL(thread_cfg->exp_thread_idx,
                get_tid_after_deregister,
                "get_tid_after_deregister=%lu is != expected index into"
                " thread array, %lu ",
                get_tid_after_deregister,
                thread_cfg->exp_thread_idx);
   return 0;
}
