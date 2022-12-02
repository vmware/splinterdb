// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * task_system_test.c --
 *
 *  These unit-tests are constructed to verify basic execution of interfaces
 *  in the SplinterDB task system, as implemented in task.c .
 *
 * ***** NOTE **** Splinter's task system has external and internal APIs:
 *
 * External interfaces:
 *  - task_thread_create()
 *  - platform_thread_join()
 *
 * Internal-use-only interfaces:
 *  - platform_thread_create(), platform_thread_join()
 *  - task_register_this_thread()
 *  - task_deregister_this_thread()
 *
 * This test exercises both variants for completeness. However, all new user
 * programs must only use the external interfaces listed above.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.
#include "platform.h"
#include "config.h" // Reqd for definition of master_config{}
#include "trunk.h"  // Needed for trunk_get_scratch_size()
#include "task.h"

// Configuration for each worker thread
typedef struct {
   task_system    *tasks;
   platform_thread this_thread_id; // OS-generated thread ID
   threadid        exp_thread_idx; // Splinter-generated expected thread index
   uint64          active_threads_bitmask;
} thread_config;

// Function prototypes
static void
exec_one_thread_use_lower_apis(void *arg);

static void
exec_one_thread_use_extern_apis(void *arg);

static void
exec_one_of_n_threads(void *arg);

/*
 * Global data declaration macro:
 */
CTEST_DATA(task_system)
{
   // Declare heap handles for io allocation.
   platform_heap_handle hh;
   platform_heap_id     hid;

   // Config structs required, to exercise task subsystem
   io_config io_cfg;

   uint8 num_bg_threads[NUM_TASK_TYPES];

   // Following get setup pointing to allocated memory
   platform_io_handle *ioh; // Only prerequisite needed to setup task system
   task_system        *tasks;

   uint64 active_threads_bitmask;
};

/*
 * ------------------------------------------------------------------------
 * Do enough setup of IO-system and task-system, without setting up
 * rest of Splinter sub-systems. This sets up just-enough to start
 * exercising and testing the task system interfaces.
 * ------------------------------------------------------------------------
 */
CTEST_SETUP(task_system)
{
   uint64 heap_capacity = (256 * MiB); // small heap is sufficient.

   platform_status rc = STATUS_OK;

   // Create a heap for io and task system to use.
   rc = platform_heap_create(
      platform_get_module_id(), heap_capacity, &data->hh, &data->hid);
   platform_assert_status_ok(rc);

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

   rc = io_handle_init(data->ioh, &data->io_cfg, data->hh, data->hid);
   ASSERT_TRUE(SUCCESS(rc),
               "Failed to init IO handle: %s\n",
               platform_status_to_string(rc));

   // no background threads by default.
   for (int idx = 0; idx < ARRAY_SIZE(data->num_bg_threads); idx++) {
      data->num_bg_threads[idx] = 0;
   }

   // Background thread support is currently deactivated.
   bool use_bg_threads = data->num_bg_threads[TASK_TYPE_NORMAL] != 0;

   rc = task_system_create(data->hid,
                           data->ioh,
                           &data->tasks,
                           TRUE,           // Use statistics,
                           use_bg_threads, // False, currently.
                           data->num_bg_threads,
                           trunk_get_scratch_size());

   // Main task (this thread) is at index 0
   ASSERT_EQUAL(0, platform_get_tid());

   // Main thread should now be marked as being active in the bitmask.
   // Active threads have their bit turned -OFF- in this bitmask.
   uint64 task_bitmask              = task_active_tasks_mask(data->tasks);
   uint64 all_threads_inactive_mask = (~0L);
   uint64 this_thread_active_mask   = (~0x1L);
   uint64 exp_bitmask = (all_threads_inactive_mask & this_thread_active_mask);
   ASSERT_EQUAL(exp_bitmask, task_bitmask);

   // Save it off, so it can be used for verification in a test case.
   data->active_threads_bitmask = exp_bitmask;
}

// Teardown function for suite, called after every test in suite
CTEST_TEARDOWN(task_system)
{
   task_system_destroy(data->hid, &data->tasks);
   platform_heap_destroy(&data->hh);
}

/*
 * ------------------------------------------------------------------------
 * Basic test case: This is a degenerate test case which essentially
 * invokes the create() / destroy() interfaces of the task sub-system.
 * Every other test case will also execute this pair. This test case
 * solely serves the purpose of a minimalistic exerciser of those methods.
 * While at it, report the value returned by platform_get_tid().
 * ------------------------------------------------------------------------
 */
CTEST2(task_system, test_basic_create_destroy)
{
   threadid main_thread_idx = platform_get_tid();
   ASSERT_EQUAL(main_thread_idx, 0);
}

/*
 * ------------------------------------------------------------------------
 * Test creation of one new thread which will do the stuff required
 * to start a thread using lower-level Splinter interfaces.
 * We validate expected outputs from interfaces in this main thread and
 * the thread's worker-function validates thread-specific expected outputs.
 *
 * This test case and its minion worker function,
 * exec_one_thread_use_lower_apis(), is written to use lower-level platform
 * and task-system APIs. In this pair of functions, we explicitly create
 * a new thread and exercise lower-level register / deregister interfaces.

 * In contrast, the test case, test_one_thread_using_extern_apis() does very
 * similar testing of thread invocation but uses external Splinter task
 * system APIs.
 * ------------------------------------------------------------------------
 */
CTEST2(task_system, test_one_thread_using_lower_apis)
{
   platform_thread new_thread;
   thread_config   thread_cfg;

   ZERO_STRUCT(thread_cfg);

   threadid main_thread_idx = platform_get_tid();
   ASSERT_EQUAL(main_thread_idx, 0, "main_thread_idx=%lu", main_thread_idx);

   // Setup thread-specific struct, needed for validation in thread's worker fn
   thread_cfg.tasks                  = data->tasks;
   thread_cfg.exp_thread_idx         = 1; // Main thread is at index 0.
   thread_cfg.active_threads_bitmask = task_active_tasks_mask(data->tasks);

   platform_status rc = STATUS_OK;

   rc = platform_thread_create(&new_thread,
                               FALSE,
                               exec_one_thread_use_lower_apis,
                               &thread_cfg,
                               data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   thread_cfg.this_thread_id = new_thread;

   // This main-thread's thread-index remains unchanged.
   ASSERT_EQUAL(main_thread_idx, platform_get_tid());

   rc = platform_thread_join(new_thread);
   ASSERT_TRUE(SUCCESS(rc));

   // This main-thread's thread-index remains unchanged.
   threadid get_tid_after_thread_exits = platform_get_tid();
   ASSERT_EQUAL(main_thread_idx,
                get_tid_after_thread_exits,
                "main_thread_idx=%lu != get_tid_after_thread_exits=%lu",
                main_thread_idx,
                get_tid_after_thread_exits);
}

/*
 * ------------------------------------------------------------------------
 * Test creation of one new thread using external Splinter interfaces (like
 * in a real application code-flow). These interfaces will do all the stuff
 * required to start a thread and set it up to perform Splinter work.
 *
 * This test case and its minion worker function,
 * exec_one_thread_use_extern_apis(), is written to use external platform
 * and task-system APIs. Otherwise, the testing done is identical to what's
 * done in its sibling test case, test_one_thread_using_lower_apis.
 * ------------------------------------------------------------------------
 */
CTEST2(task_system, test_one_thread_using_extern_apis)
{
   platform_thread new_thread;
   thread_config   thread_cfg;

   ZERO_STRUCT(thread_cfg);

   threadid main_thread_idx = platform_get_tid();
   ASSERT_EQUAL(main_thread_idx, 0, "main_thread_idx=%lu", main_thread_idx);

   // Setup thread-specific struct, needed for validation in thread's worker fn
   thread_cfg.tasks                  = data->tasks;
   thread_cfg.exp_thread_idx         = 1; // Main thread is at index 0.
   thread_cfg.active_threads_bitmask = task_active_tasks_mask(data->tasks);

   platform_status rc = STATUS_OK;

   // This interface packages all the platform_thread_create() and
   // register / deregister business, around the invocation of the
   // user's worker function, exec_one_thread_use_extern_apis().
   rc = task_thread_create("test_one_thread",
                           exec_one_thread_use_extern_apis,
                           &thread_cfg,
                           trunk_get_scratch_size(),
                           data->tasks,
                           data->hid,
                           &new_thread);
   ASSERT_TRUE(SUCCESS(rc));

   thread_cfg.this_thread_id = new_thread;

   // This main-thread's thread-index remains unchanged.
   ASSERT_EQUAL(main_thread_idx, platform_get_tid());

   // task_thread_join(), if it were to exist, would have been
   // a pass-through to platform-specific join() method, anyways.
   rc = platform_thread_join(new_thread);
   ASSERT_TRUE(SUCCESS(rc));

   // This main-thread's thread-index remains unchanged.
   threadid get_tid_after_thread_exits = platform_get_tid();
   ASSERT_EQUAL(main_thread_idx,
                get_tid_after_thread_exits,
                "main_thread_idx=%lu != get_tid_after_thread_exits=%lu",
                main_thread_idx,
                get_tid_after_thread_exits);
}

/*
 * ------------------------------------------------------------------------
 * Test creation and execution of multiple threads which will do the stuff
 * required to start threads using lower-level Splinter interfaces.
 * ------------------------------------------------------------------------
 */
CTEST2(task_system, test_multiple_threads)
{
   platform_thread new_thread;
   thread_config   thread_cfg[MAX_THREADS];
   thread_config  *thread_cfgp = NULL;
   int             tctr        = 0;
   platform_status rc          = STATUS_OK;

   ZERO_ARRAY(thread_cfg);
   ASSERT_EQUAL(task_get_max_tid(data->tasks),
                1,
                "Before threads start, task_get_max_tid() = %lu",
                task_get_max_tid(data->tasks));

   // Start-up n-threads, record their expected thread-IDs, which will be
   // validated by the thread's execution function below.
   for (tctr = 1, thread_cfgp = &thread_cfg[tctr];
        tctr < ARRAY_SIZE(thread_cfg);
        tctr++, thread_cfgp++)
   {
      // These are independent of the new thread's creation.
      thread_cfgp->tasks          = data->tasks;
      thread_cfgp->exp_thread_idx = tctr;

      rc = platform_thread_create(
         &new_thread, FALSE, exec_one_of_n_threads, thread_cfgp, data->hid);
      ASSERT_TRUE(SUCCESS(rc));

      thread_cfgp->this_thread_id = new_thread;
   }

   // Complete execution of n-threads. Worker fn does the validation.
   for (tctr = 1, thread_cfgp = &thread_cfg[tctr];
        tctr < ARRAY_SIZE(thread_cfg);
        tctr++, thread_cfgp++)
   {
      rc = platform_thread_join(thread_cfgp->this_thread_id);
      ASSERT_TRUE(SUCCESS(rc));
   }
}

/*
 * exec_one_thread_use_lower_apis() - Worker routine executed by a single
 * thread.
 *
 * This worker method uses lower-level thread register / deregister APIs,
 * as a way to exercise them and to check for correctness. In this method,
 * we also do careful examination and validation of internal structures to
 * follow the machinations of the task system.
 */
static void
exec_one_thread_use_lower_apis(void *arg)
{
   thread_config *thread_cfg = (thread_config *)arg;

   uint64 task_bitmask_before_register =
      task_active_tasks_mask(thread_cfg->tasks);

   // Verify that the state of active-threads bitmask (managed by Splinter) has
   // not changed just by creating this pthread. It should be the same as what
   // we had recorded just prior to platform_thread_create().
   ASSERT_EQUAL(thread_cfg->active_threads_bitmask,
                task_bitmask_before_register);

   // This is the important call to initialize thread-specific stuff in
   // Splinter's task-system, which sets up the thread-id (index) and records
   // this thread as active with the task system.
   task_register_this_thread(thread_cfg->tasks, trunk_get_scratch_size());

   uint64 task_bitmask_after_register =
      task_active_tasks_mask(thread_cfg->tasks);

   // _Now, the active tasks bitmask should have changed.
   ASSERT_NOT_EQUAL(task_bitmask_before_register, task_bitmask_after_register);

   threadid this_threads_idx = platform_get_tid();
   ASSERT_EQUAL(thread_cfg->exp_thread_idx, this_threads_idx);

   // This thread is recorded as 'being active' by clearing its bit from the
   // mask.
   uint64 exp_bitmask = (0x1L << this_threads_idx);
   exp_bitmask        = (task_bitmask_before_register & ~exp_bitmask);
   ASSERT_EQUAL(task_bitmask_after_register, exp_bitmask);

   // Registration should have allocated some scratch space memory.
   ASSERT_TRUE(
      task_system_get_thread_scratch(thread_cfg->tasks, platform_get_tid())
      != NULL);

   // Brain-dead cross-check, to understand what's going on with thread-IDs.
   platform_thread thread_id = platform_thread_id_self();
   ASSERT_TRUE((thread_cfg->this_thread_id == thread_id)
               || (thread_cfg->this_thread_id == 0));

   task_deregister_this_thread(thread_cfg->tasks);

   uint64 task_bitmask_after_deregister =
      task_active_tasks_mask(thread_cfg->tasks);

   // De-registering this task removes it from the active tasks mask
   ASSERT_EQUAL(task_bitmask_before_register, task_bitmask_after_deregister);

   // Deregistration releases scratch space memory.
   ASSERT_TRUE(
      task_system_get_thread_scratch(thread_cfg->tasks, this_threads_idx)
      == NULL);

   // Register / de-register of thread with SplinterDB's task system is
   // SplinterDB's jugglery to keep track of resources. get_tid() should
   // now be reset.
   threadid get_tid_after_deregister = platform_get_tid();
   ASSERT_EQUAL(0,
                get_tid_after_deregister,
                "get_tid_after_deregister=%lu is != expected index into"
                " thread array, %lu ",
                get_tid_after_deregister,
                thread_cfg->exp_thread_idx);
}

/*
 * exec_one_thread_use_extern_apis() - Worker routine executed by a single
 * thread.
 *
 * This worker method uses external interfaces to start a thread, thru
 * task_invoke_with_hooks(). That interface registers the new thread with
 * Splinter -before- invoking this worker function. Adjust the task-bitmask
 * checks accordingly.
 */
static void
exec_one_thread_use_extern_apis(void *arg)
{
   thread_config *thread_cfg = (thread_config *)arg;

   uint64 bitmask_before_thread_create = thread_cfg->active_threads_bitmask;

   uint64 bitmask_after_thread_create =
      task_active_tasks_mask(thread_cfg->tasks);

   // The task_thread_create() -> task_invoke_with_hooks() also registers this
   // thread with Splinter. First, confirm that the bitmask has changed from
   // what it was before this thread was invoked.
   ASSERT_NOT_EQUAL(bitmask_before_thread_create, bitmask_after_thread_create);

   threadid this_threads_idx = platform_get_tid();
   ASSERT_EQUAL(thread_cfg->exp_thread_idx, this_threads_idx);

   // This thread is recorded as 'being active' by clearing its bit from the
   // mask. Verify the expected task bitmask.
   uint64 exp_bitmask = (0x1L << this_threads_idx);
   exp_bitmask        = (bitmask_before_thread_create & ~exp_bitmask);
   ASSERT_EQUAL(bitmask_after_thread_create, exp_bitmask);

   /*
    * Dead Code Warning!
    * The interface used here has already registered this thread. An attempt to
    * re-register this thread will trip an assertion. (Left here for posterity.)
    */
   // task_register_this_thread(thread_cfg->tasks, trunk_get_scratch_size());

   // Registration should have allocated some scratch space memory.
   ASSERT_TRUE(
      task_system_get_thread_scratch(thread_cfg->tasks, this_threads_idx)
      != NULL);

   /*
    * Dead Code Warning!
    * You cannot explicitly deregister this thread through this interface as
    * that's done by task_invoke_with_hooks(). If you enable the call below,
    * an assertion will trap in that other function when the thread is being
    * torn down. (Left here for posterity.)
    */
   // task_deregister_this_thread(thread_cfg->tasks);
}

/*
 * exec_one_of_n_threads() - Worker routine executed by one of a set of
 * multiple threads.
 *
 * We don't have any sophisticated sync-points mechanism across threads, so we
 * cannot do any deep validation of state while multiple threads are executing.
 * Again this worker function is written to use lower-level register /
 * deregister APIs of the task system.
 */
static void
exec_one_of_n_threads(void *arg)
{
   thread_config *thread_cfg = (thread_config *)arg;

   task_register_this_thread(thread_cfg->tasks, trunk_get_scratch_size());

   threadid this_threads_index = platform_get_tid();

   ASSERT_TRUE((this_threads_index < MAX_THREADS),
               "Thread [%lu] Registered thread idx = %lu is invalid.",
               thread_cfg->exp_thread_idx,
               this_threads_index);

   // Test case is carefully constructed to fire-up n-threads. Wait for
   // them to all start-up.
   while (task_get_max_tid(thread_cfg->tasks) < MAX_THREADS) {
      platform_sleep(USEC_TO_NSEC(100000)); // 100 msec.
   }

   task_deregister_this_thread(thread_cfg->tasks);

   // Register / de-register of thread with SplinterDB's task system is just
   // SplinterDB's jugglery to keep track of resources. get_tid() should still
   // remain the expected index into the threads[] array.
   threadid get_tid_after_deregister = platform_get_tid();
   ASSERT_EQUAL(0,
                get_tid_after_deregister,
                "get_tid_after_deregister=%lu is != the index into"
                " thread array, %lu ",
                get_tid_after_deregister,
                this_threads_index);
}
