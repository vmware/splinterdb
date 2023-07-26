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
#include "splinterdb/splinterdb.h"
#include "splinterdb/default_data_config.h"

// Configuration for each worker thread
typedef struct {
   task_system    *tasks;
   platform_thread this_thread_id; // OS-generated thread ID
   threadid        exp_thread_idx; // Splinter-generated expected thread index
   uint64          active_threads_bitmask;
} thread_config;

// Configuration for worker threads used in lock-step testing exercise
typedef struct {
   task_system *tasks;
   threadid     exp_thread_idx; // Splinter-generated expected thread index
   threadid     exp_max_tid;    // After this thread gets created
   bool32       stop_thread;
   bool32       waitfor_stop_signal;
   int          line; // Thread created on / around this line #
} thread_config_lockstep;

#define TEST_MAX_KEY_SIZE 13

// Function prototypes
static platform_status
create_task_system_without_bg_threads(void *datap);

static platform_status
create_task_system_with_bg_threads(void  *datap,
                                   uint64 num_memtable_bg_threads,
                                   uint64 num_normal_bg_threads);

static void
exec_one_thread_use_lower_apis(void *arg);

static void
exec_one_thread_use_extern_apis(void *arg);

static void
exec_one_of_n_threads(void *arg);

static void
exec_user_thread_loop_for_stop(void *arg);

/*
 * Global data declaration macro:
 */
CTEST_DATA(task_system)
{
   // Declare heap handles for io allocation.
   platform_heap_handle hh;
   platform_heap_id     hid;

   // Config structs required, to exercise task subsystem
   io_config          io_cfg;
   task_system_config task_cfg;

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
   platform_status rc = STATUS_OK;

   uint64 heap_capacity = (256 * MiB); // small heap is sufficient.
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

   // Background threads are OFF by default.
   rc = create_task_system_without_bg_threads(data);
   ASSERT_TRUE(SUCCESS(rc));

   // Main task (this thread) is at index 0
   ASSERT_EQUAL(0, platform_get_tid());

   // Main thread should now be marked as being active in the bitmask.
   // Active threads have their bit turned -OFF- in this bitmask.
   uint64 task_bitmask              = task_active_tasks_mask(data->tasks);
   uint64 all_threads_inactive_mask = (~0L);
   uint64 this_thread_active_mask   = (~0x1L);
   uint64 exp_bitmask = (all_threads_inactive_mask & this_thread_active_mask);

   ASSERT_EQUAL(exp_bitmask,
                task_bitmask,
                "exp_bitmask=0x%x, task_bitmask=0x%x\n",
                exp_bitmask,
                task_bitmask);

   // Save it off, so it can be used for verification in a test case.
   data->active_threads_bitmask = exp_bitmask;
}

// Teardown function for suite, called after every test in suite
CTEST_TEARDOWN(task_system)
{
   task_system_destroy(data->hid, &data->tasks);
   io_handle_deinit(data->ioh);
   platform_heap_destroy(&data->hh);
}

/*
 * ------------------------------------------------------------------------
 * Basic test case: This is a degenerate test case which essentially
 * invokes the create() / destroy() interfaces of the task sub-system.
 * Every other test case will also execute this pair. This test case
 * solely serves the purpose of a minimalistic exerciser of those methods.
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
   thread_cfg.tasks = data->tasks;

   // Main thread is at index 0
   thread_cfg.exp_thread_idx         = 1;
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
   thread_cfg.tasks = data->tasks;

   // Main thread is at index 0
   thread_cfg.exp_thread_idx         = 1;
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
 * Background threads are off, by default.
 * ------------------------------------------------------------------------
 */
CTEST2(task_system, test_max_threads_using_lower_apis)
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
 * ------------------------------------------------------------------------
 * Test creation of task system with background threads configured.
 * ------------------------------------------------------------------------
 */
CTEST2(task_system, test_task_system_creation_with_bg_threads)
{
   // Destroy the task system setup by the harness, by default, w/o bg threads.
   task_system_destroy(data->hid, &data->tasks);
   platform_status rc = create_task_system_with_bg_threads(data, 2, 4);
   ASSERT_TRUE(SUCCESS(rc));

   uint64 all_threads_inactive_mask = (~0L);

   // Construct known bit-mask for active threads knowing that the background
   // threads are started up when task system is created w/bg threads.
   threadid max_thread_id       = task_get_max_tid(data->tasks);
   uint64   active_threads_mask = 0;
   for (int tid = 0; tid < max_thread_id; tid++) {
      active_threads_mask |= (1L << tid);
   }
   active_threads_mask = ~active_threads_mask;

   uint64 exp_bitmask  = (all_threads_inactive_mask & active_threads_mask);
   uint64 task_bitmask = task_active_tasks_mask(data->tasks);

   ASSERT_EQUAL(exp_bitmask,
                task_bitmask,
                "exp_bitmask=0x%x, task_bitmask=0x%x\n",
                exp_bitmask,
                task_bitmask);
}

/*
 * ------------------------------------------------------------------------
 * Test creation of task system using up the threads for background threads.
 * Verify that we can create just one more user-thread and that the next
 * user-thread creation should fail with a proper error message.
 * ------------------------------------------------------------------------
 */
CTEST2(task_system, test_use_all_but_one_threads_for_bg_threads)
{
   platform_status rc = STATUS_OK;
   set_log_streams_for_tests(MSG_LEVEL_ERRORS);

   // Destroy the task system setup by the harness, by default, w/o bg threads.
   task_system_destroy(data->hid, &data->tasks);

   // Consume all-but-one available threads with background threads.
   rc = create_task_system_with_bg_threads(data, 1, (MAX_THREADS - 3));
   ASSERT_TRUE(SUCCESS(rc));

   threadid main_thread_idx = platform_get_tid();
   ASSERT_EQUAL(0, main_thread_idx, "main_thread_idx=%lu", main_thread_idx);

   thread_config_lockstep thread_cfg[2];
   ZERO_ARRAY(thread_cfg);
   thread_cfg[0].tasks          = data->tasks;
   thread_cfg[0].exp_thread_idx = task_get_max_tid(data->tasks);
   thread_cfg[0].exp_max_tid    = MAX_THREADS;
   thread_cfg[0].line           = __LINE__;

   platform_thread new_thread[2] = {0};

   // This should successfully create a new (the last) thread
   rc = task_thread_create("test_one_thread",
                           exec_user_thread_loop_for_stop,
                           &thread_cfg[0],
                           trunk_get_scratch_size(),
                           data->tasks,
                           data->hid,
                           &new_thread[0]);
   ASSERT_TRUE(SUCCESS(rc));

   // Wait till 1st user-thread gets to its wait-for-stop loop
   while (!thread_cfg[0].waitfor_stop_signal) {
      platform_sleep_ns(USEC_TO_NSEC(100000)); // 100 msec.
   }
   thread_cfg[1].tasks          = data->tasks;
   thread_cfg[1].exp_thread_idx = task_get_max_tid(data->tasks);

   // We've used up all threads. This thread creation should fail.
   rc = task_thread_create("test_one_thread",
                           exec_user_thread_loop_for_stop,
                           &thread_cfg[1],
                           trunk_get_scratch_size(),
                           data->tasks,
                           data->hid,
                           &new_thread[1]);
   ASSERT_FALSE(SUCCESS(rc),
                "Thread should not have been created"
                ", new_thread=%lu, max_tid=%lu\n",
                new_thread[1],
                task_get_max_tid(data->tasks));

   // Stop the running user-thread now that our test is done.
   thread_cfg[0].stop_thread = TRUE;

   for (uint64 tctr = 0; tctr < ARRAY_SIZE(new_thread); tctr++) {
      if (new_thread[tctr]) {
         rc = platform_thread_join(new_thread[tctr]);
         ASSERT_TRUE(SUCCESS(rc));
      }
   }
   set_log_streams_for_tests(MSG_LEVEL_INFO);
}

/* Wrapper function to create Splinter Task system w/o background threads. */
static platform_status
create_task_system_without_bg_threads(void *datap)
{
   // Cast void * datap to ptr-to-CTEST_DATA() struct in use.
   struct CTEST_IMPL_DATA_SNAME(task_system) *data =
      (struct CTEST_IMPL_DATA_SNAME(task_system) *)datap;

   platform_status rc = STATUS_OK;

   // no background threads by default.
   uint64 num_bg_threads[NUM_TASK_TYPES] = {0};
   rc = task_system_config_init(&data->task_cfg,
                                TRUE, // use stats
                                num_bg_threads,
                                trunk_get_scratch_size());
   ASSERT_TRUE(SUCCESS(rc));
   rc = task_system_create(data->hid, data->ioh, &data->tasks, &data->task_cfg);
   return rc;
}

/*
 * Wrapper function to create Splinter Task system with background threads.
 * We wait till all the background threads start-up, so test code can reliably
 * check various states of threads and bitmasks.
 */
static platform_status
create_task_system_with_bg_threads(void  *datap,
                                   uint64 num_memtable_bg_threads,
                                   uint64 num_normal_bg_threads)
{
   platform_status rc;
   // Cast void * datap to ptr-to-CTEST_DATA() struct in use.
   struct CTEST_IMPL_DATA_SNAME(task_system) *data =
      (struct CTEST_IMPL_DATA_SNAME(task_system) *)datap;

   uint64 num_bg_threads[NUM_TASK_TYPES] = {0};
   num_bg_threads[TASK_TYPE_MEMTABLE]    = num_memtable_bg_threads;
   num_bg_threads[TASK_TYPE_NORMAL]      = num_normal_bg_threads;
   rc = task_system_config_init(&data->task_cfg,
                                TRUE, // use stats
                                num_bg_threads,
                                trunk_get_scratch_size());
   ASSERT_TRUE(SUCCESS(rc));

   rc = task_system_create(data->hid, data->ioh, &data->tasks, &data->task_cfg);
   if (!SUCCESS(rc)) {
      return rc;
   }

   // Wait-for all background threads to startup.
   uint64   nbg_threads   = (num_memtable_bg_threads + num_normal_bg_threads);
   threadid max_thread_id = task_get_max_tid(data->tasks);
   while (max_thread_id < nbg_threads) {
      platform_sleep_ns(USEC_TO_NSEC(100000)); // 100 msec.
      max_thread_id = task_get_max_tid(data->tasks);
   }
   return rc;
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

   CTEST_LOG_INFO("active_threads_bitmask=0x%lx\n",
                  thread_cfg->active_threads_bitmask);

   // This is the important call to initialize thread-specific stuff in
   // Splinter's task-system, which sets up the thread-id (index) and records
   // this thread as active with the task system.
   task_register_this_thread(thread_cfg->tasks, trunk_get_scratch_size());

   uint64 task_bitmask_after_register =
      task_active_tasks_mask(thread_cfg->tasks);

   // _Now, the active tasks bitmask should have changed.
   ASSERT_NOT_EQUAL(task_bitmask_before_register, task_bitmask_after_register);

   threadid this_threads_idx = platform_get_tid();
   ASSERT_EQUAL(thread_cfg->exp_thread_idx,
                this_threads_idx,
                "exp_thread_idx=%lu, this_threads_idx=%lu\n",
                thread_cfg->exp_thread_idx,
                this_threads_idx);

   // This thread is recorded as 'being active' by clearing its bit from the
   // mask.
   uint64 exp_bitmask = (0x1L << this_threads_idx);
   exp_bitmask        = (task_bitmask_before_register & ~exp_bitmask);

   CTEST_LOG_INFO("this_threads_idx=%lu"
                  ", task_bitmask_before_register=0x%lx"
                  ", task_bitmask_after_register=0x%lx"
                  ", exp_bitmask=0x%lx\n",
                  this_threads_idx,
                  task_bitmask_before_register,
                  task_bitmask_after_register,
                  exp_bitmask);

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
   ASSERT_EQUAL(INVALID_TID,
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

   // Before registration, thread ID should be in an uninit'ed state
   ASSERT_EQUAL(INVALID_TID, platform_get_tid());

   task_register_this_thread(thread_cfg->tasks, trunk_get_scratch_size());

   threadid this_threads_index = platform_get_tid();

   ASSERT_TRUE((this_threads_index < MAX_THREADS),
               "Thread [%lu] Registered thread idx = %lu is invalid.",
               thread_cfg->exp_thread_idx,
               this_threads_index);

   // Test case is carefully constructed to fire-up n-threads. Wait for
   // them to all start-up.
   while (task_get_max_tid(thread_cfg->tasks) < MAX_THREADS) {
      platform_sleep_ns(USEC_TO_NSEC(100000)); // 100 msec.
   }

   task_deregister_this_thread(thread_cfg->tasks);

   // Register / de-register of thread with SplinterDB's task system is just
   // SplinterDB's jugglery to keep track of resources. Deregistration should
   // have re-init'ed the thread ID.
   threadid get_tid_after_deregister = platform_get_tid();
   ASSERT_EQUAL(INVALID_TID,
                get_tid_after_deregister,
                "get_tid_after_deregister=%lu should be an invalid tid, %lu",
                get_tid_after_deregister,
                INVALID_TID);
}

/*
 * exec_user_thread_loop_for_stop() - Worker routine executed by a single
 * thread which will simply wait-for 'stop' command that will be sent by
 * outer main thread. This function exists simply to use up a thread.
 */
static void
exec_user_thread_loop_for_stop(void *arg)
{
   thread_config_lockstep *thread_cfg = (thread_config_lockstep *)arg;

   // If we got this far, thread was created. Reconfirm expected index.
   threadid this_threads_idx = platform_get_tid();
   ASSERT_EQUAL(thread_cfg->exp_thread_idx, this_threads_idx);

   // We should have used up all available threads. Next create should fail.
   ASSERT_EQUAL(thread_cfg->exp_max_tid,
                task_get_max_tid(thread_cfg->tasks),
                "Max tid is incorrect for thread created on line=%d\n",
                thread_cfg->line);

   // The calling interface has already registered this thread. All we do
   // here is sit in a loop, pretending to do some Splinter work, while waiting
   // for a notification to stop ourselves.
   thread_cfg->waitfor_stop_signal = TRUE;
   while (!thread_cfg->stop_thread) {
      platform_sleep_ns(USEC_TO_NSEC(100000)); // 100 msec.
   }
   CTEST_LOG_INFO("Last user thread ID=%lu, created on line=%d exiting ...\n",
                  this_threads_idx,
                  thread_cfg->line);
}
