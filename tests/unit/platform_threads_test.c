// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * platform_threads_test.c --
 *
 *  Validate platform thread registration state that is not covered by task
 *  system tests.
 * -----------------------------------------------------------------------------
 */

#include <sys/wait.h>
#include <unistd.h>

#include "ctest.h" // This is required for all test-case files.
#include "platform_threads.h"
#include "unit_tests.h"

enum {
   CHILD_OK = 0,
   CHILD_REGISTER_FAILED,
   CHILD_INVALID_TID_AFTER_REGISTER,
   CHILD_INVALID_PID_AFTER_REGISTER,
   CHILD_TID_LEAKED_AFTER_DEREGISTER,
   CHILD_PID_LEAKED_AFTER_DEREGISTER,
   CHILD_FUNCTION_RETURNED,
};

typedef void (*forked_child_fn)(void);

static const char *
child_status_to_string(int status)
{
   switch (status) {
      case CHILD_OK:
         return "ok";
      case CHILD_REGISTER_FAILED:
         return "register failed";
      case CHILD_INVALID_TID_AFTER_REGISTER:
         return "invalid tid after register";
      case CHILD_INVALID_PID_AFTER_REGISTER:
         return "invalid pid after register";
      case CHILD_TID_LEAKED_AFTER_DEREGISTER:
         return "tid leaked after deregister";
      case CHILD_PID_LEAKED_AFTER_DEREGISTER:
         return "pid leaked after deregister";
      case CHILD_FUNCTION_RETURNED:
         return "child function returned";
      default:
         return "unknown child status";
   }
}

static void
child_register_and_check_ids(void)
{
   int rc = platform_register_thread();
   if (rc != 0) {
      _exit(CHILD_REGISTER_FAILED);
   }

   if (platform_get_tid() == INVALID_TID) {
      platform_deregister_thread();
      _exit(CHILD_INVALID_TID_AFTER_REGISTER);
   }

   if (platform_linux_get_pid() >= MAX_THREADS) {
      platform_deregister_thread();
      _exit(CHILD_INVALID_PID_AFTER_REGISTER);
   }
}

static void
register_and_deregister_in_child(void)
{
   child_register_and_check_ids();

   platform_deregister_thread();

   if (platform_get_tid() != INVALID_TID) {
      _exit(CHILD_TID_LEAKED_AFTER_DEREGISTER);
   }

   if (platform_linux_get_pid() != INVALID_TID) {
      _exit(CHILD_PID_LEAKED_AFTER_DEREGISTER);
   }

   _exit(CHILD_OK);
}

static void
register_and_exit_in_child(void)
{
   child_register_and_check_ids();

   pthread_exit(NULL);
}

static void
wait_for_child(pid_t pid, uint64 iteration)
{
   int   status = 0;
   pid_t waited = waitpid(pid, &status, 0);
   ASSERT_EQUAL(pid, waited, "waitpid failed for child %d", pid);

   ASSERT_TRUE(WIFEXITED(status),
               "child %d exited abnormally; signal=%d",
               pid,
               WIFSIGNALED(status) ? WTERMSIG(status) : 0);
   ASSERT_EQUAL(CHILD_OK,
                WEXITSTATUS(status),
                "child %d failed on iteration %lu: %s",
                pid,
                iteration,
                child_status_to_string(WEXITSTATUS(status)));
}

static void
assert_parent_registration_unchanged(threadid parent_tid, threadid parent_pid)
{
   ASSERT_EQUAL(parent_tid, platform_get_tid());
   ASSERT_EQUAL(parent_pid, platform_linux_get_pid());
   ASSERT_EQUAL(1, platform_num_threads());
}

static void
churn_forked_children(forked_child_fn child_fn)
{
   threadid parent_tid = platform_get_tid();
   threadid parent_pid = platform_linux_get_pid();

   ASSERT_TRUE(parent_tid < MAX_THREADS);
   ASSERT_TRUE(parent_pid < MAX_THREADS);
   ASSERT_EQUAL(1, platform_num_threads());

   /*
    * The parent holds one TID/PID slot. If child cleanup leaks either kind of
    * slot, sequential children will exhaust the remaining slots before this
    * loop completes.
    */
   for (uint64 i = 0; i < MAX_THREADS + 1; i++) {
      pid_t pid = fork();
      ASSERT_TRUE(pid >= 0, "fork failed on iteration %lu", i);

      if (pid == 0) {
         child_fn();
         _exit(CHILD_FUNCTION_RETURNED);
      }

      wait_for_child(pid, i);
      assert_parent_registration_unchanged(parent_tid, parent_pid);
   }
}

CTEST_DATA(platform_threads){};

CTEST_SETUP(platform_threads)
{
   platform_register_thread();
}

CTEST_TEARDOWN(platform_threads)
{
   platform_deregister_thread();
}

CTEST2(platform_threads, test_forked_child_pid_slots_are_reused)
{
   (void)data;

   churn_forked_children(register_and_deregister_in_child);
}

CTEST2(platform_threads, test_forked_child_auto_cleanup_reuses_tid_and_pid_slots)
{
   (void)data;

   churn_forked_children(register_and_exit_in_child);
}
