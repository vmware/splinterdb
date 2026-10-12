// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <sys/wait.h>
#include <unistd.h>

#include "config.h"
#include "ctest.h"
#include "notification.h"
#include "platform_threads.h"
#include "platform_typed_alloc.h"
#include "unit_tests.h"

CTEST_DATA(notification)
{
   platform_heap_id hid;
};

CTEST_SETUP(notification)
{
   platform_register_thread();
   bool use_shmem     = config_parse_use_shmem(Ctest_argc, (char **)Ctest_argv);
   platform_status rc = platform_heap_create(
      platform_get_module_id(), 1 * Mega, use_shmem, &data->hid);
   ASSERT_TRUE(SUCCESS(rc));
}

CTEST_TEARDOWN(notification)
{
   platform_memory_fault_disable();
   platform_heap_destroy(&data->hid);
   platform_deregister_thread();
}

CTEST2(notification, test_completion_before_waiter_release)
{
   splinterdb_notification *note;
   ASSERT_TRUE(SUCCESS(notification_create_blocking(data->hid, &note)));
   ASSERT_FALSE(splinterdb_notification_poll(note, NULL));

   splinterdb_notification_complete(note, STATUS_IO_ERROR);
   /* Completion drops its reference, but the waiter must still be able to
    * read the result and safely destroy the condition variable afterward. */
   int status = 0;
   ASSERT_TRUE(splinterdb_notification_poll(note, &status));
   ASSERT_EQUAL(EIO, status);
   notification_release_blocking(note);
}

static void *
complete_notification(void *arg)
{
   splinterdb_notification_complete(arg, STATUS_IO_ERROR);
   return NULL;
}

CTEST2(notification, test_waiter_abandons_before_completion)
{
   splinterdb_notification *note;
   ASSERT_TRUE(SUCCESS(notification_create_blocking(data->hid, &note)));
   /* Model an early wait error: no caller may touch the note afterward, but
    * the accepted operation still owns it and completes on another thread. */
   notification_release_blocking(note);
   pthread_t thread;
   ASSERT_EQUAL(0, pthread_create(&thread, NULL, complete_notification, note));
   ASSERT_EQUAL(0, pthread_join(thread, NULL));
}

typedef struct notification_race {
   splinterdb_notification *note;
   pthread_barrier_t        barrier;
} notification_race;

static void *
complete_at_barrier(void *arg)
{
   notification_race *race = arg;
   pthread_barrier_wait(&race->barrier);
   splinterdb_notification_complete(race->note, STATUS_OK);
   return NULL;
}

CTEST2(notification, test_concurrent_final_releases)
{
   notification_race race;
   ASSERT_EQUAL(0, pthread_barrier_init(&race.barrier, NULL, 2));
   for (uint64 i = 0; i < 256; i++) {
      ASSERT_TRUE(SUCCESS(notification_create_blocking(data->hid, &race.note)));
      pthread_t thread;
      ASSERT_EQUAL(0,
                   pthread_create(&thread, NULL, complete_at_barrier, &race));
      pthread_barrier_wait(&race.barrier);
      notification_release_blocking(race.note);
      ASSERT_EQUAL(0, pthread_join(thread, NULL));
   }
   ASSERT_EQUAL(0, pthread_barrier_destroy(&race.barrier));
}

CTEST2(notification, test_repeated_early_release_and_rejected_submission)
{
   /* The shared-heap run has only 1 MiB. Repetition exceeds that capacity if
    * either the abandoned-wait or rejected-submission path leaks a note. */
   for (uint64 i = 0; i < 16384; i++) {
      splinterdb_notification *note;
      ASSERT_TRUE(SUCCESS(notification_create_blocking(data->hid, &note)));
      notification_release_blocking(note);
      splinterdb_notification_complete(note, STATUS_OK);

      ASSERT_TRUE(SUCCESS(notification_create_blocking(data->hid, &note)));
      /* No operation accepted the completion reference. */
      notification_release_blocking(note);
      notification_release_blocking(note);
   }
}

CTEST2(notification, test_completion_in_another_process)
{
   platform_heap_id hid;
   ASSERT_TRUE(SUCCESS(
      platform_heap_create(platform_get_module_id(), 1 * Mega, TRUE, &hid)));

   splinterdb_notification *note;
   ASSERT_TRUE(SUCCESS(notification_create_blocking(hid, &note)));
   pid_t child = fork();
   ASSERT_TRUE(child >= 0);
   if (child == 0) {
      splinterdb_notification_complete(note, STATUS_IO_ERROR);
      _exit(0);
   }
   int child_status = -1;
   ASSERT_EQUAL(child, waitpid(child, &child_status, 0));
   ASSERT_TRUE(WIFEXITED(child_status));
   ASSERT_EQUAL(0, WEXITSTATUS(child_status));
   int status = 0;
   ASSERT_TRUE(splinterdb_notification_poll(note, &status));
   ASSERT_EQUAL(EIO, status);
   notification_release_blocking(note);

   /* Transfer the last reference to a child after the caller abandons wait. */
   ASSERT_TRUE(SUCCESS(notification_create_blocking(hid, &note)));
   notification_release_blocking(note);
   child = fork();
   ASSERT_TRUE(child >= 0);
   if (child == 0) {
      splinterdb_notification_complete(note, STATUS_OK);
      _exit(0);
   }
   ASSERT_EQUAL(child, waitpid(child, &child_status, 0));
   ASSERT_TRUE(WIFEXITED(child_status));
   ASSERT_EQUAL(0, WEXITSTATUS(child_status));
   platform_heap_destroy(&hid);
}

CTEST2(notification, test_caller_owned_notification_remains_caller_owned)
{
   int user_data = 42;
   for (uint64 i = 0; i < 256; i++) {
      splinterdb_notification *note = TYPED_MALLOC(data->hid, note);
      ASSERT_NOT_NULL(note);
      splinterdb_notification_init_polling(note, &user_data);
      ASSERT_TRUE(splinterdb_notification_user_data(note) == &user_data);
      pthread_t thread;
      ASSERT_EQUAL(0,
                   pthread_create(&thread, NULL, complete_notification, note));
      int status = 0;
      while (!splinterdb_notification_poll(note, &status)) {
         sched_yield();
      }
      ASSERT_EQUAL(EIO, status);
      /* The API permits immediate destruction after observing completion.
       * Free before joining, so ASan can catch forbidden callback accesses
       * after the completion mutex was unlocked. */
      splinterdb_notification_deinit(note);
      platform_free(data->hid, note);
      ASSERT_EQUAL(0, pthread_join(thread, NULL));
   }
}

#if PLATFORM_MEMORY_FAULT_INJECTION
CTEST2(notification, test_blocking_allocation_failure)
#else
CTEST2_SKIP(notification, test_blocking_allocation_failure)
#endif
{
   platform_memory_fault_config saved_cfg;
   platform_memory_fault_config_get(&saved_cfg);
   platform_memory_fault_config cfg = saved_cfg;
   cfg.mode                         = PLATFORM_MEMORY_FAULT_RANGE;
   cfg.range_start                  = 1;
   cfg.range_count                  = 1;
   cfg.max_failures                 = 1;
   platform_memory_fault_config_set(&cfg);
   platform_memory_fault_enable();
   splinterdb_notification *note = SPLINTERDB_NOTIFICATION_FIRE_AND_FORGET;
   platform_status          rc = notification_create_blocking(data->hid, &note);
   platform_memory_fault_disable();
   platform_memory_fault_config_set(&saved_cfg);

   ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_NO_MEMORY));
   ASSERT_TRUE(note == NULL);
}
