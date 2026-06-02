// Copyright 2023-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * platform_apis_test.c
 *
 *  Exercise some of the interfaces in platform.c . Specifically, when
 * this test is run with --use-shmem option, we exercise interfaces that
 *  - Create a shared segment and use it as "the heap" for memory allocations
 *  - Invoke pthread-synchronization primitives that can be shared between any
 *    threads that have access to the memory containing the object, including
 *    threads in different processes.
 * -----------------------------------------------------------------------------
 */
#include <sys/mman.h>

#include "ctest.h" // This is required for all test-case files.
#include "config.h"
#include "unit_tests.h"
#include "platform_semaphore.h"
#include "platform_threads.h"
#include "platform_units.h"
#include "platform_buffer.h"
#include "platform_typed_alloc.h"
#include "platform_spinlock.h"
#include "platform_mutex.h"
#include "platform_condvar.h"

/*
 * Global data declaration macro:
 */
CTEST_DATA(platform_api)
{
   // Declare heap handles for platform heap memory.
   platform_heap_id   hid;
   platform_module_id mid;
};

CTEST_SETUP(platform_api)
{
   platform_register_thread_auto();
   platform_status rc = STATUS_OK;
   bool use_shmem     = config_parse_use_shmem(Ctest_argc, (char **)Ctest_argv);

   uint64 heap_capacity = (256 * MiB); // small heap is sufficient.
   data->mid            = platform_get_module_id();
   rc = platform_heap_create(data->mid, heap_capacity, use_shmem, &data->hid);
   platform_assert_status_ok(rc);
}

CTEST_TEARDOWN(platform_api)
{
   platform_memory_fault_disable();
   platform_heap_destroy(&data->hid);
   platform_deregister_thread();
}

#if PLATFORM_MEMORY_FAULT_INJECTION
#   define CTEST_MEMORY_FAULT CTEST2
#else
#   define CTEST_MEMORY_FAULT CTEST2_SKIP
#endif

/*
 * Test platform_buffer_init() and platform_buffer_deinit().
 */
CTEST2(platform_api, test_platform_buffer_init)
{
   platform_status rc = STATUS_NO_MEMORY;

   buffer_handle bh;
   ZERO_CONTENTS(&bh);

   rc = platform_buffer_init(&bh, KiB);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_TRUE(bh.addr != NULL);
   ASSERT_TRUE(bh.addr == platform_buffer_getaddr(&bh));
   ASSERT_TRUE(bh.length == KiB);

   rc = platform_buffer_deinit(&bh);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_TRUE(bh.addr == NULL);
   ASSERT_TRUE(bh.length == 0);
}

/*
 * Test failure to mmap() a very large buffer_init().
 */
CTEST2(platform_api, test_platform_buffer_init_fails_for_very_large_length)
{
   set_log_streams_for_tests(MSG_LEVEL_ERRORS);
   platform_status rc = STATUS_NO_MEMORY;

   buffer_handle bh;
   ZERO_CONTENTS(&bh);

   size_t length = (1024 * KiB * GiB);

   // On most test machines we use, this is expected to fail as mmap() under
   // here will fail for very large lengths. (If this test case ever fails,
   // check the 'length' here and the machine's configuration to see why
   // mmap() unexpectedly succeeded.)
   rc = platform_buffer_init(&bh, length);
   ASSERT_FALSE(SUCCESS(rc));
   ASSERT_TRUE(bh.addr == NULL);
   ASSERT_TRUE(bh.length == 0);

   // deinit() would fail horribly when nothing was successfully mmap()'ed
   rc = platform_buffer_deinit(&bh);
   ASSERT_FALSE(SUCCESS(rc));

   set_log_streams_for_tests(MSG_LEVEL_INFO);
}

CTEST_MEMORY_FAULT(platform_api, test_memory_fault_range)
{
   platform_memory_fault_disable();

   platform_memory_fault_config cfg;
   platform_memory_fault_config_get(&cfg);
   cfg.mode        = PLATFORM_MEMORY_FAULT_RANGE;
   cfg.range_start = 2;
   cfg.range_count = 2;
   platform_memory_fault_config_set(&cfg);

   char                          *p0 = TYPED_ARRAY_MALLOC(data->hid, p0, 8);
   platform_memory_fault_counters unarmed_counters =
      platform_memory_fault_get_counters();
   platform_memory_fault_enable();

   char *p1 = TYPED_ARRAY_MALLOC(data->hid, p1, 8);
   char *p2 = TYPED_ARRAY_MALLOC(data->hid, p2, 8);
   char *p3 = TYPED_ARRAY_MALLOC(data->hid, p3, 8);
   char *p4 = TYPED_ARRAY_MALLOC(data->hid, p4, 8);

   bool32                         p0_ok = p0 != NULL;
   bool32                         p1_ok = p1 != NULL;
   bool32                         p2_ok = p2 != NULL;
   bool32                         p3_ok = p3 != NULL;
   bool32                         p4_ok = p4 != NULL;
   platform_memory_fault_counters counters =
      platform_memory_fault_get_counters();

   platform_memory_fault_disable();
   platform_memory_fault_counters disabled_counters =
      platform_memory_fault_get_counters();
   platform_free(data->hid, p1);
   platform_free(data->hid, p4);
   platform_free(data->hid, p0);

   ASSERT_TRUE(p0_ok);
   ASSERT_EQUAL(0, unarmed_counters.alloc_count);
   ASSERT_EQUAL(0, unarmed_counters.failure_count);
   ASSERT_TRUE(p1_ok);
   ASSERT_FALSE(p2_ok);
   ASSERT_FALSE(p3_ok);
   ASSERT_TRUE(p4_ok);
   ASSERT_EQUAL(4, counters.alloc_count);
   ASSERT_EQUAL(2, counters.failure_count);
   ASSERT_EQUAL(counters.alloc_count, disabled_counters.alloc_count);
   ASSERT_EQUAL(counters.failure_count, disabled_counters.failure_count);
}

CTEST_MEMORY_FAULT(platform_api, test_memory_fault_random_respects_max_failures)
{
   platform_memory_fault_disable();

   platform_memory_fault_config cfg;
   platform_memory_fault_config_get(&cfg);
   cfg.mode                           = PLATFORM_MEMORY_FAULT_RANDOM;
   cfg.seed                           = 42;
   cfg.random_fail_probability        = PLATFORM_MEMORY_FAULT_PROBABILITY_SCALE;
   cfg.random_burst_start_probability = 0;
   cfg.max_failures                   = 2;
   platform_memory_fault_config_set(&cfg);
   platform_memory_fault_enable();

   char *p1 = TYPED_ARRAY_MALLOC(data->hid, p1, 8);
   char *p2 = TYPED_ARRAY_MALLOC(data->hid, p2, 8);
   char *p3 = TYPED_ARRAY_MALLOC(data->hid, p3, 8);

   bool32                         p1_ok = p1 != NULL;
   bool32                         p2_ok = p2 != NULL;
   bool32                         p3_ok = p3 != NULL;
   platform_memory_fault_counters counters =
      platform_memory_fault_get_counters();

   platform_memory_fault_disable();
   platform_memory_fault_counters disabled_counters =
      platform_memory_fault_get_counters();
   platform_free(data->hid, p3);

   ASSERT_FALSE(p1_ok);
   ASSERT_FALSE(p2_ok);
   ASSERT_TRUE(p3_ok);
   ASSERT_EQUAL(3, counters.alloc_count);
   ASSERT_EQUAL(2, counters.failure_count);
   ASSERT_EQUAL(counters.alloc_count, disabled_counters.alloc_count);
   ASSERT_EQUAL(counters.failure_count, disabled_counters.failure_count);
}


/*
 * Exercise platform_semaphore_init() to ensure that changes basically work.
 * Both are void interfaces, but will assert in debug mode.
 */
CTEST2(platform_api, test_platform_semaphore_init_destroy)
{
   platform_semaphore psema;
   platform_semaphore_init(&psema, 0, data->hid);
   platform_semaphore_destroy(&psema);
}

/*
 * Exercise platform_spinlock_init() to ensure that changes basically work.
 */
CTEST2(platform_api, test_platform_spinlock_init_destroy)
{
   platform_spinlock slock;

   platform_status rc = platform_spinlock_init(&slock);
   ASSERT_TRUE(SUCCESS(rc));

   rc = platform_spinlock_destroy(&slock);
   ASSERT_TRUE(SUCCESS(rc));
}
/*
 * Exercise platform_mutex_init() to ensure that changes basically work.
 *
 * Ref:
 * https://man7.org/linux/man-pages/man3/pthread_mutexattr_getpshared.3.html
 *  - pthread_mutexattr_setpshared() could return ENOSUP if implementation
 *    does not support it.
 */
CTEST2(platform_api, test_platform_mutex_init_destroy)
{
   platform_mutex     lock;
   platform_module_id unused = 0;

   set_log_streams_for_tests(MSG_LEVEL_ERRORS);

   platform_status rc = platform_mutex_init(&lock, unused, data->hid);
   if (STATUS_IS_EQ(rc, STATUS_NOTSUP)) {
      platform_error_log(
         "Platform possibly does not support"
         " process-shared mutexes, " STRINGIFY(PTHREAD_PROCESS_SHARED));
   }
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_EQUAL(INVALID_TID, lock.owner);

   rc = platform_mutex_destroy(&lock);
   ASSERT_TRUE(SUCCESS(rc));
   set_log_streams_for_tests(MSG_LEVEL_INFO);
}

/*
 * Exercise platform_condvar_init() to ensure that changes basically work.
 * And that there are no memory leaks, reported by SAN-build tests.
 */
CTEST2(platform_api, test_platform_condvar_init_destroy)
{
   platform_condvar cv;
   platform_status  rc = platform_condvar_init(&cv, data->hid);
   if (STATUS_IS_EQ(rc, STATUS_NOTSUP)) {
      platform_error_log(
         "Platform possibly does not support"
         " process-shared mutexes, " STRINGIFY(PTHREAD_PROCESS_SHARED));
   }
   ASSERT_TRUE(SUCCESS(rc));

   platform_condvar_destroy(&cv);
}
