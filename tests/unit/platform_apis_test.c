// Copyright 2023 VMware, Inc.
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
#include "platform.h"
#include "shmem.h"
#include "test_misc_common.h"
#include "unit_tests.h"

// Define a struct to be used for memory allocation.
typedef struct any_struct {
   struct any_struct *prev;
   struct any_struct *next;
   size_t             nbytes;
   uint32             value;
   uint32             size;
} any_struct;

/*
 * Global data declaration macro:
 */
CTEST_DATA(platform_api)
{
   // Declare heap handles for platform heap memory.
   platform_heap_handle hh;
   platform_heap_id     hid;
   platform_module_id   mid;
   bool                 use_shmem;
};

CTEST_SETUP(platform_api)
{
   platform_status rc = STATUS_OK;
   data->use_shmem    = test_using_shmem(Ctest_argc, (char **)Ctest_argv);

   uint64 heap_capacity = (256 * MiB); // small heap is sufficient.
   data->mid            = platform_get_module_id();
   rc                   = platform_heap_create(
      data->mid, heap_capacity, data->use_shmem, &data->hh, &data->hid);
   platform_assert_status_ok(rc);
}

CTEST_TEARDOWN(platform_api)
{
   platform_status rc = platform_heap_destroy(&data->hh);
   ASSERT_TRUE(SUCCESS(rc));
}

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
   platform_spinlock  slock;
   platform_module_id unused = 0;

   platform_status rc = platform_spinlock_init(&slock, unused, data->hid);
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

/*
 * ----------------------------------------------------------------------------
 * Exercise all the memory allocation interfaces, followed by a free, to ensure
 * that all combinations work cleanly, w/ and w/o shared memory.
 * ----------------------------------------------------------------------------
 */
CTEST2(platform_api, test_TYPED_MALLOC)
{
   any_struct *structp = TYPED_MALLOC(data->hid, structp);
   platform_free(data->hid, structp);
}

CTEST2(platform_api, test_TYPED_ZALLOC)
{
   any_struct *structp = TYPED_ZALLOC(data->hid, structp);
   platform_free(data->hid, structp);
}

CTEST2(platform_api, test_TYPED_ARRAY_MALLOC_MF)
{
   size_t old_mem_used = (data->use_shmem ? platform_shmused(data->hid) : 0);

   platform_memfrag  memfrag_structp;
   any_struct       *structp = TYPED_ARRAY_MALLOC_MF(data->hid, structp, 20);
   platform_memfrag *mf      = &memfrag_structp;
   platform_free(data->hid, mf);

   size_t new_mem_used = (data->use_shmem ? platform_shmused(data->hid) : 0);
   ASSERT_EQUAL(old_mem_used, new_mem_used);
}

CTEST2(platform_api, test_TYPED_ARRAY_ZALLOC_MF)
{
   size_t old_mem_used = (data->use_shmem ? platform_shmused(data->hid) : 0);

   platform_memfrag  memfrag_structp;
   any_struct       *structp = TYPED_ARRAY_ZALLOC_MF(data->hid, structp, 10);
   platform_memfrag *mf      = &memfrag_structp;
   platform_free(data->hid, mf);

   size_t new_mem_used = (data->use_shmem ? platform_shmused(data->hid) : 0);
   ASSERT_EQUAL(old_mem_used, new_mem_used);
}
