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
#include "config.h"
#include "shmem.h"
#include "trunk.h"
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
   platform_heap_id   hid;
   platform_module_id mid;
   bool               use_shmem;
};

CTEST_SETUP(platform_api)
{
   platform_status rc = STATUS_OK;
   bool use_shmem     = config_parse_use_shmem(Ctest_argc, (char **)Ctest_argv);

   uint64 heap_capacity = (256 * MiB); // small heap is sufficient.
   data->mid            = platform_get_module_id();
   data->use_shmem      = use_shmem;
   rc = platform_heap_create(data->mid, heap_capacity, use_shmem, &data->hid);
   platform_assert_status_ok(rc);
}

CTEST_TEARDOWN(platform_api)
{
   platform_heap_destroy(&data->hid);
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
 *
 * - TYPED_MALLOC(), TYPED_ZALLOC() -
 * - TYPED_ALIGNED_MALLOC(), TYPED_ALIGNED_ZALLOC()
 *
 * - TYPED_ARRAY_MALLOC(), TYPED_ARRAY_ZALLOC()
 *   TYPED_FLEXIBLE_STRUCT_MALLOC(), TYPED_FLEXIBLE_STRUCT_ZALLOC()
 *   These interfaces need an on-stack platform_memfrag{} struct for allocation
 *   and to call the free() interface.
 *
 * For case of test execution with shared memory, do a small verification
 * that used / free memory metrics are correct before/after allocation/free.
 * ----------------------------------------------------------------------------
 */
CTEST2(platform_api, test_TYPED_MALLOC)
{
   size_t used_bytes_before_alloc = 0;
   size_t free_bytes_before_alloc = 0;
   size_t used_bytes_after_free   = 0;
   size_t free_bytes_after_free   = 0;

   if (data->use_shmem) {
      used_bytes_before_alloc = platform_shmbytes_used(data->hid);
      free_bytes_before_alloc = platform_shmbytes_free(data->hid);
   }
   platform_memfrag memfrag_structp;
   any_struct      *structp = TYPED_MALLOC(data->hid, structp);
   platform_free(&memfrag_structp);
   if (data->use_shmem) {
      used_bytes_after_free = platform_shmbytes_used(data->hid);
      free_bytes_after_free = platform_shmbytes_free(data->hid);

      ASSERT_EQUAL(used_bytes_before_alloc, used_bytes_after_free);
      ASSERT_EQUAL(free_bytes_before_alloc, free_bytes_after_free);
   }
}

CTEST2(platform_api, test_TYPED_ZALLOC)
{
   size_t used_bytes_before_alloc = 0;
   size_t free_bytes_before_alloc = 0;
   size_t used_bytes_after_free   = 0;
   size_t free_bytes_after_free   = 0;

   if (data->use_shmem) {
      used_bytes_before_alloc = platform_shmbytes_used(data->hid);
      free_bytes_before_alloc = platform_shmbytes_free(data->hid);
   }
   platform_memfrag memfrag_structp;
   any_struct      *structp = TYPED_ZALLOC(data->hid, structp);
   platform_free(&memfrag_structp);
   if (data->use_shmem) {
      used_bytes_after_free = platform_shmbytes_used(data->hid);
      free_bytes_after_free = platform_shmbytes_free(data->hid);

      ASSERT_EQUAL(used_bytes_before_alloc, used_bytes_after_free);
      ASSERT_EQUAL(free_bytes_before_alloc, free_bytes_after_free);
   }
}

CTEST2(platform_api, test_TYPED_MALLOC_free_and_MALLOC)
{
   platform_memfrag memfrag_structp;
   any_struct      *structp      = TYPED_MALLOC(data->hid, structp);
   any_struct      *save_structp = structp;
   platform_free(&memfrag_structp);

   platform_memfrag memfrag_new_structp;
   any_struct      *new_structp = TYPED_MALLOC(data->hid, new_structp);

   // Memory for small structures should be recycled from shared memory
   ASSERT_TRUE(!data->use_shmem || (save_structp == new_structp));

   platform_free(&memfrag_new_structp);
}

CTEST2(platform_api, test_TYPED_ALIGNED_ZALLOC)
{
   size_t used_bytes_before_alloc = 0;
   size_t free_bytes_before_alloc = 0;
   size_t used_bytes_after_free   = 0;
   size_t free_bytes_after_free   = 0;

   if (data->use_shmem) {
      used_bytes_before_alloc = platform_shmbytes_used(data->hid);
      free_bytes_before_alloc = platform_shmbytes_free(data->hid);
   }
   platform_memfrag memfrag_structp;
   any_struct      *structp = TYPED_ALIGNED_ZALLOC(
      data->hid, PLATFORM_CACHELINE_SIZE, structp, sizeof(*structp) * 7);

   platform_free(&memfrag_structp);
   if (data->use_shmem) {
      used_bytes_after_free = platform_shmbytes_used(data->hid);
      free_bytes_after_free = platform_shmbytes_free(data->hid);

      ASSERT_EQUAL(used_bytes_before_alloc, used_bytes_after_free);
      ASSERT_EQUAL(free_bytes_before_alloc, free_bytes_after_free);
   }
}

CTEST2(platform_api, test_TYPED_ARRAY_MALLOC)
{
   int    nitems                  = 10;
   size_t used_bytes_before_alloc = 0;
   size_t free_bytes_before_alloc = 0;
   size_t used_bytes_after_free   = 0;
   size_t free_bytes_after_free   = 0;

   if (data->use_shmem) {
      used_bytes_before_alloc = platform_shmbytes_used(data->hid);
      free_bytes_before_alloc = platform_shmbytes_free(data->hid);
   }
   platform_memfrag memfrag_structp;
   any_struct      *structp = TYPED_ARRAY_MALLOC(data->hid, structp, nitems);

   platform_free(&memfrag_structp);
   if (data->use_shmem) {
      used_bytes_after_free = platform_shmbytes_used(data->hid);
      free_bytes_after_free = platform_shmbytes_free(data->hid);

      ASSERT_EQUAL(used_bytes_before_alloc, used_bytes_after_free);
      ASSERT_EQUAL(free_bytes_before_alloc, free_bytes_after_free);
   }
}

CTEST2(platform_api, test_platform_free_interface)
{
   platform_memfrag memfrag_structp;
   any_struct      *structp = TYPED_MALLOC(data->hid, structp);
   platform_free_mem(data->hid, structp, memfrag_size(&memfrag_structp));
}

/*
 * Dumb test-case to show how -not- to invoke the platform_free() API.
 * If you allocate using platform_memfrag{}, but free the allocated memory
 * directly, you will certainly get a memory leak.
 */
CTEST2_SKIP(platform_api, test_incorrect_usage_of_free)
{
   int    nitems                  = 13;
   size_t used_bytes_before_alloc = 0;
   size_t free_bytes_before_alloc = 0;
   size_t memory_allocated        = 0;
   size_t used_bytes_after_free   = 0;
   size_t free_bytes_after_free   = 0;

   if (data->use_shmem) {
      used_bytes_before_alloc = platform_shmbytes_used(data->hid);
      free_bytes_before_alloc = platform_shmbytes_free(data->hid);
   }
   platform_memfrag memfrag_structp;
   any_struct      *structp = TYPED_ARRAY_MALLOC(data->hid, structp, nitems);
   memory_allocated         = memfrag_structp.size;

   // Incorrect usage of free ... Memory fragment will be freed but there will
   // be an error in computing memory usage metrics, resulting in a slow
   // memory leak (of sorts).
   platform_free(&memfrag_structp);
   if (data->use_shmem) {
      used_bytes_after_free = platform_shmbytes_used(data->hid);
      free_bytes_after_free = platform_shmbytes_free(data->hid);

      // These asserts document "an error condition", just so the test can pass.
      ASSERT_NOT_EQUAL(used_bytes_before_alloc, used_bytes_after_free);
      ASSERT_NOT_EQUAL(free_bytes_before_alloc, free_bytes_after_free);

      printf("memory_allocated=%lu"
             ", used_bytes_after_free=%lu (!= used_bytes_before_alloc=%lu)"
             ", free_bytes_after_free=%lu (!= free_bytes_before_alloc=%lu)\n",
             memory_allocated,
             used_bytes_after_free,
             used_bytes_before_alloc,
             free_bytes_after_free,
             free_bytes_before_alloc);
   }
}

/*
 * White-box test to verify small free-fragment free-list management.
 * We track only some small ranges of sizes in the free-list:
 *   32 < x <= 64, <= 128, <= 256, <= 512
 * This test case is designed carefully to allocate a fragment in the
 * range (256, 512]. Then it's freed. A smaller fragment that falls in
 * this bucket is requested, which should find and reallocate the
 * free'd fragment.
 */
CTEST2(platform_api, test_TYPED_ARRAY_MALLOC_free_and_MALLOC)
{
   int              nitems = 10;
   platform_memfrag memfrag_arrayp;
   any_struct      *arrayp = TYPED_ARRAY_MALLOC(data->hid, arrayp, nitems);

   platform_free(&memfrag_arrayp);

   // If you re-request the same array, memory fragment should be recycled
   platform_memfrag memfrag_new_arrayp;
   any_struct *new_arrayp = TYPED_ARRAY_MALLOC(data->hid, new_arrayp, nitems);
   ASSERT_TRUE(!data->use_shmem || (arrayp == new_arrayp));
   platform_free(&memfrag_new_arrayp);

   // Allocating a smaller array should also recycle memory fragment.
   // We recycle fragments in sizes of powers-of-2. So, use a new size
   // so it will trigger a search in the free-list that the previous
   // fragment's free ended up in.
   nitems     = 9;
   new_arrayp = TYPED_ARRAY_MALLOC(data->hid, new_arrayp, nitems);
   ASSERT_TRUE(!data->use_shmem || (arrayp == new_arrayp));
   platform_free(&memfrag_new_arrayp);
}

/*
 * Allocate and free small fragments of different size so that they fall
 * into different buckets. Free them in random order to exercise the
 * management of the array tracking allocated small fragments.
 */
CTEST2(platform_api, test_alloc_free_multiple_small_frags)
{
   size_t used_bytes_before_allocs = 0;
   size_t free_bytes_before_allocs = 0;
   size_t memory_allocated         = 0;

   if (data->use_shmem) {
      used_bytes_before_allocs = platform_shmbytes_used(data->hid);
      free_bytes_before_allocs = platform_shmbytes_free(data->hid);
   }
   platform_memfrag memfrag_s1_40b;
   char            *s1_40b = TYPED_ARRAY_MALLOC(data->hid, s1_40b, 40);
   memory_allocated += memfrag_size(&memfrag_s1_40b);

   platform_memfrag memfrag_s1_80b;
   char            *s1_80b = TYPED_ARRAY_MALLOC(data->hid, s1_80b, 80);
   memory_allocated += memfrag_size(&memfrag_s1_80b);

   platform_memfrag memfrag_s1_160b;
   char            *s1_160b = TYPED_ARRAY_MALLOC(data->hid, s1_160b, 160);
   memory_allocated += memfrag_size(&memfrag_s1_160b);

   platform_memfrag memfrag_s1_200b;
   char            *s1_200b = TYPED_ARRAY_MALLOC(data->hid, s1_200b, 200);
   memory_allocated += memfrag_size(&memfrag_s1_200b);

   size_t used_bytes_after_allocs = 0;
   if (data->use_shmem) {
      used_bytes_after_allocs = platform_shmbytes_used(data->hid);

      ASSERT_EQUAL((used_bytes_after_allocs - used_bytes_before_allocs),
                   memory_allocated);
   }

   platform_free(&memfrag_s1_80b);

   platform_free(&memfrag_s1_40b);

   platform_free(&memfrag_s1_160b);

   platform_free(&memfrag_s1_200b);

   size_t used_bytes_after_frees = 0;
   size_t free_bytes_after_frees = 0;
   if (data->use_shmem) {
      used_bytes_after_frees = platform_shmbytes_used(data->hid);
      free_bytes_after_frees = platform_shmbytes_free(data->hid);

      ASSERT_EQUAL(used_bytes_before_allocs, used_bytes_after_frees);
      ASSERT_EQUAL(free_bytes_before_allocs, free_bytes_after_frees);
   }
}

CTEST2(platform_api, test_large_TYPED_MALLOC)
{
   platform_memfrag      memfrag_iter;
   trunk_range_iterator *iter = TYPED_MALLOC(data->hid, iter);
   platform_free(&memfrag_iter);
}

/*
 * Basic test case to verify that memory for large fragments is being
 * recycled as expected.
 */
CTEST2(platform_api, test_large_TYPED_MALLOC_free_and_MALLOC)
{
   platform_memfrag      memfrag_iter;
   trunk_range_iterator *iter = TYPED_MALLOC(data->hid, iter);
   // This struct should be larger than the threshold at which large
   // free fragment strategy kicks-in.
   ASSERT_TRUE(sizeof(*iter) >= SHM_LARGE_FRAG_SIZE);

   trunk_range_iterator *save_iter = iter;
   platform_free(&memfrag_iter);

   platform_memfrag      memfrag_new_iter;
   trunk_range_iterator *new_iter = TYPED_MALLOC(data->hid, new_iter);

   // Memory for large structures should be recycled from shared memory
   ASSERT_TRUE(!data->use_shmem || (save_iter == new_iter),
               "use_shmem=%d, save_iter=%p, new_iter=%p"
               ", sizeof() requested struct=%lu",
               data->use_shmem,
               save_iter,
               new_iter,
               sizeof(*iter));
   platform_free(&memfrag_new_iter);
}

CTEST2(platform_api, test_TYPED_ARRAY_MALLOC_MF)
{
   size_t old_mem_used =
      (data->use_shmem ? platform_shmbytes_used(data->hid) : 0);
   size_t old_mem_free =
      (data->use_shmem ? platform_shmbytes_free(data->hid) : 0);

   platform_memfrag memfrag_structp;
   any_struct      *structp = TYPED_ARRAY_MALLOC(data->hid, structp, 20);
   platform_free(&memfrag_structp);

   size_t new_mem_used =
      (data->use_shmem ? platform_shmbytes_used(data->hid) : 0);
   size_t new_mem_free =
      (data->use_shmem ? platform_shmbytes_free(data->hid) : 0);
   ASSERT_EQUAL(old_mem_used, new_mem_used);
   ASSERT_EQUAL(old_mem_free, new_mem_free);
}

CTEST2(platform_api, test_TYPED_ARRAY_ZALLOC_MF)
{
   size_t old_mem_used =
      (data->use_shmem ? platform_shmbytes_used(data->hid) : 0);

   platform_memfrag memfrag_structp;
   any_struct      *structp = TYPED_ARRAY_ZALLOC(data->hid, structp, 10);
   platform_free(&memfrag_structp);

   size_t new_mem_used =
      (data->use_shmem ? platform_shmbytes_used(data->hid) : 0);
   ASSERT_EQUAL(old_mem_used, new_mem_used);
}
