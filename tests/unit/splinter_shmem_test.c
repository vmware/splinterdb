// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinter_shmem_test.c --
 *
 *  Exercises the interfaces in SplinterDB shared memory allocation module.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "platform.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.
#include "shmem.h"
#include "splinterdb/splinterdb.h"
#include "splinterdb/default_data_config.h"

#define TEST_MAX_KEY_SIZE 42 // Just something to get going ...

// Test these many threads concurrently performing memory allocation.
#define TEST_MAX_THREADS 8

/*
 * To test heavily concurrent memory allocation from the shared memory, each
 * thread will allocate a small fragment described by this structure. We then
 * validate that the fragments are not clobbered by concurrent allocations.
 */
typedef struct shm_memfrag {
   threadid            owner;
   struct shm_memfrag *next;
} shm_memfrag;

// Configuration for each worker thread
typedef struct {
   splinterdb     *splinter;
   platform_thread this_thread_id; // OS-generated thread ID
   threadid        exp_thread_idx; // Splinter-generated expected thread index
   shm_memfrag    *start;          // Start of chain of allocated memfrags
} thread_config;

// Function prototypes
static void
setup_cfg_for_test(splinterdb_config *out_cfg, data_config *default_data_cfg);

static void
exec_thread_memalloc(void *arg);

/*
 * Global data declaration macro:
 */
CTEST_DATA(splinter_shmem)
{
   // Declare heap handles to shake out shared memory based allocation.
   size_t           shmem_capacity; // In bytes
   platform_heap_id hid;
};

// By default, all test cases will deal with small shared memory segment.
CTEST_SETUP(splinter_shmem)
{
   data->shmem_capacity = (256 * MiB); // bytes
   platform_status rc   = platform_heap_create(
      platform_get_module_id(), data->shmem_capacity, TRUE, &data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   // Enable tracing all allocs / frees from shmem for this test.
   platform_enable_tracing_shm_ops();
}

// Tear down the test shared segment.
CTEST_TEARDOWN(splinter_shmem)
{
   platform_heap_destroy(&data->hid);
}

/*
 * Basic test case. This goes through the basic create / destroy
 * interfaces to setup a shared memory segment. While at it, run through
 * few lookup interfaces to validate sizes.
 */
CTEST2(splinter_shmem, test_create_destroy_shmem)
{
   platform_heap_id hid           = NULL;
   size_t           requested     = (512 * MiB); // bytes
   size_t           heap_capacity = requested;
   platform_status  rc =
      platform_heap_create(platform_get_module_id(), heap_capacity, TRUE, &hid);
   ASSERT_TRUE(SUCCESS(rc));

   // Total size of shared segment must be what requested for.
   ASSERT_EQUAL(platform_shmsize(hid), requested);

   // A small chunk at the head is used for shmem_info{} tracking struct
   ASSERT_EQUAL(platform_shmbytes_free(hid),
                (requested - platform_shm_ctrlblock_size()));

   // Destroy shared memory and release memory.
   platform_shmdestroy(&hid);
   ASSERT_TRUE(hid == NULL);
}

/*
 * ---------------------------------------------------------------------------
 * Test that used space and pad-bytes tracking is happening correctly
 * when all allocation requests are fully aligned. No pad bytes should
 * have been generated for alignment.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_aligned_allocations)
{
   int keybuf_size = 64;
   int msgbuf_size = (2 * keybuf_size);

   // Self-documenting assertion ... to future-proof this area.
   ASSERT_EQUAL(keybuf_size, PLATFORM_CACHELINE_SIZE);

   void  *next_free = platform_shm_next_free_addr(data->hid);
   uint8 *keybuf    = TYPED_MANUAL_MALLOC(data->hid, keybuf, keybuf_size);

   // Validate returned memory-ptrs, knowing that no pad bytes were needed.
   ASSERT_TRUE((void *)keybuf == next_free);

   next_free     = platform_shm_next_free_addr(data->hid);
   uint8 *msgbuf = TYPED_MANUAL_MALLOC(data->hid, msgbuf, msgbuf_size);
   ASSERT_TRUE((void *)msgbuf == next_free);

   // Sum of requested alloc-sizes == total # of used-bytes
   ASSERT_EQUAL((keybuf_size + msgbuf_size), platform_shmbytes_used(data->hid));

   // Free bytes left in shared segment == (sum of requested alloc sizes, less
   // a small bit of the control block.)
   ASSERT_EQUAL((data->shmem_capacity
                 - (keybuf_size + msgbuf_size + platform_shm_ctrlblock_size())),
                platform_shmbytes_free(data->hid));
}

/*
 * ---------------------------------------------------------------------------
 * Test that used space and pad-bytes tracking is happening correctly
 * when some allocation requests are not-fully aligned. Test verifies the
 * tracking and computation of pad-bytes, free/used space.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_unaligned_allocations)
{
   void  *next_free   = platform_shm_next_free_addr(data->hid);
   int    keybuf_size = 42;
   uint8 *keybuf      = TYPED_MANUAL_MALLOC(data->hid, keybuf, keybuf_size);

   int keybuf_pad =
      platform_align_bytes_reqd(PLATFORM_CACHELINE_SIZE, keybuf_size);

   // Sum of requested allocation + pad-bytes == total # of used-bytes
   ASSERT_EQUAL((keybuf_size + keybuf_pad), platform_shmbytes_used(data->hid));

   // Should have allocated what was previously determined as next free byte.
   ASSERT_TRUE((void *)keybuf == next_free);

   // Validate returned memory-ptrs, knowing that pad bytes were needed.
   next_free = platform_shm_next_free_addr(data->hid);
   ASSERT_TRUE(next_free == (void *)keybuf + keybuf_size + keybuf_pad);

   int msgbuf_size = 100;
   int msgbuf_pad =
      platform_align_bytes_reqd(PLATFORM_CACHELINE_SIZE, msgbuf_size);
   uint8 *msgbuf = TYPED_MANUAL_MALLOC(data->hid, msgbuf, msgbuf_size);

   // Next allocation will abut prev-allocation + pad-bytes
   ASSERT_TRUE((void *)msgbuf == (void *)keybuf + keybuf_size + keybuf_pad);

   // Sum of requested allocation + pad-bytes == total # of used-bytes
   ASSERT_EQUAL((keybuf_size + keybuf_pad + msgbuf_size + msgbuf_pad),
                platform_shmbytes_used(data->hid));

   // After accounting for the control block, next-free-addr should be
   // exactly past the 2 allocations + their pad-bytes.
   next_free      = platform_shm_next_free_addr(data->hid);
   void *exp_free = ((void *)platform_heap_id_to_shmaddr(data->hid)
                     + platform_shm_ctrlblock_size() + keybuf_size + keybuf_pad
                     + msgbuf_size + msgbuf_pad);
   ASSERT_TRUE(next_free == exp_free,
               "next_free=%p != exp_free=%p\n",
               next_free,
               exp_free);
}

/*
 * ---------------------------------------------------------------------------
 * Test allocation requests that result in an OOM from shared segment.
 * Verify limits of memory allocation and handling of free/used bytes.
 * These stats are maintained w/o full spinlocks, so will be approximate
 * in concurrent scenarios. But for single-threaded allocations, these stats
 * should be accurate even when shmem-OOMs occur.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_allocations_causing_OOMs)
{
   int keybuf_size = 64;

   // Self-documenting assertion ... to future-proof this area.
   ASSERT_EQUAL(keybuf_size, PLATFORM_CACHELINE_SIZE);

   void  *next_free = platform_shm_next_free_addr(data->hid);
   uint8 *keybuf    = TYPED_MANUAL_MALLOC(data->hid, keybuf, keybuf_size);

   // Validate returned memory-ptr, knowing that no pad bytes were needed.
   ASSERT_TRUE((void *)keybuf == next_free);

   next_free = platform_shm_next_free_addr(data->hid);

   size_t space_left =
      (data->shmem_capacity - (keybuf_size + platform_shm_ctrlblock_size()));

   ASSERT_EQUAL(space_left, platform_shmbytes_free(data->hid));

   platform_error_log("\nNOTE: Test case intentionally triggers out-of-space"
                      " errors in shared segment. 'Insufficient memory'"
                      " error messages below are to be expected.\n");

   // Note that although we have asked for 1 more byte than free space available
   // the allocation interfaces round-up the # bytes for alignment. So the
   // requested # of bytes will be a bit larger than free space in the error
   // message you will see below.
   keybuf_size       = (space_left + 1);
   uint8 *keybuf_oom = TYPED_MANUAL_MALLOC(data->hid, keybuf_oom, keybuf_size);
   ASSERT_TRUE(keybuf_oom == NULL);

   // Free space counter is not touched if allocation fails.
   ASSERT_EQUAL(space_left, platform_shmbytes_free(data->hid));

   // As every memory request is rounded-up for alignment, the space left
   // counter should always be an integral multiple of this constant.
   ASSERT_EQUAL(0, (space_left % PLATFORM_CACHELINE_SIZE));

   // If we request exactly what's available, it should succeed.
   keybuf_size = space_left;
   uint8 *keybuf_no_oom =
      TYPED_MANUAL_MALLOC(data->hid, keybuf_no_oom, keybuf_size);
   ASSERT_TRUE(keybuf_no_oom != NULL);
   CTEST_LOG_INFO("Successfully allocated all remaining %lu bytes "
                  "from shared segment.\n",
                  space_left);

   // We should be out of space by now.
   ASSERT_EQUAL(0, platform_shmbytes_free(data->hid));

   // This should fail.
   keybuf_size = 1;
   keybuf_oom  = TYPED_MANUAL_MALLOC(data->hid, keybuf_oom, keybuf_size);
   ASSERT_TRUE(keybuf_oom == NULL);

   // Free allocated memory before exiting.
   platform_free(data->hid, keybuf);
   platform_free(data->hid, keybuf_no_oom);
}

/*
 * ---------------------------------------------------------------------------
 * Test allocation interface using platform_get_heap_id() accessor, which
 * is supposed to return in-use heap-ID. But, by default, this is NULL. This
 * test shows that using this API will [correctly] allocate from shared memory
 * once we've created the shared segment, and, therefore, all call-sites in
 * the running library to platform_get_heap_id() should return the right
 * handle(s) to the shared segment.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_allocations_using_get_heap_id)
{
   int keybuf_size = 64;

   void  *next_free = platform_shm_next_free_addr(data->hid);
   uint8 *keybuf =
      TYPED_MANUAL_MALLOC(platform_get_heap_id(), keybuf, keybuf_size);

   // Validate returned memory-ptrs, knowing that no pad bytes were needed.
   ASSERT_TRUE((void *)keybuf == next_free);
}

/*
 * ---------------------------------------------------------------------------
 * Currently 'free' is a no-op; no space is released. Do minimal testing of
 * this feature, to ensure that at least the code flow is exectuing correctly.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_free)
{
   int    keybuf_size = 64;
   uint8 *keybuf      = TYPED_MANUAL_MALLOC(data->hid, keybuf, keybuf_size);

   int    msgbuf_size = (2 * keybuf_size);
   uint8 *msgbuf      = TYPED_MANUAL_MALLOC(data->hid, msgbuf, msgbuf_size);

   size_t mem_used = platform_shmbytes_used(data->hid);

   void *next_free = platform_shm_next_free_addr(data->hid);

   platform_free(data->hid, keybuf);

   // Even though we freed some memory, the next addr-to-allocate is unchanged.
   ASSERT_TRUE(next_free == platform_shm_next_free_addr(data->hid));

   // Space used remains unchanged, as free didn't quite return any memory
   ASSERT_EQUAL(mem_used, platform_shmbytes_used(data->hid));
}

/*
 * ---------------------------------------------------------------------------
 * test_concurrent_allocs_by_n_threads() - Verify concurrency control
 * implemented during shared memory allocation.
 *
 * Exercise concurrent memory allocations from the shared memory of small
 * memory fragments. Each thread will record its ownership on the fragment
 * allocated. After all memory is exhausted, we cross-check the chain of
 * fragments allocated by each thread to verify that fragment still shows up
 * as owned by the allocating thread.
 *
 * In the rudimentary version of allocation from shared memory, we did not have
 * any concurrency control for allocations. So, it's likely that we may have
 * been clobbering allocated memory.
 *
 * This test case does a basic verification of the fixes implemented to avoid
 * such races during concurrent memory allocation.
 *
 * NOTE: This test case will exit immediately upon finding the first fragment
 * whose ownership is flawed. That may still leave many other fragments waiting
 * to be discovered with flawed ownership.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_concurrent_allocs_by_n_threads)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   platform_disable_tracing_shm_ops();

   ZERO_STRUCT(cfg);
   ZERO_STRUCT(default_data_cfg);

   default_data_config_init(TEST_MAX_KEY_SIZE, &default_data_cfg);
   setup_cfg_for_test(&cfg, &default_data_cfg);

   int rv = splinterdb_create(&cfg, &kvsb);
   ASSERT_EQUAL(0, rv);

   // Setup multiple threads for concurrent memory allocation.
   platform_thread new_thread;
   thread_config   thread_cfg[TEST_MAX_THREADS];
   thread_config  *thread_cfgp = NULL;
   int             tctr        = 0;
   platform_status rc          = STATUS_OK;

   ZERO_ARRAY(thread_cfg);

   platform_error_log("\nExecute %d concurrent threads peforming memory"
                      " allocation till we run out of memory in the shared"
                      " segment.\n'Insufficient memory' error messages"
                      " below are to be expected.\n",
                      TEST_MAX_THREADS);

   // Start-up n-threads, record their expected thread-IDs, which will be
   // validated by the thread's execution function below.
   for (tctr = 1, thread_cfgp = &thread_cfg[tctr];
        tctr < ARRAY_SIZE(thread_cfg);
        tctr++, thread_cfgp++)
   {
      // These are independent of the new thread's creation.
      thread_cfgp->splinter       = kvsb;
      thread_cfgp->exp_thread_idx = tctr;

      rc = platform_thread_create(
         &new_thread, FALSE, exec_thread_memalloc, thread_cfgp, NULL);
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

   // Now run thru memory fragments allocated by each thread and verify that
   // the identity recorded is kosher. If the same memory fragment was allocated
   // to multiple threads, we should catch that error here.
   for (tctr = 1, thread_cfgp = &thread_cfg[tctr];
        tctr < ARRAY_SIZE(thread_cfg);
        tctr++, thread_cfgp++)
   {
      shm_memfrag *this_frag = thread_cfgp->start;
      while (this_frag) {
         ASSERT_EQUAL(tctr,
                      this_frag->owner,
                      "Owner=%lu of memory frag=%p is not expected owner=%lu\n",
                      this_frag->owner,
                      this_frag,
                      tctr);
         this_frag = this_frag->next;
      }
   }

   splinterdb_close(&kvsb);

   platform_enable_tracing_shm_ops();
}

/*
 * ---------------------------------------------------------------------------
 * Test allocation, free and re-allocation of a large fragment should find
 * this large fragment in the local tracker. That previously allocated
 * fragment should be re-allocated. "Next-free-ptr" should, therefore, remain
 * unchanged.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_realloc_of_large_fragment)
{
   void *next_free = platform_shm_next_free_addr(data->hid);

   // Large fragments are tracked if their size >= this size.
   size_t size   = (1 * MiB);
   uint8 *keybuf = TYPED_MANUAL_MALLOC(data->hid, keybuf, size);

   // Validate that a new large fragment will create a new allocation.
   ASSERT_TRUE((void *)keybuf == next_free);

   // Re-establish next-free-ptr after this large allocation. We will use it
   // below to assert that this location will not change when we re-use this
   // large fragment for reallocation after it's been freed.
   next_free = platform_shm_next_free_addr(data->hid);

   // Save this off, as free below will NULL out handle.
   uint8 *keybuf_old = keybuf;

   // If you free this fragment and reallocate exactly the same size,
   // it should recycle the freed fragment.
   platform_free(data->hid, keybuf);

   uint8 *keybuf_new = TYPED_MANUAL_MALLOC(data->hid, keybuf_new, size);
   ASSERT_TRUE((keybuf_old == keybuf_new),
               "keybuf_old=%p, keybuf_new=%p\n",
               keybuf_old,
               keybuf_new);

   // We have re-used freed fragment, so the next-free-ptr should be unchanged.
   ASSERT_TRUE(next_free == platform_shm_next_free_addr(data->hid));

   platform_free(data->hid, keybuf_new);
}

/*
 * ---------------------------------------------------------------------------
 * Test that free followed by a request of the same size will reallocate the
 * recently-freed fragment, avoiding any existing in-use fragments of the same
 * size.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_free_realloc_around_inuse_fragments)
{
   void *next_free = platform_shm_next_free_addr(data->hid);

   // Large fragments are tracked if their size >= this size.
   size_t size         = (1 * MiB);
   uint8 *keybuf1_1MiB = TYPED_MANUAL_MALLOC(data->hid, keybuf1_1MiB, size);
   uint8 *keybuf2_1MiB = TYPED_MANUAL_MALLOC(data->hid, keybuf2_1MiB, size);
   uint8 *keybuf3_1MiB = TYPED_MANUAL_MALLOC(data->hid, keybuf3_1MiB, size);

   // Re-establish next-free-ptr after this large allocation. We will use it
   // below to assert that this location will not change when we re-use a
   // large fragment for reallocation after it's been freed.
   next_free = platform_shm_next_free_addr(data->hid);

   // Save off fragment handles as free will NULL out ptr.
   uint8 *old_keybuf2_1MiB = keybuf2_1MiB;

   // Free the middle fragment that should get reallocated, below.
   platform_free(data->hid, keybuf2_1MiB);

   // Re-request (new) fragments of the same size.
   keybuf2_1MiB = TYPED_MANUAL_MALLOC(data->hid, keybuf2_1MiB, size);
   ASSERT_TRUE((keybuf2_1MiB == old_keybuf2_1MiB),
               "Expected to satisfy new 1MiB request at %p"
               " with old 1MiB fragment ptr at %p\n",
               keybuf2_1MiB,
               old_keybuf2_1MiB);

   ASSERT_TRUE(next_free == platform_shm_next_free_addr(data->hid));

   // As large-fragments allocated / freed are tracked in an array, verify
   // that we will find the 1st one upon a re-request after a free.
   uint8 *old_keybuf1_1MiB = keybuf1_1MiB;
   platform_free(data->hid, keybuf1_1MiB);
   platform_free(data->hid, keybuf2_1MiB);

   // This re-request should re-allocate the 1st free fragment found.
   keybuf2_1MiB = TYPED_MANUAL_MALLOC(data->hid, keybuf2_1MiB, size);
   ASSERT_TRUE((keybuf2_1MiB == old_keybuf1_1MiB),
               "Expected to satisfy new 1MiB request at %p"
               " with old 1MiB fragment ptr at %p\n",
               keybuf2_1MiB,
               old_keybuf1_1MiB);

   // We've already freed keybuf1_1MiB; can't free a NULL ptr again.
   // platform_free(data->hid, keybuf1_1MiB);

   platform_free(data->hid, keybuf2_1MiB);
   platform_free(data->hid, keybuf3_1MiB);
}

/*
 * ---------------------------------------------------------------------------
 * Finding a free-fragment that's tracked for re-allocation implements a
 * very naive linear-search; first-fit algorigthm. This test case verifies
 * that:
 *
 * - Allocate 3 fragments of 1MiB, 5MiB, 2MiB
 * - Free them all.
 * - Request for a 2MiB fragment. 2nd free fragment (5MiB) will be used.
 * - Request for a 5MiB fragment. We will allocate a new fragment.
 *
 * An improved best-fit search algorithm would have allocated the free 2MiB
 * and then satisfy the next request with the free 5 MiB fragment.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_realloc_of_free_fragments_uses_first_fit)
{
   void *next_free = platform_shm_next_free_addr(data->hid);

   // Large fragments are tracked if their size >= this size.
   size_t size        = (1 * MiB);
   uint8 *keybuf_1MiB = TYPED_MANUAL_MALLOC(data->hid, keybuf_1MiB, size);

   size               = (5 * MiB);
   uint8 *keybuf_5MiB = TYPED_MANUAL_MALLOC(data->hid, keybuf_5MiB, size);

   size               = (2 * MiB);
   uint8 *keybuf_2MiB = TYPED_MANUAL_MALLOC(data->hid, keybuf_2MiB, size);

   // Re-establish next-free-ptr after this large allocation. We will use it
   // below to assert that this location will not change when we re-use a
   // large fragment for reallocation after it's been freed.
   next_free = platform_shm_next_free_addr(data->hid);

   // Save off fragment handles as free will NULL out ptr.
   uint8 *old_keybuf_1MiB = keybuf_1MiB;
   uint8 *old_keybuf_5MiB = keybuf_5MiB;
   uint8 *old_keybuf_2MiB = keybuf_2MiB;

   // Order in which we free these fragments does not matter.
   platform_free(data->hid, keybuf_1MiB);
   platform_free(data->hid, keybuf_2MiB);
   platform_free(data->hid, keybuf_5MiB);

   // Re-request (new) fragments in diff size order.
   size        = (2 * MiB);
   keybuf_2MiB = TYPED_MANUAL_MALLOC(data->hid, keybuf_2MiB, size);
   ASSERT_TRUE((keybuf_2MiB == old_keybuf_5MiB),
               "Expected to satisfy new 2MiB request at %p"
               " with old 5MiB fragment ptr at %p\n",
               keybuf_2MiB,
               old_keybuf_5MiB);
   ;

   // We have re-used freed 5MiB fragment; so next-free-ptr should be unchanged.
   ASSERT_TRUE(next_free == platform_shm_next_free_addr(data->hid));

   size        = (5 * MiB);
   keybuf_5MiB = TYPED_MANUAL_MALLOC(data->hid, keybuf_5MiB, size);

   // We allocated a new fragment at next-free-ptr
   ASSERT_TRUE(keybuf_5MiB != old_keybuf_1MiB);
   ASSERT_TRUE(keybuf_5MiB != old_keybuf_2MiB);
   ASSERT_TRUE(keybuf_5MiB == next_free);

   platform_free(data->hid, keybuf_2MiB);
   platform_free(data->hid, keybuf_5MiB);
}

static void
setup_cfg_for_test(splinterdb_config *out_cfg, data_config *default_data_cfg)
{
   *out_cfg = (splinterdb_config){.filename   = TEST_DB_NAME,
                                  .cache_size = 512 * Mega,
                                  .disk_size  = 2 * Giga,
                                  .use_shmem  = TRUE,
                                  .data_cfg   = default_data_cfg};
}

/*
 * exec_thread_memalloc() - Worker fn for each thread to do concurrent memory
 * allocation from the shared segment.
 */
static void
exec_thread_memalloc(void *arg)
{
   thread_config *thread_cfg = (thread_config *)arg;
   splinterdb    *kvs        = thread_cfg->splinter;

   splinterdb_register_thread(kvs);

   // Allocate a new memory fragment and connect head to output variable for
   // thread
   shm_memfrag **fragpp   = &thread_cfg->start;
   shm_memfrag  *new_frag = NULL;

   uint64   nallocs         = 0;
   threadid this_thread_idx = thread_cfg->exp_thread_idx;

   // Keep allocating fragments till we run out of memory.
   // Build a linked list of memory fragments for this thread.
   while ((new_frag = TYPED_ZALLOC(platform_get_heap_id(), new_frag)) != NULL) {
      *fragpp         = new_frag;
      new_frag->owner = this_thread_idx;
      fragpp          = &new_frag->next;
      nallocs++;
   }
   splinterdb_deregister_thread(kvs);

   platform_default_log(
      "Thread-ID=%lu allocated %lu memory fragments of %lu bytes each.\n",
      this_thread_idx,
      nallocs,
      sizeof(*new_frag));
}
