// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinter_shmem_test.c --
 *
 *  Exercises the interfaces in SplinterDB shared memory allocation module.
 *  Also includes tests for memory management of fingerprint object, an
 *  area that was very troubling during development.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "platform.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.
#include "shmem.h"
#include "splinterdb/splinterdb.h"
#include "splinterdb/default_data_config.h"
#include "util.h"

#define TEST_MAX_KEY_SIZE 42 // Just something to get going ...

// Size of an on-stack buffer used for testing
#define WB_ONSTACK_BUFSIZE 30

// Thread Configuration: Only used as a struct for memory allocation.
typedef struct {
   splinterdb     *splinter;
   platform_thread this_thread_id; // OS-generated thread ID
   threadid        exp_thread_idx; // Splinter-generated expected thread index
   void           *start;          // Start of chain of allocated memfrags
} thread_config;

// Function prototypes
static void
setup_cfg_for_test(splinterdb_config *out_cfg, data_config *default_data_cfg);

/*
 * Global data declaration macro:
 */
CTEST_DATA(splinter_shmem)
{
   // Declare heap handles to shake out shared memory based allocation.
   size_t               shmem_capacity; // In bytes
   platform_heap_handle hh;
   platform_heap_id     hid;
};

// By default, all test cases will deal with small shared memory segment.
CTEST_SETUP(splinter_shmem)
{
   data->shmem_capacity = (256 * MiB); // bytes
   platform_status rc   = platform_heap_create(platform_get_module_id(),
                                             data->shmem_capacity,
                                             TRUE,
                                             &data->hh,
                                             &data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   // Enable tracing all allocs / frees from shmem for this test.
   platform_enable_tracing_shm_ops();
}

// Tear down the test shared segment.
CTEST_TEARDOWN(splinter_shmem)
{
   platform_status rc = platform_heap_destroy(&data->hh);
   ASSERT_TRUE(SUCCESS(rc));
}

/*
 * Basic test case. This goes through the basic create / destroy
 * interfaces to setup a shared memory segment. While at it, run through
 * few lookup interfaces to validate sizes.
 */
CTEST2(splinter_shmem, test_create_destroy_shmem)
{
   platform_heap_handle hh            = NULL;
   platform_heap_id     hid           = NULL;
   size_t               requested     = (512 * MiB); // bytes
   size_t               heap_capacity = requested;
   platform_status      rc            = platform_heap_create(
      platform_get_module_id(), heap_capacity, TRUE, &hh, &hid);
   ASSERT_TRUE(SUCCESS(rc));

   // Total size of shared segment must be what requested for.
   ASSERT_EQUAL(platform_shmsize(hid), requested);

   // A small chunk at the head is used for shmem_info{} tracking struct
   ASSERT_EQUAL(platform_shmfree(hid),
                (requested - platform_shm_ctrlblock_size()));

   // Destroy shared memory and release memory.
   platform_shmdestroy(&hh);
   ASSERT_TRUE(hh == NULL);
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

   void            *next_free = platform_shm_next_free_addr(data->hid);
   platform_memfrag memfrag_keybuf;
   uint8           *keybuf = TYPED_ARRAY_MALLOC(data->hid, keybuf, keybuf_size);

   // Validate returned memory-ptrs, knowing that no pad bytes were needed.
   ASSERT_TRUE((void *)keybuf == next_free);

   next_free = platform_shm_next_free_addr(data->hid);
   platform_memfrag memfrag_msgbuf;
   uint8           *msgbuf = TYPED_ARRAY_MALLOC(data->hid, msgbuf, msgbuf_size);
   ASSERT_TRUE((void *)msgbuf == next_free);

   // Sum of requested alloc-sizes == total # of used-bytes
   ASSERT_EQUAL((keybuf_size + msgbuf_size), platform_shmused(data->hid));

   // Free bytes left in shared segment == (sum of requested alloc sizes, less
   // a small bit of the control block.)
   ASSERT_EQUAL((data->shmem_capacity
                 - (keybuf_size + msgbuf_size + platform_shm_ctrlblock_size())),
                platform_shmfree(data->hid));
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
   void            *next_free   = platform_shm_next_free_addr(data->hid);
   int              keybuf_size = 42;
   platform_memfrag memfrag_keybuf;
   uint8           *keybuf = TYPED_ARRAY_MALLOC(data->hid, keybuf, keybuf_size);

   int keybuf_pad = platform_alignment(PLATFORM_CACHELINE_SIZE, keybuf_size);

   // Sum of requested allocation + pad-bytes == total # of used-bytes
   ASSERT_EQUAL((keybuf_size + keybuf_pad), platform_shmused(data->hid));

   // Should have allocated what was previously determined as next free byte.
   ASSERT_TRUE((void *)keybuf == next_free);

   // Validate returned memory-ptrs, knowing that pad bytes were needed.
   next_free = platform_shm_next_free_addr(data->hid);
   ASSERT_TRUE(next_free == (void *)keybuf + keybuf_size + keybuf_pad);

   int msgbuf_size = 100;
   int msgbuf_pad  = platform_alignment(PLATFORM_CACHELINE_SIZE, msgbuf_size);
   platform_memfrag memfrag_msgbuf;
   uint8           *msgbuf = TYPED_ARRAY_MALLOC(data->hid, msgbuf, msgbuf_size);

   // Next allocation will abut prev-allocation + pad-bytes
   ASSERT_TRUE((void *)msgbuf == (void *)keybuf + keybuf_size + keybuf_pad);

   // Sum of requested allocation + pad-bytes == total # of used-bytes
   ASSERT_EQUAL((keybuf_size + keybuf_pad + msgbuf_size + msgbuf_pad),
                platform_shmused(data->hid));

   // After accounting for the control block, next-free-addr should be
   // exactly past the 2 allocations + their pad-bytes.
   next_free = platform_shm_next_free_addr(data->hid);
   ASSERT_TRUE(next_free
               == ((void *)data->hh + platform_shm_ctrlblock_size()
                   + keybuf_size + keybuf_pad + msgbuf_size + msgbuf_pad));
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

   void            *next_free = platform_shm_next_free_addr(data->hid);
   platform_memfrag memfrag_keybuf;
   uint8           *keybuf =
      TYPED_ARRAY_MALLOC(platform_get_heap_id(), keybuf, keybuf_size);

   // Validate returned memory-ptrs, knowing that no pad bytes were needed.
   ASSERT_TRUE((void *)keybuf == next_free);

   platform_memfrag *mf = &memfrag_keybuf;
   platform_free(platform_get_heap_id(), mf);
}

/*
 * Basic test of 'free' where a freed-fragment goes to a free-list. Verify that
 * the freed-fragment is found in the expected free-list, by-size.
 */
CTEST2(splinter_shmem, test_basic_free_list_size)
{
   int              keybuf_size = 64;
   platform_memfrag memfrag_keybuf;
   uint8           *keybuf = TYPED_ARRAY_MALLOC(data->hid, keybuf, keybuf_size);

   // Fragment is still allocated, so should not be in any free-list(s).
   ASSERT_EQUAL(0, platform_shm_find_freed_frag(data->hid, keybuf, NULL));

   platform_memfrag *mf = &memfrag_keybuf;
   platform_free(data->hid, mf);

   // A freed-fragment should go its appropriate free-list by-size.
   ASSERT_EQUAL(keybuf_size,
                platform_shm_find_freed_frag(data->hid, keybuf, NULL));

   // Variation testing out padding due to alignment
   keybuf_size = 100;
   keybuf      = TYPED_ARRAY_MALLOC(data->hid, keybuf, keybuf_size);

   // Memory allocation would have padded bytes up to cache line alignment.
   size_t exp_memfrag_size = keybuf_size;
   exp_memfrag_size += platform_alignment(PLATFORM_CACHELINE_SIZE, keybuf_size);
   ASSERT_EQUAL(exp_memfrag_size, memfrag_size(mf));

   platform_free(data->hid, mf);
   ASSERT_EQUAL(exp_memfrag_size,
                platform_shm_find_freed_frag(data->hid, keybuf, NULL));

   // Allocate another fragment of 100 bytes, but free it to with a wrong size.
   keybuf = TYPED_ARRAY_MALLOC(data->hid, keybuf, keybuf_size);

   /*
    * NOTE: This is --NOT-- what one should do. The free machinery just trusts
    * the input fragment's size, and will free it to that free-list. It seems to
    * work now, but in future, freed-fragment will be reallocated appearing to
    * be of a larger size. But it's really of a smaller size.
    */
   size_t wrong_frag_size = 240;

   // Fragment will be "incorrectly" freed to this larger free-list size.
   size_t free_frags_wrong_list_size = wrong_frag_size;
   free_frags_wrong_list_size +=
      platform_alignment(PLATFORM_CACHELINE_SIZE, wrong_frag_size);

   memfrag_init_size(mf, keybuf, wrong_frag_size);
   /*
    * This will work, appearing as if the fragment is freed to the larger sized
    * free-list. CAUTION: This is a sure way to induce memory corruption. The
    * test case is
    *   ** cautioning ** you how to be careful with your frees.
    */
   platform_free(data->hid, mf);
   size_t freed_frag_size_as_found = 0;
   ASSERT_EQUAL(free_frags_wrong_list_size,
                platform_shm_find_freed_frag(
                   data->hid, keybuf, &freed_frag_size_as_found));
   /*
    * The wrong size specified at the time of the free will be inscribed into
    * the free fragment. This is incorrect and can lead to memory corruption
    * bugs.
    */
   ASSERT_EQUAL(wrong_frag_size, freed_frag_size_as_found);
}

/*
 * ---------------------------------------------------------------------------
 * Currently 'free' of small fragments is implemented by returning the freed
 * fragment to a free-list, by size of the fragment. Currently, we are only
 * tracking free-lists of specific sizes. Verify that after a fragment is
 * freed that the free / used counts book-keeping is done right. We should be
 * re-allocate the same freed fragment subsequently, as long as the size is
 * sufficient.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_free)
{
   int              keybuf_size = 64;
   platform_memfrag memfrag_keybuf;
   uint8           *keybuf = TYPED_ARRAY_MALLOC(data->hid, keybuf, keybuf_size);

   int              msgbuf_size = (2 * keybuf_size);
   platform_memfrag memfrag_msgbuf;
   uint8           *msgbuf = TYPED_ARRAY_MALLOC(data->hid, msgbuf, msgbuf_size);

   size_t mem_used = platform_shmused(data->hid);

   void *next_free = platform_shm_next_free_addr(data->hid);

   platform_memfrag *mf = &memfrag_keybuf;
   platform_free(data->hid, mf);

   // Even though we freed some memory, the next addr-to-allocate is unchanged.
   ASSERT_TRUE(next_free == platform_shm_next_free_addr(data->hid));

   // Space used should go down as a fragment has been freed.
   mem_used -= keybuf_size;
   ASSERT_EQUAL(mem_used, platform_shmused(data->hid));

   // The freed fragment should be re-allocated, upon re-request.
   // Note, that there is a small discrepancy creeping in here. The caller may
   // have got a larger fragment returned, but its size is not immediately known
   // to the caller. Caller will end up free'ing a fragment specifying the size
   // as its requested size. Shmem book-keeping will return this free fragment
   // to a free-list for smaller sized fragments. (Minor issue.)
   size_t smaller_size = 32; // will get rounded up to cache-linesize, 64 bytes
   uint8 *smaller_keybuf = TYPED_ARRAY_MALLOC(data->hid, keybuf, smaller_size);
   ASSERT_TRUE(keybuf == smaller_keybuf);

   // Even though we only asked for a smaller fragment, a larger free-fragemnt
   // was allocated. Check the book-keeping.
   mem_used += keybuf_size;
   ASSERT_EQUAL(mem_used, platform_shmused(data->hid));
}

/*
 * ---------------------------------------------------------------------------
 * Test allocation, free and re-allocation of a large fragment should find
 * this large fragment in the local tracker. That previously allocated
 * fragment should be re-allocated. "Next-free-ptr" should, therefore, remain
 * unchanged.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_alloc_free_and_reuse_of_large_fragment)
{
   void *next_free = platform_shm_next_free_addr(data->hid);

   // Large fragments are tracked if their size >= this size.
   size_t           size = (1 * MiB);
   platform_memfrag memfrag_keybuf;
   uint8           *keybuf = TYPED_ARRAY_MALLOC(data->hid, keybuf, size);

   // Validate that a new large fragment will create a new allocation.
   ASSERT_TRUE((void *)keybuf == next_free);

   // Re-establish next-free-ptr after this large allocation. We will use it
   // below to assert that this location will not change when we re-use this
   // large fragment for re-allocation after it's been freed.
   next_free = platform_shm_next_free_addr(data->hid);

   // Save this off ...
   uint8 *keybuf_old = keybuf;

   // If you free this fragment and re-allocate exactly the same size,
   // it should recycle the freed fragment.
   platform_memfrag *mf = &memfrag_keybuf;
   platform_free(data->hid, mf);

   platform_memfrag memfrag_keybuf_new;
   uint8 *keybuf_new = TYPED_ARRAY_MALLOC(data->hid, keybuf_new, size);
   ASSERT_TRUE((keybuf_old == keybuf_new),
               "keybuf_old=%p, keybuf_new=%p\n",
               keybuf_old,
               keybuf_new);

   // We have re-used freed fragment, so the next-free-ptr should be unchanged.
   ASSERT_TRUE(next_free == platform_shm_next_free_addr(data->hid));

   mf = &memfrag_keybuf_new;
   platform_free(data->hid, mf);
}

/*
 * ---------------------------------------------------------------------------
 * Test that free followed by a request of the same size (of a large fragment)
 * will re-allocate the recently-freed large fragment, avoiding any existing
 * in-use large fragments of the same size.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_free_reuse_around_inuse_large_fragments)
{
   void *next_free = platform_shm_next_free_addr(data->hid);

   // Large fragments are tracked if their size >= this size.
   size_t           size = (1 * MiB);
   platform_memfrag memfrag_keybuf1_1MiB;
   uint8 *keybuf1_1MiB = TYPED_ARRAY_MALLOC(data->hid, keybuf1_1MiB, size);

   // Throw-in allocation for some random struct, to ensure that these large
   // fragments are not contiguous
   thread_config *filler_cfg1 = TYPED_MALLOC(data->hid, filler_cfg1);

   platform_memfrag memfrag_keybuf2_1MiB;
   uint8 *keybuf2_1MiB = TYPED_ARRAY_MALLOC(data->hid, keybuf2_1MiB, size);

   thread_config *filler_cfg2 = TYPED_MALLOC(data->hid, filler_cfg2);

   platform_memfrag memfrag_keybuf3_1MiB;
   uint8 *keybuf3_1MiB = TYPED_ARRAY_MALLOC(data->hid, keybuf3_1MiB, size);

   thread_config *filler_cfg3 = TYPED_MALLOC(data->hid, filler_cfg3);

   // Re-establish next-free-ptr after this large allocation. We will use it
   // below to assert that this location will not change when we re-use a
   // large fragment for re-allocation after it's been freed.
   next_free = platform_shm_next_free_addr(data->hid);

   // Save off fragment handles as free will NULL out ptr.
   uint8 *old_keybuf2_1MiB = keybuf2_1MiB;

   // Free the middle fragment that should get reused, below.
   platform_memfrag  memfrag = {.addr = keybuf2_1MiB, .size = size};
   platform_memfrag *mf      = &memfrag;
   platform_free(data->hid, mf);

   // Re-request (new) fragments of the same size.
   keybuf2_1MiB = TYPED_ARRAY_MALLOC(data->hid, keybuf2_1MiB, size);
   ASSERT_TRUE((keybuf2_1MiB == old_keybuf2_1MiB),
               "Expected to satisfy new 1MiB request at %p"
               " with old 1MiB fragment ptr at %p\n",
               keybuf2_1MiB,
               old_keybuf2_1MiB);

   ASSERT_TRUE(next_free == platform_shm_next_free_addr(data->hid));

   // As large-fragments allocated / freed are tracked in an array, verify
   // that we will find the 1st one upon a re-request after a free.
   uint8 *old_keybuf1_1MiB = keybuf1_1MiB;
   mf                      = &memfrag_keybuf1_1MiB;
   platform_free(data->hid, mf);

   mf = &memfrag_keybuf2_1MiB;
   platform_free(data->hid, mf);

   // This re-request should re-allocate the 1st free fragment found.
   keybuf2_1MiB = TYPED_ARRAY_MALLOC(data->hid, keybuf2_1MiB, size);
   ASSERT_TRUE((keybuf2_1MiB == old_keybuf1_1MiB),
               "Expected to satisfy new 1MiB request at %p"
               " with old 1MiB fragment ptr at %p\n",
               keybuf2_1MiB,
               old_keybuf1_1MiB);

   mf = &memfrag_keybuf2_1MiB;
   platform_free(data->hid, mf);

   mf = &memfrag_keybuf3_1MiB;
   platform_free(data->hid, mf);

   // Memory fragments of typed objects can be freed directly.
   platform_free(data->hid, filler_cfg1);
   platform_free(data->hid, filler_cfg2);
   platform_free(data->hid, filler_cfg3);
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
CTEST2(splinter_shmem, test_reuse_of_free_fragments_uses_first_fit)
{
   void *next_free = platform_shm_next_free_addr(data->hid);

   // Large fragments are tracked if their size >= this size.
   size_t           size = (1 * MiB);
   platform_memfrag memfrag_keybuf_1MiB;
   uint8 *keybuf_1MiB = TYPED_ARRAY_MALLOC(data->hid, keybuf_1MiB, size);

   size = (5 * MiB);
   platform_memfrag memfrag_keybuf_5MiB;
   uint8 *keybuf_5MiB = TYPED_ARRAY_MALLOC(data->hid, keybuf_5MiB, size);

   size = (2 * MiB);
   platform_memfrag memfrag_keybuf_2MiB;
   uint8 *keybuf_2MiB = TYPED_ARRAY_MALLOC(data->hid, keybuf_2MiB, size);

   // Re-establish next-free-ptr after this large allocation. We will use it
   // below to assert that this location will not change when we re-use a
   // large fragment for re-allocation after it's been freed.
   next_free = platform_shm_next_free_addr(data->hid);

   // Save off fragment handles as free will NULL out ptr.
   uint8 *old_keybuf_1MiB = keybuf_1MiB;
   uint8 *old_keybuf_5MiB = keybuf_5MiB;
   uint8 *old_keybuf_2MiB = keybuf_2MiB;

   platform_memfrag *mf = NULL;

   // Order in which we free these fragments does not matter.
   mf = &memfrag_keybuf_1MiB;
   platform_free(data->hid, mf);

   mf = &memfrag_keybuf_2MiB;
   platform_free(data->hid, mf);

   mf = &memfrag_keybuf_5MiB;
   platform_free(data->hid, mf);

   // Re-request (new) fragments in diff size order.
   size        = (2 * MiB);
   keybuf_2MiB = TYPED_ARRAY_MALLOC(data->hid, keybuf_2MiB, size);
   ASSERT_TRUE((keybuf_2MiB == old_keybuf_5MiB),
               "Expected to satisfy new 2MiB request at %p"
               " with old 5MiB fragment ptr at %p\n",
               keybuf_2MiB,
               old_keybuf_5MiB);
   ;

   // We have re-used freed 5MiB fragment; so next-free-ptr should be unchanged.
   ASSERT_TRUE(next_free == platform_shm_next_free_addr(data->hid));

   size        = (5 * MiB);
   keybuf_5MiB = TYPED_ARRAY_MALLOC(data->hid, keybuf_5MiB, size);

   // We allocated a new fragment at next-free-ptr
   ASSERT_TRUE(keybuf_5MiB != old_keybuf_1MiB);
   ASSERT_TRUE(keybuf_5MiB != old_keybuf_2MiB);
   ASSERT_TRUE(keybuf_5MiB == next_free);

   mf = &memfrag_keybuf_2MiB;
   platform_free(data->hid, mf);

   mf = &memfrag_keybuf_5MiB;
   platform_free(data->hid, mf);
}

/*
 * ---------------------------------------------------------------------------
 * Test case to verify that configuration checks that shared segment size
 * is "big enough" to allocate memory for RC-allocator cache's lookup
 * array. For very large devices, with insufficiently sized shared memory
 * config, we will not be able to boot-up.
 * RESOLVE - This test case is still incomplete.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_large_dev_with_small_shmem_error_handling)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_cfg;

   platform_disable_tracing_shm_ops();

   ZERO_STRUCT(cfg);
   ZERO_STRUCT(default_data_cfg);

   default_data_config_init(TEST_MAX_KEY_SIZE, &default_data_cfg);
   setup_cfg_for_test(&cfg, &default_data_cfg);

   int rc = splinterdb_create(&cfg, &kvsb);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_close(&kvsb);
   ASSERT_EQUAL(0, rc);

   platform_enable_tracing_shm_ops();
}

/*
 * ---------------------------------------------------------------------------
 * Basic test to verify that memory is correctly freed through realloc()
 * interface. Here, memory for an oldptr is expanded thru
 * platform_shm_realloc(). This is similar to C-realloc() API. Verify that
 * memory fragments are correctly freed. And that used / free space accounting
 * is done properly.
 *
 * Verify that for a proper sequence of alloc / realloc / free operations, the
 * used / free space metrics are correctly restored to their initial state.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_small_frag_platform_realloc)
{
   size_t shmused_initial = platform_shmused(data->hid);
   size_t shmfree_initial = platform_shmfree(data->hid);

   // As memory allocated is rounded-up to PLATFORM_CACHELINE_SIZE, ask for
   // memory just short of a cache line size. This way, when we double our
   // request, for realloc, we will necessarily go thru realloc(), rather than
   // re-using this padded memory fragment.
   size_t oldsize = (PLATFORM_CACHELINE_SIZE - 2 * sizeof(void *));

   platform_memfrag memfrag_oldptr;

   char *oldptr = TYPED_ARRAY_MALLOC(data->hid, oldptr, oldsize);

   size_t old_shmfree = platform_shmfree(data->hid);
   size_t old_shmused = platform_shmused(data->hid);

   // Free-memory should have gone down by size of memfrag allocated
   ASSERT_EQUAL(old_shmfree, (shmfree_initial - memfrag_size(&memfrag_oldptr)));

   // Used-memory should have gone up by size of memfrag allocated
   ASSERT_EQUAL(old_shmused, (shmused_initial + memfrag_size(&memfrag_oldptr)));

   size_t newsize          = (2 * oldsize);
   size_t adjusted_newsize = newsize;

   // realloc interface is a bit tricky, due to our cache-alignment based
   // memory fragment management. Even though user requested 'oldsize'
   // initially, at realloc-time, you have to supply the actual memory fragment
   // size. This will ensure correct free space accounting, and will prevent
   // 'leaking' memory. E.g., here we allocated oldsize=56 bytes. When realloc()
   // frees this fragment, if you supply 'oldsize', it will record the
   // free-fragment's size, incorrectly, as 56 bytes.
   // clang-format off
   char *newptr = platform_realloc(data->hid,
                                   memfrag_size(&memfrag_oldptr),
                                   oldptr,
                                   &adjusted_newsize);
   // clang-format on
   ASSERT_TRUE(newptr != oldptr);

   // Expect that newsize was padded up for cacheline alignment
   ASSERT_TRUE(adjusted_newsize > newsize);

   // realloc() (is expected to) pad-up newsize to cache-line alignment
   ASSERT_TRUE(platform_shm_next_free_cacheline_aligned(data->hid));

   // Check free space accounting
   newsize            = adjusted_newsize;
   size_t new_shmfree = platform_shmfree(data->hid);
   size_t exp_shmfree = (old_shmfree + memfrag_size(&memfrag_oldptr) - newsize);
   ASSERT_TRUE((exp_shmfree == new_shmfree),
               "Expected free space=%lu bytes != actual free space=%lu bytes"
               ", diff=%lu bytes. ",
               exp_shmfree,
               new_shmfree,
               diff_size_t(exp_shmfree, new_shmfree));

   // Check used space accounting after realloc()
   size_t new_shmused = platform_shmused(data->hid);
   size_t exp_shmused = (old_shmused - memfrag_size(&memfrag_oldptr) + newsize);
   ASSERT_TRUE((exp_shmused == new_shmused),
               "Expected used space=%lu bytes != actual used space=%lu bytes"
               ", diff=%lu bytes. ",
               exp_shmused,
               new_shmused,
               diff_size_t(exp_shmused, new_shmused));

   // We should be able to re-cycle the memory used by oldptr before realloc()
   // for another memory fragment of the same size
   platform_memfrag memfrag_nextptr;
   char            *nextptr = TYPED_ARRAY_MALLOC(data->hid, nextptr, oldsize);
   ASSERT_TRUE(nextptr == oldptr);

   platform_memfrag memfrag_anotherptr;
   char *anotherptr = TYPED_ARRAY_MALLOC(data->hid, anotherptr, (10 * oldsize));
   ASSERT_TRUE(anotherptr != oldptr);
   ASSERT_TRUE(anotherptr != nextptr);

   platform_memfrag *mf = &memfrag_anotherptr;
   platform_free(data->hid, mf);

   mf = &memfrag_nextptr;
   platform_free(data->hid, mf);

   // Here's the trick in book-keeping. As oldptr was realloc()'ed, its size
   // went up from what was tracked in its memfrag_oldptr to adjusted_newsize.
   // So, to correctly get free space accounting, and to not 'leak' memory, we
   // need to re-establish the fragment's correct identity before freeing it.
   mf = &memfrag_oldptr;
   memfrag_init_size(mf, newptr, newsize);
   platform_free(data->hid, mf);

   // Confirm that free/used space metrics go back to initial values
   new_shmused = platform_shmused(data->hid);
   new_shmfree = platform_shmfree(data->hid);

   ASSERT_EQUAL(shmused_initial,
                new_shmused,
                "shmused_initial=%lu != new_shmused=%lu, diff=%lu. ",
                shmused_initial,
                new_shmused,
                diff_size_t(new_shmused, shmused_initial));

   ASSERT_EQUAL(shmfree_initial,
                new_shmfree,
                "shmfree_initial=%lu != new_shmfree=%lu, diff=%lu. ",
                shmfree_initial,
                new_shmfree,
                diff_size_t(shmfree_initial, new_shmfree));
}

/*
 * ---------------------------------------------------------------------------
 * Exercise realloc() of a small-fragment to a large-fragment.
 *
 * Verify that:
 *  - We round-up to cacheline alignment even for large fragment requests
 *  - For a proper sequence of alloc / realloc / free operations, the
 *    used / free space metrics are correctly restored to their initial state.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_small_frag_platform_realloc_to_large_frag)
{
   size_t shmused_initial = platform_shmused(data->hid);
   size_t shmfree_initial = platform_shmfree(data->hid);

   // Allocate a small fragment here
   size_t oldsize = ((2 * PLATFORM_CACHELINE_SIZE) - 2 * sizeof(void *));
   platform_memfrag memfrag_oldptr;
   char            *oldptr = TYPED_ARRAY_MALLOC(data->hid, oldptr, oldsize);

   size_t old_shmfree = platform_shmfree(data->hid);
   size_t old_shmused = platform_shmused(data->hid);

   size_t old_memfrag_size = memfrag_size(&memfrag_oldptr);
   // Free-memory should have gone down by size of memfrag allocated
   ASSERT_EQUAL(old_shmfree, (shmfree_initial - old_memfrag_size));

   // Used-memory should have gone up by size of memfrag allocated
   ASSERT_EQUAL(old_shmused, (shmused_initial + old_memfrag_size));

   // Request a very large fragment, just shy of the alignment size.
   size_t newsize           = (2 * SHM_LARGE_FRAG_SIZE) - 16;
   size_t adjusted_newsize  = newsize;
   size_t expected_newsisze = newsize;
   expected_newsisze +=
      platform_alignment(PLATFORM_CACHELINE_SIZE, expected_newsisze);

   // realloc interface is a bit tricky, due to our cache-alignment based
   // memory fragment management. Even though user requested 'oldsize'
   // initially, at realloc-time, you have to supply the memory fragment's
   // actual size for 'oldsize'. This will ensure correct free space
   // accounting, and will prevent 'leaking' memory.
   // clang-format off
   char *newptr = platform_realloc(data->hid,
                                   memfrag_size(&memfrag_oldptr),
                                   oldptr,
                                   &adjusted_newsize);
   // clang-format on
   ASSERT_TRUE(newptr != oldptr);

   // Expect realloc() to have aligned to cache-line size
   ASSERT_EQUAL(expected_newsisze,
                adjusted_newsize,
                "expected_newsisze=%lu, adjusted_newsize=%lu",
                expected_newsisze,
                adjusted_newsize);

   // Check free space accounting
   size_t new_shmfree = platform_shmfree(data->hid);
   size_t exp_shmfree =
      (old_shmfree + memfrag_size(&memfrag_oldptr) - adjusted_newsize);
   ASSERT_TRUE((exp_shmfree == new_shmfree),
               "Expected free space=%lu bytes != actual free space=%lu bytes"
               ", diff=%lu bytes. ",
               exp_shmfree,
               new_shmfree,
               diff_size_t(exp_shmfree, new_shmfree));

   // Check used space accounting after realloc() allocated a new large fragment
   size_t new_shmused = platform_shmused(data->hid);
   size_t exp_shmused =
      (old_shmused - memfrag_size(&memfrag_oldptr) + adjusted_newsize);
   ASSERT_TRUE((exp_shmused == new_shmused),
               "Expected used space=%lu bytes != actual used space=%lu bytes"
               ", diff=%lu bytes. ",
               exp_shmused,
               new_shmused,
               diff_size_t(exp_shmused, new_shmused));

   platform_memfrag *mf = &memfrag_oldptr;
   memfrag_init_size(mf, newptr, expected_newsisze);
   platform_free(data->hid, mf);

   // When large fragments are 'freed', they are not really accounted in the
   // used/free bytes metrics. This is because, these large-fragments are
   // already 'used', waiting to be re-cycled to the new request.
   // Confirm that free/used space metrics go back to expected values.
   new_shmused = platform_shmused(data->hid);
   new_shmfree = platform_shmfree(data->hid);

   ASSERT_EQUAL(exp_shmfree,
                new_shmfree,
                "exp_shmfree=%lu != new_shmfree=%lu, diff=%lu. ",
                exp_shmfree,
                new_shmfree,
                diff_size_t(exp_shmfree, new_shmfree));

   ASSERT_EQUAL(exp_shmused,
                new_shmused,
                "exp_shmused=%lu != new_shmused=%lu, diff=%lu. ",
                exp_shmused,
                new_shmused,
                diff_size_t(exp_shmused, shmused_initial));
}

/*
 * ---------------------------------------------------------------------------
 * Exercise realloc() of a large-fragment to another large-fragment.
 *
 * Verify that:
 *  - We round-up to cacheline alignment even for large fragment requests
 *  - For a proper sequence of alloc / realloc / free operations, the
 *    used / free space metrics are correctly restored to their initial state.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_large_frag_platform_realloc_to_large_frag)
{
   size_t shmused_initial = platform_shmused(data->hid);
   size_t shmfree_initial = platform_shmfree(data->hid);

   // Allocate a small fragment here
   size_t           oldsize = (2 * SHM_LARGE_FRAG_SIZE);
   platform_memfrag memfrag_oldptr;
   char            *oldptr = TYPED_ARRAY_MALLOC(data->hid, oldptr, oldsize);

   size_t old_shmfree = platform_shmfree(data->hid);
   size_t old_shmused = platform_shmused(data->hid);

   size_t old_memfrag_size = memfrag_size(&memfrag_oldptr);
   // Free-memory should have gone down by size of memfrag allocated
   ASSERT_EQUAL(old_shmfree, (shmfree_initial - old_memfrag_size));

   // Used-memory should have gone up by size of memfrag allocated
   ASSERT_EQUAL(old_shmused, (shmused_initial + old_memfrag_size));

   // Request a larger fragment. (Alighnment issues covered earlier ...)
   size_t newsize           = (4 * SHM_LARGE_FRAG_SIZE) - 20;
   size_t adjusted_newsize  = newsize;
   size_t expected_newsisze = newsize;
   expected_newsisze +=
      platform_alignment(PLATFORM_CACHELINE_SIZE, expected_newsisze);

   // realloc interface is a bit tricky, due to our cache-alignment based
   // memory fragment management. Even though user requested 'oldsize'
   // initially, at realloc-time, you have to supply the memory fragment's
   // actual size for 'oldsize'. This will ensure correct free space
   // accounting, and will prevent 'leaking' memory.
   // clang-format off
   char *newptr = platform_realloc(data->hid,
                                   memfrag_size(&memfrag_oldptr),
                                   oldptr,
                                   &adjusted_newsize);
   // clang-format on
   ASSERT_TRUE(newptr != oldptr);

   // Expect realloc() to have aligned to cache-line size
   ASSERT_EQUAL(expected_newsisze,
                adjusted_newsize,
                "expected_newsisze=%lu, adjusted_newsize=%lu",
                expected_newsisze,
                adjusted_newsize);

   // Check free space accounting. Memory used by old large-fragment being
   // freed is not accounted in shared memory's memory metrics
   size_t new_shmfree = platform_shmfree(data->hid);
   size_t exp_shmfree = (old_shmfree - adjusted_newsize);
   ASSERT_TRUE((exp_shmfree == new_shmfree),
               "Expected free space=%lu bytes != actual free space=%lu bytes"
               ", diff=%lu bytes. ",
               exp_shmfree,
               new_shmfree,
               diff_size_t(exp_shmfree, new_shmfree));

   // Check used space accounting after realloc() allocated a new large fragment
   size_t new_shmused = platform_shmused(data->hid);
   size_t exp_shmused = (old_shmused + adjusted_newsize);
   ASSERT_TRUE((exp_shmused == new_shmused),
               "Expected used space=%lu bytes != actual used space=%lu bytes"
               ", diff=%lu bytes. ",
               exp_shmused,
               new_shmused,
               diff_size_t(exp_shmused, new_shmused));

   platform_memfrag *mf = &memfrag_oldptr;

   // You -must- specify the right size when free'ing even a large fragment.
   // Otherwise, debug asserts will trip.
   memfrag_init_size(mf, newptr, adjusted_newsize);
   platform_free(data->hid, mf);
   return;

   // When large fragments are 'freed', they are not really accounted in the
   // used/free bytes metrics. This is because, these large-fragments are
   // already 'used', waiting to be re-cycled to the new request.
   // Confirm that free/used space metrics go back to expected values.
   new_shmused = platform_shmused(data->hid);
   new_shmfree = platform_shmfree(data->hid);

   ASSERT_EQUAL(exp_shmfree,
                new_shmfree,
                "exp_shmfree=%lu != new_shmfree=%lu, diff=%lu. ",
                exp_shmfree,
                new_shmfree,
                diff_size_t(exp_shmfree, new_shmfree));

   ASSERT_EQUAL(exp_shmused,
                new_shmused,
                "exp_shmused=%lu != new_shmused=%lu, diff=%lu. ",
                exp_shmused,
                new_shmused,
                diff_size_t(exp_shmused, shmused_initial));
}

/*
 * ---------------------------------------------------------------------------
 * Writable buffer interface tests: Which exercise shared memory related APIs.
 * ---------------------------------------------------------------------------
 */
/*
 * Resizing of writable buffers goes through realloc(). Cross-check that memory
 * metrics are correctly done through this code-flow.
 */
CTEST2(splinter_shmem, test_writable_buffer_resize_empty_buffer)
{
   size_t shmused_initial = platform_shmused(data->hid);
   size_t shmfree_initial = platform_shmfree(data->hid);

   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   writable_buffer_init(wb, data->hid);
   uint64 new_length = 20;
   writable_buffer_resize(wb, 0, new_length);

   ASSERT_EQUAL(new_length, writable_buffer_length(wb));

   // We should have done some memory allocation.
   ASSERT_TRUE(wb->can_free);
   ASSERT_NOT_NULL(writable_buffer_data(wb));
   writable_buffer_deinit(wb);

   // Confirm that free/used space metrics go back to initial values
   size_t new_shmused = platform_shmused(data->hid);
   size_t new_shmfree = platform_shmfree(data->hid);

   ASSERT_EQUAL(shmused_initial,
                new_shmused,
                "shmused_initial=%lu != new_shmused=%lu, diff=%lu. ",
                shmused_initial,
                new_shmused,
                (new_shmused - shmused_initial));

   ASSERT_EQUAL(shmfree_initial,
                new_shmfree,
                "shmfree_initial=%lu != new_shmfree=%lu, diff=%lu. ",
                shmfree_initial,
                new_shmfree,
                (shmfree_initial - new_shmfree));
}

/*
 * ---------------------------------------------------------------------------
 * Test resizing of writable buffers that initially had an on-stack buffer.
 * Resizing goes through realloc(). Cross-check that memory metrics are
 * correctly done through this code-flow.
 */
CTEST2(splinter_shmem, test_writable_buffer_resize_onstack_buffer)
{
   size_t shmused_initial = platform_shmused(data->hid);
   size_t shmfree_initial = platform_shmfree(data->hid);

   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   char buf[WB_ONSTACK_BUFSIZE];
   writable_buffer_init_with_buffer(
      wb, data->hid, sizeof(buf), (void *)buf, WRITABLE_BUFFER_NULL_LENGTH);

   size_t new_length = (10 * sizeof(buf));
   writable_buffer_resize(wb, sizeof(buf), new_length);

   ASSERT_EQUAL(new_length, writable_buffer_length(wb));

   // We should have done some memory allocation.
   ASSERT_TRUE(wb->can_free);

   void *dataptr = writable_buffer_data(wb);
   ASSERT_NOT_NULL(dataptr);
   ASSERT_TRUE((void *)buf != dataptr);

   writable_buffer_deinit(wb);

   // Confirm that free/used space metrics go back to initial values
   size_t new_shmused = platform_shmused(data->hid);
   size_t new_shmfree = platform_shmfree(data->hid);

   ASSERT_EQUAL(shmused_initial,
                new_shmused,
                "shmused_initial=%lu != new_shmused=%lu, diff=%lu. ",
                shmused_initial,
                new_shmused,
                (new_shmused - shmused_initial));

   ASSERT_EQUAL(shmfree_initial,
                new_shmfree,
                "shmfree_initial=%lu != new_shmfree=%lu, diff=%lu. ",
                shmfree_initial,
                new_shmfree,
                (shmfree_initial - new_shmfree));
}

/*
 * ---------------------------------------------------------------------------
 * Test resizing of writable buffers that go through 'append' interface
 * correctly manage the fragment's capacity as was initially allocated from
 * shared memory. This is a test case for a small shmem-specific 'bug' in
 * writable_buffer_ensure_space() -> platform_realloc(), whereby we weren't
 * specifying the right 'oldsize' for a fragment being realloc()'ed.
 */
CTEST2(splinter_shmem, test_writable_buffer_resize_vs_capacity)
{
   size_t shmused_initial = platform_shmused(data->hid);
   size_t shmfree_initial = platform_shmfree(data->hid);

   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   writable_buffer_init(wb, data->hid);
   const char *input_str = "Hello World!";
   writable_buffer_append(wb, strlen(input_str), (const void *)input_str);

   // Min fragment allocated is of one cache line
   ASSERT_EQUAL(PLATFORM_CACHELINE_SIZE, writable_buffer_capacity(wb));

   void *data_ptr = writable_buffer_data(wb);

   // If you append another short string that fits within buffer capacity,
   // no reallocation should occur.
   input_str = "Another Hello World!";
   writable_buffer_append(wb, strlen(input_str), (const void *)input_str);

   void *new_data_ptr = writable_buffer_data(wb);
   ASSERT_TRUE(data_ptr == new_data_ptr);

   size_t old_buffer_capacity = writable_buffer_capacity(wb);

   // Now if you append a bigger chunk so that the writable buffer's capacity
   // is exceeded, it will be realloc()'ed.
   char filler[PLATFORM_CACHELINE_SIZE];
   memset(filler, 'X', sizeof(filler));
   writable_buffer_append(wb, sizeof(filler), (const void *)filler);

   // Should allocate a new memory fragment, so data ptr must change
   new_data_ptr = writable_buffer_data(wb);
   ASSERT_FALSE(data_ptr == new_data_ptr);

   size_t freed_frag_size_as_found = 0;

   // Old writable-buffer should have been freed to the free-list
   // corresponding to its capacity.
   ASSERT_EQUAL(old_buffer_capacity,
                platform_shm_find_freed_frag(
                   data->hid, data_ptr, &freed_frag_size_as_found));

   // The buffer should have been freed with its right capacity as 'size',
   // but there was a latent bug that was tripping up this assertion.
   ASSERT_EQUAL(old_buffer_capacity,
                freed_frag_size_as_found,
                "Expected free size=%lu, found free size of frag=%lu. ",
                old_buffer_capacity,
                freed_frag_size_as_found);

   writable_buffer_deinit(wb);

   // Confirm that free/used space metrics go back to initial values
   size_t new_shmused = platform_shmused(data->hid);
   size_t new_shmfree = platform_shmfree(data->hid);

   ASSERT_EQUAL(shmused_initial,
                new_shmused,
                "shmused_initial=%lu != new_shmused=%lu, diff=%lu. ",
                shmused_initial,
                new_shmused,
                (new_shmused - shmused_initial));

   ASSERT_EQUAL(shmfree_initial,
                new_shmfree,
                "shmfree_initial=%lu != new_shmfree=%lu, diff=%lu. ",
                shmfree_initial,
                new_shmfree,
                (shmfree_initial - new_shmfree));
}

/*
 * ---------------------------------------------------------------------------
 * Test cases exercising fingerprint object management.
 * ---------------------------------------------------------------------------
 */
CTEST2(splinter_shmem, test_fingerprint_basic)
{
   size_t mem_free_prev = platform_shmfree(data->hid);

   size_t  nitems = (1 * KiB);
   fp_hdr  fp;
   uint32 *fp_arr = fingerprint_init(&fp, data->hid, nitems);
   ASSERT_TRUE(fp_arr == fingerprint_start(&fp));
   ASSERT_EQUAL(nitems, fingerprint_ntuples(&fp));
   ASSERT_FALSE(fingerprint_is_empty(&fp));

   size_t mem_free_now = platform_shmfree(data->hid);
   ASSERT_TRUE(mem_free_now == (mem_free_prev - fingerprint_size(&fp)));

   fingerprint_deinit(data->hid, &fp);
   ASSERT_TRUE(fingerprint_is_empty(&fp));
   mem_free_now = platform_shmfree(data->hid);
   ASSERT_TRUE(mem_free_now == mem_free_prev);
}

/* Verify move operation */
CTEST2(splinter_shmem, test_fingerprint_move)
{
   size_t  nitems = (1 * KiB);
   fp_hdr  fp_src;
   uint32 *fp_src_arr = fingerprint_init(&fp_src, data->hid, nitems);

   size_t src_size = fingerprint_size(&fp_src);

   fp_hdr  fp_dst     = {0};
   uint32 *fp_dst_arr = fingerprint_move(&fp_dst, &fp_src);

   // Fingerprint is now owned by destination object
   ASSERT_TRUE(fp_dst_arr == fingerprint_start(&fp_dst));
   ASSERT_TRUE(fp_dst_arr == fp_src_arr);
   ASSERT_EQUAL(nitems, fingerprint_ntuples(&fp_dst));

   size_t dst_size = fingerprint_size(&fp_dst);
   ASSERT_EQUAL(src_size, dst_size);

   // Source is empty
   ASSERT_TRUE(fingerprint_is_empty(&fp_src));
   ASSERT_EQUAL(0, fingerprint_ntuples(&fp_src));
}

/* Verify sequence of alias, unalias, only then you can do a deinit on src */
CTEST2(splinter_shmem, test_fingerprint_alias_unalias_deinit)
{
   size_t  nitems = (1 * KiB);
   fp_hdr  fp_src;
   uint32 *fp_src_arr = fingerprint_init(&fp_src, data->hid, nitems);

   size_t src_size = fingerprint_size(&fp_src);

   fp_hdr  fp_dst     = {0};
   uint32 *fp_dst_arr = fingerprint_alias(&fp_dst, &fp_src);

   // Fingerprint is now owned by destination object
   ASSERT_TRUE(fp_dst_arr == fingerprint_start(&fp_dst));
   ASSERT_TRUE(fp_dst_arr == fp_src_arr);
   ASSERT_EQUAL(nitems, fingerprint_ntuples(&fp_dst));

   size_t dst_size = fingerprint_size(&fp_dst);
   ASSERT_EQUAL(src_size, dst_size);

   // Source is still not empty
   ASSERT_TRUE(!fingerprint_is_empty(&fp_src));
   ASSERT_EQUAL(nitems, fingerprint_ntuples(&fp_src));

   // You have to unalias dst from src before you can release src's memory
   fp_dst_arr = fingerprint_unalias(&fp_dst);
   ASSERT_TRUE(fp_dst_arr == NULL);

   fingerprint_deinit(data->hid, &fp_src);
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
