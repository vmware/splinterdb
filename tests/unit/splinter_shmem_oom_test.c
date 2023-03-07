// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinter_shmem_oom_test.c --
 *
 * Slightly slow-running test that will induce OOM in shared memory.
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

// Test these many threads concurrently performing memory allocation.
#define TEST_MAX_THREADS 8

// Size of an on-stack buffer used for testing
#define WB_ONSTACK_BUFSIZE 30

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
CTEST_DATA(splinter_shmem_oom)
{
   // Declare heap handles to shake out shared memory based allocation.
   size_t               shmem_capacity; // In bytes
   platform_heap_handle hh;
   platform_heap_id     hid;
};

// By default, all test cases will deal with small shared memory segment.
CTEST_SETUP(splinter_shmem_oom)
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
CTEST_TEARDOWN(splinter_shmem_oom)
{
   platform_status rc = platform_heap_destroy(&data->hh);
   ASSERT_TRUE(SUCCESS(rc));
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
CTEST2(splinter_shmem_oom, test_allocations_causing_OOMs)
{
   int keybuf_size = 64;

   // Self-documenting assertion ... to future-proof this area.
   ASSERT_EQUAL(keybuf_size, PLATFORM_CACHELINE_SIZE);

   void            *next_free = platform_shm_next_free_addr(data->hid);
   platform_memfrag memfrag_keybuf;
   uint8           *keybuf = TYPED_ARRAY_MALLOC(data->hid, keybuf, keybuf_size);

   // Validate returned memory-ptr, knowing that no pad bytes were needed.
   ASSERT_TRUE((void *)keybuf == next_free);

   next_free = platform_shm_next_free_addr(data->hid);

   size_t space_left =
      (data->shmem_capacity - (keybuf_size + platform_shm_ctrlblock_size()));

   ASSERT_EQUAL(space_left, platform_shmfree(data->hid));

   platform_error_log("\nNOTE: Test case intentionally triggers out-of-space"
                      " errors in shared segment. 'Insufficient memory'"
                      " error messages below are to be expected.\n");

   // Note that although we have asked for 1 more byte than free space available
   // the allocation interfaces round-up the # bytes for alignment. So the
   // requested # of bytes will be a bit larger than free space in the error
   // message you will see below.
   keybuf_size = (space_left + 1);
   uint8 *keybuf_oom =
      TYPED_ARRAY_MALLOC_MF(data->hid, keybuf_oom, keybuf_size, NULL);
   ASSERT_TRUE(keybuf_oom == NULL);

   // Free space counter is not touched if allocation fails.
   ASSERT_EQUAL(space_left, platform_shmfree(data->hid));

   // As every memory request is rounded-up for alignment, the space left
   // counter should always be an integral multiple of this constant.
   ASSERT_EQUAL(0, (space_left % PLATFORM_CACHELINE_SIZE));

   // If we request exactly what's available, it should succeed.
   keybuf_size = space_left;
   platform_memfrag memfrag_keybuf_no_oom;
   uint8           *keybuf_no_oom =
      TYPED_ARRAY_MALLOC(data->hid, keybuf_no_oom, keybuf_size);
   ASSERT_TRUE(keybuf_no_oom != NULL);

   CTEST_LOG_INFO("Successfully allocated all remaining %lu bytes "
                  "from shared segment.\n",
                  space_left);

   // We should be out of space by now.
   ASSERT_EQUAL(0, platform_shmfree(data->hid));

   // This should fail.
   keybuf_size = 1;
   keybuf_oom = TYPED_ARRAY_MALLOC_MF(data->hid, keybuf_oom, keybuf_size, NULL);
   ASSERT_TRUE(keybuf_oom == NULL);

   // Free allocated memory before exiting.
   platform_memfrag *mf = &memfrag_keybuf;
   platform_free(data->hid, mf);

   mf = &memfrag_keybuf_no_oom;
   platform_free(data->hid, mf);
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
CTEST2(splinter_shmem_oom, test_concurrent_allocs_by_n_threads)
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

   rv = splinterdb_close(&kvsb);
   ASSERT_EQUAL(0, rv);

   platform_enable_tracing_shm_ops();
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
