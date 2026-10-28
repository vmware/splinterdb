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
#include <stdlib.h>
#include <unistd.h>

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
#include "platform_io.h"

#if defined(__has_feature)
#   if __has_feature(memory_sanitizer)
#      include <sanitizer/msan_interface.h>
#      define PLATFORM_API_TEST_MSAN 1
#   endif
#endif
#ifndef PLATFORM_API_TEST_MSAN
#   define PLATFORM_API_TEST_MSAN 0
#endif

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
   platform_register_thread();
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

/* Exercise both storage capacities with real vectored I/O. The canaries catch
 * an append using the extent limit when the caller supplied a page buffer. */
static void
test_async_io_capacity(platform_heap_id hid, bool32 extent)
{
   char filename[] = "/tmp/splinterdb-async-capacity-XXXXXX";
   int  fd         = mkstemp(filename);
   ASSERT_TRUE(fd >= 0);
   ASSERT_EQUAL(0, close(fd));
   io_config cfg;
   io_config_init(&cfg,
                  IO_DEFAULT_PAGE_SIZE,
                  IO_DEFAULT_EXTENT_SIZE,
                  IO_DEFAULT_FLAGS,
                  IO_DEFAULT_PERMS,
                  IO_DEFAULT_KERNEL_QUEUE_SIZE,
                  filename);
   io_handle *io = io_handle_create(&cfg, hid);
   ASSERT_NOT_NULL(io);
   ASSERT_EQUAL(0, unlink(filename));

   struct {
      char                       prefix;
      io_async_state_page_buffer state;
      unsigned char              canary[32];
   } page;
   struct {
      char                         prefix;
      io_async_state_extent_buffer state;
      unsigned char                canary[32];
   } full;
   memset(page.canary, 0xa5, sizeof(page.canary));
   memset(full.canary, 0xa5, sizeof(full.canary));
   void          *state  = extent ? (void *)full.state : (void *)page.state;
   unsigned char *canary = extent ? full.canary : page.canary;
   const uint64   pages  = extent ? IO_DEFAULT_PAGES_PER_EXTENT : 1;
   const uint64   bytes  = pages * IO_DEFAULT_PAGE_SIZE;
   buffer_handle  buffer;
   ASSERT_TRUE(SUCCESS(platform_buffer_init(&buffer, bytes)));
   char *buf = platform_buffer_getaddr(&buffer);

   /* An invalid capacity must fail before writing even the backend header. */
   memset(state, 0xa5, extent ? sizeof(full.state) : sizeof(page.state));
   ASSERT_TRUE(STATUS_IS_EQ(
      io->ops->async_state_init(state, 0, io, io_async_preadv, 0, NULL, NULL),
      STATUS_BAD_PARAM));
   ASSERT_EQUAL(0xa5, ((unsigned char *)state)[0]);

   for (unsigned reading = 0; reading < 2; reading++) {
      io_async_cmd    cmd = reading ? io_async_preadv : io_async_pwritev;
      platform_status rc =
         extent ? io_async_state_init_extent(full.state, io, cmd, 0, NULL, NULL)
                : io_async_state_init_page(page.state, io, cmd, 0, NULL, NULL);
      ASSERT_TRUE(SUCCESS(rc));
      for (uint64 i = 0; i < pages; i++) {
         memset(buf + i * IO_DEFAULT_PAGE_SIZE,
                reading ? 0 : (int)(i + 1),
                IO_DEFAULT_PAGE_SIZE);
         ASSERT_TRUE(SUCCESS(
            io_async_state_append_page(state, buf + i * IO_DEFAULT_PAGE_SIZE)));
      }
      ASSERT_TRUE(STATUS_IS_EQ(io_async_state_append_page(state, buf),
                               STATUS_LIMIT_EXCEEDED));
      uint64 count;
      io_async_state_get_iovec(state, &count);
      ASSERT_EQUAL(pages, count);
      io_async_run(state);
      io_wait_all(io);
      ASSERT_EQUAL(ASYNC_STATUS_DONE, io_async_run(state));
      ASSERT_TRUE(SUCCESS(io_async_state_get_result(state)));
      io_async_state_deinit(state);
      for (unsigned i = 0; i < 32; i++) {
         ASSERT_EQUAL(0xa5, canary[i]);
      }
   }
   for (uint64 i = 0; i < bytes; i++) {
      ASSERT_EQUAL(i / IO_DEFAULT_PAGE_SIZE + 1, (unsigned char)buf[i]);
   }
   ASSERT_TRUE(SUCCESS(platform_buffer_deinit(&buffer)));
   io_handle_destroy(io);
}

CTEST2(platform_api, test_async_page_io_capacity)
{
   test_async_io_capacity(data->hid, FALSE);
}

CTEST2(platform_api, test_async_extent_io_capacity)
{
   test_async_io_capacity(data->hid, TRUE);
}

typedef struct async_read_shadow_probe {
   io_async_state_extent_buffer state;
   char                        *buffer;
   uint64                       length;
   uint64                       initialized_bytes;
   bool32                       shadow_ok;
   bool32                       data_ok;
   uint64                       callbacks;
} async_read_shadow_probe;

/* Inspect shadow before touching the data so the unfixed implementation
 * produces an ordinary assertion failure after resources have been released.
 * In particular, inspect from the completion callback, not just after wait. */
static void
async_read_shadow_check(async_read_shadow_probe *probe)
{
   probe->shadow_ok = TRUE;
#if PLATFORM_API_TEST_MSAN
   if (probe->initialized_bytes != 0) {
      probe->shadow_ok =
         __msan_test_shadow(probe->buffer, probe->initialized_bytes) == -1;
   }
   for (uint64 i = probe->initialized_bytes; i < probe->length; i++) {
      if (__msan_test_shadow(probe->buffer + i, 1) != 0) {
         probe->shadow_ok = FALSE;
      }
   }
#endif
   probe->data_ok = probe->shadow_ok;
   if (probe->shadow_ok) {
      for (uint64 i = 0; i < probe->initialized_bytes; i++) {
         if (probe->buffer[i] != 0x5a) {
            probe->data_ok = FALSE;
         }
      }
   }
}

static void
async_read_shadow_callback(void *arg)
{
   async_read_shadow_probe *probe = arg;
   if (io_async_run(probe->state) != ASYNC_STATUS_DONE) {
      return;
   }
   async_read_shadow_check(probe);
   probe->callbacks++;
}

static void
test_async_io_shadow(platform_heap_id hid,
                     io_async_cmd     cmd,
                     uint64           file_bytes,
                     uint64           addr,
                     uint64           initialized_bytes,
                     bool32           expect_success,
                     bool32           allow_submit_failure)
{
   char filename[] = "/tmp/splinterdb-async-shadow-XXXXXX";
   int  fd         = mkstemp(filename);
   ASSERT_TRUE(fd >= 0);
   io_config cfg;
   io_config_init(&cfg,
                  IO_DEFAULT_PAGE_SIZE,
                  IO_DEFAULT_EXTENT_SIZE,
                  O_RDWR,
                  IO_DEFAULT_PERMS,
                  IO_DEFAULT_KERNEL_QUEUE_SIZE,
                  filename);
   io_handle *io = io_handle_create(&cfg, hid);
   ASSERT_NOT_NULL(io);
   ASSERT_EQUAL(0, unlink(filename));

   const uint64  page_size = IO_DEFAULT_PAGE_SIZE;
   buffer_handle buffer;
   ASSERT_TRUE(SUCCESS(platform_buffer_init(&buffer, 3 * page_size)));
   char *bytes = platform_buffer_getaddr(&buffer);
   memset(bytes, 0x5a, 3 * page_size);
   ASSERT_TRUE(SUCCESS(io_write(io, bytes, 3 * page_size, 0)));
   /* Buffered I/O permits EOF partway through an iovec. Creation may extend
    * the file for a superblock, so establish this length after opening it. */
   ASSERT_EQUAL(0, ftruncate(fd, file_bytes));
   ASSERT_EQUAL(0, close(fd));

   async_read_shadow_probe probe = {
      .buffer            = bytes,
      .length            = 3 * page_size,
      .initialized_bytes = initialized_bytes,
   };
   ASSERT_TRUE(SUCCESS(io_async_state_init_extent(
      probe.state, io, cmd, addr, async_read_shadow_callback, &probe)));
   for (uint64 i = 0; i < 3; i++) {
      ASSERT_TRUE(SUCCESS(
         io_async_state_append_page(probe.state, bytes + i * page_size)));
   }
   if (cmd == io_async_preadv) {
      memset(bytes, 0xa5, probe.length);
   }
#if PLATFORM_API_TEST_MSAN
   __msan_poison(bytes, probe.length);
#endif
   async_status started = io_async_run(probe.state);
   io_wait_all(io);
   if (started == ASYNC_STATUS_DONE) {
      /* A negative offset can be rejected by submission rather than by a
       * completion. Neither path may mark destination bytes initialized. */
      async_read_shadow_check(&probe);
   }
   platform_status rc = io_async_state_get_result(probe.state);
   io_async_state_deinit(probe.state);
#if PLATFORM_API_TEST_MSAN
   __msan_unpoison(bytes, probe.length);
#endif
   ASSERT_TRUE(SUCCESS(platform_buffer_deinit(&buffer)));
   io_handle_destroy(io);

   ASSERT_TRUE(probe.shadow_ok);
   ASSERT_TRUE(probe.data_ok);
   ASSERT_EQUAL(expect_success, SUCCESS(rc));
   if (started == ASYNC_STATUS_DONE) {
      ASSERT_TRUE(allow_submit_failure);
      ASSERT_EQUAL(0, probe.callbacks);
   } else {
      ASSERT_EQUAL(1, probe.callbacks);
   }
}

CTEST2(platform_api, test_async_read_initializes_all_iovecs)
{
   test_async_io_shadow(data->hid,
                        io_async_preadv,
                        3 * IO_DEFAULT_PAGE_SIZE,
                        0,
                        3 * IO_DEFAULT_PAGE_SIZE,
                        TRUE,
                        FALSE);
}

CTEST2(platform_api, test_async_short_read_initializes_only_returned_bytes)
{
   test_async_io_shadow(data->hid,
                        io_async_preadv,
                        IO_DEFAULT_PAGE_SIZE + 37,
                        0,
                        IO_DEFAULT_PAGE_SIZE + 37,
                        FALSE,
                        FALSE);
}

CTEST2(platform_api, test_async_eof_read_keeps_buffer_poisoned)
{
   test_async_io_shadow(data->hid, io_async_preadv, 0, 0, 0, FALSE, FALSE);
}

CTEST2(platform_api, test_async_failed_read_keeps_buffer_poisoned)
{
   test_async_io_shadow(data->hid,
                        io_async_preadv,
                        3 * IO_DEFAULT_PAGE_SIZE,
                        UINT64_MAX,
                        0,
                        FALSE,
                        TRUE);
}

CTEST2(platform_api, test_async_write_does_not_initialize_source)
{
   test_async_io_shadow(data->hid, io_async_pwritev, 0, 0, 0, TRUE, FALSE);
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
   platform_status  rc =
      platform_condvar_init(&cv, data->hid != PROCESS_PRIVATE_HEAP_ID);
   if (STATUS_IS_EQ(rc, STATUS_NOTSUP)) {
      platform_error_log(
         "Platform possibly does not support"
         " process-shared mutexes, " STRINGIFY(PTHREAD_PROCESS_SHARED));
   }
   ASSERT_TRUE(SUCCESS(rc));

   platform_condvar_destroy(&cv);
}
