// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include <stdarg.h>
#include <sys/mman.h>
#include "platform.h"
#include "shmem.h"

__thread threadid xxxtid = INVALID_TID;

bool32 platform_use_hugetlb = FALSE;
bool32 platform_use_mlock   = FALSE;

// By default, platform_default_log() messages are sent to /dev/null
// and platform_error_log() messages go to stderr (see below).
//
// Use platform_set_log_streams() to send the log messages elsewhere.
platform_log_handle *Platform_default_log_handle = NULL;
platform_log_handle *Platform_error_log_handle   = NULL;

/*
 * Declare globals to track heap handle/ID that may have been created when
 * using shared memory. We stash away these handles so that we can return the
 * right handle via platform_get_heap_id() interface, in case shared segments
 * are in use.
 */
platform_heap_id Heap_id = NULL;

// This function is run automatically at library-load time
void __attribute__((constructor)) platform_init_log_file_handles(void)
{
   FILE *dev_null_file = fopen("/dev/null", "w");
   platform_assert(dev_null_file != NULL);

   Platform_default_log_handle = dev_null_file;
   Platform_error_log_handle   = stderr;
}

// Set the streams where informational and error messages will be printed.
void
platform_set_log_streams(platform_log_handle *info_stream,
                         platform_log_handle *error_stream)
{
   platform_assert(info_stream != NULL);
   platform_assert(error_stream != NULL);
   Platform_default_log_handle = info_stream;
   Platform_error_log_handle   = error_stream;
}

// Return the stdout log-stream handle
platform_log_handle *
platform_get_stdout_stream(void)
{
   return Platform_default_log_handle;
}

/*
 * platform_heap_create() - Create a heap for memory allocation.
 *
 * By default, we just revert to process' heap-memory and use malloc() / free()
 * for memory management. If Splinter is run with shared-memory configuration,
 * create a shared-segment which acts as the 'heap' for memory allocation.
 */
platform_status
platform_heap_create(platform_module_id UNUSED_PARAM(module_id),
                     size_t             max,
                     bool               use_shmem,
                     platform_heap_id  *heap_id)
{
   *heap_id = PROCESS_PRIVATE_HEAP_ID;

   if (use_shmem) {
      platform_status rc = platform_shmcreate(max, heap_id);
      if (SUCCESS(rc)) {
         Heap_id = *heap_id;
      }
      return rc;
   }
   *heap_id = NULL;
   return STATUS_OK;
}

void
platform_heap_destroy(platform_heap_id *heap_id)
{
   // If shared segment was allocated, it's being tracked thru heap ID.
   if (*heap_id) {
      return platform_shmdestroy(heap_id);
   }
}

/*
 * Certain modules, e.g. the buffer cache, need a very large buffer which
 * may not be serviceable by the heap. Create the requested buffer using
 * mmap() and initialize the input 'bh' to track this memory allocation.
 */
platform_status
platform_buffer_init(buffer_handle *bh, size_t length)
{
   platform_status rc = STATUS_NO_MEMORY;

   int prot = PROT_READ | PROT_WRITE;

   // Technically, for threaded execution model, MAP_PRIVATE is sufficient.
   // And we only need to create this mmap()'ed buffer in MAP_SHARED for
   // process-execution mode. But, at this stage, we don't know apriori if
   // we will be using SplinterDB in a multi-process execution environment.
   // So, always create this in SHARED mode. This still works for multiple
   // threads.
   int flags = MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE;
   if (platform_use_hugetlb) {
      flags |= MAP_HUGETLB;
   }

   bh->addr = mmap(NULL, length, prot, flags, -1, 0);
   if (bh->addr == MAP_FAILED) {
      platform_error_log(
         "mmap (%lu bytes) failed with error: %s\n", length, strerror(errno));
      goto error;
   }

   if (platform_use_mlock) {
      int mlock_rv = mlock(bh->addr, length);
      if (mlock_rv != 0) {
         platform_error_log("mlock (%lu bytes) failed with error: %s\n",
                            length,
                            strerror(errno));
         // Save off rc from mlock()-failure, so we return that as status
         rc = CONST_STATUS(errno);

         int rv = munmap(bh->addr, length);
         if (rv != 0) {
            // We are losing rc from this failure; can't return 2 statuses
            platform_error_log("munmap %p (%lu bytes) failed with error: %s\n",
                               bh->addr,
                               length,
                               strerror(errno));
         }
         goto error;
      }
   }
   bh->length = length;
   return STATUS_OK;

error:
   // Reset, in case mmap() or mlock() failed.
   bh->addr = NULL;
   return rc;
}

void *
platform_buffer_getaddr(const buffer_handle *bh)
{
   return bh->addr;
}

/*
 * platform_buffer_deinit() - Deinit the buffer handle, which involves
 * unmapping the memory for the large buffer created using mmap().
 * This is expected to be called on a 'bh' that has been successfully
 * initialized, thru a prior platform_buffer_init() call.
 */
platform_status
platform_buffer_deinit(buffer_handle *bh)
{
   int ret;
   ret = munmap(bh->addr, bh->length);
   if (ret) {
      return CONST_STATUS(errno);
   }

   bh->addr   = NULL;
   bh->length = 0;
   return STATUS_OK;
}

/*
 * platform_thread_create() - External interface to create a Splinter thread.
 */
platform_status
platform_thread_create(platform_thread       *thread,
                       bool32                 detached,
                       platform_thread_worker worker,
                       void                  *arg,
                       platform_heap_id       UNUSED_PARAM(heap_id))
{
   int ret;

   if (detached) {
      pthread_attr_t attr;
      pthread_attr_init(&attr);
      size_t stacksize = 16UL * 1024UL;
      pthread_attr_setstacksize(&attr, stacksize);
      pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
      ret = pthread_create(thread, &attr, (void *(*)(void *))worker, arg);
      pthread_attr_destroy(&attr);
   } else {
      ret = pthread_create(thread, NULL, (void *(*)(void *))worker, arg);
   }

   return CONST_STATUS(ret);
}

platform_status
platform_thread_join(platform_thread thread)
{
   int   ret;
   void *retval;

   ret = pthread_join(thread, &retval);

   return CONST_STATUS(ret);
}

platform_thread
platform_thread_id_self()
{
   return pthread_self();
}

platform_status
platform_mutex_init(platform_mutex    *lock,
                    platform_module_id UNUSED_PARAM(module_id),
                    platform_heap_id   heap_id)
{
   platform_status status;
   // Init mutex so it can be shared between processes, if so configured
   pthread_mutexattr_t mattr;
   pthread_mutexattr_init(&mattr);
   status.r = pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
   if (!SUCCESS(status)) {
      return status;
   }

   // clang-format off
   status.r = pthread_mutex_init(&lock->mutex,
                                 ((heap_id == PROCESS_PRIVATE_HEAP_ID)
                                    ? NULL : &mattr));
   // clang-format on

   // Mess with output vars only in case of a success
   if (SUCCESS(status)) {
      lock->owner = INVALID_TID;
   }
   return status;
}

platform_status
platform_mutex_destroy(platform_mutex *lock)
{
   // Cannot call destroy on a locked lock
   platform_assert(lock->owner == INVALID_TID);
   int ret = pthread_mutex_destroy(&lock->mutex);
   return CONST_STATUS(ret);
}

platform_status
platform_spinlock_init(platform_spinlock *lock,
                       platform_module_id UNUSED_PARAM(module_id),
                       platform_heap_id   heap_id)
{
   int ret;

   // Init spinlock so it can be shared between processes, if so configured
   ret = pthread_spin_init(lock,
                           ((heap_id == PROCESS_PRIVATE_HEAP_ID)
                               ? PTHREAD_PROCESS_PRIVATE
                               : PTHREAD_PROCESS_SHARED));
   return CONST_STATUS(ret);
}

platform_status
platform_spinlock_destroy(platform_spinlock *lock)
{
   int ret;

   ret = pthread_spin_destroy(lock);

   return CONST_STATUS(ret);
}

/*
 *-----------------------------------------------------------------------------
 * Batch Read/Write Locks
 *
 * These are generic distributed reader/writer locks with an intermediate claim
 * state. These offer similar semantics to the locks in the clockcache, but in
 * these locks read locks cannot be held with write locks.
 *
 * These locks are allocated in batches of PLATFORM_CACHELINE_SIZE / 2, so that
 * the write lock and claim bits, as well as the distributed read counters, can
 * be colocated across the batch.
 *-----------------------------------------------------------------------------
 */

void
platform_batch_rwlock_init(platform_batch_rwlock *lock)
{
   ZERO_CONTENTS(lock);
}

/*
 *-----------------------------------------------------------------------------
 * lock/unlock
 *
 * Drains and blocks all gets (read locks)
 *
 * Caller must hold a claim
 * Caller cannot hold a get (read lock)
 *-----------------------------------------------------------------------------
 */

void
platform_batch_rwlock_lock(platform_batch_rwlock *lock, uint64 lock_idx)
{
   platform_assert(lock->write_lock[lock_idx].claim);
   debug_only uint8 was_locked =
      __sync_lock_test_and_set(&lock->write_lock[lock_idx].lock, 1);
   debug_assert(!was_locked,
                "platform_batch_rwlock_lock: Attempt to lock a locked page.\n");

   uint64 wait = 1;
   for (uint64 i = 0; i < MAX_THREADS; i++) {
      while (lock->read_counter[i][lock_idx] != 0) {
         platform_sleep_ns(wait);
         wait = wait > 2048 ? wait : 2 * wait;
      }
      wait = 1;
   }
}

void
platform_batch_rwlock_unlock(platform_batch_rwlock *lock, uint64 lock_idx)
{
   __sync_lock_release(&lock->write_lock[lock_idx].lock);
}

void
platform_batch_rwlock_full_unlock(platform_batch_rwlock *lock, uint64 lock_idx)
{
   platform_batch_rwlock_unlock(lock, lock_idx);
   platform_batch_rwlock_unclaim(lock, lock_idx);
   platform_batch_rwlock_unget(lock, lock_idx);
}

/*
 *-----------------------------------------------------------------------------
 * try_claim/claim/unlock
 *
 * A claim blocks all other claimants (and therefore all other writelocks,
 * because writelocks are required to hold a claim during the writelock).
 *
 * Must hold a get (read lock)
 * try_claim returns whether the claim succeeded
 *-----------------------------------------------------------------------------
 */

bool32
platform_batch_rwlock_try_claim(platform_batch_rwlock *lock, uint64 lock_idx)
{
   threadid tid = platform_get_tid();
   debug_assert(lock->read_counter[tid][lock_idx]);
   if (__sync_lock_test_and_set(&lock->write_lock[lock_idx].claim, 1)) {
      return FALSE;
   }
   debug_only uint8 old_counter =
      __sync_fetch_and_sub(&lock->read_counter[tid][lock_idx], 1);
   debug_assert(0 < old_counter);
   return TRUE;
}

void
platform_batch_rwlock_claim_loop(platform_batch_rwlock *lock, uint64 lock_idx)
{
   uint64 wait = 1;
   while (!platform_batch_rwlock_try_claim(lock, lock_idx)) {
      platform_batch_rwlock_unget(lock, lock_idx);
      platform_sleep_ns(wait);
      wait = wait > 2048 ? wait : 2 * wait;
      platform_batch_rwlock_get(lock, lock_idx);
   }
}

void
platform_batch_rwlock_unclaim(platform_batch_rwlock *lock, uint64 lock_idx)
{
   threadid tid = platform_get_tid();
   __sync_fetch_and_add(&lock->read_counter[tid][lock_idx], 1);
   __sync_lock_release(&lock->write_lock[lock_idx].claim);
}

/*
 *-----------------------------------------------------------------------------
 * get/unget
 *
 * Acquire a read lock
 *-----------------------------------------------------------------------------
 */

void
platform_batch_rwlock_get(platform_batch_rwlock *lock, uint64 lock_idx)
{
   threadid tid = platform_get_tid();
   while (1) {
      uint64 wait = 1;
      while (lock->write_lock[lock_idx].lock) {
         platform_sleep_ns(wait);
         wait = wait > 2048 ? wait : 2 * wait;
      }
      debug_only uint8 old_counter =
         __sync_fetch_and_add(&lock->read_counter[tid][lock_idx], 1);
      debug_assert(old_counter == 0);
      if (!lock->write_lock[lock_idx].lock) {
         return;
      }
      old_counter = __sync_fetch_and_sub(&lock->read_counter[tid][lock_idx], 1);
      debug_assert(old_counter == 1);
   }
   platform_assert(0);
}

void
platform_batch_rwlock_unget(platform_batch_rwlock *lock, uint64 lock_idx)
{
   threadid         tid = platform_get_tid();
   debug_only uint8 old_counter =
      __sync_fetch_and_sub(&lock->read_counter[tid][lock_idx], 1);
   debug_assert(old_counter == 1);
}


platform_status
platform_condvar_init(platform_condvar *cv, platform_heap_id heap_id)
{
   platform_status status;
   bool32          pth_condattr_setpshared_failed = FALSE;

   // Init mutex so it can be shared between processes, if so configured
   pthread_mutexattr_t mattr;
   pthread_mutexattr_init(&mattr);
   status.r = pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
   if (!SUCCESS(status)) {
      goto mutexattr_setpshared_failed;
   }

   // clang-format off
   status.r = pthread_mutex_init(&cv->lock,
                                ((heap_id == PROCESS_PRIVATE_HEAP_ID)
                                    ? NULL : &mattr));
   // clang-format on
   debug_only int rv = pthread_mutexattr_destroy(&mattr);
   debug_assert(rv == 0);

   if (!SUCCESS(status)) {
      goto mutex_init_failed;
   }

   // Init condition so it can be shared between processes, if so configured
   pthread_condattr_t cattr;
   pthread_condattr_init(&cattr);
   status.r = pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_SHARED);
   if (!SUCCESS(status)) {
      pth_condattr_setpshared_failed = TRUE;
      goto condattr_setpshared_failed;
   }

   // clang-format off
   status.r = pthread_cond_init(&cv->cond,
                                ((heap_id == PROCESS_PRIVATE_HEAP_ID)
                                    ? NULL : &cattr));
   // clang-format on

   rv = pthread_condattr_destroy(&cattr);
   debug_assert(rv == 0);

   // Upon a failure, before exiting, release mutex init'ed above ...
   if (!SUCCESS(status)) {
      goto cond_init_failed;
   }

   return status;

   int ret = 0;
cond_init_failed:
condattr_setpshared_failed:
   ret = pthread_mutex_destroy(&cv->lock);
   // Yikes! Even this failed. We will lose the prev errno ...
   if (ret) {
      platform_error_log("%s() failed with error: %s\n",
                         (pth_condattr_setpshared_failed
                             ? "pthread_condattr_setpshared"
                             : "pthread_cond_init"),
                         platform_status_to_string(status));

      // Return most recent failure rc
      return CONST_STATUS(ret);
   }
mutex_init_failed:
mutexattr_setpshared_failed:
   return status;
}

platform_status
platform_condvar_wait(platform_condvar *cv)
{
   int status;

   status = pthread_cond_wait(&cv->cond, &cv->lock);
   return CONST_STATUS(status);
}

// The pthread function requires the absolute time, but this function
// takes a duration for waiting in ns for convenience.
platform_status
platform_condvar_timedwait(platform_condvar *cv, timestamp timeouts_ns)
{
   int status;

   struct timespec ts;
   clock_gettime(CLOCK_REALTIME, &ts);
   const timestamp one_second_ns = SEC_TO_NSEC(1);
   ts.tv_sec += (ts.tv_nsec + timeouts_ns) / one_second_ns;
   ts.tv_nsec = (ts.tv_nsec + timeouts_ns) % one_second_ns;
   status     = pthread_cond_timedwait(&cv->cond, &cv->lock, &ts);
   return CONST_STATUS(status);
}

platform_status
platform_condvar_signal(platform_condvar *cv)
{
   int status;

   status = pthread_cond_signal(&cv->cond);
   return CONST_STATUS(status);
}

platform_status
platform_condvar_broadcast(platform_condvar *cv)
{
   int status;

   status = pthread_cond_broadcast(&cv->cond);
   return CONST_STATUS(status);
}

platform_status
platform_histo_create(platform_heap_id       heap_id,
                      uint32                 num_buckets,
                      const int64 *const     bucket_limits,
                      platform_histo_handle *histo)
{
   platform_histo_handle hh;
   hh = TYPED_MANUAL_MALLOC(
      heap_id, hh, sizeof(hh) + num_buckets * sizeof(hh->count[0]));
   if (!hh) {
      return STATUS_NO_MEMORY;
   }
   hh->num_buckets   = num_buckets;
   hh->bucket_limits = bucket_limits;
   hh->total         = 0;
   hh->min           = INT64_MAX;
   hh->max           = INT64_MIN;
   hh->num           = 0;
   memset(hh->count, 0, hh->num_buckets * sizeof(hh->count[0]));

   *histo = hh;
   return STATUS_OK;
}

void
platform_histo_destroy(platform_heap_id       heap_id,
                       platform_histo_handle *histo_out)
{
   platform_assert(histo_out);
   platform_histo_handle histo = *histo_out;
   platform_free(heap_id, histo);
   *histo_out = NULL;
}

void
platform_histo_print(platform_histo_handle histo,
                     const char           *name,
                     platform_log_handle  *log_handle)
{
   if (histo->num == 0) {
      return;
   }

   platform_log(log_handle, "%s\n", name);
   platform_log(log_handle, "min: %ld\n", histo->min);
   platform_log(log_handle, "max: %ld\n", histo->max);
   platform_log(log_handle,
                "mean: %ld\n",
                histo->num == 0 ? 0 : histo->total / histo->num);
   platform_log(log_handle, "count: %ld\n", histo->num);
   for (uint32 i = 0; i < histo->num_buckets; i++) {
      if (i == histo->num_buckets - 1) {
         platform_log(log_handle,
                      "%-12ld  > %12ld\n",
                      histo->count[i],
                      histo->bucket_limits[i - 1]);
      } else {
         platform_log(log_handle,
                      "%-12ld <= %12ld\n",
                      histo->count[i],
                      histo->bucket_limits[i]);
      }
   }
   platform_log(log_handle, "\n");
}

char *
platform_strtok_r(char *str, const char *delim, platform_strtok_ctx *ctx)
{
   return strtok_r(str, delim, &ctx->token_str);
}

void
platform_sort_slow(void               *base,
                   size_t              nmemb,
                   size_t              size,
                   platform_sort_cmpfn cmpfn,
                   void               *cmparg,
                   void               *temp)
{
   return qsort_r(base, nmemb, size, cmpfn, cmparg);
}

/*
 * platform_assert_false() -
 *
 * Platform-specific assert implementation, with support to print an optional
 * message and arguments involved in the assertion failure. The caller-macro
 * ensures that this function will only be called when the 'exprval' is FALSE;
 * i.e. the assertion is failing.
 */
__attribute__((noreturn)) void
platform_assert_false(const char *filename,
                      int         linenumber,
                      const char *functionname,
                      const char *expr,
                      const char *message,
                      ...)
{
   va_list varargs;
   va_start(varargs, message);

   // Run-time assertion messages go to stderr.
   platform_assert_msg(Platform_error_log_handle,
                       filename,
                       linenumber,
                       functionname,
                       expr,
                       message,
                       varargs);
   va_end(varargs);
   platform_error_log("\n");

   abort();
}

/*
 * Lower-level function to generate the assertion message, alone.
 */
void
platform_assert_msg(platform_log_handle *log_handle,
                    const char          *filename,
                    int                  linenumber,
                    const char          *functionname,
                    const char          *expr,
                    const char          *message,
                    va_list              varargs)
{
   static char assert_msg_fmt[] = "OS-pid=%d, OS-tid=%lu, Thread-ID=%lu, "
                                  "Assertion failed at %s:%d:%s(): \"%s\". ";
   platform_log(log_handle,
                assert_msg_fmt,
                platform_getpid(),
                gettid(),
                platform_get_tid(), // SplinterDB's thread-ID (index)
                filename,
                linenumber,
                functionname,
                expr);
   vfprintf(log_handle, message, varargs);
}
