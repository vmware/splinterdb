// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include <stdarg.h>
#include <unistd.h>
#include "platform.h"

#include <sys/mman.h>

__thread threadid xxxtid = INVALID_TID;

bool platform_use_hugetlb = FALSE;
bool platform_use_mlock   = FALSE;

// By default, platform_default_log() messages are sent to /dev/null
// and platform_error_log() messages go to stderr (see below).
//
// Use platform_set_log_streams() to send the log messages elsewhere.
platform_log_handle *Platform_default_log_handle = NULL;
platform_log_handle *Platform_error_log_handle   = NULL;

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

platform_status
platform_heap_create(platform_module_id    UNUSED_PARAM(module_id),
                     uint32                max,
                     platform_heap_handle *heap_handle,
                     platform_heap_id     *heap_id)
{
   *heap_handle = NULL;
   *heap_id     = NULL;

   return STATUS_OK;
}

void
platform_heap_destroy(platform_heap_handle UNUSED_PARAM(*heap_handle))
{}

/*
 * platform_buffer_init() - Initialize an input buffer_handle, bh.
 *
 * Certain modules, e.g. the buffer cache, need a very large buffer which
 * may not be serviceable by the heap. Create the requested buffer using
 * mmap() and initialize the input 'bh' to track this memory allocation.
 */
platform_status
platform_buffer_init(buffer_handle *bh, size_t length)
{
   platform_status rc = STATUS_NO_MEMORY;

   int prot  = PROT_READ | PROT_WRITE;
   int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
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
         munmap(bh->addr, length);
         rc = CONST_STATUS(errno);
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
                       bool                   detached,
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
                    platform_heap_id   UNUSED_PARAM(heap_id))
{
   int ret     = pthread_mutex_init(&lock->mutex, NULL);
   lock->owner = INVALID_TID;
   return CONST_STATUS(ret);
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
                       platform_heap_id   UNUSED_PARAM(heap_id))
{
   int ret;

   ret = pthread_spin_init(lock, PTHREAD_PROCESS_PRIVATE);

   return CONST_STATUS(ret);
}

platform_status
platform_spinlock_destroy(platform_spinlock *lock)
{
   int ret;

   ret = pthread_spin_destroy(lock);

   return CONST_STATUS(ret);
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
platform_histo_destroy(platform_heap_id heap_id, platform_histo_handle histo)
{
   platform_assert(histo);
   platform_free(heap_id, histo);
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

platform_status
platform_condvar_init(platform_condvar *cv, platform_heap_id heap_id)
{
   platform_status status;

   status.r = pthread_mutex_init(&cv->lock, NULL);
   if (!SUCCESS(status)) {
      return status;
   }

   status.r = pthread_cond_init(&cv->cond, NULL);
   if (!SUCCESS(status)) {
      status.r = pthread_mutex_destroy(&cv->lock);
   }

   return status;
}

platform_status
platform_condvar_wait(platform_condvar *cv)
{
   int status;

   status = pthread_cond_wait(&cv->cond, &cv->lock);
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
                getpid(),
                gettid(),
                platform_get_tid(), // SplinterDB's thread-ID (index)
                filename,
                linenumber,
                functionname,
                expr);
   vfprintf(log_handle, message, varargs);
}
