// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include <stdarg.h>
#include <sys/mman.h>
#include "platform.h"
#include "shmem.h"
#include "task.h"
#include "io.h"

bool32 platform_use_hugetlb = FALSE;
bool32 platform_use_mlock   = FALSE;

// By default, platform_default_log() messages are sent to /dev/null
// and platform_error_log() messages go to stderr (see below).
//
// Use platform_set_log_streams() to send the log messages elsewhere.
platform_log_handle *Platform_default_log_handle = NULL;
platform_log_handle *Platform_error_log_handle   = NULL;

// This function is run automatically at library-load time
void __attribute__((constructor))
platform_init_log_file_handles(void)
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
