#include "platform_condvar.h"
#include "platform_assert.h"
#include "platform_status.h"
#include "platform_log.h"
#include <pthread.h>

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
