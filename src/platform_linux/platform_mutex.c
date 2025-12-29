#include "platform_mutex.h"
#include "platform_assert.h"
#include "platform_status.h"
#include <pthread.h>

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
