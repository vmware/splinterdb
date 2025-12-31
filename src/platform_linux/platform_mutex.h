#pragma once

#include "splinterdb/platform_linux/public_platform.h"
#include "platform_assert.h"
#include "platform_threads.h"
#include "platform_status.h"

// Thread-specific mutex, with ownership tracking.
typedef struct platform_mutex {
   pthread_mutex_t mutex;
   threadid        owner;
} platform_mutex;

platform_status
platform_mutex_init(platform_mutex    *mu,
                    platform_module_id module_id,
                    platform_heap_id   heap_id);

platform_status
platform_mutex_destroy(platform_mutex *mu);

static inline platform_status
platform_mutex_lock(platform_mutex *lock)
{
   int ret = pthread_mutex_lock(&lock->mutex);
   platform_assert(lock->owner == INVALID_TID,
                   "Found an unlocked a mutex with an existing owner:\n"
                   "lock: %p, tid: %lu, owner: %lu\n",
                   lock,
                   platform_get_tid(),
                   lock->owner);
   lock->owner = platform_get_tid();
   return CONST_STATUS(ret);
}

static inline platform_status
platform_mutex_unlock(platform_mutex *lock)
{
   platform_assert(lock->owner == platform_get_tid(),
                   "Attempt to unlock a mutex without ownership:\n"
                   "lock: %p, tid: %lu, owner: %lu\n",
                   lock,
                   platform_get_tid(),
                   lock->owner);
   lock->owner = INVALID_TID;
   int ret     = pthread_mutex_unlock(&lock->mutex);
   return CONST_STATUS(ret);
}
