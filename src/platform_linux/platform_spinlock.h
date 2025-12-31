// Copyright 2018-2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "platform_status.h"
#include <pthread.h>

// Spin lock
typedef pthread_spinlock_t platform_spinlock;

static inline platform_status
platform_spinlock_init(platform_spinlock *lock)
{
   int ret;

   // Init spinlock so it can be shared between processes, if so configured
   ret = pthread_spin_init(lock, PTHREAD_PROCESS_SHARED);
   return CONST_STATUS(ret);
}


static inline platform_status
platform_spinlock_destroy(platform_spinlock *lock)
{
   int ret;

   ret = pthread_spin_destroy(lock);

   return CONST_STATUS(ret);
}

static inline platform_status
platform_spin_lock(platform_spinlock *lock)
{
   int ret;

   ret = pthread_spin_lock(lock);

   return CONST_STATUS(ret);
}

static inline platform_status
platform_spin_unlock(platform_spinlock *lock)
{
   int ret;

   ret = pthread_spin_unlock(lock);

   return CONST_STATUS(ret);
}
