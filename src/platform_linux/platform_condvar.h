// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "platform_status.h"
#include "platform_heap.h"
#include <pthread.h>

typedef struct platform_condvar {
   pthread_mutex_t lock;
   pthread_cond_t  cond;
} platform_condvar;

platform_status
platform_condvar_init(platform_condvar *cv, platform_heap_id heap_id);

platform_status
platform_condvar_wait(platform_condvar *cv);

platform_status
platform_condvar_signal(platform_condvar *cv);

platform_status
platform_condvar_broadcast(platform_condvar *cv);


static inline platform_status
platform_condvar_lock(platform_condvar *cv)
{
   int status;

   status = pthread_mutex_lock(&cv->lock);
   return CONST_STATUS(status);
}

static inline platform_status
platform_condvar_unlock(platform_condvar *cv)
{
   int status;

   status = pthread_mutex_unlock(&cv->lock);
   return CONST_STATUS(status);
}

static inline void
platform_condvar_destroy(platform_condvar *cv)
{
   pthread_mutex_destroy(&cv->lock);
   pthread_cond_destroy(&cv->cond);
}
