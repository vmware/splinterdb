// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once
#include "platform_heap.h"
#include <semaphore.h>


typedef sem_t platform_semaphore;

static inline void
platform_semaphore_destroy(platform_semaphore *sema)
{
   __attribute__((unused)) int err = sem_destroy(sema);
   debug_assert(!err);
}

/*
 * Ref: https://man7.org/linux/man-pages/man3/sem_init.3.html
 * for choice of 'pshared' arg to sem_init().
 */
static inline void
platform_semaphore_init(platform_semaphore *sema,
                        int                 value,
                        platform_heap_id    heap_id)
{
   // If we are running with a shared segment, it's likely that we
   // may also fork child processes attaching to Splinter's shmem.
   // Then, use 1 => spinlocks are shared across process boundaries.
   // Else, use 0 => spinlocks are shared between threads in a process.
   __attribute__((unused)) int err =
      sem_init(sema, ((heap_id == PROCESS_PRIVATE_HEAP_ID) ? 0 : 1), value);
   debug_assert(!err);
}

static inline void
platform_semaphore_post(platform_semaphore *sema)
{
   __attribute__((unused)) int err = sem_post(sema);
   debug_assert(!err);
}

static inline void
platform_semaphore_wait(platform_semaphore *sema)
{
   __attribute__((unused)) int err = sem_wait(sema);
   debug_assert(!err);
}

/*
 * STATUS_OK: if wait succeeded
 * STATUS_BUSY: if need to retry
 * other: failure
 */
static inline platform_status
platform_semaphore_try_wait(platform_semaphore *sema)
{
   int ret = sem_trywait(sema);

   if (ret == 0) {
      return STATUS_OK;
   }
   if (errno == EAGAIN) {
      return STATUS_BUSY;
   }

   return CONST_STATUS(errno);
}
