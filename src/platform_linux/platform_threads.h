#pragma once

#include "splinterdb/platform_linux/public_platform.h"
#include "platform_status.h"
#include "platform_heap.h"
#include <unistd.h>
#include <pthread.h>

/*
 * MAX_THREADS is used primarily for convenience, where allocations made on a
 * per-thread basis create an array with MAX_THREADS items, e.g. the
 * trunk_stats field in trunk_handle. The task subsystem also uses a 64-bit
 * bit-array to track thread IDs in use. This could be changed relatively
 * easily if needed.
 */
#define MAX_THREADS (64)
#define INVALID_TID (MAX_THREADS)


static inline threadid
platform_get_tid()
{
   extern __thread threadid xxxtid;
   return xxxtid;
}

static inline int
platform_getpid()
{
   return getpid();
}

typedef void (*platform_thread_worker)(void *);

typedef pthread_t platform_thread;

platform_status
platform_thread_create(platform_thread       *thread,
                       bool32                 detached,
                       platform_thread_worker worker,
                       void                  *arg,
                       platform_heap_id       heap_id);

platform_status
platform_thread_join(platform_thread *thread);

platform_thread
platform_thread_id_self();
