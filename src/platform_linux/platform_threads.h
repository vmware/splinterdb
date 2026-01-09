// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

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

/* This is not part of the platform API.  It is used internally to this platform
 * implementation. Specifically, it is used in laio.c. */
static inline threadid
platform_linux_get_pid()
{
   extern threadid xxxpid;
   return xxxpid;
}

typedef void (*process_event_callback)(threadid, void *);

typedef struct process_event_callback_list_node {
   process_event_callback                   termination;
   void                                    *arg;
   struct process_event_callback_list_node *next;
} process_event_callback_list_node;

void
platform_linux_add_process_event_callback(
   process_event_callback_list_node *node);

void
platform_linux_remove_process_event_callback(
   process_event_callback_list_node *node);

static inline int
platform_get_os_pid()
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

threadid
platform_num_threads(void);
