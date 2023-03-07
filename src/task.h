// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "platform.h"

typedef struct task_system task_system;

typedef void (*task_hook)(task_system *arg);
typedef void (*task_fn)(void *arg, void *scratch);

typedef struct task {
   struct task *next;
   struct task *prev;
   task_fn      func;
   void        *arg;
   task_system *ts;
   timestamp    enqueue_time;
} task;

/*
 * Run-time task-specific execution metrics structure.
 */
typedef struct {
   timestamp max_runtime_ns;
   void     *max_runtime_func;
   uint64    max_queue_wait_time_ns;
   uint64    max_outstanding_tasks;
   uint64    total_queue_wait_time_ns;
   uint64    total_bg_task_executions;
   uint64    total_fg_task_executions;
} PLATFORM_CACHELINE_ALIGNED task_stats;

typedef struct task_queue {
   task *head;
   task *tail;
} task_queue;

typedef struct task_bg_thread_group {
   bool            stop;
   uint8           num_threads;
   platform_thread threads[MAX_THREADS];
} task_bg_thread_group;

/*
 * Tasks are grouped into NUM_TASK_TYPES groups. Each group is described
 * by a structure of this type.
 */
typedef struct task_group {
   task_system *ts;
   task_queue   tq; // Queue of tasks in this group, of a task type

   volatile uint64 current_waiting_tasks;
   volatile uint64 current_executing_tasks;

   platform_condvar     cv;
   task_bg_thread_group bg;

   // Per thread stats.
   bool       use_stats;
   task_stats stats[MAX_THREADS];
} task_group;

/*
 * We maintain separate task groups for the memtable because memtable
 * jobs are much shorter than other jobs and are latency critical.  By
 * separating them out, we can devote a small number of threads to
 * deal with these small, latency critical tasks.
 */
typedef enum task_type {
   TASK_TYPE_INVALID = 0,
   TASK_TYPE_MEMTABLE,
   TASK_TYPE_NORMAL,
   NUM_TASK_TYPES,
   TASK_TYPE_FIRST = TASK_TYPE_MEMTABLE
} task_type;

typedef struct task_system_config {
   bool   use_stats;
   uint64 num_background_threads[NUM_TASK_TYPES];
   uint64 scratch_size;
} task_system_config;

platform_status
task_system_config_init(task_system_config *task_cfg,
                        bool                use_stats,
                        const uint64 num_background_threads[NUM_TASK_TYPES],
                        uint64       scratch_size);


#define TASK_MAX_HOOKS (4)

/*
 * ----------------------------------------------------------------------
 * Splinter specific state that gets created during initialization in
 * splinter_system_init(). Contains global state for splinter such as the
 * init thread, init thread's scratch memory, thread_id counter and an array
 * of all the threads, which acts like a map that is accessed by thread ID
 * to get the thread pointer.
 *
 * This structure is passed around like an opaque structure to all the
 * entities that need to access it. Some of them are task creation and
 * execution, task queue and clockcache.
 * ----------------------------------------------------------------------
 */
struct task_system {
   const task_system_config *cfg;
   // array of scratch space pointers for this system.
   // IO handle (currently one splinter system has just one)
   platform_io_handle *ioh;
   platform_heap_id    heap_id;
   /*
    * bitmask used for generating and clearing thread id's.
    * If a bit is set to 0, it means we have an in use thread id for that
    * particular position, 1 means it is unset and that thread id is available
    * for use.
    */
   uint64 tid_bitmask;
   // max thread id so far.
   threadid max_tid;
   void    *thread_scratch[MAX_THREADS];
   size_t   thread_scratch_mem_size;
   // task groups
   task_group group[NUM_TASK_TYPES];

   uint64    ntasks_enqueued;
   int       hook_init_done;
   int       num_hooks;
   task_hook hooks[TASK_MAX_HOOKS];
};

platform_status
task_thread_create(const char            *name,
                   platform_thread_worker func,
                   void                  *arg,
                   size_t                 scratch_size,
                   task_system           *ts,
                   platform_heap_id       hid,
                   platform_thread       *thread);

/*
 * Thread registration and deregistration.  These functions are for
 * threads created outside of the task system.  Threads created with
 * task_thread_create are automatically registered and unregistered.
 */

// Register the calling thread, allocating scratch space for it
#define task_register_this_thread(ts, scratch_size)                            \
   task_register_thread((ts), (scratch_size), __FILE__, __LINE__, __func__)

platform_status
task_register_thread(task_system *ts,
                     uint64       scratch_size,
                     const char  *file,
                     const int    lineno,
                     const char  *func);

// Unregister the calling thread and free its scratch space
#define task_deregister_this_thread(ts)                                        \
   task_deregister_thread((ts), __FILE__, __LINE__, __func__)

void
task_deregister_thread(task_system *ts,
                       const char  *file,
                       const int    lineno,
                       const char  *func);

/*
 * Create a task system and register the calling thread.
 */
platform_status
task_system_create(platform_heap_id          hid,
                   platform_io_handle       *ioh,
                   task_system             **system,
                   const task_system_config *cfg);

/*
 * Deregister the calling thread (if it is registered) and destroy the
 * task system.  It is recommended to not destroy the task system
 * until all registered threads have deregistered.
 *
 * task_system_destroy() waits for currently executing background
 * tasks and cleanly shuts down all background threads, but it
 * abandons tasks that are still waiting to execute.  To ensure that
 * no enqueued tasks are abandoned by a shutdown,
 *
 * 1. Ensure that your application will not enqueue any more tasks.
 * 2. Call task_perform_until_quiescent().
 * 3. Then call task_system_destroy().
 */
void
task_system_destroy(platform_heap_id hid, task_system **ts);


void *
task_system_get_thread_scratch(task_system *ts, threadid tid);

platform_status
task_enqueue(task_system *ts,
             task_type    type,
             task_fn      func,
             void        *arg,
             bool         at_head);

/*
 * Possibly performs one background task if there is one waiting,
 * based on the specified queue_scale_percent.  Otherwise returns
 * immediately.
 *
 * Returns:
 * - STATUS_TIMEDOUT to indicate that it did not run any task.
 * - STATUS_OK indicates that it did run a task.
 * - Other return codes indicate an error.
 *
 * queue_scale_percent specifies how big the queue must be, relative
 * to the number of background threads for that task group, for us to
 * perform a task in that group.
 *
 * So, for example,
 * - A queue_scale_percent of 0 means always perform a task if one is
 *   waiting.
 * - A queue_scale_percent of 100 means perform a task if there are
 *   more waiting tasks than background threads for that task
 *   queue. (a reasonable default)
 * - A queue_scale_percent of UINT64_MAX means (essentially) to never
 *   perform any tasks on that queue unless the number of background
 *   threads for that queue is 0.
 */
platform_status
task_perform_one_if_needed(task_system *ts, uint64 queue_scale_percent);

/*
 * Performs one task if there is one waiting.  Otherwise returns
 * immediately.  Returns STATUS_TIMEDOUT to indicate that there was no
 * task to run.
 */
static inline platform_status
task_perform_one(task_system *ts)
{
   return task_perform_one_if_needed(ts, 0);
}

/*
 * task_perform_all() - Perform all tasks queued with the task system.
 * Returns as soon as it finds the queue is empty.  Useful
 * specifically when you are preparing to shut down the task system.
 */
void
task_perform_all(task_system *ts);

/* TRUE if there are no running or waiting tasks. */
bool
task_system_is_quiescent(task_system *ts);

/*
 * Execute background tasks until there are no executing or enqueued
 * background tasks. Once the system is quiescent, no new tasks will be
 * enqueued unless the application does so.
 */
platform_status
task_perform_until_quiescent(task_system *ts);

/*
 *Functions for tests and debugging.
 */

void
task_wait_for_completion(task_system *ts);

threadid
task_get_max_tid(task_system *ts);

uint64
task_active_tasks_mask(task_system *ts);

void
task_print_stats(task_system *ts);
