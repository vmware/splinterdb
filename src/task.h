// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/platform_linux/public_platform.h"
#include "platform_status.h"
#include "platform_heap.h"
#include "platform_threads.h"
#include "platform_condvar.h"

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
   bool32 use_stats;
   uint64 num_background_threads[NUM_TASK_TYPES];
} task_system_config;

platform_status
task_system_config_init(task_system_config *task_cfg,
                        bool32              use_stats,
                        const uint64 num_background_threads[NUM_TASK_TYPES]);


typedef struct task_system task_system;

typedef void (*task_fn)(void *arg);

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
   uint64    total_tasks_enqueued;
} PLATFORM_CACHELINE_ALIGNED task_stats;

typedef struct task_queue {
   task *head;
   task *tail;
} task_queue;

typedef struct task_bg_thread_group {
   bool32          stop;
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
   bool32     use_stats;
   task_stats stats[MAX_THREADS];
} task_group;

/*
 * ----------------------------------------------------------------------
 * Splinter specific state that gets created during initialization in
 * splinter_system_init(). Contains global state for splinter such as the
 * init thread, thread_id counter and an array of all the threads, which
 * acts like a map that is accessed by thread ID to get the thread pointer.
 *
 * This structure is passed around like an opaque structure to all the
 * entities that need to access it. Some of them are task creation and
 * execution, task queue and clockcache.
 * ----------------------------------------------------------------------
 */
struct task_system {
   const task_system_config *cfg;
   platform_heap_id          heap_id;
   // task groups
   task_group group[NUM_TASK_TYPES];
};

/*
 * Create a task system and register the calling thread.
 */
platform_status
task_system_create(platform_heap_id          hid,
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

platform_status
task_enqueue(task_system *ts,
             task_type    type,
             task_fn      func,
             void        *arg,
             bool32       at_head);

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
bool32
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
task_print_stats(task_system *ts);
