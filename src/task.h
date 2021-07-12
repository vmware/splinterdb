// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef __TASK_H
#define __TASK_H

#include "platform.h"

typedef struct task_system task_system;

typedef void (*task_hook) (task_system *arg);
typedef void (*task_fn)(void *arg, void *scratch);

typedef struct thread_task {
   platform_thread_worker  func;
   void                   *arg;
   // scratch memory for use by task dispatch functions
   void                   *scratch;
   // task system, used for running hook function and thread lookups.
   task_system            *ts;
   platform_heap_id        heap_id;
} thread_task;

typedef struct task {
   struct task *next;
   struct task *prev;
   task_fn      func;
   void        *arg;
   task_system *ts;
   timestamp    enqueue_time;
} task;

typedef struct {
   timestamp  max_runtime_ns;
   void      *max_runtime_func;
   uint64     total_latency_ns;
   uint64     total_tasks;
   uint64     max_latency_ns;
} PLATFORM_CACHELINE_ALIGNED task_stats;

typedef struct task_queue {
   task *head;
   task *tail;
} task_queue;

typedef struct task_bg_thread_group {
   platform_condvar cv;
   bool             stop;
   uint8            num_threads;
   platform_thread  threads[MAX_THREADS];
} task_bg_thread_group;

typedef struct task_fg_thread_group {
   platform_mutex mutex;
} task_fg_thread_group;

typedef struct task_group {
   task_system *ts;
   task_queue   tq;

   volatile uint64 current_outstanding_tasks;
   volatile uint64 max_outstanding_tasks;

   union {
      // a condition variable and thread tracking
      task_bg_thread_group bg;
      // a mutex
      task_fg_thread_group fg;
   };

   // Per thread stats.
   bool         use_stats;
   task_stats   stats[MAX_THREADS];
} task_group;

typedef enum task_type {
   TASK_TYPE_MEMTABLE,
   TASK_TYPE_NORMAL,
   NUM_TASK_TYPES,
} task_type;

/*
 * FIXME: [aconway 2020-09-09] I moved this into task, because this struct
 * doesn't really have any splinter-level concepts.
 *
 * Splinter specific state that gets created during initialization in
 * splinter_system_init(). Contains global state for splinter such as the
 * init thread, init thread's scratch memory, thread_id counter and an array
 * of all the threads, which acts like a map that is accessed by thread id
 * to get the thread pointer.
 *
 * This structure is passed around like an opaque structure to all the
 * entities that need to access it. Some of them are task creation and
 * execution, task queue and clockcache.
 */

struct task_system {
   // array of threads for this system.
   thread_task        *thread_tasks[MAX_THREADS];
   // IO handle (currently one splinter system has just one)
   platform_io_handle *ioh;
   /*
    * bitmask used for generating and clearing thread id's.
    * If a bit is set to 0, it means we have an in use thread id for that
    * particular position, 1 means it is unset and that thread id is available
    * for use.
    */
   uint64             tid_bitmask;
   // max thread id so far.
   threadid           max_tid;
   // task groups
   task_group         group[NUM_TASK_TYPES];
   bool               use_bg_threads;
   platform_heap_id   heap_id;

   // scratch memory for the init thread.
   uint64             scratch_size;
   thread_task        init_thread_task;
   threadid           init_tid;
   // FIXME: [aconway 2020-09-14] maybe just alloc this separately?
   //                             or just platform_cacheline_aligned?
   char               init_task_scratch[];
};

platform_status
task_register_hook(task_hook newhook);
void register_init_tid_hook(void);
void task_run_thread_hooks(task_system *ts);
void task_init_thread_task(task_system *ts,
                           const threadid tid,
                           thread_task *task,
                           void *task_scratch);
void            task_clear_threadid     (task_system *ts,
                                         threadid tid);
threadid        task_get_max_tid        (task_system *tst);

platform_status
task_thread_create(const char             *name,
                   platform_thread_worker  func,
                   void                   *arg,
                   size_t                  scratch_size,
                   task_system            *ts,
                   platform_heap_id        hid,
                   platform_thread        *thread);

platform_status
task_create_thread_with_hooks(platform_thread        *thread,
                              bool                    detached,
                              platform_thread_worker  func,
                              void                   *arg,
                              size_t                  scratch_size,
                              task_system            *ts,
                              platform_heap_id        hid);

void
task_init_tid_bitmask(uint64 *thread_bitmask);

uint64 *
task_system_get_tid_bitmask(task_system *ts);

platform_status
task_system_create(platform_heap_id     hid,
                   platform_io_handle  *ioh,
                   task_system        **system,
                   bool                 use_stats,
                   bool                 use_bg_threads,
                   uint8                num_bg_threads[NUM_TASK_TYPES],
                   uint64               scratch_size);

thread_task **
task_system_get_system_threads(task_system *ts);

threadid *
task_system_get_max_tid(task_system *ts);

void
task_system_register_thread(task_system *ts);

void
task_system_io_register_thread(task_system *ts);

void
task_system_destroy(platform_heap_id hid, task_system *ts);

bool
task_is_threadid_allocated(task_system *ts,
                           threadid tid);

void *
task_system_get_thread_scratch(task_system *ts,
                               threadid     tid);

bool
task_system_use_bg_threads(task_system *ts);

platform_status
task_enqueue(task_system *ts,
             task_type type,
             task_fn func,
             void *arg,
             bool at_head);

platform_status
task_perform_one(task_system *ts);

void
task_perform_all(task_system *ts);

void
task_wait_for_completion(task_system *ts);

void
task_print_stats(task_system *ts);

#endif // __TASK_H
