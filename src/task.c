// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform.h"
#include "task.h"

#include "poison.h"

#define MAX_HOOKS (2048)

int              hook_init_done = 0;
static int       num_hooks      = 0;
static task_hook hooks[MAX_HOOKS];

// forward declarations that aren't part of the public API of the task system
platform_status
task_register_hook(task_hook newhook);

threadid *
task_system_get_max_tid(task_system *ts);

uint64 *
task_system_get_tid_bitmask(task_system *ts);

void
task_system_io_register_thread(task_system *ts);
// end forward declarations

void
task_init_tid_bitmask(uint64 *tid_bitmask)
{
   // We use a 64 bit word to act as the thread bitmap.
   _Static_assert(MAX_THREADS == 64, "Max threads should be 64");
   /*
    * This is a special bitmask where 1 indicates free and 0 indicates
    * allocated. So, we set all bits to 1 during init.
    */
   for (int i = 0; i < MAX_THREADS; i++) {
      *tid_bitmask |= (1ULL << i);
   }
}

static void
init_threadid(task_system *ts)
{
   threadid  tid         = INVALID_TID;
   uint64   *tid_bitmask = task_system_get_tid_bitmask(ts);
   threadid *max_tid     = task_system_get_max_tid(ts);

   while (1) {
      uint64 tmp_bitmask = *tid_bitmask;
      // first bit set to 1 starting from LSB.
      uint64 pos = __builtin_ffsl(tmp_bitmask);
      // builtin_ffsl returns the position plus 1.
      tid = pos - 1;
      // set bit at that position to 0, indicating in use.
      uint64 new_val = (tmp_bitmask & ~(1ULL << (pos - 1)));
      if (__sync_bool_compare_and_swap(tid_bitmask, tmp_bitmask, new_val)) {
         // atomically update the max_tid.
         for (threadid tmp = *max_tid; tid > tmp; tmp = *max_tid) {
            if (__sync_bool_compare_and_swap(max_tid, tmp, tid)) {
               goto out;
            }
         }
         goto out;
      }
   }

out:
   debug_assert(tid != INVALID_TID);
   platform_set_tid(tid);
}

threadid
task_get_max_tid(task_system *ts)
{
   threadid *max_tid = task_system_get_max_tid(ts);
   /*
    * The caller(s) iterate till < return_value from this function.
    * We have assigned threads till max_tid, hence return plus
    * one to ensure that the last thread is covered in the iteration.
    */
   return *max_tid + 1;
}

/*
 * This is part of task initialization and needs to be called at the
 * beginning of the main thread that uses the task, similar to how
 * __attribute__((constructor)) works.
 */
void
register_init_tid_hook(void)
{
   // hooks need to be initialized only once.
   if (__sync_fetch_and_add(&hook_init_done, 1) == 0) {
      task_register_hook(init_threadid);
      task_register_hook(task_system_io_register_thread);
   }
}

void
task_run_thread_hooks(task_system *ts)
{
   for (int i = 0; i < num_hooks; i++) {
      hooks[i](ts);
   }
}

void
task_clear_threadid(task_system *ts, threadid tid)
{
   uint64 *tid_bitmask = task_system_get_tid_bitmask(ts);
   platform_assert(!(*tid_bitmask & (1ULL << tid)));
   // set bit back to 1 to indicate a free slot.
   while (1) {
      uint64 tmp_bitmask = *tid_bitmask;
      uint64 new_value   = tmp_bitmask | (1ULL << tid);
      if (__sync_bool_compare_and_swap(tid_bitmask, tmp_bitmask, new_value)) {
         return;
      }
   }
}

platform_status
task_register_hook(task_hook newhook)
{
   int my_hook_idx = __sync_fetch_and_add(&num_hooks, 1);
   if (my_hook_idx >= MAX_HOOKS) {
      return STATUS_LIMIT_EXCEEDED;
   }
   hooks[my_hook_idx] = newhook;

   return STATUS_OK;
}

// Register the calling thread and allocate scratch space for it
void
task_register_this_thread(task_system *ts, uint64 scratch_size)
{
   char *scratch = NULL;
   if (scratch_size > 0) {
      scratch = TYPED_ZALLOC_MANUAL(ts->heap_id, scratch, scratch_size);
   }
   task_run_thread_hooks(ts);
   ts->thread_scratch[platform_get_tid()] = scratch;
}

// Deregister the calling thread and free its scratch space
void
task_deregister_this_thread(task_system *ts)
{
   uint64 tid = platform_get_tid();

   void *scratch = ts->thread_scratch[tid];
   if (scratch != NULL) {
      platform_free(ts->heap_id, scratch);
      ts->thread_scratch[tid] = NULL;
   }
   task_clear_threadid(ts, tid); // allow thread id to be re-used
}

typedef struct {
   platform_thread_worker func;
   void                  *arg;

   task_system     *ts;
   uint64           scratch_size;
   platform_heap_id heap_id;
} thread_invoke;

static void
task_invoke_with_hooks(void *func_and_args)
{
   thread_invoke         *thread_to_start = (thread_invoke *)func_and_args;
   platform_thread_worker func            = thread_to_start->func;
   void                  *arg             = thread_to_start->arg;

   task_register_this_thread(thread_to_start->ts,
                             thread_to_start->scratch_size);

   func(arg);

   task_deregister_this_thread(thread_to_start->ts);

   platform_free(thread_to_start->heap_id, func_and_args);
}

platform_status
task_create_thread_with_hooks(platform_thread       *thread,
                              bool                   detached,
                              platform_thread_worker func,
                              void                  *arg,
                              size_t                 scratch_size,
                              task_system           *ts,
                              platform_heap_id       hid)
{
   platform_status ret;
   thread_invoke  *thread_to_create = TYPED_ZALLOC(hid, thread_to_create);
   if (thread_to_create == NULL) {
      return STATUS_NO_MEMORY;
   }

   thread_to_create->func         = func;
   thread_to_create->arg          = arg;
   thread_to_create->heap_id      = hid;
   thread_to_create->scratch_size = scratch_size;
   thread_to_create->ts           = ts;

   ret = platform_thread_create(
      thread, detached, task_invoke_with_hooks, thread_to_create, hid);
   if (!SUCCESS(ret)) {
      platform_free(hid, thread_to_create);
   }

   return ret;
}

platform_status
task_thread_create(const char            *name,
                   platform_thread_worker func,
                   void                  *arg,
                   size_t                 scratch_size,
                   task_system           *ts,
                   platform_heap_id       hid,
                   platform_thread       *thread)
{
   platform_thread thr;
   platform_status ret;

   ret = task_create_thread_with_hooks(
      &thr, FALSE, func, arg, scratch_size, ts, hid);
   if (!SUCCESS(ret)) {
      platform_error_log("could not create a thread");
      return ret;
   }

   if (thread != NULL) {
      *thread = thr;
   }
   return STATUS_OK;
}

/* Worker function for the background task pool. */
void
task_worker_thread(void *arg)
{
   task_group    *group = (task_group *)arg;
   const threadid tid   = platform_get_tid();
   task_queue    *tq    = &group->tq;
   task_system   *ts    = group->ts;

   while (TRUE) {
      task           *task_to_run = NULL;
      platform_status rc          = platform_condvar_lock(&group->bg.cv);
      platform_assert(SUCCESS(rc));
      if (tq->head == NULL) {
         // Queue is empty.
         if (group->bg.stop == TRUE) {
            // asked to exit.
            platform_condvar_unlock(&group->bg.cv);
            return;
         }
         // wait for tasks to be generated in the queue.
         rc = platform_condvar_wait(&group->bg.cv);
         platform_assert(SUCCESS(rc));
      }

      if (tq->head != NULL) {
         task_to_run = tq->head;
         tq->head    = tq->head->next;
         if (tq->head == NULL) {
            platform_assert(tq->tail == task_to_run);
            tq->tail = NULL;
         }
      }
      platform_condvar_unlock(&group->bg.cv);

      if (task_to_run != NULL) {
         task_stats *stats   = &group->stats[tid];
         timestamp   current = platform_get_timestamp();
         if (stats != NULL) {
            timestamp latency = current - task_to_run->enqueue_time;
            stats[tid].total_latency_ns += latency;
            stats[tid].total_tasks++;
            if (latency > stats[tid].max_latency_ns) {
               stats[tid].max_latency_ns = latency;
            }
         }

         // Run the task.
         task_to_run->func(task_to_run->arg,
                           task_system_get_thread_scratch(ts, tid));

         __sync_fetch_and_sub(&group->current_outstanding_tasks, 1);
         if (group->use_stats) {
            timestamp task_run_time = platform_timestamp_elapsed(current);
            if (task_run_time > stats[tid].max_runtime_ns) {
               stats[tid].max_runtime_ns   = task_run_time;
               stats[tid].max_runtime_func = task_to_run->func;
            }
         }
         platform_free(ts->heap_id, task_to_run);
      }
   }
}

static void
task_group_stop_and_wait_for_threads(task_group *group)
{
   bool use_bg_threads = task_system_use_bg_threads(group->ts);
   if (use_bg_threads) {
      platform_condvar_lock(&group->bg.cv);
   } else {
      platform_mutex_lock(&group->fg.mutex);
   }

   platform_assert(group->tq.head == NULL);
   platform_assert(group->tq.tail == NULL);
   platform_assert(group->current_outstanding_tasks == 0);

   if (!use_bg_threads) {
      return;
   }

   uint8 num_threads = group->bg.num_threads;

   group->bg.stop = TRUE;
   platform_condvar_broadcast(&group->bg.cv);
   platform_condvar_unlock(&group->bg.cv);

   for (uint8 i = 0; i < num_threads; i++) {
      platform_thread_join(group->bg.threads[i]);
   }
}

void
task_group_deinit(task_group *group)
{
   task_group_stop_and_wait_for_threads(group);
   if (task_system_use_bg_threads(group->ts)) {
      platform_condvar_destroy(&group->bg.cv);
   } else {
      platform_mutex_unlock(&group->fg.mutex);
      platform_mutex_destroy(&group->fg.mutex);
   }
}

static platform_status
task_group_init(task_group  *group,
                task_system *ts,
                bool         use_stats,
                bool         use_bg_threads,
                uint8        num_bg_threads,
                uint64       scratch_size)
{
   ZERO_CONTENTS(group);
   group->ts            = ts;
   group->use_stats     = use_stats;
   platform_heap_id hid = group->ts->heap_id;
   platform_status  rc;
   if (use_bg_threads) {
      group->bg.num_threads = num_bg_threads;

      rc = platform_condvar_init(&group->bg.cv, hid);
      if (!SUCCESS(rc)) {
         return rc;
      }

      for (uint8 i = 0; i < num_bg_threads; i++) {
         rc = task_thread_create("splinter-bg-thread",
                                 task_worker_thread,
                                 (void *)group,
                                 scratch_size,
                                 ts,
                                 hid,
                                 &group->bg.threads[i]);
         if (!SUCCESS(rc)) {
            task_group_stop_and_wait_for_threads(group);
            goto out;
         }
      }
   } else {
      rc = platform_mutex_init(&group->fg.mutex, 0, hid);
      if (!SUCCESS(rc)) {
         goto out;
      }
   }
   return STATUS_OK;

out:
   debug_assert(!SUCCESS(rc));
   if (use_bg_threads) {
      platform_condvar_destroy(&group->bg.cv);
   }
   return rc;
}

static inline platform_status
task_lock_task_queue(task_group *group)
{
   task_system *ts = group->ts;
   return task_system_use_bg_threads(ts)
             ? platform_condvar_lock(&group->bg.cv)
             : platform_mutex_lock(&group->fg.mutex);
}

static inline platform_status
task_unlock_task_queue(task_group *group)
{
   task_system *ts = group->ts;
   if (task_system_use_bg_threads(ts)) {
      // signal to the bg thread to pick up a task and unlock.
      platform_condvar_signal(&group->bg.cv);
      return platform_condvar_unlock(&group->bg.cv);
   } else {
      return platform_mutex_unlock(&group->fg.mutex);
   }
}

/* Adds one task to the task queue. */
platform_status
task_enqueue(task_system *ts,
             task_type    type,
             task_fn      func,
             void        *arg,
             bool         at_head)
{
   task *new_task = TYPED_ZALLOC(ts->heap_id, new_task);
   if (new_task == NULL) {
      return STATUS_NO_MEMORY;
   }
   new_task->func = func;
   new_task->arg  = arg;
   new_task->ts   = ts;

   task_group     *group = &ts->group[type];
   task_queue     *tq    = &group->tq;
   platform_status rc;

   rc = task_lock_task_queue(group);
   if (!SUCCESS(rc)) {
      platform_free(ts->heap_id, new_task);
      platform_assert(!ts->use_bg_threads || !group->bg.stop);
      return rc;
   }
   if (tq->tail) {
      if (at_head) {
         tq->head->prev = new_task;
         new_task->next = tq->head;
         tq->head       = new_task;
      } else {
         tq->tail->next = new_task;
         new_task->prev = tq->tail;
         tq->tail       = new_task;
      }
   } else {
      platform_assert(tq->head == NULL);
      tq->head = tq->tail = new_task;
   }

   __sync_fetch_and_add(&group->current_outstanding_tasks, 1);

   if (group->use_stats) {
      new_task->enqueue_time = platform_get_timestamp();
   }
   if (group->current_outstanding_tasks > group->max_outstanding_tasks) {
      group->max_outstanding_tasks = group->current_outstanding_tasks;
   }
   return task_unlock_task_queue(group);
}

/* Returns as soon as it finds the queue is empty */
void
task_perform_all(task_system *ts)
{
   if (!ts->use_bg_threads) {
      platform_status rc;
      do {
         rc = task_perform_one(ts);
         debug_assert(SUCCESS(rc) || STATUS_IS_EQ(rc, STATUS_TIMEDOUT));
      } while (STATUS_IS_NE(rc, STATUS_TIMEDOUT));

   } else {
      // wait for bg threads to finish running all the queued up tasks.
      for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
         task_group *group = &ts->group[type];
         while (group->current_outstanding_tasks != 0) {
            platform_sleep(USEC_TO_NSEC(100000)); // 100 msec.
         }
      }
   }
}

static void
task_queue_unlock(void *arg)
{
   task_group *group = (task_group *)arg;
   platform_assert(!task_system_use_bg_threads(group->ts));
   platform_mutex_unlock(&group->fg.mutex);
}

static inline platform_status
task_group_perform_one(task_group *group)
{
   platform_assert(!task_system_use_bg_threads(group->ts));
   platform_status rc;
   task           *assigned_task = NULL;
   task_queue     *tq            = &group->tq;
   if (group->current_outstanding_tasks == 0) {
      return STATUS_TIMEDOUT;
   }

   platform_thread_cleanup_push(task_queue_unlock, group);
   rc = platform_mutex_lock(&group->fg.mutex);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (group->current_outstanding_tasks == 0) {
      rc = STATUS_TIMEDOUT;
      goto out;
   }

   platform_assert(tq->head != NULL);
   platform_assert(tq->tail != NULL);

   __attribute__((unused)) uint64 outstanding_tasks =
      __sync_fetch_and_sub(&group->current_outstanding_tasks, 1);
   platform_assert(outstanding_tasks != 0);

   assigned_task = tq->head;
   tq->head      = tq->head->next;
   if (tq->head == NULL) {
      platform_assert(tq->tail == assigned_task);
      tq->tail = NULL;
      platform_assert(outstanding_tasks == 1);
   }

out:
   platform_mutex_unlock(&group->fg.mutex);
   platform_thread_cleanup_pop(0);

   if (assigned_task) {
      const threadid tid = platform_get_tid();
      timestamp      current;

      if (group->use_stats) {
         current = platform_get_timestamp();
      }
      assigned_task->func(assigned_task->arg,
                          task_system_get_thread_scratch(group->ts, tid));
      if (group->use_stats) {
         current = platform_timestamp_elapsed(current);
         if (current > group->stats[tid].max_runtime_ns) {
            group->stats[tid].max_runtime_ns   = current;
            group->stats[tid].max_runtime_func = assigned_task->func;
         }
      }
      platform_free(group->ts->heap_id, assigned_task);
      rc = STATUS_OK;
   }

   return rc;
}

/* Checks if there is a task to run and runs it.
   returns ETIMEDOUT if it didn't run any task;
 */
platform_status
task_perform_one(task_system *ts)
{
   platform_assert(!ts->use_bg_threads);
   platform_status rc = STATUS_OK;
   for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
      rc = task_group_perform_one(&ts->group[type]);
      if (STATUS_IS_NE(rc, STATUS_TIMEDOUT)) {
         return rc;
      }
   }
   return rc;
}

/*
 * -----------------------------------------------------------------------------
 * Task system initializer. Makes sure that the initial thread has an
 * adequately sized scratch.
 * Needs to be called at the beginning of the main thread that uses splinter,
 * similar to how __attribute__((constructor)) works.
 * -----------------------------------------------------------------------------
 */
platform_status
task_system_create(platform_heap_id    hid,
                   platform_io_handle *ioh,
                   task_system       **system,
                   bool                use_stats,
                   bool                use_bg_threads,
                   uint8               num_bg_threads[NUM_TASK_TYPES],
                   uint64              scratch_size)
{
   task_system *ts =
      TYPED_FLEXIBLE_STRUCT_ZALLOC(hid, ts, init_task_scratch, scratch_size);

   if (ts == NULL) {
      *system = NULL;
      return STATUS_NO_MEMORY;
   }
   ts->ioh = ioh;
   task_init_tid_bitmask(&ts->tid_bitmask);
   // task initialization
   register_init_tid_hook();

   ts->use_bg_threads = use_bg_threads;
   ts->heap_id        = hid;
   ts->scratch_size   = scratch_size;
   ts->init_tid       = INVALID_TID;

   for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
      platform_status rc = task_group_init(&ts->group[type],
                                           ts,
                                           use_stats,
                                           use_bg_threads,
                                           num_bg_threads[type],
                                           scratch_size);
      if (!SUCCESS(rc)) {
         platform_free(hid, ts);
         *system = NULL;
         return rc;
      }
   }

   task_run_thread_hooks(ts);
   const threadid tid      = platform_get_tid();
   ts->thread_scratch[tid] = ts->init_task_scratch;

   *system = ts;
   return STATUS_OK;
}

bool
task_system_use_bg_threads(task_system *ts)
{
   return ts->use_bg_threads;
}

void
task_system_io_register_thread(task_system *ts)
{
   io_thread_register(&ts->ioh->super);
}

void
task_system_destroy(platform_heap_id hid, task_system **ts_in)
{
   task_system *ts = *ts_in;
   for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
      task_group_deinit(&ts->group[type]);
   }
   platform_free(hid, ts);
   *ts_in = (task_system *)NULL;
}

uint64 *
task_system_get_tid_bitmask(task_system *ts)
{
   return &ts->tid_bitmask;
}

threadid *
task_system_get_max_tid(task_system *ts)
{
   return &ts->max_tid;
}

void *
task_system_get_thread_scratch(task_system *ts, const threadid tid)
{
   return ts->thread_scratch[tid];
}

void
task_wait_for_completion(task_system *ts)
{
   for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
      task_group *group             = &ts->group[type];
      uint64      outstanding_tasks = 0;
      while (group->current_outstanding_tasks != 0) {
         if (group->current_outstanding_tasks != outstanding_tasks) {
            platform_default_log("waiting for %lu tasks of type %d\n",
                                 group->current_outstanding_tasks,
                                 type);
            outstanding_tasks = group->current_outstanding_tasks;
         }
         platform_sleep(1000);
      }
   }
}

static void
task_group_print_stats(task_group *group, task_type type)
{
   if (!group->use_stats) {
      platform_default_log("no stats\n");
      return;
   }

   task_stats global = {0};

   for (threadid i = 0; i < MAX_THREADS; i++) {
      global.total_tasks += group->stats[i].total_tasks;
      global.total_latency_ns += group->stats[i].total_latency_ns;
      if (group->stats[i].max_runtime_ns > global.max_runtime_ns) {
         global.max_runtime_ns   = group->stats[i].max_runtime_ns;
         global.max_runtime_func = group->stats[i].max_runtime_func;
      }
      if (group->stats[i].max_latency_ns > global.max_latency_ns)
         global.max_latency_ns = group->stats[i].max_latency_ns;
   }

   switch (type) {
      case TASK_TYPE_NORMAL:
         platform_default_log("\nMain Task Group Statistics\n");
         break;
      case TASK_TYPE_MEMTABLE:
         platform_default_log("\nMemtable Task Group Statistics\n");
         break;
      default:
         platform_assert(0);
         break;
   }
   platform_default_log("--------------------------------\n");
   platform_default_log("| max runtime (ns)     : %10lu\n",
                        global.max_runtime_ns);
   platform_default_log("| max runtime func     : %10p\n",
                        global.max_runtime_func);
   platform_default_log("| total latency (ns)   : %10lu\n",
                        global.total_latency_ns);
   platform_default_log("| max latency (ns)     : %10lu\n",
                        global.max_latency_ns);
   platform_default_log("| total tasks run      : %10lu\n", global.total_tasks);
   platform_default_log("| current outstanding tasks : %lu\n",
                        group->current_outstanding_tasks);
   platform_default_log("| max outstanding tasks : %lu\n",
                        group->max_outstanding_tasks);
   platform_default_log("\n");
}

void
task_print_stats(task_system *ts)
{
   for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
      task_group_print_stats(&ts->group[type], type);
   }
}
