// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "task.h"
#include "platform_log.h"
#include "platform_time.h"
#include "platform_typed_alloc.h"
#include "platform_sleep.h"
#include "poison.h"

const char *task_type_name[] = {"TASK_TYPE_INVALID",
                                "TASK_TYPE_MEMTABLE",
                                "TASK_TYPE_NORMAL"};
_Static_assert((ARRAY_SIZE(task_type_name) == NUM_TASK_TYPES),
               "Array task_type_name[] is incorrectly sized.");

/****************************************
 * Background task management
 ****************************************/

static inline platform_status
task_group_lock(task_group *group)
{
   return platform_condvar_lock(&group->cv);
}

static inline platform_status
task_group_unlock(task_group *group)
{
   return platform_condvar_unlock(&group->cv);
}

/* Caller must hold lock on the group. */
static task *
task_group_get_next_task(task_group *group)
{
   task_queue *tq            = &group->tq;
   task       *assigned_task = NULL;
   if (group->current_waiting_tasks == 0) {
      return assigned_task;
   }

   platform_assert(tq->head != NULL);
   platform_assert(tq->tail != NULL);

   uint64 outstanding_tasks =
      __sync_fetch_and_sub(&group->current_waiting_tasks, 1);
   platform_assert(outstanding_tasks != 0);

   assigned_task = tq->head;
   tq->head      = tq->head->next;
   if (tq->head == NULL) {
      platform_assert(tq->tail == assigned_task);
      tq->tail = NULL;
      platform_assert((outstanding_tasks == 1),
                      "outstanding_tasks=%lu\n",
                      outstanding_tasks);
      ;
   }

   return assigned_task;
}

/*
 * Do not need to hold lock on the group. (And advisably should not
 * hold lock on group for performance reasons.)
 */
static platform_status
task_group_run_task(task_group *group, task *assigned_task)
{
   const threadid tid = platform_get_tid();
   timestamp      current;

   if (group->use_stats) {
      current                   = platform_get_timestamp();
      timestamp queue_wait_time = current - assigned_task->enqueue_time;
      group->stats[tid].total_queue_wait_time_ns += queue_wait_time;
      if (queue_wait_time > group->stats[tid].max_queue_wait_time_ns) {
         group->stats[tid].max_queue_wait_time_ns = queue_wait_time;
      }
   }

   task_fn func        = assigned_task->func;
   assigned_task->func = (task_fn)(void *)0xdeadbeef;
   func(assigned_task);

   if (group->use_stats) {
      current = platform_timestamp_elapsed(current);
      if (current > group->stats[tid].max_runtime_ns) {
         group->stats[tid].max_runtime_ns   = current;
         group->stats[tid].max_runtime_func = func;
      }
   }

   return STATUS_OK;
}

/*
 * task_worker_thread() - Worker function for the background task pool.
 *
 * This function is invoked when configured background threads are created.
 * We sit in an endless-loop looking for work to do and execute the tasks
 * enqueued.
 */
static void
task_worker_thread(void *arg)
{
   task_group *group = (task_group *)arg;

   platform_status rc = task_group_lock(group);
   platform_assert(SUCCESS(rc));

   while (group->bg.stop != TRUE) {
      /* Invariant: we hold the lock */
      task *task_to_run = NULL;
      task_to_run       = task_group_get_next_task(group);

      if (task_to_run != NULL) {
         __sync_fetch_and_add(&group->current_executing_tasks, 1);
         task_group_unlock(group);
         const threadid tid = platform_get_tid();
         group->stats[tid].total_bg_task_executions++;
         task_group_run_task(group, task_to_run);
         rc = task_group_lock(group);
         platform_assert(SUCCESS(rc));
         __sync_fetch_and_sub(&group->current_executing_tasks, 1);
      } else {
         rc = platform_condvar_wait(&group->cv);
         platform_assert(SUCCESS(rc));
      }
   }

   task_group_unlock(group);
}

/*
 * Function to terminate all background threads and clean up.
 */
static void
task_group_stop_and_wait_for_threads(task_group *group)
{
   task_group_lock(group);

   platform_assert(group->tq.head == NULL);
   platform_assert(group->tq.tail == NULL);
   platform_assert(group->current_waiting_tasks == 0,
                   "Attempt to shut down task group with %lu waiting tasks",
                   group->current_waiting_tasks);

   uint8 num_threads = group->bg.num_threads;

   // Inform the background thread that it's time to exit now.
   group->bg.stop = TRUE;
   platform_condvar_broadcast(&group->cv);
   task_group_unlock(group);

   // Allow all background threads to wrap up their work.
   for (uint8 i = 0; i < num_threads; i++) {
      platform_thread_join(&group->bg.threads[i]);
      group->bg.num_threads--;
   }
}

static void
task_group_deinit(task_group *group)
{
   task_group_stop_and_wait_for_threads(group);
   platform_condvar_destroy(&group->cv);
}

static platform_status
task_group_init(task_group  *group,
                task_system *ts,
                bool32       use_stats,
                uint8        num_bg_threads)
{
   ZERO_CONTENTS(group);
   group->ts            = ts;
   group->use_stats     = use_stats;
   platform_heap_id hid = ts->heap_id;
   platform_status  rc;

   rc = platform_condvar_init(&group->cv, hid);
   if (!SUCCESS(rc)) {
      return rc;
   }

   for (uint8 i = 0; i < num_bg_threads; i++) {
      rc = platform_thread_create(&group->bg.threads[i],
                                  FALSE,
                                  task_worker_thread,
                                  (void *)group,
                                  ts->heap_id);
      if (!SUCCESS(rc)) {
         task_group_stop_and_wait_for_threads(group);
         goto out;
      }
      group->bg.num_threads++;
   }
   return STATUS_OK;

out:
   debug_assert(!SUCCESS(rc));
   platform_condvar_destroy(&group->cv);
   return rc;
}

/*
 * task_enqueue() - Adds one task to the task queue.
 */
platform_status
task_enqueue(task_system *ts,
             task_type    type,
             task        *new_task,
             task_fn      func,
             bool32       at_head)
{
   memset(new_task, 0, sizeof(*new_task));
   new_task->func = func;
   new_task->ts   = ts;

   task_group     *group = &ts->group[type];
   task_queue     *tq    = &group->tq;
   platform_status rc;

   rc = task_group_lock(group);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (tq->tail) {
      if (at_head) {
         new_task->next = tq->head;
         tq->head       = new_task;
      } else {
         tq->tail->next = new_task;
         tq->tail       = new_task;
      }
   } else {
      platform_assert(tq->head == NULL);
      tq->head = tq->tail = new_task;
   }

   __sync_fetch_and_add(&group->current_waiting_tasks, 1);

   if (group->use_stats) {
      new_task->enqueue_time = platform_get_timestamp();
      const threadid tid     = platform_get_tid();
      if (group->current_waiting_tasks
          > group->stats[tid].max_outstanding_tasks)
      {
         group->stats[tid].max_outstanding_tasks = group->current_waiting_tasks;
      }
      group->stats[tid].total_tasks_enqueued += 1;
   }
   platform_condvar_signal(&group->cv);
   return task_group_unlock(group);
}

/*
 * Run a task if the number of waiting tasks is at least
 * queue_scale_percent of the number of background threads for that
 * group.
 */
static platform_status
task_group_perform_one(task_group *group, uint64 queue_scale_percent)
{
   platform_status rc;
   task           *assigned_task = NULL;

   /* We do the queue size comparison in this round-about way to avoid
      integer overflow. */
   if (queue_scale_percent
       && 100 * group->current_waiting_tasks / queue_scale_percent
             < group->bg.num_threads)
   {
      return STATUS_TIMEDOUT;
   }

   rc = task_group_lock(group);
   if (!SUCCESS(rc)) {
      return rc;
   }

   assigned_task = task_group_get_next_task(group);

   /*
    * It is important to update the current_executing_tasks while
    * holding the lock. The reason is that, if we release the lock
    * before updating current_executing_tasks, then another thread
    * might observe that both the number of enqueued tasks and the
    * number of executing tasks are both 0, and hence that the system
    * is quiescent, even though it is not.
    */
   if (assigned_task) {
      __sync_fetch_and_add(&group->current_executing_tasks, 1);
   }

   task_group_unlock(group);

   if (assigned_task) {
      const threadid tid = platform_get_tid();
      group->stats[tid].total_fg_task_executions++;
      task_group_run_task(group, assigned_task);
      __sync_fetch_and_sub(&group->current_executing_tasks, 1);
   } else {
      rc = STATUS_TIMEDOUT;
   }

   return rc;
}

/*
 * Perform a task only if there are more waiting tasks than
 * queue_scale_percent * num bg threads.
 */
platform_status
task_perform_one_if_needed(task_system *ts, uint64 queue_scale_percent)
{
   platform_status rc = STATUS_OK;
   for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
      rc = task_group_perform_one(&ts->group[type], queue_scale_percent);
      /* STATUS_TIMEDOUT means no task was waiting. */
      if (STATUS_IS_NE(rc, STATUS_TIMEDOUT)) {
         return rc;
      }
   }
   return rc;
}

void
task_perform_all(task_system *ts)
{
   platform_status rc;
   do {
      rc = task_perform_one(ts);
      debug_assert(SUCCESS(rc) || STATUS_IS_EQ(rc, STATUS_TIMEDOUT));
      /* STATUS_TIMEDOUT means no task was waiting. */
   } while (STATUS_IS_NE(rc, STATUS_TIMEDOUT));
}

bool32
task_system_is_quiescent(task_system *ts)
{
   platform_status rc;
   task_type       ttlocked;
   bool32          result = FALSE;

   for (ttlocked = TASK_TYPE_FIRST; ttlocked < NUM_TASK_TYPES; ttlocked++) {
      rc = task_group_lock(&ts->group[ttlocked]);
      if (!SUCCESS(rc)) {
         goto cleanup;
      }
   }

   for (task_type ttchecked = TASK_TYPE_FIRST; ttchecked < NUM_TASK_TYPES;
        ttchecked++)
   {
      if (ts->group[ttchecked].current_executing_tasks
          || ts->group[ttchecked].current_waiting_tasks)
      {
         goto cleanup;
      }
   }
   result = TRUE;

cleanup:
   for (task_type tt = TASK_TYPE_FIRST; tt < ttlocked; tt++) {
      rc = task_group_unlock(&ts->group[tt]);
      debug_assert(SUCCESS(rc));
   }

   return result;
}

platform_status
task_perform_until_quiescent(task_system *ts)
{
   uint64 wait = 1;
   while (!task_system_is_quiescent(ts)) {
      platform_status rc = task_perform_one(ts);
      if (SUCCESS(rc)) {
         wait = 1;
      } else if (STATUS_IS_EQ(rc, STATUS_TIMEDOUT)) {
         platform_sleep_ns(wait);
         wait = MIN(2 * wait, 1 << 16);
      } else {
         return rc;
      }
   }
   return STATUS_OK;
}

/*
 * Validate that the task system configuration is basically supportable.
 */
static platform_status
task_config_valid(const uint64 num_background_threads[NUM_TASK_TYPES])
{
   uint64 normal_bg_threads   = num_background_threads[TASK_TYPE_NORMAL];
   uint64 memtable_bg_threads = num_background_threads[TASK_TYPE_MEMTABLE];

   if ((normal_bg_threads + memtable_bg_threads) >= MAX_THREADS) {
      platform_error_log("Total number of background threads configured"
                         ", normal_bg_threads=%lu, memtable_bg_threads=%lu, "
                         "must be <= %d.\n",
                         normal_bg_threads,
                         memtable_bg_threads,
                         (MAX_THREADS - 1));
      return STATUS_BAD_PARAM;
   }
   return STATUS_OK;
}

platform_status
task_system_config_init(task_system_config *task_cfg,
                        bool32              use_stats,
                        const uint64        num_bg_threads[NUM_TASK_TYPES])
{
   platform_status rc = task_config_valid(num_bg_threads);
   if (!SUCCESS(rc)) {
      return rc;
   }

   task_cfg->use_stats = use_stats;

   memcpy(task_cfg->num_background_threads,
          num_bg_threads,
          NUM_TASK_TYPES * sizeof(num_bg_threads[0]));
   return STATUS_OK;
}

/*
 * -----------------------------------------------------------------------------
 * Task system initializer.
 *
 * Needs to be called at the beginning of the main thread that uses splinter,
 * similar to how __attribute__((constructor)) works.
 * -----------------------------------------------------------------------------
 */
platform_status
task_system_init(task_system              *ts,
                 platform_heap_id          hid,
                 const task_system_config *cfg)
{
   platform_status rc = task_config_valid(cfg->num_background_threads);
   if (!SUCCESS(rc)) {
      return rc;
   }

   ZERO_CONTENTS(ts);
   ts->cfg     = cfg;
   ts->heap_id = hid;

   for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
      platform_status rc = task_group_init(&ts->group[type],
                                           ts,
                                           cfg->use_stats,
                                           cfg->num_background_threads[type]);
      if (!SUCCESS(rc)) {
         task_system_deinit(ts);
         return rc;
      }
      uint64 nbg_threads = cfg->num_background_threads[type];
      if (nbg_threads) {
         platform_default_log("Splinter task system created %lu"
                              " background thread%sof type '%s'.\n",
                              nbg_threads,
                              ((nbg_threads > 1) ? "s " : " "),
                              task_type_name[type]);
      }
   }
   return STATUS_OK;
}

/*
 * -----------------------------------------------------------------------------
 * task_system_deinit() : Task system de-initializer.
 *
 * Tear down task system structures.
 * -----------------------------------------------------------------------------
 */
void
task_system_deinit(task_system *ts)
{
   for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
      task_group_deinit(&ts->group[type]);
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
      global.total_bg_task_executions +=
         group->stats[i].total_bg_task_executions;
      global.total_fg_task_executions +=
         group->stats[i].total_fg_task_executions;
      global.total_queue_wait_time_ns +=
         group->stats[i].total_queue_wait_time_ns;
      if (group->stats[i].max_runtime_ns > global.max_runtime_ns) {
         global.max_runtime_ns   = group->stats[i].max_runtime_ns;
         global.max_runtime_func = group->stats[i].max_runtime_func;
      }
      if (group->stats[i].max_queue_wait_time_ns
          > global.max_queue_wait_time_ns)
      {
         global.max_queue_wait_time_ns = group->stats[i].max_queue_wait_time_ns;
      }
      global.max_outstanding_tasks = MAX(global.max_outstanding_tasks,
                                         group->stats[i].max_outstanding_tasks);
      global.total_tasks_enqueued += group->stats[i].total_tasks_enqueued;
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
   platform_default_log("| total queue_wait_time (ns)   : %10lu\n",
                        global.total_queue_wait_time_ns);
   platform_default_log("| max queue_wait_time (ns)     : %10lu\n",
                        global.max_queue_wait_time_ns);

   uint64 nbytes = (global.total_tasks_enqueued * sizeof(task));
   platform_default_log("| total tasks enqueued : %lu consumed=%lu bytes (%s) "
                        "of memory\n",
                        global.total_tasks_enqueued,
                        nbytes,
                        size_str(nbytes));

   platform_default_log("| total bg tasks run      : %10lu\n",
                        global.total_bg_task_executions);
   platform_default_log("| total fg tasks run      : %10lu\n",
                        global.total_fg_task_executions);
   platform_default_log("| current waiting tasks : %lu\n",
                        group->current_waiting_tasks);
   platform_default_log("| max outstanding tasks : %lu\n",
                        global.max_outstanding_tasks);
   platform_default_log("\n");
}

void
task_print_stats(task_system *ts)
{
   for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
      task_group_print_stats(&ts->group[type], type);
   }
}
