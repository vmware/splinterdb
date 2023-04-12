// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform.h"
#include "task.h"
#include "util.h"

#include "poison.h"

const char *task_type_name[] = {"TASK_TYPE_INVALID",
                                "TASK_TYPE_MEMTABLE",
                                "TASK_TYPE_NORMAL"};
_Static_assert((ARRAY_SIZE(task_type_name) == NUM_TASK_TYPES),
               "Array task_type_name[] is incorrectly sized.");

/****************************************
 * Thread ID allocation and management  *
 ****************************************/
/*
 * task_init_tid_bitmask() - Initialize the global bitmask of active threads in
 * the task system structure to indicate that no threads are currently active.
 */
static void
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

static inline uint64 *
task_system_get_tid_bitmask(task_system *ts)
{
   return &ts->tid_bitmask;
}

static threadid *
task_system_get_max_tid(task_system *ts)
{
   return &ts->max_tid;
}

/*
 * Return the bitmasks of tasks active. Mainly intended as a testing hook.
 */
uint64
task_active_tasks_mask(task_system *ts)
{
   return *task_system_get_tid_bitmask(ts);
}

/*
 * Allocate a threadid.  Returns INVALID_TID when no tid is available.
 */
static threadid
task_allocate_threadid(task_system *ts)
{
   threadid tid         = INVALID_TID;
   uint64  *tid_bitmask = task_system_get_tid_bitmask(ts);
   uint64   old_bitmask;
   uint64   new_bitmask;

   do {
      old_bitmask = *tid_bitmask;
      // first bit set to 1 starting from LSB.
      uint64 pos = __builtin_ffsl(old_bitmask);

      // If all threads are in-use, bitmask will be all 0s.
      if (pos == 0) {
         return INVALID_TID;
      }

      // builtin_ffsl returns the position plus 1.
      tid = pos - 1;
      // set bit at that position to 0, indicating in use.
      new_bitmask = (old_bitmask & ~(1ULL << (pos - 1)));
   } while (
      !__sync_bool_compare_and_swap(tid_bitmask, old_bitmask, new_bitmask));

   // Invariant: we have successfully allocated tid

   // atomically update the max_tid.
   threadid *max_tid = task_system_get_max_tid(ts);
   threadid  tmp     = *max_tid;
   while (tmp < tid && !__sync_bool_compare_and_swap(max_tid, tmp, tid)) {
      tmp = *max_tid;
   }

   return tid;
}

/*
 * De-registering a task frees up the thread's index so that it can be re-used.
 */
static void
task_deallocate_threadid(task_system *ts, threadid tid)
{
   uint64 *tid_bitmask = task_system_get_tid_bitmask(ts);

   uint64 bitmask_val = *tid_bitmask;

   // Ensure that caller is only clearing for a thread that's in-use.
   platform_assert(!(bitmask_val & (1ULL << tid)),
                   "Thread [%lu] is expected to be in-use. Bitmap: 0x%lx",
                   tid,
                   bitmask_val);

   // set bit back to 1 to indicate a free slot.
   uint64 tmp_bitmask = *tid_bitmask;
   uint64 new_value   = tmp_bitmask | (1ULL << tid);
   while (!__sync_bool_compare_and_swap(tid_bitmask, tmp_bitmask, new_value)) {
      tmp_bitmask = *tid_bitmask;
      new_value   = tmp_bitmask | (1ULL << tid);
   }
}


/*
 * Return the max thread-index across all active tasks.
 * Mainly intended as a testing hook.
 */
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

/****************************************
 * Thread creation hooks                *
 ****************************************/

static platform_status
task_register_hook(task_system *ts, task_hook newhook)
{
   int my_hook_idx = __sync_fetch_and_add(&ts->num_hooks, 1);
   if (my_hook_idx >= TASK_MAX_HOOKS) {
      return STATUS_LIMIT_EXCEEDED;
   }
   ts->hooks[my_hook_idx] = newhook;

   return STATUS_OK;
}

static void
task_system_io_register_thread(task_system *ts)
{
   io_register_thread(&ts->ioh->super);
}

static void
task_system_io_deregister_thread(task_system *ts)
{
   io_deregister_thread(&ts->ioh->super);
}

/*
 * This is part of task initialization and needs to be called at the
 * beginning of the main thread that uses the task, similar to how
 * __attribute__((constructor)) works.
 */
static void
register_standard_hooks(task_system *ts)
{
   // hooks need to be initialized only once.
   if (__sync_fetch_and_add(&ts->hook_init_done, 1) == 0) {
      task_register_hook(ts, task_system_io_register_thread);
   }
}

static void
task_run_thread_hooks(task_system *ts)
{
   for (int i = 0; i < ts->num_hooks; i++) {
      ts->hooks[i](ts);
   }
}

/****************************************
 * Thread creation                      *
 ****************************************/

typedef struct {
   platform_thread_worker func;
   void                  *arg;

   task_system     *ts;
   threadid         tid;
   platform_heap_id heap_id;
} thread_invoke;

/*
 * task_invoke_with_hooks() - Single interface to invoke a user-specified
 * call-back function, 'func', to perform Splinter work. Both user-threads'
 * and background-threads' creation goes through this interface.
 *
 * A thread has been created with this function as the worker function. Also,
 * the thread-creator has registered another call-back function to execute.
 * This function invokes that call-back function bracketted by calls to
 * register / deregister the thread with Splinter's task system. This allows
 * the call-back function 'func' to now call Splinter APIs to do diff things.
 */
static void
task_invoke_with_hooks(void *func_and_args)
{
   thread_invoke         *thread_started = (thread_invoke *)func_and_args;
   platform_thread_worker func           = thread_started->func;
   void                  *arg            = thread_started->arg;

   platform_set_tid(thread_started->tid);

   task_run_thread_hooks(thread_started->ts);

   // Execute the user-provided call-back function which is where
   // the actual Splinter work will be done.
   func(arg);

   // Release scratch space, release thread-ID
   // For background threads', also, IO-deregistration will happen here.
   task_deregister_this_thread(thread_started->ts);

   platform_free(thread_started->heap_id, thread_started);
}

/*
 * task_create_thread_with_hooks() - Creates a thread to execute func
 * with argument 'arg'.
 */
static platform_status
task_create_thread_with_hooks(platform_thread       *thread,
                              bool                   detached,
                              platform_thread_worker func,
                              void                  *arg,
                              size_t                 scratch_size,
                              task_system           *ts,
                              platform_heap_id       hid)
{
   platform_status ret;

   threadid newtid = task_allocate_threadid(ts);
   if (newtid == INVALID_TID) {
      platform_error_log("Cannot create a new thread as the limit on"
                         " concurrent threads, %d, will be exceeded.\n",
                         MAX_THREADS);
      return STATUS_BUSY;
   }

   platform_memfrag  memfrag_scratch = {0};
   platform_memfrag *mf              = &memfrag_scratch;
   if (0 < scratch_size) {
      char *scratch = TYPED_ARRAY_MALLOC(ts->heap_id, scratch, scratch_size);
      if (scratch == NULL) {
         ret = STATUS_NO_MEMORY;
         goto dealloc_tid;
      }
      ts->thread_scratch[newtid] = scratch;
   }

   thread_invoke *thread_to_create = TYPED_ZALLOC(hid, thread_to_create);
   if (thread_to_create == NULL) {
      ret = STATUS_NO_MEMORY;
      goto free_scratch;
   }

   thread_to_create->func    = func;
   thread_to_create->arg     = arg;
   thread_to_create->heap_id = hid;
   thread_to_create->ts      = ts;
   thread_to_create->tid     = newtid;

   ret = platform_thread_create(
      thread, detached, task_invoke_with_hooks, thread_to_create, hid);
   if (!SUCCESS(ret)) {
      goto free_thread;
   }

   return ret;

free_thread:
   platform_free(hid, thread_to_create);
free_scratch:
   platform_free(ts->heap_id, mf);
dealloc_tid:
   task_deallocate_threadid(ts, newtid);
   return ret;
}

/*
 * task_thread_create() - Create a new task and to do the required
 * registration with Splinter.
 */
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
      platform_error_log("Could not create a thread: %s\n",
                         platform_status_to_string(ret));
      return ret;
   }

   if (thread != NULL) {
      *thread = thr;
   }
   return STATUS_OK;
}

/******************************************************************************
 * Registering/deregistering threads that were not started by the task
 * system.
 ******************************************************************************/

/*
 * task_register_thread(): Register this new thread with the task system.
 *
 * This is for use by threads created outside of the splinter system.
 *
 * Registration implies:
 *  - Acquire a new thread ID (index) for this to-be-active thread
 *  - Allocate scratch space for this thread, and track this space..
 */
platform_status
task_register_thread(task_system *ts,
                     uint64       scratch_size,
                     const char  *file,
                     const int    lineno,
                     const char  *func)
{
   threadid thread_tid;

   thread_tid = platform_get_tid();

   // Before registration, all SplinterDB threads' tid will be its default
   // value; i.e. INVALID_TID. However, for the special case when we run tests
   // that simulate process-model of execution, we fork() from the main test.
   // Before registration, this 'thread' inherits the thread ID from the main
   // thread, which will be == 0.
   platform_assert(((thread_tid == INVALID_TID) || (thread_tid == 0)),
                   "[%s:%d::%s()] Attempt to register thread that is already "
                   "registered as thread %lu\n",
                   file,
                   lineno,
                   func,
                   thread_tid);

   thread_tid = task_allocate_threadid(ts);
   // Unavailable threads is a temporary state that could go away.
   if (thread_tid == INVALID_TID) {
      return STATUS_BUSY;
   }

   platform_assert(ts->thread_scratch[thread_tid] == NULL,
                   "Scratch space should not yet exist for tid %lu.",
                   thread_tid);

   platform_memfrag memfrag_scratch = {0};
   if (0 < scratch_size) {
      char *scratch = TYPED_ARRAY_ZALLOC(ts->heap_id, scratch, scratch_size);
      if (scratch == NULL) {
         task_deallocate_threadid(ts, thread_tid);
         return STATUS_NO_MEMORY;
      }
      ts->thread_scratch[thread_tid] = scratch;
      ts->thread_scratch_mem_size    = memfrag_size(&memfrag_scratch);
   }

   platform_set_tid(thread_tid);
   task_run_thread_hooks(ts);

   return STATUS_OK;
}

/*
 * task_deregister_thread() - Deregister an active thread from the task system.
 *
 * Deregistration involves:
 *  - Releasing any scratch space acquired for this thread.
 *  - De-registering w/ IO sub-system, which will release IO resources
 *  - Clearing the thread ID (index) for this thread
 */
void
task_deregister_thread(task_system *ts,
                       const char  *file,
                       const int    lineno,
                       const char  *func)
{
   threadid tid = platform_get_tid();

   platform_assert(
      tid != INVALID_TID,
      "[%s:%d::%s()] Error! Attempt to deregister unregistered thread.\n",
      file,
      lineno,
      func);

   void *scratch = ts->thread_scratch[tid];
   if (scratch != NULL) {
      platform_memfrag  memfrag = {.addr = scratch,
                                   .size = ts->thread_scratch_mem_size};
      platform_memfrag *mf      = &memfrag;
      platform_free(ts->heap_id, mf);
      ts->thread_scratch[tid] = NULL;
   }

   task_system_io_deregister_thread(ts);
   platform_set_tid(INVALID_TID);
   task_deallocate_threadid(ts, tid); // allow thread id to be re-used
}

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

   assigned_task->func(assigned_task->arg,
                       task_system_get_thread_scratch(group->ts, tid));

   if (group->use_stats) {
      current = platform_timestamp_elapsed(current);
      if (current > group->stats[tid].max_runtime_ns) {
         group->stats[tid].max_runtime_ns   = current;
         group->stats[tid].max_runtime_func = assigned_task->func;
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
         platform_free(group->ts->heap_id, task_to_run);
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
      platform_thread_join(group->bg.threads[i]);
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
                bool         use_stats,
                uint8        num_bg_threads,
                uint64       scratch_size)
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

   rc = task_group_lock(group);
   if (!SUCCESS(rc)) {
      platform_free(ts->heap_id, new_task);
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

   __sync_fetch_and_add(&group->current_waiting_tasks, 1);
   __sync_fetch_and_add(&ts->ntasks_enqueued, 1);

   if (group->use_stats) {
      new_task->enqueue_time = platform_get_timestamp();
   }
   if (group->use_stats) {
      const threadid tid = platform_get_tid();
      if (group->current_waiting_tasks
          > group->stats[tid].max_outstanding_tasks) {
         group->stats[tid].max_outstanding_tasks = group->current_waiting_tasks;
      }
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
      platform_free(group->ts->heap_id, assigned_task);
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

bool
task_system_is_quiescent(task_system *ts)
{
   platform_status rc;
   task_type       ttlocked;
   bool            result = FALSE;

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
                        bool                use_stats,
                        const uint64        num_bg_threads[NUM_TASK_TYPES],
                        uint64              scratch_size)
{
   platform_status rc = task_config_valid(num_bg_threads);
   if (!SUCCESS(rc)) {
      return rc;
   }

   task_cfg->use_stats    = use_stats;
   task_cfg->scratch_size = scratch_size;

   memcpy(task_cfg->num_background_threads,
          num_bg_threads,
          NUM_TASK_TYPES * sizeof(num_bg_threads[0]));
   return STATUS_OK;
}

/*
 * -----------------------------------------------------------------------------
 * Task system initializer. Makes sure that the initial thread has an
 * adequately sized scratch space.
 *
 * Needs to be called at the beginning of the main thread that uses splinter,
 * similar to how __attribute__((constructor)) works.
 * -----------------------------------------------------------------------------
 */
platform_status
task_system_create(platform_heap_id          hid,
                   platform_io_handle       *ioh,
                   task_system             **system,
                   const task_system_config *cfg)
{
   platform_status rc = task_config_valid(cfg->num_background_threads);
   if (!SUCCESS(rc)) {
      return rc;
   }

   task_system *ts = TYPED_ZALLOC(hid, ts);
   if (ts == NULL) {
      *system = NULL;
      return STATUS_NO_MEMORY;
   }
   ts->cfg     = cfg;
   ts->ioh     = ioh;
   ts->heap_id = hid;
   task_init_tid_bitmask(&ts->tid_bitmask);

   // task initialization
   register_standard_hooks(ts);

   // Ensure that the main thread gets registered and init'ed first before
   // any background threads are created. (Those may grab their own tids.).
   task_register_this_thread(ts, cfg->scratch_size);

   for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
      platform_status rc = task_group_init(&ts->group[type],
                                           ts,
                                           cfg->use_stats,
                                           cfg->num_background_threads[type],
                                           cfg->scratch_size);
      if (!SUCCESS(rc)) {
         task_deregister_this_thread(ts);
         task_system_destroy(hid, &ts);
         *system = NULL;
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
   debug_assert((*system == NULL),
                "Task system handle, %p, is expected to be NULL.\n",
                *system);
   *system = ts;
   return STATUS_OK;
}

/*
 * -----------------------------------------------------------------------------
 * task_system_destroy() : Task system de-initializer.
 *
 * Tear down task system structures, free allocated memory.
 * -----------------------------------------------------------------------------
 */
void
task_system_destroy(platform_heap_id hid, task_system **ts_in)
{
   task_system *ts = *ts_in;
   for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
      task_group_deinit(&ts->group[type]);
   }
   threadid tid = platform_get_tid();
   if (tid != INVALID_TID) {
      task_deregister_this_thread(ts);
   }
   if (ts->tid_bitmask != ((uint64)-1)) {
      platform_error_log(
         "Destroying task system that still has some registered threads."
         ", tid=%lu, tid_bitmask=0x%lx\n",
         tid,
         ts->tid_bitmask);
   }
   platform_free(hid, ts);
   *ts_in = (task_system *)NULL;
}

void *
task_system_get_thread_scratch(task_system *ts, const threadid tid)
{
   platform_assert((tid < MAX_THREADS), "tid=%lu", tid);
   return ts->thread_scratch[tid];
}

void
task_wait_for_completion(task_system *ts)
{
   for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
      task_group *group             = &ts->group[type];
      uint64      outstanding_tasks = 0;
      while (group->current_waiting_tasks != 0) {
         if (group->current_waiting_tasks != outstanding_tasks) {
            platform_default_log("waiting for %lu tasks of type %d\n",
                                 group->current_waiting_tasks,
                                 type);
            outstanding_tasks = group->current_waiting_tasks;
         }
         platform_sleep_ns(1000);
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
          > global.max_queue_wait_time_ns) {
         global.max_queue_wait_time_ns = group->stats[i].max_queue_wait_time_ns;
      }
      global.max_outstanding_tasks = MAX(global.max_outstanding_tasks,
                                         group->stats[i].max_outstanding_tasks);
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
   uint64 nbytes = (ts->ntasks_enqueued * sizeof(task));
   char   nbytes_str[SIZE_TO_STR_LEN];
   size_to_str(nbytes_str, sizeof(nbytes_str), nbytes);
   platform_default_log(
      "Number of tasks enqueued=%lu, consumed=%lu bytes (%s) of memory.\n",
      ts->ntasks_enqueued,
      nbytes,
      nbytes_str);

   for (task_type type = TASK_TYPE_FIRST; type != NUM_TASK_TYPES; type++) {
      task_group_print_stats(&ts->group[type], type);
   }
}
