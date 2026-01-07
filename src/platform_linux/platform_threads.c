// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform_threads.h"
#include "splinterdb/platform_linux/public_platform.h"
#include "platform_log.h"
#include <sys/mman.h>
#include <string.h>

__thread threadid xxxtid = INVALID_TID;

// xxxpid is valid iff ospid == getpid()
threadid xxxpid;
pid_t    ospid;


/****************************************
 * Thread ID allocation and management  *
 ****************************************/

/*
 * bitmask used for allocating thread id's.
 * If a bit is set to 0, it means we have an in use thread id for that
 * particular position, 1 means it is unset and that thread id is available
 * for use.
 */
typedef struct threadid_allocator {
   volatile uint64 bitmask_lock;
   uint64          available_tids[(MAX_THREADS + 63) / 64];
   // number of threads allocated so far.
   threadid num_threads;
} threadid_allocator;

typedef struct pid_allocator {
   volatile uint64 lock;
   uint64          thread_count[MAX_THREADS];
} pid_allocator;

typedef struct thread_invocation {
   platform_thread_worker worker;
   void                  *arg;
} thread_invocation;

typedef struct id_allocator {
   threadid_allocator                tid_allocator;
   pid_allocator                     pid_allocator;
   volatile uint64                   process_event_callback_list_lock;
   process_event_callback_list_node *process_event_callback_list;
   thread_invocation                 thread_invocations[MAX_THREADS];
} id_allocator;

// This will be allocated in shared memory so that it is shared by all
// processes.
static id_allocator *id_alloc = NULL;

/*
 * task_init_tid_bitmask() - Initialize the global bitmask of active threads
 * in the task system structure to indicate that no threads are currently
 * active.
 */
static void
id_allocator_init_if_needed(void)
{
   if (id_alloc == NULL) {
      id_allocator *my_id_alloc = NULL;
      my_id_alloc               = mmap(NULL,
                         sizeof(id_allocator),
                         PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_ANONYMOUS,
                         -1,
                         0);
      if (my_id_alloc == MAP_FAILED) {
         platform_error_log("Failed to allocate memory for id allocator");
         return;
      }
      memset(my_id_alloc, 0x00, sizeof(id_allocator));
      memset(my_id_alloc->tid_allocator.available_tids,
             0xFF,
             sizeof(my_id_alloc->tid_allocator.available_tids));
      if (!__sync_bool_compare_and_swap(&id_alloc, NULL, my_id_alloc)) {
         munmap(my_id_alloc, sizeof(id_allocator));
      }
   }
}

/*
 * Allocate a threadid.  Returns INVALID_TID when no tid is available.
 */
static threadid
allocate_threadid()
{
   threadid tid         = INVALID_TID;
   uint64  *tid_bitmask = id_alloc->tid_allocator.available_tids;
   uint64   old_bitmask;
   uint64   new_bitmask;

   while (__sync_lock_test_and_set(&id_alloc->tid_allocator.bitmask_lock, 1)) {
      // spin
   }

   int    i;
   uint64 pos = 0;
   for (i = 0; pos == 0 && i < (MAX_THREADS + 63) / 64; i++) {
      old_bitmask = tid_bitmask[i];
      // first bit set to 1 starting from LSB.
      pos = __builtin_ffsl(old_bitmask);
   }

   if (pos == 0) {
      __sync_lock_release(&id_alloc->tid_allocator.bitmask_lock);
      platform_error_log("No thread id available");
      return INVALID_TID;
   }

   i--;

   // builtin_ffsl returns the position plus 1.
   tid = pos - 1;
   // set bit at that position to 0, indicating in use.
   new_bitmask = (old_bitmask & ~(1ULL << tid));
   int r       = __sync_bool_compare_and_swap(
      &id_alloc->tid_allocator.available_tids[i], old_bitmask, new_bitmask);
   platform_assert(r);

   __sync_lock_release(&id_alloc->tid_allocator.bitmask_lock);

   tid += i * 64;

   // Invariant: we have successfully allocated tid

   // atomically increment the num_threads.
   uint64 tmp = __sync_fetch_and_add(&id_alloc->tid_allocator.num_threads, 1);
   platform_assert(tmp < MAX_THREADS);

   return tid;
}

/*
 * De-registering a task frees up the thread's index so that it can be
 * re-used.
 */
static void
deallocate_threadid(threadid tid)
{
   uint64 *tid_bitmask = id_alloc->tid_allocator.available_tids;

   while (__sync_lock_test_and_set(&id_alloc->tid_allocator.bitmask_lock, 1)) {
      // spin
   }

   uint64 bitmask_val = tid_bitmask[tid / 64];

   // Ensure that caller is only clearing for a thread that's in-use.
   platform_assert(!(bitmask_val & (1ULL << (tid % 64))),
                   "Thread [%lu] is expected to be in-use. Bitmap: 0x%lx",
                   tid,
                   bitmask_val);

   // set bit back to 1 to indicate a free slot.
   uint64 tmp_bitmask = tid_bitmask[tid / 64];
   uint64 new_value   = tmp_bitmask | (1ULL << (tid % 64));
   while (!__sync_bool_compare_and_swap(
      tid_bitmask + tid / 64, tmp_bitmask, new_value))
   {
      tmp_bitmask = tid_bitmask[tid / 64];
      new_value   = tmp_bitmask | (1ULL << (tid % 64));
   }

   __sync_lock_release(&id_alloc->tid_allocator.bitmask_lock);

   // atomically decrement the num_threads.
   uint64 tmp = __sync_fetch_and_sub(&id_alloc->tid_allocator.num_threads, 1);
   platform_assert(0 < tmp);

   xxxtid = INVALID_TID;
}

static platform_status
ensure_xxxpid_is_setup(void)
{
   pid_t myospid = getpid();

   while (__sync_lock_test_and_set(&id_alloc->pid_allocator.lock, 1)) {
      // spin
   }

   if (myospid == ospid) {
      platform_assert(xxxpid < INVALID_TID);
      id_alloc->pid_allocator.thread_count[xxxpid]++;
      __sync_lock_release(&id_alloc->pid_allocator.lock);
      return STATUS_OK;
   }

   for (int i = 0; i < MAX_THREADS; i++) {
      if (id_alloc->pid_allocator.thread_count[i] == 0) {
         id_alloc->pid_allocator.thread_count[i] = 1;
         xxxpid                                  = i;
         ospid                                   = myospid;
         xxxtid                                  = INVALID_TID;

         break;
      }
   }

   __sync_lock_release(&id_alloc->pid_allocator.lock);

   if (ospid != myospid) {
      return STATUS_BUSY;
   }

   return STATUS_OK;
}

static void
decref_xxxpid(void)
{
   while (__sync_lock_test_and_set(&id_alloc->pid_allocator.lock, 1)) {
      // spin
   }

   platform_assert(xxxpid < INVALID_TID);

   id_alloc->pid_allocator.thread_count[xxxpid]--;
   if (id_alloc->pid_allocator.thread_count[xxxpid] == 0) {

      while (__sync_lock_test_and_set(
         &id_alloc->process_event_callback_list_lock, 1))
      {
         // spin
      }
      process_event_callback_list_node *current =
         id_alloc->process_event_callback_list;
      while (current != NULL) {
         current->termination(xxxpid, current->arg);
         current = current->next;
      }
      __sync_lock_release(&id_alloc->process_event_callback_list_lock);

      xxxpid = INVALID_TID;
      ospid  = 0;
   }

   __sync_lock_release(&id_alloc->pid_allocator.lock);
}

void
platform_linux_add_process_event_callback(
   process_event_callback_list_node *node)
{
   id_allocator_init_if_needed();
   while (
      __sync_lock_test_and_set(&id_alloc->process_event_callback_list_lock, 1))
   {
      // spin
   }
   node->next = id_alloc->process_event_callback_list;
   id_alloc->process_event_callback_list = node;
   __sync_lock_release(&id_alloc->process_event_callback_list_lock);
}

void
platform_linux_remove_process_event_callback(
   process_event_callback_list_node *node)
{
   while (
      __sync_lock_test_and_set(&id_alloc->process_event_callback_list_lock, 1))
   {
      // spin
   }

   process_event_callback_list_node *current =
      id_alloc->process_event_callback_list;
   process_event_callback_list_node *prev = NULL;
   while (current != NULL) {
      if (current == node) {
         if (prev == NULL) {
            id_alloc->process_event_callback_list = current->next;
         } else {
            prev->next = current->next;
         }
         break;
      }
      prev    = current;
      current = current->next;
   }

   __sync_lock_release(&id_alloc->process_event_callback_list_lock);
}

/******************************************************************************
 * Registering/deregistering threads.
 ******************************************************************************/

/*
 * platform_deregister_thread() - Deregister an active thread.
 *
 * Deregistration involves clearing the thread ID (index) for this thread.
 */
void
platform_deregister_thread()
{
   threadid tid = platform_get_tid();

   platform_assert(tid != INVALID_TID,
                   "Error! Attempt to deregister unregistered thread.\n");

   deallocate_threadid(tid);
   decref_xxxpid();
}

static void
thread_registration_cleanup_function(void *arg)
{
   if (xxxtid == INVALID_TID) {
      platform_error_log("Thread registration cleanup function called for "
                         "unregistered thread %lu",
                         xxxtid);
   } else {
      platform_deregister_thread();
   }
}


/*
 * platform_register_thread(): Register this new thread.
 *
 * Registration implies:
 *  - Acquire a new thread ID (index) for this to-be-active thread
 */
int
platform_register_thread(void)
{
   id_allocator_init_if_needed();

   platform_status status = ensure_xxxpid_is_setup();
   if (!SUCCESS(status)) {
      return -1;
   }

   threadid thread_tid = xxxtid;

   // Before registration, all SplinterDB threads' tid will be its default
   // value; i.e. INVALID_TID.
   platform_assert(thread_tid == INVALID_TID,
                   "Attempt to register thread that is already "
                   "registered as thread %lu\n",
                   thread_tid);

   thread_tid = allocate_threadid();
   // Unavailable threads is a temporary state that could go away.
   if (thread_tid == INVALID_TID) {
      decref_xxxpid();
      return -1;
   }

   platform_assert(thread_tid < MAX_THREADS);
   xxxtid = thread_tid;

   return 0;
}


/******************************************************************************
 * Thread creation and joining.
 ******************************************************************************/

static void *
thread_worker_function(void *arg)
{
   pthread_cleanup_push(thread_registration_cleanup_function, NULL);
   thread_invocation *thread_inv = (thread_invocation *)arg;
   threadid           tid        = thread_inv - id_alloc->thread_invocations;
   xxxtid                        = tid;
   thread_inv->worker(thread_inv->arg);
   pthread_cleanup_pop(1);
   return NULL;
}

/*
 * platform_thread_create() - External interface to create a Splinter thread.
 */
platform_status
platform_thread_create(platform_thread       *thread,
                       bool32                 detached,
                       platform_thread_worker worker,
                       void                  *arg,
                       platform_heap_id       heap_id)
{
   int ret;

   id_allocator_init_if_needed();
   platform_status rc = ensure_xxxpid_is_setup();
   if (!SUCCESS(rc)) {
      return rc;
   }

   // We allocate the threadid here, rather than in the thread_worker_function,
   // so that we can report an error if the threadid allocation fails.
   threadid tid = allocate_threadid();
   if (tid == INVALID_TID) {
      return STATUS_BUSY;
   }
   thread_invocation *thread_inv = &id_alloc->thread_invocations[tid];
   thread_inv->worker            = worker;
   thread_inv->arg               = arg;

   ret = pthread_create(thread, NULL, thread_worker_function, thread_inv);
   if (ret != 0) {
      deallocate_threadid(tid);
      decref_xxxpid();
      return STATUS_NO_MEMORY;
   }

   return CONST_STATUS(ret);
}

platform_status
platform_thread_join(platform_thread *thread)
{
   int   ret;
   void *retval;

   ret = pthread_join(*thread, &retval);

   return CONST_STATUS(ret);
}

threadid
platform_num_threads(void)
{
   id_allocator_init_if_needed();
   return id_alloc->tid_allocator.num_threads;
}
