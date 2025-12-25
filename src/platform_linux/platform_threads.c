#include "platform_threads.h"
#include "splinterdb/platform_linux/public_platform.h"
#include "platform_log.h"
#include <sys/mman.h>
#include <string.h>

__thread threadid xxxtid;

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
   uint64 bitmask_lock;
   uint64 available_tids[(MAX_THREADS + 63) / 64];
   // max thread id so far.
   threadid max_tid;
} threadid_allocator;

// This will be allocated in shared memory so that it is shared by all
// processes.
static threadid_allocator *tid_allocator = NULL;

/*
 * task_init_tid_bitmask() - Initialize the global bitmask of active threads
 * in the task system structure to indicate that no threads are currently
 * active.
 */
static void
tid_allocator_init_if_needed(void)
{
   if (tid_allocator == NULL) {
      tid_allocator = mmap(NULL,
                           sizeof(threadid_allocator),
                           PROT_READ | PROT_WRITE,
                           MAP_SHARED | MAP_ANONYMOUS,
                           -1,
                           0);
      if (tid_allocator == MAP_FAILED) {
         platform_error_log("Failed to allocate memory for threadid allocator");
         return;
      }
      memset(tid_allocator, 0x00, sizeof(threadid_allocator));
      int num_bitmasks = (MAX_THREADS + 63) / 64;

      for (int i = 0; i < num_bitmasks; i++) {
         tid_allocator->available_tids[i] = (uint64)-1;
      }
   }
}

/*
 * Allocate a threadid.  Returns INVALID_TID when no tid is available.
 */
static threadid
allocate_threadid()
{
   tid_allocator_init_if_needed();

   threadid tid         = INVALID_TID;
   uint64  *tid_bitmask = tid_allocator->available_tids;
   uint64   old_bitmask;
   uint64   new_bitmask;

   while (__sync_lock_test_and_set(&tid_allocator->bitmask_lock, 1)) {
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
      __sync_lock_release(&ts->tid_bitmask_lock);
      platform_error_log("No thread id available");
      return INVALID_TID;
   }

   i--;

   // builtin_ffsl returns the position plus 1.
   tid = pos - 1;
   // set bit at that position to 0, indicating in use.
   new_bitmask = (old_bitmask & ~(1ULL << tid));
   int r =
      __sync_bool_compare_and_swap(&tid_bitmask[i], old_bitmask, new_bitmask);
   platform_assert(r);

   __sync_lock_release(&tid_allocator->bitmask_lock);

   tid += i * 64;

   // Invariant: we have successfully allocated tid

   // atomically update the max_tid.
   threadid *max_tid = &tid_allocator->max_tid;
   threadid  tmp     = *max_tid;
   while (tmp < tid && !__sync_bool_compare_and_swap(max_tid, tmp, tid)) {
      tmp = *max_tid;
   }

   return tid;
}

/*
 * De-registering a task frees up the thread's index so that it can be
 * re-used.
 */
static void
deallocate_threadid(threadid tid)
{
   uint64 *tid_bitmask = tid_allocator->available_tids;

   while (__sync_lock_test_and_set(&tid_allocator->bitmask_lock, 1)) {
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

   __sync_lock_release(&tid_allocator->bitmask_lock);
}


/*
 * Return the max thread-index across all active tasks.
 * Mainly intended as a testing hook.
 */
threadid
platform_get_max_tid()
{
   tid_allocator_init_if_needed();
   return tid_allocator->max_tid + 1;
}


/******************************************************************************
 * Registering/deregistering threads.
 ******************************************************************************/

/*
 * platform_register_thread(): Register this new thread.
 *
 * Registration implies:
 *  - Acquire a new thread ID (index) for this to-be-active thread
 */
int
platform_register_thread(const char *file, const int lineno, const char *func)
{
   tid_allocator_init_if_needed();

   threadid thread_tid = xxxtid;

   thread_tid = xxxtid;

   // Before registration, all SplinterDB threads' tid will be its default
   // value; i.e. INVALID_TID. However, for the special case when we run
   // tests that simulate process-model of execution, we fork() from the main
   // test. Before registration, this 'thread' inherits the thread ID from
   // the main thread, which will be == 0.
   platform_assert(((thread_tid == INVALID_TID) || (thread_tid == 0)),
                   "[%s:%d::%s()] Attempt to register thread that is already "
                   "registered as thread %lu\n",
                   file,
                   lineno,
                   func,
                   thread_tid);

   thread_tid = platform_allocate_threadid();
   // Unavailable threads is a temporary state that could go away.
   if (thread_tid == INVALID_TID) {
      return -1;
   }

   platform_assert(thread_tid < MAX_THREADS);
   xxxtid = thread_tid;


   laio_register_thread(ioh);

   return 0;
}

/*
 * platform_deregister_thread() - Deregister an active thread.
 *
 * Deregistration involves clearing the thread ID (index) for this thread.
 */
void
platform_deregister_thread(const char *file, const int lineno, const char *func)
{
   threadid tid = platform_get_tid();

   platform_assert(
      tid != INVALID_TID,
      "[%s:%d::%s()] Error! Attempt to deregister unregistered thread.\n",
      file,
      lineno,
      func);

   laio_deregister_thread(ioh);
   platform_set_tid(INVALID_TID);
   process_deallocate_threadid(tid); // allow thread id to be re-used
}


/******************************************************************************
 * Thread creation and joining.
 ******************************************************************************/

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

   if (detached) {
      pthread_attr_t attr;
      pthread_attr_init(&attr);
      size_t stacksize = 16UL * 1024UL;
      pthread_attr_setstacksize(&attr, stacksize);
      pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
      ret = pthread_create(thread, &attr, (void *(*)(void *))worker, arg);
      pthread_attr_destroy(&attr);
   } else {
      ret = pthread_create(thread, NULL, (void *(*)(void *))worker, arg);
   }

   return CONST_STATUS(ret);
}

platform_status
platform_thread_join(platform_thread thread)
{
   int   ret;
   void *retval;

   ret = pthread_join(thread, &retval);

   return CONST_STATUS(ret);
}

platform_thread
platform_thread_id_self()
{
   return pthread_self();
}
