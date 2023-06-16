#if !defined(_GNU_SOURCE)
#   define _GNU_SOURCE
#endif

#include "lock.h"

#define NUM_COUNTERS 64

/**
 * Try to acquire a lock once and return even if the lock is busy.
 * If spin flag is set, then spin until the lock is available.
 */
bool
lock(volatile int *var, uint8_t flag)
{
   if (GET_WAIT_FOR_LOCK(flag) != WAIT_FOR_LOCK) {
      return !__sync_lock_test_and_set(var, 1);
   } else {
      while (__sync_lock_test_and_set(var, 1))
         while (var)
            ;
      return true;
   }

   return false;
}

void
unlock(volatile int *var)
{
   __sync_lock_release(var);
   return;
}

void
rw_lock_init(ReaderWriterLock *rwlock)
{
   rwlock->readers = 0;
   rwlock->writer  = 0;
   pc_init(&rwlock->pc_counter, &rwlock->readers, NUM_COUNTERS, NUM_COUNTERS);
}


#if 0
/**
* Try to acquire a lock and spin until the lock is available.
*/
bool read_lock(ReaderWriterLock *rwlock, uint8_t flag, uint8_t thread_id) {
  __sync_lock_test_and_set(&rwlock->pc_counter.local_counters[thread_id].counter, 1);

  while (rwlock->writer) {
    __sync_lock_release(&rwlock->pc_counter.local_counters[thread_id].counter);
    //rwlock->pc_counter.local_counters[thread_id].counter = 0;
    while (rwlock->writer) {
      __builtin_ia32_pause();
    }
    __sync_lock_test_and_set(&rwlock->pc_counter.local_counters[thread_id].counter, 1);
  }

  return true;
}

void read_unlock(ReaderWriterLock *rwlock, uint8_t thread_id) {
  __sync_lock_release(&rwlock->pc_counter.local_counters[thread_id].counter);
  //rwlock->pc_counter.local_counters[thread_id].counter = 0;
  return;
}
#else
/**
 * Try to acquire a lock and spin until the lock is available.
 */
bool
read_lock(ReaderWriterLock *rwlock, uint8_t flag, uint8_t thread_id)
{
   __atomic_add_fetch(&rwlock->pc_counter.local_counters[thread_id].counter,
                      1,
                      __ATOMIC_SEQ_CST);

   if (GET_WAIT_FOR_LOCK(flag) != WAIT_FOR_LOCK) {
      if (rwlock->writer) {
         __atomic_add_fetch(
            &rwlock->pc_counter.local_counters[thread_id].counter,
            -1,
            __ATOMIC_SEQ_CST);
         return false;
      }
      return true;
   }

   while (rwlock->writer) {
      __atomic_add_fetch(&rwlock->pc_counter.local_counters[thread_id].counter,
                         -1,
                         __ATOMIC_SEQ_CST);
      while (rwlock->writer)
         ;
      __atomic_add_fetch(&rwlock->pc_counter.local_counters[thread_id].counter,
                         1,
                         __ATOMIC_SEQ_CST);
   }

   return true;
}

void
read_unlock(ReaderWriterLock *rwlock, uint8_t thread_id)
{
   __atomic_add_fetch(&rwlock->pc_counter.local_counters[thread_id].counter,
                      -1,
                      __ATOMIC_SEQ_CST);
   return;
}
#endif

/**
 * Try to acquire a write lock and spin until the lock is available.
 * Then wait till reader count is 0.
 */
bool
write_lock(ReaderWriterLock *rwlock, uint8_t flag)
{
   // acquire write lock.
   if (GET_WAIT_FOR_LOCK(flag) != WAIT_FOR_LOCK) {
      if (__sync_lock_test_and_set(&rwlock->writer, 1))
         return false;
   } else {
      while (__sync_lock_test_and_set(&rwlock->writer, 1))
         while (rwlock->writer != 0)
            ;
   }
   // wait for readers to finish
   for (int i = 0; i < NUM_COUNTERS; i++)
      while (rwlock->pc_counter.local_counters[i].counter)
         ;

   return true;
}

void
write_unlock(ReaderWriterLock *rwlock)
{
   __sync_lock_release(&rwlock->writer);
   return;
}
