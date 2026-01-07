// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "batch_rwlock.h"
#include "platform_sleep.h"
#include "poison.h"

/*
 *-----------------------------------------------------------------------------
 * Batch Read/Write Locks
 *
 * These are generic distributed reader/writer locks with an intermediate claim
 * state. These offer similar semantics to the locks in the clockcache, but in
 * these locks read locks cannot be held with write locks.
 *
 * These locks are allocated in batches of PLATFORM_CACHELINE_SIZE / 2, so that
 * the write lock and claim bits, as well as the distributed read counters, can
 * be colocated across the batch.
 *-----------------------------------------------------------------------------
 */

void
batch_rwlock_init(batch_rwlock *lock)
{
   ZERO_CONTENTS(lock);
}

void
batch_rwlock_deinit(batch_rwlock *lock)
{
   ZERO_CONTENTS(lock);
}

/*
 *-----------------------------------------------------------------------------
 * lock/unlock
 *
 * Drains and blocks all gets (read locks)
 *
 * Caller must hold a claim
 * Caller cannot hold a get (read lock)
 *-----------------------------------------------------------------------------
 */

void
batch_rwlock_lock(batch_rwlock *lock, uint64 lock_idx)
{
   platform_assert(lock->write_lock[lock_idx].claim);
   debug_only uint8 was_locked =
      __sync_lock_test_and_set(&lock->write_lock[lock_idx].lock, 1);
   debug_assert(!was_locked,
                "batch_rwlock_lock: Attempt to lock a locked page.\n");

   uint64 wait = 1;
   for (uint64 i = 0; i < MAX_THREADS; i++) {
      while (lock->read_counter[i][lock_idx] != 0) {
         platform_sleep_ns(wait);
         wait = wait > 2048 ? wait : 2 * wait;
      }
      wait = 1;
   }
}

void
batch_rwlock_unlock(batch_rwlock *lock, uint64 lock_idx)
{
   __sync_lock_release(&lock->write_lock[lock_idx].lock);
}

void
batch_rwlock_full_unlock(batch_rwlock *lock, uint64 lock_idx)
{
   batch_rwlock_unlock(lock, lock_idx);
   batch_rwlock_unclaim(lock, lock_idx);
   batch_rwlock_unget(lock, lock_idx);
}

/*
 *-----------------------------------------------------------------------------
 * try_claim/claim/unlock
 *
 * A claim blocks all other claimants (and therefore all other writelocks,
 * because writelocks are required to hold a claim during the writelock).
 *
 * Must hold a get (read lock)
 * try_claim returns whether the claim succeeded
 *-----------------------------------------------------------------------------
 */

bool32
batch_rwlock_try_claim(batch_rwlock *lock, uint64 lock_idx)
{
   threadid tid = platform_get_tid();
   debug_assert(lock->read_counter[tid][lock_idx]);
   if (__sync_lock_test_and_set(&lock->write_lock[lock_idx].claim, 1)) {
      return FALSE;
   }
   debug_only uint8 old_counter =
      __sync_fetch_and_sub(&lock->read_counter[tid][lock_idx], 1);
   debug_assert(0 < old_counter);
   return TRUE;
}

void
batch_rwlock_claim_loop(batch_rwlock *lock, uint64 lock_idx)
{
   uint64 wait = 1;
   while (!batch_rwlock_try_claim(lock, lock_idx)) {
      batch_rwlock_unget(lock, lock_idx);
      platform_sleep_ns(wait);
      wait = wait > 2048 ? wait : 2 * wait;
      batch_rwlock_get(lock, lock_idx);
   }
}

void
batch_rwlock_unclaim(batch_rwlock *lock, uint64 lock_idx)
{
   threadid tid = platform_get_tid();
   __sync_fetch_and_add(&lock->read_counter[tid][lock_idx], 1);
   __sync_lock_release(&lock->write_lock[lock_idx].claim);
}

/*
 *-----------------------------------------------------------------------------
 * get/unget
 *
 * Acquire a read lock
 *-----------------------------------------------------------------------------
 */

void
batch_rwlock_get(batch_rwlock *lock, uint64 lock_idx)
{
   threadid tid = platform_get_tid();
   while (1) {
      uint64 wait = 1;
      while (lock->write_lock[lock_idx].lock) {
         platform_sleep_ns(wait);
         wait = wait > 2048 ? wait : 2 * wait;
      }
      debug_only uint8 old_counter =
         __sync_fetch_and_add(&lock->read_counter[tid][lock_idx], 1);
      debug_assert(old_counter == 0);
      if (!lock->write_lock[lock_idx].lock) {
         return;
      }
      old_counter = __sync_fetch_and_sub(&lock->read_counter[tid][lock_idx], 1);
      debug_assert(old_counter == 1);
   }
   platform_assert(0);
}

void
batch_rwlock_unget(batch_rwlock *lock, uint64 lock_idx)
{
   threadid         tid = platform_get_tid();
   debug_only uint8 old_counter =
      __sync_fetch_and_sub(&lock->read_counter[tid][lock_idx], 1);
   debug_assert(old_counter == 1);
}
