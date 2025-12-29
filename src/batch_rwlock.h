// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef BATCH_RWLOCK_H
#define BATCH_RWLOCK_H

#include "platform_linux/platform.h"

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

// Distributed Batch RW Lock
typedef struct {
   volatile uint8 lock;
   volatile uint8 claim;
} batch_claimlock;

typedef struct {
   batch_claimlock write_lock[PLATFORM_CACHELINE_SIZE / 2];
   volatile uint8  read_counter[MAX_THREADS][PLATFORM_CACHELINE_SIZE / 2];
} PLATFORM_CACHELINE_ALIGNED batch_rwlock;

_Static_assert(sizeof(batch_rwlock)
                  == PLATFORM_CACHELINE_SIZE * (MAX_THREADS / 2 + 1),
               "Missized batch_rwlock\n");


/*
 * The state machine for a thread interacting with a batch_rwlock is:
 *
 *             get                   claim                lock
 * unlocked <-------> read-locked <----------> claimed <--------> write-locked
 *            unget                 unclaim              unlock
 *
 * Note that try_claim() may fail, in which case the state of the lock
 * is unchanged, i.e. the caller still holds a read lock.
 */


void
batch_rwlock_init(batch_rwlock *lock);

void
batch_rwlock_deinit(batch_rwlock *lock);

/* no lock -> shared lock */
void
batch_rwlock_get(batch_rwlock *lock, uint64 lock_idx);

/* shared lock -> no lock */
void
batch_rwlock_unget(batch_rwlock *lock, uint64 lock_idx);

/*
 * shared-lock -> claim (may fail)
 *
 * Callers still hold a shared lock after a failed claim attempt.
 * Callers _must_ release their shared lock after a failed claim attempt.
 */
bool32
batch_rwlock_try_claim(batch_rwlock *lock, uint64 lock_idx);

/* shared-lock -> claim, BUT(!) may temporarily release the shared-lock in the
 * process. */
void
batch_rwlock_claim_loop(batch_rwlock *lock, uint64 lock_idx);

/* claim -> shared lock */
void
batch_rwlock_unclaim(batch_rwlock *lock, uint64 lock_idx);

/* claim -> exclusive lock */
void
batch_rwlock_lock(batch_rwlock *lock, uint64 lock_idx);

/* exclusive lock -> claim */
void
batch_rwlock_unlock(batch_rwlock *lock, uint64 lock_idx);

/* exclusive-lock -> unlocked */
void
batch_rwlock_full_unlock(batch_rwlock *lock, uint64 lock_idx);

#endif /* BATCH_RWLOCK_H */

