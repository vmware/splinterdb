// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "allocator.h"
#include "platform_mutex.h"

/*
 * The packed tags use the allocator's raw counts: 0 is free, 1 is the
 * allocation floor with no logical owners, and 2 is one logical owner.
 * Tag 3 redirects to an exact uint32 count in an independently locked shard.
 * No address/key bits are discarded when matching an overflow entry.
 */
#define RC_REFCOUNT_SHARD_COUNT 64

struct rc_refcount_entry;

typedef struct rc_refcount_shard {
   platform_mutex            lock;
   struct rc_refcount_entry *entries;
   uint64                    capacity;
   uint64                    used;
} PLATFORM_CACHELINE_ALIGNED rc_refcount_shard;

#if SPLINTER_DEBUG
typedef enum rc_refcount_test_hook_event {
   RC_REFCOUNT_TEST_ESCAPE_OBSERVED,
   RC_REFCOUNT_TEST_INCREMENT_LOCK_WAIT,
   RC_REFCOUNT_TEST_PROMOTE_INSERTED,
   RC_REFCOUNT_TEST_DEMOTE_PUBLISHED,
} rc_refcount_test_hook_event;

typedef void (*rc_refcount_test_hook_fn)(void                       *arg,
                                         rc_refcount_test_hook_event event,
                                         uint64                      extent);
#endif

typedef struct rc_refcount {
   uint64           *words;
   uint64            extent_capacity;
   uint64            word_count;
   platform_heap_id  heap_id;
   uint32            initialized_shards;
   rc_refcount_shard shards[RC_REFCOUNT_SHARD_COUNT];
#if SPLINTER_DEBUG
   /* Set only while quiescent. Hooks may coordinate other threads, but must
    * not reenter this map from the hook thread. PROMOTE_INSERTED and
    * DEMOTE_PUBLISHED execute while holding the corresponding shard lock. */
   rc_refcount_test_hook_fn test_hook;
   void                    *test_hook_arg;
   /* -1 disables injection; 0 fails growth; positive values allow that many
    * table-allocation attempts before failure. Accessed atomically. */
   int64 test_allocations_before_failure;
#endif
} rc_refcount;

typedef struct rc_refcount_usage {
   uint64 container_bytes;
   uint64 packed_bytes;
   uint64 overflow_bytes; /* retained allocation, including empty slots */
   uint64 overflow_entries;
   uint64 overflow_slots;
} rc_refcount_usage;

/* Initialization, reset, set, and destruction require a quiescent container.
 * On failed initialization, deinit is safe; there is no initialized map. */
platform_status
rc_refcount_init(rc_refcount       *map,
                 uint64             extent_capacity,
                 platform_heap_id   heap_id,
                 platform_module_id module_id);

void
rc_refcount_deinit(rc_refcount *map);

/* Clear every count and free all overflow tables, retaining the packed map. */
void
rc_refcount_reset(rc_refcount *map);

/* Quiescent load/recovery setter. An allocation failure leaves the logical
 * count unchanged. Raw values span the entire uint32 refcount domain. */
platform_status
rc_refcount_set(rc_refcount *map, uint64 extent, refcount value);

refcount
rc_refcount_get(rc_refcount *map, uint64 extent);

/* Allocation predicate needs only a tag load, including escaped counts. */
bool32
rc_refcount_is_allocated(rc_refcount *map, uint64 extent);

/* Atomically claim a free extent, changing raw count 0 to AL_ONE_REF (2). */
bool32
rc_refcount_try_claim(rc_refcount *map, uint64 extent);

/* Concurrent increment. On failure this operation does not change the
 * logical count, and leaves *new_value untouched. Returns NO_MEMORY for an
 * overflow-table growth failure, INVALID_STATE for a free extent, and
 * LIMIT_EXCEEDED for UINT32_MAX. Other threads may still update the extent. */
platform_status
rc_refcount_try_inc(rc_refcount *map, uint64 extent, refcount *new_value);

/* Concurrent, allocation-free decrement. A zero input is a caller error. */
refcount
rc_refcount_dec(rc_refcount *map, uint64 extent);

/* Per-shard coherent accounting; the aggregate may span concurrent updates.
 * Includes requested container/table storage, not heap allocation headers. */
void
rc_refcount_get_usage(rc_refcount *map, rc_refcount_usage *usage);
