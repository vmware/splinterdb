// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "rc_refcount.h"
#include "platform_typed_alloc.h"
#include "poison.h"

#define RC_REFCOUNT_INITIAL_SLOTS 16U
#define RC_REFCOUNT_SHARD_BITS    6U

_Static_assert(RC_REFCOUNT_SHARD_COUNT == (1U << RC_REFCOUNT_SHARD_BITS),
               "shard count must match hash partition");
_Static_assert(AL_FREE == 0 && AL_NO_REFS == 1 && AL_ONE_REF == 2,
               "packed tags must match allocator raw counts");
_Static_assert(sizeof(refcount) == sizeof(uint64),
               "overflow counts retain the uint64 domain");

typedef struct rc_refcount_entry {
   uint64   extent;
   refcount count; /* 0 means empty; occupied counts are at least ESCAPE */
} rc_refcount_entry;

#if SPLINTER_DEBUG
static inline void
rc_refcount_run_hook(rc_refcount                *map,
                     rc_refcount_test_hook_event event,
                     uint64                      extent)
{
   if (map->test_hook != NULL) {
      map->test_hook(map->test_hook_arg, event, extent);
   }
}

static bool32
rc_refcount_inject_allocation_failure(rc_refcount *map)
{
   int64 remaining =
      __atomic_load_n(&map->test_allocations_before_failure, __ATOMIC_RELAXED);
   while (remaining >= 0) {
      if (remaining == 0) {
         return TRUE;
      }
      if (__atomic_compare_exchange_n(&map->test_allocations_before_failure,
                                      &remaining,
                                      remaining - 1,
                                      FALSE,
                                      __ATOMIC_RELAXED,
                                      __ATOMIC_RELAXED))
      {
         return FALSE;
      }
   }
   return FALSE;
}
#endif

static inline void
rc_refcount_check_extent(const rc_refcount *map, uint64 extent)
{
   platform_assert(map->words != NULL);
   platform_assert(extent < map->extent_capacity,
                   "Invalid refcount extent %lu (capacity %lu)",
                   extent,
                   map->extent_capacity);
}

static inline uint32
rc_refcount_tag(rc_refcount *map, uint64 extent)
{
   uint64 word = __atomic_load_n(
      &map->words[extent / RC_REFCOUNT_COUNTS_PER_WORD], __ATOMIC_RELAXED);
   return (word >> (SPLINTER_RC_BITS * (extent % RC_REFCOUNT_COUNTS_PER_WORD)))
          & RC_REFCOUNT_ESCAPE;
}

/* Only the requested lane changes. An unrelated lane's mutation retries
 * the CAS; a change of this lane to a different tag reports failure. */
static bool32
rc_refcount_change_tag(rc_refcount *map,
                       uint64       extent,
                       uint32       old_tag,
                       uint32       new_tag)
{
   uint64 *word     = &map->words[extent / RC_REFCOUNT_COUNTS_PER_WORD];
   uint32  shift    = SPLINTER_RC_BITS * (extent % RC_REFCOUNT_COUNTS_PER_WORD);
   uint64  mask     = (uint64)RC_REFCOUNT_ESCAPE << shift;
   uint64  old_word = __atomic_load_n(word, __ATOMIC_RELAXED);
   while (((old_word >> shift) & RC_REFCOUNT_ESCAPE) == old_tag) {
      uint64 new_word = (old_word & ~mask) | ((uint64)new_tag << shift);
      if (__atomic_compare_exchange_n(word,
                                      &old_word,
                                      new_word,
                                      FALSE,
                                      __ATOMIC_SEQ_CST,
                                      __ATOMIC_RELAXED))
      {
         return TRUE;
      }
   }
   return FALSE;
}

/* A reversible 64-bit mixing permutation; equality always compares the full
 * extent key. Sharding consumes low hash bits; probing uses the rest. */
static inline uint64
rc_refcount_hash(uint64 extent)
{
   extent ^= extent >> 30;
   extent *= UINT64_C(0xbf58476d1ce4e5b9);
   extent ^= extent >> 27;
   extent *= UINT64_C(0x94d049bb133111eb);
   return extent ^ (extent >> 31);
}

static inline rc_refcount_shard *
rc_refcount_shard_for(rc_refcount *map, uint64 extent)
{
   return &map->shards[rc_refcount_hash(extent)
                       & (RC_REFCOUNT_SHARD_COUNT - 1)];
}

static inline uint64
rc_refcount_home(uint64 extent, uint64 capacity)
{
   return (rc_refcount_hash(extent) >> RC_REFCOUNT_SHARD_BITS) & (capacity - 1);
}

/* Caller holds the shard lock, or the entire map is quiescent. */
static rc_refcount_entry *
rc_refcount_find_slot(rc_refcount_shard *shard, uint64 extent)
{
   platform_assert(shard->capacity != 0);
   uint64 slot = rc_refcount_home(extent, shard->capacity);
   while (shard->entries[slot].count != 0
          && shard->entries[slot].extent != extent)
   {
      slot = (slot + 1) & (shard->capacity - 1);
   }
   return &shard->entries[slot];
}

static rc_refcount_entry *
rc_refcount_find_existing(rc_refcount_shard *shard, uint64 extent)
{
   rc_refcount_entry *entry = rc_refcount_find_slot(shard, extent);
   platform_assert(entry->count >= RC_REFCOUNT_ESCAPE,
                   "Missing overflow refcount for extent %lu",
                   extent);
   return entry;
}

/* Backward-shift deletion leaves no tombstones and allocates nothing. */
static void
rc_refcount_erase(rc_refcount_shard *shard, rc_refcount_entry *entry)
{
   uint64 mask = shard->capacity - 1;
   uint64 hole = entry - shard->entries;
   uint64 scan = (hole + 1) & mask;
   while (shard->entries[scan].count != 0) {
      uint64 home =
         rc_refcount_home(shard->entries[scan].extent, shard->capacity);
      if (((scan - home) & mask) >= ((scan - hole) & mask)) {
         shard->entries[hole] = shard->entries[scan];
         hole                 = scan;
      }
      scan = (scan + 1) & mask;
   }
   memset(&shard->entries[hole], 0, sizeof(shard->entries[hole]));
   platform_assert(shard->used != 0);
   shard->used--;
}

static platform_status
rc_refcount_ensure_space(rc_refcount *map, rc_refcount_shard *shard)
{
   /* sizeof(entry) >= 8 bounds capacity*7 below UINT64_MAX. */
   if (shard->used + 1 <= shard->capacity * 7 / 10) {
      return STATUS_OK;
   }
   if (shard->capacity > SIZE_MAX / (2 * sizeof(rc_refcount_entry))) {
      return STATUS_NO_MEMORY;
   }
   uint64 capacity =
      shard->capacity == 0 ? RC_REFCOUNT_INITIAL_SLOTS : shard->capacity * 2;
   debug_code(if (rc_refcount_inject_allocation_failure(map)) {
      return STATUS_NO_MEMORY;
   });
   rc_refcount_entry *entries =
      TYPED_ARRAY_ZALLOC(map->heap_id, entries, capacity);
   if (entries == NULL) {
      return STATUS_NO_MEMORY;
   }
   rc_refcount_shard replacement = {.entries = entries, .capacity = capacity};
   for (uint64 slot = 0; slot < shard->capacity; slot++) {
      if (shard->entries[slot].count != 0) {
         *rc_refcount_find_slot(&replacement, shard->entries[slot].extent) =
            shard->entries[slot];
      }
   }
   if (shard->entries != NULL) {
      platform_free(map->heap_id, shard->entries);
   }
   shard->entries  = entries;
   shard->capacity = capacity;
   return STATUS_OK;
}

platform_status
rc_refcount_init(rc_refcount       *map,
                 uint64             extent_capacity,
                 platform_heap_id   heap_id,
                 platform_module_id module_id)
{
   memset(map, 0, sizeof(*map));
   map->heap_id = heap_id;
   debug_code(map->test_allocations_before_failure = -1);
   if (extent_capacity == 0) {
      return STATUS_BAD_PARAM;
   }
   map->word_count = extent_capacity / RC_REFCOUNT_COUNTS_PER_WORD
                     + (extent_capacity % RC_REFCOUNT_COUNTS_PER_WORD != 0);
   if (map->word_count > SIZE_MAX / sizeof(*map->words)) {
      return STATUS_NO_MEMORY;
   }
   map->words = TYPED_ARRAY_ZALLOC(heap_id, map->words, map->word_count);
   if (map->words == NULL) {
      return STATUS_NO_MEMORY;
   }
   map->extent_capacity = extent_capacity;
   for (uint32 shard = 0; shard < RC_REFCOUNT_SHARD_COUNT; shard++) {
      platform_status status =
         platform_mutex_init(&map->shards[shard].lock, module_id, heap_id);
      if (!SUCCESS(status)) {
         rc_refcount_deinit(map);
         return status;
      }
      map->initialized_shards++;
   }
   return STATUS_OK;
}

void
rc_refcount_deinit(rc_refcount *map)
{
   for (uint32 shard = 0; shard < map->initialized_shards; shard++) {
      if (map->shards[shard].entries != NULL) {
         platform_free(map->heap_id, map->shards[shard].entries);
      }
      platform_status status = platform_mutex_destroy(&map->shards[shard].lock);
      platform_assert_status_ok(status);
   }
   if (map->words != NULL) {
      platform_free(map->heap_id, map->words);
   }
   memset(map, 0, sizeof(*map));
}

void
rc_refcount_reset(rc_refcount *map)
{
   platform_assert(map->words != NULL);
   for (uint32 shard = 0; shard < map->initialized_shards; shard++) {
      if (map->shards[shard].entries != NULL) {
         platform_free(map->heap_id, map->shards[shard].entries);
      }
      map->shards[shard].entries  = NULL;
      map->shards[shard].capacity = 0;
      map->shards[shard].used     = 0;
   }
   memset(map->words, 0, map->word_count * sizeof(*map->words));
}

platform_status
rc_refcount_set(rc_refcount *map, uint64 extent, refcount value)
{
   rc_refcount_check_extent(map, extent);
   rc_refcount_shard *shard   = rc_refcount_shard_for(map, extent);
   uint32             old_tag = rc_refcount_tag(map, extent);
   if (value >= RC_REFCOUNT_ESCAPE) {
      if (old_tag != RC_REFCOUNT_ESCAPE) {
         platform_status status = rc_refcount_ensure_space(map, shard);
         if (!SUCCESS(status)) {
            return status;
         }
      }
      rc_refcount_entry *entry = rc_refcount_find_slot(shard, extent);
      platform_assert((old_tag == RC_REFCOUNT_ESCAPE) == (entry->count != 0));
      shard->used += old_tag != RC_REFCOUNT_ESCAPE;
      *entry = (rc_refcount_entry){.extent = extent, .count = value};
      platform_assert(
         rc_refcount_change_tag(map, extent, old_tag, RC_REFCOUNT_ESCAPE));
   } else {
      if (old_tag == RC_REFCOUNT_ESCAPE) {
         rc_refcount_erase(shard, rc_refcount_find_existing(shard, extent));
      }
      platform_assert(rc_refcount_change_tag(map, extent, old_tag, value));
   }
   return STATUS_OK;
}

refcount
rc_refcount_get(rc_refcount *map, uint64 extent)
{
   rc_refcount_check_extent(map, extent);
   uint32 tag = rc_refcount_tag(map, extent);
   if (tag != RC_REFCOUNT_ESCAPE) {
      return tag;
   }
   debug_code(
      rc_refcount_run_hook(map, RC_REFCOUNT_TEST_ESCAPE_OBSERVED, extent));
   rc_refcount_shard *shard = rc_refcount_shard_for(map, extent);
   platform_mutex_lock(&shard->lock);
   tag             = rc_refcount_tag(map, extent);
   refcount result = tag == RC_REFCOUNT_ESCAPE
                        ? rc_refcount_find_existing(shard, extent)->count
                        : tag;
   platform_mutex_unlock(&shard->lock);
   return result;
}

bool32
rc_refcount_is_allocated(rc_refcount *map, uint64 extent)
{
   rc_refcount_check_extent(map, extent);
   return rc_refcount_tag(map, extent) != AL_FREE;
}

bool32
rc_refcount_try_claim(rc_refcount *map, uint64 extent)
{
   rc_refcount_check_extent(map, extent);
   return rc_refcount_change_tag(map, extent, AL_FREE, AL_ONE_REF);
}

platform_status
rc_refcount_try_inc(rc_refcount *map, uint64 extent, refcount *new_value)
{
   rc_refcount_check_extent(map, extent);
   while (TRUE) {
      uint32 tag = rc_refcount_tag(map, extent);
      if (tag == AL_FREE) {
         return STATUS_INVALID_STATE;
      }
      if (tag < RC_REFCOUNT_ESCAPE - 1) {
         if (rc_refcount_change_tag(map, extent, tag, tag + 1)) {
            if (new_value != NULL) {
               *new_value = tag + 1;
            }
            return STATUS_OK;
         }
         continue;
      }
      rc_refcount_shard *shard = rc_refcount_shard_for(map, extent);
      debug_code(rc_refcount_run_hook(
         map, RC_REFCOUNT_TEST_INCREMENT_LOCK_WAIT, extent));
      platform_mutex_lock(&shard->lock);
      tag = rc_refcount_tag(map, extent);
      if (tag == RC_REFCOUNT_ESCAPE) {
         rc_refcount_entry *entry = rc_refcount_find_existing(shard, extent);
         if (entry->count == UINT64_MAX) {
            platform_mutex_unlock(&shard->lock);
            return STATUS_LIMIT_EXCEEDED;
         }
         entry->count++;
         if (new_value != NULL) {
            *new_value = entry->count;
         }
         platform_mutex_unlock(&shard->lock);
         return STATUS_OK;
      }
      if (tag != RC_REFCOUNT_ESCAPE - 1) {
         platform_mutex_unlock(&shard->lock);
         continue;
      }
      platform_status status = rc_refcount_ensure_space(map, shard);
      if (!SUCCESS(status)) {
         platform_mutex_unlock(&shard->lock);
         return status;
      }
      rc_refcount_entry *entry = rc_refcount_find_slot(shard, extent);
      platform_assert(entry->count == 0);
      *entry =
         (rc_refcount_entry){.extent = extent, .count = RC_REFCOUNT_ESCAPE};
      shard->used++;
      debug_code(
         rc_refcount_run_hook(map, RC_REFCOUNT_TEST_PROMOTE_INSERTED, extent));
      if (rc_refcount_change_tag(
             map, extent, RC_REFCOUNT_ESCAPE - 1, RC_REFCOUNT_ESCAPE))
      {
         if (new_value != NULL) {
            *new_value = RC_REFCOUNT_ESCAPE;
         }
         platform_mutex_unlock(&shard->lock);
         return STATUS_OK;
      }
      /* A concurrent inline decrement won. Remove the tentative entry before
       * releasing the lock; no escaped reader can observe this failed insert.
       */
      rc_refcount_erase(shard, entry);
      platform_mutex_unlock(&shard->lock);
   }
}

refcount
rc_refcount_dec(rc_refcount *map, uint64 extent)
{
   rc_refcount_check_extent(map, extent);
   while (TRUE) {
      uint32 tag = rc_refcount_tag(map, extent);
      platform_assert(tag != AL_FREE, "Decrement of free extent %lu", extent);
      if (tag != RC_REFCOUNT_ESCAPE) {
         if (rc_refcount_change_tag(map, extent, tag, tag - 1)) {
            return tag - 1;
         }
         continue;
      }
      rc_refcount_shard *shard = rc_refcount_shard_for(map, extent);
      platform_mutex_lock(&shard->lock);
      if (rc_refcount_tag(map, extent) != RC_REFCOUNT_ESCAPE) {
         platform_mutex_unlock(&shard->lock);
         continue;
      }
      rc_refcount_entry *entry = rc_refcount_find_existing(shard, extent);
      if (entry->count > RC_REFCOUNT_ESCAPE) {
         refcount result = --entry->count;
         platform_mutex_unlock(&shard->lock);
         return result;
      }
      /* Publish the inline count while locked. Low-tag operations may proceed,
       * but a fresh promotion waits for us to erase the old overflow entry. */
      platform_assert(rc_refcount_change_tag(
         map, extent, RC_REFCOUNT_ESCAPE, RC_REFCOUNT_ESCAPE - 1));
      debug_code(
         rc_refcount_run_hook(map, RC_REFCOUNT_TEST_DEMOTE_PUBLISHED, extent));
      rc_refcount_erase(shard, entry);
      platform_mutex_unlock(&shard->lock);
      return RC_REFCOUNT_ESCAPE - 1;
   }
}

void
rc_refcount_get_usage(rc_refcount *map, rc_refcount_usage *usage)
{
   memset(usage, 0, sizeof(*usage));
   usage->container_bytes = sizeof(*map);
   usage->packed_bytes    = map->word_count * sizeof(*map->words);
   for (uint32 shard_no = 0; shard_no < map->initialized_shards; shard_no++) {
      rc_refcount_shard *shard = &map->shards[shard_no];
      platform_mutex_lock(&shard->lock);
      usage->overflow_bytes += shard->capacity * sizeof(*shard->entries);
      usage->overflow_slots += shard->capacity;
      usage->overflow_entries += shard->used;
      platform_mutex_unlock(&shard->lock);
   }
}
