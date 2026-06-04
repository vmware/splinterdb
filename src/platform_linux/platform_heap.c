// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform_heap.h"
#include "platform_status.h"
#include <sys/mman.h>

/*
 * Declare globals to track heap handle/ID that may have been created when
 * using shared memory. We stash away these handles so that we can return the
 * right handle via platform_get_heap_id() interface, in case shared segments
 * are in use.
 */
platform_heap_id Heap_id = NULL;

#define PLATFORM_MEMORY_FAULT_CONFIG_DEFAULT                                   \
   {                                                                           \
      .mode                           = PLATFORM_MEMORY_FAULT_RANGE,           \
      .range_start                    = 0,                                     \
      .range_count                    = 0,                                     \
      .seed                           = 0,                                     \
      .random_fail_probability        = 1000,                                  \
      .random_burst_start_probability = 1000,                                  \
      .random_burst_fail_probability  = 850000,                                \
      .random_burst_min_length        = 1,                                     \
      .random_burst_max_length        = 4096,                                  \
      .max_failures                   = UINT64_MAX,                            \
      .verbose                        = FALSE,                                 \
   }

static platform_memory_fault_config Platform_memory_fault_config =
   PLATFORM_MEMORY_FAULT_CONFIG_DEFAULT;

void
platform_memory_fault_config_get(platform_memory_fault_config *cfg)
{
   *cfg = Platform_memory_fault_config;
}

void
platform_memory_fault_config_set(const platform_memory_fault_config *cfg)
{
   Platform_memory_fault_config = *cfg;
   platform_memory_fault_reset_counters();
}

#if PLATFORM_MEMORY_FAULT_INJECTION

static uint32 Platform_memory_fault_enabled;
static uint64 Platform_memory_fault_alloc_count;
static uint64 Platform_memory_fault_failure_count;
static uint64 Platform_memory_fault_burst_remaining;

static inline uint64
platform_memory_fault_mix(uint64 x)
{
   x += UINT64_C(0x9e3779b97f4a7c15);
   x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
   x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
   return x ^ (x >> 31);
}

static inline bool32
platform_memory_fault_chance(const platform_memory_fault_config *cfg,
                             uint64                              alloc_no,
                             uint64                              stream,
                             uint32                              probability)
{
   if (probability == 0) {
      return FALSE;
   }
   if (probability >= PLATFORM_MEMORY_FAULT_PROBABILITY_SCALE) {
      return TRUE;
   }

   uint64 sample = platform_memory_fault_mix(
      cfg->seed ^ alloc_no ^ (stream * UINT64_C(0x9e3779b97f4a7c15)));
   return sample % PLATFORM_MEMORY_FAULT_PROBABILITY_SCALE < probability;
}

static inline uint64
platform_memory_fault_random_below(uint64 seed, uint64 alloc_no, uint64 max)
{
   if (max == 0) {
      return 0;
   }
   return platform_memory_fault_mix(seed ^ alloc_no) % max;
}

static uint64
platform_memory_fault_burst_length(const platform_memory_fault_config *cfg,
                                   uint64                              alloc_no)
{
   uint64 min_length = cfg->random_burst_min_length;
   uint64 max_length = cfg->random_burst_max_length;

   if (max_length < min_length) {
      return 0;
   }

   uint64 bucket =
      platform_memory_fault_random_below(cfg->seed, alloc_no ^ 0xbad51eed, 100);
   uint64 low = min_length;
   uint64 high;

   if (bucket < 70) {
      high = MIN(max_length, MAX(min_length, 10));
   } else if (bucket < 95) {
      low  = MAX(min_length, 10);
      high = MIN(max_length, MAX(low, 100));
   } else {
      low  = MAX(min_length, 100);
      high = max_length;
   }

   if (high < low) {
      low  = min_length;
      high = max_length;
   }

   return low
          + platform_memory_fault_random_below(
             cfg->seed, alloc_no ^ 0x51eed5eed, high - low + 1);
}

static bool32
platform_memory_fault_consume_burst(void)
{
   for (;;) {
      uint64 remaining = Platform_memory_fault_burst_remaining;
      if (remaining == 0) {
         return FALSE;
      }
      if (__sync_bool_compare_and_swap(
             &Platform_memory_fault_burst_remaining, remaining, remaining - 1))
      {
         return TRUE;
      }
   }
}

void
platform_memory_fault_enable(void)
{
   __sync_lock_test_and_set(&Platform_memory_fault_enabled, TRUE);
}

void
platform_memory_fault_disable(void)
{
   __sync_lock_test_and_set(&Platform_memory_fault_enabled, FALSE);
   __sync_lock_test_and_set(&Platform_memory_fault_burst_remaining, 0);
}

void
platform_memory_fault_reset_counters(void)
{
   __sync_lock_test_and_set(&Platform_memory_fault_alloc_count, 0);
   __sync_lock_test_and_set(&Platform_memory_fault_failure_count, 0);
   __sync_lock_test_and_set(&Platform_memory_fault_burst_remaining, 0);
}

platform_memory_fault_counters
platform_memory_fault_get_counters(void)
{
   return (platform_memory_fault_counters){
      .alloc_count =
         __sync_fetch_and_add(&Platform_memory_fault_alloc_count, 0),
      .failure_count =
         __sync_fetch_and_add(&Platform_memory_fault_failure_count, 0),
   };
}

bool32
platform_memory_fault_should_fail(platform_heap_id heap_id,
                                  size_t           size,
                                  const char      *objname,
                                  const char      *func,
                                  const char      *file,
                                  int              lineno)
{
   (void)heap_id;

   if (!__sync_fetch_and_add(&Platform_memory_fault_enabled, 0) || size == 0) {
      return FALSE;
   }

   platform_memory_fault_config cfg = Platform_memory_fault_config;

   uint64 alloc_no =
      __sync_add_and_fetch(&Platform_memory_fault_alloc_count, 1);
   bool32 should_fail = FALSE;

   switch (cfg.mode) {
      case PLATFORM_MEMORY_FAULT_RANGE:
         should_fail = alloc_no >= cfg.range_start
                       && alloc_no - cfg.range_start < cfg.range_count;
         break;

      case PLATFORM_MEMORY_FAULT_RANDOM:
      {
         bool32 in_burst = platform_memory_fault_consume_burst();

         if (!in_burst
             && platform_memory_fault_chance(
                &cfg, alloc_no, 1, cfg.random_burst_start_probability))
         {
            uint64 burst_length =
               platform_memory_fault_burst_length(&cfg, alloc_no);
            if (burst_length != 0) {
               in_burst = TRUE;
               if (burst_length > 1) {
                  __sync_bool_compare_and_swap(
                     &Platform_memory_fault_burst_remaining,
                     0,
                     burst_length - 1);
               }
            }
         }

         should_fail =
            in_burst ? platform_memory_fault_chance(
                          &cfg, alloc_no, 2, cfg.random_burst_fail_probability)
                     : platform_memory_fault_chance(
                          &cfg, alloc_no, 3, cfg.random_fail_probability);
         break;
      }

      default:
         should_fail = FALSE;
         break;
   }

   if (!should_fail) {
      return FALSE;
   }

   uint64 failure_no =
      __sync_add_and_fetch(&Platform_memory_fault_failure_count, 1);
   if (failure_no > cfg.max_failures) {
      __sync_fetch_and_sub(&Platform_memory_fault_failure_count, 1);
      return FALSE;
   }

   if (cfg.verbose) {
      platform_error_log("memory fault: allocation %lu failed"
                         " (failure %lu, size %zu, object %s, at %s:%d %s)\n",
                         alloc_no,
                         failure_no,
                         size,
                         objname ? objname : "<unknown>",
                         file ? file : "<unknown>",
                         lineno,
                         func ? func : "<unknown>");
   }

   return TRUE;
}

#else

void
platform_memory_fault_enable(void)
{
}

void
platform_memory_fault_disable(void)
{
}

void
platform_memory_fault_reset_counters(void)
{
}

platform_memory_fault_counters
platform_memory_fault_get_counters(void)
{
   return (platform_memory_fault_counters){0};
}

bool32
platform_memory_fault_should_fail(platform_heap_id heap_id,
                                  size_t           size,
                                  const char      *objname,
                                  const char      *func,
                                  const char      *file,
                                  int              lineno)
{
   return FALSE;
}

#endif // PLATFORM_MEMORY_FAULT_INJECTION

/*
 * platform_heap_create() - Create a heap for memory allocation.
 *
 * By default, we just revert to process' heap-memory and use malloc() / free()
 * for memory management. If Splinter is run with shared-memory configuration,
 * create a shared-segment which acts as the 'heap' for memory allocation.
 */
platform_status
platform_heap_create(platform_module_id UNUSED_PARAM(module_id),
                     size_t             max,
                     bool               use_shmem,
                     platform_heap_id  *heap_id)
{
   if (use_shmem) {
      shmallocator *shm = mmap(
         NULL, max, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
      if (shm == MAP_FAILED) {
         return STATUS_NO_MEMORY;
      }
      shmallocator_init(shm, max / 4096, max);
      *heap_id = (platform_heap_id)shm;

   } else {
      *heap_id = PROCESS_PRIVATE_HEAP_ID;
   }
   return STATUS_OK;
}

void
platform_heap_destroy(platform_heap_id *heap_id)
{
   if (*heap_id) {
      size_t size = shmallocator_size((shmallocator *)*heap_id);
      shmallocator_deinit((shmallocator *)*heap_id);
      munmap((void *)*heap_id, size);
      *heap_id = NULL;
   }
}
