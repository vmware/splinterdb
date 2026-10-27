// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * Refcount-container microbenchmark; not an allocator or database benchmark.
 * Run timed invocations only under the campaign's exclusive quiet window.
 * --validate executes the same operations and checks without reading a timer.
 *
 * Mutation workers own disjoint striped extent sets, except "hot", where all
 * workers increment/decrement one shared high-count extent. Operation streams
 * depend only on seed, worker number, extents, and workload, never on backend.
 * Timings include PRNG, indirect dispatch, and identical result checks.
 * Initial allocation, table warming, registration, and final scans are untimed.
 */
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "platform_threads.h"
#include "rc_refcount.h"

_Static_assert(RC_REFCOUNT_ESCAPE == 15,
               "benchmark requires fixed four-bit tags");
#define HIGH_RAW_COUNT ((refcount)RC_REFCOUNT_ESCAPE + 5)

typedef enum workload {
   WORK_GET,
   WORK_ALLOCATED,
   WORK_CLAIM,
   WORK_PROMOTE,
   WORK_HIGH,
   WORK_HOT
} workload;

typedef struct dense_map {
   refcount *counts;
   uint64    capacity;
} dense_map;

typedef struct map_ops {
   refcount (*get)(void *, uint64);
   bool32 (*allocated)(void *, uint64);
   bool32 (*claim)(void *, uint64);
   platform_status (*inc)(void *, uint64, refcount *);
   refcount (*dec)(void *, uint64);
   platform_status (*set)(void *, uint64, refcount);
} map_ops;

typedef struct benchmark benchmark;
typedef struct worker {
   benchmark *bench;
   uint64     id;
   uint64     checksum;
   uint64     ended_ns;
} worker;

struct benchmark {
   const char     *backend;
   const char     *workload_name;
   workload        work;
   uint64          extents, threads, operations, seed, high_count;
   bool            validate_only;
   void           *map;
   map_ops         ops;
   pthread_mutex_t start_lock;
   pthread_cond_t  start_cond;
   uint64          ready;
   bool            start;
};

static void
fail(const char *reason)
{
   fprintf(stderr, "refcount_bench: %s\n", reason);
   exit(1);
}

static void
check_status(platform_status status, const char *reason)
{
   if (!SUCCESS(status)) {
      fprintf(stderr,
              "refcount_bench: %s: %s\n",
              reason,
              platform_status_to_string(status));
      exit(1);
   }
}

static uint64
now_ns(void)
{
   struct timespec value;
   if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
      fail("clock_gettime failed");
   }
   return (uint64)value.tv_sec * UINT64_C(1000000000) + value.tv_nsec;
}

static uint64
next_random(uint64 *state)
{
   uint64 x = *state;
   x ^= x >> 12;
   x ^= x << 25;
   x ^= x >> 27;
   *state = x;
   return x * UINT64_C(2685821657736338717);
}

static refcount
dense_get(void *map, uint64 extent)
{
   return __atomic_load_n(&((dense_map *)map)->counts[extent],
                          __ATOMIC_RELAXED);
}

static bool32
dense_allocated(void *map, uint64 extent)
{
   return dense_get(map, extent) != AL_FREE;
}

static bool32
dense_claim(void *map, uint64 extent)
{
   return __sync_bool_compare_and_swap(
      &((dense_map *)map)->counts[extent], AL_FREE, AL_ONE_REF);
}

static platform_status
dense_inc(void *map, uint64 extent, refcount *value)
{
   // Standalone dense64 control, not a selectable production allocator.
   // Give it the same fallible count/output contract as the packed backend.
   refcount *count = &((dense_map *)map)->counts[extent];
   refcount  old   = __atomic_load_n(count, __ATOMIC_RELAXED);
   while (TRUE) {
      if (old == AL_FREE) {
         return STATUS_INVALID_STATE;
      }
      if (old == UINT64_MAX) {
         return STATUS_LIMIT_EXCEEDED;
      }
      if (__atomic_compare_exchange_n(
             count, &old, old + 1, FALSE, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED))
      {
         if (value != NULL) {
            *value = old + 1;
         }
         return STATUS_OK;
      }
   }
}

static refcount
dense_dec(void *map, uint64 extent)
{
   return __sync_sub_and_fetch(&((dense_map *)map)->counts[extent], 1);
}

static platform_status
dense_set(void *map, uint64 extent, refcount value)
{
   ((dense_map *)map)->counts[extent] = value;
   return STATUS_OK;
}

static refcount
compact_get(void *m, uint64 e)
{
   return rc_refcount_get(m, e);
}
static bool32
compact_allocated(void *m, uint64 e)
{
   return rc_refcount_is_allocated(m, e);
}
static bool32
compact_claim(void *m, uint64 e)
{
   return rc_refcount_try_claim(m, e);
}
static platform_status
compact_inc(void *m, uint64 e, refcount *v)
{
   return rc_refcount_try_inc(m, e, v);
}
static refcount
compact_dec(void *m, uint64 e)
{
   return rc_refcount_dec(m, e);
}
static platform_status
compact_set(void *m, uint64 e, refcount v)
{
   return rc_refcount_set(m, e, v);
}

static refcount
initial_count(const benchmark *b, uint64 extent)
{
   if (b->work == WORK_CLAIM) {
      return AL_FREE;
   }
   if (b->work == WORK_PROMOTE) {
      return RC_REFCOUNT_ESCAPE - 1;
   }
   return extent < b->high_count ? HIGH_RAW_COUNT : AL_ONE_REF;
}

static void *
run_worker(void *argument)
{
   worker    *w = argument;
   benchmark *b = w->bench;
   if (platform_register_thread() != 0) {
      fail("worker thread registration failed");
   }
   if (pthread_mutex_lock(&b->start_lock) != 0) {
      fail("worker start lock failed");
   }
   b->ready++;
   pthread_cond_broadcast(&b->start_cond);
   while (!b->start) {
      if (pthread_cond_wait(&b->start_cond, &b->start_lock) != 0) {
         fail("worker start wait failed");
      }
   }
   pthread_mutex_unlock(&b->start_lock);

   uint64 random = b->seed ^ ((w->id + 1) * UINT64_C(0x9e3779b97f4a7c15));
   if (random == 0) {
      random = UINT64_C(0xd1b54a32d192ed03);
   }
   uint64 checksum = 0;
   uint64 span     = b->work == WORK_HIGH ? b->high_count : b->extents;
   /* Each worker owns all e where e % threads == worker id. */
   uint64 owned = (span - 1 - w->id) / b->threads + 1;
   for (uint64 op = 0; op < b->operations; op++) {
      uint64 random_value = next_random(&random);
      uint64 extent;
      if (b->work == WORK_GET || b->work == WORK_ALLOCATED) {
         extent = random_value % b->extents;
      } else if (b->work == WORK_HOT) {
         extent = 0;
      } else {
         extent = w->id + b->threads * (random_value % owned);
      }
      refcount value = 0, after;
      switch (b->work) {
         case WORK_GET:
            value = b->ops.get(b->map, extent);
            if (value != initial_count(b, extent)) {
               fail("get returned an incorrect count");
            }
            checksum += value;
            break;
         case WORK_ALLOCATED:
         {
            bool32 value_is_allocated = b->ops.allocated(b->map, extent);
            if (value_is_allocated != (initial_count(b, extent) != AL_FREE)) {
               fail("allocated predicate disagreed with initial state");
            }
            checksum += value_is_allocated;
            break;
         }
         case WORK_CLAIM:
            if (!b->ops.claim(b->map, extent)) {
               fail("claim of privately owned free extent failed");
            }
            value = b->ops.dec(b->map, extent);
            after = b->ops.dec(b->map, extent);
            if (value != AL_NO_REFS || after != AL_FREE) {
               fail("claim/retire/free count mismatch");
            }
            checksum += AL_ONE_REF + value + after;
            break;
         case WORK_PROMOTE:
         case WORK_HIGH:
         case WORK_HOT:
            check_status(b->ops.inc(b->map, extent, &value),
                         "increment failed");
            after = b->ops.dec(b->map, extent);
            if (b->work == WORK_HOT) {
               if (value < HIGH_RAW_COUNT + 1
                   || value > HIGH_RAW_COUNT + b->threads
                   || after < HIGH_RAW_COUNT
                   || after >= HIGH_RAW_COUNT + b->threads)
               {
                  fail("hot shared extent count outside concurrent bounds");
               }
            } else if (value != initial_count(b, extent) + 1
                       || after != initial_count(b, extent))
            {
               fail("increment/decrement count mismatch");
            }
            checksum += value + after;
            break;
      }
   }
   if (!b->validate_only) {
      w->ended_ns = now_ns();
   }
   w->checksum = checksum;
   platform_deregister_thread();
   return NULL;
}

static uint64
number(const char *argument)
{
   if (*argument == '\0' || *argument == '-' || *argument == '+') {
      fail("numeric arguments must be unsigned decimal integers");
   }
   for (const char *p = argument; *p; p++) {
      if (*p < '0' || *p > '9') {
         fail("numeric arguments must be unsigned decimal integers");
      }
   }
   errno = 0;
   char              *end;
   unsigned long long result = strtoull(argument, &end, 10);
   if (errno != 0 || *end != '\0') {
      fail("numeric argument outside uint64 range");
   }
   return (uint64)result;
}

static void
usage(FILE *out)
{
   fprintf(
      out,
      "Usage: refcount_bench --backend dense|compact\n"
      "  dense is a standalone dense64 checked-CAS control, not production.\n"
      "  compact is the production fixed four-bit container (escape raw15).\n"
      "  --workload get|allocated|claim|promote|high|hot\n"
      "  [--extents N] [--threads N] [--ops N_PER_THREAD] [--seed N]\n"
      "  [--high-count N] [--validate]\n"
      "Default N=1048576, threads=1, ops=1000000, seed=1.\n"
      "high-count defaults to 0 for get/allocated, max(N/100,threads)\n"
      "for high, and 1 for hot; claim/promote require zero.\n"
      "--validate performs checks without timing. Timed invocations require\n"
      "an externally coordinated exclusive benchmark window.\n");
}

static void
parse(benchmark *b, int argc, char **argv)
{
   b->extents          = UINT64_C(1048576);
   b->threads          = 1;
   b->operations       = UINT64_C(1000000);
   b->seed             = 1;
   bool high_count_set = false;
   for (int i = 1; i < argc; i++) {
      const char *key = argv[i];
      if (strcmp(key, "--help") == 0) {
         usage(stdout);
         exit(0);
      }
      if (strcmp(key, "--validate") == 0) {
         b->validate_only = true;
         continue;
      }
      if (++i >= argc) {
         fail("option requires a value");
      }
      const char *value = argv[i];
      if (strcmp(key, "--backend") == 0)
         b->backend = value;
      else if (strcmp(key, "--workload") == 0)
         b->workload_name = value;
      else if (strcmp(key, "--extents") == 0)
         b->extents = number(value);
      else if (strcmp(key, "--threads") == 0)
         b->threads = number(value);
      else if (strcmp(key, "--ops") == 0)
         b->operations = number(value);
      else if (strcmp(key, "--seed") == 0)
         b->seed = number(value);
      else if (strcmp(key, "--high-count") == 0) {
         b->high_count  = number(value);
         high_count_set = true;
      } else
         fail("unknown option");
   }
   if (b->backend == NULL || b->workload_name == NULL) {
      usage(stderr);
      fail("backend and workload are required");
   }
   if (strcmp(b->backend, "dense") != 0 && strcmp(b->backend, "compact") != 0) {
      fail("backend must be dense or compact");
   }
   const char *names[] = {
      "get", "allocated", "claim", "promote", "high", "hot"};
   bool found = false;
   for (unsigned i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
      if (strcmp(b->workload_name, names[i]) == 0) {
         b->work = (workload)i;
         found   = true;
      }
   }
   if (!found)
      fail("unknown workload");
   if (b->threads == 0 || b->threads >= MAX_THREADS || b->extents < b->threads
       || b->extents > SIZE_MAX / sizeof(refcount) || b->operations == 0
       || b->operations > UINT64_C(1000000000))
   {
      fail("require 1<=threads<64, extents>=threads, representable bytes, and "
           "1<=ops<=1000000000");
   }
   if (!high_count_set) {
      if (b->work == WORK_HIGH) {
         b->high_count = b->extents / 100;
         if (b->high_count < b->threads)
            b->high_count = b->threads;
      } else if (b->work == WORK_HOT) {
         b->high_count = 1;
      }
   }
   if (b->high_count > b->extents
       || ((b->work == WORK_CLAIM || b->work == WORK_PROMOTE)
           && b->high_count != 0)
       || (b->work == WORK_HIGH && b->high_count < b->threads)
       || (b->work == WORK_HOT && b->high_count != 1))
   {
      fail("high-count incompatible with workload or capacity");
   }
}

static void
setup(benchmark *b)
{
   if (platform_register_thread() != 0)
      fail("main thread registration failed");
   if (strcmp(b->backend, "dense") == 0) {
      dense_map *map = calloc(1, sizeof(*map));
      if (map == NULL)
         fail("dense container allocation failed");
      map->capacity = b->extents;
      /* Keep the standalone dense control's buffer page aligned. */
      if (posix_memalign(
             (void **)&map->counts, 4096, b->extents * sizeof(*map->counts))
          != 0)
         fail("dense count allocation failed");
      memset(map->counts, 0, b->extents * sizeof(*map->counts));
      b->map = map;
      b->ops = (map_ops){dense_get,
                         dense_allocated,
                         dense_claim,
                         dense_inc,
                         dense_dec,
                         dense_set};
   } else {
      rc_refcount *map = NULL;
      if (posix_memalign((void **)&map, _Alignof(rc_refcount), sizeof(*map))
          != 0)
      {
         fail("compact container allocation failed");
      }
      memset(map, 0, sizeof(*map));
      check_status(
         rc_refcount_init(
            map, b->extents, PROCESS_PRIVATE_HEAP_ID, platform_get_module_id()),
         "compact init");
      b->map = map;
      b->ops = (map_ops){compact_get,
                         compact_allocated,
                         compact_claim,
                         compact_inc,
                         compact_dec,
                         compact_set};
   }
   for (uint64 e = 0; e < b->extents; e++) {
      check_status(b->ops.set(b->map, e, initial_count(b, e)),
                   "initialize count");
   }
   /* Promotion runs can have at most threads outstanding overflow entries.
    * Warm every participating shard to hold that many entries, even if all
    * workers happen to hash to one shard. No table allocation belongs in the
    * timed promotion loop. Retained allocation is included in reported bytes.
    */
   if (b->work == WORK_PROMOTE && strcmp(b->backend, "compact") == 0) {
      rc_refcount *map    = b->map;
      uint64       warmed = 0;
      for (; warmed < b->extents; warmed++) {
         check_status(rc_refcount_set(map, warmed, RC_REFCOUNT_ESCAPE),
                      "warm promotion tables");
         bool ready = true;
         for (uint64 shard = 0; shard < RC_REFCOUNT_SHARD_COUNT; shard++) {
            if (map->shards[shard].capacity * 7 / 10 < b->threads) {
               ready = false;
               break;
            }
         }
         if (ready) {
            warmed++;
            break;
         }
      }
      for (uint64 e = 0; e < warmed; e++) {
         check_status(rc_refcount_set(map, e, initial_count(b, e)),
                      "clear warm entries");
      }
   }
   if (b->work == WORK_PROMOTE && strcmp(b->backend, "compact") == 0) {
      rc_refcount_usage usage;
      refcount          promoted = 0;
      check_status(b->ops.inc(b->map, 0, &promoted), "verify promotion");
      rc_refcount_get_usage(b->map, &usage);
      if (promoted != RC_REFCOUNT_ESCAPE || usage.overflow_entries != 1) {
         fail("promotion did not create exactly one escaped count");
      }
      refcount demoted = b->ops.dec(b->map, 0);
      rc_refcount_get_usage(b->map, &usage);
      if (demoted != RC_REFCOUNT_ESCAPE - 1 || usage.overflow_entries != 0) {
         fail("demotion did not remove the escaped count");
      }
   }
   if (pthread_mutex_init(&b->start_lock, NULL) != 0
       || pthread_cond_init(&b->start_cond, NULL) != 0)
   {
      fail("start synchronization initialization failed");
   }
}

static rc_refcount_usage
memory_usage(benchmark *b)
{
   rc_refcount_usage usage = {0};
   if (strcmp(b->backend, "compact") == 0) {
      rc_refcount_get_usage(b->map, &usage);
   } else {
      usage.container_bytes = sizeof(dense_map);
      usage.packed_bytes    = b->extents * sizeof(refcount);
   }
   return usage;
}

int
main(int argc, char **argv)
{
   benchmark b = {0};
   parse(&b, argc, argv);
   setup(&b);
   rc_refcount_usage before               = memory_usage(&b);
   pthread_t         threads[MAX_THREADS] = {0};
   worker            workers[MAX_THREADS] = {0};
   for (uint64 i = 0; i < b.threads; i++) {
      workers[i].bench = &b;
      workers[i].id    = i;
      if (pthread_create(&threads[i], NULL, run_worker, &workers[i]) != 0) {
         fail("worker creation failed");
      }
   }
   pthread_mutex_lock(&b.start_lock);
   while (b.ready < b.threads) {
      if (pthread_cond_wait(&b.start_cond, &b.start_lock) != 0) {
         fail("main ready wait failed");
      }
   }
   uint64 started = b.validate_only ? 0 : now_ns();
   b.start        = true;
   pthread_cond_broadcast(&b.start_cond);
   pthread_mutex_unlock(&b.start_lock);
   uint64 ended = 0, checksum = 0;
   for (uint64 i = 0; i < b.threads; i++) {
      if (pthread_join(threads[i], NULL) != 0)
         fail("worker join failed");
      if (workers[i].ended_ns > ended)
         ended = workers[i].ended_ns;
      checksum += workers[i].checksum;
   }
   rc_refcount_usage after = memory_usage(&b);
   if (before.overflow_slots != after.overflow_slots) {
      fail("overflow table grew during measured operations; invalid sample");
   }
   for (uint64 e = 0; e < b.extents; e++) {
      if (b.ops.get(b.map, e) != initial_count(&b, e)) {
         fail("final full-map validation failed");
      }
   }
   uint64 expected_entries = b.high_count;
   if (strcmp(b.backend, "compact") == 0
       && after.overflow_entries != expected_entries)
   {
      fail("unexpected final overflow entry count");
   }
   uint64 factor  = b.work == WORK_CLAIM ? 3 : (b.work >= WORK_PROMOTE ? 2 : 1);
   uint64 logical = b.operations * b.threads;
   printf("{\"backend\":\"%s\",\"workload\":\"%s\",\"extents\":%" PRIu64
          ",\"count_bits\":64,\"packed_tag_bits\":4,\"escape_raw_count\":15"
          ",\"backend_role\":\"%s\""
          ",\"threads\":%" PRIu64 ",\"ops_per_thread\":%" PRIu64
          ",\"logical_operations\":%" PRIu64
          ",\"primitive_operations\":%" PRIu64 ",\"high_count\":%" PRIu64
          ",\"seed\":%" PRIu64 ",\"validation_only\":%s,\"elapsed_ns\":",
          b.backend,
          b.workload_name,
          b.extents,
          strcmp(b.backend, "dense") == 0 ? "standalone_dense64_cas_control"
                                          : "production_four_bit_container",
          b.threads,
          b.operations,
          logical,
          logical * factor,
          b.high_count,
          b.seed,
          b.validate_only ? "true" : "false");
   if (b.validate_only)
      printf("null");
   else
      printf("%" PRIu64, ended - started);
   printf(",\"checksum\":%" PRIu64 ",\"validation\":\"PASS\""
          ",\"memory\":{\"container_bytes\":%" PRIu64
          ",\"count_array_bytes\":%" PRIu64 ",\"overflow_bytes\":%" PRIu64
          ",\"overflow_slots\":%" PRIu64 ",\"overflow_entries\":%" PRIu64
          ",\"total_bytes\":%" PRIu64 "}}\n",
          checksum,
          after.container_bytes,
          after.packed_bytes,
          after.overflow_bytes,
          after.overflow_slots,
          after.overflow_entries,
          after.container_bytes + after.packed_bytes + after.overflow_bytes);
   pthread_cond_destroy(&b.start_cond);
   pthread_mutex_destroy(&b.start_lock);
   if (strcmp(b.backend, "compact") == 0) {
      rc_refcount_deinit(b.map);
   } else {
      free(((dense_map *)b.map)->counts);
   }
   free(b.map);
   platform_deregister_thread();
   return 0;
}
