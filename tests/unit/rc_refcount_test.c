// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include <pthread.h>
#include <sys/wait.h>
#include <unistd.h>

#include "ctest.h"
#include "platform_threads.h"
#include "platform_typed_alloc.h"
#include "rc_refcount.h"
#include "unit_tests.h"

#define TEST_EXTENTS 65536

CTEST_DATA(rc_refcount)
{
   platform_heap_id heap_id;
   rc_refcount     *map;
};

CTEST_SETUP(rc_refcount)
{
   ASSERT_EQUAL(0, platform_register_thread());
   bool use_shmem = FALSE;
   for (int arg = 1; arg < Ctest_argc; arg++) {
      use_shmem |= strcmp(Ctest_argv[arg], "--use-shmem") == 0;
   }
   ASSERT_TRUE(SUCCESS(platform_heap_create(
      platform_get_module_id(), 16 * Mega, use_shmem, &data->heap_id)));
   data->map = TYPED_ZALLOC(data->heap_id, data->map);
   ASSERT_NOT_NULL(data->map);
   ASSERT_TRUE(SUCCESS(rc_refcount_init(
      data->map, TEST_EXTENTS, data->heap_id, platform_get_module_id())));
}

CTEST_TEARDOWN(rc_refcount)
{
   rc_refcount_deinit(data->map);
   platform_free(data->heap_id, data->map);
   platform_heap_destroy(&data->heap_id);
   platform_deregister_thread();
}

CTEST2(rc_refcount, failed_init_empty_reset_and_partial_word)
{
   rc_refcount *temporary = TYPED_ZALLOC(data->heap_id, temporary);
   ASSERT_NOT_NULL(temporary);
   ASSERT_TRUE(STATUS_IS_EQ(
      STATUS_BAD_PARAM,
      rc_refcount_init(temporary, 0, data->heap_id, platform_get_module_id())));
   rc_refcount_deinit(temporary);
   ASSERT_TRUE(SUCCESS(rc_refcount_init(
      temporary, 65, data->heap_id, platform_get_module_id())));
   rc_refcount_reset(temporary);
   ASSERT_TRUE(rc_refcount_try_claim(temporary, 64));
   refcount value = 0;
   ASSERT_TRUE(SUCCESS(rc_refcount_try_inc(temporary, 64, &value)));
   ASSERT_EQUAL(3, value);
   ASSERT_EQUAL(0, rc_refcount_get(temporary, 63));
   rc_refcount_reset(temporary);
   ASSERT_EQUAL(0, rc_refcount_get(temporary, 64));
   rc_refcount_deinit(temporary);
   rc_refcount_deinit(temporary);
   platform_free(data->heap_id, temporary);
}

CTEST2(rc_refcount, inline_escape_max_and_reset)
{
   uint64 extents[] = {0,
                       1,
                       RC_REFCOUNT_COUNTS_PER_WORD - 1,
                       RC_REFCOUNT_COUNTS_PER_WORD,
                       2 * RC_REFCOUNT_COUNTS_PER_WORD - 1,
                       2 * RC_REFCOUNT_COUNTS_PER_WORD,
                       TEST_EXTENTS - 1};
   for (uint64 i = 0; i < ARRAY_SIZE(extents); i++) {
      uint64 extent = extents[i];
      ASSERT_FALSE(rc_refcount_is_allocated(data->map, extent));
      ASSERT_TRUE(rc_refcount_try_claim(data->map, extent));
      ASSERT_FALSE(rc_refcount_try_claim(data->map, extent));
      ASSERT_TRUE(rc_refcount_is_allocated(data->map, extent));
      refcount result = 0;
      ASSERT_TRUE(SUCCESS(rc_refcount_try_inc(data->map, extent, &result)));
      ASSERT_EQUAL(3, result);
      ASSERT_EQUAL(3, rc_refcount_get(data->map, extent));
      ASSERT_EQUAL(2, rc_refcount_dec(data->map, extent));
      ASSERT_EQUAL(1, rc_refcount_dec(data->map, extent));
      ASSERT_TRUE(SUCCESS(rc_refcount_try_inc(data->map, extent, &result)));
      ASSERT_EQUAL(2, result);
      ASSERT_EQUAL(1, rc_refcount_dec(data->map, extent));
      ASSERT_EQUAL(0, rc_refcount_dec(data->map, extent));
      result = 42;
      ASSERT_TRUE(
         STATUS_IS_EQ(STATUS_INVALID_STATE,
                      rc_refcount_try_inc(data->map, extent, &result)));
      ASSERT_EQUAL(42, result);
   }
   ASSERT_TRUE(SUCCESS(rc_refcount_set(data->map, 32, UINT64_MAX)));
   refcount result = 99;
   ASSERT_TRUE(STATUS_IS_EQ(STATUS_LIMIT_EXCEEDED,
                            rc_refcount_try_inc(data->map, 32, &result)));
   ASSERT_EQUAL(99, result);
   ASSERT_EQUAL_U(UINT64_MAX, rc_refcount_get(data->map, 32));
   ASSERT_EQUAL_U(UINT64_MAX - 1, rc_refcount_dec(data->map, 32));
   rc_refcount_reset(data->map);
   rc_refcount_usage usage;
   rc_refcount_get_usage(data->map, &usage);
   ASSERT_EQUAL(TEST_EXTENTS * SPLINTER_RC_BITS / 8, usage.packed_bytes);
   ASSERT_EQUAL(0, usage.overflow_bytes);
   ASSERT_EQUAL(0, usage.overflow_entries);
   ASSERT_EQUAL(0, usage.overflow_slots);
   for (uint64 extent = 0; extent < TEST_EXTENTS; extent++) {
      ASSERT_EQUAL(0, rc_refcount_get(data->map, extent));
   }
}

CTEST2(rc_refcount, wide_counts_cross_uint32_and_signed_boundary)
{
   ASSERT_TRUE(SUCCESS(rc_refcount_set(data->map, 1, UINT32_MAX - 1)));
   refcount result = 0;
   ASSERT_TRUE(SUCCESS(rc_refcount_try_inc(data->map, 1, &result)));
   ASSERT_EQUAL_U(UINT32_MAX, result);
   ASSERT_TRUE(SUCCESS(rc_refcount_try_inc(data->map, 1, &result)));
   ASSERT_EQUAL_U((uint64)UINT32_MAX + 1, result);
   ASSERT_EQUAL_U((uint64)UINT32_MAX + 1, rc_refcount_get(data->map, 1));
   ASSERT_EQUAL_U(UINT32_MAX, rc_refcount_dec(data->map, 1));

   const refcount high = UINT64_C(1) << 63;
   ASSERT_TRUE(SUCCESS(rc_refcount_set(data->map, 1, high)));
   ASSERT_EQUAL_U(high, rc_refcount_get(data->map, 1));
   ASSERT_TRUE(SUCCESS(rc_refcount_try_inc(data->map, 1, &result)));
   ASSERT_EQUAL_U(high + 1, result);
   ASSERT_EQUAL_U(high, rc_refcount_dec(data->map, 1));

   ASSERT_TRUE(SUCCESS(rc_refcount_set(data->map, 1, UINT64_MAX - 1)));
   ASSERT_TRUE(SUCCESS(rc_refcount_try_inc(data->map, 1, &result)));
   ASSERT_EQUAL_U(UINT64_MAX, result);
   result = high + 37;
   ASSERT_TRUE(STATUS_IS_EQ(STATUS_LIMIT_EXCEEDED,
                            rc_refcount_try_inc(data->map, 1, &result)));
   ASSERT_EQUAL_U(high + 37, result);
   ASSERT_EQUAL_U(UINT64_MAX, rc_refcount_get(data->map, 1));
   ASSERT_EQUAL_U(UINT64_MAX - 1, rc_refcount_dec(data->map, 1));
}

CTEST2(rc_refcount, every_inline_count_and_escape_boundary)
{
   const uint64 extent = RC_REFCOUNT_COUNTS_PER_WORD - 1;
   ASSERT_TRUE(rc_refcount_try_claim(data->map, extent - 1));
   ASSERT_TRUE(rc_refcount_try_claim(data->map, extent + 1));
   ASSERT_TRUE(SUCCESS(rc_refcount_set(data->map, extent, AL_NO_REFS)));
   debug_code(data->map->test_allocations_before_failure = 0);
   for (refcount count = AL_ONE_REF; count < RC_REFCOUNT_ESCAPE; count++) {
      refcount result = 0;
      ASSERT_TRUE(SUCCESS(rc_refcount_try_inc(data->map, extent, &result)));
      ASSERT_EQUAL(count, result);
      ASSERT_EQUAL(count, rc_refcount_get(data->map, extent));
   }
   rc_refcount_usage usage;
   rc_refcount_get_usage(data->map, &usage);
   ASSERT_EQUAL(0, usage.overflow_entries);
   ASSERT_EQUAL(0, usage.overflow_slots);
   debug_code(data->map->test_allocations_before_failure = -1);
   for (refcount count = RC_REFCOUNT_ESCAPE; count <= RC_REFCOUNT_ESCAPE + 2;
        count++)
   {
      refcount result = 0;
      ASSERT_TRUE(SUCCESS(rc_refcount_try_inc(data->map, extent, &result)));
      ASSERT_EQUAL(count, result);
   }
   rc_refcount_get_usage(data->map, &usage);
   ASSERT_EQUAL(1, usage.overflow_entries);
   for (refcount count = RC_REFCOUNT_ESCAPE + 1; count > 0; count--) {
      ASSERT_EQUAL(count, rc_refcount_dec(data->map, extent));
      ASSERT_EQUAL(count, rc_refcount_get(data->map, extent));
      rc_refcount_get_usage(data->map, &usage);
      ASSERT_EQUAL(count >= RC_REFCOUNT_ESCAPE, usage.overflow_entries);
   }
   ASSERT_EQUAL(0, rc_refcount_dec(data->map, extent));
   ASSERT_EQUAL(AL_ONE_REF, rc_refcount_get(data->map, extent - 1));
   ASSERT_EQUAL(AL_ONE_REF, rc_refcount_get(data->map, extent + 1));
}

static uint64
next_random(uint64 *state)
{
   *state ^= *state << 13;
   *state ^= *state >> 7;
   *state ^= *state << 17;
   return *state;
}

CTEST2(rc_refcount, differential_churn)
{
   refcount reference[257] = {0};
   uint64   state          = 0x123456789abcdefULL;
   uint64   reuses         = 0;
   bool32   seen[257]      = {0};
   for (uint64 step = 0; step < 100000; step++) {
      uint64 extent = next_random(&state) % ARRAY_SIZE(reference);
      uint64 op     = next_random(&state) % 4;
      if (reference[extent] == 0) {
         ASSERT_TRUE(rc_refcount_try_claim(data->map, extent));
         reference[extent] = 2;
         reuses += seen[extent];
         seen[extent] = TRUE;
      } else if (op == 0) {
         refcount value = 0;
         ASSERT_TRUE(SUCCESS(rc_refcount_try_inc(data->map, extent, &value)));
         ASSERT_EQUAL(++reference[extent], value);
      } else if (op < 3) {
         ASSERT_EQUAL(--reference[extent], rc_refcount_dec(data->map, extent));
      } else {
         ASSERT_EQUAL(reference[extent], rc_refcount_get(data->map, extent));
      }
      ASSERT_EQUAL(reference[extent] != 0,
                   rc_refcount_is_allocated(data->map, extent));
      if (step % 1024 == 0) {
         for (uint64 j = 0; j < ARRAY_SIZE(reference); j++) {
            ASSERT_EQUAL(reference[j], rc_refcount_get(data->map, j));
         }
      }
   }
   ASSERT_TRUE(reuses > 1000);
   for (uint64 j = 0; j < ARRAY_SIZE(reference); j++) {
      ASSERT_EQUAL(reference[j], rc_refcount_get(data->map, j));
   }
}

/* Reproduce the hash partition solely to construct a wraparound collision
 * cluster, not as an oracle for the counter results. */
static uint64
collision_hash(uint64 extent)
{
   extent ^= extent >> 30;
   extent *= UINT64_C(0xbf58476d1ce4e5b9);
   extent ^= extent >> 27;
   extent *= UINT64_C(0x94d049bb133111eb);
   return extent ^ (extent >> 31);
}

static void
collision_keys(uint64 *keys, uint64 count)
{
   uint64 found = 0;
   for (uint64 extent = 0; extent < TEST_EXTENTS && found < count; extent++) {
      /* Same shard, home bucket 15 of the initial 16-slot table. */
      if ((collision_hash(extent) & 1023) == (15 << 6)) {
         keys[found++] = extent;
      }
   }
   ASSERT_EQUAL(count, found);
}

CTEST2(rc_refcount, collision_delete_and_growth)
{
   const refcount high = UINT64_C(1) << 40;
   uint64         keys[32];
   collision_keys(keys, ARRAY_SIZE(keys));
   for (uint64 i = 0; i < ARRAY_SIZE(keys); i++) {
      ASSERT_TRUE(SUCCESS(rc_refcount_set(data->map, keys[i], high + i)));
   }
   for (uint64 i = 0; i < ARRAY_SIZE(keys); i += 2) {
      ASSERT_TRUE(SUCCESS(rc_refcount_set(data->map, keys[i], 2)));
      for (uint64 j = i + 1; j < ARRAY_SIZE(keys); j++) {
         ASSERT_EQUAL(high + j, rc_refcount_get(data->map, keys[j]));
      }
   }
   for (uint64 i = 0; i < ARRAY_SIZE(keys); i++) {
      ASSERT_EQUAL((i % 2) ? high + i : 2, rc_refcount_get(data->map, keys[i]));
   }
   rc_refcount_usage usage;
   rc_refcount_get_usage(data->map, &usage);
   ASSERT_EQUAL(16, usage.overflow_entries);
   ASSERT_EQUAL(64, usage.overflow_slots);
   /* Reinsert into the holes, then delete all entries in reverse order. */
   for (uint64 i = 0; i < ARRAY_SIZE(keys); i += 2) {
      ASSERT_TRUE(SUCCESS(rc_refcount_set(data->map, keys[i], high + i)));
   }
   for (uint64 i = ARRAY_SIZE(keys); i > 0; i--) {
      ASSERT_TRUE(SUCCESS(rc_refcount_set(data->map, keys[i - 1], 0)));
   }
   rc_refcount_get_usage(data->map, &usage);
   ASSERT_EQUAL(0, usage.overflow_entries);
   ASSERT_EQUAL(64, usage.overflow_slots);
}

typedef struct worker_args {
   rc_refcount *map;
   uint64       extent;
   uint64       iterations;
   bool32       claim_cycles;
} worker_args;

static void
run_counter_worker(worker_args *args)
{
   for (uint64 i = 0; i < args->iterations; i++) {
      if (args->claim_cycles) {
         platform_assert(rc_refcount_try_claim(args->map, args->extent));
      }
      refcount        value = 0;
      platform_status status =
         rc_refcount_try_inc(args->map, args->extent, &value);
      platform_assert_status_ok(status);
      platform_assert(value >= 3);
      platform_assert(rc_refcount_dec(args->map, args->extent) >= 2);
      if (args->claim_cycles) {
         platform_assert(rc_refcount_dec(args->map, args->extent) == 1);
         platform_assert(rc_refcount_dec(args->map, args->extent) == 0);
      }
   }
}

static void *
counter_worker(void *arg)
{
   platform_assert(platform_register_thread() == 0);
   run_counter_worker(arg);
   platform_deregister_thread();
   return NULL;
}

CTEST2(rc_refcount, neighboring_lanes_and_shared_counter)
{
   enum { THREADS = 8 };
   pthread_t   threads[THREADS];
   worker_args args[THREADS];
   for (uint64 round = 0; round < 2; round++) {
      if (round == 1) {
         ASSERT_TRUE(
            SUCCESS(rc_refcount_set(data->map, 1, RC_REFCOUNT_ESCAPE - 1)));
      }
      for (uint64 i = 0; i < THREADS; i++) {
         args[i] = (worker_args){.map          = data->map,
                                 .extent       = round == 0 ? i + 1 : 1,
                                 .iterations   = 4096,
                                 .claim_cycles = round == 0};
         ASSERT_EQUAL(
            0, pthread_create(&threads[i], NULL, counter_worker, &args[i]));
      }
      for (uint64 i = 0; i < THREADS; i++) {
         ASSERT_EQUAL(0, pthread_join(threads[i], NULL));
      }
      for (uint64 i = 0; i < THREADS; i++) {
         ASSERT_EQUAL(round == 0 ? 0 : RC_REFCOUNT_ESCAPE - 1,
                      rc_refcount_get(data->map, round == 0 ? i + 1 : 1));
      }
   }
}

CTEST2(rc_refcount, shared_heap_fork)
{
   platform_heap_id heap_id;
   ASSERT_TRUE(SUCCESS(platform_heap_create(
      platform_get_module_id(), 16 * Mega, TRUE, &heap_id)));
   rc_refcount *map = TYPED_ZALLOC(heap_id, map);
   ASSERT_NOT_NULL(map);
   ASSERT_TRUE(
      SUCCESS(rc_refcount_init(map, 65, heap_id, platform_get_module_id())));
   ASSERT_TRUE(SUCCESS(rc_refcount_set(map, 1, RC_REFCOUNT_ESCAPE - 1)));
   worker_args args  = {.map = map, .extent = 1, .iterations = 4096};
   pid_t       child = fork();
   ASSERT_TRUE(child >= 0);
   if (child == 0) {
      if (platform_register_thread() != 0) {
         _exit(1);
      }
      run_counter_worker(&args);
      platform_deregister_thread();
      _exit(0);
   }
   run_counter_worker(&args);
   int status = -1;
   ASSERT_EQUAL(child, waitpid(child, &status, 0));
   ASSERT_TRUE(WIFEXITED(status));
   ASSERT_EQUAL(0, WEXITSTATUS(status));
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 1, rc_refcount_get(map, 1));
   rc_refcount_deinit(map);
   platform_free(heap_id, map);
   platform_heap_destroy(&heap_id);
}

#if SPLINTER_DEBUG
CTEST2(rc_refcount, allocation_failure_preserves_count)
{
   uint64 keys[12];
   collision_keys(keys, ARRAY_SIZE(keys));
   ASSERT_TRUE(
      SUCCESS(rc_refcount_set(data->map, keys[0], RC_REFCOUNT_ESCAPE - 1)));
   data->map->test_allocations_before_failure = 0;
   refcount value                             = 77;
   ASSERT_TRUE(STATUS_IS_EQ(STATUS_NO_MEMORY,
                            rc_refcount_try_inc(data->map, keys[0], &value)));
   ASSERT_EQUAL(77, value);
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 1, rc_refcount_get(data->map, keys[0]));
   ASSERT_TRUE(STATUS_IS_EQ(
      STATUS_NO_MEMORY,
      rc_refcount_set(data->map, keys[0], RC_REFCOUNT_ESCAPE + 10)));
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 1, rc_refcount_get(data->map, keys[0]));
   data->map->test_allocations_before_failure = 1;
   for (uint64 i = 0; i < 11; i++) {
      ASSERT_TRUE(
         SUCCESS(rc_refcount_set(data->map, keys[i], RC_REFCOUNT_ESCAPE + i)));
   }
   ASSERT_TRUE(
      SUCCESS(rc_refcount_set(data->map, keys[11], RC_REFCOUNT_ESCAPE - 1)));
   ASSERT_TRUE(STATUS_IS_EQ(STATUS_NO_MEMORY,
                            rc_refcount_try_inc(data->map, keys[11], &value)));
   ASSERT_EQUAL(77, value);
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 1, rc_refcount_get(data->map, keys[11]));
   for (uint64 i = 0; i < 11; i++) {
      ASSERT_EQUAL(RC_REFCOUNT_ESCAPE + i, rc_refcount_get(data->map, keys[i]));
   }
   /* Deletion and reusing existing capacity must not allocate. */
   ASSERT_TRUE(SUCCESS(rc_refcount_set(data->map, keys[0], 2)));
   ASSERT_TRUE(SUCCESS(rc_refcount_try_inc(data->map, keys[11], &value)));
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE, value);
}

typedef struct race_gate {
   pthread_mutex_t             lock;
   pthread_cond_t              condition;
   rc_refcount_test_hook_event blocked_event;
   bool32                      reached;
   bool32                      release;
   bool32                      increment_waiting;
} race_gate;

static void
race_hook(void *arg, rc_refcount_test_hook_event event, uint64 extent)
{
   race_gate *gate = arg;
   if (extent != 1) {
      return;
   }
   pthread_mutex_lock(&gate->lock);
   if (event == RC_REFCOUNT_TEST_INCREMENT_LOCK_WAIT) {
      gate->increment_waiting = TRUE;
      pthread_cond_broadcast(&gate->condition);
   }
   if (event == gate->blocked_event && !gate->reached) {
      gate->reached = TRUE;
      pthread_cond_broadcast(&gate->condition);
      while (!gate->release) {
         pthread_cond_wait(&gate->condition, &gate->lock);
      }
   }
   pthread_mutex_unlock(&gate->lock);
}

static void
gate_init(race_gate *gate, rc_refcount *map, rc_refcount_test_hook_event event)
{
   memset(gate, 0, sizeof(*gate));
   ASSERT_EQUAL(0, pthread_mutex_init(&gate->lock, NULL));
   ASSERT_EQUAL(0, pthread_cond_init(&gate->condition, NULL));
   gate->blocked_event = event;
   map->test_hook      = race_hook;
   map->test_hook_arg  = gate;
}

static void
gate_wait(race_gate *gate, bool32 wait_for_increment)
{
   pthread_mutex_lock(&gate->lock);
   while (wait_for_increment ? !gate->increment_waiting : !gate->reached) {
      pthread_cond_wait(&gate->condition, &gate->lock);
   }
   pthread_mutex_unlock(&gate->lock);
}

static void
gate_release(race_gate *gate)
{
   pthread_mutex_lock(&gate->lock);
   gate->release = TRUE;
   pthread_cond_broadcast(&gate->condition);
   pthread_mutex_unlock(&gate->lock);
}

static void
gate_destroy(race_gate *gate, rc_refcount *map)
{
   map->test_hook     = NULL;
   map->test_hook_arg = NULL;
   ASSERT_EQUAL(0, pthread_cond_destroy(&gate->condition));
   ASSERT_EQUAL(0, pthread_mutex_destroy(&gate->lock));
}

typedef struct race_operation {
   rc_refcount    *map;
   char            op;
   refcount        result;
   platform_status status;
} race_operation;

static void *
race_worker(void *arg)
{
   race_operation *operation = arg;
   platform_assert(platform_register_thread() == 0);
   if (operation->op == 'g') {
      operation->result = rc_refcount_get(operation->map, 1);
   } else if (operation->op == 'd') {
      operation->result = rc_refcount_dec(operation->map, 1);
   } else {
      operation->status =
         rc_refcount_try_inc(operation->map, 1, &operation->result);
   }
   platform_deregister_thread();
   return NULL;
}

CTEST2(rc_refcount, stale_escape_reader_rechecks_after_demotion)
{
   ASSERT_TRUE(SUCCESS(rc_refcount_set(data->map, 1, RC_REFCOUNT_ESCAPE)));
   race_gate gate;
   gate_init(&gate, data->map, RC_REFCOUNT_TEST_ESCAPE_OBSERVED);
   race_operation reader = {.map = data->map, .op = 'g'};
   pthread_t      thread;
   ASSERT_EQUAL(0, pthread_create(&thread, NULL, race_worker, &reader));
   gate_wait(&gate, FALSE);
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 1, rc_refcount_dec(data->map, 1));
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 2, rc_refcount_dec(data->map, 1));
   gate_release(&gate);
   ASSERT_EQUAL(0, pthread_join(thread, NULL));
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 2, reader.result);
   gate_destroy(&gate, data->map);
}

CTEST2(rc_refcount, failed_promotion_removes_tentative_entry)
{
   ASSERT_TRUE(SUCCESS(rc_refcount_set(data->map, 1, RC_REFCOUNT_ESCAPE - 1)));
   race_gate gate;
   gate_init(&gate, data->map, RC_REFCOUNT_TEST_PROMOTE_INSERTED);
   race_operation increment = {.map = data->map, .op = 'i'};
   pthread_t      thread;
   ASSERT_EQUAL(0, pthread_create(&thread, NULL, race_worker, &increment));
   gate_wait(&gate, FALSE);
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 2, rc_refcount_dec(data->map, 1));
   gate_release(&gate);
   ASSERT_EQUAL(0, pthread_join(thread, NULL));
   ASSERT_TRUE(SUCCESS(increment.status));
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 1, increment.result);
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 1, rc_refcount_get(data->map, 1));
   rc_refcount_usage usage;
   rc_refcount_get_usage(data->map, &usage);
   ASSERT_EQUAL(0, usage.overflow_entries);
   gate_destroy(&gate, data->map);
}

CTEST2(rc_refcount, promotion_waits_for_demotion_entry_removal)
{
   ASSERT_TRUE(SUCCESS(rc_refcount_set(data->map, 1, RC_REFCOUNT_ESCAPE)));
   race_gate gate;
   gate_init(&gate, data->map, RC_REFCOUNT_TEST_DEMOTE_PUBLISHED);
   race_operation decrement = {.map = data->map, .op = 'd'};
   pthread_t      down;
   ASSERT_EQUAL(0, pthread_create(&down, NULL, race_worker, &decrement));
   gate_wait(&gate, FALSE);
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 1, rc_refcount_get(data->map, 1));
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 2, rc_refcount_dec(data->map, 1));
   refcount value;
   ASSERT_TRUE(SUCCESS(rc_refcount_try_inc(data->map, 1, &value)));
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 1, value);
   race_operation increment = {.map = data->map, .op = 'i'};
   pthread_t      up;
   ASSERT_EQUAL(0, pthread_create(&up, NULL, race_worker, &increment));
   gate_wait(&gate, TRUE);
   gate_release(&gate);
   ASSERT_EQUAL(0, pthread_join(down, NULL));
   ASSERT_EQUAL(0, pthread_join(up, NULL));
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE - 1, decrement.result);
   ASSERT_TRUE(SUCCESS(increment.status));
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE, increment.result);
   ASSERT_EQUAL(RC_REFCOUNT_ESCAPE, rc_refcount_get(data->map, 1));
   rc_refcount_usage usage;
   rc_refcount_get_usage(data->map, &usage);
   ASSERT_EQUAL(1, usage.overflow_entries);
   gate_destroy(&gate, data->map);
}
#endif
