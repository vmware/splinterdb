// Copyright 2021-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinterdb_quick_test.c --
 *
 *     Quick test of the public API of SplinterDB
 *
 * NOTE: This test case file also serves as an example for how-to build
 *  CTests, and the syntax for different commands etc. Note the
 *  annotations to learn how to write new unit-tests using Ctests.
 *
 * Naming Conventions:
 *
 *  o The file containing unit-test cases for a module / functionality is
 *    expected to be named <something>_test.c
 *
 *  o Individual test cases [ see below ] in a file are prefaced with a
 *    term naming the test suite, for the module / functionality being tested.
 *    Usually it will just be <something>; .e.g., in splinterdb_test.c
 *    the suite-name is 'splinterdb_quick'.
 *
 *  o Each test case should be named test_<operation>
 * -----------------------------------------------------------------------------
 */
#include <stdlib.h> // Needed for system calls; e.g. free
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <sys/wait.h>
#include <unistd.h>
#include "shard_log.h"

#include "platform_threads.h"
#include "platform_sleep.h"
#include "splinterdb/splinterdb.h"
#include "splinterdb/data.h"
#include "splinterdb/default_data_config.h"
#include "unit_tests.h"
#include "util.h"
#include "test_data.h"
#include "ctest.h" // This is required for all test-case files.
#include "btree.h" // for MAX_INLINE_MESSAGE_SIZE
#include "config.h"
#include "splinterdb_tests_private.h"
#include "clockcache.h"

#define TEST_LARGE_KEY_SIZE 23

/* -1 for message encoding overhead */
#define TEST_MAX_VALUE_SIZE 32

// Hard-coded format strings to generate key and values
static const char key_fmt[] = "key-%018x";
static const char val_fmt[] = "val-%018x";
#define KEY_FMT_LENGTH         (22)
#define VAL_FMT_LENGTH         (22)
#define TEST_INSERT_KEY_LENGTH (KEY_FMT_LENGTH + 1)
#define TEST_INSERT_VAL_LENGTH (VAL_FMT_LENGTH + 1)

// Function Prototypes
static void
create_default_cfg(splinterdb_config *out_cfg, data_config *default_data_cfg);


static int
insert_some_keys(const int num_inserts, splinterdb *kvsb);

static int
insert_keys(splinterdb *kvsb, const int minkey, int numkeys, const int incr);

static int
check_current_tuple(splinterdb_iterator *it, const int expected_i);

static void
insert_string_kv(splinterdb *kvsb, const char *k, const char *v);

static void
check_current_string_kv(splinterdb_iterator *it, const char *k, const char *v);

static int
test_two_step_iterator(splinterdb *kvsb,
                       slice       start_key,
                       int         num_keys,
                       int         minkey,
                       int         start_i,
                       int         hop_i);

static int
check_bounded_iterator_sequence(splinterdb *kvsb,
                                comparison  min_key_comparison,
                                slice       min_key,
                                comparison  max_key_comparison,
                                slice       max_key,
                                comparison  start_type,
                                const int  *expected,
                                uint64      expected_count);

static int
check_bounded_iterator_current_from_start(splinterdb *kvsb,
                                          comparison  min_key_comparison,
                                          slice       min_key,
                                          comparison  max_key_comparison,
                                          slice       max_key,
                                          comparison  start_key_comparison,
                                          slice       start_key,
                                          int         expected_i);

static int
check_bounded_iterator_boundary_from_start(splinterdb *kvsb,
                                           comparison  min_key_comparison,
                                           slice       min_key,
                                           comparison  max_key_comparison,
                                           slice       max_key,
                                           comparison  start_key_comparison,
                                           slice       start_key,
                                           bool32      step_next,
                                           int         expected_i);

static int
custom_key_comparator(const data_config *cfg, user_key key1, user_key key2);

static uint64
sum_branch_lookups(const splinterdb *kvsb);

static uint64
force_flush_current_memtable(splinterdb *kvsb);

static void
assert_lookup_result_not_found(const splinterdb_lookup_result *result);

static void
assert_lookup_result_matches_slice(const splinterdb_lookup_result *result,
                                   slice                           expected);

static void *
auto_registered_lookup_thread(void *arg);

static void *
fetch_and_update_thread(void *arg);

static void *
stats_reset_worker(void *arg);

typedef struct {
   data_config super;
   uint64      num_comparisons;
} comparison_counting_data_config;

typedef struct {
   data_config     super;
   bool32          fail;
   uint64          calls;
   volatile bool32 finalize_block_once;
   volatile bool32 finalize_blocked;
   volatile bool32 finalize_release;
   volatile bool32 merge_block_once;
   volatile bool32 merge_blocked;
   volatile bool32 merge_release;
} final_merge_data_config;

typedef struct {
   splinterdb               *kvsb;
   slice                     key;
   slice                     value;
   splinterdb_lookup_result *old_result;
   int                       rc;
} fetch_and_update_thread_args;

typedef struct {
   splinterdb *kvsb;
   int         rc;
   threadid    tid_before;
   threadid    tid_after;
} auto_registration_thread_args;

typedef struct {
   splinterdb      *kvsb;
   uint64           worker_no;
   volatile uint64 *started;
   volatile bool32 *stop;
   volatile uint64  operations;
   int              rc;
} stats_reset_worker_args;

/*
 * Global data declaration macro:
 *
 * This is converted into a struct, with a generated name prefixed by the
 * suite name. This structure is then automatically passed to all tests in
 * the test suite. In this function, declare all structures and
 * variables that you need globally to setup Splinter. This macro essentially
 * resolves to a bunch of structure declarations, so no code fragments can
 * be added here.
 *
 * NOTE: All data structures will hang off of data->, where 'data' is a
 * global static variable manufactured by CTEST_SETUP() macro.
 */
CTEST_DATA(splinterdb_quick)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;

   comparison_counting_data_config default_data_cfg;
   final_merge_data_config         final_merge_cfg;
};


// Optional setup function for suite, called before every test in suite
CTEST_SETUP(splinterdb_quick)
{
   default_data_config_init(&data->default_data_cfg.super);
   create_default_cfg(&data->cfg, &data->default_data_cfg.super);
   data->cfg.use_shmem =
      config_parse_use_shmem(Ctest_argc, (char **)Ctest_argv);

   int rc = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(TEST_MAX_VALUE_SIZE
               < MAX_INLINE_MESSAGE_SIZE(IO_DEFAULT_PAGE_SIZE));
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(splinterdb_quick)
{
   if (data->kvsb) {
      splinterdb_close(&data->kvsb, FALSE);
   }
   platform_deregister_thread();
}

CTEST2(splinterdb_quick, test_get_config_returns_effective_config)
{
   const splinterdb_config *cfg = splinterdb_get_config(data->kvsb);

   ASSERT_TRUE(cfg != NULL);
   ASSERT_TRUE(cfg != &data->cfg);

   ASSERT_STREQ(TEST_CONFIG_DEFAULT_IO_FILENAME, cfg->filename);
   ASSERT_EQUAL(64 * Mega, cfg->cache_size);
   ASSERT_EQUAL(127 * Mega, cfg->disk_size);
   ASSERT_TRUE(data->cfg.data_cfg == cfg->data_cfg);
   ASSERT_EQUAL(data->cfg.use_shmem, cfg->use_shmem);

   ASSERT_EQUAL(IO_DEFAULT_PAGE_SIZE, cfg->page_size);
   ASSERT_EQUAL(IO_DEFAULT_EXTENT_SIZE, cfg->extent_size);
   ASSERT_EQUAL(IO_DEFAULT_FLAGS, cfg->io_flags);
   ASSERT_EQUAL(IO_DEFAULT_PERMS, cfg->io_perms);
   ASSERT_EQUAL(IO_DEFAULT_ASYNC_QUEUE_DEPTH, cfg->io_async_queue_depth);

   ASSERT_EQUAL(512, cfg->blob_max_descriptor_size);
   ASSERT_EQUAL(1, cfg->btree_rough_count_height);
   ASSERT_EQUAL(27, cfg->filter_hash_size);
   ASSERT_EQUAL(9, cfg->filter_log_index_size);
   ASSERT_EQUAL(24 * Mega, cfg->memtable_capacity);
   ASSERT_EQUAL(8, cfg->fanout);
   ASSERT_EQUAL_U(UINT64_MAX, cfg->reclaim_threshold);
}

CTEST2(splinterdb_quick, test_get_cost_stats)
{
   splinterdb_cost_stats stats = {0};

   int rc = splinterdb_get_cost_stats(data->kvsb, &stats);
   ASSERT_EQUAL(0, rc);

   ASSERT_EQUAL(8, stats.target_fanout);
   ASSERT_EQUAL(0, stats.trunk_height);
   ASSERT_EQUAL(4, stats.nmemtables);

   for (uint32 mt_no = 0; mt_no < SPLINTERDB_COST_STATS_MAX_MEMTABLES; mt_no++)
   {
      ASSERT_EQUAL(0, stats.memtable_heights[mt_no]);
   }
}

/*
 * ***********************************************************************
 * All tests in each file are named with one term, which represents the
 * module / functionality you are testing. Here, it is: splinterdb_quick
 *
 * This is an individual test case, testing [usually] just one thing.
 * The 2nd term is the test-case name, e.g., 'test_basic_flow'.
 * ***********************************************************************
 */
CTEST2(splinterdb_quick, test_checkpoint_log_grace_default)
{
   const core_handle *core = splinterdb_get_trunk_handle(data->kvsb);
   ASSERT_EQUAL(2 * core->cfg.trunk_node_cfg->incorporation_size_kv_bytes,
                core->cfg.checkpoint_log_grace_bytes);
}

CTEST2(splinterdb_quick, test_durable_barrier)
{
   // Re-create the instance with logging enabled (SETUP created it without).
   int rc = splinterdb_close(&data->kvsb, FALSE);
   ASSERT_EQUAL(0, rc);
   data->cfg.use_log = TRUE;
   rc                = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   slice key   = slice_create(strlen("durable-key"), "durable-key");
   slice value = slice_create(strlen("durable-value"), "durable-value");

   rc = splinterdb_insert(data->kvsb, key, value, NULL);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_durable_barrier(data->kvsb, NULL);
   ASSERT_EQUAL(0, rc);
}

CTEST2(splinterdb_quick, test_async_durable_barrier_notifications)
{
   for (uint64 logging = 0; logging < 2; logging++) {
      for (uint64 workers = 0; workers < 2; workers++) {
         ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
         data->cfg.use_log                 = logging;
         data->cfg.num_normal_bg_threads   = workers;
         data->cfg.num_memtable_bg_threads = 0;
         ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));
         ASSERT_EQUAL(0, insert_some_keys(512, data->kvsb));

         /* Several checkpoint waits can nest on one task worker. In
          * particular, the non-WAL path must not wait for its own task to
          * become quiescent. The zero-worker variant pumps progress in wait. */
         splinterdb_notification notes[8];
         for (uint64 i = 0; i < ARRAY_SIZE(notes); i++) {
            splinterdb_notification_init_polling(&notes[i], &notes[i]);
            ASSERT_EQUAL(0, splinterdb_durable_barrier(data->kvsb, &notes[i]));
            if (workers == 0) {
               ASSERT_FALSE(splinterdb_notification_poll(&notes[i], NULL));
            }
         }
         for (uint64 i = 0; i < ARRAY_SIZE(notes); i++) {
            ASSERT_EQUAL(0,
                         splinterdb_notification_wait(data->kvsb, &notes[i]));
            int status = -1;
            ASSERT_TRUE(splinterdb_notification_poll(&notes[i], &status));
            ASSERT_EQUAL(0, status);
            ASSERT_TRUE(splinterdb_notification_user_data(&notes[i])
                        == &notes[i]);
            splinterdb_notification_deinit(&notes[i]);
         }

         ASSERT_EQUAL(0,
                      splinterdb_durable_barrier(
                         data->kvsb, SPLINTERDB_NOTIFICATION_BLOCKING));

         /* Close drains accepted work even when no thread is waiting on it. */
         splinterdb_notification closing;
         splinterdb_notification_init_polling(&closing, NULL);
         ASSERT_EQUAL(0, splinterdb_durable_barrier(data->kvsb, &closing));
         ASSERT_EQUAL(0,
                      splinterdb_durable_barrier(
                         data->kvsb, SPLINTERDB_NOTIFICATION_FIRE_AND_FORGET));
         ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
         int status = -1;
         ASSERT_TRUE(splinterdb_notification_poll(&closing, &status));
         ASSERT_EQUAL(0, status);
         splinterdb_notification_deinit(&closing);
         ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->kvsb));
         splinterdb_iterator *it;
         ASSERT_EQUAL(0,
                      splinterdb_iterator_init(
                         data->kvsb, &it, greater_than_or_equal, NULL_SLICE));
         for (uint64 i = 0; i < 512; i++) {
            ASSERT_TRUE(splinterdb_iterator_valid(it));
            ASSERT_EQUAL(0, check_current_tuple(it, i));
            splinterdb_iterator_next(it);
         }
         ASSERT_FALSE(splinterdb_iterator_valid(it));
         ASSERT_EQUAL(0, splinterdb_iterator_status(it));
         splinterdb_iterator_deinit(it);
      }
   }
}

CTEST2(splinterdb_quick, test_logged_large_value_replaces_old_value)
{
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   data->cfg.use_log   = TRUE;
   data->cfg.disk_size = 384 * Mega;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));

   slice user_key =
      slice_create(strlen("oversized-log-value"), "oversized-log-value");
   slice old_value = slice_create(strlen("old-value"), "old-value");
   ASSERT_EQUAL(0, splinterdb_insert(data->kvsb, user_key, old_value, NULL));

   /* This used to exceed the flat descriptor's log-page limit. */
   const uint64 value_len = 500 * IO_DEFAULT_EXTENT_SIZE;
   char        *value_data =
      TYPED_ARRAY_MALLOC(data->cfg.heap_id, value_data, value_len);
   ASSERT_NOT_NULL(value_data);
   memset(value_data, 'v', value_len);

   int rc = splinterdb_insert(
      data->kvsb, user_key, slice_create(value_len, value_data), NULL);
   ASSERT_EQUAL(0, rc);

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   ASSERT_EQUAL(0, splinterdb_lookup(data->kvsb, user_key, &result));
   assert_lookup_result_matches_slice(&result,
                                      slice_create(value_len, value_data));
   splinterdb_lookup_result_deinit(&result);

   /* The pointer tree participates in the log durability barrier. */
   ASSERT_EQUAL(0, splinterdb_durable_barrier(data->kvsb, NULL));
   slice flat_key   = slice_create(4, "flat");
   slice flat_value = slice_create(32 * IO_DEFAULT_EXTENT_SIZE, value_data);
   ASSERT_EQUAL(0, splinterdb_insert(data->kvsb, flat_key, flat_value, NULL));
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   data->cfg.blob_max_descriptor_size = 128;
   ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->kvsb));
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   ASSERT_EQUAL(0, splinterdb_lookup(data->kvsb, flat_key, &result));
   assert_lookup_result_matches_slice(&result, flat_value);
   ASSERT_EQUAL(0, splinterdb_lookup(data->kvsb, user_key, &result));
   assert_lookup_result_matches_slice(&result,
                                      slice_create(value_len, value_data));
   splinterdb_lookup_result_deinit(&result);
   platform_free(data->cfg.heap_id, value_data);
}

/*
 *
 * Basic test case that exercises and validates the basic flow of the
 * Splinter APIs.  We exercise:
 *  - splinterdb_insert()
 *  - splinterdb_lookup() and
 *  - splinterdb_delete()
 *
 * Validate that they behave as expected, including some basic error
 * condition checking.
 */
CTEST2(splinterdb_quick, test_basic_flow)
{
   char  *key_data = "some-key";
   size_t key_len  = sizeof("some-key");
   slice  user_key = slice_create(key_len, key_data);

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   int rc = splinterdb_lookup(data->kvsb, user_key, &result);
   ASSERT_EQUAL(0, rc);

   // Lookup of a non-existent key should return not-found.
   ASSERT_FALSE(splinterdb_lookup_found(&result));

   static char *to_insert_data = "some-value";
   size_t       to_insert_len  = strlen(to_insert_data);
   slice        to_insert      = slice_create(to_insert_len, to_insert_data);

   // Basic insert of new key should succeed.
   rc = splinterdb_insert(data->kvsb, user_key, to_insert, NULL);
   ASSERT_EQUAL(0, rc);

   // Lookup of inserted key should succeed.
   rc = splinterdb_lookup(data->kvsb, user_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   slice value;
   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(to_insert_len, slice_length(value));
   ASSERT_STREQN(to_insert_data, slice_data(value), slice_length(value));

   // Delete key
   rc = splinterdb_delete(data->kvsb, user_key, NULL);
   ASSERT_EQUAL(0, rc);

   // Deleted key should not be found
   rc = splinterdb_lookup(data->kvsb, user_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_lookup_found(&result));

   splinterdb_lookup_result_deinit(&result);
}

CTEST2(splinterdb_quick, test_pthread_auto_registration)
{
   pthread_t                     thread;
   auto_registration_thread_args args = {.kvsb = data->kvsb};

   int rc = pthread_create(&thread, NULL, auto_registered_lookup_thread, &args);
   ASSERT_EQUAL(0, rc);

   rc = pthread_join(thread, NULL);
   ASSERT_EQUAL(0, rc);

   ASSERT_EQUAL(0, args.rc);
   ASSERT_EQUAL(INVALID_TID, args.tid_before);
   ASSERT_TRUE(args.tid_after < MAX_THREADS);
}

static void *
auto_registered_result_thread(void *arg)
{
   auto_registration_thread_args *args = arg;
   args->tid_before                    = platform_get_tid();

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      args->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   splinterdb_lookup_result_deinit(&result);
   args->tid_after = platform_get_tid();
   if (args->tid_after >= MAX_THREADS) {
      args->rc = EINVAL;
      return NULL;
   }

   slice key = slice_create(3, "key");
   splinterdb_lookup_result_init(
      args->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   args->rc = splinterdb_lookup(args->kvsb, key, &result);
   if (args->rc == 0) {
      // Value materialization also needs to register before taking the lock.
      platform_deregister_thread();
      slice value;
      args->rc = splinterdb_lookup_result_value(&result, &value);
      if (args->rc == 0
          && (platform_get_tid() >= MAX_THREADS
              || slice_lex_cmp(value, slice_create(5, "value")) != 0))
      {
         args->rc = EINVAL;
      }
   }
   splinterdb_lookup_result_deinit(&result);
   return NULL;
}

CTEST2(splinterdb_quick, test_result_auto_registration_with_stats)
{
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   data->cfg.use_stats = TRUE;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));
   ASSERT_EQUAL(
      0,
      splinterdb_insert(
         data->kvsb, slice_create(3, "key"), slice_create(5, "value"), NULL));

   pthread_t                     thread;
   auto_registration_thread_args args = {.kvsb = data->kvsb};
   ASSERT_EQUAL(
      0, pthread_create(&thread, NULL, auto_registered_result_thread, &args));
   ASSERT_EQUAL(0, pthread_join(thread, NULL));
   ASSERT_EQUAL(0, args.rc);
   ASSERT_EQUAL(INVALID_TID, args.tid_before);
   ASSERT_TRUE(args.tid_after < MAX_THREADS);
}

CTEST2(splinterdb_quick, test_stats_reset_clears_cache_io)
{
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   data->cfg.use_stats       = TRUE;
   data->cfg.cache_use_stats = TRUE;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));
   slice key   = slice_create(3, "key");
   slice value = slice_create(5, "value");
   ASSERT_EQUAL(0, splinterdb_insert(data->kvsb, key, value, NULL));
   force_flush_current_memtable(data->kvsb);
   splinterdb_cache_flush(data->kvsb);
   cache *cc = (cache *)splinterdb_get_cache_handle(data->kvsb);
   ASSERT_TRUE(SUCCESS(cache_evict(cc, TRUE)));

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   ASSERT_EQUAL(0, splinterdb_lookup(data->kvsb, key, &result));
   assert_lookup_result_matches_slice(&result, value);
   splinterdb_lookup_result_deinit(&result);

   uint64 reads, writes;
   cache_io_stats(cc, &reads, &writes);
   ASSERT_TRUE(reads > 0);
   ASSERT_TRUE(writes > 0);
   splinterdb_stats_reset(data->kvsb);
   cache_io_stats(cc, &reads, &writes);
   ASSERT_EQUAL_U(0, reads);
   ASSERT_EQUAL_U(0, writes);
   const clockcache *clock = (const clockcache *)cc;
   for (uint64 i = 0; i < MAX_THREADS; i++) {
      ASSERT_EQUAL_U(0, clock->stats[i].writes_issued);
      ASSERT_EQUAL_U(0, clock->stats[i].syncs_issued);
      for (page_type type = 0; type < NUM_PAGE_TYPES; type++) {
         ASSERT_EQUAL_U(0, clock->stats[i].prefetches_issued[type]);
      }
   }
}

static int
final_merge_test_merge(const data_config *cfg,
                       slice              key,
                       message            older,
                       merge_accumulator *newer)
{
   final_merge_data_config *test_cfg = (final_merge_data_config *)cfg;
   if (__sync_bool_compare_and_swap(&test_cfg->merge_block_once, TRUE, FALSE)) {
      __atomic_store_n(&test_cfg->merge_blocked, TRUE, __ATOMIC_RELEASE);
      while (!__atomic_load_n(&test_cfg->merge_release, __ATOMIC_ACQUIRE)) {
         platform_sleep_ns(1000);
      }
   }
   merge_accumulator_set_class(newer, MESSAGE_TYPE_INSERT);
   return 0;
}

static int
final_merge_test_finalize(const data_config *cfg,
                          slice              key,
                          merge_accumulator *value)
{
   final_merge_data_config *test_cfg = (final_merge_data_config *)cfg;
   if (__sync_bool_compare_and_swap(
          &test_cfg->finalize_block_once, TRUE, FALSE))
   {
      __atomic_store_n(&test_cfg->finalize_blocked, TRUE, __ATOMIC_RELEASE);
      while (!__atomic_load_n(&test_cfg->finalize_release, __ATOMIC_ACQUIRE)) {
         platform_sleep_ns(1000);
      }
   }
   __sync_fetch_and_add(&test_cfg->calls, 1);
   merge_accumulator_set_class(value, MESSAGE_TYPE_INSERT);
   return test_cfg->fail ? ENOMEM : 0;
}

CTEST2(splinterdb_quick, test_final_merge_error_propagation)
{
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   final_merge_data_config *cfg = &data->final_merge_cfg;
   default_data_config_init(&cfg->super);
   cfg->super.merge_tuples       = final_merge_test_merge;
   cfg->super.merge_tuples_final = final_merge_test_finalize;
   cfg->calls                    = 0;
   cfg->finalize_block_once      = FALSE;
   cfg->finalize_blocked         = FALSE;
   cfg->finalize_release         = FALSE;
   cfg->merge_block_once         = FALSE;
   cfg->merge_blocked            = FALSE;
   cfg->merge_release            = FALSE;
   data->cfg.data_cfg            = &cfg->super;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));
   core_handle *core = (core_handle *)splinterdb_get_trunk_handle(data->kvsb);

   // Cover both inline updates and materialization of a blob update.
   char payload[8192] = {1};
   for (uint64 i = 0; i < 2; i++) {
      slice key = i == 0 ? slice_create(6, "inline") : slice_create(4, "blob");
      slice value = slice_create(i == 0 ? 7 : sizeof(payload), payload);
      ASSERT_EQUAL(0, splinterdb_update(data->kvsb, key, value, NULL));

      splinterdb_lookup_result result;
      splinterdb_lookup_result_init(
         data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
      cfg->fail = TRUE;
      ASSERT_EQUAL(ENOMEM, splinterdb_lookup(data->kvsb, key, &result));

      lookup_result internal_result;
      lookup_result_init(
         &internal_result, &cfg->super, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
      platform_status rc =
         async_call_sync_callback(cache_cleanup(core->cc),
                                  core_lookup_async,
                                  core,
                                  key_create_from_slice(TRUE, key),
                                  &internal_result);
      ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_NO_MEMORY));
      lookup_result_deinit(&internal_result);

      cfg->fail = FALSE;
      ASSERT_EQUAL(0, splinterdb_lookup(data->kvsb, key, &result));
      assert_lookup_result_matches_slice(&result, value);

      cfg->fail         = TRUE;
      slice replacement = slice_create(11, "replacement");
      ASSERT_EQUAL(ENOMEM,
                   splinterdb_insert(data->kvsb, key, replacement, &result));
      // Resolving the old value failed before the replacement was inserted.
      // Ensure the error path released its resources and locks and left the
      // logical value unchanged.
      cfg->fail = FALSE;
      ASSERT_EQUAL(0, splinterdb_lookup(data->kvsb, key, &result));
      assert_lookup_result_matches_slice(&result, value);
      splinterdb_lookup_result_deinit(&result);
   }
   ASSERT_EQUAL_U(10, cfg->calls);
}

CTEST2(splinterdb_quick, test_fetch_and_update_catches_up_after_rotation)
{
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   final_merge_data_config *cfg = &data->final_merge_cfg;
   default_data_config_init(&cfg->super);
   cfg->super.merge_tuples       = final_merge_test_merge;
   cfg->super.merge_tuples_final = final_merge_test_finalize;
   cfg->fail                     = FALSE;
   cfg->calls                    = 0;
   cfg->finalize_block_once      = FALSE;
   cfg->finalize_blocked         = FALSE;
   cfg->finalize_release         = FALSE;
   cfg->merge_block_once         = FALSE;
   cfg->merge_blocked            = FALSE;
   cfg->merge_release            = FALSE;
   data->cfg.data_cfg            = &cfg->super;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));

   slice key         = slice_create(12, "rotation-key");
   slice lower       = slice_create(5, "lower");
   slice skipped     = slice_create(7, "skipped");
   slice replacement = slice_create(11, "replacement");

   ASSERT_EQUAL(0, splinterdb_update(data->kvsb, key, lower, NULL));
   core_handle *core = (core_handle *)splinterdb_get_trunk_handle(data->kvsb);
   ASSERT_TRUE(SUCCESS(memtable_force_rotation(&core->mt_ctxt, NULL)));
   ASSERT_EQUAL(0, splinterdb_update(data->kvsb, key, skipped, NULL));

   splinterdb_lookup_result old_result;
   splinterdb_lookup_result_init(
      data->kvsb, &old_result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   fetch_and_update_thread_args args = {
      .kvsb       = data->kvsb,
      .key        = key,
      .value      = replacement,
      .old_result = &old_result,
   };

   cfg->finalize_block_once = TRUE;
   cfg->merge_block_once    = TRUE;
   pthread_t thread;
   ASSERT_EQUAL(0,
                pthread_create(&thread, NULL, fetch_and_update_thread, &args));
   while (!__atomic_load_n(&cfg->finalize_blocked, __ATOMIC_ACQUIRE)) {
      platform_sleep_ns(1000);
   }

   // Rotate the generation skipped by the blocked first pass, but leave it
   // unincorporated so the second pass can catch up from its memtable.
   ASSERT_TRUE(SUCCESS(memtable_force_rotation(&core->mt_ctxt, NULL)));
   __atomic_store_n(&cfg->finalize_release, TRUE, __ATOMIC_RELEASE);

   // The catch-up merge can be arbitrarily expensive.  Rotate once more while
   // it is blocked to prove that catch-up does not retain the insert lock.  The
   // fetch-and-update must then catch up through the newly retired generation.
   while (!__atomic_load_n(&cfg->merge_blocked, __ATOMIC_ACQUIRE)) {
      platform_sleep_ns(1000);
   }
   ASSERT_TRUE(SUCCESS(memtable_force_rotation(&core->mt_ctxt, NULL)));
   __atomic_store_n(&cfg->merge_release, TRUE, __ATOMIC_RELEASE);
   ASSERT_EQUAL(0, pthread_join(thread, NULL));

   ASSERT_EQUAL(0, args.rc);
   assert_lookup_result_matches_slice(&old_result, skipped);
   splinterdb_lookup_result_deinit(&old_result);
}

CTEST2(splinterdb_quick, test_fetch_and_update_rejects_incorporated_boundary)
{
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   final_merge_data_config *cfg = &data->final_merge_cfg;
   default_data_config_init(&cfg->super);
   cfg->super.merge_tuples       = final_merge_test_merge;
   cfg->super.merge_tuples_final = final_merge_test_finalize;
   cfg->fail                     = FALSE;
   cfg->calls                    = 0;
   cfg->finalize_block_once      = FALSE;
   cfg->finalize_blocked         = FALSE;
   cfg->finalize_release         = FALSE;
   cfg->merge_block_once         = FALSE;
   cfg->merge_blocked            = FALSE;
   cfg->merge_release            = FALSE;
   data->cfg.data_cfg            = &cfg->super;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));

   slice key         = slice_create(12, "boundary-key");
   slice lower       = slice_create(5, "lower");
   slice skipped     = slice_create(7, "skipped");
   slice replacement = slice_create(11, "replacement");

   ASSERT_EQUAL(0, splinterdb_update(data->kvsb, key, lower, NULL));
   core_handle *core = (core_handle *)splinterdb_get_trunk_handle(data->kvsb);
   ASSERT_TRUE(SUCCESS(memtable_force_rotation(&core->mt_ctxt, NULL)));
   ASSERT_EQUAL(0, splinterdb_update(data->kvsb, key, skipped, NULL));

   splinterdb_lookup_result old_result;
   splinterdb_lookup_result_init(
      data->kvsb, &old_result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   fetch_and_update_thread_args args = {
      .kvsb       = data->kvsb,
      .key        = key,
      .value      = replacement,
      .old_result = &old_result,
   };

   cfg->finalize_block_once = TRUE;
   pthread_t thread;
   ASSERT_EQUAL(0,
                pthread_create(&thread, NULL, fetch_and_update_thread, &args));
   while (!__atomic_load_n(&cfg->finalize_blocked, __ATOMIC_ACQUIRE)) {
      platform_sleep_ns(1000);
   }

   ASSERT_TRUE(SUCCESS(memtable_force_rotation(&core->mt_ctxt, NULL)));
   ASSERT_TRUE(SUCCESS(task_perform_until_quiescent(core->ts)));
   __atomic_store_n(&cfg->finalize_release, TRUE, __ATOMIC_RELEASE);
   ASSERT_EQUAL(0, pthread_join(thread, NULL));

   ASSERT_EQUAL(EAGAIN, args.rc);
   ASSERT_FALSE(splinterdb_lookup_found(&old_result));

   // The failed fetch-and-update did not publish its replacement.
   ASSERT_EQUAL(0, splinterdb_lookup(data->kvsb, key, &old_result));
   assert_lookup_result_matches_slice(&old_result, skipped);
   splinterdb_lookup_result_deinit(&old_result);
}

static void
count_range_tuple(key tuple_key, message msg, void *arg)
{
   (*(uint64 *)arg)++;
}

CTEST2(splinterdb_quick, test_range_init_error_cleanup_preserves_branches)
{
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   final_merge_data_config *cfg = &data->final_merge_cfg;
   default_data_config_init(&cfg->super);
   cfg->super.merge_tuples       = final_merge_test_merge;
   cfg->super.merge_tuples_final = final_merge_test_finalize;
   cfg->fail                     = FALSE;
   cfg->calls                    = 0;
   data->cfg.data_cfg            = &cfg->super;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));

   // Keep a compacted branch and a mutable memtable in the failed iterator.
   slice old_key = slice_create(3, "zzz");
   slice new_key = slice_create(3, "aaa");
   slice value   = slice_create(5, "value");
   ASSERT_EQUAL(0, splinterdb_insert(data->kvsb, old_key, value, NULL));
   force_flush_current_memtable(data->kvsb);
   ASSERT_EQUAL(0, splinterdb_update(data->kvsb, new_key, value, NULL));

   core_handle *core = (core_handle *)splinterdb_get_trunk_handle(data->kvsb);
   uint64       visited = 0;
   cfg->fail            = TRUE;
   platform_status rc   = core_apply_to_range(
      core, NEGATIVE_INFINITY_KEY, 10, count_range_tuple, &visited);
   ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_NO_MEMORY));
   ASSERT_EQUAL_U(0, visited);
   ASSERT_TRUE(cfg->calls > 0);

   // Both init and its caller cleaned up. The database must still own its
   // branches, so point lookups and a new range scan must remain usable.
   cfg->fail = FALSE;
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   ASSERT_EQUAL(0, splinterdb_lookup(data->kvsb, old_key, &result));
   assert_lookup_result_matches_slice(&result, value);
   ASSERT_EQUAL(0, splinterdb_lookup(data->kvsb, new_key, &result));
   assert_lookup_result_matches_slice(&result, value);
   splinterdb_lookup_result_deinit(&result);
   rc = core_apply_to_range(
      core, NEGATIVE_INFINITY_KEY, 10, count_range_tuple, &visited);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_EQUAL_U(2, visited);
}

CTEST2(splinterdb_quick, stats_reset_excludes_active_operations)
{
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   create_default_cfg(&data->cfg, &data->default_data_cfg.super);
   data->cfg.use_stats = TRUE;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));

   enum { NUM_WORKERS = 4 };
   pthread_t               threads[NUM_WORKERS];
   stats_reset_worker_args args[NUM_WORKERS];
   volatile uint64         started = 0;
   volatile bool32         stop    = FALSE;

   for (uint64 i = 0; i < NUM_WORKERS; i++) {
      args[i] = (stats_reset_worker_args){.kvsb      = data->kvsb,
                                          .worker_no = i,
                                          .started   = &started,
                                          .stop      = &stop};
      ASSERT_EQUAL(
         0, pthread_create(&threads[i], NULL, stats_reset_worker, &args[i]));
   }

   while (__atomic_load_n(&started, __ATOMIC_ACQUIRE) != NUM_WORKERS) {
      platform_sleep_ns(1000);
   }
   uint64 operations;
   do {
      operations = 0;
      for (uint64 i = 0; i < NUM_WORKERS; i++) {
         operations += __atomic_load_n(&args[i].operations, __ATOMIC_RELAXED);
      }
      if (operations < 100) {
         platform_sleep_ns(1000);
      }
   } while (operations < 100);

   for (uint64 i = 0; i < 256; i++) {
      splinterdb_stats_reset(data->kvsb);
   }

   __atomic_store_n(&stop, TRUE, __ATOMIC_RELEASE);
   for (uint64 i = 0; i < NUM_WORKERS; i++) {
      ASSERT_EQUAL(0, pthread_join(threads[i], NULL));
      ASSERT_EQUAL(0, args[i].rc);
      ASSERT_TRUE(args[i].operations != 0);
   }
}

/*
 * Basic test case that exercises and validates the basic flow of the
 * Splinter APIs for a larger key.
 */
CTEST2(splinterdb_quick, test_apis_for_large_key)
{
   char   large_key_data[TEST_LARGE_KEY_SIZE];
   size_t large_key_len = TEST_LARGE_KEY_SIZE;
   memset(large_key_data, 'a', TEST_LARGE_KEY_SIZE);
   slice large_key = slice_create(large_key_len, large_key_data);

   static char *to_insert_data = "a-value";
   size_t       to_insert_len  = strlen(to_insert_data);
   slice        to_insert      = slice_create(to_insert_len, to_insert_data);

   // **** Insert of a larger key should succeed.
   int rc = splinterdb_insert(data->kvsb, large_key, to_insert, NULL);
   ASSERT_EQUAL(0, rc);

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   // **** Lookup of max-size key should return correct value
   rc = splinterdb_lookup(data->kvsb, large_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   slice value;
   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);

   ASSERT_EQUAL(strlen(to_insert_data), slice_length(value));
   ASSERT_STREQN(to_insert_data,
                 slice_data(value),
                 slice_length(value),
                 "Large key-value did not match as expected.");

   rc = splinterdb_delete(data->kvsb, large_key, NULL);
   ASSERT_EQUAL(0, rc);

   // **** Should not find this large-key once it's deleted
   rc = splinterdb_lookup(data->kvsb, large_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_lookup_found(&result));

   splinterdb_lookup_result_deinit(&result);
}

/*
 * Test case to verify core interfaces when key-size is too large.
 */
CTEST2(splinterdb_quick, test_key_size_gt_max_inline_key_size)
{
   char   too_large_key_data[MAX_INLINE_KEY_SIZE(IO_DEFAULT_PAGE_SIZE) + 1];
   size_t too_large_key_len = sizeof(too_large_key_data);
   memset(too_large_key_data, 'a', too_large_key_len);
   slice too_large_key = slice_create(too_large_key_len, too_large_key_data);

   int rc = splinterdb_insert(
      data->kvsb, too_large_key, slice_create(sizeof("foo"), "foo"), NULL);
   ASSERT_EQUAL(EINVAL, rc);

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   rc = splinterdb_delete(data->kvsb, too_large_key, NULL);
   ASSERT_EQUAL(EINVAL, rc);

   splinterdb_lookup_result_deinit(&result);
}

/*
 * Test case to verify core interfaces when value-size is larger than a page.
 */
CTEST2(splinterdb_quick, test_value_size_gt_max_value_size)
{
   size_t too_large_value_len = 3 * IO_DEFAULT_PAGE_SIZE + 123;
   char  *too_large_value_data;
   too_large_value_data = TYPED_ARRAY_MALLOC(
      data->cfg.heap_id, too_large_value_data, too_large_value_len);
   memset(too_large_value_data, 'z', too_large_value_len);
   slice too_large_value =
      slice_create(too_large_value_len, too_large_value_data);

   int rc = splinterdb_insert(
      data->kvsb, slice_create(sizeof("foo"), "foo"), too_large_value, NULL);
   ASSERT_EQUAL(0, rc);

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   rc = splinterdb_lookup(
      data->kvsb, slice_create(sizeof("foo"), "foo"), &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   slice value;
   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(too_large_value_len, slice_length(value));
   ASSERT_EQUAL(0, slice_lex_cmp(too_large_value, value));

   splinterdb_lookup_result_deinit(&result);

   splinterdb_close(&data->kvsb, FALSE);
   rc = splinterdb_open(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   rc = splinterdb_lookup(
      data->kvsb, slice_create(sizeof("foo"), "foo"), &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(too_large_value_len, slice_length(value));
   ASSERT_EQUAL(0, slice_lex_cmp(too_large_value, value));

   splinterdb_lookup_result_deinit(&result);
   platform_free(data->cfg.heap_id, too_large_value_data);
}

CTEST2(splinterdb_quick, test_large_lookup_survives_memtable_recycle)
{
   size_t large_value_len = 3 * IO_DEFAULT_PAGE_SIZE + 123;
   char  *large_value_data;
   large_value_data =
      TYPED_ARRAY_MALLOC(data->cfg.heap_id, large_value_data, large_value_len);
   memset(large_value_data, 'z', large_value_len);
   slice large_value = slice_create(large_value_len, large_value_data);
   slice user_key    = slice_create(sizeof("foo"), "foo");

   int rc = splinterdb_insert(data->kvsb, user_key, large_value, NULL);
   ASSERT_EQUAL(0, rc);

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   rc = splinterdb_lookup(data->kvsb, user_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   core_handle *core = (core_handle *)splinterdb_get_trunk_handle(data->kvsb);
   uint64       generation = force_flush_current_memtable(data->kvsb);
   memtable    *mt =
      &core->mt_ctxt.mt[generation % core->mt_ctxt.cfg.max_memtables];
   ASSERT_EQUAL(MEMTABLE_STATE_READY, mt->state);

   for (uint64 i = 0; i < core->mt_ctxt.cfg.max_memtables; i++) {
      char recycle_key[TEST_INSERT_KEY_LENGTH] = {0};
      char recycle_val[TEST_INSERT_VAL_LENGTH] = {0};

      snprintf(recycle_key, sizeof(recycle_key), "r-%06lu", i);
      snprintf(recycle_val, sizeof(recycle_val), "v-%06lu", i);
      rc = splinterdb_insert(data->kvsb,
                             slice_create(strlen(recycle_key), recycle_key),
                             slice_create(strlen(recycle_val), recycle_val),
                             NULL);
      ASSERT_EQUAL(0, rc);
      force_flush_current_memtable(data->kvsb);
   }

   slice value;
   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(large_value_len, slice_length(value));
   ASSERT_EQUAL(0, slice_lex_cmp(large_value, value));

   splinterdb_lookup_result_deinit(&result);
   platform_free(data->cfg.heap_id, large_value_data);
}

CTEST2(splinterdb_quick, test_iterator_survives_memtable_recycle)
{
   const int num_inserts = 50;
   int       rc          = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator *it = NULL;
   rc                      = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, NULL_SLICE);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_iterator_valid(it));

   core_handle *core = (core_handle *)splinterdb_get_trunk_handle(data->kvsb);
   uint64       generation = force_flush_current_memtable(data->kvsb);
   memtable    *mt =
      &core->mt_ctxt.mt[generation % core->mt_ctxt.cfg.max_memtables];
   ASSERT_EQUAL(MEMTABLE_STATE_READY, mt->state);

   int i = 0;
   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      rc = check_current_tuple(it, i);
      ASSERT_EQUAL(0, rc);
      i++;
   }
   ASSERT_EQUAL(num_inserts, i);

   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);
   splinterdb_iterator_deinit(it);
}

/*
 * Test case to exercise APIs for variable-length values; empty value,
 * short and somewhat longish value. After inserting this data, the lookup
 * sub-cases exercises different combinations to cover internal re-allocation
 * when supplied output buffer is smaller than the datum value.
 */
CTEST2(splinterdb_quick, test_variable_length_values)
{
   slice      key_empty      = slice_create(sizeof("empty"), "empty");
   const char empty_string[] = "";

   slice      key_short       = slice_create(sizeof("short"), "short");
   const char short_string[1] = "v";

   slice key_long = slice_create(sizeof("long"), "long");
   char  almost_max_length_string[TEST_MAX_VALUE_SIZE - 1];
   memset(almost_max_length_string, 'a', TEST_MAX_VALUE_SIZE - 1);

   slice key_max = slice_create(sizeof("max"), "max");
   char  max_length_string[TEST_MAX_VALUE_SIZE];
   memset(max_length_string, 'b', TEST_MAX_VALUE_SIZE);

   // Insert keys with different value (lengths)
   int rc = splinterdb_insert(
      data->kvsb, key_empty, slice_create(0, empty_string), NULL);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_insert(data->kvsb,
                          key_short,
                          slice_create(sizeof(short_string), short_string),
                          NULL);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_insert(
      data->kvsb,
      key_long,
      slice_create(sizeof(almost_max_length_string), almost_max_length_string),
      NULL);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_insert(
      data->kvsb,
      key_max,
      slice_create(sizeof(max_length_string), max_length_string),
      NULL);
   ASSERT_EQUAL(0, rc);


   // Allocate and mark a buffer with ample space to hold the results
   char big_buffer[2 * TEST_MAX_VALUE_SIZE];
   memset(big_buffer, 'x', sizeof(big_buffer));

   splinterdb_lookup_result result;
   slice                    value;

   // allocate a result that has full access to the buffer
   splinterdb_lookup_result_init(data->kvsb,
                                 &result,
                                 SPLINTERDB_LOOKUP_VALUE,
                                 sizeof(big_buffer),
                                 big_buffer);

   // look up a 0-length value
   rc = splinterdb_lookup(data->kvsb, key_empty, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));
   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(0, slice_length(value));

   // lookup tuple with value of length 1, providing sufficient buffer
   rc = splinterdb_lookup(data->kvsb, key_short, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));
   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(1, slice_length(value));

   // lookup tuple with almost max-sized-value
   rc = splinterdb_lookup(data->kvsb, key_long, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));
   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(TEST_MAX_VALUE_SIZE - 1, slice_length(value));
   ASSERT_STREQN(
      almost_max_length_string, slice_data(value), slice_length(value));

   // lookup tuple with max-sized-value
   rc = splinterdb_lookup(data->kvsb, key_max, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));
   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(TEST_MAX_VALUE_SIZE, slice_length(value));
   ASSERT_STREQN(max_length_string, slice_data(value), slice_length(value));

   // done with the big buffer
   splinterdb_lookup_result_deinit(&result);

   // freshen up the buffer
   memset(big_buffer, 'x', sizeof(big_buffer));
   char saved_big_buffer[sizeof(big_buffer)];
   memset(saved_big_buffer, 'x', sizeof(saved_big_buffer));

   // init the result again, but give it a buffer too small for the value
   uint64 short_buffer_len = TEST_MAX_VALUE_SIZE - 1;
   splinterdb_lookup_result_init(data->kvsb,
                                 &result,
                                 SPLINTERDB_LOOKUP_VALUE,
                                 short_buffer_len,
                                 big_buffer);

   // lookup tuple with max-sized-value, passing it the short buffer
   rc = splinterdb_lookup(data->kvsb, key_max, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   // we get the full result back, because internally splinterdb did an
   // allocation
   ASSERT_EQUAL(TEST_MAX_VALUE_SIZE, slice_length(value));
   ASSERT_STREQN(max_length_string, slice_data(value), slice_length(value));

   // we can deinit the result, and it doesn't try to free the stack space we
   // originally gave it
   splinterdb_lookup_result_deinit(&result);
   ASSERT_EQUAL(0, memcmp(saved_big_buffer, big_buffer, sizeof(big_buffer)));


   // init another result, but don't give it a buffer
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   // lookup, see we get the full result back
   rc = splinterdb_lookup(data->kvsb, key_max, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));
   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   // we get the full result back, because internally splinterdb did an
   // allocation
   ASSERT_EQUAL(TEST_MAX_VALUE_SIZE, slice_length(value));
   ASSERT_STREQN(max_length_string, slice_data(value), slice_length(value));

   // we can de-init the result, and it doesn't crash
   splinterdb_lookup_result_deinit(&result);
}

/*
 * iterator test case.
 */
CTEST2(splinterdb_quick, test_basic_iterator)
{
   const int num_inserts = 50;
   int       rc          = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   int i = 0;

   splinterdb_iterator *it = NULL;

   rc = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, NULL_SLICE);
   ASSERT_EQUAL(0, rc);

   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      rc = check_current_tuple(it, i);
      ASSERT_EQUAL(0, rc);
      i++;
   }
   ASSERT_EQUAL(num_inserts, i);

   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator_deinit(it);
}

CTEST2(splinterdb_quick, test_iterator_delete_current)
{
   const int num_inserts = 32;
   int       rc          = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator *it = NULL;
   rc                      = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, NULL_SLICE);
   ASSERT_EQUAL(0, rc);

   int num_deleted = 0;
   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      slice key;
      slice value;

      splinterdb_iterator_get_current(it, &key, &value);
      ASSERT_EQUAL(TEST_INSERT_KEY_LENGTH, slice_length(key));

      rc = splinterdb_delete(data->kvsb, key, NULL);
      ASSERT_EQUAL(0, rc);
      num_deleted++;
   }

   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(num_inserts, num_deleted);
   splinterdb_iterator_deinit(it);

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   for (int i = 0; i < num_inserts; i++) {
      char key[TEST_INSERT_KEY_LENGTH] = {0};

      ASSERT_EQUAL(KEY_FMT_LENGTH, snprintf(key, sizeof(key), key_fmt, i));
      rc =
         splinterdb_lookup(data->kvsb, slice_create(sizeof(key), key), &result);
      ASSERT_EQUAL(0, rc);
      ASSERT_FALSE(splinterdb_lookup_found(&result));
   }

   splinterdb_lookup_result_deinit(&result);
}

/*
 * empty iterator test case.
 */
CTEST2(splinterdb_quick, test_empty_iterator)
{
   splinterdb_iterator *it = NULL;
   int                  rc = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, NULL_SLICE);
   ASSERT_EQUAL(0, rc);

   ASSERT_FALSE(splinterdb_iterator_valid(it));
   ASSERT_FALSE(splinterdb_iterator_can_next(it));
   ASSERT_FALSE(splinterdb_iterator_can_prev(it));
   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator_deinit(it);
}

CTEST2(splinterdb_quick, test_bounded_iterator)
{
   const int num_inserts = 10;
   int       rc          = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   char min_key[TEST_INSERT_KEY_LENGTH] = {0};
   char max_key[TEST_INSERT_KEY_LENGTH] = {0};

   snprintf(min_key, sizeof(min_key), key_fmt, 3);
   snprintf(max_key, sizeof(max_key), key_fmt, 6);

   slice min = slice_create(sizeof(min_key), min_key);
   slice max = slice_create(sizeof(max_key), max_key);

   const int forward_exclusive_max[] = {3, 4, 5};
   rc = check_bounded_iterator_sequence(data->kvsb,
                                        greater_than_or_equal,
                                        min,
                                        less_than,
                                        max,
                                        greater_than_or_equal,
                                        forward_exclusive_max,
                                        ARRAY_SIZE(forward_exclusive_max));
   ASSERT_EQUAL(0, rc);

   const int forward_exclusive_min[] = {4, 5, 6};
   rc = check_bounded_iterator_sequence(data->kvsb,
                                        greater_than,
                                        min,
                                        less_than_or_equal,
                                        max,
                                        greater_than_or_equal,
                                        forward_exclusive_min,
                                        ARRAY_SIZE(forward_exclusive_min));
   ASSERT_EQUAL(0, rc);

   const int reverse_exclusive_max[] = {5, 4, 3};
   rc = check_bounded_iterator_sequence(data->kvsb,
                                        greater_than_or_equal,
                                        min,
                                        less_than,
                                        max,
                                        less_than_or_equal,
                                        reverse_exclusive_max,
                                        ARRAY_SIZE(reverse_exclusive_max));
   ASSERT_EQUAL(0, rc);

   snprintf(min_key, sizeof(min_key), key_fmt, 5);
   snprintf(max_key, sizeof(max_key), key_fmt, 5);
   min = slice_create(sizeof(min_key), min_key);
   max = slice_create(sizeof(max_key), max_key);

   const int singleton[] = {5};
   rc                    = check_bounded_iterator_sequence(data->kvsb,
                                        greater_than_or_equal,
                                        min,
                                        less_than_or_equal,
                                        max,
                                        greater_than_or_equal,
                                        singleton,
                                        ARRAY_SIZE(singleton));
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_iterator_init_with_bounds(data->kvsb,
                                             NULL,
                                             less_than,
                                             min,
                                             less_than_or_equal,
                                             max,
                                             greater_than_or_equal,
                                             NULL_SLICE);
   ASSERT_EQUAL(EINVAL, rc);
}

CTEST2(splinterdb_quick, test_bounded_iterator_start_normalization)
{
   const int num_inserts = 10;
   int       rc          = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   char min_key[TEST_INSERT_KEY_LENGTH]       = {0};
   char max_key[TEST_INSERT_KEY_LENGTH]       = {0};
   char below_min_key[TEST_INSERT_KEY_LENGTH] = {0};
   char above_max_key[TEST_INSERT_KEY_LENGTH] = {0};

   snprintf(min_key, sizeof(min_key), key_fmt, 3);
   snprintf(max_key, sizeof(max_key), key_fmt, 6);
   snprintf(below_min_key, sizeof(below_min_key), key_fmt, 1);
   snprintf(above_max_key, sizeof(above_max_key), key_fmt, 8);

   slice min       = slice_create(sizeof(min_key), min_key);
   slice max       = slice_create(sizeof(max_key), max_key);
   slice below_min = slice_create(sizeof(below_min_key), below_min_key);
   slice above_max = slice_create(sizeof(above_max_key), above_max_key);

   // start_key < min_key: forward starts at the lower bound.
   rc = check_bounded_iterator_current_from_start(data->kvsb,
                                                  greater_than_or_equal,
                                                  min,
                                                  less_than,
                                                  max,
                                                  greater_than_or_equal,
                                                  below_min,
                                                  3);
   ASSERT_EQUAL(0, rc);
   rc = check_bounded_iterator_current_from_start(data->kvsb,
                                                  greater_than,
                                                  min,
                                                  less_than_or_equal,
                                                  max,
                                                  greater_than_or_equal,
                                                  below_min,
                                                  4);
   ASSERT_EQUAL(0, rc);

   // start_key < min_key: backward starts just before the lower bound.
   rc = check_bounded_iterator_boundary_from_start(data->kvsb,
                                                   greater_than_or_equal,
                                                   min,
                                                   less_than,
                                                   max,
                                                   less_than_or_equal,
                                                   below_min,
                                                   TRUE,
                                                   3);
   ASSERT_EQUAL(0, rc);
   rc = check_bounded_iterator_boundary_from_start(data->kvsb,
                                                   greater_than,
                                                   min,
                                                   less_than_or_equal,
                                                   max,
                                                   less_than_or_equal,
                                                   below_min,
                                                   TRUE,
                                                   4);
   ASSERT_EQUAL(0, rc);

   // start_key == min_key: an exclusive lower bound moves forward starts above
   // min_key and backward starts to the invalid low-side boundary.
   rc = check_bounded_iterator_current_from_start(data->kvsb,
                                                  greater_than,
                                                  min,
                                                  less_than_or_equal,
                                                  max,
                                                  greater_than_or_equal,
                                                  min,
                                                  4);
   ASSERT_EQUAL(0, rc);
   rc = check_bounded_iterator_boundary_from_start(data->kvsb,
                                                   greater_than,
                                                   min,
                                                   less_than_or_equal,
                                                   max,
                                                   less_than_or_equal,
                                                   min,
                                                   TRUE,
                                                   4);
   ASSERT_EQUAL(0, rc);

   // start_key == min_key: an inclusive lower bound remains on min_key.
   rc = check_bounded_iterator_current_from_start(data->kvsb,
                                                  greater_than_or_equal,
                                                  min,
                                                  less_than,
                                                  max,
                                                  greater_than_or_equal,
                                                  min,
                                                  3);
   ASSERT_EQUAL(0, rc);
   rc = check_bounded_iterator_current_from_start(data->kvsb,
                                                  greater_than_or_equal,
                                                  min,
                                                  less_than,
                                                  max,
                                                  less_than_or_equal,
                                                  min,
                                                  3);
   ASSERT_EQUAL(0, rc);

   // start_key > max_key: backward starts at the upper bound.
   rc = check_bounded_iterator_current_from_start(data->kvsb,
                                                  greater_than_or_equal,
                                                  min,
                                                  less_than,
                                                  max,
                                                  less_than_or_equal,
                                                  above_max,
                                                  5);
   ASSERT_EQUAL(0, rc);
   rc = check_bounded_iterator_current_from_start(data->kvsb,
                                                  greater_than_or_equal,
                                                  min,
                                                  less_than_or_equal,
                                                  max,
                                                  less_than_or_equal,
                                                  above_max,
                                                  6);
   ASSERT_EQUAL(0, rc);

   // start_key > max_key: forward starts just after the upper bound.
   rc = check_bounded_iterator_boundary_from_start(data->kvsb,
                                                   greater_than_or_equal,
                                                   min,
                                                   less_than,
                                                   max,
                                                   greater_than_or_equal,
                                                   above_max,
                                                   FALSE,
                                                   5);
   ASSERT_EQUAL(0, rc);
   rc = check_bounded_iterator_boundary_from_start(data->kvsb,
                                                   greater_than_or_equal,
                                                   min,
                                                   less_than_or_equal,
                                                   max,
                                                   greater_than_or_equal,
                                                   above_max,
                                                   FALSE,
                                                   6);
   ASSERT_EQUAL(0, rc);

   // start_key == max_key: an exclusive upper bound moves backward starts below
   // max_key and forward starts to the invalid high-side boundary.
   rc = check_bounded_iterator_current_from_start(data->kvsb,
                                                  greater_than_or_equal,
                                                  min,
                                                  less_than,
                                                  max,
                                                  less_than_or_equal,
                                                  max,
                                                  5);
   ASSERT_EQUAL(0, rc);
   rc = check_bounded_iterator_boundary_from_start(data->kvsb,
                                                   greater_than_or_equal,
                                                   min,
                                                   less_than,
                                                   max,
                                                   greater_than_or_equal,
                                                   max,
                                                   FALSE,
                                                   5);
   ASSERT_EQUAL(0, rc);

   // start_key == max_key: an inclusive upper bound remains on max_key.
   rc = check_bounded_iterator_current_from_start(data->kvsb,
                                                  greater_than_or_equal,
                                                  min,
                                                  less_than_or_equal,
                                                  max,
                                                  greater_than_or_equal,
                                                  max,
                                                  6);
   ASSERT_EQUAL(0, rc);
   rc = check_bounded_iterator_current_from_start(data->kvsb,
                                                  greater_than_or_equal,
                                                  min,
                                                  less_than_or_equal,
                                                  max,
                                                  less_than_or_equal,
                                                  max,
                                                  6);
   ASSERT_EQUAL(0, rc);
}

/*
 * Test case to exercise and verify that splinterdb iterator interfaces with a
 * non-NULL start key correctly sets up the start scan at the requested
 * initial key value.
 */
CTEST2(splinterdb_quick, test_splinterdb_iterator_with_startkey)
{
   const int            num_inserts = 50;
   splinterdb_iterator *it          = NULL;
   int                  rc          = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   char key[TEST_INSERT_KEY_LENGTH] = {0};

   for (int ictr = 0; ictr < num_inserts; ictr++) {

      // Initialize the i'th key
      snprintf(key, sizeof(key), key_fmt, ictr);
      slice start_key = slice_create(strlen(key), key);
      rc              = splinterdb_iterator_init(
         data->kvsb, &it, greater_than_or_equal, start_key);
      ASSERT_EQUAL(0, rc);

      bool32 is_valid = splinterdb_iterator_valid(it);
      ASSERT_TRUE(is_valid);

      // Scan should have been positioned at the i'th key
      rc = check_current_tuple(it, ictr);
      ASSERT_EQUAL(0, rc);

      splinterdb_iterator_deinit(it);
   }
}

/*
 * Test case to exercise splinterdb iterator with a non-NULL but non-existent
 * start-key. The iterator just starts at the first key, if any, after the
 * specified start-key.
 *  . If start-key > max-key, we will find no more keys to scan.
 *  . If start-key < min-key, we will start scan from 1st key in set.
 */
CTEST2(splinterdb_quick, test_splinterdb_iterator_with_non_existent_startkey)
{
   int                  rc = 0;
   splinterdb_iterator *it = NULL;

   const int num_inserts = 50;
   rc                    = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   // start-key > max-key ('key-50')
   char *keystring = "unknownKey";

   slice start_key = slice_create(strlen(keystring), keystring);
   rc              = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, start_key);

   // Iterator should be invalid, as lookup key is non-existent.
   bool32 is_valid = splinterdb_iterator_valid(it);
   ASSERT_FALSE(is_valid);

   splinterdb_iterator_deinit(it);

   // If you start with a key before min-key-value, scan will start from
   // 1st key inserted. (We do lexicographic comparison, so 'U' sorts
   // before 'key...', which is what key's format is.)
   keystring = "UnknownKey";
   start_key = slice_create(strlen(keystring), keystring);
   rc        = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, start_key);
   ASSERT_EQUAL(0, rc);

   int ictr = 0;
   // Iterator should be initialized to 1st key inserted, if the supplied
   // start_key is not found, but below the min-key inserted.
   rc = check_current_tuple(it, ictr);
   ASSERT_EQUAL(0, rc);

   // Just to be sure, run through the set of keys, to cross-check that
   // we are getting all of them back in the right order.
   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      rc = check_current_tuple(it, ictr);
      ASSERT_EQUAL(0, rc);
      ictr++;
   }
   // We should have iterated thru all the keys that were inserted
   ASSERT_EQUAL(num_inserts, ictr);

   if (it) {
      splinterdb_iterator_deinit(it);
   }
}

/*
 * Test case to exercise splinterdb iterator with a non-NULL but non-existent
 * start-key.  The data in this test case is loaded such that we have a
 * sequence of key values with gaps of 2 (i.e. 1, 4, 7, 10, ...).
 *
 * Then, there are basically 4 sub-cases we exercise here:
 *
 *  a) start-key exactly == min-key
 *  b) start-key < min-key
 *  c) start-key between some existing key values; (Choose 5, which should
 *      end up starting the scan at 7.)
 *  d) start-key beyond max-key (Scan should come out as invalid.)
 */
CTEST2(splinterdb_quick,
       test_splinterdb_iterator_with_missing_startkey_in_sequence)
{
   const int num_inserts = 50;
   // Should insert keys: 1, 4, 7, 10 13, 16, 19, ...
   int minkey = 1;
   int rc     = insert_keys(data->kvsb, minkey, num_inserts, 3);
   ASSERT_EQUAL(0, rc);

   char key[TEST_INSERT_KEY_LENGTH];

   // (a) Test iter_init with a key == the min-key
   snprintf(key, sizeof(key), key_fmt, minkey);

   splinterdb_iterator *it        = NULL;
   slice                start_key = slice_create(strlen(key), key);
   rc                             = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, start_key);
   ASSERT_EQUAL(0, rc);

   bool32 is_valid = splinterdb_iterator_valid(it);
   ASSERT_TRUE(is_valid);

   // Iterator should be initialized to 1st key inserted, if the supplied
   // start_key is below min-key inserted thus far.
   int ictr = minkey;
   rc       = check_current_tuple(it, ictr);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator_deinit(it);

   // (b) Test iter_init with a value below the min-key-value.
   int kctr = (minkey - 1);

   snprintf(key, sizeof(key), key_fmt, kctr);
   start_key = slice_create(strlen(key), key);
   rc        = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, start_key);
   ASSERT_EQUAL(0, rc);

   is_valid = splinterdb_iterator_valid(it);
   ASSERT_TRUE(is_valid);

   // Iterator should be initialized to 1st key inserted, if the supplied
   // start_key is below min-key inserted thus far.
   ictr = minkey;
   rc   = check_current_tuple(it, ictr);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator_deinit(it);

   // (c) Test with a non-existent value between 2 valid key values.
   kctr = 5;
   snprintf(key, sizeof(key), key_fmt, kctr);
   start_key = slice_create(strlen(key), key);
   rc        = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, start_key);
   ASSERT_EQUAL(0, rc);

   is_valid = splinterdb_iterator_valid(it);
   ASSERT_TRUE(is_valid);

   // Iterator should be initialized to next key following kctr.
   ictr = 7;
   rc   = check_current_tuple(it, ictr);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator_deinit(it);

   // (d) Test with a non-existent value beyond max key value.
   //     iter_init should end up as being invalid.
   kctr = minkey + 3 * num_inserts;
   snprintf(key, sizeof(key), key_fmt, kctr);
   start_key = slice_create(strlen(key), key);
   rc        = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, start_key);
   ASSERT_EQUAL(0, rc);

   is_valid = splinterdb_iterator_valid(it);
   ASSERT_FALSE(is_valid);

   if (it) {
      splinterdb_iterator_deinit(it);
   }
}

CTEST2(splinterdb_quick, test_iterator_prev_and_next)
{
   const int num_inserts = 1 << 14;
   // Should insert keys: 1, 4, 7, 10 13, 16, 19, ...
   int minkey  = 1;
   int hop_amt = 3;
   int rc      = insert_keys(data->kvsb, minkey, num_inserts, 3);
   ASSERT_EQUAL(0, rc);

   char key[TEST_INSERT_KEY_LENGTH];

   // test starting with a null key
   ASSERT_EQUAL(0,
                test_two_step_iterator(
                   data->kvsb, NULL_SLICE, num_inserts, minkey, 0, hop_amt));

   // test starting with key < minkey
   snprintf(key, sizeof(key), key_fmt, 0);
   slice start_key = slice_create(strlen(key), key);
   ASSERT_EQUAL(0,
                test_two_step_iterator(
                   data->kvsb, start_key, num_inserts, minkey, 0, hop_amt));

   // test starting between two keys
   int start_i = num_inserts / 4;
   snprintf(key, sizeof(key), key_fmt, hop_amt * start_i + minkey - 1);
   start_key = slice_create(strlen(key), key);
   ASSERT_EQUAL(
      0,
      test_two_step_iterator(
         data->kvsb, start_key, num_inserts, minkey, start_i, hop_amt));
}

/*
 * Reversing direction on a key stored in more than one branch must move to the
 * neighboring key, rather than returning the same key again merged from only
 * some of its branches (which can expose an overwritten value).
 */
CTEST2(splinterdb_quick, test_iterator_reversal_on_key_in_two_branches)
{
   insert_string_kv(data->kvsb, "a", "a0");
   insert_string_kv(data->kvsb, "b", "b0");
   force_flush_current_memtable(data->kvsb);
   insert_string_kv(data->kvsb, "b", "b1");
   insert_string_kv(data->kvsb, "c", "c1");

   splinterdb_iterator *it = NULL;
   int                  rc = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, NULL_SLICE);
   ASSERT_EQUAL(0, rc);

   check_current_string_kv(it, "a", "a0");
   splinterdb_iterator_next(it);
   check_current_string_kv(it, "b", "b1");
   splinterdb_iterator_prev(it);
   check_current_string_kv(it, "a", "a0");
   splinterdb_iterator_next(it);
   check_current_string_kv(it, "b", "b1");
   splinterdb_iterator_next(it);
   check_current_string_kv(it, "c", "c1");
   splinterdb_iterator_prev(it);
   check_current_string_kv(it, "b", "b1");
   splinterdb_iterator_prev(it);
   check_current_string_kv(it, "a", "a0");
   splinterdb_iterator_prev(it);
   ASSERT_FALSE(splinterdb_iterator_valid(it));
   splinterdb_iterator_next(it);
   check_current_string_kv(it, "a", "a0");

   ASSERT_EQUAL(0, splinterdb_iterator_status(it));
   splinterdb_iterator_deinit(it);
}

/*
 * After a direction change, the branch that sorts last must not keep a stale
 * duplicate-key flag: once the other branches are exhausted it would be merged
 * with a branch that has no current key.
 */
CTEST2(splinterdb_quick, test_iterator_reversal_with_three_branches)
{
   insert_string_kv(data->kvsb, "c", "c0");
   force_flush_current_memtable(data->kvsb);
   insert_string_kv(data->kvsb, "b", "b1");
   insert_string_kv(data->kvsb, "d", "d1");
   force_flush_current_memtable(data->kvsb);
   insert_string_kv(data->kvsb, "a", "a2");
   insert_string_kv(data->kvsb, "d", "d2");

   splinterdb_iterator *it = NULL;
   int                  rc = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, NULL_SLICE);
   ASSERT_EQUAL(0, rc);

   check_current_string_kv(it, "a", "a2");
   splinterdb_iterator_next(it);
   check_current_string_kv(it, "b", "b1");
   splinterdb_iterator_next(it);
   check_current_string_kv(it, "c", "c0");
   splinterdb_iterator_prev(it);
   check_current_string_kv(it, "b", "b1");
   splinterdb_iterator_prev(it);
   check_current_string_kv(it, "a", "a2");
   splinterdb_iterator_prev(it);
   ASSERT_FALSE(splinterdb_iterator_valid(it));

   ASSERT_EQUAL(0, splinterdb_iterator_status(it));
   splinterdb_iterator_deinit(it);
}

/*
 * Rewriting existing keys must not keep growing the trunk. Leaf splits size the
 * new leaves by the leaf's estimated number of unique keys, and that estimate
 * used to report every tuple as unique once a leaf with compacted data received
 * filtered bundles, so each pass over the keys split leaves of duplicates.
 */
CTEST2(splinterdb_quick, test_rewriting_keys_does_not_grow_trunk)
{
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   data->cfg.memtable_capacity = 1 * Mega;
   data->cfg.fanout            = 2;
   data->cfg.disk_size         = 384 * Mega;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));
   core_handle *core = (core_handle *)splinterdb_get_trunk_handle(data->kvsb);

   const uint64 num_keys     = 100000;
   uint64       first_height = 0;
   uint64       height       = 0;
   char         key[24];
   char         value[100];
   for (uint64 pass = 0; pass < 4; pass++) {
      memset(value, 'a' + pass, sizeof(value));
      for (uint64 i = 0; i < num_keys; i++) {
         // Visit the keys in a different order on each pass.
         uint64 k = (i * 2654435761ULL + pass * 40503) % num_keys;
         snprintf(key, sizeof(key), "%020lu", k);
         ASSERT_EQUAL(0,
                      splinterdb_insert(data->kvsb,
                                        slice_create(20, key),
                                        slice_create(sizeof(value), value),
                                        NULL));
      }
      ASSERT_TRUE(SUCCESS(task_perform_until_quiescent(core->ts)));
      ASSERT_TRUE(SUCCESS(trunk_get_height(&core->trunk_context, &height)));
      if (pass == 0) {
         first_height = height;
      }
   }
   // Pending duplicates may still justify one more level.
   ASSERT_TRUE(height <= first_height + 1,
               "trunk grew from height %lu to %lu",
               first_height,
               height);
}

/*
 * Test case to verify the interfaces to close() and reopen() a KVS work
 * as expected. After reopening the KVS, we should be able to retrieve data
 * that was inserted in the previous open.
 */
CTEST2(splinterdb_quick, test_close_and_reopen)
{
   slice        user_key = slice_create(strlen("some-key"), "some-key");
   const char  *val      = "some-value";
   const size_t val_len  = strlen(val);

   int rc =
      splinterdb_insert(data->kvsb, user_key, slice_create(val_len, val), NULL);
   ASSERT_EQUAL(0, rc);

   // Close and re-open the database
   rc = splinterdb_close(&data->kvsb, FALSE);
   ASSERT_EQUAL(0, rc);
   rc = splinterdb_open(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   slice value;

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   rc = splinterdb_lookup(data->kvsb, user_key, &result);
   ASSERT_EQUAL(0, rc);

   ASSERT_TRUE(splinterdb_lookup_found(&result));
   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(val_len, slice_length(value));
   ASSERT_STREQN(val,
                 slice_data(value),
                 slice_length(value),
                 "value found did not match expected 'val' up to %d bytes\n",
                 val_len);

   splinterdb_lookup_result_deinit(&result);
}

/*
 * With logging enabled, the two-log lifecycle records the live log in the
 * superblock at create/mount and discards it (seal + dec_ref) at a clean close.
 * A reopen must still find data inserted before the close.
 */
CTEST2(splinterdb_quick, test_logged_close_and_reopen)
{
   // Re-create the instance with logging enabled (SETUP created it without).
   int rc = splinterdb_close(&data->kvsb, FALSE);
   ASSERT_EQUAL(0, rc);
   data->cfg.use_log = TRUE;
   rc                = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   slice        user_key = slice_create(strlen("logged-key"), "logged-key");
   const char  *val      = "logged-value";
   const size_t val_len  = strlen(val);
   rc =
      splinterdb_insert(data->kvsb, user_key, slice_create(val_len, val), NULL);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_close(&data->kvsb, FALSE);
   ASSERT_EQUAL(0, rc);
   rc = splinterdb_open(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   rc = splinterdb_lookup(data->kvsb, user_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   slice value;
   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(val_len, slice_length(value));
   ASSERT_STREQN(val,
                 slice_data(value),
                 slice_length(value),
                 "logged reopen value mismatch up to %d bytes\n",
                 val_len);
   splinterdb_lookup_result_deinit(&result);
}

/*
 * Regression test for bug where repeating a cycle of insert-close-reopen
 * causes a space leak and eventually hits an assertion
 * (fixed in PR #214 / commit 8b33fd149d33054173790a8a30b99e97f08ffa81)
 */
CTEST2(splinterdb_quick, test_repeated_insert_close_reopen)
{
   char  *keystring = "some-key";
   size_t key_len   = strlen(keystring);
   char  *val       = "f";
   size_t val_len   = strlen(val);

   for (int i = 0; i < 20; i++) {
      int rc = splinterdb_insert(data->kvsb,
                                 slice_create(key_len, keystring),
                                 slice_create(val_len, val),
                                 NULL);
      ASSERT_EQUAL(0, rc, "Insert is expected to pass, iter=%d.", i);

      rc = splinterdb_close(&data->kvsb, FALSE);
      ASSERT_EQUAL(0, rc);

      rc = splinterdb_open(&data->cfg, &data->kvsb);
      ASSERT_EQUAL(0, rc);
   }
}

// Check that the value-oriented functions work sensibly with a custom
// data_config
CTEST2(splinterdb_quick, test_custom_data_config)
{
   // We need to reconfigure Splinter with user-specified data_config
   // Tear down default instance, and create a new one.
   splinterdb_close(&data->kvsb, FALSE);
   data->cfg.data_cfg = test_data_config;
   int rc             = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   const size_t key_len   = 3;
   const char  *key_data  = "foo";
   slice        user_key  = slice_create(key_len, key_data);
   data_handle  msg       = {.ref_count = 1};
   slice        msg_slice = slice_create(sizeof(msg), &msg);

   ASSERT_EQUAL(0, rc);
   rc = splinterdb_insert(data->kvsb, user_key, msg_slice, NULL);

   // confirm its there
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   rc = splinterdb_lookup(data->kvsb, user_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   slice value;
   rc = splinterdb_lookup_result_value(&result, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(0, slice_lex_cmp(value, msg_slice));

   // insert a message that adds to the refcount
   msg.ref_count = 5;
   rc            = splinterdb_update(data->kvsb, user_key, msg_slice, NULL);
   ASSERT_EQUAL(0, rc);

   // check still found
   rc = splinterdb_lookup(data->kvsb, user_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   // insert a message that drops the refcount to zero
   msg.ref_count = -6;
   rc            = splinterdb_update(data->kvsb, user_key, msg_slice, NULL);
   ASSERT_EQUAL(0, rc);

   // on lookup, merge will decide the tuple is deleted
   rc = splinterdb_lookup(data->kvsb, user_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_lookup_found(&result));

   // add it back as a value
   msg.ref_count = 12;
   rc            = splinterdb_insert(data->kvsb, user_key, msg_slice, NULL);
   ASSERT_EQUAL(0, rc);

   // delete it using a raw message
   rc = splinterdb_delete(data->kvsb, user_key, NULL);
   ASSERT_EQUAL(0, rc);

   // on lookup, it should not be found
   rc = splinterdb_lookup(data->kvsb, user_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_lookup_found(&result));

   splinterdb_lookup_result_deinit(&result);
}

CTEST2(splinterdb_quick, test_existence_only_memtable_lookup)
{
   slice user_key = slice_create(strlen("existence-key"), "existence-key");
   slice value    = slice_create(strlen("some-value"), "some-value");

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      data->kvsb, &result, SPLINTERDB_LOOKUP_MIGHT_EXIST, 0, NULL);

   int rc = splinterdb_lookup(data->kvsb, user_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_lookup_found(&result));

   rc = splinterdb_insert(data->kvsb, user_key, value, NULL);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_lookup(data->kvsb, user_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   slice looked_up_value;
   rc = splinterdb_lookup_result_value(&result, &looked_up_value);
   ASSERT_EQUAL(EINVAL, rc);

   rc = splinterdb_delete(data->kvsb, user_key, NULL);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_lookup(data->kvsb, user_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_lookup_found(&result));

   splinterdb_lookup_result_deinit(&result);
}

CTEST2(splinterdb_quick, test_existence_only_trunk_lookup_skips_branches)
{
   splinterdb_close(&data->kvsb, FALSE);
   create_default_cfg(&data->cfg, &data->default_data_cfg.super);
   data->cfg.use_stats = 1;

   int rc = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   slice user_key = slice_create(strlen("trunk-key"), "trunk-key");
   slice value    = slice_create(strlen("trunk-value"), "trunk-value");

   rc = splinterdb_insert(data->kvsb, user_key, value, NULL);
   ASSERT_EQUAL(0, rc);

   splinterdb_close(&data->kvsb, FALSE);
   rc = splinterdb_open(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   core_handle *core = (core_handle *)splinterdb_get_trunk_handle(data->kvsb);
   ASSERT_NOT_NULL(core->trunk_context.stats);
   core_reset_stats(core);

   splinterdb_lookup_result existence_result;
   splinterdb_lookup_result_init(
      data->kvsb, &existence_result, SPLINTERDB_LOOKUP_MIGHT_EXIST, 0, NULL);

   rc = splinterdb_lookup(data->kvsb, user_key, &existence_result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&existence_result));
   ASSERT_EQUAL(0, sum_branch_lookups(data->kvsb));

   slice looked_up_value;
   rc = splinterdb_lookup_result_value(&existence_result, &looked_up_value);
   ASSERT_EQUAL(EINVAL, rc);
   splinterdb_lookup_result_deinit(&existence_result);

   core_reset_stats(core);

   splinterdb_lookup_result normal_result;
   splinterdb_lookup_result_init(
      data->kvsb, &normal_result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   rc = splinterdb_lookup(data->kvsb, user_key, &normal_result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&normal_result));
   ASSERT_TRUE(sum_branch_lookups(data->kvsb) > 0);
   splinterdb_lookup_result_deinit(&normal_result);
}

CTEST2(splinterdb_quick, test_write_api_returns_old_value)
{
   slice user_key = slice_create(strlen("old-value-key"), "old-value-key");
   slice value0   = slice_create(strlen("old-value-0"), "old-value-0");
   slice value1   = slice_create(strlen("old-value-1"), "old-value-1");

   splinterdb_lookup_result old_result;
   splinterdb_lookup_result_init(
      data->kvsb, &old_result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   int rc = splinterdb_insert(data->kvsb, user_key, value0, &old_result);
   ASSERT_EQUAL(0, rc);
   assert_lookup_result_not_found(&old_result);

   rc = splinterdb_insert(data->kvsb, user_key, value1, &old_result);
   ASSERT_EQUAL(0, rc);
   assert_lookup_result_matches_slice(&old_result, value0);

   rc = splinterdb_delete(data->kvsb, user_key, &old_result);
   ASSERT_EQUAL(0, rc);
   assert_lookup_result_matches_slice(&old_result, value1);

   rc = splinterdb_insert(data->kvsb, user_key, value0, &old_result);
   ASSERT_EQUAL(0, rc);
   assert_lookup_result_not_found(&old_result);

   splinterdb_lookup_result_deinit(&old_result);
}

CTEST2(splinterdb_quick, test_write_api_old_result_existence_only)
{
   slice user_key = slice_create(strlen("exist-old"), "exist-old");
   slice value0   = slice_create(strlen("existence-old-0"), "existence-old-0");
   slice value1   = slice_create(strlen("existence-old-1"), "existence-old-1");

   splinterdb_lookup_result old_result;
   splinterdb_lookup_result_init(
      data->kvsb, &old_result, SPLINTERDB_LOOKUP_MIGHT_EXIST, 0, NULL);

   int rc = splinterdb_insert(data->kvsb, user_key, value0, &old_result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_lookup_found(&old_result));

   rc = splinterdb_insert(data->kvsb, user_key, value1, &old_result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&old_result));

   slice looked_up_value;
   rc = splinterdb_lookup_result_value(&old_result, &looked_up_value);
   ASSERT_EQUAL(EINVAL, rc);

   rc = splinterdb_delete(data->kvsb, user_key, &old_result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&old_result));

   rc = splinterdb_insert(data->kvsb, user_key, value0, &old_result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_lookup_found(&old_result));

   splinterdb_lookup_result_deinit(&old_result);
}

CTEST2(splinterdb_quick, test_write_api_old_result_custom_merge_semantics)
{
   splinterdb_close(&data->kvsb, FALSE);
   data->cfg.data_cfg = test_data_config;
   int rc             = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   slice       user_key  = slice_create(strlen("merge-key"), "merge-key");
   data_handle msg       = {.ref_count = 1};
   slice       msg_slice = slice_create(sizeof(msg), &msg);

   splinterdb_lookup_result old_result;
   splinterdb_lookup_result_init(
      data->kvsb, &old_result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   rc = splinterdb_insert(data->kvsb, user_key, msg_slice, &old_result);
   ASSERT_EQUAL(0, rc);
   assert_lookup_result_not_found(&old_result);

   msg.ref_count = 5;
   msg_slice     = slice_create(sizeof(msg), &msg);
   rc = splinterdb_update(data->kvsb, user_key, msg_slice, &old_result);
   ASSERT_EQUAL(0, rc);
   assert_lookup_result_matches_slice(
      &old_result, slice_create(sizeof(msg), &(data_handle){.ref_count = 1}));

   merge_accumulator expected;
   merge_accumulator_init(&expected, PROCESS_PRIVATE_HEAP_ID);
   test_data_generate_message(
      test_data_config, MESSAGE_TYPE_INSERT, 6, &expected);

   msg.ref_count = -6;
   msg_slice     = slice_create(sizeof(msg), &msg);
   rc = splinterdb_update(data->kvsb, user_key, msg_slice, &old_result);
   ASSERT_EQUAL(0, rc);
   assert_lookup_result_matches_slice(&old_result,
                                      merge_accumulator_to_value(&expected));

   merge_accumulator_deinit(&expected);

   msg.ref_count = 12;
   msg_slice     = slice_create(sizeof(msg), &msg);
   rc = splinterdb_insert(data->kvsb, user_key, msg_slice, &old_result);
   ASSERT_EQUAL(0, rc);
   assert_lookup_result_not_found(&old_result);

   splinterdb_lookup_result_deinit(&old_result);
}

CTEST2(splinterdb_quick, test_write_api_old_result_merges_memtable_and_trunk)
{
   splinterdb_close(&data->kvsb, FALSE);
   data->cfg.data_cfg = test_data_config;
   int rc             = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   slice user_key  = slice_create(strlen("trunk-merge-key"), "trunk-merge-key");
   data_handle msg = {.ref_count = 3};
   slice       msg_slice = slice_create(sizeof(msg), &msg);

   rc = splinterdb_insert(data->kvsb, user_key, msg_slice, NULL);
   ASSERT_EQUAL(0, rc);

   splinterdb_close(&data->kvsb, FALSE);
   rc = splinterdb_open(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   msg.ref_count = 4;
   msg_slice     = slice_create(sizeof(msg), &msg);
   rc            = splinterdb_update(data->kvsb, user_key, msg_slice, NULL);
   ASSERT_EQUAL(0, rc);

   splinterdb_lookup_result old_result;
   splinterdb_lookup_result_init(
      data->kvsb, &old_result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   merge_accumulator expected;
   merge_accumulator_init(&expected, PROCESS_PRIVATE_HEAP_ID);
   test_data_generate_message(
      test_data_config, MESSAGE_TYPE_INSERT, 7, &expected);

   msg.ref_count = 2;
   msg_slice     = slice_create(sizeof(msg), &msg);
   rc = splinterdb_update(data->kvsb, user_key, msg_slice, &old_result);
   ASSERT_EQUAL(0, rc);
   assert_lookup_result_matches_slice(&old_result,
                                      merge_accumulator_to_value(&expected));

   merge_accumulator_deinit(&expected);
   splinterdb_lookup_result_deinit(&old_result);
}

CTEST2(splinterdb_quick, test_write_api_old_result_respects_trunk_delete_shadow)
{
   slice user_key = slice_create(strlen("shadow-key"), "shadow-key");
   slice value0   = slice_create(strlen("shadow-old"), "shadow-old");
   slice value1   = slice_create(strlen("shadow-new"), "shadow-new");

   int rc = splinterdb_insert(data->kvsb, user_key, value0, NULL);
   ASSERT_EQUAL(0, rc);

   splinterdb_close(&data->kvsb, FALSE);
   rc = splinterdb_open(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_delete(data->kvsb, user_key, NULL);
   ASSERT_EQUAL(0, rc);

   splinterdb_lookup_result old_result;
   splinterdb_lookup_result_init(
      data->kvsb, &old_result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   rc = splinterdb_insert(data->kvsb, user_key, value1, &old_result);
   ASSERT_EQUAL(0, rc);
   assert_lookup_result_not_found(&old_result);

   splinterdb_lookup_result_deinit(&old_result);
}

CTEST2(splinterdb_quick, test_iterator_custom_comparator)
{
   // We need to reconfigure Splinter with user-specified key comparator fn.
   // Tear down default instance, and create a new one.
   splinterdb_close(&data->kvsb, FALSE);

   data->default_data_cfg.super.key_compare = custom_key_comparator;
   data->default_data_cfg.num_comparisons   = 0;

   int rc = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   const int num_inserts = 50;

   rc = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator *it = NULL;
   rc                      = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, NULL_SLICE);
   ASSERT_EQUAL(0, rc);

   int i = 0;
   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      rc = check_current_tuple(it, i);
      ASSERT_EQUAL(0, rc);
      i++;
   }

   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);

   // Expect that iterator has stopped at num_inserts
   ASSERT_EQUAL(num_inserts, i);
   ASSERT_TRUE(data->default_data_cfg.num_comparisons > (2 * num_inserts));

   bool32 is_valid = splinterdb_iterator_valid(it);
   ASSERT_FALSE(is_valid);

   if (it) {
      splinterdb_iterator_deinit(it);
   }
}

/*
 * Test case to verify that iterator interfaces work correctly.
 * Prior to fix for issue #419, this test case would fail with an assertion
 * (that is activated only) in debug mode runs.
 */
CTEST2(splinterdb_quick, test_iterator_init_bug)
{
   // We need to reconfigure Splinter with user-specified data_config
   // Tear down default instance, and create a new one.
   splinterdb_close(&data->kvsb, FALSE);
   data->cfg.data_cfg = test_data_config;

   int rc = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   // Iterator init should find nothing when no keys were inserted, yet.
   splinterdb_iterator *it = NULL;
   rc                      = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, NULL_SLICE);
   ASSERT_EQUAL(0, rc);

   bool32 iter_valid = splinterdb_iterator_valid(it);
   ASSERT_FALSE(iter_valid);

   splinterdb_iterator_deinit(it);

   // Insert some kv-pairs, so iterator is initialized to something valid
   const int num_inserts = 5;
   rc                    = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   it = NULL;
   rc = splinterdb_iterator_init(
      data->kvsb, &it, greater_than_or_equal, NULL_SLICE);
   ASSERT_EQUAL(0, rc);

   iter_valid = splinterdb_iterator_valid(it);
   ASSERT_TRUE(iter_valid);

   int i = 0;
   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      i++;
   }
   ASSERT_EQUAL(num_inserts, i);

   splinterdb_iterator_deinit(it);
}

/*
 * ------------------------------------------------------------------------
 * Test that SplinterDB can be created with the task system configured with
 * background threads.
 * ------------------------------------------------------------------------
 */
CTEST2(splinterdb_quick, test_splinterdb_create_w_background_threads)
{
   splinterdb_close(&data->kvsb, FALSE);

   default_data_config_init(&data->default_data_cfg.super);
   create_default_cfg(&data->cfg, &data->default_data_cfg.super);

   // Task system should be setup with background threads
   data->cfg.num_normal_bg_threads   = 1;
   data->cfg.num_memtable_bg_threads = 1;

   int rv = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rv);
}

/*
 * ------------------------------------------------------------------------
 * Test that SplinterDB can be created even when background threads use
 * up all the slots.
 * ------------------------------------------------------------------------
 */
CTEST2(splinterdb_quick, test_splinterdb_create_w_all_background_threads)
{
   splinterdb_close(&data->kvsb, FALSE);

   default_data_config_init(&data->default_data_cfg.super);
   create_default_cfg(&data->cfg, &data->default_data_cfg.super);

   // Task system should be setup with all background threads
   data->cfg.num_normal_bg_threads   = (MAX_THREADS - 2);
   data->cfg.num_memtable_bg_threads = 1;

   int rv = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rv);
}

/*
 * ********************************************************************************
 * Define minions and helper functions here, after all test cases are
 * enumerated.
 * ********************************************************************************
 */

static void
create_default_cfg(splinterdb_config *out_cfg, data_config *default_data_cfg)
{
   *out_cfg = (splinterdb_config){.filename   = TEST_CONFIG_DEFAULT_IO_FILENAME,
                                  .cache_size = 64 * Mega,
                                  .disk_size  = 127 * Mega,
                                  .use_shmem  = FALSE,
                                  .data_cfg   = default_data_cfg};
}

static uint64
force_flush_current_memtable(splinterdb *kvsb)
{
   core_handle *core = (core_handle *)splinterdb_get_trunk_handle(kvsb);

   platform_status rc = task_perform_until_quiescent(core->ts);
   ASSERT_TRUE(SUCCESS(rc));

   uint64 generation;
   rc = memtable_force_rotation(&core->mt_ctxt, &generation);
   ASSERT_TRUE(SUCCESS(rc));

   rc = task_perform_until_quiescent(core->ts);
   ASSERT_TRUE(SUCCESS(rc));
   return generation;
}

static void
insert_string_kv(splinterdb *kvsb, const char *k, const char *v)
{
   int rc = splinterdb_insert(
      kvsb, slice_create(strlen(k), k), slice_create(strlen(v), v), NULL);
   ASSERT_EQUAL(0, rc);
}

static void
check_current_string_kv(splinterdb_iterator *it, const char *k, const char *v)
{
   ASSERT_TRUE(splinterdb_iterator_valid(it));
   slice key, value;
   splinterdb_iterator_get_current(it, &key, &value);
   ASSERT_EQUAL(strlen(k), slice_length(key));
   ASSERT_STREQN(k, slice_data(key), slice_length(key));
   ASSERT_EQUAL(strlen(v), slice_length(value));
   ASSERT_STREQN(v, slice_data(value), slice_length(value));
}

static uint64
sum_branch_lookups(const splinterdb *kvsb)
{
   const core_handle *core  = splinterdb_get_trunk_handle(kvsb);
   threadid           tid   = platform_get_tid();
   uint64             total = 0;

   for (uint64 height = 0; height < TRUNK_MAX_HEIGHT; height++) {
      total += core->trunk_context.stats[tid].branch_lookups[height];
   }

   return total;
}

static void
assert_lookup_result_not_found(const splinterdb_lookup_result *result)
{
   ASSERT_FALSE(splinterdb_lookup_found(result));
}

static void
assert_lookup_result_matches_slice(const splinterdb_lookup_result *result,
                                   slice                           expected)
{
   slice value;

   ASSERT_TRUE(splinterdb_lookup_found(result));
   ASSERT_EQUAL(0, splinterdb_lookup_result_value(result, &value));
   ASSERT_EQUAL(slice_length(expected), slice_length(value));
   ASSERT_EQUAL(0, slice_lex_cmp(value, expected));
}

static void *
auto_registered_lookup_thread(void *arg)
{
   auto_registration_thread_args *args = (auto_registration_thread_args *)arg;
   char                           key_data[] = "thread-key";
   slice user_key = slice_create(sizeof(key_data), key_data);

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      args->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   args->tid_before = platform_get_tid();
   args->rc         = splinterdb_lookup(args->kvsb, user_key, &result);
   args->tid_after  = platform_get_tid();

   splinterdb_lookup_result_deinit(&result);
   return NULL;
}

static void *
fetch_and_update_thread(void *arg)
{
   fetch_and_update_thread_args *args = arg;
   args->rc =
      splinterdb_insert(args->kvsb, args->key, args->value, args->old_result);
   return NULL;
}

static void *
stats_reset_worker(void *arg)
{
   stats_reset_worker_args *params = arg;
   char                     key_data[32];
   char                     value_data[32];
   int                      key_len = snprintf(
      key_data, sizeof(key_data), "stats-reset-key-%lu", params->worker_no);
   int value_len = snprintf(value_data,
                            sizeof(value_data),
                            "stats-reset-value-%lu",
                            params->worker_no);
   platform_assert(0 < key_len && key_len < sizeof(key_data));
   platform_assert(0 < value_len && value_len < sizeof(value_data));
   slice tuple_key = slice_create(key_len, key_data);
   slice value     = slice_create(value_len, value_data);

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      params->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   __atomic_fetch_add(params->started, 1, __ATOMIC_RELEASE);

   while (!__atomic_load_n(params->stop, __ATOMIC_ACQUIRE)) {
      params->rc = splinterdb_insert(params->kvsb, tuple_key, value, NULL);
      if (params->rc != 0) {
         break;
      }
      params->rc = splinterdb_lookup(params->kvsb, tuple_key, &result);
      if (params->rc != 0) {
         break;
      }
      __atomic_fetch_add(&params->operations, 1, __ATOMIC_RELAXED);
   }

   splinterdb_lookup_result_deinit(&result);
   platform_deregister_thread();
   return NULL;
}

/*
 * Helper function to insert n-keys (num_inserts), using pre-formatted
 * key and value strings.
 *
 * Returns: Return code: rc == 0 => success; anything else => failure
 */
static int
insert_some_keys(const int num_inserts, splinterdb *kvsb)
{
   int rc = 0;
   // insert keys backwards, just for kicks
   for (int i = num_inserts - 1; i >= 0; i--) {
      char key[TEST_INSERT_KEY_LENGTH] = {0};
      char val[TEST_INSERT_VAL_LENGTH] = {0};

      ASSERT_EQUAL(KEY_FMT_LENGTH, snprintf(key, sizeof(key), key_fmt, i));
      ASSERT_EQUAL(VAL_FMT_LENGTH, snprintf(val, sizeof(val), val_fmt, i));

      rc = splinterdb_insert(kvsb,
                             slice_create(sizeof(key), key),
                             slice_create(sizeof(val), val),
                             NULL);
      ASSERT_EQUAL(0, rc);
   }

   return rc;
}

/*
 * Helper function to insert n-keys (num_inserts), using pre-formatted
 * key and value strings. Allows user to specify start value and increment
 * between keys. This can be used to load either fully sequential keys
 * or some with defined gaps.
 *
 * Parameters:
 *  kvsb    - SplinterDB handle
 *  minkey  - Start key to insert
 *  numkeys - # of keys to insert
 *  incr    - Increment between keys (default is 1)
 *
 * Returns: Return code: rc == 0 => success; anything else => failure
 */
static int
insert_keys(splinterdb *kvsb, const int minkey, int numkeys, const int incr)
{
   int rc = -1;

   // Minimally, error check input arguments
   if (!kvsb || (numkeys <= 0) || (incr < 0))
      return rc;

   // insert keys forwards, starting from minkey value
   for (int kctr = minkey; numkeys; kctr += incr, numkeys--) {
      char key[TEST_INSERT_KEY_LENGTH] = {0};
      char val[TEST_INSERT_VAL_LENGTH] = {0};

      snprintf(key, sizeof(key), key_fmt, kctr);
      snprintf(val, sizeof(val), val_fmt, kctr);

      rc = splinterdb_insert(kvsb,
                             slice_create(sizeof(key), key),
                             slice_create(sizeof(val), val),
                             NULL);
      ASSERT_EQUAL(0, rc);
   }
   return rc;
}

/*
 * Work horse routine to check if the current tuple pointed to by the
 * iterator is the expected one, as indicated by its index,
 * expected_i. We use pre-constructed key / value formats to verify
 * if the current tuple is of the expected format.
 *
 * Returns: Return code: rc == 0 => success; anything else => failure
 */
static int
check_current_tuple(splinterdb_iterator *it, const int expected_i)
{
   int rc = 0;

   char expected_key[TEST_INSERT_KEY_LENGTH] = {0};
   char expected_val[TEST_INSERT_VAL_LENGTH] = {0};
   ASSERT_EQUAL(
      KEY_FMT_LENGTH,
      snprintf(expected_key, sizeof(expected_key), key_fmt, expected_i));
   ASSERT_EQUAL(
      VAL_FMT_LENGTH,
      snprintf(expected_val, sizeof(expected_val), val_fmt, expected_i));

   slice key, value;

   splinterdb_iterator_get_current(it, &key, &value);

   ASSERT_EQUAL(TEST_INSERT_KEY_LENGTH, slice_length(key));
   ASSERT_EQUAL(TEST_INSERT_VAL_LENGTH, slice_length(value));

   int key_cmp = memcmp(expected_key, slice_data(key), slice_length(key));
   int val_cmp = memcmp(expected_val, slice_data(value), slice_length(value));
   ASSERT_EQUAL(0, key_cmp);
   ASSERT_EQUAL(0, val_cmp);

   return rc;
}

static int
check_bounded_iterator_sequence(splinterdb *kvsb,
                                comparison  min_key_comparison,
                                slice       min_key,
                                comparison  max_key_comparison,
                                slice       max_key,
                                comparison  start_type,
                                const int  *expected,
                                uint64      expected_count)
{
   splinterdb_iterator *it = NULL;
   int                  rc = splinterdb_iterator_init_with_bounds(kvsb,
                                                 &it,
                                                 min_key_comparison,
                                                 min_key,
                                                 max_key_comparison,
                                                 max_key,
                                                 start_type,
                                                 NULL_SLICE);
   ASSERT_EQUAL(0, rc);

   for (uint64 i = 0; i < expected_count; i++) {
      ASSERT_TRUE(splinterdb_iterator_valid(it));
      rc = check_current_tuple(it, expected[i]);
      ASSERT_EQUAL(0, rc);
      if (start_type <= less_than_or_equal) {
         splinterdb_iterator_prev(it);
      } else {
         splinterdb_iterator_next(it);
      }
   }

   ASSERT_FALSE(splinterdb_iterator_valid(it));
   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);
   splinterdb_iterator_deinit(it);
   return rc;
}

static int
check_bounded_iterator_current_from_start(splinterdb *kvsb,
                                          comparison  min_key_comparison,
                                          slice       min_key,
                                          comparison  max_key_comparison,
                                          slice       max_key,
                                          comparison  start_key_comparison,
                                          slice       start_key,
                                          int         expected_i)
{
   splinterdb_iterator *it = NULL;
   int                  rc = splinterdb_iterator_init_with_bounds(kvsb,
                                                 &it,
                                                 min_key_comparison,
                                                 min_key,
                                                 max_key_comparison,
                                                 max_key,
                                                 start_key_comparison,
                                                 start_key);
   ASSERT_EQUAL(0, rc);

   ASSERT_TRUE(splinterdb_iterator_valid(it));
   rc = check_current_tuple(it, expected_i);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);
   splinterdb_iterator_deinit(it);
   return rc;
}

static int
check_bounded_iterator_boundary_from_start(splinterdb *kvsb,
                                           comparison  min_key_comparison,
                                           slice       min_key,
                                           comparison  max_key_comparison,
                                           slice       max_key,
                                           comparison  start_key_comparison,
                                           slice       start_key,
                                           bool32      step_next,
                                           int         expected_i)
{
   splinterdb_iterator *it = NULL;
   int                  rc = splinterdb_iterator_init_with_bounds(kvsb,
                                                 &it,
                                                 min_key_comparison,
                                                 min_key,
                                                 max_key_comparison,
                                                 max_key,
                                                 start_key_comparison,
                                                 start_key);
   ASSERT_EQUAL(0, rc);

   ASSERT_FALSE(splinterdb_iterator_valid(it));
   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);

   if (step_next) {
      ASSERT_TRUE(splinterdb_iterator_can_next(it));
      ASSERT_FALSE(splinterdb_iterator_can_prev(it));
      splinterdb_iterator_next(it);
   } else {
      ASSERT_FALSE(splinterdb_iterator_can_next(it));
      ASSERT_TRUE(splinterdb_iterator_can_prev(it));
      splinterdb_iterator_prev(it);
   }

   ASSERT_TRUE(splinterdb_iterator_valid(it));
   rc = check_current_tuple(it, expected_i);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);
   splinterdb_iterator_deinit(it);
   return rc;
}

// Test moving iterator 2 steps up, 1 step back and then all the way back down
static int
test_two_step_iterator(splinterdb *kvsb,
                       slice       start_key,
                       int         num_keys,
                       int         minkey,
                       int         start_i,
                       int         hop_i)
{
   int                  rc;
   splinterdb_iterator *it = NULL;
   rc = splinterdb_iterator_init(kvsb, &it, greater_than_or_equal, start_key);
   ASSERT_EQUAL(0, rc);

   for (int i = start_i; i < num_keys; i++) {
      bool32 is_valid = splinterdb_iterator_valid(it);
      ASSERT_TRUE(is_valid);
      check_current_tuple(it, i * hop_i + minkey);
      splinterdb_iterator_next(it);

      if (i < num_keys - 2) {
         is_valid = splinterdb_iterator_valid(it);
         ASSERT_TRUE(is_valid);
         check_current_tuple(it, (i + 1) * hop_i + minkey);
         splinterdb_iterator_next(it);

         is_valid = splinterdb_iterator_valid(it);
         ASSERT_TRUE(is_valid);
         check_current_tuple(it, (i + 2) * hop_i + minkey);
         splinterdb_iterator_prev(it);
      }
   }

   bool32 is_valid = splinterdb_iterator_valid(it);
   ASSERT_FALSE(is_valid);
   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_iterator_can_prev(it));

   // Start going down
   splinterdb_iterator_prev(it);
   for (int i = num_keys - 1; i >= 0; i--) {
      bool32 is_valid = splinterdb_iterator_valid(it);
      ASSERT_TRUE(is_valid);
      check_current_tuple(it, i * hop_i + minkey);

      splinterdb_iterator_prev(it);
   }

   is_valid = splinterdb_iterator_valid(it);
   ASSERT_FALSE(is_valid);
   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator_deinit(it);
   return rc;
}

// A user-specified spy comparator
static int
custom_key_comparator(const data_config *cfg, user_key key1, user_key key2)
{
   platform_assert(slice_data(key1.key) != NULL);
   platform_assert(slice_data(key2.key) != NULL);

   int r = slice_lex_cmp(key1.key, key2.key);

   // record that this spy was called
   comparison_counting_data_config *ccfg =
      (comparison_counting_data_config *)cfg;
   ccfg->num_comparisons += 1;
   return r;
}

static bool32 hash_range_reject_unmatched_merge;

/* Different tenants deliberately collide in the full 32-bit hash, at every
 * seed. Only the predicate may decide whether a tuple belongs in the result. */
static uint32
hash_range_key_hash(const data_config *cfg, user_key k, uint32 seed)
{
   const uint8 *bytes  = slice_data(k.key);
   uint8        tenant = bytes[slice_length(k.key) - 1];
   return seed ^ (tenant == 'b' ? 'a' : tenant);
}

static int
hash_range_add(const data_config *cfg,
               slice              k,
               message            older,
               merge_accumulator *newer)
{
   const char *bytes = slice_data(k);
   if (hash_range_reject_unmatched_merge && bytes[slice_length(k) - 1] != 'a') {
      return EINVAL;
   }
   char       *new_value = merge_accumulator_data(newer);
   const char *old_value = message_data(older);
   new_value[0] += old_value[0] - '0';
   merge_accumulator_set_class(newer, message_class(older));
   return 0;
}

static int
hash_range_finalize(const data_config *cfg, slice k, merge_accumulator *msg)
{
   const char *bytes = slice_data(k);
   if (hash_range_reject_unmatched_merge && bytes[slice_length(k) - 1] != 'a') {
      return EINVAL;
   }
   merge_accumulator_set_class(msg, MESSAGE_TYPE_INSERT);
   return 0;
}

typedef struct range_tenant_context {
   char     tenant;
   uint64   calls;
   threadid last_tid;
} range_tenant_context;

static bool
range_tenant_matches(void *context, slice k)
{
   range_tenant_context *tenant = context;
   tenant->calls++;
   tenant->last_tid = platform_get_tid();
   return slice_length(k) != 0
          && ((const char *)slice_data(k))[slice_length(k) - 1]
                == tenant->tenant;
}

CTEST2(splinterdb_quick, test_filtered_range_merges_only_matching_keys)
{
   data->default_data_cfg.super.key_hash           = hash_range_key_hash;
   data->default_data_cfg.super.merge_tuples       = hash_range_add;
   data->default_data_cfg.super.merge_tuples_final = hash_range_finalize;
   range_tenant_context tenant                     = {.tenant = 'a'};
   const char          *keys[] = {"0a", "0b", "1a", "1b", "2a", "2b"};
   for (uint64 i = 0; i < ARRAY_SIZE(keys); i++) {
      insert_string_kv(data->kvsb, keys[i], "1");
   }
   force_flush_current_memtable(data->kvsb);
   for (uint64 i = 0; i < ARRAY_SIZE(keys); i++) {
      ASSERT_EQUAL(
         0,
         splinterdb_update(
            data->kvsb, slice_create(2, keys[i]), slice_create(1, "2"), NULL));
   }
   ASSERT_EQUAL(0, splinterdb_delete(data->kvsb, slice_create(2, "1a"), NULL));
   insert_string_kv(data->kvsb, "3a", "3");
   /* A rejected update-only key also must not reach finalization. */
   ASSERT_EQUAL(
      0,
      splinterdb_update(
         data->kvsb, slice_create(2, "4b"), slice_create(1, "2"), NULL));
   hash_range_reject_unmatched_merge = TRUE;
   for (int forwards = 0; forwards < 2; forwards++) {
      splinterdb_iterator *it = NULL;
      ASSERT_EQUAL(0,
                   splinterdb_iterator_init_with_filter(
                      data->kvsb,
                      &it,
                      forwards ? greater_than_or_equal : less_than_or_equal,
                      NULL_SLICE,
                      range_tenant_matches,
                      &tenant,
                      slice_create(1, "a")));
      const char *expected[] = {"0a", "2a", "3a"};
      for (int i = 0; i < 3; i++) {
         check_current_string_kv(it, expected[forwards ? i : 2 - i], "3");
         if (forwards) {
            splinterdb_iterator_next(it);
         } else {
            splinterdb_iterator_prev(it);
         }
      }
      ASSERT_FALSE(splinterdb_iterator_valid(it));
      ASSERT_EQUAL(0, splinterdb_iterator_status(it));
      if (forwards) {
         splinterdb_iterator_prev(it);
         check_current_string_kv(it, "3a", "3");
         splinterdb_iterator_prev(it);
         check_current_string_kv(it, "2a", "3");
         splinterdb_iterator_next(it);
         check_current_string_kv(it, "3a", "3");
      } else {
         splinterdb_iterator_next(it);
         check_current_string_kv(it, "0a", "3");
         splinterdb_iterator_next(it);
         check_current_string_kv(it, "2a", "3");
         splinterdb_iterator_prev(it);
         check_current_string_kv(it, "0a", "3");
      }
      ASSERT_EQUAL(0, splinterdb_iterator_status(it));
      splinterdb_iterator_deinit(it);
   }
   hash_range_reject_unmatched_merge = FALSE;

   /* Persist/reopen, then cover exclusive bounds, no match, and opt-out. */
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->kvsb));
   splinterdb_iterator *it = NULL;
   ASSERT_EQUAL(
      0,
      splinterdb_iterator_init_with_bounds_and_filter(data->kvsb,
                                                      &it,
                                                      greater_than,
                                                      slice_create(2, "0a"),
                                                      less_than,
                                                      slice_create(2, "3a"),
                                                      greater_than_or_equal,
                                                      NULL_SLICE,
                                                      range_tenant_matches,
                                                      &tenant,
                                                      slice_create(1, "a")));
   check_current_string_kv(it, "2a", "3");
   splinterdb_iterator_next(it);
   ASSERT_FALSE(splinterdb_iterator_valid(it));
   splinterdb_iterator_prev(it);
   check_current_string_kv(it, "2a", "3");
   ASSERT_EQUAL(0, splinterdb_iterator_status(it));
   splinterdb_iterator_deinit(it);
   tenant.tenant = 'z';
   ASSERT_EQUAL(0,
                splinterdb_iterator_init_with_filter(data->kvsb,
                                                     &it,
                                                     greater_than_or_equal,
                                                     NULL_SLICE,
                                                     range_tenant_matches,
                                                     &tenant,
                                                     slice_create(1, "z")));
   ASSERT_FALSE(splinterdb_iterator_valid(it));
   ASSERT_EQUAL(0, splinterdb_iterator_status(it));
   splinterdb_iterator_deinit(it);
   ASSERT_EQUAL(0,
                splinterdb_iterator_init_with_filter(data->kvsb,
                                                     &it,
                                                     greater_than_or_equal,
                                                     NULL_SLICE,
                                                     NULL,
                                                     NULL,
                                                     NULL_SLICE));
   check_current_string_kv(it, "0a", "3");
   splinterdb_iterator_next(it);
   check_current_string_kv(it, "0b", "3");
   splinterdb_iterator_deinit(it);
}

static uint32
hash_range_sparse_hash(const data_config *cfg, user_key k, uint32 seed)
{
   uint64 encoded;
   memcpy(&encoded, slice_data(k.key), sizeof(encoded));
   uint64 id = __builtin_bswap64(encoded);
   if (id == UINT64_MAX) {
      return seed ^ 0x80000000;
   }
   if (id % 2048 == 0) {
      return seed;
   }
   return seed
          ^ ((platform_hash32(&encoded, sizeof(encoded), seed) & 0x7fffffff)
             | 1);
}

static bool
range_sparse_matches(void *context, slice k)
{
   uint64 encoded;
   memcpy(&encoded, slice_data(k), sizeof(encoded));
   uint64 id = __builtin_bswap64(encoded);
   return *(bool *)context ? id == UINT64_MAX : id % 2048 == 0;
}

CTEST2(splinterdb_quick, test_filtered_range_crosses_empty_trunk_leaves)
{
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   data->default_data_cfg.super.key_hash = hash_range_sparse_hash;
   data->cfg.memtable_capacity           = 4 * MiB;
   data->cfg.fanout                      = 2;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));
   core_handle *core = (core_handle *)splinterdb_get_trunk_handle(data->kvsb);
   /* Keep a viable memtable while forcing small trunk leaves for the scan. */
   ((trunk_config *)core->trunk_context.cfg)->incorporation_size_kv_bytes =
      64 * KiB;
   char value[32] = {0};
   for (uint64 id = 0; id < 32768; id++) {
      uint64 encoded = __builtin_bswap64(id);
      ASSERT_EQUAL(0,
                   splinterdb_insert(data->kvsb,
                                     slice_create(sizeof(encoded), &encoded),
                                     slice_create(sizeof(value), value),
                                     NULL));
   }
   force_flush_current_memtable(data->kvsb);
   uint64 height;
   ASSERT_TRUE(SUCCESS(trunk_get_height(&core->trunk_context, &height)));
   ASSERT_TRUE(height > 0);
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->kvsb));
   for (int forwards = 0; forwards < 2; forwards++) {
      bool                 no_match = false;
      uint64               hash_key = 0;
      splinterdb_iterator *it       = NULL;
      ASSERT_EQUAL(0,
                   splinterdb_iterator_init_with_filter(
                      data->kvsb,
                      &it,
                      forwards ? greater_than_or_equal : less_than_or_equal,
                      NULL_SLICE,
                      range_sparse_matches,
                      &no_match,
                      slice_create(sizeof(hash_key), &hash_key)));
      /* Later trunk spans must use the copied pruning key. Its original
       * storage may be reused immediately after initialization. */
      hash_key = UINT64_MAX;
      for (int i = 0; i < 16; i++) {
         ASSERT_TRUE(splinterdb_iterator_valid(it));
         slice k, v;
         splinterdb_iterator_get_current(it, &k, &v);
         uint64 encoded;
         memcpy(&encoded, slice_data(k), sizeof(encoded));
         ASSERT_EQUAL((forwards ? i : 15 - i) * 2048,
                      __builtin_bswap64(encoded));
         if (forwards) {
            splinterdb_iterator_next(it);
         } else {
            splinterdb_iterator_prev(it);
         }
      }
      ASSERT_FALSE(splinterdb_iterator_valid(it));
      ASSERT_EQUAL(0, splinterdb_iterator_status(it));
      splinterdb_iterator_deinit(it);

      no_match = true;
      hash_key = UINT64_MAX;
      ASSERT_EQUAL(0,
                   splinterdb_iterator_init_with_filter(
                      data->kvsb,
                      &it,
                      forwards ? greater_than_or_equal : less_than_or_equal,
                      NULL_SLICE,
                      range_sparse_matches,
                      &no_match,
                      slice_create(sizeof(hash_key), &hash_key)));
      ASSERT_FALSE(splinterdb_iterator_valid(it));
      ASSERT_EQUAL(0, splinterdb_iterator_status(it));
      splinterdb_iterator_deinit(it);
   }
}

/* The hash function stays fixed across every reopen. Only construction seeds
 * change. Tenants a/b collide at seed 11 and differ at every other seed. */
static bool32 range_mixed_seed_track;
static bool32 range_mixed_seed_reject_b_merge;
static uint32 range_mixed_seed_seen;

static uint32
range_mixed_seed_hash(const data_config *cfg, user_key k, uint32 seed)
{
   const uint8 *bytes = slice_data(k.key);
   uint64       len   = slice_length(k.key);
   ASSERT_TRUE(len == 1 || len == 9);
   if (range_mixed_seed_track && k.is_query_key && len == 1) {
      ASSERT_EQUAL('a', bytes[0]);
      range_mixed_seed_seen |= seed == 11   ? 1
                               : seed == 22 ? 2
                               : seed == 99 ? 4
                                            : 8;
   }
   uint8 tenant = bytes[len - 1];
   return seed * 2654435761U
          ^ ((tenant == 'b' && seed != 11) ? 0x80000000U : 0);
}

typedef struct range_mixed_seed_value {
   uint64 multiplier;
   uint64 addend;
   uint8  padding[80];
} range_mixed_seed_value;

/* Updates encode x -> multiplier*x + addend. Composition is deliberately
 * noncommutative: base 1, then append 2, then append 3 must produce 123. */
static int
range_mixed_seed_merge(const data_config *cfg,
                       slice              tuple_key,
                       message            older,
                       merge_accumulator *newer)
{
   const uint8 *key_bytes = slice_data(tuple_key);
   if (range_mixed_seed_reject_b_merge
       && key_bytes[slice_length(tuple_key) - 1] == 'b')
   {
      return EINVAL;
   }
   range_mixed_seed_value prior = {0};
   range_mixed_seed_value result;
   ASSERT_EQUAL_U(sizeof(result), merge_accumulator_length(newer));
   memcpy(&result, merge_accumulator_data(newer), sizeof(result));
   if (message_class(older) != MESSAGE_TYPE_DELETE) {
      ASSERT_EQUAL_U(sizeof(prior), message_length(older));
      memcpy(&prior, message_data(older), sizeof(prior));
   }
   result.addend += result.multiplier * prior.addend;
   result.multiplier *= prior.multiplier;
   memcpy(merge_accumulator_data(newer), &result, sizeof(result));
   merge_accumulator_set_class(newer,
                               message_class(older) == MESSAGE_TYPE_UPDATE
                                  ? MESSAGE_TYPE_UPDATE
                                  : MESSAGE_TYPE_INSERT);
   return 0;
}

static int
range_mixed_seed_finalize(const data_config *cfg,
                          slice              tuple_key,
                          merge_accumulator *msg)
{
   const uint8 *key_bytes = slice_data(tuple_key);
   if (range_mixed_seed_reject_b_merge
       && key_bytes[slice_length(tuple_key) - 1] == 'b')
   {
      return EINVAL;
   }
   range_mixed_seed_value result;
   ASSERT_EQUAL_U(sizeof(result), merge_accumulator_length(msg));
   memcpy(&result, merge_accumulator_data(msg), sizeof(result));
   result.multiplier = 0;
   memcpy(merge_accumulator_data(msg), &result, sizeof(result));
   merge_accumulator_set_class(msg, MESSAGE_TYPE_INSERT);
   return 0;
}

static slice
range_mixed_seed_key(uint8 *bytes, uint64 id, uint8 tenant)
{
   uint64 encoded = __builtin_bswap64(id);
   memcpy(bytes, &encoded, sizeof(encoded));
   bytes[8] = tenant;
   return slice_create(9, bytes);
}

static void
range_mixed_seed_construction_config(splinterdb *db, uint32 seed)
{
   core_handle *core = (core_handle *)splinterdb_get_trunk_handle(db);
   ASSERT_TRUE(SUCCESS(task_perform_until_quiescent(core->ts)));
   trunk_config *trunk_cfg = (trunk_config *)core->trunk_context.cfg;
   trunk_cfg->incorporation_size_kv_bytes          = 64 * KiB;
   ((routing_config *)trunk_cfg->filter_cfg)->seed = seed;
}

static void
range_mixed_seed_check_scan(splinterdb *db,
                            uint64      total,
                            bool32      bounded,
                            bool32      forwards,
                            bool32      pruned)
{
   uint8                lower[9], upper[9];
   uint8                routing_key[] = {'a'};
   const uint64         low           = bounded ? 1200 : 0;
   const uint64         high          = bounded ? total - 1200 : total;
   range_tenant_context tenant        = {.tenant = 'a'};
   splinterdb_iterator *it            = NULL;
   range_mixed_seed_seen              = 0;
   range_mixed_seed_track             = TRUE;
   ASSERT_EQUAL(
      0,
      splinterdb_iterator_init_with_bounds_and_filter(
         db,
         &it,
         greater_than,
         bounded ? range_mixed_seed_key(lower, low, 'a') : NULL_SLICE,
         less_than,
         bounded ? range_mixed_seed_key(upper, high, 'a') : NULL_SLICE,
         forwards ? greater_than_or_equal : less_than_or_equal,
         NULL_SLICE,
         range_tenant_matches,
         &tenant,
         pruned ? slice_create(sizeof(routing_key), routing_key) : NULL_SLICE));
   const uint32 initial_seed_mask = range_mixed_seed_seen;
   /* The iterator must retain its own routing key for later saved seeds.
    * a and b collide under seed 11, but differ under seed 22. */
   routing_key[0] = 'b';
   int64 expected =
      forwards ? (bounded ? low + 1 : 0) : (bounded ? high - 1 : total);
   uint64 count = 0;
   while (splinterdb_iterator_valid(it)) {
      while (expected >= 0 && expected < total && expected % 13 == 0) {
         expected += forwards ? 1 : -1;
      }
      ASSERT_TRUE(expected >= (bounded ? low + 1 : 0));
      ASSERT_TRUE(expected <= (bounded ? high - 1 : total));
      slice key, value;
      splinterdb_iterator_get_current(it, &key, &value);
      uint8 expected_key[9];
      slice expected_slice = range_mixed_seed_key(expected_key, expected, 'a');
      ASSERT_EQUAL(0, slice_lex_cmp(key, expected_slice));
      range_mixed_seed_value result;
      ASSERT_EQUAL_U(sizeof(result), slice_length(value));
      memcpy(&result, slice_data(value), sizeof(result));
      range_mixed_seed_value wanted = {
         .addend = expected == total   ? 7
                   : expected % 7 == 0 ? 123
                                       : 12,
      };
      ASSERT_EQUAL(0, memcmp(&wanted, &result, sizeof(result)));
      count++;
      expected += forwards ? 1 : -1;
      if (forwards) {
         splinterdb_iterator_next(it);
      } else {
         splinterdb_iterator_prev(it);
      }
   }
   ASSERT_EQUAL(0, splinterdb_iterator_status(it));
   uint64 wanted_count = 0;
   for (uint64 id = bounded ? low + 1 : 0; id <= (bounded ? high - 1 : total);
        id++)
   {
      wanted_count += id == total || id % 13 != 0;
   }
   ASSERT_EQUAL_U(wanted_count, count);
   splinterdb_iterator_deinit(it);
   range_mixed_seed_track = FALSE;
   if (pruned) {
      /* This proves actual persisted descriptors with both seeds were read.
       * Changing cfg->seed alone would not prove a mixed-seed fixture. */
      CTEST_LOG_INFO("mixed-seed scan bounded=%u forwards=%u: "
                     "initial seeds=%u final seeds=%u\n",
                     bounded,
                     forwards,
                     initial_seed_mask,
                     range_mixed_seed_seen);
      /* A different persisted seed must first be hashed after caller storage
       * was overwritten. The hash callback also checks the retained tenant. */
      ASSERT_EQUAL_U(forwards ? 1 : 2, initial_seed_mask);
      ASSERT_EQUAL_U(3, range_mixed_seed_seen);
   } else {
      ASSERT_EQUAL_U(0, range_mixed_seed_seen);
   }
}

CTEST2(splinterdb_quick, test_filtered_range_persisted_mixed_seeds)
{
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   data->default_data_cfg.super.key_hash           = range_mixed_seed_hash;
   data->default_data_cfg.super.merge_tuples       = range_mixed_seed_merge;
   data->default_data_cfg.super.merge_tuples_final = range_mixed_seed_finalize;
   data->cfg.memtable_capacity                     = 4 * MiB;
   data->cfg.disk_size                             = 512 * MiB;
   data->cfg.fanout                                = 2;
   data->cfg.num_normal_bg_threads                 = 0;
   data->cfg.num_memtable_bg_threads               = 0;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));
   const uint64           per_seed = 4096;
   const uint64           total    = 2 * per_seed;
   range_mixed_seed_value initial  = {.addend = 1};
   range_mixed_seed_value second   = {.multiplier = 10, .addend = 2};
   uint8                  key[9];
   range_mixed_seed_track          = FALSE;
   range_mixed_seed_reject_b_merge = FALSE;
   for (uint64 round = 0; round < 2; round++) {
      range_mixed_seed_construction_config(data->kvsb, round ? 22 : 11);
      for (uint64 id = round * per_seed; id < (round + 1) * per_seed; id++) {
         for (uint8 tenant = 'a'; tenant <= 'b'; tenant++) {
            ASSERT_EQUAL(
               0,
               splinterdb_insert(data->kvsb,
                                 range_mixed_seed_key(key, id, tenant),
                                 slice_create(sizeof(initial), &initial),
                                 NULL));
         }
         if (id % 1024 == 1023) {
            force_flush_current_memtable(data->kvsb);
         }
      }
      for (uint64 id = round * per_seed; id < (round + 1) * per_seed; id++) {
         for (uint8 tenant = 'a'; tenant <= 'b'; tenant++) {
            ASSERT_EQUAL(
               0,
               splinterdb_update(data->kvsb,
                                 range_mixed_seed_key(key, id, tenant),
                                 slice_create(sizeof(second), &second),
                                 NULL));
         }
      }
      force_flush_current_memtable(data->kvsb);
      ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
      ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->kvsb));
   }
   /* Neither persisted construction seed equals the current default. */
   range_mixed_seed_construction_config(data->kvsb, 99);
   core_handle *core = (core_handle *)splinterdb_get_trunk_handle(data->kvsb);
   uint64       height;
   ASSERT_TRUE(SUCCESS(trunk_get_height(&core->trunk_context, &height)));
   ASSERT_TRUE(height > 0);
   range_mixed_seed_value third = {.multiplier = 10, .addend = 3};
   for (uint64 id = 0; id < total; id++) {
      if (id % 7 == 0) {
         for (uint8 tenant = 'a'; tenant <= 'b'; tenant++) {
            ASSERT_EQUAL(
               0,
               splinterdb_update(data->kvsb,
                                 range_mixed_seed_key(key, id, tenant),
                                 slice_create(sizeof(third), &third),
                                 NULL));
         }
      }
      if (id % 13 == 0) {
         ASSERT_EQUAL(0,
                      splinterdb_delete(
                         data->kvsb, range_mixed_seed_key(key, id, 'a'), NULL));
      }
   }
   range_mixed_seed_value live = {.addend = 7};
   ASSERT_EQUAL(0,
                splinterdb_insert(data->kvsb,
                                  range_mixed_seed_key(key, total, 'a'),
                                  slice_create(sizeof(live), &live),
                                  NULL));
   range_mixed_seed_reject_b_merge = TRUE;
   for (bool32 bounded = FALSE; bounded <= TRUE; bounded++) {
      for (bool32 forwards = FALSE; forwards <= TRUE; forwards++) {
         for (bool32 pruned = FALSE; pruned <= TRUE; pruned++) {
            range_mixed_seed_check_scan(
               data->kvsb, total, bounded, forwards, pruned);
         }
      }
   }
   range_mixed_seed_reject_b_merge = FALSE;
}

typedef struct range_digit_context {
   uint8  modulus;
   uint8  remainder;
   uint64 calls;
} range_digit_context;

static bool
range_digit_matches(void *context, slice k)
{
   range_digit_context *digits = context;
   digits->calls++;
   return slice_length(k) == 1
          && (*(const uint8 *)slice_data(k) - '0') % digits->modulus
                == digits->remainder;
}

CTEST2(splinterdb_quick, test_filtered_range_predicate_only_and_bounds)
{
   range_digit_context  digits = {.modulus = 2, .remainder = 1};
   splinterdb_iterator *it     = NULL;
   /* No callback is needed on an empty database. */
   ASSERT_EQUAL(0,
                splinterdb_iterator_init_with_filter(data->kvsb,
                                                     &it,
                                                     greater_than_or_equal,
                                                     NULL_SLICE,
                                                     range_digit_matches,
                                                     &digits,
                                                     NULL_SLICE));
   ASSERT_FALSE(splinterdb_iterator_valid(it));
   ASSERT_EQUAL(0, splinterdb_iterator_status(it));
   ASSERT_EQUAL_U(0, digits.calls);
   splinterdb_iterator_deinit(it);

   for (char key = '0'; key <= '9'; key++) {
      ASSERT_EQUAL(
         0,
         splinterdb_insert(
            data->kvsb, slice_create(1, &key), slice_create(1, "v"), NULL));
      if (key == '4') {
         force_flush_current_memtable(data->kvsb);
      }
   }
   /* Odd numbers have unrelated ordinary hashes. Without a pruning key the
    * predicate must visit every branch, including the current memtable. */
   for (int forwards = 0; forwards < 2; forwards++) {
      ASSERT_EQUAL(0,
                   splinterdb_iterator_init_with_filter(
                      data->kvsb,
                      &it,
                      forwards ? greater_than_or_equal : less_than_or_equal,
                      NULL_SLICE,
                      range_digit_matches,
                      &digits,
                      NULL_SLICE));
      for (int i = 0; i < 5; i++) {
         char expected[] = {(forwards ? 1 + 2 * i : 9 - 2 * i) + '0', 0};
         check_current_string_kv(it, expected, "v");
         if (forwards) {
            splinterdb_iterator_next(it);
         } else {
            splinterdb_iterator_prev(it);
         }
      }
      ASSERT_FALSE(splinterdb_iterator_valid(it));
      ASSERT_EQUAL(0, splinterdb_iterator_status(it));
      splinterdb_iterator_deinit(it);

      ASSERT_EQUAL(0,
                   splinterdb_iterator_init_with_bounds_and_filter(
                      data->kvsb,
                      &it,
                      greater_than,
                      slice_create(1, "1"),
                      less_than,
                      slice_create(1, "9"),
                      forwards ? greater_than_or_equal : less_than_or_equal,
                      NULL_SLICE,
                      range_digit_matches,
                      &digits,
                      NULL_SLICE));
      for (int i = 0; i < 3; i++) {
         char expected[] = {(forwards ? 3 + 2 * i : 7 - 2 * i) + '0', 0};
         check_current_string_kv(it, expected, "v");
         if (forwards) {
            splinterdb_iterator_next(it);
         } else {
            splinterdb_iterator_prev(it);
         }
      }
      ASSERT_FALSE(splinterdb_iterator_valid(it));
      if (forwards) {
         splinterdb_iterator_prev(it);
         check_current_string_kv(it, "7", "v");
         splinterdb_iterator_prev(it);
      } else {
         splinterdb_iterator_next(it);
         check_current_string_kv(it, "3", "v");
         splinterdb_iterator_next(it);
      }
      check_current_string_kv(it, "5", "v");
      ASSERT_EQUAL(0, splinterdb_iterator_status(it));
      splinterdb_iterator_deinit(it);

      ASSERT_EQUAL(0,
                   splinterdb_iterator_init_with_filter(data->kvsb,
                                                        &it,
                                                        forwards ? greater_than
                                                                 : less_than,
                                                        slice_create(1, "5"),
                                                        range_digit_matches,
                                                        &digits,
                                                        NULL_SLICE));
      check_current_string_kv(it, forwards ? "7" : "3", "v");
      ASSERT_EQUAL(0, splinterdb_iterator_status(it));
      splinterdb_iterator_deinit(it);
   }
   ASSERT_TRUE(digits.calls > 0);

   /* Context remains caller-owned; each iterator borrows its current value. */
   digits.remainder = 2;
   ASSERT_EQUAL(0,
                splinterdb_iterator_init_with_filter(data->kvsb,
                                                     &it,
                                                     greater_than_or_equal,
                                                     NULL_SLICE,
                                                     range_digit_matches,
                                                     &digits,
                                                     NULL_SLICE));
   ASSERT_FALSE(splinterdb_iterator_valid(it));
   ASSERT_EQUAL(0, splinterdb_iterator_status(it));
   splinterdb_iterator_deinit(it);

   it = NULL;
   ASSERT_EQUAL(EINVAL,
                splinterdb_iterator_init_with_filter(data->kvsb,
                                                     &it,
                                                     greater_than_or_equal,
                                                     NULL_SLICE,
                                                     NULL,
                                                     NULL,
                                                     slice_create(1, "1")));
   ASSERT_NULL(it);
   ASSERT_EQUAL(
      EINVAL,
      splinterdb_iterator_init_with_bounds_and_filter(data->kvsb,
                                                      &it,
                                                      greater_than_or_equal,
                                                      NULL_SLICE,
                                                      less_than_or_equal,
                                                      NULL_SLICE,
                                                      greater_than_or_equal,
                                                      NULL_SLICE,
                                                      NULL,
                                                      NULL,
                                                      slice_create(1, "1")));
   ASSERT_NULL(it);
}

static int
range_fail_finalize(const data_config *cfg, slice k, merge_accumulator *msg)
{
   return ENOMEM;
}

CTEST2(splinterdb_quick, test_filtered_range_propagates_matching_merge_errors)
{
   data->default_data_cfg.super.merge_tuples       = hash_range_add;
   data->default_data_cfg.super.merge_tuples_final = range_fail_finalize;
   range_tenant_context tenant                     = {.tenant = 'a'};
   ASSERT_EQUAL(
      0,
      splinterdb_update(
         data->kvsb, slice_create(2, "0b"), slice_create(1, "v"), NULL));
   insert_string_kv(data->kvsb, "1a", "v");
   ASSERT_EQUAL(
      0,
      splinterdb_update(
         data->kvsb, slice_create(2, "2a"), slice_create(1, "v"), NULL));
   splinterdb_iterator *it = NULL;
   ASSERT_EQUAL(0,
                splinterdb_iterator_init_with_filter(data->kvsb,
                                                     &it,
                                                     greater_than_or_equal,
                                                     NULL_SLICE,
                                                     range_tenant_matches,
                                                     &tenant,
                                                     NULL_SLICE));
   check_current_string_kv(it, "1a", "v");
   splinterdb_iterator_next(it);
   ASSERT_FALSE(splinterdb_iterator_valid(it));
   ASSERT_EQUAL(ENOMEM, splinterdb_iterator_status(it));
   splinterdb_iterator_deinit(it);

   it = NULL;
   ASSERT_EQUAL(ENOMEM,
                splinterdb_iterator_init_with_filter(data->kvsb,
                                                     &it,
                                                     less_than_or_equal,
                                                     NULL_SLICE,
                                                     range_tenant_matches,
                                                     &tenant,
                                                     NULL_SLICE));
   ASSERT_NULL(it);
   /* Restore a successful finalizer before the fixture closes the database. */
   data->default_data_cfg.super.merge_tuples_final = hash_range_finalize;
}

typedef struct filtered_iterator_handoff {
   splinterdb          *db;
   splinterdb_iterator *itor;
   range_tenant_context tenant;
   int                  rc;
} filtered_iterator_handoff;

static void *
filtered_iterator_handoff_create(void *context)
{
   filtered_iterator_handoff *handoff       = context;
   char                       pruning_key[] = "a";
   handoff->rc =
      splinterdb_iterator_init_with_filter(handoff->db,
                                           &handoff->itor,
                                           greater_than_or_equal,
                                           NULL_SLICE,
                                           range_tenant_matches,
                                           &handoff->tenant,
                                           slice_create(1, pruning_key));
   return NULL;
}

static void *
filtered_iterator_handoff_consume(void *context)
{
   filtered_iterator_handoff *handoff = context;
   uint64                     calls   = handoff->tenant.calls;
   for (char digit = '0'; digit <= '9'; digit++) {
      char expected[] = {digit, 'a', 0};
      check_current_string_kv(handoff->itor, expected, "v");
      splinterdb_iterator_next(handoff->itor);
   }
   ASSERT_FALSE(splinterdb_iterator_valid(handoff->itor));
   splinterdb_iterator_prev(handoff->itor);
   check_current_string_kv(handoff->itor, "9a", "v");
   splinterdb_iterator_prev(handoff->itor);
   check_current_string_kv(handoff->itor, "8a", "v");
   ASSERT_TRUE(handoff->tenant.calls > calls);
   ASSERT_EQUAL_U(platform_get_tid(), handoff->tenant.last_tid);
   handoff->rc = splinterdb_iterator_status(handoff->itor);
   splinterdb_iterator_deinit(handoff->itor);
   handoff->itor = NULL;
   return NULL;
}

CTEST2(splinterdb_quick, test_filtered_range_context_survives_thread_handoff)
{
   data->default_data_cfg.super.key_hash = hash_range_key_hash;
   for (char digit = '0'; digit <= '9'; digit++) {
      char key[] = {digit, 'a', 0};
      insert_string_kv(data->kvsb, key, "v");
      key[1] = 'b';
      insert_string_kv(data->kvsb, key, "v");
   }
   for (unsigned persisted = 0; persisted < 2; persisted++) {
      if (persisted) {
         ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
         ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->kvsb));
      }
      filtered_iterator_handoff handoff = {
         .db     = data->kvsb,
         .tenant = {.tenant = 'a'},
      };
      pthread_t producer;
      ASSERT_EQUAL(
         0,
         pthread_create(
            &producer, NULL, filtered_iterator_handoff_create, &handoff));
      ASSERT_EQUAL(0, pthread_join(producer, NULL));
      ASSERT_EQUAL(0, handoff.rc);
      ASSERT_TRUE(handoff.tenant.calls > 0);
      ASSERT_NOT_EQUAL(platform_get_tid(), handoff.tenant.last_tid);
      filtered_iterator_handoff_consume(&handoff);
      ASSERT_EQUAL(0, handoff.rc);

      filtered_iterator_handoff_create(&handoff);
      ASSERT_EQUAL(0, handoff.rc);
      pthread_t receiver;
      ASSERT_EQUAL(
         0,
         pthread_create(
            &receiver, NULL, filtered_iterator_handoff_consume, &handoff));
      ASSERT_EQUAL(0, pthread_join(receiver, NULL));
      ASSERT_EQUAL(0, handoff.rc);
      ASSERT_NOT_EQUAL(platform_get_tid(), handoff.tenant.last_tid);
   }
}

typedef struct iterator_handoff {
   splinterdb          *db;
   splinterdb_iterator *itor;
   int                  rc;
   threadid             tid;
} iterator_handoff;

static void *
iterator_handoff_create_and_exit(void *arg)
{
   iterator_handoff *handoff = arg;
   handoff->rc               = splinterdb_iterator_init(
      handoff->db, &handoff->itor, greater_than_or_equal, NULL_SLICE);
   handoff->tid = platform_get_tid();
   return NULL;
}

static void *
iterator_handoff_advance_and_deinit(void *arg)
{
   iterator_handoff *handoff = arg;
   /* next/get_current/deinit perform the normal public auto-registration. */
   for (int i = 0; i < 500; i++) {
      if (check_current_tuple(handoff->itor, i) != 0) {
         handoff->rc = -1;
         break;
      }
      splinterdb_iterator_next(handoff->itor);
   }
   if (handoff->rc == 0) {
      for (int i = 499; i >= 490; i--) {
         splinterdb_iterator_prev(handoff->itor);
         if (check_current_tuple(handoff->itor, i) != 0) {
            handoff->rc = -1;
            break;
         }
      }
   }
   if (handoff->rc == 0) {
      handoff->rc = splinterdb_iterator_status(handoff->itor);
   }
   handoff->tid = platform_get_tid();
   splinterdb_iterator_deinit(handoff->itor);
   handoff->itor = NULL;
   return NULL;
}

CTEST2(splinterdb_quick, test_iterator_thread_handoff)
{
   ASSERT_EQUAL(0, insert_some_keys(2000, data->kvsb));
   /* Exercise live memtable leaves, then persisted branch leaves. */
   for (unsigned persisted = 0; persisted < 2; persisted++) {
      if (persisted) {
         ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
         ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->kvsb));
      }
      iterator_handoff handoff = {.db = data->kvsb};
      ASSERT_EQUAL(
         0,
         splinterdb_iterator_init(
            data->kvsb, &handoff.itor, greater_than_or_equal, NULL_SLICE));
      pthread_t receiver;
      ASSERT_EQUAL(
         0,
         pthread_create(
            &receiver, NULL, iterator_handoff_advance_and_deinit, &handoff));
      ASSERT_EQUAL(0, pthread_join(receiver, NULL));
      ASSERT_EQUAL(0, handoff.rc);
      ASSERT_NOT_EQUAL(platform_get_tid(), handoff.tid);

      /* Reverse direction: the producer exits and its registration is released
       * before the main thread advances/deinitializes the iterator. */
      pthread_t producer;
      ASSERT_EQUAL(
         0,
         pthread_create(
            &producer, NULL, iterator_handoff_create_and_exit, &handoff));
      ASSERT_EQUAL(0, pthread_join(producer, NULL));
      ASSERT_EQUAL(0, handoff.rc);
      ASSERT_NOT_EQUAL(platform_get_tid(), handoff.tid);
      iterator_handoff_advance_and_deinit(&handoff);
      ASSERT_EQUAL(0, handoff.rc);
   }
}

/* The child exits without closing the database. In the damaged variant, a
 * later group's second pointer page is invalid, after hundreds of data extents
 * have already been discovered. Recovery must keep the preceding group and
 * reclaim those conservative reservations in its root-only rebuild. */
CTEST2(splinterdb_quick, test_blob_tree_crash_recovery)
{
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   data->cfg.use_log                   = TRUE;
   data->cfg.disk_size                 = 384 * Mega;
   data->cfg.memtable_capacity         = 128 * Mega;
   data->cfg.checkpoint_log_size_bytes = UINT64_MAX;
   data->cfg.num_normal_bg_threads     = 0;
   data->cfg.num_memtable_bg_threads   = 0;
   const uint64 length                 = 514 * IO_DEFAULT_EXTENT_SIZE + 17;
   slice        prefix                 = slice_create(6, "prefix");
   slice        large                  = slice_create(5, "large");
   slice        suffix                 = slice_create(6, "suffix");
   slice        small                  = slice_create(2, "ok");
   const char  *child_mode = getenv("SPLINTER_TEST_BLOB_CRASH_CHILD");
   if (child_mode != NULL) {
      bool32      corrupt = child_mode[0] == '1';
      splinterdb *db;
      if (splinterdb_create(&data->cfg, &db) != 0)
         _exit(10);
      if (splinterdb_insert(db, prefix, small, NULL) != 0)
         _exit(11);
      if (splinterdb_durable_barrier(db, NULL) != 0)
         _exit(12);
      char *bytes = malloc(length);
      if (bytes == NULL)
         _exit(13);
      memset(bytes, 'L', length);
      if (splinterdb_insert(db, large, slice_create(length, bytes), NULL) != 0)
         _exit(14);
      if (splinterdb_durable_barrier(db, NULL) != 0)
         _exit(15);
      if (splinterdb_insert(db, suffix, small, NULL) != 0)
         _exit(16);
      if (splinterdb_durable_barrier(db, NULL) != 0)
         _exit(17);
      if (corrupt) {
         const core_handle *core = splinterdb_get_trunk_handle(db);
         cache             *cc   = (cache *)splinterdb_get_cache_handle(db);
         log_iterator      *iter;
         platform_status    rc =
            shard_log_iterator_create(cc,
                                      (shard_log_config *)core->cfg.log_cfg,
                                      data->cfg.heap_id,
                                      log_get_head(core->log),
                                      0,
                                      &iter);
         if (!SUCCESS(rc))
            _exit(18);
         uint64 pointer_page = 0;
         while (log_iterator_can_next(iter)) {
            key     k;
            message msg;
            log_iterator_curr(iter, &k, &msg);
            if (message_is_blob(msg)) {
               parsed_blob parsed;
               if (!SUCCESS(
                      blob_parse_descriptor(cc, message_slice(msg), &parsed))
                   || parsed.height != 2)
                  _exit(19);
               pointer_page = parsed.base->addrs[0] + cache_page_size(cc);
               break;
            }
            if (!SUCCESS(log_iterator_next(iter)))
               _exit(20);
         }
         log_iterator_deinit(iter);
         if (pointer_page == 0)
            _exit(21);
         page_handle page;
         if (!SUCCESS(cache_get(cc, pointer_page, TRUE, PAGE_TYPE_BLOB, &page)))
            _exit(27);
         if (!cache_try_claim(cc, &page))
            _exit(22);
         cache_lock(cc, &page);
         uint64 invalid = data->cfg.disk_size;
         memcpy(page_handle_data(&page), &invalid, sizeof(invalid));
         cache_unlock(cc, &page);
         cache_unclaim(cc, &page);
         writeback_set set;
         writeback_set_init(&set, cc, data->cfg.heap_id);
         if (!SUCCESS(writeback_set_add_page(&set, &page, PAGE_TYPE_BLOB)))
            _exit(23);
         cache_unget(cc, &page);
         if (!SUCCESS(writeback_set_wait(&set)))
            _exit(24);
         if (!SUCCESS(writeback_set_make_durable(&set)))
            _exit(25);
         writeback_set_deinit(&set);
      }
      _exit(0);
   }
   for (uint64 corrupt = 0; corrupt < 2; corrupt++) {
      pid_t pid = fork();
      ASSERT_TRUE(pid >= 0);
      if (pid == 0) {
         setenv("SPLINTER_TEST_BLOB_CRASH_CHILD", corrupt ? "1" : "0", 1);
         execl("/proc/self/exe",
               "splinterdb_quick_test",
               "test_blob_tree_crash_recovery",
               (char *)NULL);
         _exit(26);
      }
      int status;
      ASSERT_EQUAL(pid, waitpid(pid, &status, 0));
      ASSERT_TRUE(WIFEXITED(status));
      ASSERT_EQUAL(0, WEXITSTATUS(status));
      for (uint64 reopen = 0; reopen < 2; reopen++) {
         ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->kvsb));
         splinterdb_lookup_result result;
         splinterdb_lookup_result_init(
            data->kvsb, &result, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
         ASSERT_EQUAL(0, splinterdb_lookup(data->kvsb, prefix, &result));
         assert_lookup_result_matches_slice(&result, small);
         ASSERT_EQUAL(0, splinterdb_lookup(data->kvsb, large, &result));
         ASSERT_EQUAL(!corrupt, splinterdb_lookup_found(&result));
         if (!corrupt) {
            slice actual;
            ASSERT_EQUAL(0, splinterdb_lookup_result_value(&result, &actual));
            ASSERT_EQUAL(length, slice_length(actual));
            const char *bytes = slice_data(actual);
            for (uint64 i = 0; i < length; i++) {
               if (bytes[i] != 'L') {
                  ASSERT_TRUE(FALSE);
                  break;
               }
            }
         }
         ASSERT_EQUAL(0, splinterdb_lookup(data->kvsb, suffix, &result));
         ASSERT_EQUAL(!corrupt, splinterdb_lookup_found(&result));
         splinterdb_lookup_result_deinit(&result);
         if (corrupt) {
            allocator *al =
               (allocator *)splinterdb_get_allocator_handle(data->kvsb);
            ASSERT_TRUE(allocator_in_use(al) < length / 2);
         }
         ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
      }
   }
   /* Keep the fixture's usual close path valid. */
   ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->kvsb));
}
