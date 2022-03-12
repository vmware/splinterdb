// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinter_test.c --
 *
 *  Exercises the basic SplinterDB interfaces.
 *
 * NOTE: There is some duplication of the splinter_do_inserts() in the test
 * cases which adds considerable execution times. The test_inserts() test case
 * will run with the default test configuration, which is sufficiently large
 * enough to trigger a compaction. The expectation is that this unit test case
 * will be invoked on its own, with a reduced memtable capacity to invoke the
 * lookups test case(s):
 *
 * $ bin/unit/splinter_test test_inserts
 * $ bin/unit/splinter_test --memtable-capacity-mib 4 test_lookups
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/platform_public.h"
#include "trunk.h"
#include "clockcache.h"
#include "allocator.h"
#include "platform.h"
#include "task.h"
#include "functional/test.h"
#include "functional/test_async.h"
#include "test_common.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

typedef struct shadow_entry {
   uint64 key_offset;
   uint64 key_length;
   uint64 value_length;
} shadow_entry;

typedef struct trunk_shadow {
   bool            sorted;
   writable_buffer entries;
   writable_buffer data;
} trunk_shadow;

/* Function prototypes */
static uint64
splinter_do_inserts(void         *datap,
                    trunk_handle *spl,
                    bool          verify,
                    trunk_shadow *shadow); // Out

static platform_status
test_lookup_by_range(void         *datap,
                     trunk_handle *spl,
                     uint64        num_inserts,
                     trunk_shadow *shadow,
                     uint64        num_ranges);

// Verify consistency of data after so-many inserts
#define TEST_VERIFY_GRANULARITY 100000

/* Macro to show progress message as workload is running */
#define SHOW_PCT_PROGRESS(op_num, num_ops, msg)                                \
   do {                                                                        \
      if (((op_num) % ((num_ops) / 100)) == 0) {                               \
         platform_default_log(PLATFORM_CR msg, (op_num) / ((num_ops) / 100));  \
      }                                                                        \
   } while (0)

/*
 * Global data declaration macro:
 */
CTEST_DATA(splinter)
{
   // Declare head handles for io, allocator, cache and splinter allocation.
   platform_heap_handle hh;
   platform_heap_id     hid;

   // Thread-related config parameters. These don't change for unit tests
   uint32 num_insert_threads;
   uint32 num_lookup_threads;
   uint32 max_async_inflight;
   int    spl_num_tables;

   uint8 num_bg_threads[NUM_TASK_TYPES];

   // Config structs required, as per splinter_test() setup work.
   io_config           io_cfg;
   rc_allocator_config al_cfg;
   shard_log_config    log_cfg;

   rc_allocator al;

   // Following get setup pointing to allocated memory
   trunk_config          *splinter_cfg;
   data_config           *data_cfg;
   clockcache_config     *cache_cfg;
   platform_io_handle    *io;
   clockcache            *clock_cache;
   task_system           *tasks;
   test_message_generator gen;

   // Test execution related configuration
   test_exec_config test_exec_cfg;
};

/*
 * Setup Splinter configuration: Extracted from functional/splinter_test.c:
 * splinter_test().
 */
// clang-format off
CTEST_SETUP(splinter)
{
   Platform_stdout_fh = fopen("/tmp/unit_test.stdout", "a+");
   Platform_stderr_fh = fopen("/tmp/unit_test.stderr", "a+");

   // Defaults: For basic unit-tests, use single threads
   data->num_insert_threads = 1;
   data->num_lookup_threads = 1;
   data->max_async_inflight = 64;
   data->spl_num_tables = 1;

   bool cache_per_table = FALSE;
   int num_tables       = data->spl_num_tables; // Cache, for re-use below
   uint8 num_caches     = (cache_per_table ? num_tables : 1);
   uint64 heap_capacity = MAX(1024 * MiB * num_caches, 512 * MiB * num_tables);
   heap_capacity        = MIN(heap_capacity, UINT32_MAX);
   heap_capacity        = MAX(heap_capacity, 2 * GiB);

   // Create a heap for io, allocator, cache and splinter
   platform_status rc = platform_heap_create(platform_get_module_id(),
                                             heap_capacity,
                                             &data->hh,
                                             &data->hid);
   platform_assert_status_ok(rc);

   // Allocate memory for global config structures
   data->splinter_cfg = TYPED_ARRAY_MALLOC(data->hid, data->splinter_cfg,
                                          num_tables);

   data->data_cfg = TYPED_ARRAY_MALLOC(data->hid, data->data_cfg,
                                       num_tables);

   data->cache_cfg = TYPED_ARRAY_MALLOC(data->hid, data->cache_cfg,
                                        num_tables);

   ZERO_STRUCT(data->test_exec_cfg);

   rc = test_parse_args_n(data->splinter_cfg,
                          &data->data_cfg,
                          &data->io_cfg,
                          &data->al_cfg,
                          data->cache_cfg,
                          &data->log_cfg,
                          &data->test_exec_cfg,
                          &data->gen,
                          num_tables,
                          Ctest_argc,   // argc/argv globals setup by CTests
                          (char **)Ctest_argv);
   platform_assert_status_ok(rc);

   // Establish Max active threads
   uint32 total_threads = data->num_lookup_threads;
   if (total_threads < data->num_insert_threads) {
      total_threads = data->num_insert_threads;
   }

   // Check if IO subsystem has enough reqs for max async IOs inflight
   io_config * io_cfgp = &data->io_cfg;
   if (io_cfgp->async_queue_size < total_threads * data->max_async_inflight) {
      io_cfgp->async_queue_size = ROUNDUP(total_threads * data->max_async_inflight, 32);
      platform_log("Bumped up IO queue size to %lu\n", io_cfgp->async_queue_size);
   }
   if (io_cfgp->kernel_queue_size < total_threads * data->max_async_inflight) {
      io_cfgp->kernel_queue_size =
         ROUNDUP(total_threads * data->max_async_inflight, 32);
      platform_log("Bumped up IO queue size to %lu\n",
                   io_cfgp->kernel_queue_size);
   }

   // Allocate and initialize the IO sub-system.
   data->io = TYPED_MALLOC(data->hid, data->io);
   ASSERT_TRUE((data->io != NULL));
   rc = io_handle_init(data->io, &data->io_cfg, data->hh, data->hid);

   // no bg threads by default.
   for (int idx = 0; idx < NUM_TASK_TYPES; idx++) {
       data->num_bg_threads[idx] = 0;
   }

   bool use_bg_threads = data->num_bg_threads[TASK_TYPE_NORMAL] != 0;

   rc = test_init_task_system(data->hid, data->io, &data->tasks, data->splinter_cfg->use_stats,
                           use_bg_threads, data->num_bg_threads);
   ASSERT_TRUE(SUCCESS(rc),
              "Failed to init splinter state: %s\n",
              platform_status_to_string(rc));

   rc_allocator_init(&data->al, &data->al_cfg, (io_handle *)data->io, data->hh, data->hid,
                     platform_get_module_id());

   data->clock_cache = TYPED_ARRAY_MALLOC(data->hid, data->clock_cache, num_caches);
   ASSERT_TRUE((data->clock_cache != NULL));

   for (uint8 idx = 0; idx < num_caches; idx++) {
      rc = clockcache_init(&data->clock_cache[idx],
                           &data->cache_cfg[idx],
                           (io_handle *)data->io,
                           (allocator *)&data->al,
                           "test",
                           data->tasks,
                           data->hh,
                           data->hid,
                           platform_get_module_id());

      ASSERT_TRUE(SUCCESS(rc), "clockcache_init() failed for index=%d. ", idx);
   }

}

// clang-format on

/*
 * Tear down memory allocated for various sub-systems. Shutdown Splinter.
 */
CTEST_TEARDOWN(splinter)
{
   clockcache_deinit(data->clock_cache);
   platform_free(data->hid, data->clock_cache);

   allocator *alp = (allocator *)&data->al;
   allocator_assert_noleaks(alp);

   rc_allocator_deinit(&data->al);
   test_deinit_task_system(data->hid, data->tasks);

   io_handle_deinit(data->io);
   platform_free(data->hid, data->io);

   if (data->cache_cfg) {
      platform_free(data->hid, data->cache_cfg);
   }

   if (data->splinter_cfg) {
      platform_free(data->hid, data->splinter_cfg);
   }

   platform_heap_destroy(&data->hh);
}

/*
 * **************************************************************************
 * Basic test case to verify trunk_insert() API and validate a very large #
 * of inserts. This test case is designed to insert enough rows to trigger
 * compaction. (We don't, quite, actually verify that compaction has occurred
 * but based on the default test configs, we expect that it would trigger.)
 * **************************************************************************
 */
CTEST2(splinter, test_inserts)
{
   allocator *alp = (allocator *)&data->al;

   trunk_handle *spl = trunk_create(data->splinter_cfg,
                                    alp,
                                    (cache *)data->clock_cache,
                                    data->tasks,
                                    test_generate_allocator_root_id(),
                                    data->hid);
   ASSERT_TRUE(spl != NULL);

   // TRUE : Also do verification-after-inserts
   uint64 num_inserts = splinter_do_inserts(data, spl, TRUE, NULL);
   ASSERT_NOT_EQUAL(0,
                    num_inserts,
                    "Expected to have inserted non-zero rows, num_inserts=%lu.",
                    num_inserts);

   trunk_destroy(spl);
}

static void
trunk_shadow_init(trunk_shadow *shadow)
{
   shadow->sorted = TRUE;
   writable_buffer_init(&shadow->entries, NULL);
   writable_buffer_init(&shadow->data, NULL);
}

static void
trunk_shadow_deinit(trunk_shadow *shadow)
{
   writable_buffer_deinit(&shadow->entries);
   writable_buffer_deinit(&shadow->data);
}

static void
trunk_shadow_reinit(trunk_shadow *shadow)
{
   shadow->sorted = TRUE;
   writable_buffer_set_to_null(&shadow->entries);
   writable_buffer_set_to_null(&shadow->data);
}

/*
 * Copy the newly inserted key/value row to a shadow buffer. This set of
 * rows will be used later during lookup-validation using range searches.
 */
static void
trunk_shadow_append(trunk_shadow *shadow, slice key, slice value)
{
   uint64 key_offset =
      writable_buffer_append(&shadow->data, slice_length(key), slice_data(key));
   writable_buffer_append(
      &shadow->data, slice_length(value), slice_data(value));

   shadow_entry new_entry = {.key_offset   = key_offset,
                             .key_length   = slice_length(key),
                             .value_length = slice_length(value)};
   writable_buffer_append(&shadow->entries, sizeof(new_entry), &new_entry);
   shadow->sorted = FALSE;
}

static slice
shadow_entry_key(const shadow_entry *entry, char *data)
{
   return slice_create(entry->key_length, data + entry->key_offset);
}

static slice
shadow_entry_value(const shadow_entry *entry, char *data)
{
   return slice_create(entry->value_length,
                       data + entry->key_offset + entry->key_length);
}

static int
compare_shadow_entries(const void *a, const void *b, void *arg)
{
   slice akey = shadow_entry_key(a, arg);
   slice bkey = shadow_entry_key(b, arg);
   return slice_lex_cmp(akey, bkey);
}

static uint64
trunk_shadow_length(trunk_shadow *shadow)
{
   return writable_buffer_length(&shadow->entries) / sizeof(shadow_entry);
}

static void
trunk_shadow_sort(trunk_shadow *shadow)
{
   shadow_entry *entries  = writable_buffer_data(&shadow->entries);
   uint64        nentries = trunk_shadow_length(shadow);
   shadow_entry  temp;
   char         *data = writable_buffer_data(&shadow->data);

   platform_sort_slow(
      entries, nentries, sizeof(*entries), compare_shadow_entries, data, &temp);
   shadow->sorted = TRUE;
}

static void
trunk_shadow_get(trunk_shadow *shadow, uint64 i, slice *key, slice *value)
{

   if (!shadow->sorted) {
      trunk_shadow_sort(shadow);
   }

   shadow_entry     *entries  = writable_buffer_data(&shadow->entries);
   debug_only uint64 nentries = trunk_shadow_length(shadow);
   debug_assert(i < nentries);
   shadow_entry *entry = &entries[i];

   char *data = writable_buffer_data(&shadow->data);
   *key       = shadow_entry_key(entry, data);
   *value     = shadow_entry_value(entry, data);
}

static uint64
test_splinter_bsearch(trunk_shadow *shadow, slice key)
{
   uint64 lo = 0;
   uint64 hi = trunk_shadow_length(shadow);
   while (lo < hi) {
      // invariant: forall i | i < lo  :: s[i] < key
      // invariant: forall i | hi <= i :: key <= s[i]
      slice  ckey;
      slice  cvalue;
      uint64 mid = (lo + hi) / 2;
      trunk_shadow_get(shadow, mid, &ckey, &cvalue);
      int cmp = slice_lex_cmp(key, ckey);
      if (cmp <= 0) {
         // key <= s[mid]
         hi = mid;
      } else {
         // s[mid] < key
         lo = mid + 1;
      }
   }

   return lo;
}

/*
 * **************************************************************************
 * Test case to run a bunch of inserts into Splinter, and then perform
 * different types of lookup-verification. As all lookups need an inserts
 * step, this test case is really a set of multiple sub-test-cases for
 * inserts, synchronous and async lookups, and lookups-by-range rolled into
 * one.
 * **************************************************************************
 */
CTEST2(splinter, test_lookups)
{
   allocator *alp = (allocator *)&data->al;

   trunk_handle *spl = trunk_create(data->splinter_cfg,
                                    alp,
                                    (cache *)data->clock_cache,
                                    data->tasks,
                                    test_generate_allocator_root_id(),
                                    data->hid);
   ASSERT_TRUE(spl != NULL);

   trunk_shadow shadow;
   trunk_shadow_init(&shadow);

   // FALSE : No need to do verification-after-inserts, as that functionality
   // has been tested earlier in test_inserts() case.
   uint64 num_inserts = splinter_do_inserts(data, spl, FALSE, &shadow);
   ASSERT_NOT_EQUAL(0,
                    num_inserts,
                    "Expected to have inserted non-zero rows, num_inserts=%lu.",
                    num_inserts);

   writable_buffer qdata;
   writable_buffer_init(&qdata, NULL);
   char         key[MAX_KEY_SIZE];
   const size_t key_size = trunk_key_size(spl);

   platform_status rc;

   // **************************************************************************
   // Test sub-case 1: Validate using synchronous trunk_lookup().
   //   Verify that all the keys inserted are found via lookup.
   // **************************************************************************
   uint64 start_time = platform_get_timestamp();

   platform_default_log("\n");
   for (uint64 insert_num = 0; insert_num < num_inserts; insert_num++) {

      // Show progress message in %age-completed to stdout
      SHOW_PCT_PROGRESS(
         insert_num, num_inserts, "Verify positive lookups %3lu%% complete");

      test_key(key, TEST_RANDOM, insert_num, 0, 0, key_size, 0);
      writable_buffer_set_to_null(&qdata);

      rc = trunk_lookup(spl, key, &qdata);
      ASSERT_TRUE(SUCCESS(rc),
                  "trunk_lookup() FAILURE, insert_num=%lu: %s\n",
                  insert_num,
                  platform_status_to_string(rc));

      verify_tuple(spl,
                   &data->gen,
                   insert_num,
                   key,
                   writable_buffer_to_slice(&qdata),
                   TRUE);
   }

   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
   platform_default_log(
      " ... splinter positive lookup time %lu s, per tuple %lu ns\n",
      NSEC_TO_SEC(elapsed_ns),
      (elapsed_ns / num_inserts));

   // **************************************************************************
   // Test sub-case 2: Validate using synchronous trunk_lookup() that we
   //   do not find any keys outside the range of keys inserted.
   // **************************************************************************

   start_time = platform_get_timestamp();

   for (uint64 insert_num = num_inserts; insert_num < 2 * num_inserts;
        insert_num++)
   {
      // Show progress message in %age-completed to stdout
      SHOW_PCT_PROGRESS((insert_num - num_inserts),
                        num_inserts,
                        "Verify negative lookups %3lu%% complete");

      test_key(key, TEST_RANDOM, insert_num, 0, 0, key_size, 0);

      rc = trunk_lookup(spl, key, &qdata);
      ASSERT_TRUE(SUCCESS(rc),
                  "trunk_lookup() FAILURE, insert_num=%lu: %s\n",
                  insert_num,
                  platform_status_to_string(rc));

      verify_tuple(spl,
                   &data->gen,
                   insert_num,
                   key,
                   writable_buffer_to_slice(&qdata),
                   FALSE);
   }

   elapsed_ns = platform_timestamp_elapsed(start_time);
   platform_default_log(
      " ... splinter negative lookup time %lu s, per tuple %lu ns\n",
      NSEC_TO_SEC(elapsed_ns),
      (elapsed_ns / num_inserts));

   // **************************************************************************
   // Test sub-case 3: Validate using binary searches across ranges for the
   //   keys inside the range of keys inserted.
   // **************************************************************************

   int niters = 3;
   platform_default_log(
      "Perform test_lookup_by_range() for %d iterations ...\n", niters);
   // Iterate thru small set of num_ranges for additional coverage.
   trunk_shadow_sort(&shadow);
   for (int ictr = 1; ictr <= 3; ictr++) {

      uint64 num_ranges = (num_inserts / 128) * ictr;

      // Range search uses the shadow-copy of the rows previously inserted while
      // doing a binary-search.
      rc = test_lookup_by_range(
         (void *)data, spl, num_inserts, &shadow, num_ranges);
      ASSERT_TRUE(SUCCESS(rc),
                  "test_lookup_by_range() FAILURE, num_ranges=%d: %s\n",
                  num_ranges,
                  platform_status_to_string(rc));
   }

   /*
   ** **********************************************
   ** **** Start of Async lookup sub-test-cases ****
   ** **********************************************
   */
   // Setup Async-context sub-system for async lookups.
   test_async_lookup *async_lookup;
   async_ctxt_init(data->hid, data->max_async_inflight, &async_lookup);

   test_async_ctxt *ctxt = NULL;

   // **************************************************************************
   // Test sub-case 4: Validate using asynchronous trunk_lookup().
   //   Verify that all the keys inserted are found via lookup.
   // **************************************************************************

   // Declare an expected data tuple that will be found.
   verify_tuple_arg vtarg_true = {.expected_found = TRUE};

   start_time = platform_get_timestamp();
   for (uint64 insert_num = 0; insert_num < num_inserts; insert_num++) {

      // Show progress message in %age-completed to stdout
      SHOW_PCT_PROGRESS(insert_num,
                        num_inserts,
                        "Verify async positive lookups %3lu%% complete");

      ctxt = test_async_ctxt_get(spl, async_lookup, &vtarg_true);

      test_key(ctxt->key, TEST_RANDOM, insert_num, 0, 0, key_size, 0);
      ctxt->lookup_num = insert_num;
      async_ctxt_process_one(
         spl, async_lookup, ctxt, NULL, verify_tuple_callback, &vtarg_true);
   }
   test_wait_for_inflight(spl, async_lookup, &vtarg_true);

   elapsed_ns = platform_timestamp_elapsed(start_time);
   platform_default_log(
      " ... splinter positive async lookup time %lu s, per tuple %lu ns\n",
      NSEC_TO_SEC(elapsed_ns),
      (elapsed_ns / num_inserts));

   // **************************************************************************
   // Test sub-case 5: Validate using asynchronous trunk_lookup() that we
   //   do not find any keys outside the range of keys inserted.
   // **************************************************************************

   // Declare a tuple that data will not be found.
   verify_tuple_arg vtarg_false = {.expected_found = FALSE};

   start_time = platform_get_timestamp();
   for (uint64 insert_num = num_inserts; insert_num < 2 * num_inserts;
        insert_num++)
   {
      // Show progress message in %age-completed to stdout
      SHOW_PCT_PROGRESS((insert_num - num_inserts),
                        num_inserts,
                        "Verify async negative lookups %3lu%% complete");

      ctxt = test_async_ctxt_get(spl, async_lookup, &vtarg_false);
      test_key(ctxt->key, TEST_RANDOM, insert_num, 0, 0, key_size, 0);
      ctxt->lookup_num = insert_num;
      async_ctxt_process_one(
         spl, async_lookup, ctxt, NULL, verify_tuple_callback, &vtarg_false);
   }
   test_wait_for_inflight(spl, async_lookup, &vtarg_false);

   elapsed_ns = platform_timestamp_elapsed(start_time);
   platform_default_log(
      " ... splinter negative async lookup time %lu s, per tuple %lu ns\n",
      NSEC_TO_SEC(elapsed_ns),
      (elapsed_ns / num_inserts));

   // Cleanup memory allocated in this test case
   if (async_lookup) {
      async_ctxt_deinit(data->hid, async_lookup);
   }

   trunk_destroy(spl);
   trunk_shadow_deinit(&shadow);
}

/*
 * Work-horse function to drive inserts into Splinter. # of inserts is
 * determined by config parameters, and computed below.
 *
 * Parmeters:
 *  datap       - Ptr to global data struct { }
 *  spl         - Ptr to splinter handle, established by caller.
 *  verify      - Boolean; periodically verify splinter tree consistency
 *  shadowpp    - Addr of ptr to shadow buffer, which will be allocated
 *                and filled-out in this function, if supplied.
 *
 * Returns the # of rows inserted.
 */
static uint64
splinter_do_inserts(void         *datap,
                    trunk_handle *spl,
                    bool          verify,
                    trunk_shadow *shadow) // Out
{
   // Cast void * datap to ptr-to-CTEST_DATA() struct in use.
   struct CTEST_IMPL_DATA_SNAME(splinter) *data =
      (struct CTEST_IMPL_DATA_SNAME(splinter) *)datap;

   // First see if test was invoked with --num-inserts execution parameter
   int num_inserts = data->test_exec_cfg.num_inserts;

   // If not, derive total # of rows to be inserted
   if (!num_inserts) {
      trunk_config *splinter_cfg = data->splinter_cfg;
      num_inserts                = splinter_cfg[0].max_kv_bytes_per_node
                    * splinter_cfg[0].fanout / 2
                    / generator_average_message_size(&data->gen);
   }

   platform_default_log("Splinter_cfg max_kv_bytes_per_node=%lu"
                        ", fanout=%lu"
                        ", max_extents_per_memtable=%lu, num_inserts=%d. ",
                        data->splinter_cfg[0].max_kv_bytes_per_node,
                        data->splinter_cfg[0].fanout,
                        data->splinter_cfg[0].mt_cfg.max_extents_per_memtable,
                        num_inserts);

   // Debug hook: Override this to smaller value for faster test execution,
   // while doing test-dev / debugging. Default is some big value, like
   // 12988800; num_inserts = (100 * 100 * 100);

   uint64       start_time = platform_get_timestamp();
   uint64       insert_num;
   char         key[MAX_KEY_SIZE];
   const size_t key_size = trunk_key_size(spl);

   // Allocate a large array for copying over shadow copies of rows
   // inserted, if user has asked to return such an array.
   if (shadow) {
      trunk_shadow_reinit(shadow);
   }

   platform_status rc;

   platform_default_log("trunk_insert() test with %d inserts %s ...\n",
                        num_inserts,
                        (verify ? "and verify" : ""));
   writable_buffer msg;
   writable_buffer_init(&msg, NULL);
   for (insert_num = 0; insert_num < num_inserts; insert_num++) {

      // Show progress message in %age-completed to stdout
      SHOW_PCT_PROGRESS(insert_num, num_inserts, "inserting %3lu%% complete");

      if (verify && (insert_num != 0)
          && (insert_num % TEST_VERIFY_GRANULARITY) == 0) {
         bool result = trunk_verify_tree(spl);
         ASSERT_TRUE(result,
                     "trunk_verify_tree() failed after %d inserts. ",
                     insert_num);
      }
      test_key(key, TEST_RANDOM, insert_num, 0, 0, key_size, 0);
      generate_test_message(&data->gen, insert_num, &msg);
      rc = trunk_insert(spl, key, writable_buffer_to_slice(&msg));
      ASSERT_TRUE(SUCCESS(rc),
                  "trunk_insert() FAILURE: %s\n",
                  platform_status_to_string(rc));

      // Caller is interested in using a copy of the rows inserted for
      // verification; e.g. by range-search lookups.
      if (shadow) {
         trunk_shadow_append(
            shadow, trunk_key_slice(spl, key), writable_buffer_to_slice(&msg));
      }
   }

   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
   uint64 elapsed_s  = NSEC_TO_SEC(elapsed_ns);

   // For small # of inserts, elapsed sec will be 0. Deal with it.
   platform_default_log(
      "... average tuple_size=%lu, splinter insert time %lu s, per "
      "tuple %lu ns, %s%lu rows/sec. ",
      key_size + generator_average_message_size(&data->gen),
      elapsed_s,
      (elapsed_ns / num_inserts),
      (elapsed_s ? "" : "(n/a)"),
      (elapsed_s ? (num_inserts / NSEC_TO_SEC(elapsed_ns)) : num_inserts));

   platform_assert(trunk_verify_tree(spl));
   cache_assert_free((cache *)data->clock_cache);

   // Cleanup memory allocated in this test case
   writable_buffer_deinit(&msg);
   return num_inserts;
}

typedef struct shadow_check_tuple_arg {
   trunk_handle *spl;
   trunk_shadow *shadow;
   uint64        pos;
   uint64        errors;
} shadow_check_tuple_arg;

static void
shadow_check_tuple_func(slice key, slice value, void *varg)
{
   shadow_check_tuple_arg *arg = varg;

   slice shadow_key;
   slice shadow_value;
   trunk_shadow_get(arg->shadow, arg->pos, &shadow_key, &shadow_value);
   if (slice_lex_cmp(key, shadow_key) || slice_lex_cmp(value, shadow_value)) {
      char expected_key[128];
      char actual_key[128];
      char expected_value[128];
      char actual_value[128];

      trunk_key_to_string(arg->spl, slice_data(shadow_key), expected_key);
      trunk_key_to_string(arg->spl, slice_data(key), actual_key);

      trunk_message_to_string(arg->spl, shadow_value, expected_value);
      trunk_message_to_string(arg->spl, value, actual_value);

      platform_log("expected: '%s' | '%s'\n", expected_key, expected_value);
      platform_log("actual  : '%s' | '%s'\n", actual_key, actual_value);
      arg->errors++;
   }

   arg->pos++;
}

/*
 * Driver routine to verify Splinter lookup by range searches.
 *
 * Parameters:
 *  datap       - Ptr to global data struct
 *  spl         - Ptr to trunk_handle
 *  num_inserts - # of inserts that we want ranges to span
 *  shadow      - Shadow buffer allocated by caller, while inserting rows.
 *  num_ranges  - # of range searches to do in this run
 */
static platform_status
test_lookup_by_range(void         *datap,
                     trunk_handle *spl,
                     uint64        num_inserts,
                     trunk_shadow *shadow,
                     uint64        num_ranges)
{
   const size_t key_size = trunk_key_size(spl);

   uint64 start_time = platform_get_timestamp();

   platform_status rc;

   for (uint64 range_num = 0; range_num != num_ranges; range_num++) {

      // Show progress message in %age-completed to stdout
      SHOW_PCT_PROGRESS(
         range_num, num_ranges, "Verify range    lookups %3lu%% complete");

      char start_key_buf[MAX_KEY_SIZE];
      test_key(start_key_buf,
               TEST_RANDOM,
               num_inserts + range_num,
               0,
               0,
               key_size,
               0);
      slice  start_key    = slice_create(key_size, start_key_buf);
      uint64 range_tuples = test_range(range_num, 1, 100);

      uint64 start_idx = test_splinter_bsearch(shadow, start_key);
      uint64 expected_returned_tuples = num_inserts - start_idx > range_tuples
                                           ? range_tuples
                                           : num_inserts - start_idx;

      shadow_check_tuple_arg arg = {
         .spl = spl, .shadow = shadow, .pos = start_idx, .errors = 0};

      rc = trunk_range(spl,
                       slice_data(start_key),
                       range_tuples,
                       shadow_check_tuple_func,
                       &arg);

      ASSERT_TRUE(SUCCESS(rc));
      ASSERT_TRUE(
         arg.errors == 0, "trunk_range() found %lu mismatches", arg.errors);
      ASSERT_TRUE(arg.pos == start_idx + expected_returned_tuples,
                  "trunk_range() saw wrong number of tuples: "
                  " expected_returned_tuples=%lu"
                  ", returned_tuples=%lu"
                  ", start_key='%.*s'"
                  ", errors=%lu",
                  expected_returned_tuples,
                  arg.pos - start_idx,
                  key_size,
                  start_key,
                  arg.errors);
   }

   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
   platform_default_log(" ... splinter range time %lu s, per operation %lu ns"
                        ", %lu ranges\n",
                        NSEC_TO_SEC(elapsed_ns),
                        (elapsed_ns / num_ranges),
                        num_ranges);

   return rc;
}
