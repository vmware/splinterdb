// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinter_test.c --
 *
 *  Exercises the basic SplinterDB interfaces.
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

/* Function prototypes */
static uint64
splinter_do_inserts(void         *datap,
                    trunk_handle *spl,
                    bool          verify,
                    char        **shadowpp,
                    int          *tuple_size);

static void
test_splinter_shadow_insert(trunk_handle *spl,
                            char         *shadow,
                            char         *key,
                            char         *data,
                            uint64        idx);

static platform_status
test_lookup_by_range(void         *datap,
                     trunk_handle *spl,
                     uint64        num_inserts,
                     char         *shadow,
                     uint64        num_ranges);

static int
test_trunk_key_compare(const void *left, const void *right, void *spl);

static void *
test_splinter_bsearch(register const void *key,
                      void                *base0,
                      size_t               nmemb,
                      register size_t      size,
                      register int (*compar)(const void *,
                                             const void *,
                                             void *),
                      void *ctxt);


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
   uint64               seed;

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
   trunk_config           *splinter_cfg;
   data_config            *data_cfg;
   clockcache_config      *cache_cfg;
   platform_io_handle     *io;
   clockcache             *clock_cache;
   task_system            *tasks;
   test_message_generator  gen;
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

   data->seed = 0;

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

   rc = test_parse_args_n(data->splinter_cfg,
                          data->data_cfg,
                          &data->io_cfg,
                          &data->al_cfg,
                          data->cache_cfg,
                          &data->log_cfg,
                          &data->seed,
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

   if (data->data_cfg) {
      platform_free(data->hid, data->data_cfg);
   }

   if (data->splinter_cfg) {
      platform_free(data->hid, data->splinter_cfg);
   }

   platform_heap_destroy(&data->hh);
}

/*
 * **************************************************************************
 * Basic test case to verify trunk_insert() API and validate inserts.
 * This is a valid test case and does run successfully. However, enabling
 * this increases the execution time of this test, tipping the elapsed time
 * for debug builds over the timeout limits (25 mins, at the time of this
 * writing). The insert phase is also required, and covered, in the subsequent
 * lookups test case, hence, this test case is skipped.
 * **************************************************************************
 */
CTEST2_SKIP(splinter, test_inserts)
{
   allocator *alp = (allocator *)&data->al;

   trunk_handle *spl = trunk_create(data->splinter_cfg,
                                    alp,
                                    (cache *)data->clock_cache,
                                    data->tasks,
                                    test_generate_allocator_root_id(),
                                    data->hid);
   ASSERT_TRUE(spl != NULL);

   int    tuple_size  = 0;
   uint64 num_inserts = splinter_do_inserts(data, spl, TRUE, NULL, &tuple_size);
   ASSERT_NOT_EQUAL(0,
                    num_inserts,
                    "Expected to have inserted non-zero rows, num_inserts=%lu"
                    ", tuple_size=%d.",
                    num_inserts,
                    tuple_size);

   trunk_destroy(spl);
}

/*
 * **************************************************************************
 * Test case to run a bunch of inserts into Splinter, and then perform
 * different types of lookup-verification. As all lookups need an inserts step,
 * this test case is really a set of multiple sub-test-cases for inserts,
 * synchronous and async lookups, and lookups-by-range rolled into one.
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

   char *shadow     = NULL;
   int   tuple_size = 0;

   // TRUE : Also do verification-after-inserts
   uint64 num_inserts =
      splinter_do_inserts(data, spl, TRUE, &shadow, &tuple_size);
   ASSERT_NOT_EQUAL(0,
                    num_inserts,
                    "Expected to have inserted non-zero rows, num_inserts=%lu.",
                    num_inserts);

   const size_t data_size = trunk_message_size(spl);

   writable_buffer qdata;
   writable_buffer_init_null(&qdata, NULL);
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
      writable_buffer_reinit(&qdata);

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
   char *temp = TYPED_ARRAY_ZALLOC(data->hid, temp, tuple_size);
   platform_sort_slow(
      shadow, num_inserts, tuple_size, test_trunk_key_compare, spl, temp);
   platform_free(data->hid, temp);

   int niters = 3;
   platform_default_log(
      "Perform test_lookup_by_range() for %d iterations ...\n", niters);
   // Iterate thru small set of num_ranges for additional coverage.
   for (int ictr = 1; ictr <= 3; ictr++) {

      uint64 num_ranges = (num_inserts / 128) * ictr;

      // Range search uses the shadow-copy of the rows previously inserted while
      // doing a binary-search.
      rc = test_lookup_by_range(
         (void *)data, spl, num_inserts, shadow, num_ranges);
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
   async_ctxt_init(
      data->hid, data->max_async_inflight, data_size, &async_lookup);

   test_async_ctxt *ctxt = NULL;

   // **************************************************************************
   // Test sub-case 4: Validate using asynchronous trunk_lookup().
   //   Verify that all the keys inserted are found via lookup.
   // **************************************************************************

   // Declare an expected data tuple that will be found.
   char *expected_data =
      TYPED_ARRAY_MALLOC(data->hid, expected_data, data_size);
   ASSERT_TRUE(expected_data != NULL);

   verify_tuple_arg vtarg_true = {.expected_data  = expected_data,
                                  .data_size      = data_size,
                                  .expected_found = TRUE};

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
   verify_tuple_arg vtarg_false = {
      .expected_data = NULL, .data_size = 0, .expected_found = FALSE};

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
   platform_free(data->hid, expected_data);
   if (async_lookup) {
      async_ctxt_deinit(data->hid, async_lookup);
   }

   platform_free(data->hid, shadow);
   trunk_destroy(spl);
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
 *  tuple_size  - Size of tuple
 *
 * Returns the # of rows inserted.
 */
static uint64
splinter_do_inserts(void         *datap,
                    trunk_handle *spl,
                    bool          verify,
                    char        **shadowpp, // Out
                    int          *tuple_size)        // Out
{
   // Cast void * datap to ptr-to-CTEST_DATA() struct in use.
   struct CTEST_IMPL_DATA_SNAME(splinter) *data =
      (struct CTEST_IMPL_DATA_SNAME(splinter) *)datap;
   int num_inserts =
      data->splinter_cfg[0].max_tuples_per_node * data->splinter_cfg[0].fanout;

   // Debug hook: Override this to smaller value for faster test execution,
   // while doing test-dev / debugging. Default is some big value, like
   // 12988800; num_inserts = (100 * 100 * 100);

   uint64       start_time = platform_get_timestamp();
   uint64       insert_num;
   char         key[MAX_KEY_SIZE];
   const size_t key_size  = trunk_key_size(spl);
   const size_t data_size = trunk_message_size(spl);
   char        *databuf   = TYPED_ARRAY_MALLOC(data->hid, databuf, data_size);
   ASSERT_TRUE(databuf != NULL);

   *tuple_size        = (int)(key_size + data_size);
   uint64 shadow_size = (*tuple_size * num_inserts);

   // Allocate a large array for copying over shadow copies of rows
   // inserted, if user has asked to return such an array.
   char *shadow = NULL;
   if (shadowpp) {
      shadow = TYPED_ARRAY_ZALLOC(data->hid, shadow, shadow_size);
   }

   platform_status rc;

   platform_default_log("trunk_insert() test with %d inserts %s ...\n",
                        num_inserts,
                        (verify ? "and verify" : ""));
   writable_buffer msg;
   writable_buffer_init_null(&msg, NULL);
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
      if (shadowpp) {
         test_splinter_shadow_insert(spl, shadow, key, databuf, insert_num);
      }
   }

   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);

   platform_default_log(
      "... tuple_size=%d, splinter insert time %lu s, per tuple %lu ns. ",
      *tuple_size,
      NSEC_TO_SEC(elapsed_ns),
      (elapsed_ns / num_inserts));

   platform_assert(trunk_verify_tree(spl));
   cache_assert_free((cache *)data->clock_cache);

   // Return allocated memory for shadow copies to caller. [ Caller is expected
   // to free this memory. ]
   if (shadowpp) {
      platform_default_log(
         "\nAllocated shadow buffer %p, of %lu bytes.", shadow, shadow_size);
      *shadowpp = shadow;
   }
   // Cleanup memory allocated in this test case
   platform_free(data->hid, databuf);
   return num_inserts;
}

/*
 * Copy the newly inserted key/value row to a shadow buffer. This set of
 * rows will be used later during lookup-validation using range searches.
 */
static void
test_splinter_shadow_insert(trunk_handle *spl,
                            char         *shadow,
                            char         *key,
                            char         *data,
                            uint64        idx)
{
   uint64 byte_pos = idx * (trunk_key_size(spl) + trunk_message_size(spl));
   memmove(&shadow[byte_pos], key, trunk_key_size(spl));
   byte_pos += trunk_key_size(spl);
   memmove(&shadow[byte_pos], data, trunk_message_size(spl));
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
                     char         *shadow,
                     uint64        num_ranges)
{
   // Cast void * datap to ptr-to-CTEST_DATA() struct in use.
   struct CTEST_IMPL_DATA_SNAME(splinter) *data =
      (struct CTEST_IMPL_DATA_SNAME(splinter) *)datap;

   const size_t key_size   = trunk_key_size(spl);
   const size_t data_size  = trunk_message_size(spl);
   uint64       tuple_size = key_size + data_size;

   uint64 start_time = platform_get_timestamp();

   char *range_output =
      TYPED_ARRAY_MALLOC(data->hid, range_output, 100 * tuple_size);

   platform_status rc;

   for (uint64 range_num = 0; range_num != num_ranges; range_num++) {

      // Show progress message in %age-completed to stdout
      SHOW_PCT_PROGRESS(
         range_num, num_ranges, "Verify range    lookups %3lu%% complete");

      char start_key[MAX_KEY_SIZE];
      test_key(
         start_key, TEST_RANDOM, num_inserts + range_num, 0, 0, key_size, 0);
      uint64 range_tuples    = test_range(range_num, 1, 100);
      uint64 returned_tuples = 0;

      rc = trunk_range(
         spl, start_key, range_tuples, &returned_tuples, range_output);
      ASSERT_TRUE(SUCCESS(rc),
                  "trunk_range() failed for range_tuples=%lu"
                  ", returned_tuples=%lu"
                  ", start_key='%.*s'",
                  range_tuples,
                  returned_tuples,
                  key_size,
                  start_key);

      char *shadow_start = test_splinter_bsearch(start_key,
                                                 shadow,
                                                 num_inserts,
                                                 tuple_size,
                                                 test_trunk_key_compare,
                                                 spl);

      uint64 start_idx                = (shadow_start - shadow) / tuple_size;
      uint64 expected_returned_tuples = num_inserts - start_idx > range_tuples
                                           ? range_tuples
                                           : num_inserts - start_idx;
      if (returned_tuples != expected_returned_tuples
          || memcmp(shadow_start, range_output, returned_tuples * tuple_size)
                != 0)
      {
         platform_log("range lookup: incorrect return\n");
         char start[128];
         trunk_key_to_string(spl, start_key, start);
         platform_log("start_key: %s\n", start);
         platform_log("tuples returned: expected %lu actual %lu\n",
                      expected_returned_tuples,
                      returned_tuples);
         for (uint64 i = 0; i < expected_returned_tuples; i++) {
            char   expected[128];
            char   actual[128];
            uint64 offset = i * tuple_size;
            trunk_key_to_string(spl, shadow_start + offset, expected);
            trunk_key_to_string(spl, range_output + offset, actual);
            char expected_data[128];
            char actual_data[128];
            offset += key_size;

            trunk_message_to_string(
               spl,
               trunk_message_slice(spl, shadow_start + offset),
               expected_data);

            trunk_message_to_string(
               spl,
               trunk_message_slice(spl, range_output + offset),
               actual_data);
            if (i < returned_tuples) {
               platform_log("expected: '%s' | '%s'\n", expected, expected_data);
               platform_log("actual  : '%s' | '%s'\n", actual, actual_data);
            } else {
               platform_log("expected: '%s' | '%s'\n", expected, expected_data);
            }
         }
         platform_assert(0);
      }
   }

   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
   platform_default_log(" ... splinter range time %lu s, per operation %lu ns"
                        ", %lu ranges\n",
                        NSEC_TO_SEC(elapsed_ns),
                        (elapsed_ns / num_ranges),
                        num_ranges);

   // Cleanup memory allocated in this routine
   platform_free(data->hid, range_output);

   return rc;
}

static int
test_trunk_key_compare(const void *left, const void *right, void *spl)
{
   return trunk_key_compare(
      (trunk_handle *)spl, (const char *)left, (const char *)right);
}

static void *
test_splinter_bsearch(register const void *key,
                      void                *base0,
                      size_t               nmemb,
                      register size_t      size,
                      register int (*compar)(const void *,
                                             const void *,
                                             void *),
                      void *ctxt)
{
   register char *base = (char *)base0;
   register int   lim, cmp;
   register void *p;

   platform_assert(nmemb != 0);
   for (lim = nmemb; lim != 0; lim >>= 1) {
      p   = base + (lim >> 1) * size;
      cmp = (*compar)(key, p, ctxt);
      if (cmp >= 0) { /* key > p: move right */
         base = (char *)p + size;
         lim--;
      } /* else move left */
   }
   if (cmp < 0) {
      return (void *)p;
   }
   return (void *)p + size;
}
