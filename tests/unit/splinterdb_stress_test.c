// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * Simple stress test driving inserts into the public API of SplinterDB
 */

#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

#include "splinterdb/public_platform.h"
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "unit_tests.h"
#include "util.h"
#include "../functional/random.h"
#include "ctest.h" // This is required for all test-case files.

#define TEST_KEY_SIZE   20
#define TEST_VALUE_SIZE 116

// Function Prototypes
static void *
exec_worker_thread(void *w);

static void
naive_range_delete(const splinterdb *kvsb, slice start_key, uint32 count);

// Configuration for each worker thread
typedef struct {
   uint32_t    num_inserts;
   int         random_data;
   splinterdb *kvsb;
   uint16_t    max_key_size;
   uint16_t    max_value_size;
} worker_config;

// Global test data
CTEST_DATA(splinterdb_stress)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_config;
};

// Setup function for suite, called before every test in suite
CTEST_SETUP(splinterdb_stress)
{
   data->cfg           = (splinterdb_config){.filename   = TEST_DB_NAME,
                                             .cache_size = 1000 * Mega,
                                             .disk_size  = 9000 * Mega,
                                             .data_cfg   = &data->default_data_config,
                                             .num_memtable_bg_threads = 2,
                                             .num_normal_bg_threads   = 2};
   size_t max_key_size = TEST_KEY_SIZE;
   default_data_config_init(max_key_size, data->cfg.data_cfg);

   int rc = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(splinterdb_stress)
{
   splinterdb_close(&data->kvsb);
}

// Do random inserts across multiple threads
CTEST2(splinterdb_stress, test_random_inserts_concurrent)
{
   int random_data = open("/dev/urandom", O_RDONLY);
   ASSERT_TRUE(random_data >= 0);

   worker_config wcfg = {
      .num_inserts = 1000 * 1000,
      .random_data = random_data,
      .kvsb        = data->kvsb,
   };

#define num_threads 4
   pthread_t thread_ids[num_threads];

   for (int i = 0; i < num_threads; i++) {
      int rc = pthread_create(&thread_ids[i], NULL, &exec_worker_thread, &wcfg);
      ASSERT_EQUAL(0, rc);
   }

   CTEST_LOG_INFO("Waiting for %d worker threads ...\n", num_threads);
   for (int i = 0; i < num_threads; i++) {
      CTEST_LOG_INFO("  Thread[%d] ID=%lu\n", i, thread_ids[i]);
   }

   for (int i = 0; i < num_threads; i++) {
      void *thread_rc;
      int   rc = pthread_join(thread_ids[i], &thread_rc);
      ASSERT_EQUAL(0, rc);
      if (thread_rc != 0) {
         CTEST_ERR(
            "Thread %d [ID=%lu] had error: %p\n", i, thread_ids[i], thread_rc);
      }
   }
}


// Do some inserts, and then some range-deletes
CTEST2(splinterdb_stress, test_naive_range_delete)
{
   random_state rand_state;
   random_init(&rand_state, 42, 0);

   const uint32 num_inserts = 2 * 1000 * 1000;

   CTEST_LOG_INFO("loading data...");
   for (uint32 i = 0; i < num_inserts; i++) {
      char key_buffer[TEST_KEY_SIZE]     = {0};
      char value_buffer[TEST_VALUE_SIZE] = {0};
      random_bytes(&rand_state, key_buffer, TEST_KEY_SIZE);
      random_bytes(&rand_state, value_buffer, TEST_VALUE_SIZE);
      int rc = splinterdb_insert(data->kvsb,
                                 slice_create(TEST_KEY_SIZE, key_buffer),
                                 slice_create(TEST_VALUE_SIZE, value_buffer));
      ASSERT_EQUAL(0, rc);
   }

   CTEST_LOG_INFO("loaded %u k/v pairs\n", num_inserts);

   uint32 num_rounds = 5;
   for (uint32 round = 0; round < num_rounds; round++) {
      CTEST_LOG_INFO("range delete round %d...\n", round);
      char start_key_data[4];
      random_bytes(&rand_state, start_key_data, sizeof(start_key_data));
      const uint32 num_to_delete = num_inserts / num_rounds;

      naive_range_delete(data->kvsb,
                         slice_create(sizeof(start_key_data), start_key_data),
                         num_to_delete);
   }
}

/*
 * Test case that inserts a large number of KV-pairs and then performs a
 * range query over them, backwards and then forwards.
 */
#define KEY_SIZE 13
CTEST2(splinterdb_stress, test_iterator_over_many_kvs)
{
   char         key_str[KEY_SIZE];
   char        *value_str = "This is the value string\0";
   const uint32 inserts   = 1 << 25; // 16 million
   for (int i = 0; i < inserts; i++) {
      snprintf(key_str, sizeof(key_str), "key-%08x", i);
      slice key = slice_create(sizeof(key_str), key_str);
      slice val = slice_create(sizeof(value_str), value_str);
      ASSERT_EQUAL(0, splinterdb_insert(data->kvsb, key, val));
   }

   // create an iterator at end of keys
   splinterdb_iterator *it = NULL;
   snprintf(key_str, sizeof(key_str), "key-%08x", inserts);
   slice start_key = slice_create(sizeof(key_str), key_str);
   ASSERT_EQUAL(0, splinterdb_iterator_init(data->kvsb, &it, start_key));

   // assert that the iterator is in the state we expect
   ASSERT_FALSE(splinterdb_iterator_valid(it));
   ASSERT_FALSE(splinterdb_iterator_can_next(it));
   ASSERT_TRUE(splinterdb_iterator_can_prev(it));
   ASSERT_EQUAL(0, splinterdb_iterator_status(it));

   splinterdb_iterator_prev(it);

   // iterate down all the keys
   for (int i = inserts - 1; i >= 0; i--) {
      ASSERT_TRUE(splinterdb_iterator_valid(it));
      slice key;
      slice val;
      splinterdb_iterator_get_current(it, &key, &val);
      snprintf(key_str, sizeof(key_str), "key-%08x", i);

      ASSERT_EQUAL(KEY_SIZE, slice_length(key));
      int comp = memcmp(slice_data(key), key_str, slice_length(key));
      ASSERT_EQUAL(0, comp);
      splinterdb_iterator_prev(it);
   }

   // assert that the iterator is in the state we expect
   ASSERT_FALSE(splinterdb_iterator_valid(it));
   ASSERT_TRUE(splinterdb_iterator_can_next(it));
   ASSERT_FALSE(splinterdb_iterator_can_prev(it));
   ASSERT_EQUAL(0, splinterdb_iterator_status(it));

   splinterdb_iterator_next(it);

   // iterate back up all the keys
   for (int i = 0; i < inserts; i++) {
      ASSERT_TRUE(splinterdb_iterator_valid(it));
      slice key;
      slice val;
      splinterdb_iterator_get_current(it, &key, &val);
      snprintf(key_str, sizeof(key_str), "key-%08x", i);

      ASSERT_EQUAL(KEY_SIZE, slice_length(key));
      int comp = memcmp(slice_data(key), key_str, slice_length(key));
      ASSERT_EQUAL(0, comp);
      splinterdb_iterator_next(it);
   }

   // assert that the iterator is in the state we expect
   ASSERT_FALSE(splinterdb_iterator_valid(it));
   ASSERT_FALSE(splinterdb_iterator_can_next(it));
   ASSERT_TRUE(splinterdb_iterator_can_prev(it));
   ASSERT_EQUAL(0, splinterdb_iterator_status(it));

   splinterdb_iterator_deinit(it);
}

/*
 * Test case that inserts large # of KV-pairs, and goes into a code path
 * reported by issue# 458, tripping a debug assert. This test case also
 * triggered the failure(s) reported by issue # 545.
 * FIXME: This test still runs into an assertion "filter->addr != 0"
 * from trunk_inc_filter(), which is being triaged separately.
 */
CTEST2_SKIP(splinterdb_stress, test_issue_458_mini_destroy_unused_debug_assert)
{
   char key_data[TEST_KEY_SIZE];
   char val_data[TEST_VALUE_SIZE];

   uint64 test_start_time = platform_get_timestamp();

   for (uint64 ictr = 0, jctr = 0; ictr < 100; ictr++) {

      uint64 start_time = platform_get_timestamp();

      for (jctr = 0; jctr < MILLION; jctr++) {

         uint64 id = (ictr * MILLION) + jctr;
         snprintf(key_data, sizeof(key_data), "%lu", id);
         snprintf(val_data, sizeof(val_data), "Row-%lu", id);

         slice key = slice_create(strlen(key_data), key_data);
         slice val = slice_create(strlen(val_data), val_data);

         int rc = splinterdb_insert(data->kvsb, key, val);
         ASSERT_EQUAL(0, rc);
      }
      uint64 elapsed_ns      = platform_timestamp_elapsed(start_time);
      uint64 test_elapsed_ns = platform_timestamp_elapsed(test_start_time);

      uint64 elapsed_s = NSEC_TO_SEC(elapsed_ns);
      if (elapsed_s == 0) {
         elapsed_s = 1;
      }
      uint64 test_elapsed_s = NSEC_TO_SEC(test_elapsed_ns);
      if (test_elapsed_s == 0) {
         test_elapsed_s = 1;
      }

      CTEST_LOG_INFO(
         "\n" // PLATFORM_CR
         "Inserted %lu million KV-pairs"
         ", this batch: %lu s, %lu rows/s, cumulative: %lu s, %lu rows/s ...",
         (ictr + 1),
         elapsed_s,
         (jctr / elapsed_s),
         test_elapsed_s,
         (((ictr + 1) * jctr) / test_elapsed_s));
   }
}

// Per-thread workload
static void *
exec_worker_thread(void *w)
{
   char key_buf[TEST_KEY_SIZE]     = {0};
   char value_buf[TEST_VALUE_SIZE] = {0};

   worker_config *wcfg        = (worker_config *)w;
   uint32_t       num_inserts = wcfg->num_inserts;
   int            random_data = wcfg->random_data;
   splinterdb    *kvsb        = wcfg->kvsb;

   splinterdb_register_thread(kvsb);

   pthread_t thread_id = pthread_self();

   CTEST_LOG_INFO("Writing lots of data from thread %lu\n", thread_id);
   int rc = 0;
   for (uint32_t i = 0; i < num_inserts; i++) {
      size_t result = read(random_data, key_buf, sizeof key_buf);
      ASSERT_TRUE(result >= 0);

      result = read(random_data, value_buf, sizeof value_buf);
      ASSERT_TRUE(result >= 0);

      rc = splinterdb_insert(kvsb,
                             slice_create(TEST_KEY_SIZE, key_buf),
                             slice_create(TEST_VALUE_SIZE, value_buf));
      ASSERT_EQUAL(0, rc);

      if (i && (i % 100000 == 0)) {
         CTEST_LOG_INFO("Thread %lu has completed %u inserts\n", thread_id, i);
      }
   }

   splinterdb_deregister_thread(kvsb);
   return 0;
}


// Do a "range delete" by collecting keys and then deleting them one at a time
static void
naive_range_delete(const splinterdb *kvsb, slice start_key, uint32 count)
{
   CTEST_LOG_INFO("\tcollecting keys to delete...\n");
   char *keys_to_delete = calloc(count, TEST_KEY_SIZE);

   splinterdb_iterator *it;

   int rc = splinterdb_iterator_init(kvsb, &it, start_key);
   ASSERT_EQUAL(0, rc);

   slice key, value;

   uint32 num_found = 0;
   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      splinterdb_iterator_get_current(it, &key, &value);
      ASSERT_EQUAL(TEST_KEY_SIZE, slice_length(key));
      memcpy(keys_to_delete + num_found * TEST_KEY_SIZE,
             slice_data(key),
             TEST_KEY_SIZE);
      num_found++;
      if (num_found >= count) {
         break;
      }
   }
   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);
   splinterdb_iterator_deinit(it);

   CTEST_LOG_INFO("\tdeleting collected keys...\n");
   for (uint32 i = 0; i < num_found; i++) {
      slice key_to_delete =
         slice_create(TEST_KEY_SIZE, keys_to_delete + i * TEST_KEY_SIZE);
      splinterdb_delete(kvsb, key_to_delete);
   }

   free(keys_to_delete);
   CTEST_LOG_INFO("\tdeleted %u k/v pairs\n", num_found);
}
