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
   if (Ctest_verbose) {
      platform_set_log_streams(stdout, stderr);
   }

   data->cfg           = (splinterdb_config){.filename   = TEST_DB_NAME,
                                             .cache_size = 1000 * Mega,
                                             .disk_size  = 9000 * Mega,
                                             .data_cfg   = &data->default_data_config,
                                             .queue_scale_percent = 100};
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
