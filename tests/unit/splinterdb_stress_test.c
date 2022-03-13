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
#include "ctest.h" // This is required for all test-case files.

#define TEST_KEY_SIZE   20
#define TEST_VALUE_SIZE 116

// Function Prototypes
static void *
exec_worker_thread(void *w);

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
   Platform_stdout_fh = fopen("/tmp/unit_test.stdout", "a+");
   Platform_stderr_fh = fopen("/tmp/unit_test.stderr", "a+");

   data->cfg           = (splinterdb_config){.filename   = TEST_DB_NAME,
                                             .cache_size = 1000 * Mega,
                                             .disk_size  = 9000 * Mega,
                                             .data_cfg   = &data->default_data_config};
   size_t max_key_size = TEST_KEY_SIZE;
   default_data_config_init(max_key_size, data->cfg.data_cfg);

   int rc = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(splinterdb_stress)
{
   splinterdb_close(data->kvsb);
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

   fprintf(stderr, "Waiting for %d worker threads ...\n", num_threads);
   for (int i = 0; i < num_threads; i++) {
      fprintf(stderr, "  Thread[%d] ID=%lu\n", i, thread_ids[i]);
   }

   for (int i = 0; i < num_threads; i++) {
      void *thread_rc;
      int   rc = pthread_join(thread_ids[i], &thread_rc);
      ASSERT_EQUAL(0, rc);
      if (thread_rc != 0) {
         fprintf(stderr,
                 "Thread %d [ID=%lu] had error: %p\n",
                 i,
                 thread_ids[i],
                 thread_rc);
         ASSERT_TRUE(FALSE);
      }
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

   fprintf(stderr, "Writing lots of data from thread %lu\n", thread_id);
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
         fprintf(stderr, "Thread %lu has completed %u inserts\n", thread_id, i);
      }
   }

   splinterdb_deregister_thread(kvsb);
   return 0;
}
