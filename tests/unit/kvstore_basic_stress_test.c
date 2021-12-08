// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * kvstore_basic_stress_test.c -- KVStore Basic Stress test
 *
 * Exercises the kvstore_basic API, with larger data volumes, and multiple
 * threads.
 * -----------------------------------------------------------------------------
 */
#include <stdlib.h> // Needed for system calls; e.g. free
#include <stdio.h>
#include <unistd.h>
#include "ctest.h" // This is required for all test-case files.

#include "platform.h"
#include "splinterdb/kvstore_basic.h"

#define Mega (1024UL * 1024UL)

// Function Prototypes
static int
setup_kvstore_basic(kvstore_basic **kvsb, kvstore_basic_cfg *cfg);

static void *
exec_worker_thread(void *w);

// Structure defining worker thread configuration.
typedef struct {
   uint32_t       num_inserts;
   int            random_data;
   kvstore_basic *kvsb;
   uint16_t       max_key_size;
   uint16_t       max_value_size;
} worker_config;

/*
 * Global data declaration macro:
 */
CTEST_DATA(kvstore_basic_stress)
{
   kvstore_basic *   kvsb;
   kvstore_basic_cfg cfg;
};

// Setup function for suite, called before every test in suite
CTEST_SETUP(kvstore_basic_stress)
{
   memset(&data->cfg, 0, sizeof(data->cfg));
   data->cfg.cache_size     = 200 * Mega;
   data->cfg.disk_size      = 900 * Mega;
   data->cfg.max_key_size   = 22;
   data->cfg.max_value_size = 116;

   int rc = setup_kvstore_basic(&data->kvsb, &data->cfg);
   ASSERT_EQUAL(0, rc);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(kvstore_basic_stress)
{
   kvstore_basic_close(data->kvsb);
}

/*
 * ---------------------------------------------------------------------------
 * Test case that exercises inserts of large volume of data, single-threaded.
 * We exercise these Splinter APIs:
 *  - kvstore_basic_insert()
 *  - kvstore_basic_lookup() and
 *  - kvstore_basic_delete()
 */
CTEST2(kvstore_basic_stress, test_random_inserts_serial)
{
   int random_data = open("/dev/urandom", O_RDONLY);
   ASSERT_TRUE(random_data > 0);

   char key_buf[KVSTORE_BASIC_MAX_KEY_SIZE]     = {0};
   char value_buf[KVSTORE_BASIC_MAX_VALUE_SIZE] = {0};

   int rc = 0;

   uint64_t numkeys = (2 * Mega);

   printf(" Insert %lu keys ...", numkeys);
   fflush(stdout);

   for (uint64_t i = 0; i < numkeys; i++) {
      size_t result = read(random_data, key_buf, sizeof key_buf);
      ASSERT_TRUE(result >= 0);

      result = read(random_data, value_buf, sizeof key_buf);
      ASSERT_TRUE(result >= 0);

      rc = kvstore_basic_insert(data->kvsb,
                                key_buf,
                                data->cfg.max_key_size,
                                value_buf,
                                data->cfg.max_value_size);
      ASSERT_EQUAL(rc, 0);
   }
}

/*
 * -------------------------------------------------------------------------
 * Test case to exercise random inserts of large volumes of data, across
 * multiple threads. This test case verifies that registration of threads
 * to Splinter is working stably.
 */
CTEST2(kvstore_basic_stress, test_random_inserts_concurrent)
{
   // We need a configuration larger than the default setup.
   // Teardown the default splinter, and create a new one.
   kvstore_basic_close(data->kvsb);

   data->cfg.cache_size     = 1000 * Mega;
   data->cfg.disk_size      = 9000 * Mega;
   data->cfg.max_key_size   = 20;
   data->cfg.max_value_size = 116;

   int rc = setup_kvstore_basic(&data->kvsb, &data->cfg);
   ASSERT_EQUAL(0, rc);

   int random_data = open("/dev/urandom", O_RDONLY);
   ASSERT_TRUE(random_data >= 0);

   worker_config wcfg = {
      .num_inserts    = 1000 * 1000,
      .random_data    = random_data,
      .kvsb           = data->kvsb,
      .max_key_size   = data->cfg.max_key_size,
      .max_value_size = data->cfg.max_value_size,
   };

   const uint8_t num_threads = 4;
   pthread_t     thread_ids[num_threads];

   for (int i = 0; i < num_threads; i++) {
      rc = pthread_create(&thread_ids[i], NULL, &exec_worker_thread, &wcfg);
      ASSERT_EQUAL(0, rc);
   }

   fprintf(stderr, "Waiting for %d worker threads ...\n", num_threads);
   for (int i = 0; i < num_threads; i++) {
      fprintf(stderr, "  Thread[%d] ID=%lu\n", i, thread_ids[i]);
   }

   for (int i = 0; i < num_threads; i++) {
      void *thread_rc;
      rc = pthread_join(thread_ids[i], &thread_rc);
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

/*
 * ********************************************************************************
 * Define minions and helper functions used by this test suite.
 * ********************************************************************************
 */

static int
setup_kvstore_basic(kvstore_basic **kvsb, kvstore_basic_cfg *cfg)
{
   platform_stdout_fh = fopen("/tmp/unit_test.stdout", "a+");
   platform_stderr_fh = fopen("/tmp/unit_test.stderr", "a+");

   *cfg = (kvstore_basic_cfg){
      .filename       = TEST_DB_NAME,
      .cache_size     = (cfg->cache_size) ? cfg->cache_size : Mega,
      .disk_size      = (cfg->disk_size) ? cfg->disk_size : 30 * Mega,
      .max_key_size   = (cfg->max_key_size) ? cfg->max_key_size : 21,
      .max_value_size = (cfg->max_value_size) ? cfg->max_value_size : 16,
      .key_comparator = cfg->key_comparator,
      .key_comparator_context = cfg->key_comparator_context,
   };

   int rc = kvstore_basic_create(cfg, kvsb);
   ASSERT_EQUAL(rc, 0);

   return rc;
}

/*
 * -------------------------------------------------------------------------
 * Work-horse function to drive the execution of inserts for one thread.
 *
 * This exercises the threading interfaces of SplinterDB:
 *  - kvstore_basic_register_thread()
 *  - (Perform some work on this thread; here, kvstore_basic_insert() ]
 *  - kvstore_basic_deregister_thread()
 */
static void *
exec_worker_thread(void *w)
{
   char key_buf[KVSTORE_BASIC_MAX_KEY_SIZE]     = {0};
   char value_buf[KVSTORE_BASIC_MAX_VALUE_SIZE] = {0};

   worker_config *wcfg           = (worker_config *)w;
   uint32_t       num_inserts    = wcfg->num_inserts;
   int            random_data    = wcfg->random_data;
   kvstore_basic *kvsb           = wcfg->kvsb;
   uint16_t       max_key_size   = wcfg->max_key_size;
   uint16_t       max_value_size = wcfg->max_value_size;

   kvstore_basic_register_thread(kvsb);

   pthread_t thread_id = pthread_self();

   fprintf(stderr, "Writing lots of data from thread %lu\n", thread_id);
   int rc = 0;
   for (uint32_t i = 0; i < num_inserts; i++) {
      size_t result = read(random_data, key_buf, sizeof key_buf);
      ASSERT_TRUE(result >= 0);

      result = read(random_data, value_buf, sizeof value_buf);
      ASSERT_TRUE(result >= 0);

      rc = kvstore_basic_insert(
         kvsb, key_buf, max_key_size, value_buf, max_value_size);
      ASSERT_EQUAL(0, rc);

      if (i && (i % 100000 == 0)) {
         fprintf(stderr, "Thread %lu has completed %u inserts\n", thread_id, i);
      }
   }

   kvstore_basic_deregister_thread(kvsb);
   return 0;
}
