// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * large_inserts_bugs_stress_test.c --
 *
 * This test exercises simple very large #s of inserts which have found to
 * trigger some bugs in some code paths. This is just a miscellaneous collection
 * of test cases for different issues reported.
 * -----------------------------------------------------------------------------
 */
#include <string.h>
#include <sys/wait.h>

#include "platform.h"
#include "splinterdb/public_platform.h"
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "config.h"
#include "unit_tests.h"
#include "test_misc_common.h"
#include "ctest.h" // This is required for all test-case files.

// Nothing particularly significant about these constants.
#define TEST_KEY_SIZE   20
#define TEST_VALUE_SIZE 116

// Configuration for each worker thread
typedef struct {
   splinterdb    *kvsb;
   master_config *master_cfg;
   uint64         start_value;
   uint64         num_inserts;
   uint64         num_threads;
   int            random_key_fd; // Also used as a boolean
   int            random_val_fd; // Also used as a boolean
   bool           is_thread;     // Is main() or thread executing worker fn
} worker_config;

// Function Prototypes
static void *
exec_worker_thread(void *w);

static void
do_inserts_n_threads(splinterdb      *kvsb,
                     master_config   *master_cfg,
                     platform_heap_id hid,
                     int              random_key_fd,
                     int              random_val_fd,
                     uint64           num_inserts,
                     uint64           num_threads);

// Run n-threads concurrently inserting many KV-pairs
#define NUM_THREADS 8

/*
 * Global data declaration macro:
 */
CTEST_DATA(large_inserts_bugs_stress)
{
   // Declare heap handles for on-stack buffer allocations
   platform_heap_handle hh;
   platform_heap_id     hid;

   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_config;
   master_config     master_cfg;
   uint64            num_inserts; // per main() process or per thread
   int               this_pid;
   bool              am_parent;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(large_inserts_bugs_stress)
{
   // First, register that main() is being run as a parent process
   data->am_parent = TRUE;
   data->this_pid  = getpid();

   platform_status rc;
   uint64          heap_capacity = (64 * MiB); // small heap is sufficient.

   // Create a heap for allocating on-stack buffers for various arrays.
   rc = platform_heap_create(
      platform_get_module_id(), heap_capacity, FALSE, &data->hh, &data->hid);
   platform_assert_status_ok(rc);

   // If --use-shmem was provided, parse past that argument.
   int    argc      = Ctest_argc;
   char **argv      = (char **)Ctest_argv;
   bool   use_shmem = (argc >= 1) && test_using_shmem(argc, (char **)argv);
   if (use_shmem) {
      argc--;
      argv++;
   }

   data->cfg = (splinterdb_config){.filename   = TEST_DB_NAME,
                                   .cache_size = 256 * Mega,
                                   .disk_size  = 40 * Giga,
                                   .use_shmem  = use_shmem,
                                   .data_cfg   = &data->default_data_config};

   ZERO_STRUCT(data->master_cfg);
   config_set_defaults(&data->master_cfg);

   // Expected args to parse --num-inserts, --num-threads, --verbose-progress.
   rc = config_parse(&data->master_cfg, 1, argc, (char **)argv);
   ASSERT_TRUE(SUCCESS(rc));

   data->num_inserts =
      (data->master_cfg.num_inserts ? data->master_cfg.num_inserts
                                    : (20 * MILLION));

   // If num_threads is unspecified, use default for this test.
   if (!data->master_cfg.num_threads) {
      data->master_cfg.num_threads = NUM_THREADS;
   }

   if ((data->num_inserts % MILLION) != 0) {
      platform_error_log("Test expects --num-inserts parameter to be an"
                         " integral multiple of a million.\n");
      ASSERT_EQUAL(0, (data->num_inserts % MILLION));
      return;
   }
   size_t max_key_size = TEST_KEY_SIZE;
   default_data_config_init(max_key_size, data->cfg.data_cfg);

   int rv = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rv);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(large_inserts_bugs_stress)
{
   // Only parent process should tear down Splinter.
   if (data->am_parent) {
      splinterdb_close(&data->kvsb);
      platform_heap_destroy(&data->hh);
   }
}

/*
 * Test case that inserts large # of KV-pairs, and goes into a code path
 * reported by issue# 458, tripping a debug assert.
 */
CTEST2_SKIP(large_inserts_bugs_stress,
            test_issue_458_mini_destroy_unused_debug_assert)
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

      platform_default_log(
         PLATFORM_CR
         "Inserted %lu million KV-pairs"
         ", this batch: %lu s, %lu rows/s, cumulative: %lu s, %lu rows/s ...",
         (ictr + 1),
         NSEC_TO_SEC(elapsed_ns),
         (jctr / NSEC_TO_SEC(elapsed_ns)),
         NSEC_TO_SEC(test_elapsed_ns),
         (((ictr + 1) * jctr) / NSEC_TO_SEC(test_elapsed_ns)));
   }
}

/*
 * Test cases exercise the thread's worker-function, exec_worker_thread(),
 * from the main connection to splinter, for specified number of inserts.
 *
 * We play with 4 combinations just to get some basic coverage:
 *  - sequential keys and values
 *  - random keys, sequential values
 *  - sequential keys, random values
 *  - random keys, random values
 */
CTEST2(large_inserts_bugs_stress, test_seq_key_seq_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb        = data->kvsb;
   wcfg.master_cfg  = &data->master_cfg;
   wcfg.num_inserts = data->num_inserts;

   exec_worker_thread(&wcfg);
}

CTEST2(large_inserts_bugs_stress, test_seq_key_seq_values_inserts_forked)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb        = data->kvsb;
   wcfg.master_cfg  = &data->master_cfg;
   wcfg.num_inserts = data->num_inserts;

   int pid = getpid();

   if (wcfg.master_cfg->fork_child) {
      pid = fork();

      if (pid < 0) {
         platform_error_log("fork() of child process failed: pid=%d\n", pid);
         return;
      } else if (pid) {
         platform_default_log("Thread-ID=%lu, OS-pid=%d: "
                              "Waiting for child pid=%d to complete ...\n",
                              platform_get_tid(),
                              getpid(),
                              pid);

         wait(NULL);

         platform_default_log("Thread-ID=%lu, OS-pid=%d: "
                              "Child execution wait() completed."
                              " Resuming parent ...\n",
                              platform_get_tid(),
                              getpid());
      }
   }
   if (pid == 0) {
      // Record in global data that we are now running as a child.
      data->am_parent = FALSE;
      data->this_pid  = getpid();

      platform_default_log(
         "Running as %s process OS-pid=%d ...\n",
         (wcfg.master_cfg->fork_child ? "forked child" : "parent"),
         data->this_pid);

      splinterdb_register_thread(wcfg.kvsb);

      exec_worker_thread(&wcfg);

      platform_default_log("Child process Thread-ID=%lu"
                           ", OS-pid=%d completed inserts.\n",
                           platform_get_tid(),
                           data->this_pid);
      splinterdb_deregister_thread(wcfg.kvsb);
      return;
   }
}

CTEST2(large_inserts_bugs_stress, test_random_key_seq_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb          = data->kvsb;
   wcfg.master_cfg    = &data->master_cfg;
   wcfg.num_inserts   = data->num_inserts;
   wcfg.random_key_fd = open("/dev/urandom", O_RDONLY);

   exec_worker_thread(&wcfg);

   close(wcfg.random_key_fd);
}

CTEST2(large_inserts_bugs_stress, test_seq_key_random_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb          = data->kvsb;
   wcfg.master_cfg    = &data->master_cfg;
   wcfg.num_inserts   = data->num_inserts;
   wcfg.random_val_fd = open("/dev/urandom", O_RDONLY);

   exec_worker_thread(&wcfg);

   close(wcfg.random_val_fd);
}

CTEST2(large_inserts_bugs_stress, test_random_key_random_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb          = data->kvsb;
   wcfg.master_cfg    = &data->master_cfg;
   wcfg.num_inserts   = data->num_inserts;
   wcfg.random_key_fd = open("/dev/urandom", O_RDONLY);
   wcfg.random_val_fd = open("/dev/urandom", O_RDONLY);

   exec_worker_thread(&wcfg);

   close(wcfg.random_key_fd);
   close(wcfg.random_val_fd);
}

/*
 * Test case that fires up many threads each concurrently inserting large # of
# KV-pairs.
 */
CTEST2(large_inserts_bugs_stress, test_seq_key_seq_values_inserts_threaded)
{
   // Run n-threads with sequential key and sequential values inserted
   do_inserts_n_threads(data->kvsb,
                        &data->master_cfg,
                        data->hid,
                        0,
                        0,
                        data->num_inserts,
                        data->master_cfg.num_threads);
}

CTEST2(large_inserts_bugs_stress, test_random_keys_seq_values_threaded)
{
   int random_key_fd = open("/dev/urandom", O_RDONLY);
   ASSERT_TRUE(random_key_fd > 0);

   // Run n-threads with sequential key and sequential values inserted
   do_inserts_n_threads(data->kvsb,
                        &data->master_cfg,
                        data->hid,
                        random_key_fd,
                        0,
                        data->num_inserts,
                        data->master_cfg.num_threads);

   close(random_key_fd);
}

CTEST2(large_inserts_bugs_stress, test_seq_keys_random_values_threaded)
{
   int random_val_fd = open("/dev/urandom", O_RDONLY);
   ASSERT_TRUE(random_val_fd > 0);

   // Run n-threads with sequential key and sequential values inserted
   do_inserts_n_threads(data->kvsb,
                        &data->master_cfg,
                        data->hid,
                        0,
                        random_val_fd,
                        data->num_inserts,
                        data->master_cfg.num_threads);

   close(random_val_fd);
}

CTEST2(large_inserts_bugs_stress, test_random_keys_random_values_threaded)
{
   int random_key_fd = open("/dev/urandom", O_RDONLY);
   ASSERT_TRUE(random_key_fd > 0);

   int random_val_fd = open("/dev/urandom", O_RDONLY);
   ASSERT_TRUE(random_val_fd > 0);

   // Run n-threads with sequential key and sequential values inserted
   do_inserts_n_threads(data->kvsb,
                        &data->master_cfg,
                        data->hid,
                        random_key_fd,
                        random_val_fd,
                        data->num_inserts,
                        data->master_cfg.num_threads);

   close(random_key_fd);
   close(random_val_fd);
}


static void
do_inserts_n_threads(splinterdb      *kvsb,
                     master_config   *master_cfg,
                     platform_heap_id hid,
                     int              random_key_fd,
                     int              random_val_fd,
                     uint64           num_inserts,
                     uint64           num_threads)
{
   worker_config *wcfg = TYPED_ARRAY_ZALLOC(hid, wcfg, num_threads);

   // Setup thread-specific insert parameters
   for (int ictr = 0; ictr < num_threads; ictr++) {
      wcfg[ictr].kvsb          = kvsb;
      wcfg[ictr].master_cfg    = master_cfg;
      wcfg[ictr].num_inserts   = num_inserts;
      wcfg[ictr].start_value   = (wcfg[ictr].num_inserts * ictr);
      wcfg[ictr].random_key_fd = random_key_fd;
      wcfg[ictr].random_val_fd = random_val_fd;
      wcfg[ictr].is_thread     = TRUE;
   }

   platform_thread *thread_ids =
      TYPED_ARRAY_ZALLOC(hid, thread_ids, num_threads);

   // Fire-off the threads to drive inserts ...
   for (int tctr = 0; tctr < num_threads; tctr++) {
      int rc = pthread_create(
         &thread_ids[tctr], NULL, &exec_worker_thread, &wcfg[tctr]);
      ASSERT_EQUAL(0, rc);
   }

   // Wait for all threads to complete ...
   for (int tctr = 0; tctr < num_threads; tctr++) {
      void *thread_rc;
      int   rc = pthread_join(thread_ids[tctr], &thread_rc);
      ASSERT_EQUAL(0, rc);
      if (thread_rc != 0) {
         fprintf(stderr,
                 "Thread %d [ID=%lu] had error: %p\n",
                 tctr,
                 thread_ids[tctr],
                 thread_rc);
         ASSERT_TRUE(FALSE);
      }
   }
   platform_free(hid, thread_ids);
   platform_free(hid, wcfg);
}

/*
 * exec_worker_thread() - Thread-specific insert work-horse function.
 *
 * Each thread inserts 'num_inserts' KV-pairs from a 'start_value' ID.
 * All inserts are sequential.
 */
static void *
exec_worker_thread(void *w)
{
   char key_data[TEST_KEY_SIZE];
   char val_data[TEST_VALUE_SIZE];

   worker_config *wcfg = (worker_config *)w;

   splinterdb *kvsb          = wcfg->kvsb;
   uint64      start_key     = wcfg->start_value;
   uint64      num_inserts   = wcfg->num_inserts;
   int         random_key_fd = wcfg->random_key_fd;
   int         random_val_fd = wcfg->random_val_fd;

   uint64 start_time = platform_get_timestamp();

   if (wcfg->is_thread) {
      splinterdb_register_thread(kvsb);
   }

   threadid thread_idx = platform_get_tid();

   // Test is written to insert multiples of millions per thread.
   ASSERT_EQUAL(0, (num_inserts % MILLION));

   platform_default_log("%s()::%d:Thread %-2lu inserts %lu (%lu million)"
                        ", %s key, %s value, "
                        "KV-pairs starting from %lu (%lu%s) ...\n",
                        __FUNCTION__,
                        __LINE__,
                        thread_idx,
                        num_inserts,
                        (num_inserts / MILLION),
                        (random_key_fd ? "random" : "sequential"),
                        (random_val_fd ? "random" : "sequential"),
                        start_key,
                        (start_key / MILLION),
                        (start_key ? " million" : ""));

   uint64 ictr = 0;
   uint64 jctr = 0;

   bool verbose_progress = wcfg->master_cfg->verbose_progress;

   for (ictr = 0; ictr < (num_inserts / MILLION); ictr++) {
      for (jctr = 0; jctr < MILLION; jctr++) {

         uint64 id = (start_key + (ictr * MILLION) + jctr);
         uint64 key_len;
         uint64 val_len;

         // Generate random key / value if calling test-case requests it.
         if (random_key_fd) {
            size_t result = read(random_key_fd, key_data, sizeof(key_data));
            ASSERT_TRUE(result >= 0);

            key_len = result;
         } else {
            // Generate sequential key data
            snprintf(key_data, sizeof(key_data), "%lu", id);
            key_len = strlen(key_data);
         }

         if (random_val_fd) {
            size_t result = read(random_val_fd, val_data, sizeof(val_data));
            ASSERT_TRUE(result >= 0);

            val_len = result;
         } else {
            // Generate sequential value data
            snprintf(val_data, sizeof(val_data), "Row-%lu", id);
            val_len = strlen(val_data);
         }

         slice key = slice_create(key_len, key_data);
         slice val = slice_create(val_len, val_data);

         int rc = splinterdb_insert(kvsb, key, val);
         ASSERT_EQUAL(0, rc);
      }
      if (verbose_progress) {
         platform_default_log(
            "%s()::%d:Thread-%lu Inserted %lu million KV-pairs ...\n",
            __FUNCTION__,
            __LINE__,
            thread_idx,
            (ictr + 1));
      }
   }
   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);

   platform_default_log("%s()::%d:Thread-%lu Inserted %lu million KV-pairs in "
                        "%lu s, %lu rows/s\n",
                        __FUNCTION__,
                        __LINE__,
                        thread_idx,
                        ictr, // outer-loop ends at #-of-Millions inserted
                        NSEC_TO_SEC(elapsed_ns),
                        (num_inserts / NSEC_TO_SEC(elapsed_ns)));

   if (wcfg->is_thread) {
      splinterdb_deregister_thread(kvsb);
   }

   return 0;
}
