// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * large_inserts_stress_test.c --
 *
 * This test exercises simple very large #s of inserts which have found to
 * trigger some bugs in some code paths. This is just a miscellaneous collection
 * of test cases for different issues reported.
 * -----------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <string.h>
#include <sys/wait.h>

#include "platform.h"
#include "splinterdb/public_platform.h"
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "config.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

// Nothing particularly significant about these constants.
#define TEST_KEY_SIZE   30
#define TEST_VALUE_SIZE 256

/*
 * Configuration for each worker thread. See the selection of 'fd'-semantics
 * as implemented in exec_worker_thread(), to select diff types of key/value's
 * data distribution during inserts.
 */
typedef struct {
   splinterdb    *kvsb;
   master_config *master_cfg;
   uint64         start_value;
   uint64         num_inserts;
   uint64         num_insert_threads;
   int            random_key_fd; // Options to choose the type of key inserted
   int            random_val_fd; // Options to choose the type of value inserted
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
                     uint64           num_insert_threads);

// Run n-threads concurrently inserting many KV-pairs
#define NUM_THREADS 8

/*
 * Some test-cases can drive multiple threads to use either the same start
 * value for all threads. Or, each thread will use its own start value so
 * that all threads are inserting in non-intersecting bands of keys.
 * These mnemonics control these behaviours.
 */
#define TEST_INSERTS_SEQ_KEY_DIFF_START_KEYID_FD ((int)0)
#define TEST_INSERTS_SEQ_KEY_SAME_START_KEYID_FD ((int)-1)

/* Drive inserts to generate sequential short-length values */
#define TEST_INSERT_SEQ_VALUES_FD ((int)0)

/*
 * Some test-cases drive inserts to choose a fully-packed value of size
 * TEST_VALUE_SIZE bytes. This variation has been seen to trigger some
 * assertions.
 */
#define TEST_INSERT_FULLY_PACKED_CONSTANT_VALUE_FD (int)-1

/*
 * Global data declaration macro:
 */
CTEST_DATA(large_inserts_stress)
{
   // Declare heap handles for on-stack buffer allocations
   platform_heap_id hid;

   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_config;
   master_config     master_cfg;
   uint64            num_inserts; // per main() process or per thread
   uint64            num_insert_threads;
   int               this_pid;
   bool              am_parent;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(large_inserts_stress)
{
   // First, register that main() is being run as a parent process
   data->am_parent = TRUE;
   data->this_pid  = platform_getpid();

   platform_status rc;
   uint64          heap_capacity = (64 * MiB); // small heap is sufficient.

   config_set_defaults(&data->master_cfg);

   // Expected args to parse --num-inserts, --use-shmem, --verbose-progress.
   rc = config_parse(&data->master_cfg, 1, Ctest_argc, (char **)Ctest_argv);
   ASSERT_TRUE(SUCCESS(rc));

   // Create a heap for allocating on-stack buffers for various arrays.
   rc = platform_heap_create(platform_get_module_id(),
                             heap_capacity,
                             data->master_cfg.use_shmem,
                             &data->hid);
   platform_assert_status_ok(rc);

   data->cfg = (splinterdb_config){.filename   = TEST_DB_NAME,
                                   .io_flags   = data->master_cfg.io_flags,
                                   .cache_size = 1 * Giga,
                                   .disk_size  = 40 * Giga,
                                   .use_shmem  = data->master_cfg.use_shmem,
                                   .shmem_size = (4 * GiB),
                                   .data_cfg   = &data->default_data_config};

   data->num_inserts =
      (data->master_cfg.num_inserts ? data->master_cfg.num_inserts
                                    : (1 * MILLION));
   data->num_insert_threads = NUM_THREADS;

   if ((data->num_inserts % MILLION) != 0) {
      platform_error_log("Test expects --num-inserts parameter to be an"
                         " integral multiple of a million.\n");
      ASSERT_EQUAL(0, (data->num_inserts % MILLION));
      return;
   }

   // Run with higher configured shared memory, if specified
   if (data->master_cfg.shmem_size > data->cfg.shmem_size) {
      data->cfg.shmem_size = data->master_cfg.shmem_size;
   }
   // Setup Splinter's background thread config, if specified
   data->cfg.num_memtable_bg_threads = data->master_cfg.num_memtable_bg_threads;
   data->cfg.num_normal_bg_threads   = data->master_cfg.num_normal_bg_threads;


   size_t max_key_size = TEST_KEY_SIZE;
   default_data_config_init(max_key_size, data->cfg.data_cfg);

   int rv = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rv);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(large_inserts_stress)
{
   // Only parent process should tear down Splinter.
   if (data->am_parent) {
      splinterdb_close(&data->kvsb);
      platform_heap_destroy(&data->hid);
   }
}

/*
 * Test case that inserts large # of KV-pairs, and goes into a code path
 * reported by issue# 458, tripping a debug assert.
 */
CTEST2_SKIP(large_inserts_stress,
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
CTEST2(large_inserts_stress, test_seq_key_seq_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb        = data->kvsb;
   wcfg.master_cfg  = &data->master_cfg;
   wcfg.num_inserts = data->num_inserts;

   exec_worker_thread(&wcfg);
}

CTEST2(large_inserts_stress, test_random_key_seq_values_inserts)
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

CTEST2(large_inserts_stress, test_seq_key_random_values_inserts)
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

CTEST2(large_inserts_stress, test_random_key_random_values_inserts)
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

static void
safe_wait()
{
   int wstatus;
   int wr = wait(&wstatus);
   platform_assert(wr != -1, "wait failure: %s", strerror(errno));
   platform_assert(WIFEXITED(wstatus),
                   "Child terminated abnormally: SIGNAL=%d",
                   WIFSIGNALED(wstatus) ? WTERMSIG(wstatus) : 0);
   platform_assert(WEXITSTATUS(wstatus) == 0);
}

/*
 * ----------------------------------------------------------------------------
 * test_seq_key_seq_values_inserts_forked() --
 *
 * Test case is identical to test_seq_key_seq_values_inserts() but the
 * actual execution of the function that does inserts is done from
 * a forked-child process. This test, therefore, does basic validation
 * that from a forked-child process we can drive basic SplinterDB commands.
 * And then the parent can resume after the child exits, and can cleanly
 * shutdown the instance.
 * ----------------------------------------------------------------------------
 */
CTEST2(large_inserts_stress, test_seq_key_seq_values_inserts_forked)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb        = data->kvsb;
   wcfg.master_cfg  = &data->master_cfg;
   wcfg.num_inserts = data->num_inserts;

   int pid = platform_getpid();

   if (wcfg.master_cfg->fork_child) {
      pid = fork();

      if (pid < 0) {
         platform_error_log("fork() of child process failed: pid=%d\n", pid);
         return;
      } else if (pid) {
         platform_default_log("OS-pid=%d, Thread-ID=%lu: "
                              "Waiting for child pid=%d to complete ...\n",
                              platform_getpid(),
                              platform_get_tid(),
                              pid);

         safe_wait();

         platform_default_log("Thread-ID=%lu, OS-pid=%d: "
                              "Child execution wait() completed."
                              " Resuming parent ...\n",
                              platform_get_tid(),
                              platform_getpid());
      }
   }
   if (pid == 0) {
      // Record in global data that we are now running as a child.
      data->am_parent = FALSE;
      data->this_pid  = platform_getpid();

      platform_default_log(
         "OS-pid=%d Running as %s process ...\n",
         data->this_pid,
         (wcfg.master_cfg->fork_child ? "forked child" : "parent"));

      splinterdb_register_thread(wcfg.kvsb);

      exec_worker_thread(&wcfg);

      platform_default_log("OS-pid=%d, Thread-ID=%lu, Child process"
                           ", completed inserts.\n",
                           data->this_pid,
                           platform_get_tid());
      splinterdb_deregister_thread(wcfg.kvsb);
      exit(0);
      return;
   }
}

/*
 * ----------------------------------------------------------------------------
 * Collection of test cases that fire-up diff combinations of inserts
 * (sequential, random keys & values) executed by n-threads.
 * ----------------------------------------------------------------------------
 */
/*
 * Test case that fires up many threads each concurrently inserting large # of
 * KV-pairs, with discrete ranges of keys inserted by each thread.
 * RESOLVE: This hangs in this flow; never completes ...
 * clockcache_try_get_read() -> memtable_maybe_rotate_and_get_insert_lock()
 * This problem will probably occur in /main as well.
 */
CTEST2_SKIP(large_inserts_stress, test_seq_key_seq_values_inserts_threaded)
{
   // Run n-threads with sequential key and sequential values inserted
   do_inserts_n_threads(data->kvsb,
                        &data->master_cfg,
                        data->hid,
                        TEST_INSERTS_SEQ_KEY_DIFF_START_KEYID_FD,
                        TEST_INSERT_SEQ_VALUES_FD,
                        data->num_inserts,
                        data->num_insert_threads);
}

/*
 * Test case that fires up many threads each concurrently inserting large # of
 * KV-pairs, with all threads inserting from same start-value.
 *
 * With --num-threads 63, hangs in
 *  clockcache_get_read() -> memtable_maybe_rotate_and_get_insert_lock()
 */
CTEST2(large_inserts_stress,
       test_seq_key_seq_values_inserts_threaded_same_start_keyid)
{
   // Run n-threads with sequential key and sequential values inserted
   do_inserts_n_threads(data->kvsb,
                        &data->master_cfg,
                        data->hid,
                        TEST_INSERTS_SEQ_KEY_SAME_START_KEYID_FD,
                        TEST_INSERT_SEQ_VALUES_FD,
                        data->num_inserts,
                        data->num_insert_threads);
}

/*
 * Test case that fires up many threads each concurrently inserting large # of
 * KV-pairs, with all threads inserting from same start-value, using a fixed
 * fully-packed value.
 */
CTEST2(large_inserts_stress,
       test_seq_key_fully_packed_value_inserts_threaded_same_start_keyid)
{
   // Run n-threads with sequential key and sequential values inserted
   do_inserts_n_threads(data->kvsb,
                        &data->master_cfg,
                        data->hid,
                        TEST_INSERTS_SEQ_KEY_SAME_START_KEYID_FD,
                        TEST_INSERT_FULLY_PACKED_CONSTANT_VALUE_FD,
                        data->num_inserts,
                        data->num_insert_threads);
}

CTEST2(large_inserts_stress, test_random_keys_seq_values_threaded)
{
   int random_key_fd = open("/dev/urandom", O_RDONLY);
   ASSERT_TRUE(random_key_fd > 0);

   // Run n-threads with sequential key and sequential values inserted
   do_inserts_n_threads(data->kvsb,
                        &data->master_cfg,
                        data->hid,
                        random_key_fd,
                        TEST_INSERT_SEQ_VALUES_FD,
                        data->num_inserts,
                        data->num_insert_threads);

   close(random_key_fd);
}

CTEST2(large_inserts_stress, test_seq_keys_random_values_threaded)
{
   int random_val_fd = open("/dev/urandom", O_RDONLY);
   ASSERT_TRUE(random_val_fd > 0);

   // Run n-threads with sequential key and sequential values inserted
   do_inserts_n_threads(data->kvsb,
                        &data->master_cfg,
                        data->hid,
                        TEST_INSERTS_SEQ_KEY_DIFF_START_KEYID_FD,
                        random_val_fd,
                        data->num_inserts,
                        data->num_insert_threads);

   close(random_val_fd);
}

CTEST2(large_inserts_stress,
       test_seq_keys_random_values_threaded_same_start_keyid)
{
   int random_val_fd = open("/dev/urandom", O_RDONLY);
   ASSERT_TRUE(random_val_fd > 0);

   // Run n-threads with sequential key and sequential values inserted
   do_inserts_n_threads(data->kvsb,
                        &data->master_cfg,
                        data->hid,
                        TEST_INSERTS_SEQ_KEY_SAME_START_KEYID_FD,
                        random_val_fd,
                        data->num_inserts,
                        data->num_insert_threads);

   close(random_val_fd);
}

CTEST2(large_inserts_stress, test_random_keys_random_values_threaded)
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
                        data->num_insert_threads);

   close(random_key_fd);
   close(random_val_fd);
}

/*
 * ----------------------------------------------------------------------------
 * do_inserts_n_threads() - Driver function that will fire-up n-threads to
 * perform different forms of inserts run by all the threads. The things we
 * control via parameters are:
 *
 * Parameters:
 * - random_key_fd      - Sequential / random key
 * - random_val_fd      - Sequential / random value / fully-packed value.
 * - num_inserts        - # of inserts / thread
 * - num_insert_threads - # of inserting threads to start-up
 * - same_start_value   - Boolean to control inserted batch' start-value.
 *
 * NOTE: Semantics of random_key_fd:
 *
 *  fd == 0: => Each thread will insert into its own assigned space of
 *              {start-value, num-inserts} range. The concurrent inserts are all
 *              unique non-conflicting keys.
 *
 *  fd  > 0: => Each thread will insert num_inserts rows with randomly generated
 *              keys, usually fully-packed to TEST_KEY_SIZE.
 *
 *  fd  < 0: => Each thread will insert num_inserts rows all starting at the
 *              same start value; chosen as 0.
 *              This is a lapsed case to exercise heavy inserts of duplicate
 *              keys, creating diff BTree split dynamics.
 *
 * NOTE: Semantics of random_val_fd:
 *
 * You can use this to control the type of value that will be generated:
 *  fd == 0: Use sequential small-length values.
 *  fd == 1: Use randomly generated values, fully-packed to TEST_VALUE_SIZE.
 * ----------------------------------------------------------------------------
 */
static void
do_inserts_n_threads(splinterdb      *kvsb,
                     master_config   *master_cfg,
                     platform_heap_id hid,
                     int              random_key_fd,
                     int              random_val_fd,
                     uint64           num_inserts,
                     uint64           num_insert_threads)
{
   worker_config *wcfg = TYPED_ARRAY_ZALLOC(hid, wcfg, num_insert_threads);

   // Setup thread-specific insert parameters
   for (int ictr = 0; ictr < num_insert_threads; ictr++) {
      wcfg[ictr].kvsb        = kvsb;
      wcfg[ictr].master_cfg  = master_cfg;
      wcfg[ictr].num_inserts = num_inserts;

      // Choose the same or diff start key-ID for each thread.
      wcfg[ictr].start_value =
         ((random_key_fd < 0) ? 0 : (wcfg[ictr].num_inserts * ictr));
      wcfg[ictr].random_key_fd = random_key_fd;
      wcfg[ictr].random_val_fd = random_val_fd;
      wcfg[ictr].is_thread     = TRUE;
   }

   platform_thread *thread_ids =
      TYPED_ARRAY_ZALLOC(hid, thread_ids, num_insert_threads);

   // Fire-off the threads to drive inserts ...
   for (int tctr = 0; tctr < num_insert_threads; tctr++) {
      int rc = pthread_create(
         &thread_ids[tctr], NULL, &exec_worker_thread, &wcfg[tctr]);
      ASSERT_EQUAL(0, rc);
   }

   // Wait for all threads to complete ...
   for (int tctr = 0; tctr < num_insert_threads; tctr++) {
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
 * ----------------------------------------------------------------------------
 * exec_worker_thread() - Thread-specific insert work-horse function.
 *
 * Each thread inserts 'num_inserts' KV-pairs from a 'start_value' ID.
 * Nature of the inserts is controlled by wcfg config parameters. Caller can
 * choose between sequential / random keys and/or sequential / random values
 * to be inserted. Can also choose whether value will be fully-packed.
 * ----------------------------------------------------------------------------
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

   const char *random_val_descr = NULL;
   random_val_descr             = ((random_val_fd > 0)    ? "random"
                                   : (random_val_fd == 0) ? "sequential"
                                                          : "fully-packed constant");

   platform_default_log("%s()::%d:Thread %-2lu inserts %lu (%lu million)"
                        ", %s key, %s value, "
                        "KV-pairs starting from %lu (%lu%s) ...\n",
                        __func__,
                        __LINE__,
                        thread_idx,
                        num_inserts,
                        (num_inserts / MILLION),
                        ((random_key_fd > 0) ? "random" : "sequential"),
                        random_val_descr,
                        start_key,
                        (start_key / MILLION),
                        (start_key ? " million" : ""));

   uint64 ictr = 0;
   uint64 jctr = 0;

   bool verbose_progress = wcfg->master_cfg->verbose_progress;

   // Insert fully-packed wider-values so we fill pages faster.
   // This value-data will be chosen when random_key_fd < 0.
   memset(val_data, 'V', sizeof(val_data));
   uint64 val_len = sizeof(val_data);

   bool val_length_msg_printed = FALSE;

   for (ictr = 0; ictr < (num_inserts / MILLION); ictr++) {
      for (jctr = 0; jctr < MILLION; jctr++) {

         uint64 id = (start_key + (ictr * MILLION) + jctr);
         uint64 key_len;

         // Generate random key / value if calling test-case requests it.
         if (random_key_fd > 0) {

            // Generate random key-data for full width of key.
            size_t result = read(random_key_fd, key_data, sizeof(key_data));
            ASSERT_TRUE(result >= 0);

            key_len = result;
         } else {
            // Generate sequential key data
            snprintf(key_data, sizeof(key_data), "%lu", id);
            key_len = strlen(key_data);
         }

         // Manage how the value-data is generated based on random_val_fd
         if (random_val_fd > 0) {

            // Generate random value for full width of value.
            size_t result = read(random_val_fd, val_data, sizeof(val_data));
            ASSERT_TRUE(result >= 0);

            val_len = result;
            if (!val_length_msg_printed) {
               platform_default_log("OS-pid=%d, Thread-ID=%lu"
                                    ", Insert random value of "
                                    "fixed-length=%lu bytes.\n",
                                    platform_getpid(),
                                    thread_idx,
                                    val_len);
               val_length_msg_printed = TRUE;
            }
         } else if (random_val_fd == 0) {
            // Generate small-length sequential value data
            snprintf(val_data, sizeof(val_data), "Row-%lu", id);
            val_len = strlen(val_data);

            if (!val_length_msg_printed) {
               platform_default_log("OS-pid=%d, Thread-ID=%lu"
                                    ", Insert small-width sequential values of "
                                    "different lengths.\n",
                                    platform_getpid(),
                                    thread_idx);
               val_length_msg_printed = TRUE;
            }
         } else if (random_val_fd < 0) {
            if (!val_length_msg_printed) {
               platform_default_log("OS-pid=%d, Thread-ID=%lu"
                                    ", Insert fully-packed fixed value of "
                                    "length=%lu bytes.\n",
                                    platform_getpid(),
                                    thread_idx,
                                    val_len);
               val_length_msg_printed = TRUE;
            }
         }

         slice key = slice_create(key_len, key_data);
         slice val = slice_create(val_len, val_data);

         int rc = splinterdb_insert(kvsb, key, val);
         ASSERT_EQUAL(0, rc);
      }
      if (verbose_progress) {
         platform_default_log(
            "%s()::%d:Thread-%lu Inserted %lu million KV-pairs ...\n",
            __func__,
            __LINE__,
            thread_idx,
            (ictr + 1));
      }
   }
   // Deal with low ns-elapsed times when inserting small #s of rows
   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
   uint64 elapsed_s  = NSEC_TO_SEC(elapsed_ns);
   if (elapsed_s == 0) {
      elapsed_s = 1;
   }

   platform_default_log("%s()::%d:Thread-%lu Inserted %lu million KV-pairs in "
                        "%lu s, %lu rows/s\n",
                        __func__,
                        __LINE__,
                        thread_idx,
                        ictr, // outer-loop ends at #-of-Millions inserted
                        elapsed_s,
                        (num_inserts / elapsed_s));

   if (wcfg->is_thread) {
      splinterdb_deregister_thread(kvsb);
   }

   return 0;
}
