// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * large_inserts_stress_test.c --
 *
 * This test exercises simple very large #s of inserts which have found to
 * trigger some bugs in some code paths. This is just a miscellaneous collection
 * of test cases for different issues reported / encountered over time.
 * -----------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <unistd.h>
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
#include "test_splinterdb_apis.h"

// Nothing particularly significant about these constants.
#define TEST_KEY_SIZE   30
#define TEST_VALUE_SIZE 32

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
   uint64         num_threads;
   int            random_key_fd; // Options to choose the type of key inserted
   int            random_val_fd; // Options to choose the type of value inserted
   bool           is_thread;     // Is main() or thread executing worker fn
} worker_config;

/*
 * RESOLVE: FIXME - This overloading of 'fd' to pass-down semantics to
 * what type of key/value distributions to use -- is 'workable' but
 * error prone. Need a diff arg to manage these test cases.
 */
/*
 * random_key_fd types to select how key's data is inserted
 */
// Randomly generated key, inserted in big-endian 32-bit order.
// This facilitates lookup using lexcmp()
#define RANDOM_KEY_BIG_ENDIAN_32_FD 2

// Randomly generated key, inserted in host-endian order
#define RANDOM_KEY_HOST_ENDIAN_FD 1

// Sequentially generated key, inserted in host-endian order
#define SEQ_KEY_HOST_ENDIAN_FD 0

// Sequentially generated key, inserted in big-endian 32-bit order.
// This facilitates lookup using lexcmp()
#define SEQ_KEY_BIG_ENDIAN_32_FD -2

/*
 * random_val_fd types to select how value's data is generated
 */
// Small value, sequentially generated based on key-ID is stored.
#define SEQ_VAL_SMALL_LENGTH_FD 0

// Random value generated, exactly of 6 bytes. This case is used to simulate
// data insertions into SplinterDB for Postgres-integration, where we store
// the 6-byte tuple-ID (TID) as the value.
#define RANDOM_VAL_FIXED_LEN_FD 6

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
CTEST_SETUP(large_inserts_stress)
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

   data->cfg =
      (splinterdb_config){.filename = "splinterdb_large_inserts_stress_test_db",
                          .cache_size = 4 * Giga,
                          .disk_size  = 40 * Giga,
                          .use_shmem  = use_shmem,
                          .shmem_size = (1 * GiB),
                          .data_cfg   = &data->default_data_config};

   ZERO_STRUCT(data->master_cfg);
   config_set_defaults(&data->master_cfg);

   // Expected args to parse --num-inserts, --num-threads, --verbose-progress.
   rc = config_parse(&data->master_cfg, 1, argc, (char **)argv);
   ASSERT_TRUE(SUCCESS(rc));

   data->num_inserts =
      (data->master_cfg.num_inserts ? data->master_cfg.num_inserts
                                    : (2 * MILLION));

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

   // Run with higher configured shared memory, if specified
   if (data->master_cfg.shmem_size > data->cfg.shmem_size) {
      data->cfg.shmem_size = data->master_cfg.shmem_size;
   }
   // Setup Splinter's background thread config, if specified
   data->cfg.num_memtable_bg_threads = data->master_cfg.num_memtable_bg_threads;
   data->cfg.num_normal_bg_threads   = data->master_cfg.num_normal_bg_threads;
   data->cfg.use_stats               = data->master_cfg.use_stats;

   size_t max_key_size = TEST_KEY_SIZE;
   default_data_config_init(max_key_size, data->cfg.data_cfg);

   platform_enable_tracing_large_frags();

   int rv = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rv);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(large_inserts_stress)
{
   // Only parent process should tear down Splinter.
   if (data->am_parent) {
      int rv = splinterdb_close(&data->kvsb);
      ASSERT_EQUAL(0, rv);

      platform_disable_tracing_large_frags();
      platform_status rc = platform_heap_destroy(&data->hh);
      ASSERT_TRUE(SUCCESS(rc));
   }
}

/*
 * Test case that inserts large # of KV-pairs, and goes into a code path
 * reported by issue# 458, tripping a debug assert. This test case also
 * triggered the failure(s) reported by issue # 545.
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

      uint64 elapsed_s      = NSEC_TO_SEC(elapsed_ns);
      uint64 test_elapsed_s = NSEC_TO_SEC(test_elapsed_ns);

      elapsed_s      = ((elapsed_s > 0) ? elapsed_s : 1);
      test_elapsed_s = ((test_elapsed_s > 0) ? test_elapsed_s : 1);

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
   wcfg.kvsb          = data->kvsb;
   wcfg.master_cfg    = &data->master_cfg;
   wcfg.num_inserts   = data->num_inserts;
   wcfg.random_key_fd = SEQ_KEY_HOST_ENDIAN_FD;

   exec_worker_thread(&wcfg);
}

/*
 * Fails due to assertion failure as reported in issue #560.
 */
CTEST2_SKIP(large_inserts_stress,
            test_seq_htobe32_key_random_6byte_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb          = data->kvsb;
   wcfg.master_cfg    = &data->master_cfg;
   wcfg.num_inserts   = data->num_inserts;
   wcfg.random_key_fd = SEQ_KEY_BIG_ENDIAN_32_FD;
   wcfg.random_val_fd = RANDOM_VAL_FIXED_LEN_FD;

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

   int pid = getpid();

   if (wcfg.master_cfg->fork_child) {
      pid = fork();

      if (pid < 0) {
         platform_error_log("fork() of child process failed: pid=%d\n", pid);
         return;
      } else if (pid) {
         CTEST_LOG_INFO("OS-pid=%d, Thread-ID=%lu: "
                        "Waiting for child pid=%d to complete ...\n",
                        getpid(),
                        platform_get_tid(),
                        pid);

         safe_wait();

         CTEST_LOG_INFO("Thread-ID=%lu, OS-pid=%d: "
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

      CTEST_LOG_INFO("OS-pid=%d Running as %s process ...\n",
                     data->this_pid,
                     (wcfg.master_cfg->fork_child ? "forked child" : "parent"));

      splinterdb_register_thread(wcfg.kvsb);

      exec_worker_thread(&wcfg);

      CTEST_LOG_INFO("OS-pid=%d, Thread-ID=%lu, Child process"
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
 * FIXME: Runs into btree_pack(): req->num_tuples=6291457 exceeded output size
 * limit: req->max_tuples=6291456
 */
CTEST2(large_inserts_stress, test_seq_key_seq_values_inserts_threaded)
{
   // Run n-threads with sequential key and sequential values inserted
   do_inserts_n_threads(data->kvsb,
                        &data->master_cfg,
                        data->hid,
                        TEST_INSERTS_SEQ_KEY_DIFF_START_KEYID_FD,
                        TEST_INSERT_SEQ_VALUES_FD,
                        data->num_inserts,
                        data->master_cfg.num_threads);
}

/*
 * Test case that fires up many threads each concurrently inserting large # of
 * KV-pairs, with all threads inserting from same start-value.
 *
 * With --num-threads 63, hangs in
 *  clockcache_get_read() -> memtable_maybe_rotate_and_get_insert_lock()
 * FIXME: Runs into shmem OOM. (Should be fixed now by free-list mgmt.)
 * FIXME: Causes CI-timeout after 2h in debug-test runs.
 */
CTEST2_SKIP(large_inserts_stress,
            test_seq_key_seq_values_inserts_threaded_same_start_keyid)
{
   // Run n-threads with sequential key and sequential values inserted
   do_inserts_n_threads(data->kvsb,
                        &data->master_cfg,
                        data->hid,
                        TEST_INSERTS_SEQ_KEY_SAME_START_KEYID_FD,
                        TEST_INSERT_SEQ_VALUES_FD,
                        data->num_inserts,
                        data->master_cfg.num_threads);
}

/*
 * Test case that fires up many threads each concurrently inserting large # of
 * KV-pairs, with all threads inserting from same start-value, using a fixed
 * fully-packed value.
 * FIXME: Runs into shmem OOM. (Should be fixed now by free-list mgmt.)
 * FIXME: Causes CI-timeout after 2h in debug-test runs.
 */
CTEST2_SKIP(large_inserts_stress,
            test_seq_key_fully_packed_value_inserts_threaded_same_start_keyid)
{
   // Run n-threads with sequential key and sequential values inserted
   do_inserts_n_threads(data->kvsb,
                        &data->master_cfg,
                        data->hid,
                        TEST_INSERTS_SEQ_KEY_SAME_START_KEYID_FD,
                        TEST_INSERT_FULLY_PACKED_CONSTANT_VALUE_FD,
                        data->num_inserts,
                        data->master_cfg.num_threads);
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
                        data->master_cfg.num_threads);

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
                        data->master_cfg.num_threads);

   close(random_val_fd);
}

/*
 * FIXME: Runs into shmem OOM. (Should be fixed now by free-list mgmt.)
 * FIXME: Causes CI-timeout after 2h in debug-test runs.
 */
CTEST2_SKIP(large_inserts_stress,
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
                        data->master_cfg.num_threads);

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
                        data->master_cfg.num_threads);

   close(random_key_fd);
   close(random_val_fd);
}

/*
 * Test case developed to repro an out-of-bounds assertion tripped up in
 * trunk_build_filters() -> fingerprint_ntuples(). The fix has been id'ed
 * to relocate fingerprint_ntuples() in its flow. There was no real logic
 * error but a code-flow error. The now-fixed-bug would only repro with
 * something like --num-inserts 20M.
 */
CTEST2(large_inserts_stress,
       test_fp_num_tuples_out_of_bounds_bug_trunk_build_filters)
{
   char key_data[TEST_KEY_SIZE];
   char val_data[TEST_VALUE_SIZE];

   uint64 start_key = 0;

   uint64 start_time = platform_get_timestamp();

   threadid thread_idx = platform_get_tid();

   // Test is written to insert multiples of millions per thread.
   ASSERT_EQUAL(0, (data->num_inserts % MILLION));

   platform_default_log("%s()::%d:Thread-%-lu inserts %lu (%lu million)"
                        ", sequential key, sequential value, "
                        "KV-pairs starting from %lu ...\n",
                        __func__,
                        __LINE__,
                        thread_idx,
                        data->num_inserts,
                        (data->num_inserts / MILLION),
                        start_key);

   uint64 ictr = 0;
   uint64 jctr = 0;

   bool verbose_progress = TRUE;
   memset(val_data, 'V', sizeof(val_data));
   uint64 val_len = sizeof(val_data);

   for (ictr = 0; ictr < (data->num_inserts / MILLION); ictr++) {
      for (jctr = 0; jctr < MILLION; jctr++) {

         uint64 id = (start_key + (ictr * MILLION) + jctr);

         // Generate sequential key data
         snprintf(key_data, sizeof(key_data), "%lu", id);
         uint64 key_len = strlen(key_data);

         slice key = slice_create(key_len, key_data);
         slice val = slice_create(val_len, val_data);

         int rc = splinterdb_insert(data->kvsb, key, val);
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
                        (data->num_inserts / elapsed_s));
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
 * - num_threads        - # of threads to start-up
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
                     uint64           num_threads)
{
   platform_memfrag memfrag_wcfg;
   worker_config   *wcfg = TYPED_ARRAY_ZALLOC(hid, wcfg, num_threads);

   // Setup thread-specific insert parameters
   for (int ictr = 0; ictr < num_threads; ictr++) {
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

   platform_memfrag memfrag_thread_ids;
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
   platform_memfrag *mf = &memfrag_thread_ids;
   platform_free(hid, mf);
   mf = &memfrag_wcfg;
   platform_free(hid, mf);
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
   char   key_buf[TEST_KEY_SIZE];
   char  *key_data = key_buf;
   uint64 key_len;

   char val_data[TEST_VALUE_SIZE];

   worker_config *wcfg = (worker_config *)w;

   splinterdb *kvsb          = wcfg->kvsb;
   uint64      start_key     = wcfg->start_value;
   uint64      num_inserts   = wcfg->num_inserts;
   int         random_key_fd = wcfg->random_key_fd;
   int         random_val_fd = wcfg->random_val_fd;

   int32 key_data_be; // int-32 keys generated in big-endian-32 notation
   if (random_key_fd == SEQ_KEY_BIG_ENDIAN_32_FD) {
      key_data = (char *)&key_data_be;
      key_len  = sizeof(key_data_be);
   }

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

   CTEST_LOG_INFO("%s()::%d:Thread %-2lu inserts %lu (%lu million)"
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

   // Initialize on-stack buffer to avoid MSAN failures
   memset(key_buf, 'X', sizeof(key_buf));

   // Insert fully-packed wider-values so we fill pages faster.
   // This value-data will be chosen when random_key_fd < 0.
   memset(val_data, 'V', sizeof(val_data));
   uint64 val_len = sizeof(val_data);

   bool val_length_msg_printed = FALSE;

   for (ictr = 0; ictr < (num_inserts / MILLION); ictr++) {
      for (jctr = 0; jctr < MILLION; jctr++) {

         uint64 id = (start_key + (ictr * MILLION) + jctr);

         // Generate random key / value if calling test-case requests it.
         if (random_key_fd > 0) {

            // Generate random key-data for full width of key.
            size_t result = read(random_key_fd, key_buf, sizeof(key_buf));
            ASSERT_TRUE(result >= 0);

            key_len = result;
         } else if (random_key_fd == SEQ_KEY_HOST_ENDIAN_FD) {
            // Generate sequential key data, stored in host-endian order
            snprintf(key_buf, sizeof(key_buf), "%lu", id);
            key_len = strlen(key_buf);
         } else if (random_key_fd == SEQ_KEY_BIG_ENDIAN_32_FD) {
            // Generate sequential key data, stored in big-endian order
            key_data_be = htobe32(id);
         }

         // Manage how the value-data is generated based on random_val_fd
         if (random_val_fd > 0) {

            // Generate random value choosing the width of value generated.
            val_len = ((random_val_fd == RANDOM_VAL_FIXED_LEN_FD)
                          ? RANDOM_VAL_FIXED_LEN_FD
                          : sizeof(val_data));

            size_t result = read(random_val_fd, val_data, val_len);
            ASSERT_TRUE(result >= 0);

            if (!val_length_msg_printed) {
               CTEST_LOG_INFO("OS-pid=%d, Thread-ID=%lu"
                              ", Insert random value of "
                              "fixed-length=%lu bytes.\n",
                              getpid(),
                              thread_idx,
                              val_len);
               val_length_msg_printed = TRUE;
            }
         } else if (random_val_fd == SEQ_VAL_SMALL_LENGTH_FD) {
            // Generate small-length sequential value data
            snprintf(val_data, sizeof(val_data), "Row-%lu", id);
            val_len = strlen(val_data);

            if (!val_length_msg_printed) {
               CTEST_LOG_INFO("OS-pid=%d, Thread-ID=%lu"
                              ", Insert small-width sequential values of "
                              "different lengths.\n",
                              getpid(),
                              thread_idx);
               val_length_msg_printed = TRUE;
            }
         } else if (random_val_fd < 0) {
            if (!val_length_msg_printed) {
               CTEST_LOG_INFO("OS-pid=%d, Thread-ID=%lu"
                              ", Insert fully-packed fixed value of "
                              "length=%lu bytes.\n",
                              getpid(),
                              thread_idx,
                              val_len);
               val_length_msg_printed = TRUE;
            }
         }

         key_len   = 4;
         slice key = slice_create(key_len, key_data);
         val_len   = sizeof(val_data);
         slice val = slice_create(val_len, val_data);

         int rc = splinterdb_insert(kvsb, key, val);
         ASSERT_EQUAL(0, rc);
      }
      if (verbose_progress) {
         CTEST_LOG_INFO(
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

   CTEST_LOG_INFO("%s()::%d:Thread-%lu Inserted %lu million KV-pairs in "
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
