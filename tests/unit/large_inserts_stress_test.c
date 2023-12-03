// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * large_inserts_stress_test.c --
 *
 * This test exercises simple very large #s of inserts which have found to
 * trigger some bugs in some code paths. This is just a miscellaneous collection
 * of test cases for different issues reported / encountered over time.
 *
 * Single-client test cases:
 *  Different strategies of loading key-data and value-data are defined as
 *  tokens in key_strategy and val_strategy enums. These tests exercise
 *  different pairs of these strategies for a single-client. The driving
 *  function is exec_worker_thread() which receives the test-case parameters
 *  via worker_config{} structure.
 *
 * Multiple-threads test cases:
 *  Similar to the single-client test cases, except that we now run through
 *  various combinations of key-data and value-data strategies across multiple
 *  threads. Few variations of tests that start from the same start key-ID
 * across all threads are added to exercise the logic of maintaining the BTrees
 * across tons of duplicate key insertions.
 *
 * Test-case with forked process: test_Seq_key_be32_Seq_values_inserts_forked()
 *  Identical to test_Seq_key_be32_Seq_values_inserts() but the test is run in
 *  forked child process. Only one such scenario is exercised for forked
 *  processes.
 *
 * Regression fix test cases:
 *  test_issue_458_mini_destroy_unused_debug_assert
 *  test_fp_num_tuples_out_of_bounds_bug_trunk_build_filters
 *
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
#include "ctest.h" // This is required for all test-case files.
#include "splinterdb_tests_private.h"
#include "functional/random.h"

// Nothing particularly significant about these constants.
#define TEST_KEY_SIZE   30
#define TEST_VALUE_SIZE 32

/*
 * ----------------------------------------------------------------------------
 * Key-data test strategies:
 *
 * SEQ_KEY_BIG_ENDIAN_32 - Sequential int32 key-data in big-endian format.
 *
 * SEQ_KEY_HOST_ENDIAN_32 - Sequential int32 key-data in host-endian format.
 *
 * SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH - Sequential int32 key-data in
 *  host-endian format, packed-out with 'K' to the length of the key-data
 *  buffer. The sorted-ness exercises different tree management algorithms,
 *  while the padding bytes increases the key-size to trigger different tree
 *  management operations.
 *
 * RAND_KEY_RAND_LENGTH - Randomly generated random number of bytes of length
 *  within [1, key-data-buffer-size]. This is the most general use-case to
 *  exercise random key payloads of varying lengths.
 *
 * RAND_KEY_DATA_BUF_SIZE - Randomly generated key of length == key-data-buffer
 *  size.
 * ----------------------------------------------------------------------------
 */
// clang-format off
typedef enum {                              // Test-case
   SEQ_KEY_BIG_ENDIAN_32 = 1,               //  1
   SEQ_KEY_HOST_ENDIAN_32,                  //  2
   SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH,    //  3
   RAND_KEY_RAND_LENGTH,                    //  4
   RAND_KEY_DATA_BUF_SIZE,                  //  5
   SEQ_KEY_HOST_ENDIAN_32_SAME_START_ID,    //  6
   SEQ_KEY_BIG_ENDIAN_32_SAME_START_ID,     //  7
   NUM_KEY_DATA_STRATEGIES
} key_strategy;

// Key-data strategy names, indexed by key_strategy enum values.
const char *Key_strategy_names[] = {
     "Undefined key-data strategy"
   , "Sequential key, 32-bit big-endian"
   , "Sequential key, 32-bit host-endian"
   , "Sequential key, fully-packed to key-data buffer, 32-bit host-endian"
   , "Random key-data, random length"
   , "Random key-data, fully-packed to key-data buffer"
   , "Sequential key, 32-bit host-endian, same start-ID across all threads"
   , "Sequential key, 32-bit big-endian, same start-ID across all threads"
};

// clang-format on

// Ensure that the strategy name-lookup array is adequately sized.
_Static_assert(ARRAY_SIZE(Key_strategy_names) == NUM_KEY_DATA_STRATEGIES,
               "Lookup array Key_strategy_names[] is incorrectly sized for "
               "NUM_KEY_DATA_STRATEGIES");

#define Key_strategy_name(id)                                                  \
   ((((id) > 0) && ((id) < NUM_KEY_DATA_STRATEGIES))                           \
       ? Key_strategy_names[(id)]                                              \
       : Key_strategy_names[0])

/*
 * ----------------------------------------------------------------------------
 * Value-data test strategies:
 *
 * SEQ_VAL_SMALL - Generate sprintf("Row-%d")'ed small value, whose length will
 *  be few bytes
 *
 * SEQ_VAL_PADDED_LENGTH - Similarly sprintf()'ed value but padded-out to the
 *  length of the value-data buffer. This exercises large-values so we can
 *  fill-up pages more easily.
 *
 * RAND_VAL_RAND_LENGTH - Randomly generated random number of bytes of length
 *  within [1, value-data-buffer-size]. This is the most general use-case to
 *  exercise random message payloads of varying lengths.
 *
 * RAND_6BYTE_VAL - Randomly generated value 6-bytes length. (6 bytes is the
 * length of the payload when integrating SplinterDB with Postgres.
 * ----------------------------------------------------------------------------
 */
// clang-format off
typedef enum {            // Sub-case
   SEQ_VAL_SMALL = 1,     //  (a) 'Row-%d'
   SEQ_VAL_PADDED_LENGTH, //  (b) 'Row-%d' padded to value data buffer size
   RAND_VAL_RAND_LENGTH,  //  (c)
   RAND_6BYTE_VAL,        //  (d)
   NUM_VALUE_DATA_STRATEGIES
} val_strategy;

// Value-data strategy names, indexed by val_strategy enum values.
const char *Val_strategy_names[] = {
     "Undefined value-data strategy"
   , "Small length sequential value"
   , "Sequential value, fully-packed to value-data buffer"
   , "Random value, of random-length"
   , "Random value, 6-bytes length"
};

// clang-format on

// Ensure that the strategy name-lookup array is adequately sized.
_Static_assert(ARRAY_SIZE(Val_strategy_names) == NUM_VALUE_DATA_STRATEGIES,
               "Lookup array Key_strategy_names[] is incorrectly sized for "
               "NUM_VALUE_DATA_STRATEGIES");

#define Val_strategy_name(id)                                                  \
   ((((id) > 0) && ((id) < NUM_VALUE_DATA_STRATEGIES))                         \
       ? Val_strategy_names[(id)]                                              \
       : Val_strategy_names[0])

/*
 * Configuration for each worker thread. See the selection of 'fd'-semantics
 * as implemented in exec_worker_thread(), to select diff types of
 * key/value's data distribution during inserts.
 */
typedef struct worker_config {
   platform_heap_id hid;
   splinterdb      *kvsb;
   uint64           start_value;
   uint64           num_inserts;
   size_t           key_size; // --key-size test execution argument
   size_t           val_size; // --data-size test execution argument
   uint64           rand_seed;
   key_strategy     key_type;
   val_strategy     val_type;
   bool             is_thread;
   bool             fork_child;
   bool             verbose_progress;
   bool             show_strategy;
} worker_config;

// Function Prototypes
static void *
exec_worker_thread(void *w);

static void
do_inserts_n_threads(splinterdb      *kvsb,
                     platform_heap_id hid,
                     size_t           key_size,
                     size_t           val_size,
                     key_strategy     key_type,
                     val_strategy     val_type,
                     uint64           num_inserts,
                     uint64           num_insert_threads);

// Run n-threads concurrently inserting many KV-pairs
#define NUM_THREADS 8

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
   uint64            num_inserts; // per main() process or per thread
   uint64            num_insert_threads;
   size_t            key_size; // --key-size test execution argument
   size_t            val_size; // --data-size test execution argument
   uint64            rand_seed;
   int               this_pid;
   bool              verbose_progress;
   bool              am_parent;
   bool              key_val_sizes_printed;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(large_inserts_stress)
{
   master_config master_cfg = {0};
   // First, register that main() is being run as a parent process
   data->am_parent = TRUE;
   data->this_pid  = getpid();

   platform_status rc;
   config_set_defaults(&master_cfg);

   // Expected args to parse --num-inserts, --use-shmem, --verbose-progress.
   rc = config_parse(&master_cfg, 1, Ctest_argc, (char **)Ctest_argv);
   ASSERT_TRUE(SUCCESS(rc));

   data->cfg =
      (splinterdb_config){.filename = "splinterdb_large_inserts_stress_test_db",
                          .cache_size = 4 * GiB,
                          .disk_size  = 40 * GiB,
                          .use_shmem  = master_cfg.use_shmem,
                          .shmem_size = (1 * GiB),
                          .data_cfg   = &data->default_data_config};

   data->num_inserts =
      (master_cfg.num_inserts ? master_cfg.num_inserts : (2 * MILLION));

   // If num_threads is unspecified, use default for this test.
   data->num_insert_threads =
      (master_cfg.num_threads ? master_cfg.num_threads : NUM_THREADS);

   if ((data->num_inserts % MILLION) != 0) {
      size_t num_million = (data->num_inserts / MILLION);
      data->num_inserts  = (num_million * MILLION);
      CTEST_LOG_INFO("Test expects --num-inserts parameter to be an"
                     " integral multiple of a million."
                     " Reset --num-inserts to %lu million.\n",
                     num_million);
   }

   // Run with higher configured shared memory, if specified
   if (master_cfg.shmem_size > data->cfg.shmem_size) {
      data->cfg.shmem_size = master_cfg.shmem_size;
   }
   // Setup Splinter's background thread config, if specified
   data->cfg.num_memtable_bg_threads = master_cfg.num_memtable_bg_threads;
   data->cfg.num_normal_bg_threads   = master_cfg.num_normal_bg_threads;
   data->cfg.use_stats               = master_cfg.use_stats;

   data->key_size =
      (master_cfg.max_key_size ? master_cfg.max_key_size : TEST_KEY_SIZE);
   data->val_size =
      (master_cfg.message_size ? master_cfg.message_size : TEST_VALUE_SIZE);
   default_data_config_init(data->key_size, data->cfg.data_cfg);

   data->verbose_progress = master_cfg.verbose_progress;

   // platform_enable_tracing_large_frags();

   int rv = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rv);

   CTEST_LOG_INFO("... with key-size=%lu, value-size=%lu bytes\n",
                  data->key_size,
                  data->val_size);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(large_inserts_stress)
{
   // Only parent process should tear down Splinter.
   if (data->am_parent) {
      int rv = splinterdb_close(&data->kvsb);
      ASSERT_EQUAL(0, rv);

      platform_disable_tracing_large_frags();
      platform_status rc = platform_heap_destroy(&data->hid);
      ASSERT_TRUE(SUCCESS(rc));
   }
}

/*
 * Test case that inserts large # of KV-pairs, and goes into a code path
 * reported by issue# 458, tripping a debug assert. This test case also
 * triggered the failure(s) reported by issue # 545.
 */
// clang-format off
CTEST2_SKIP(large_inserts_stress, test_issue_458_mini_destroy_unused_debug_assert)
{
   // clang-format on
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
// Case 1(a) - SEQ_KEY_BIG_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_be32_Seq_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = SEQ_KEY_BIG_ENDIAN_32;
   wcfg.val_type         = SEQ_VAL_SMALL;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 1(b) - SEQ_KEY_BIG_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_be32_Seq_values_packed_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = SEQ_KEY_BIG_ENDIAN_32;
   wcfg.val_type         = SEQ_VAL_PADDED_LENGTH;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 1(c) - SEQ_KEY_BIG_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_be32_Rand_length_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = SEQ_KEY_BIG_ENDIAN_32;
   wcfg.val_type         = RAND_VAL_RAND_LENGTH;
   wcfg.rand_seed        = data->rand_seed;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

/*
 * Fails, sometimes, due to assertion failure as reported in issue #560.
 */
// Case 1(d) - SEQ_KEY_BIG_ENDIAN_32
// clang-format off
CTEST2(large_inserts_stress, test_Seq_key_be32_Rand_6byte_values_inserts)
// clang-format on
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = SEQ_KEY_BIG_ENDIAN_32;
   wcfg.val_type         = RAND_6BYTE_VAL;
   wcfg.rand_seed        = data->rand_seed;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 2(a) - SEQ_KEY_HOST_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_he32_Seq_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = SEQ_KEY_HOST_ENDIAN_32;
   wcfg.val_type         = SEQ_VAL_SMALL;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 2(b) - SEQ_KEY_HOST_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_he32_Seq_values_packed_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = SEQ_KEY_HOST_ENDIAN_32;
   wcfg.val_type         = SEQ_VAL_PADDED_LENGTH;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 2(c) - SEQ_KEY_HOST_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_he32_Rand_length_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = SEQ_KEY_HOST_ENDIAN_32;
   wcfg.val_type         = RAND_VAL_RAND_LENGTH;
   wcfg.rand_seed        = data->rand_seed;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

/*
 * Fails, sometimes, due to assertion failure as reported in issue #560.
 */
// Case 2(d) - SEQ_KEY_HOST_ENDIAN_32
// clang-format off
CTEST2(large_inserts_stress, test_Seq_key_he32_Rand_6byte_values_inserts)
// clang-format on
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = SEQ_KEY_HOST_ENDIAN_32;
   wcfg.val_type         = RAND_6BYTE_VAL;
   wcfg.rand_seed        = data->rand_seed;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 3(a) - SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH
CTEST2(large_inserts_stress, test_Seq_key_packed_he32_Seq_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH;
   wcfg.val_type         = SEQ_VAL_SMALL;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 3(b) - SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH
CTEST2(large_inserts_stress, test_Seq_key_packed_he32_Seq_values_packed_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH;
   wcfg.val_type         = SEQ_VAL_PADDED_LENGTH;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 3(c) - SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH
// clang-format off
CTEST2(large_inserts_stress, test_Seq_key_packed_he32_Rand_length_values_inserts)
// clang-format on
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH;
   wcfg.val_type         = RAND_VAL_RAND_LENGTH;
   wcfg.rand_seed        = data->rand_seed;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 4(a) - RAND_KEY_RAND_LENGTH
CTEST2(large_inserts_stress, test_Rand_key_Seq_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = RAND_KEY_RAND_LENGTH;
   wcfg.val_type         = SEQ_VAL_SMALL;
   wcfg.rand_seed        = data->rand_seed;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 4(b) - RAND_KEY_RAND_LENGTH
CTEST2(large_inserts_stress, test_Rand_key_Seq_values_packed_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = RAND_KEY_RAND_LENGTH;
   wcfg.val_type         = SEQ_VAL_PADDED_LENGTH;
   wcfg.rand_seed        = data->rand_seed;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 4(c) - RAND_KEY_RAND_LENGTH
CTEST2(large_inserts_stress, test_Rand_key_Rand_length_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = RAND_KEY_RAND_LENGTH;
   wcfg.val_type         = RAND_VAL_RAND_LENGTH;
   wcfg.rand_seed        = data->rand_seed;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 4(d) - RAND_KEY_RAND_LENGTH
CTEST2(large_inserts_stress, test_Rand_key_Rand_6byte_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = RAND_KEY_RAND_LENGTH;
   wcfg.val_type         = RAND_6BYTE_VAL;
   wcfg.rand_seed        = data->rand_seed;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 5(a) - RAND_KEY_DATA_BUF_SIZE
CTEST2(large_inserts_stress, test_Rand_key_packed_Seq_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = RAND_KEY_DATA_BUF_SIZE;
   wcfg.val_type         = SEQ_VAL_SMALL;
   wcfg.rand_seed        = data->rand_seed;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 5(b) - RAND_KEY_DATA_BUF_SIZE
CTEST2(large_inserts_stress, test_Rand_key_packed_Seq_values_packed_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = RAND_KEY_DATA_BUF_SIZE;
   wcfg.val_type         = SEQ_VAL_PADDED_LENGTH;
   wcfg.rand_seed        = data->rand_seed;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
}

// Case 5(c) - RAND_KEY_DATA_BUF_SIZE
CTEST2(large_inserts_stress, test_Rand_key_packed_Rand_length_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = RAND_KEY_DATA_BUF_SIZE;
   wcfg.val_type         = RAND_VAL_RAND_LENGTH;
   wcfg.rand_seed        = data->rand_seed;
   wcfg.verbose_progress = data->verbose_progress;

   exec_worker_thread(&wcfg);
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
 * test_Seq_key_be32_Seq_values_inserts_forked() --
 *
 * Test case is identical to test_Seq_key_be32_Seq_values_inserts() but the
 * actual execution of the function that does inserts is done from
 * a forked-child process. This test, therefore, does basic validation
 * that from a forked-child process we can drive basic SplinterDB commands.
 * And then the parent can resume after the child exits, and can cleanly
 * shutdown the instance.
 * ----------------------------------------------------------------------------
 */
// RESOLVE: Fails due to assertion:
// OS-pid=1576708, OS-tid=1576708, Thread-ID=0, Assertion failed at
// src/rc_allocator.c:536:rc_allocator_dec_ref(): "(ref_count != UINT8_MAX)".
// extent_no=14, ref_count=255 (0xff)
CTEST2_SKIP(large_inserts_stress, test_Seq_key_be32_Seq_values_inserts_forked)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb             = data->kvsb;
   wcfg.num_inserts      = data->num_inserts;
   wcfg.key_size         = data->key_size;
   wcfg.val_size         = data->val_size;
   wcfg.key_type         = SEQ_KEY_BIG_ENDIAN_32;
   wcfg.val_type         = SEQ_VAL_SMALL;
   wcfg.verbose_progress = data->verbose_progress;
   wcfg.fork_child       = TRUE;

   int pid = getpid();

   if (wcfg.fork_child) {
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
                     (wcfg.fork_child ? "forked child" : "parent"));

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
 * These test cases are identical to the list 1(a), 1(b), etc., which are all
 * single-threaded. These test cases execute the same workload (key- and
 * value-data distribution strategies), except they run multiple threads.
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
// Case 1(a) - SEQ_KEY_BIG_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_be32_Seq_values_inserts_threaded)
{
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        SEQ_KEY_BIG_ENDIAN_32,
                        SEQ_VAL_SMALL,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 1(b) - SEQ_KEY_BIG_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_be32_Seq_values_packed_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        SEQ_KEY_BIG_ENDIAN_32,
                        SEQ_VAL_PADDED_LENGTH,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 1(c) - SEQ_KEY_BIG_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_be32_Rand_length_values_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        SEQ_KEY_BIG_ENDIAN_32,
                        RAND_VAL_RAND_LENGTH,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 1(d) - SEQ_KEY_BIG_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_be32_Rand_6byte_values_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        SEQ_KEY_BIG_ENDIAN_32,
                        RAND_6BYTE_VAL,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 2(a) - SEQ_KEY_HOST_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_he32_Seq_values_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        SEQ_KEY_HOST_ENDIAN_32,
                        SEQ_VAL_SMALL,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 2(b) - SEQ_KEY_HOST_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_he32_Seq_values_packed_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        SEQ_KEY_HOST_ENDIAN_32,
                        SEQ_VAL_PADDED_LENGTH,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 2(c) - SEQ_KEY_HOST_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_he32_Rand_length_values_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        SEQ_KEY_HOST_ENDIAN_32,
                        RAND_VAL_RAND_LENGTH,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 2(d) - SEQ_KEY_HOST_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_he32_Rand_6byte_values_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        SEQ_KEY_HOST_ENDIAN_32,
                        RAND_6BYTE_VAL,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 3(a) - SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH
CTEST2(large_inserts_stress, test_Seq_key_packed_he32_Seq_values_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH,
                        SEQ_VAL_SMALL,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 3(b) - SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH
CTEST2(large_inserts_stress, test_Seq_key_packed_he32_Seq_values_packed_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH,
                        SEQ_VAL_PADDED_LENGTH,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 3(c) - SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH
CTEST2(large_inserts_stress, test_Seq_key_packed_he32_Rand_length_values_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH,
                        RAND_VAL_RAND_LENGTH,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 4(a) - RAND_KEY_RAND_LENGTH
CTEST2(large_inserts_stress, test_Rand_key_Seq_values_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        RAND_KEY_RAND_LENGTH,
                        SEQ_VAL_SMALL,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 4(b) - RAND_KEY_RAND_LENGTH
CTEST2(large_inserts_stress, test_Rand_key_Seq_values_packed_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        RAND_KEY_RAND_LENGTH,
                        SEQ_VAL_PADDED_LENGTH,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 4(c) - RAND_KEY_RAND_LENGTH
CTEST2(large_inserts_stress, test_Rand_key_Rand_length_values_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        RAND_KEY_RAND_LENGTH,
                        RAND_VAL_RAND_LENGTH,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 4(d) - RAND_KEY_RAND_LENGTH
CTEST2(large_inserts_stress, test_Rand_key_Rand_6byte_values_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        RAND_KEY_RAND_LENGTH,
                        RAND_6BYTE_VAL,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 5(a) - RAND_KEY_DATA_BUF_SIZE
CTEST2(large_inserts_stress, test_Rand_key_packed_Seq_values_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        RAND_KEY_DATA_BUF_SIZE,
                        SEQ_VAL_SMALL,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 5(b) - RAND_KEY_DATA_BUF_SIZE
// FIXME: Failed in CI main-pr-asan job:
// OS-pid=2690, OS-tid=2820, Thread-ID=2, Assertion failed at src/trunk.c:5500:trunk_compact_bundle(): "height != 0"
CTEST2_SKIP(large_inserts_stress, test_Rand_key_packed_Seq_values_packed_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        RAND_KEY_DATA_BUF_SIZE,
                        SEQ_VAL_PADDED_LENGTH,
                        data->num_inserts,
                        data->num_insert_threads);
}

// clang-format off
// Case 5(c) - RAND_KEY_DATA_BUF_SIZE
CTEST2(large_inserts_stress, test_Rand_key_packed_Rand_length_values_inserts_threaded)
{
   // clang-format on
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        RAND_KEY_DATA_BUF_SIZE,
                        RAND_VAL_RAND_LENGTH,
                        data->num_inserts,
                        data->num_insert_threads);
}

/*
 * Test case that fires up many threads each concurrently inserting large # of
 * KV-pairs, with all threads inserting from same start-value.
 *
 * With --num-threads 63, hangs in
 *  clockcache_get_read() -> memtable_maybe_rotate_and_get_insert_lock()
 * FIXME: Runs into BTree pack errors:
 * btree_pack(): req->num_tuples=6291456 exceeded output size limit,
 * req->max_tuples=6291456 btree_pack failed: No space left on device
 * FIXME: Causes CI-timeout after 2h in debug-test runs.
 */
// clang-format off
// Case 6(a) Variation of Case 2(a) - SEQ_KEY_HOST_ENDIAN_32
CTEST2_SKIP(large_inserts_stress, test_Seq_key_he32_same_start_keyid_Seq_values_inserts_threaded)
// clang-format on
{
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        SEQ_KEY_HOST_ENDIAN_32_SAME_START_ID,
                        SEQ_VAL_SMALL,
                        data->num_inserts,
                        data->num_insert_threads);
}

/*
 * Test case that fires up many threads each concurrently inserting large # of
 * KV-pairs, with all threads inserting from same start-value, using a fixed
 * fully-packed value.
 * FIXME: Causes CI-timeout after 2h in debug-test runs.
 */
// clang-format off
// Case 7(b) Variation of Case 1(b) - SEQ_KEY_BIG_ENDIAN_32
CTEST2(large_inserts_stress, test_Seq_key_be32_same_start_keyid_Seq_values_packed_inserts_threaded)
// clang-format on
{
   do_inserts_n_threads(data->kvsb,
                        data->hid,
                        data->key_size,
                        data->val_size,
                        SEQ_KEY_BIG_ENDIAN_32_SAME_START_ID,
                        SEQ_VAL_PADDED_LENGTH,
                        data->num_inserts,
                        data->num_insert_threads);
}

/*
 * Test case developed to repro an out-of-bounds assertion tripped up in
 * trunk_build_filters() -> fingerprint_ntuples(). The fix has been id'ed
 * to relocate fingerprint_ntuples() in its flow. There was no real logic
 * error but a code-flow error. The now-fixed-bug would only repro with
 * something like --num-inserts 20M.
 */
// clang-format off
CTEST2(large_inserts_stress, test_fp_num_tuples_out_of_bounds_bug_trunk_build_filters)
{
   // clang-format on
   char key_data[TEST_KEY_SIZE];
   char val_data[TEST_VALUE_SIZE];

   uint64 start_key = 0;

   uint64 start_time = platform_get_timestamp();

   threadid thread_idx = platform_get_tid();

   // Test is written to insert multiples of millions per thread.
   ASSERT_EQUAL(0, (data->num_inserts % MILLION));

   CTEST_LOG_INFO("%s()::%d:Thread-%-lu inserts %lu (%lu million)"
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
         CTEST_LOG_INFO("Thread-%lu Inserted %lu million KV-pairs ...\n",
                        thread_idx,
                        (ictr + 1));
      }
   }
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
                  (data->num_inserts / elapsed_s));
}

/*
 * ----------------------------------------------------------------------------
 * do_inserts_n_threads() - Driver function that will fire-up n-threads to
 * perform different forms of inserts run by all the threads. The things we
 * control via parameters are:
 *
 * Parameters:
 * - key_type           - Key-data strategy
 * - val_type           - Value-data strategy
 * - num_inserts        - # of inserts / thread
 * - num_insert_threads - # of inserting threads to start-up
 * ----------------------------------------------------------------------------
 */
static void
do_inserts_n_threads(splinterdb      *kvsb,
                     platform_heap_id hid,
                     size_t           key_size,
                     size_t           val_size,
                     key_strategy     key_type,
                     val_strategy     val_type,
                     uint64           num_inserts,
                     uint64           num_insert_threads)
{
   platform_memfrag memfrag_wcfg;
   worker_config   *wcfg = TYPED_ARRAY_ZALLOC(hid, wcfg, num_insert_threads);

   // Setup thread-specific insert parameters
   for (int ictr = 0; ictr < num_insert_threads; ictr++) {
      wcfg[ictr].kvsb        = kvsb;
      wcfg[ictr].key_size    = key_size;
      wcfg[ictr].val_size    = val_size;
      wcfg[ictr].num_inserts = num_inserts;

      // Choose the start key-ID for each thread.
      switch (key_type) {
         case SEQ_KEY_HOST_ENDIAN_32_SAME_START_ID:
         case SEQ_KEY_BIG_ENDIAN_32_SAME_START_ID:
            CTEST_LOG_INFO("All threads start from same start key ID=0\n");
            wcfg[ictr].start_value = 0;
            key_type = ((key_type == SEQ_KEY_HOST_ENDIAN_32_SAME_START_ID)
                           ? SEQ_KEY_HOST_ENDIAN_32
                           : SEQ_KEY_BIG_ENDIAN_32);
            break;
         default:
            wcfg[ictr].start_value = (wcfg[ictr].num_inserts * ictr);
            break;
      }

      wcfg[ictr].key_type         = key_type;
      wcfg[ictr].val_type         = val_type;
      wcfg[ictr].is_thread        = TRUE;
      wcfg[ictr].verbose_progress = TRUE;
   }
   wcfg[0].show_strategy = TRUE;

   platform_memfrag memfrag_thread_ids;
   platform_thread *thread_ids =
      TYPED_ARRAY_ZALLOC(hid, thread_ids, num_insert_threads);

   // Fire-off the threads to drive inserts ...
   // clang-format off
   for (int tctr = 0; tctr < num_insert_threads; tctr++) {
      int rc = pthread_create(&thread_ids[tctr],
                              NULL,
                              &exec_worker_thread,
                              &wcfg[tctr]);
      ASSERT_EQUAL(0, rc);
   }
   // clang-format on

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
   platform_free(hid, &memfrag_thread_ids);
   platform_free(hid, &memfrag_wcfg);
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
   worker_config *wcfg = (worker_config *)w;

   platform_memfrag memfrag_key_buf;
   char  *key_buf      = TYPED_ARRAY_MALLOC(wcfg->hid, key_buf, wcfg->key_size);
   size_t key_buf_size = wcfg->key_size;

   size_t           val_buf_size = wcfg->val_size;
   platform_memfrag memfrag_val_buf;
   char *val_buf = TYPED_ARRAY_MALLOC(wcfg->hid, val_buf, (val_buf_size + 1));

   splinterdb *kvsb        = wcfg->kvsb;
   uint64      start_key   = wcfg->start_value;
   uint64      num_inserts = wcfg->num_inserts;

   uint64 start_time = platform_get_timestamp();

   if (wcfg->is_thread) {
      splinterdb_register_thread(kvsb);
   }
   threadid thread_idx = platform_get_tid();

   // Test is written to insert multiples of millions per thread.
   ASSERT_EQUAL(0, (num_inserts % MILLION));

   if (wcfg->show_strategy) {
      CTEST_LOG_INFO("\nKey-data: '%s', Value-data: '%s' ...\n",
                     Key_strategy_names[wcfg->key_type],
                     Val_strategy_names[wcfg->val_type]);
   }
   CTEST_LOG_INFO("%s()::%d:Thread %-2lu inserts %lu (%lu million)"
                  " KV-pairs starting from %lu (%lu%s)\n",
                  __func__,
                  __LINE__,
                  thread_idx,
                  num_inserts,
                  (num_inserts / MILLION),
                  start_key,
                  (start_key / MILLION),
                  (start_key ? " million" : ""));

   uint64 ictr = 0;
   uint64 jctr = 0;

   bool verbose_progress = wcfg->verbose_progress;

   // Initialize allocated buffers to avoid MSAN failures
   memset(key_buf, 'K', key_buf_size);

   // Insert fully-packed wider-values so we fill pages faster.
   uint64 val_len = val_buf_size;
   memset(val_buf, 'V', val_buf_size);

   int32  key_data_be; // int-32 keys generated in big-endian-32 notation
   int32  key_data_he; // int-32 keys generated in host-endian-32 notation
   uint64 key_len;

   random_state key_rs = {0};
   switch (wcfg->key_type) {
      case SEQ_KEY_BIG_ENDIAN_32:
         key_buf = (char *)&key_data_be;
         key_len = sizeof(key_data_be);
         break;

      case SEQ_KEY_HOST_ENDIAN_32:
         key_buf = (char *)&key_data_he;
         key_len = sizeof(key_data_he);
         break;

      case SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH:
         key_len = key_buf_size;
         break;

      case RAND_KEY_DATA_BUF_SIZE:
         key_len = key_buf_size;
         // Fall-through
      case RAND_KEY_RAND_LENGTH:
         random_init(&key_rs, wcfg->rand_seed, 0);
         break;

      default:
         platform_assert(FALSE,
                         "Unknown key-data strategy %d (%s)",
                         wcfg->key_type,
                         Key_strategy_name(wcfg->key_type));
   }

   random_state val_rs = {0};
   switch (wcfg->val_type) {
      case RAND_6BYTE_VAL:
         val_len = 6;
         // Fall-through
      case RAND_VAL_RAND_LENGTH:
         random_init(&val_rs, wcfg->rand_seed, 0);
         break;

      default:
         break;
   }


   for (ictr = 0; ictr < (num_inserts / MILLION); ictr++) {
      for (jctr = 0; jctr < MILLION; jctr++) {

         uint64 id = (start_key + (ictr * MILLION) + jctr);

         // Generate key-data based on key-strategy specified.
         switch (wcfg->key_type) {
            case SEQ_KEY_BIG_ENDIAN_32:
               // Generate sequential key data, stored in big-endian order
               key_data_be = htobe32(id);
               break;

            case SEQ_KEY_HOST_ENDIAN_32:
               key_data_he = id;
               break;
            case SEQ_KEY_HOST_ENDIAN_32_PADDED_LENGTH:
            {
               int tmp_len      = snprintf(key_buf, key_buf_size, "%lu", id);
               key_buf[tmp_len] = 'K';
               break;
            }

            case RAND_KEY_RAND_LENGTH:
               // Fill-up key-data buffer with random data for random length.
               key_len = random_next_int(
                  &key_rs, TEST_CONFIG_MIN_KEY_SIZE, key_buf_size);
               random_bytes(&key_rs, key_buf, key_len);
               break;

            case RAND_KEY_DATA_BUF_SIZE:
               // Pack-up key-data buffer with random data
               random_bytes(&key_rs, key_buf, key_len);
               break;

            default:
               break;
         }

         // Generate value-data based on value-strategy specified.
         switch (wcfg->val_type) {
            case SEQ_VAL_SMALL:
               // Generate small-length sequential value data
               val_len = snprintf(val_buf, val_buf_size, "Row-%lu", id);
               break;

            case SEQ_VAL_PADDED_LENGTH:
            {
               // Generate small-length sequential value packed-data
               int tmp_len = snprintf(val_buf, val_buf_size, "Row-%lu", id);
               val_buf[tmp_len] = 'V';
               break;
            }
            case RAND_VAL_RAND_LENGTH:
               // Fill-up value-data buffer with random data for random length.
               val_len = random_next_int(&val_rs, 1, val_buf_size);
               random_bytes(&val_rs, val_buf, val_len);
               break;

            case RAND_6BYTE_VAL:
               // Fill-up value-data buffer with random data for 6-bytes
               random_bytes(&val_rs, val_buf, val_len);
               break;

            default:
               platform_assert(FALSE,
                               "Unknown value-data strategy %d (%s)",
                               wcfg->val_type,
                               Val_strategy_name(wcfg->val_type));
               break;
         }
         slice key = slice_create(key_len, key_buf);
         slice val = slice_create(val_len, val_buf);

         int rc = splinterdb_insert(kvsb, key, val);
         ASSERT_EQUAL(0, rc);
      }
      if (verbose_progress) {
         CTEST_LOG_INFO("%s()::%d:Thread-%lu Inserted %lu million "
                        "KV-pairs ...\n",
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

   // Cleanup resources opened in this call.
   platform_free(wcfg->hid, &memfrag_key_buf);
   platform_free(wcfg->hid, &memfrag_val_buf);
   return 0;
}
