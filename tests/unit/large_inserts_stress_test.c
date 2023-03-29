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
#include "platform.h"
#include "splinterdb/public_platform.h"
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "config.h"
#include "unit_tests.h"
#include "ctest.h"

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
   int            random_key_fd; // Options to choose the type of key inserted
   int            random_val_fd; // Options to choose the type of value inserted
} worker_config;

/*
 * random_key_fd types to select how key's data is inserted
 */
// Randomly generated key, inserted in big-endian 32-bit order.
// This facilitates lookup using lexcmp()
#define RANDOM_KEY_BIG_ENDIAN_32_FD 2

// Randomly generated key, inserted in host-endian order
#define RANDOM_KEY_HOST_ENDIAN_FD 1

// Sequentially generated key, inserted using snprintf() to key-buffer
#define SEQ_KEY_SNPRINTF_FD 0

// Sequentially generated key, inserted in host-endian order
#define SEQ_KEY_HOST_ENDIAN_32_FD -1

// Sequentially generated key, inserted in big-endian 32-bit order.
// This facilitates lookup using lexcmp()
#define SEQ_KEY_BIG_ENDIAN_32_FD -2

/*
 * random_val_fd types to select how value's data is generated
 */
// Small value, sequentially generated based on key-ID is stored.
#define SEQ_VAL_SMALL_LENGTH_FD 0

#define RANDOM_VAL_FIXED_LEN_5_FD 5

// Random value generated, exactly of 6 bytes. This case is used to simulate
// data insertions into SplinterDB for Postgres-integration, where we store
// the 6-byte tuple-ID (TID) as the value.
#define RANDOM_VAL_FIXED_LEN_6_FD 6

#define RANDOM_VAL_FIXED_LEN_8_FD 8

// Function Prototypes
static void *
exec_worker_thread(void *w);

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
      platform_get_module_id(), heap_capacity, &data->hh, &data->hid);
   platform_assert_status_ok(rc);

   int    argc = Ctest_argc;
   char **argv = (char **)Ctest_argv;
   data->cfg =
      (splinterdb_config){.filename = "splinterdb_large_inserts_stress_test_db",
                          .cache_size = 1 * Giga,
                          .disk_size  = 40 * Giga,
                          .data_cfg   = &data->default_data_config};

   ZERO_STRUCT(data->master_cfg);
   config_set_defaults(&data->master_cfg);

   // Expected args to parse --num-inserts, --num-threads, --verbose-progress.
   rc = config_parse(&data->master_cfg, 1, argc, (char **)argv);
   ASSERT_TRUE(SUCCESS(rc));

   data->num_inserts =
      (data->master_cfg.num_inserts ? data->master_cfg.num_inserts
                                    : (1 * MILLION));

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
CTEST_TEARDOWN(large_inserts_stress)
{
   // Only parent process should tear down Splinter.
   if (data->am_parent) {
      splinterdb_close(&data->kvsb);
      platform_heap_destroy(&data->hh);
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
   wcfg.random_key_fd = SEQ_KEY_SNPRINTF_FD;

   exec_worker_thread(&wcfg);
}

/*
 * Specialized test case to reproduce issue #560 discovered
 * during large-inserts performance benchmarking. Here, we
 * load 4-byte int32 ID-keys, in big-endian format, with
 * randomly generated 6-byte 'value'. Value is supposed to
 * reflect 6-byte tuple-ID (TID).
 */
CTEST2(large_inserts_stress,
       test_560_seq_host_endian32_key_random_6byte_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb          = data->kvsb;
   wcfg.master_cfg    = &data->master_cfg;
   wcfg.num_inserts   = data->num_inserts;
   wcfg.random_key_fd = SEQ_KEY_HOST_ENDIAN_32_FD;
   wcfg.random_val_fd = RANDOM_VAL_FIXED_LEN_6_FD;

   exec_worker_thread(&wcfg);
}

/*
 * Specialized test case to reproduce issue #560 discovered
 * during large-inserts performance benchmarking. Here, we
 * load 4-byte int32 ID-keys, in big-endian format, with
 * randomly generated 6-byte 'value'. Value is supposed to
 * reflect 6-byte tuple-ID (TID).
 */
CTEST2(large_inserts_stress,
       test_560_seq_htobe32_key_random_6byte_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb          = data->kvsb;
   wcfg.master_cfg    = &data->master_cfg;
   wcfg.num_inserts   = data->num_inserts;
   wcfg.random_key_fd = SEQ_KEY_BIG_ENDIAN_32_FD;
   wcfg.random_val_fd = RANDOM_VAL_FIXED_LEN_6_FD;

   exec_worker_thread(&wcfg);
}

/*
 * Specialized test case to reproduce issue #560: Variation of prev case;
 * test_560_seq_htobe32_key_random_6byte_values_inserts(). In this case, we
 * load 4-byte int32 ID-keys, in host-endian format, with randomly generated
 * 5-byte 'value' This variation tests if the repro is affected by choice of
 * host-endian storage for the key ID, and a shorter value length.
 * Even with both changes, the problems still repros.
 */
CTEST2(large_inserts_stress,
       test_560_seq_host_endian32_key_random_5byte_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb          = data->kvsb;
   wcfg.master_cfg    = &data->master_cfg;
   wcfg.num_inserts   = data->num_inserts;
   wcfg.random_key_fd = SEQ_KEY_HOST_ENDIAN_32_FD;
   wcfg.random_val_fd = RANDOM_VAL_FIXED_LEN_5_FD;

   exec_worker_thread(&wcfg);
}

/*
 * Variation of above 2 cases: Here with host-endian storage of the key and
 * 8-byte value, we somehow do not run into above bug.
 */
CTEST2(large_inserts_stress,
       test_560_seq_host_endian32_key_random_8byte_values_inserts)
{
   worker_config wcfg;
   ZERO_STRUCT(wcfg);

   // Load worker config params
   wcfg.kvsb          = data->kvsb;
   wcfg.master_cfg    = &data->master_cfg;
   wcfg.num_inserts   = data->num_inserts;
   wcfg.random_key_fd = SEQ_KEY_HOST_ENDIAN_32_FD;
   wcfg.random_val_fd = RANDOM_VAL_FIXED_LEN_8_FD;

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

   int32 key_data_he; // int-32 keys generated in host-endian-32 notation
   if (random_key_fd == SEQ_KEY_HOST_ENDIAN_32_FD) {
      key_data = (char *)&key_data_he;
      key_len  = sizeof(key_data_he);
   }

   // Test is written to insert multiples of millions per thread.
   ASSERT_EQUAL(0, (num_inserts % MILLION));

   const char *random_val_descr = NULL;

   random_val_descr =
      ((random_val_fd > SEQ_VAL_SMALL_LENGTH_FD)    ? "random"
       : (random_val_fd == SEQ_VAL_SMALL_LENGTH_FD) ? "sequential"
                                                    : "fully-packed constant");

   threadid thread_idx = platform_get_tid();
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

   // Insert fully-packed wider-values so we fill pages faster.
   // This value-data will be chosen as default case when
   // (random_val_fd < SEQ_VAL_SMALL_LENGTH_FD)
   memset(val_data, 'V', sizeof(val_data));
   uint64 val_len = sizeof(val_data);

   if (random_val_fd > SEQ_VAL_SMALL_LENGTH_FD) {
      // Generate random value choosing the width of value generated.
      // Value-length selector from 5, 6, 8 bytes, as specified by test case
      val_len =
         ((random_val_fd <= RANDOM_VAL_FIXED_LEN_8_FD) ? random_val_fd
                                                       : sizeof(val_data));

      platform_default_log("OS-pid=%d, Thread-ID=%lu"
                           ", Insert random value of "
                           "fixed-length=%lu bytes.\n",
                           getpid(),
                           thread_idx,
                           val_len);
   } else if (random_val_fd == SEQ_VAL_SMALL_LENGTH_FD) {
      platform_default_log("OS-pid=%d, Thread-ID=%lu"
                           ", Insert small-width sequential values of "
                           "different lengths.\n",
                           getpid(),
                           thread_idx);
   } else { // => if (random_val_fd < SEQ_VAL_SMALL_LENGTH_FD)
      platform_default_log("OS-pid=%d, Thread-ID=%lu"
                           ", Insert fully-packed fixed value of "
                           "length=%lu bytes.\n",
                           getpid(),
                           thread_idx,
                           val_len);
   }

   bool   verbose_progress = wcfg->master_cfg->verbose_progress;
   uint64 start_time       = platform_get_timestamp();

   uint64 ictr = 0;
   uint64 jctr = 0;

   for (ictr = 0; ictr < (num_inserts / MILLION); ictr++) {
      for (jctr = 0; jctr < MILLION; jctr++) {

         uint64 id = (start_key + (ictr * MILLION) + jctr);

         // Generate random key / value if calling test-case requests it.
         if (random_key_fd > 0) {

            // Generate random key-data for full width of key.
            size_t result = read(random_key_fd, key_buf, sizeof(key_buf));
            ASSERT_TRUE(result >= 0);

            key_len = result;
         } else if (random_key_fd == SEQ_KEY_SNPRINTF_FD) {
            // Generate sequential key data, stored in host-endian order
            snprintf(key_buf, sizeof(key_buf), "%lu", id);
            key_len = strlen(key_buf);
         } else if (random_key_fd == SEQ_KEY_HOST_ENDIAN_32_FD) {
            // Generate sequential key data, stored in host-endian order
            key_data_he = id;
         } else if (random_key_fd == SEQ_KEY_BIG_ENDIAN_32_FD) {
            // Generate sequential key data, stored in big-endian order
            key_data_be = htobe32(id);
         }

         // Manage how the value-data is generated based on random_val_fd
         if (random_val_fd > SEQ_VAL_SMALL_LENGTH_FD) {

            size_t result = read(random_val_fd, val_data, val_len);
            ASSERT_TRUE(result >= 0);
         } else if (random_val_fd == SEQ_VAL_SMALL_LENGTH_FD) {
            // Generate small-length sequential value data
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
   return 0;
}
