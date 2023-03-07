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
#include <string.h>
#include "platform.h"
#include "splinterdb/public_platform.h"
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

// Nothing particularly significant about these constants.
#define TEST_KEY_SIZE   20
#define TEST_VALUE_SIZE 116

/*
 * Global data declaration macro:
 */
CTEST_DATA(large_inserts_stress)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       default_data_config;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(large_inserts_stress)
{
   data->cfg           = (splinterdb_config){.filename   = "splinterdb_large_inserts_stress_tests_db",
                                             .cache_size = 256 * Mega,
                                             .disk_size  = 9000 * Mega,
                                             .data_cfg   = &data->default_data_config};
   size_t max_key_size = TEST_KEY_SIZE;
   default_data_config_init(max_key_size, data->cfg.data_cfg);

   int rc = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(large_inserts_stress)
{
   splinterdb_close(&data->kvsb);
}

/*
 * Test case that inserts large # of KV-pairs, and goes into a code path
 * reported by issue# 458, tripping a debug assert. This test case also
 * triggered the failure(s) reported by issue # 545.
 */
CTEST2(large_inserts_stress,
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
