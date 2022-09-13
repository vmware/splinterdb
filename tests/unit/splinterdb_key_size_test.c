// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * Test when data_config has large maximum key_size, but application
 * data inserts smaller keys
 */

#include "splinterdb/public_platform.h"
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "unit_tests.h"
#include "util.h"
#include "../functional/random.h"
#include "ctest.h" // This is required for all test-case files.

CTEST_DATA(splinterdb_key_size) {};
CTEST_SETUP(splinterdb_key_size) {};
CTEST_TEARDOWN(splinterdb_key_size) {};

void inner_test(size_t key_size, size_t value_size, int num_operations) {
   platform_default_log("initializing for max_key_size = %lu...\n", key_size);
   data_config data_cfg = {};
   default_data_config_init(key_size, &data_cfg);
   splinterdb_config cfg  = (splinterdb_config){
      .filename   = "db",
      .cache_size = 900 * Mega,
      .disk_size  = 9000 * Mega,
      .data_cfg   = &data_cfg,
   };
   splinterdb* sdb;
   int rc = splinterdb_create(&cfg, &sdb);
   ASSERT_EQUAL(0, rc);
   splinterdb_register_thread(sdb);

   random_state rand_state;
   random_init(&rand_state, 42, 0);

   char key_buffer[120] = {0};
   ASSERT_TRUE(sizeof(key_buffer) > key_size);

   char value_buffer[2048] = {0};
   ASSERT_TRUE(sizeof(value_buffer) > value_size);
   random_bytes(&rand_state, value_buffer, value_size);

   size_t actual_key_size = 24; // application uses smaller keys

   for (int i =0; i < num_operations; i++) {
      random_bytes(&rand_state, key_buffer, actual_key_size);
      int rc = splinterdb_insert(sdb, slice_create(actual_key_size, key_buffer), slice_create(value_size, value_buffer));
      ASSERT_EQUAL(0, rc);
      if (i % 100000 == 0) {
         platform_default_log("loaded %d keys...\n", i);
      }
   }

   splinterdb_close(&sdb);
   platform_default_log("done\n");
}

int parse_env_var(const char* var) {
   char* as_str = getenv(var);
   if (as_str == NULL) {
      return 0;
   }
   return atoi(as_str);
}

// Do some inserts with configurable-sized tuple
CTEST2(splinterdb_key_size, test_different_key_sizes)
{
   int key_size = parse_env_var("MAX_KEY_SIZE");
   if (key_size == 0) {
      key_size = 103;
   }

   int value_size = parse_env_var("VALUE_SIZE");
   if (value_size == 0) {
      value_size = 1050;
   }

   int num_ops = parse_env_var("NUM_OPS");
   if (num_ops == 0) {
      num_ops = 900000;
   }

   inner_test((size_t)(key_size), (size_t)(value_size), num_ops);
}
