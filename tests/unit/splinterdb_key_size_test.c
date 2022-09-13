// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * Minimal reproducer for some weird crashes on particular keys/values
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

void inner_test(size_t max_key_size, size_t value_size, FILE* key_file) {
   platform_default_log("initializing for max_key_size = %lu...\n", max_key_size);
   data_config data_cfg = {};
   default_data_config_init(max_key_size, &data_cfg);
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


   char value_buffer[2048] = {0};
   ASSERT_TRUE(sizeof(value_buffer) > value_size);
   random_bytes(&rand_state, value_buffer, value_size);

   ssize_t key_len;
   char* key_buffer = NULL;
   size_t key_buffer_len = 0;
   int i = 0;
   while((key_len = getline(&key_buffer, &key_buffer_len, key_file)) != -1) {
      int rc = splinterdb_insert(sdb, slice_create(key_len-1, key_buffer), slice_create(value_size, value_buffer));
      ASSERT_EQUAL(0, rc);
      if (i % 100000 == 0) {
         platform_default_log("loaded %d keys...\n", i);
      }
      i++;
   }
   free(key_buffer);  // because getline allocates the buffer

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

// Do some inserts, and then some range-deletes
CTEST2(splinterdb_key_size, test_different_key_sizes)
{
   int key_size = parse_env_var("MAX_KEY_SIZE");
   if (key_size == 0) {
      key_size = SPLINTERDB_MAX_KEY_SIZE;
   }

   int value_size = parse_env_var("VALUE_SIZE");
   if (value_size == 0) {
      value_size = 1050;
   }

   char* key_file_name = getenv("KEY_FILE");
   if (key_file_name == NULL) {
      key_file_name = "test-keys.txt";
   }

   platform_default_log("\nloading data from key file %s\n", key_file_name);
   FILE* key_file = fopen(key_file_name, "r");
   if (key_file == NULL) {
      platform_error_log("unable to open key file %s, try downloading it with\n\t%s\n",
            key_file_name, "curl -o test-keys.txt --compressed https://splinterdb-test-data.s3.us-west-1.amazonaws.com/gabe-crash-test-keys.txt");
      exit(1);
   }

   inner_test((size_t)(key_size), (size_t)(value_size), key_file);

   fclose(key_file);
}
