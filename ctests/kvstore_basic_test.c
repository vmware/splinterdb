// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * kvstore_basic_test.c --
 *
 *     Exercises the kvstore_basic API, which exposes keys & values
 *     instead of the keys & messages of the lower layers.
 *
 *     This test code can be easily modified to be an example of a standalone
 *     program that integrates with SplinterDB.
 *
 *     To compile this into a standalone program, just rename the function
 *     kvstore_basic_test() to be main(), and ensure you've got the
 *     kvstore_basic.h header and libsplinterdb.a available for linking.
 *
 *     $ cc -L splinterdb/lib -I splinterdb/include \
 *          my_program.c -lsplinterdb -lxxhash -laio -lpthread -lm
 *
 * NOTE: This test case file also serves as an example for how-to build
 *  CTests, and the syntax for different commands etc. Note the
 *  annotations to learn how to write new unit-tests using Ctests.
 *
 * Naming Conventions:
 *
 *  o The file containing unit-test cases for a module / functionality is
 *    expected to be named <something>_test.c
 *
 *  o Individual test cases [ see below ] in a file are prefaced with a
 *    term naming the test suite, for the module / functionality being tested.
 *    Usually it will just be <something>; i.e. in kvstore_basic_test.c
 *    the suite-name is 'kvstore_basic'.
 *
 *  o Each test case should be named <operation>_test
 */
#include <stdlib.h> // Needed for system calls; e.g. free
#include <stdio.h>  // Needed for system calls; e.g. fprintf
#include "ctest.h"  // This is required for all test-case files.

#include "platform.h"
#include "splinterdb/kvstore_basic.h"

#define Mega (1024UL * 1024UL)

#define TEST_DB_NAME "db"

#define TEST_INSERT_KEY_LENGTH 7
#define TEST_INSERT_VAL_LENGTH 7

// Hard-coded format strings to generate key and values
static const char key_fmt[] = "key-%02x";
static const char val_fmt[] = "val-%02x";

// We use only short values while loading data in these test cases
#define TEST_SHORT_VALUE_SIZE MAX_KEY_SIZE

// Function Prototypes
static int
setup_kvstore_basic(kvstore_basic **kvsb, kvstore_basic_cfg *cfg);

static int
insert_some_keys(const int num_inserts, kvstore_basic *kvsb);

static int
check_current_tuple(kvstore_basic_iterator *it, const int expected_i);

/*
 * All tests in this file are named with one term, which represents the
 * module / functionality you are testing. Here, it is: kvstore_basic
 *
 * This is an individual test case, testing [usually] just one thing.
 * The 2nd term is the test-case name. Here, just: 'empty_test'
 */
CTEST(kvstore_basic, empty_test) {}

/*
 * Global data declaration macro:
 *
 * This is converted into a struct, with a generated name prefixed by the
 * suite name. This structure is then automatically passed to all tests in
 * the test suite. In this function, declare all structures and
 * variables that you need globally to setup Splinter. This macro essentially
 * resolves to a bunch of structure declarations, so no code fragments can
 * be added here.
 *
 * NOTE: All data structures will hang off of data->, where 'data' is a
 * global static variable manufactured by CTEST_SETUP() macro.
 */
CTEST_DATA(kvstore_basic)
{
   kvstore_basic *   kvsb;
   kvstore_basic_cfg cfg;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(kvstore_basic)
{
   memset(&data->cfg, 0, sizeof(data->cfg));
   int rc = setup_kvstore_basic(&data->kvsb, &data->cfg);
   ASSERT_EQUAL(0, rc);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(kvstore_basic)
{
   kvstore_basic_close(data->kvsb);
}

/*
 * Basic test case that exercises and validates the basic flow of the
 * Splinter APIs.  We exercise:
 *  - kvstore_basic_insert()
 *  - kvstore_basic_lookup() and
 *  - kvstore_basic_delete()
 *
 * Validate that they behave as expected, including some basic error
 * condition checking.
 */
CTEST2(kvstore_basic, test_basic_flow)
{
   char * key     = "some-key";
   size_t key_len = sizeof("some-key");
   _Bool  found;
   _Bool  val_truncated;
   char * value = calloc(1, data->cfg.max_value_size);
   size_t val_len;

   int rc = 0;
   // **** Lookup of a non-existent key should fail.
   rc = kvstore_basic_lookup(data->kvsb,
                             key,
                             key_len,
                             value,
                             data->cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(found);

   // **** Basic insert of new key should succeed.
   static char *insval = "some-value";
   rc = kvstore_basic_insert(data->kvsb, key, key_len, insval, sizeof(insval));
   ASSERT_EQUAL(0, rc);

   // **** Should be able to lookup key/value just inserted above
   rc = kvstore_basic_lookup(data->kvsb,
                             key,
                             key_len,
                             value,
                             data->cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   ASSERT_EQUAL(0, rc);
   ASSERT_STREQN(insval, value, val_len);
   ASSERT_EQUAL(sizeof(insval), val_len);
   ASSERT_FALSE(val_truncated);
   ASSERT_TRUE(found);

   // **** Basic delete of an existing key should succeed
   rc = kvstore_basic_delete(data->kvsb, key, key_len);
   ASSERT_EQUAL(0, rc);

   // **** Lookup of now-deleted key should succeed, but key is not found.
   rc = kvstore_basic_lookup(data->kvsb,
                             key,
                             key_len,
                             value,
                             data->cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(found);
   if (value)
      free(value);
}

/*
 * Basic test case that exercises and validates the basic flow of the
 * Splinter APIs for key of max-key-length.
 */
CTEST2(kvstore_basic, test_apis_for_max_key_length)
{
   char *large_key = calloc(1, data->cfg.max_key_size);
   memset(large_key, 7, data->cfg.max_key_size);

   static char *large_key_value = "a-value";
   int          rc              = 0;
   // **** Insert of a max-size key should succeed.
   rc = kvstore_basic_insert(data->kvsb,
                             large_key,
                             data->cfg.max_key_size,
                             large_key_value,
                             sizeof(large_key_value));
   ASSERT_EQUAL(0, rc);

   _Bool  found;
   _Bool  val_truncated;
   size_t val_len;
   char * value = calloc(1, data->cfg.max_value_size);

   // **** Lookup of max-size key should return correct value
   rc = kvstore_basic_lookup(data->kvsb,
                             large_key,
                             data->cfg.max_key_size,
                             value,
                             data->cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   ASSERT_EQUAL(0, rc);
   ASSERT_STREQN(large_key_value, value, val_len);
   ASSERT_EQUAL(sizeof(large_key_value), val_len);
   ASSERT_FALSE(val_truncated);
   ASSERT_TRUE(found);

   // **** Delete of max-size key should also succeed.
   rc = kvstore_basic_delete(data->kvsb, large_key, data->cfg.max_key_size);
   ASSERT_EQUAL(0, rc);

   // **** Should not find this large-key once it's deleted
   rc = kvstore_basic_lookup(data->kvsb,
                             large_key,
                             data->cfg.max_key_size,
                             value,
                             data->cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(found);

   if (large_key)
      free(large_key);
   if (value)
      free(value);
}

/*
 * Test case to verify core interfaces when key-size is > max key-size.
 */
CTEST2(kvstore_basic, test_key_size_gt_max_key_size)
{
   size_t too_large_key_len = data->cfg.max_key_size + 1;
   char * too_large_key     = calloc(1, too_large_key_len);
   memset(too_large_key, 'a', too_large_key_len);
   char *value = calloc(1, data->cfg.max_value_size);

   int rc = kvstore_basic_insert(data->kvsb,
                                 too_large_key,
                                 too_large_key_len,
                                 "a-value",
                                 sizeof("a-value"));
   ASSERT_EQUAL(EINVAL, rc);

   _Bool  found;
   _Bool  val_truncated;
   size_t val_len;
   rc = kvstore_basic_lookup(data->kvsb,
                             too_large_key,
                             too_large_key_len,
                             value,
                             data->cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   ASSERT_EQUAL(EINVAL, rc);

   if (too_large_key) {
      free(too_large_key);
   }
   if (value) {
      free(value);
   }
}

/*
 * Test case to verify core interfaces when value-size is > max value-size.
 * Here, we basically exercise the insert interface, which will trip up
 * if very large values are supplied. (Once insert fails, there is
 * no further need to verify the other interfaces for very-large-values.)
 */
CTEST2(kvstore_basic, test_value_size_gt_max_value_size)
{
   size_t            too_large_value_len = data->cfg.max_value_size + 1;
   char *            too_large_value     = calloc(1, too_large_value_len);
   static const char short_key[]         = "a_short_key";

   memset(too_large_value, 'z', too_large_value_len);
   int rc = kvstore_basic_insert(data->kvsb,
                                 short_key,
                                 sizeof(short_key),
                                 too_large_value,
                                 too_large_value_len);

   ASSERT_EQUAL(EINVAL, rc);
   if (too_large_value) {
      free(too_large_value);
   }
}

/*
 * RESOLVE: Need to port test case test_kvstore_basic_variable_length_values()
 */
CTEST2(kvstore_basic, test_variable_length_values) {}

/*
 * Basic KVStore iterator test case.
 */
CTEST2(kvstore_basic, test_basic_iterator)
{
   const int num_inserts = 50;
   int       rc          = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   int i = 0;

   kvstore_basic_iterator *it = NULL;

   rc = kvstore_basic_iter_init(data->kvsb, &it, NULL, 0);
   ASSERT_EQUAL(0, rc);

   for (; kvstore_basic_iter_valid(it); kvstore_basic_iter_next(it)) {
      rc = check_current_tuple(it, i);
      ASSERT_EQUAL(0, rc);
      i++;
   }
}


/*
 * ********************************************************************************
 * Define minions and helper functions here, after all test cases are
 * enumerated.
 * ********************************************************************************
 */

static int
setup_kvstore_basic(kvstore_basic **kvsb, kvstore_basic_cfg *cfg)
{
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
 * Helper function to insert n-keys (num_inserts), using pre-formatted
 * key and value strings.
 *
 * Returns: Return code: rc == 0 => success; anything else => failure
 */
static int
insert_some_keys(const int num_inserts, kvstore_basic *kvsb)
{
   int rc = 0;
   // insert keys backwards, just for kicks
   for (int i = num_inserts - 1; i >= 0; i--) {
      char key[TEST_INSERT_KEY_LENGTH] = {0};
      char val[TEST_INSERT_VAL_LENGTH] = {0};

      ASSERT_EQUAL(6, snprintf(key, sizeof(key), key_fmt, i));
      ASSERT_EQUAL(6, snprintf(val, sizeof(val), val_fmt, i));

      rc = kvstore_basic_insert(kvsb, key, sizeof(key), val, sizeof(val));
      ASSERT_EQUAL(0, rc);
   }

   return rc;
}

/*
 * Work horse routine to check if the current tuple pointed to by the
 * iterator is the expected one, as indicated by its index,
 * expected_i. We use pre-constructed key / value formats to verify
 * if the current tuple is of the expected format.
 *
 * Returns: Return code: rc == 0 => success; anything else => failure
 */
static int
check_current_tuple(kvstore_basic_iterator *it, const int expected_i)
{
   int rc = 0;

   char expected_key[MAX_KEY_SIZE]          = {0};
   char expected_val[TEST_SHORT_VALUE_SIZE] = {0};
   ASSERT_EQUAL(
      6, snprintf(expected_key, sizeof(expected_key), key_fmt, expected_i));
   ASSERT_EQUAL(
      6, snprintf(expected_val, sizeof(expected_val), val_fmt, expected_i));

   const char *key;
   const char *val;
   size_t      key_len, val_len;

   kvstore_basic_iter_get_current(it, &key, &key_len, &val, &val_len);

   ASSERT_EQUAL(TEST_INSERT_KEY_LENGTH, key_len);
   ASSERT_EQUAL(TEST_INSERT_VAL_LENGTH, val_len);

   int key_cmp = memcmp(expected_key, key, key_len);
   int val_cmp = memcmp(expected_val, val, val_len);
   ASSERT_EQUAL(0, key_cmp);
   ASSERT_EQUAL(0, val_cmp);

   return rc;
}
