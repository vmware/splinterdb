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

 *     To compile this into a standalone program, just rename the function
 *     kvstore_basic_test() to be main(), and ensure you've got the
 *     kvstore_basic.h header and libsplinterdb.a available for linking.
 *
 *     $ cc -L splinterdb/lib -I splinterdb/include \
 *          my_program.c -lsplinterdb -lxxhash -laio -lpthread -lm
 */

#include "splinterdb/kvstore_basic.h"
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define Mega (1024UL * 1024UL)

#define TEST_DB_NAME "db"

// Hard-coded format strings to generate key and values
static const char key_fmt[] = "key-%02x";
static const char val_fmt[] = "val-%02x";

// We use only short values while loading data in these test cases
#define TEST_SHORT_VALUE_SIZE MAX_KEY_SIZE

// Function prototypes
static int
insert_keys(kvstore_basic *kvsb, const int minkey, int numkeys, const int incr);

static int
setup_kvstore_basic(kvstore_basic **kvsb, kvstore_basic_cfg *cfg)
{
   // fprintf(stderr, "kvstore_basic_test: setup\n");

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
   if (rc != 0) {
      fprintf(stderr, "setup: init error: %d\n", rc);
      return -1;
   }
   kvstore_basic_register_thread(*kvsb);
   return 0;
}

#define test_assert(expression, format, ...)                                   \
   {                                                                           \
      if (!(expression)) {                                                     \
         fprintf(stderr, format, ##__VA_ARGS__);                               \
         fprintf(stderr, "\n");                                                \
         rc = -1;                                                              \
         goto cleanup;                                                         \
      }                                                                        \
   }

#define test_assert_rc(expression, format, ...)                                \
   {                                                                           \
      int inner_rc = (expression);                                             \
      test_assert(                                                             \
         0 == inner_rc, "exit code %d: " format, inner_rc, ##__VA_ARGS__);     \
   }


int
test_kvstore_basic_flow()
{
   kvstore_basic *   kvsb;
   kvstore_basic_cfg cfg = {0};

   int rc = setup_kvstore_basic(&kvsb, &cfg);
   if (rc != 0) {
      return -1;
   }

   fprintf(stderr, "kvstore_basic_test: initializing test data\n");
   char * key     = "some-key";
   size_t key_len = sizeof("some-key");
   bool   found, val_truncated;
   char * value = calloc(1, cfg.max_value_size);
   size_t val_len;
   char * large_key = calloc(1, cfg.max_key_size);

   fprintf(stderr, "kvstore_basic_test: lookup non-existent key...");
   rc = kvstore_basic_lookup(kvsb,
                             key,
                             key_len,
                             value,
                             cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   test_assert_rc(rc, "lookup non-existent key: %d", rc);
   test_assert(!found, "lookup non-existent key: unexpectedly found!");

   fprintf(stderr, "kvstore_basic_test: inserting key with value some-value\n");
   rc = kvstore_basic_insert(
      kvsb, key, key_len, "some-value", sizeof("some-value"));
   test_assert_rc(rc, "insert: %d", rc);

   fprintf(stderr, "kvstore_basic_test: lookup #2...");
   rc = kvstore_basic_lookup(kvsb,
                             key,
                             key_len,
                             value,
                             cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   test_assert_rc(rc, "lookup #2: %d", rc);
   test_assert(found, "lookup #2: unexpectedly not found");
   test_assert(val_len == sizeof("some-value"),
               "lookup #2: unexpected length: %lu",
               val_len);
   test_assert_rc(memcmp(value, "some-value", val_len),
                  "lookup #2: wrong value");

   fprintf(stderr, "kvstore_basic_test: delete key\n");
   rc = kvstore_basic_delete(kvsb, key, key_len);
   test_assert_rc(rc, "delete: %d", rc);

   fprintf(stderr, "kvstore_basic_test: lookup #3, for now-deleted key...");
   rc = kvstore_basic_lookup(kvsb,
                             key,
                             key_len,
                             value,
                             cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   test_assert_rc(rc, "lookup #3: %d", rc);
   test_assert(!found, "lookup #3: unexpectedly found");

   fprintf(stderr, "kvstore_basic_test: add key of max length...\n");
   memset(large_key, 7, cfg.max_key_size);
   rc = kvstore_basic_insert(
      kvsb, large_key, cfg.max_key_size, "a-value", sizeof("a-value"));
   test_assert_rc(rc, "insert key with max-length");

   fprintf(stderr, "kvstore_basic_test: lookup #4 for large key...\n");
   rc = kvstore_basic_lookup(kvsb,
                             large_key,
                             cfg.max_key_size,
                             value,
                             cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   test_assert_rc(rc, "lookup #4: %d", rc);
   test_assert(found, "lookup #4: unexpectedly not found");
   test_assert(val_len == sizeof("a-value"), "lookup #4: wrong length");
   fprintf(stderr, "%s: PASS\n", __FUNCTION__);

cleanup:
   kvstore_basic_close(kvsb);
   if (large_key)
      free(large_key);
   if (rc) {
      fprintf(stderr, "%s: FAILED\n", __FUNCTION__);
      rc = -1;
   }
   return rc;
}

/*
 * Exercise test case to verify core interfaces dealing with max key-size.
 */
int
test_kvstore_basic_large_keys()
{
   kvstore_basic *   kvsb;
   kvstore_basic_cfg cfg = {0};

   int rc = setup_kvstore_basic(&kvsb, &cfg);
   if (rc != 0) {
      return -1;
   }

   fprintf(stderr, "unit_large_keys: add key of max length...\n");
   char *large_key = calloc(1, cfg.max_key_size);
   char *value     = calloc(1, cfg.max_value_size);
   memset(large_key, 7, cfg.max_key_size);
   rc = kvstore_basic_insert(
      kvsb, large_key, cfg.max_key_size, "a-value", sizeof("a-value"));
   test_assert_rc(rc, "insert large key: %d", rc);

   bool   found;
   bool   val_truncated;
   size_t val_len;

   fprintf(stderr, "lookup for large key...\n");
   rc = kvstore_basic_lookup(kvsb,
                             large_key,
                             cfg.max_key_size,
                             value,
                             cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   test_assert_rc(rc, "lookup large key: %d", rc);
   test_assert(found, "lookup large key: unexpectedly not found");
   test_assert(val_len == sizeof("a-value"), "lookup large key: wrong length");

   fprintf(stderr, "lookup correct, now delete...\n");
   rc = kvstore_basic_delete(kvsb, large_key, cfg.max_key_size);
   test_assert_rc(rc, "delete large key: %d", rc);
   fprintf(stderr, "%s: PASS\n", __FUNCTION__);

cleanup:
   if (large_key)
      free(large_key);
   if (value)
      free(value);
   kvstore_basic_close(kvsb);
   if (rc) {
      fprintf(stderr, "%s: FAILED\n", __FUNCTION__);
      rc = -1;
   }
   return rc;
}

/*
 * Test case to verify core interfaces when key-size is > max key-size.
 */
int
test_kvstore_basic_key_size_gt_max_key_size()
{
   kvstore_basic *   kvsb;
   kvstore_basic_cfg cfg = {0};

   int rc = setup_kvstore_basic(&kvsb, &cfg);
   if (rc != 0) {
      return -1;
   }

   size_t too_large_key_len = cfg.max_key_size + 1;
   char * too_large_key     = calloc(1, too_large_key_len);
   memset(too_large_key, 'a', too_large_key_len);
   char *value = calloc(1, cfg.max_value_size);

   rc = kvstore_basic_insert(
      kvsb, too_large_key, too_large_key_len, "a-value", sizeof("a-value"));
   test_assert(rc == EINVAL, "insert too-large key: %d", rc);

   rc = kvstore_basic_delete(kvsb, too_large_key, too_large_key_len);
   test_assert(rc == EINVAL, "delete too-large key: %d", rc);

   bool   found;
   bool   val_truncated;
   size_t val_len;
   rc = kvstore_basic_lookup(kvsb,
                             too_large_key,
                             too_large_key_len,
                             value,
                             cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   test_assert(rc == EINVAL, "lookup too-large key: %d", rc);

   fprintf(stderr, "large key handling is correct\n");
   rc = 0;
   fprintf(stderr, "%s: PASS\n", __FUNCTION__);

cleanup:
   if (too_large_key)
      free(too_large_key);
   if (value)
      free(value);

   kvstore_basic_close(kvsb);
   if (rc) {
      fprintf(stderr, "%s: FAILED\n", __FUNCTION__);
      rc = -1;
   }
   return rc;
}

/*
 * Test case to verify core interfaces when value-size is > max value-size
 * and key-size is also max-key-size, then insert will fail.
 * Here, we basically exercise the insert interface, which will trip up
 * because for chunking up wide-values we need at least 1 spare byte in the
 * key (to generate the chunk counter).
 */
int
test_kvstore_insert_value_gt_max_value_size_with_max_key()
{
   kvstore_basic *   kvsb;
   kvstore_basic_cfg cfg = {0};

   int rc = setup_kvstore_basic(&kvsb, &cfg);
   if (rc != 0) {
      return -1;
   }

   size_t max_key_len = cfg.max_key_size;
   char *max_key = calloc(1, max_key_len);
   size_t            too_large_value_len = cfg.max_value_size + 1;
   char *            too_large_value     = calloc(1, too_large_value_len);

   memset(max_key, 'M', max_key_len);
   memset(too_large_value, 'z', too_large_value_len);
   rc = kvstore_basic_insert(
      kvsb, max_key, max_key_len, too_large_value, too_large_value_len);

   test_assert(rc == EINVAL, "insert too-large value, with max-key-size (%lu): %d",
               max_key_len, rc);
   fprintf(stderr, "%s: PASS\n", __FUNCTION__);
   rc = 0;

cleanup:
   if (max_key)
       free(max_key);
   if (too_large_value)
      free(too_large_value);
   kvstore_basic_close(kvsb);

   if (rc == 0) {
      fprintf(stderr, "succeeded\n");
      return 0;
   } else {
      fprintf(stderr, "FAILED\n");
      return -1;
   }
}

/*
 * Test case to verify core interfaces when value-size is > max value-size
 * but key-size is short-enough that we can still do chunked insertions.
 */
int
test_kvstore_insert_value_size_gt_max_value_size_with_short_keys()
{
   kvstore_basic *   kvsb;
   kvstore_basic_cfg cfg = {0};

   int rc = setup_kvstore_basic(&kvsb, &cfg);
   if (rc != 0) {
      return -1;
   }

   size_t            too_large_value_len = cfg.max_value_size + 1;
   char *            too_large_value     = calloc(1, too_large_value_len);
   static const char short_key[]         = "a_short_key";

   memset(too_large_value, 'z', too_large_value_len);
   rc = kvstore_basic_insert(
      kvsb, short_key, sizeof(short_key), too_large_value, too_large_value_len);

   // test_assert(rc == EINVAL, "insert too-large value: %d", rc);
   test_assert_rc(rc, "insert too-large value: %d", rc);

   fprintf(stderr, "%s: PASS\n", __FUNCTION__);
   rc = 0;

cleanup:
   if (too_large_value)
      free(too_large_value);
   kvstore_basic_close(kvsb);
   if (rc == 0) {
      fprintf(stderr, "succeeded\n");
      return 0;
   } else {
      fprintf(stderr, "FAILED\n");
      return -1;
   }
}

/*
 * Test case to verify core interfaces for very large values which will
 * be chunked up in multiple inserts.
 */
int
test_kvstore_insert_very_large_values_with_short_keys()
{
   kvstore_basic *   kvsb;
   kvstore_basic_cfg cfg = {0};

   int rc = setup_kvstore_basic(&kvsb, &cfg);
   if (rc != 0) {
      return -1;
   }

   size_t            too_large_value_len = (3 * cfg.max_value_size) + 100;
   char *            too_large_value     = calloc(1, too_large_value_len);
   static const char short_key[]         = "a_short_key";

   memset(too_large_value, 'Z', too_large_value_len);
   rc = kvstore_basic_insert(
      kvsb, short_key, sizeof(short_key), too_large_value, too_large_value_len);

   test_assert_rc(rc, "insert very-wide values: %d", rc);

   fprintf(stderr, "%s: PASS\n", __FUNCTION__);
   rc = 0;

cleanup:
   if (too_large_value)
      free(too_large_value);
   kvstore_basic_close(kvsb);
   if (rc) {
      fprintf(stderr, "%s: FAILED\n", __FUNCTION__);
      rc = -1;
   }
   return rc;
}

/*
 * Test case to verify lookup of wide-values with short keys. Insert would
 * have chunked up wide value into multiple inserts, with sequentially
 * numbered keys. Verify that a warning message is generated by the 1st
 * lookup. The bulk of this test builds upon
 * test_kvstore_insert_very_large_values_with_short_keys()
 */
int
test_kvstore_lookup_very_large_values_with_short_keys()
{
   kvstore_basic *   kvsb;
   kvstore_basic_cfg cfg = {0};

   int rc = setup_kvstore_basic(&kvsb, &cfg);
   if (rc != 0) {
      return -1;
   }

   size_t            too_large_value_len = (3 * cfg.max_value_size) + 100;
   char *            too_large_value     = calloc(1, too_large_value_len);
   static const char short_key[]         = "a_short_key";

   memset(too_large_value, 'Z', too_large_value_len);
   rc = kvstore_basic_insert(
      kvsb, short_key, sizeof(short_key), too_large_value, too_large_value_len);

   test_assert_rc(rc, "insert very-wide values: %d", rc);

   bool   found;
   bool   val_truncated;
   size_t val_len;
   char *value     = calloc(1, cfg.max_value_size);
   rc = kvstore_basic_lookup(kvsb,
                             short_key,
                             sizeof(short_key),
                             value,
                             cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   test_assert_rc(rc, "lookup wide-key: %d", rc);

   fprintf(stderr, "%s: PASS\n", __FUNCTION__);
   rc = 0;

cleanup:
   if (too_large_value)
      free(too_large_value);
   if (value)
      free(value);
   kvstore_basic_close(kvsb);
   if (rc == 0) {
      fprintf(stderr, "succeeded\n");
      return 0;
   } else {
      fprintf(stderr, "FAILED\n");
      return -1;
   }
}

int
test_kvstore_basic_variable_length_values()
{
   kvstore_basic *   kvsb;
   kvstore_basic_cfg cfg = {0};

   int rc = setup_kvstore_basic(&kvsb, &cfg);
   if (rc != 0) {
      return -1;
   }

   const char empty_string[0];
   const char short_string[1] = "v";
   const char long_string[]   = "some-long-value";

   rc = kvstore_basic_insert(
      kvsb, "empty", sizeof("empty"), empty_string, sizeof(empty_string));
   test_assert_rc(rc, "insert of empty value: %d", rc);

   rc = kvstore_basic_insert(
      kvsb, "short", sizeof("short"), short_string, sizeof(short_string));
   test_assert_rc(rc, "insert of short value: %d", rc);

   rc = kvstore_basic_insert(
      kvsb, "long", sizeof("long"), long_string, sizeof(long_string));
   test_assert_rc(rc, "insert of long value: %d", rc);

   bool found, val_truncated;

   // add extra length so we can check for overflow
   char found_value[KVSTORE_BASIC_MAX_VALUE_SIZE + 2];
   memset(found_value, 'x', sizeof(found_value));

   size_t val_len;

   fprintf(stderr, "lookup tuple with empty value\n");
   rc = kvstore_basic_lookup(kvsb,
                             "empty",
                             sizeof("empty"),
                             found_value,
                             cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   test_assert_rc(rc, "lookup for empty value: %d", rc);
   test_assert(found, "lookup for empty value: not found");
   test_assert(!val_truncated, "lookup for empty value: unexpected truncate");
   test_assert(val_len == 0, "lookup for empty value: unexpected length");

   fprintf(
      stderr,
      "lookup tuple with value of length 1, providing sufficient buffer\n");
   rc = kvstore_basic_lookup(kvsb,
                             "short",
                             sizeof("short"),
                             found_value,
                             cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   test_assert_rc(rc, "lookup for short value: %d", rc);
   test_assert(found, "lookup for short value: not found");
   test_assert(
      !val_truncated,
      "lookup for short value with sufficient buffer: unexpected truncate");
   test_assert(val_len == 1, "lookup for short value: unexpected length");
   test_assert_rc(memcmp(short_string, found_value, val_len),
                  "expected to find value=%.*s but instead found %.*s",
                  (int)(sizeof(short_string)),
                  short_string,
                  (int)(val_len),
                  found_value)

      fprintf(stderr,
              "lookup tuple with value of length 1, providing empty buffer\n");
   rc = kvstore_basic_lookup(kvsb,
                             "short",
                             sizeof("short"),
                             found_value,
                             0, // test case
                             &val_len,
                             &val_truncated,
                             &found);
   test_assert_rc(rc, "lookup for short value, empty buffer: %d", rc);
   test_assert(found, "lookup for short value, empty buffer: not found");
   test_assert(
      val_truncated,
      "lookup for short value, empty buffer: unexpectedly did not truncate");
   test_assert(val_len == 0,
               "lookup for short value, empty buffer: unexpected length");

   fprintf(stderr, "lookup tuple with max-sized-value\n");
   rc = kvstore_basic_lookup(kvsb,
                             "long",
                             sizeof("long"),
                             found_value,
                             cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   test_assert_rc(rc, "lookup for long value: %d", rc);
   test_assert(found, "lookup for long value: not found");
   test_assert(!val_truncated, "lookup for long value: unexpectedly truncated");
   test_assert(val_len == sizeof(long_string),
               "lookup for long value: unexpected length");
   test_assert_rc(memcmp(long_string, found_value, val_len),
                  "expected to find value=%.*s but instead found %.*s",
                  (int)(sizeof(long_string)),
                  long_string,
                  (int)(val_len),
                  found_value)

      fprintf(stderr, "lookup tuple with max-sized-value, short buffer\n");
   rc = kvstore_basic_lookup(kvsb,
                             "long",
                             sizeof("long"),
                             found_value,
                             5,
                             &val_len,
                             &val_truncated,
                             &found);
   test_assert_rc(rc, "lookup for long value, short buffer: %d", rc);
   test_assert(found, "lookup for long value, short buffer: not found");
   test_assert(val_truncated,
               "lookup for long value: unexpectedly did not truncate");
   test_assert(val_len == 5, "lookup for long value: unexpected length");
   test_assert_rc(memcmp(long_string, found_value, val_len),
                  "expected to find value=%.*s but instead found %.*s",
                  (int)(sizeof(long_string)),
                  long_string,
                  (int)(val_len),
                  found_value);
   fprintf(stderr, "%s: PASS\n", __FUNCTION__);

cleanup:
   kvstore_basic_close(kvsb);
   if (rc) {
      fprintf(stderr, "%s: FAILED\n", __FUNCTION__);
      rc = -1;
   }
   return rc;
}

#define TEST_INSERT_KEY_LENGTH 7
#define TEST_INSERT_VAL_LENGTH 7

/*
 * Helper function to insert n-keys (num_inserts), using pre-formatted
 * key and value strings.
 *
 * Returns: Return code: rc == 0 => success; anything else => failure
 */
int
insert_some_keys(const int num_inserts, kvstore_basic *kvsb)
{
   int rc = 0;
   fprintf(stderr, "inserting %d keys", num_inserts);
   // insert keys backwards, just for kicks
   for (int i = num_inserts - 1; i >= 0; i--) {
      fprintf(stderr, ".");
      char key[TEST_INSERT_KEY_LENGTH] = {0};
      char val[TEST_INSERT_VAL_LENGTH] = {0};

      test_assert(6 == snprintf(key, sizeof(key), key_fmt, i), "key length");
      test_assert(6 == snprintf(val, sizeof(val), val_fmt, i), "val length");

      rc = kvstore_basic_insert(kvsb, key, sizeof(key), val, sizeof(val));
      test_assert_rc(rc, "insert: %d", rc);
   }
   fprintf(stderr, "\n");

cleanup:
   return rc;
}

/*
 * Helper function to insert n-keys (num_inserts), using pre-formatted
 * key and value strings. Allows user to specify start value and increment
 * between keys. This can be used to load either fully sequential keys
 * or some with defined gaps.
 *
 * Parameters:
 *  kvsb    - Ptr to KVStore handle
 *  minkey  - Start key to insert
 *  numkeys - # of keys to insert
 *  incr    - Increment between keys (default is 1)
 *
 * Returns: Return code: rc == 0 => success; anything else => failure
 */
static int
insert_keys(kvstore_basic *kvsb, const int minkey, int numkeys, const int incr)
{
   int rc = -1;

   // Minimally, error check input arguments
   if (!kvsb || (numkeys <= 0) || (incr < 0))
      return rc;

   // insert keys forwards, starting from minkey value
   for (int kctr = minkey; numkeys; kctr += incr, numkeys--) {
      char key[TEST_INSERT_KEY_LENGTH] = {0};
      char val[TEST_INSERT_VAL_LENGTH] = {0};

      snprintf(key, sizeof(key), key_fmt, kctr);
      snprintf(val, sizeof(val), val_fmt, kctr);

      rc = kvstore_basic_insert(kvsb, key, sizeof(key), val, sizeof(val));
      test_assert_rc(rc, "insert key=%d: rc=%d", kctr, rc);
   }
   rc = 0;

cleanup:
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
int
check_current_tuple(kvstore_basic_iterator *it, const int expected_i)
{
   int  rc               = 0;
   char expected_key[MAX_KEY_SIZE]          = {0};
   char expected_val[TEST_SHORT_VALUE_SIZE] = {0};
   test_assert(
      6 == snprintf(expected_key, sizeof(expected_key), key_fmt, expected_i),
      "key");
   test_assert(
      6 == snprintf(expected_val, sizeof(expected_val), val_fmt, expected_i),
      "val");

   const char *key;
   const char *val;
   size_t      key_len, val_len;

   kvstore_basic_iter_get_current(it, &key, &key_len, &val, &val_len);

   test_assert(
      TEST_INSERT_KEY_LENGTH == key_len, "wrong key length: %lu", key_len);
   test_assert(
      TEST_INSERT_VAL_LENGTH == val_len, "wrong value length: %lu", val_len);
   int key_cmp = memcmp(expected_key, key, key_len);
   int val_cmp = memcmp(expected_val, val, val_len);
   test_assert(0 == key_cmp,
               "key match failed: expected key='%s'"
               ", found key='%.*s', key_cmp=%d",
               expected_key,
               (int)key_len,
               key,
               key_cmp);
   test_assert(0 == val_cmp,
               "val match failed: expected val='%s'"
               ", found val='%.*s', val_cmp=%d",
               expected_val,
               (int)val_len,
               val,
               val_cmp);

cleanup:
   return rc;
}


int
test_kvstore_basic_iterator()
{
   kvstore_basic *         kvsb = NULL;
   kvstore_basic_cfg       cfg  = {0};
   kvstore_basic_iterator *it   = NULL;
   int                     rc   = 0;

   test_assert_rc(setup_kvstore_basic(&kvsb, &cfg), "setup");

   const int num_inserts = 50;
   test_assert_rc(insert_some_keys(num_inserts, kvsb), "inserting keys ");
   fprintf(stderr, "now using iterator:");

   test_assert_rc(kvstore_basic_iter_init(kvsb, &it, NULL, 0), "init iter");

   int i = 0;
   for (; kvstore_basic_iter_valid(it); kvstore_basic_iter_next(it)) {
      test_assert_rc(check_current_tuple(it, i), "check current");
      fprintf(stderr, ".%d.", i);
      i++;
   }

   fprintf(stderr, "checking status...\n");
   test_assert_rc(kvstore_basic_iter_status(it),
                  "iterator stopped with error status: %d",
                  rc);

   test_assert(
      i == num_inserts, "iterator stopped at %d, expected %d", i, num_inserts);

   test_assert(!kvstore_basic_iter_valid(it),
               "iterator still valid, this should not happen");

   fprintf(stderr, "OK.  iterator test complete\n");
   fprintf(stderr, "%s: PASS\n", __FUNCTION__);

cleanup:
   if (it != NULL) {
      kvstore_basic_iter_deinit(&it);
   }
   if (kvsb != NULL) {
      kvstore_basic_close(kvsb);
   }
   if (rc) {
      fprintf(stderr, "%s: FAILED\n", __FUNCTION__);
      rc = -1;
   }
   return rc;
}

/*
 * Test case to exercise and verify that kvstore iterator interfaces with a
 * non-NULL start key correctly sets up the start scan at the requested
 * initial key value.
 */
int
test_kvstore_iterator_with_startkey()
{
   kvstore_basic *         kvsb = NULL;
   kvstore_basic_cfg       cfg  = {0};
   kvstore_basic_iterator *it   = NULL;
   int                     rc   = 0;

   test_assert_rc(setup_kvstore_basic(&kvsb, &cfg), "setup");

   const int num_inserts = 50;
   test_assert_rc(insert_some_keys(num_inserts, kvsb), "inserting keys ");

   char key[TEST_INSERT_KEY_LENGTH] = {0};

   for (int ictr = 0; ictr < num_inserts; ictr++) {

      // Initialize the i'th key
      snprintf(key, sizeof(key), key_fmt, ictr);
      test_assert_rc(kvstore_basic_iter_init(kvsb, &it, key, strlen(key)),
                     "init iter");

      test_assert(kvstore_basic_iter_valid(it), "iter is valid");

      // Scan should have been positioned at the i'th key
      test_assert_rc(check_current_tuple(it, ictr), "check current");

      kvstore_basic_iter_deinit(&it);
   }
   fprintf(stderr, "%s: PASS\n", __FUNCTION__);

cleanup:
   if (it != NULL) {
      kvstore_basic_iter_deinit(&it);
   }
   if (kvsb != NULL) {
      kvstore_basic_close(kvsb);
   }
   if (rc) {
      fprintf(stderr, "%s: FAILED\n", __FUNCTION__);
      rc = -1;
   }
   return rc;
}

/*
 * Test case to exercise kvstore iterator with a non-NULL but non-existent
 * start-key. The iterator just starts at the first key, if any, after the
 * specified start-key.
 *  . If start-key > max-key, we will find no more keys to scan.
 *  . If start-key < min-key, we will start scan from 1st key in set.
 */
int
test_kvstore_iterator_with_non_existent_startkey()
{
   kvstore_basic *         kvsb = NULL;
   kvstore_basic_cfg       cfg  = {0};
   kvstore_basic_iterator *it   = NULL;
   int                     rc   = 0;

   test_assert_rc(setup_kvstore_basic(&kvsb, &cfg), "setup");

   const int num_inserts = 50;
   test_assert_rc(insert_some_keys(num_inserts, kvsb), "inserting keys ");

   // start-key > max-key ('key-50')
   char *key = "unknownKey";

   test_assert_rc(kvstore_basic_iter_init(kvsb, &it, key, strlen(key)),
                  "init iter with non-existent start key > max-key");

   test_assert(!kvstore_basic_iter_valid(it), "iterator should be invalid");

   kvstore_basic_iter_deinit(&it);

   // If you start with a key before min-key-value, scan will start from
   // 1st key inserted. (We do lexicographic comparison, so 'U' sorts
   // before 'key...', which is what key's format is.)
   key = "UnknownKey";
   test_assert_rc(kvstore_basic_iter_init(kvsb, &it, key, strlen(key)),
                  "init iter with non-existent start key < min-key");

   // Iterator should be initialized to 1st key inserted, if the supplied
   // start_key is not found, but below the min-key inserted.
   int ictr = 0;
   test_assert_rc(check_current_tuple(it, ictr), "check current");

   // Just to be sure, run through the set of keys, to cross-check that
   // we are getting all of them back in the right order.
   for (; kvstore_basic_iter_valid(it); kvstore_basic_iter_next(it)) {
      test_assert_rc(check_current_tuple(it, ictr), "check current");
      ictr++;
   }

   // We should have run through all the keys inserted
   test_assert((ictr == num_inserts),
               "Expected to find all %d keys, only processed %d keys",
               num_inserts,
               ictr);

   fprintf(stderr, "%s: PASS\n", __FUNCTION__);

cleanup:
   if (it != NULL) {
      kvstore_basic_iter_deinit(&it);
   }
   if (kvsb != NULL) {
      kvstore_basic_close(kvsb);
   }
   if (rc) {
      fprintf(stderr, "%s: FAILED\n", __FUNCTION__);
      rc = -1;
   }
   return rc;
}

/*
 * Test case to exercise kvstore iterator with a non-NULL but non-existent
 * start-key.  The data in this test case is loaded such that we have a
 * sequence of key values with gaps of 2 (i.e. 1, 4, 7, 10, ...).
 *
 * Then, there are basically 4 sub-cases we exercise here:
 *
 *  a) start-key exactly == min-key
 *  b) start-key < min-key
 *  c) start-key between some existing key values; (Choose 5, which should
 *      end up starting the scan at 7.)
 *  d) start-key beyond max-key (Scan should come out as invalid.)
 */
int
test_kvstore_iterator_with_missing_startkey_in_sequence()
{
   kvstore_basic *         kvsb = NULL;
   kvstore_basic_cfg       cfg  = {0};
   kvstore_basic_iterator *it   = NULL;
   int                     rc   = 0;

   test_assert_rc(setup_kvstore_basic(&kvsb, &cfg), "setup");

   const int num_inserts = 50;
   // Should insert keys: 1, 4, 7, 10 13, 16, 19, ...
   int minkey = 1;
   test_assert_rc(insert_keys(kvsb, minkey, num_inserts, 3),
                  "insert keys with incr=3");

   char key[TEST_INSERT_KEY_LENGTH];

   // (a) Test iter_init with a key == the min-key
   snprintf(key, sizeof(key), key_fmt, minkey);

   test_assert_rc(kvstore_basic_iter_init(kvsb, &it, key, strlen(key)),
                  "init iter with start key == min-key");

   test_assert(kvstore_basic_iter_valid(it), "iterator should be valid");

   // Iterator should be initialized to 1st key inserted, if the supplied
   // start_key is below min-key inserted thus far.
   int ictr = minkey;
   test_assert_rc(check_current_tuple(it, ictr), "check current ictr=<minkey>");

   kvstore_basic_iter_deinit(&it);

   // (b) Test iter_init with a value below the min-key-value.
   int kctr = (minkey - 1);

   snprintf(key, sizeof(key), key_fmt, kctr);

   test_assert_rc(kvstore_basic_iter_init(kvsb, &it, key, strlen(key)),
                  "init iter with start key less than min-key");

   test_assert(kvstore_basic_iter_valid(it),
               "iterator should be valid, kctr==(minkey - 1)");

   // Iterator should be initialized to 1st key inserted, if the supplied
   // start_key is below min-key inserted thus far.
   ictr = minkey;
   test_assert_rc(check_current_tuple(it, ictr), "check current, expected 1");

   kvstore_basic_iter_deinit(&it);

   // (c) Test with a non-existent value between 2 valid key values.
   kctr = 5;
   snprintf(key, sizeof(key), key_fmt, kctr);

   test_assert_rc(kvstore_basic_iter_init(kvsb, &it, key, strlen(key)),
                  "init iter with non-existent start key");

   test_assert(kvstore_basic_iter_valid(it),
               "iterator should be valid, kctr=5");

   // Iterator should be initialized to next key following kctr.
   ictr = 7;
   test_assert_rc(check_current_tuple(it, ictr), "check current expected 7");

   kvstore_basic_iter_deinit(&it);

   // (d) Test with a non-existent value beyond max key value.
   //     iter_init should end up as being invalid.
   kctr = -1;
   snprintf(key, sizeof(key), key_fmt, kctr);

   test_assert_rc(kvstore_basic_iter_init(kvsb, &it, key, strlen(key)),
                  "init iter with non-existent start key beyond max-key");

   test_assert(!kvstore_basic_iter_valid(it),
               "iterator should not be valid, kctr=-1");

   fprintf(stderr, "%s: PASS\n", __FUNCTION__);

cleanup:
   if ((it != NULL) && kvstore_basic_iter_valid(it)) {
      kvstore_basic_iter_deinit(&it);
   }
   if (kvsb != NULL) {
      kvstore_basic_close(kvsb);
   }
   if (rc) {
      fprintf(stderr, "%s: FAILED\n", __FUNCTION__);
      rc = -1;
   }
   return rc;
}


static uint64_t key_comp_context = 0;

// a spy comparator
int
custom_key_comparator(const void *context,
                      const void *key1,
                      size_t      key1_len,
                      const void *key2,
                      size_t      key2_len)
{
   // check the key lengths match what we inserted
   assert(key1_len <= 21);
   assert(key2_len <= 21);
   size_t min_len = (key1_len <= key2_len ? key1_len : key2_len);
   assert(key1 != NULL && key2 != NULL);
   int r = memcmp(key1, key2, min_len);
   if (r == 0) {
      if (key1_len < key2_len)
         r = -1;
      else if (key1_len > key2_len)
         r = +1;
   }
   uint64_t *counter = (uint64_t *)context;
   *counter += 1;
   return r;
}

int
test_kvstore_basic_iterator_custom_comparator()
{
   kvstore_basic *         kvsb = NULL;
   kvstore_basic_cfg       cfg  = {0};
   kvstore_basic_iterator *it   = NULL;
   int                     rc   = 0;

   cfg.key_comparator         = &custom_key_comparator;
   cfg.key_comparator_context = &key_comp_context;

   test_assert_rc(setup_kvstore_basic(&kvsb, &cfg), "setup");

   const int num_inserts = 50;
   test_assert_rc(insert_some_keys(num_inserts, kvsb), "inserting keys ");
   fprintf(stderr, "now using iterator:");

   test_assert_rc(kvstore_basic_iter_init(kvsb, &it, NULL, 0), "init iter");

   int i = 0;
   for (; kvstore_basic_iter_valid(it); kvstore_basic_iter_next(it)) {
      test_assert_rc(check_current_tuple(it, i), "check current: %d", i);
      fprintf(stderr, ".");
      i++;
   }

   test_assert_rc(kvstore_basic_iter_status(it),
                  "iterator stopped with error status: %d",
                  rc);

   test_assert(
      i == num_inserts, "iterator stopped at %d, expected %d", i, num_inserts);

   test_assert(key_comp_context > 2 * num_inserts,
               "key comparison count: %lu",
               key_comp_context);

   test_assert(!kvstore_basic_iter_valid(it),
               "iterator still valid, this should not happen");

   fprintf(stderr, "OK.  iterator test complete\n");
   fprintf(stderr, "%s: PASS\n", __FUNCTION__);

cleanup:
   if (it != NULL) {
      fprintf(stderr, "deinit iterator...");
      kvstore_basic_iter_deinit(&it);
   }
   if (kvsb != NULL) {
      fprintf(stderr, "deinit kvstore_basic...");
      kvstore_basic_close(kvsb);
   }
   if (rc) {
      fprintf(stderr, "%s: FAILED\n", __FUNCTION__);
      rc = -1;
   }
   return rc;
}

int
test_kvstore_basic_close_and_reopen()
{
   kvstore_basic *   kvsb = NULL;
   kvstore_basic_cfg cfg  = {0};
   int               rc   = 0;

   fprintf(stderr, "remove old db...");
   test_assert(remove(TEST_DB_NAME) == 0, "removing old db");

   fprintf(stderr, "creating new db...");
   test_assert_rc(setup_kvstore_basic(&kvsb, &cfg), "setup");

   char * key     = "some-key";
   size_t key_len = sizeof("some-key");
   bool   found, val_truncated;
   char * value = calloc(1, cfg.max_value_size);
   size_t val_len;

   fprintf(stderr, "insert...");
   test_assert_rc(kvstore_basic_insert(
                     kvsb, key, key_len, "some-value", sizeof("some-value")),
                  "insert");

   fprintf(stderr, "close and reopen...");
   kvstore_basic_close(kvsb);
   test_assert_rc(kvstore_basic_open(&cfg, &kvsb), "reopen");

   fprintf(stderr, "lookup...");
   rc = kvstore_basic_lookup(kvsb,
                             key,
                             key_len,
                             value,
                             cfg.max_value_size,
                             &val_len,
                             &val_truncated,
                             &found);
   test_assert_rc(rc, "lookup: %d", rc);
   test_assert(found, "ERROR: unexpectedly lookup did not succeed.");

   fprintf(stderr, "%s: PASS\n", __FUNCTION__);

cleanup:
   if (kvsb != NULL) {
      fprintf(stderr, "deinit kvstore_basic...");
      kvstore_basic_close(kvsb);
   }
   if (rc) {
      fprintf(stderr, "%s: FAILED\n", __FUNCTION__);
      rc = -1;
   }
   return rc;
}

int
test_kvstore_basic_lots_of_data()
{
   kvstore_basic *   kvsb;
   kvstore_basic_cfg cfg = {0};

   cfg.cache_size     = 200 * Mega;
   cfg.disk_size      = 900 * Mega;
   cfg.max_key_size   = 22;
   cfg.max_value_size = 116;
   int rc             = setup_kvstore_basic(&kvsb, &cfg);
   if (rc != 0) {
      return -1;
   }

   int random_data = open("/dev/urandom", O_RDONLY);
   if (random_data < 0) {
      return -1;
   }

   char key_buf[KVSTORE_BASIC_MAX_KEY_SIZE]     = {0};
   char value_buf[KVSTORE_BASIC_MAX_VALUE_SIZE] = {0};

   fprintf(stderr, "writing lots of data...");
   for (uint64_t i = 0; i < 2 * Mega; i++) {
      size_t result = read(random_data, key_buf, sizeof key_buf);
      if (result < 0) {
         rc = -1;
         break;
      }
      result = read(random_data, value_buf, sizeof key_buf);
      if (result < 0) {
         rc = -1;
         break;
      }
      rc = kvstore_basic_insert(
         kvsb, key_buf, cfg.max_key_size, value_buf, cfg.max_value_size);
      test_assert_rc(rc, "insert: %d", rc);
   }
   fprintf(stderr, "%s: PASS\n", __FUNCTION__);

cleanup:
   kvstore_basic_close(kvsb);
   if (rc) {
      fprintf(stderr, "%s: FAILED\n", __FUNCTION__);
      rc = -1;
   }
   return rc;
}

int
kvstore_basic_test(int argc, char *argv[])
{
   int rc = 0;

   fprintf(stderr, "\nstart: kvstore_basic flow\n");
   test_assert_rc(test_kvstore_basic_flow(), "kvstore_basic_flow");

   fprintf(stderr, "\nstart: kvstore_basic large keys\n");
   test_assert_rc(test_kvstore_basic_large_keys(), "kvstore_basic_large_keys");

   test_assert_rc(test_kvstore_basic_key_size_gt_max_key_size(),
                  "kvstore_basic_key_size_gt_max_key_size");

   test_assert_rc(test_kvstore_insert_value_gt_max_value_size_with_max_key(),
                  "kvstore_insert_value_gt_max_value_size_with_max_key");

   test_assert_rc(test_kvstore_insert_value_size_gt_max_value_size_with_short_keys(),
                  "kvstore_insert_value_size_gt_max_value_size_with_short_keys");

   test_assert_rc(test_kvstore_insert_very_large_values_with_short_keys(),
                  "kvstore_insert_very_large_values_with_short_keys");

   test_assert_rc(test_kvstore_lookup_very_large_values_with_short_keys(),
                  "kvstore_lookup_very_large_values_with_short_keys");

   fprintf(stderr, "start: kvstore_basic variable-length values\n");
   test_assert_rc(test_kvstore_basic_variable_length_values(),
                  "kvstore_basic_variable_length_values");

   fprintf(stderr, "\nstart: kvstore_basic iterator\n");
   test_assert_rc(test_kvstore_basic_iterator(), "kvstore_basic_iterator");

   fprintf(stderr, "\nstart: kvstore_basic iterator with start key\n");
   test_assert_rc(test_kvstore_iterator_with_startkey(),
                  "kvstore_iterator_with_startkey");

   fprintf(stderr, "\nstart: kvstore_basic iterator with unknown start key\n");
   test_assert_rc(test_kvstore_iterator_with_non_existent_startkey(),
                  "kvstore_iterator_with_non_existent_startkey");

   fprintf(stderr,
           "\nstart: kvstore_basic iterator with unknown start key"
           " from middle\n");
   test_assert_rc(test_kvstore_iterator_with_missing_startkey_in_sequence(),
                  "kvstore_iterator_with_missing_startkey_in_sequence");

   fprintf(stderr, "\nstart: kvstore_basic iterator with custom comparator\n");
   test_assert_rc(test_kvstore_basic_iterator_custom_comparator(),
                  "kvstore_basic_iterator_custom_comparator");

   fprintf(stderr, "\nstart: kvstore_basic close and re-open\n");
   test_assert_rc(test_kvstore_basic_close_and_reopen(),
                  "kvstore_basic_close_and_reopen");

   fprintf(stderr, "\nstart: kvstore_basic lots of data\n");
   test_assert_rc(test_kvstore_basic_lots_of_data(),
                  "kvstore_basic_lots_of_data");
cleanup:
   if (rc == 0) {
      fprintf(stderr, "OK\n");
   } else {
      fprintf(stderr, "FAILED\n");
   }
   return rc;
}
