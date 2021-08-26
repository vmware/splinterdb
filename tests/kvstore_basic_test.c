// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * kvstore_basic_test.c --
 *
 *     exercises the kvstore_basic API
 *
 *     API deals with keys/values rather than keys/messages
 */

#include "kvstore_basic.h"
#include "util.h"
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define Mega (1024UL * 1024UL)

static int
setup_kvstore_basic(kvstore_basic **kvsb, kvstore_basic_cfg *cfg)
{
   fprintf(stderr, "kvstore_basic_test: setup\n");

   *cfg = (kvstore_basic_cfg){
      .filename       = "db",
      .cache_size     = Mega,
      .disk_size      = 30 * Mega,
      .max_key_size   = 21, // less than MAX_KEY_SIZE, just to try things out
      .max_value_size = 16,
      .key_comparator = cfg->key_comparator,
      .key_comparator_context = cfg->key_comparator_context,
   };

   int rc = kvstore_basic_init(cfg, kvsb);
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
   char *large_key = calloc(1, cfg.max_key_size);
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

cleanup:
   kvstore_basic_deinit(kvsb);
   if (rc == 0) {
      fprintf(stderr, "succeeded\n");
      return 0;
   } else {
      fprintf(stderr, "FAILED\n");
      return -1;
   }
}

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
   memset(large_key, 7, cfg.max_key_size);
   rc = kvstore_basic_insert(
      kvsb, large_key, cfg.max_key_size, "a-value", sizeof("a-value"));
   test_assert_rc(rc, "insert large key: %d", rc);

   bool   found, val_truncated;
   char * value = calloc(1, cfg.max_value_size);
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

   fprintf(stderr, "delete ok, now try with too large of a key...\n");
   size_t too_large_key_len = cfg.max_key_size + 1;
   char * too_large_key     = calloc(1, too_large_key_len);
   memset(too_large_key, 7, too_large_key_len);
   rc = kvstore_basic_insert(
      kvsb, too_large_key, too_large_key_len, "a-value", sizeof("a-value"));
   test_assert(rc == EINVAL, "insert too-large key: %d", rc);

   rc = kvstore_basic_delete(kvsb, too_large_key, too_large_key_len);
   test_assert(rc == EINVAL, "delete too-large key: %d", rc);

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

cleanup:
   kvstore_basic_deinit(kvsb);
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

cleanup:
   kvstore_basic_deinit(kvsb);
   if (rc == 0) {
      fprintf(stderr, "succeeded\n");
   } else {
      fprintf(stderr, "FAILED\n");
   }
   return rc;
}

#define TEST_INSERT_KEY_LENGTH 7
#define TEST_INSERT_VAL_LENGTH 7

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

      test_assert(6 == snprintf(key, sizeof(key), "key-%02x", i), "key length");
      test_assert(6 == snprintf(val, sizeof(val), "val-%02x", i), "val length");

      rc = kvstore_basic_insert(kvsb, key, sizeof(key), val, sizeof(val));
      test_assert_rc(rc, "insert: %d", rc);
   }
   fprintf(stderr, "\n done.\n");

cleanup:
   return rc;
}

int
check_current_tuple(kvstore_basic_iterator *it, const int expected_i)
{
   int  rc               = 0;
   char expected_key[24] = {0};
   char expected_val[24] = {0};
   test_assert(
      6 == snprintf(expected_key, sizeof(expected_key), "key-%02x", expected_i),
      "key");
   test_assert(
      6 == snprintf(expected_val, sizeof(expected_val), "val-%02x", expected_i),
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
   test_assert(0 == key_cmp, "key match failed: %d", key_cmp);
   test_assert(0 == val_cmp, "val match failed: %d", val_cmp);

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

cleanup:
   if (it != NULL) {
      kvstore_basic_iter_deinit(it);
   }
   if (kvsb != NULL) {
      kvstore_basic_deinit(kvsb);
   }
   if (rc == 0) {
      fprintf(stderr, "succeeded\n");
   } else {
      fprintf(stderr, "FAILED\n");
   }
   return rc;
}


static uint64 key_comp_context = 0;

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
   uint64 *counter = (uint64 *)context;
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

cleanup:
   if (it != NULL) {
      fprintf(stderr, "deinit iterator...");
      kvstore_basic_iter_deinit(it);
   }
   if (kvsb != NULL) {
      fprintf(stderr, "deinit kvstore_basic...");
      kvstore_basic_deinit(kvsb);
   }
   if (rc == 0) {
      fprintf(stderr, "succeeded\n");
   } else {
      fprintf(stderr, "FAILED\n");
   }
   return rc;
}

int
kvstore_basic_test(int argc, char *argv[])
{
   int rc = 0;
   fprintf(stderr, "start: kvstore_basic flow\n");
   test_assert_rc(test_kvstore_basic_flow(), "kvstore_basic_flow");

   fprintf(stderr, "start: kvstore_basic large keys\n");
   test_assert_rc(test_kvstore_basic_large_keys(), "kvstore_basic_large_keys");

   fprintf(stderr, "start: kvstore_basic variable-length values\n");
   test_assert_rc(test_kvstore_basic_variable_length_values(),
                  "kvstore_basic_variable_length_values");

   fprintf(stderr, "start: kvstore_basic iterator\n");
   test_assert_rc(test_kvstore_basic_iterator(), "kvstore_basic_iterator");

   fprintf(stderr, "start: kvstore_basic iterator with custom comparator\n");
   test_assert_rc(test_kvstore_basic_iterator_custom_comparator(),
                  "kvstore_basic_iterator_custom_comparator");
cleanup:
   if (rc == 0) {
      fprintf(stderr, "OK\n");
   } else {
      fprintf(stderr, "FAILED\n");
   }
   return rc;
}