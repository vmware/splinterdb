// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * kvstore_test.c --
 *
 *  Tests for the simplified KVStore API.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/platform_public.h"
#include "splinterdb/kvstore.h"
#include "../test_data.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

// Function Prototypes
static int
setup_kvstore(kvstore **kvs, kvstore_config *kvs_cfg);

static int
do_inserts(const int       num_inserts,
           kvstore        *kvs,
           kvstore_config *kvs_cfg,
           char           *key,
           char           *msg_buffer);

/*
 * Global data declaration macro:
 */
CTEST_DATA(kvstore)
{
   kvstore_config kvs_cfg;
   kvstore       *kvs;
   char          *key;
   char          *msg_buffer;
};

// Setup function for suite, called before every test in suite
CTEST_SETUP(kvstore)
{
   memset(&data->kvs_cfg, 0, sizeof(data->kvs_cfg));
   int rc = setup_kvstore(&data->kvs, &data->kvs_cfg);
   ASSERT_EQUAL(0, rc);

   data->key        = calloc(1, data->kvs_cfg.data_cfg.key_size);
   data->msg_buffer = calloc(1, data->kvs_cfg.data_cfg.message_size);

   ASSERT_TRUE((data->key != NULL), "Memory allocation for key failed!\n");
   ASSERT_TRUE((data->msg_buffer != NULL),
               "Memory allocation for msg_buffer failed!\n");
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(kvstore)
{
   if (data->key) {
      free(data->key);
      data->key = NULL;
   }
   if (data->msg_buffer) {
      free(data->msg_buffer);
      data->msg_buffer = NULL;
   }
   kvstore_close(data->kvs);
}

/*
 * Basic test case: Lookup a non-existent key should not find key.
 */
CTEST2(kvstore, test_lookup_non_existent_key)
{
   memcpy(data->key, "foo", 3);

   kvstore_lookup_result result;
   kvstore_lookup_result_init(data->kvs,
                              &result,
                              data->kvs_cfg.data_cfg.message_size,
                              data->msg_buffer);
   int rc = kvstore_lookup(data->kvs, data->key, &result);
   ASSERT_EQUAL(
      0, rc, "kvstore_lookup() of non-existent key failed, rc=%d\n", rc);

   bool found = kvstore_lookup_result_found(&result);
   ASSERT_FALSE(
      found, "found=%d, Unexpectedly found a key we haven't set\n", found);

   kvstore_lookup_result_deinit(&result);
}

/*
 * Verify INSERT, LOOKUP, DELETE APIs
 */
CTEST2(kvstore, test_insert_lookup_delete)
{
   // Exercise INSERT of new key / value pair.
   memcpy(data->key, "foo", 3);
   data_handle *msg  = (data_handle *)data->msg_buffer;
   msg->message_type = MESSAGE_TYPE_INSERT;
   msg->ref_count    = 1;

   const char *exp_val = "bar";
   int         exp_len = strlen(exp_val);
   memcpy((void *)(msg->data), exp_val, exp_len);

   int rc = kvstore_insert(data->kvs,
                           data->key,
                           data->kvs_cfg.data_cfg.message_size,
                           data->msg_buffer);
   ASSERT_EQUAL(
      0, rc, "kvstore_insert() of new key '%s' failed, rc=%d\n", data->key, rc);

   // Exercise lookup: It should successfully retrieve the value for given key
   // Muck with the value, just to see that it gets reset
   msg->message_type = MESSAGE_TYPE_UPDATE;
   snprintf((void *)(msg->data),
            data->kvs_cfg.data_cfg.message_size - offsetof(data_handle, data),
            "zzz");

   kvstore_lookup_result result;
   kvstore_lookup_result_init(data->kvs,
                              &result,
                              data->kvs_cfg.data_cfg.message_size,
                              data->msg_buffer);
   rc = kvstore_lookup(data->kvs, data->key, &result);
   ASSERT_EQUAL(
      0, rc, "kvstore_lookup() for key '%s' failed. rc=%d.", data->key, rc);

   bool found = kvstore_lookup_result_found(&result);
   ASSERT_TRUE(found,
               "kvstore_lookup() for key '%s' failed. found=%d.",
               data->key,
               found);

   size_t result_size = kvstore_lookup_result_size(&result);
   ASSERT_EQUAL(data->kvs_cfg.data_cfg.message_size,
                result_size,
                "Unexpectedly, lookup value is short, message_size = %lu. ",
                result_size);

   if (kvstore_lookup_result_data(&result) != data->msg_buffer) {
      msg = kvstore_lookup_result_data(&result);
   }

   ASSERT_STREQN(exp_val,
                 (const char *)msg->data,
                 exp_len,
                 "Lookup returned an unexpected value for data = '%.*s'."
                 " Expected value is '%.*s'. ",
                 exp_len,
                 (char *)msg->data,
                 exp_len,
                 exp_val);

   // Exercise DELETE of existing key should succeed.
   msg->message_type = MESSAGE_TYPE_DELETE;
   rc                = kvstore_insert(data->kvs,
                       data->key,
                       data->kvs_cfg.data_cfg.message_size,
                       data->msg_buffer);
   ASSERT_EQUAL(0, rc, "kvstore_insert (for delete) failed, rc=%d. ", rc);

   // Lookup of now-deleted key should succeed, but not find the key
   rc = kvstore_lookup(data->kvs, data->key, &result);
   ASSERT_EQUAL(0,
                rc,
                "kvstore_lookup() for now-deleted key '%s' failed, rc=%d. ",
                data->key,
                rc);

   found = kvstore_lookup_result_found(&result);
   ASSERT_FALSE(found,
                "Unexpectedly found now-deleted key '%s', found=%d. ",
                data->key,
                found);

   kvstore_lookup_result_deinit(&result);
}

/*
 * Insert a bunch of key / value pairs. Exercise and validate the iterator APIs.
 */
CTEST2(kvstore, test_iterator)
{
   // Load a bunch of rows to the store
   const int num_inserts = 50;

   int rc = do_inserts(
      num_inserts, data->kvs, &data->kvs_cfg, data->key, data->msg_buffer);
   ASSERT_EQUAL(0, rc);

   // Start exercising iterator interfaces.
   kvstore_iterator *it = NULL;
   rc = kvstore_iterator_init(data->kvs, &it, NULL /* start key */);
   ASSERT_EQUAL(0, rc, "Initializing iterator failed with rc=%d. ", rc);

   const char *current_key;
   const char *current_msg;
   size_t      current_msg_len;
   int         i = 0;
   const int   max_val_size =
      data->kvs_cfg.data_cfg.message_size - sizeof(data_handle);

   for (; kvstore_iterator_valid(it); kvstore_iterator_next(it), i++) {
      char expected_key[24] = {0};
      char expected_val[24] = {0};

      int expected_key_len =
         snprintf(expected_key, data->kvs_cfg.data_cfg.key_size, "key-%04d", i);
      ASSERT_TRUE(
         ((expected_key_len > 0)
          && (expected_key_len < data->kvs_cfg.data_cfg.key_size)),
         "expected_key_len = %d is not within expected range of [0 .. %d]. ",
         expected_key_len,
         data->kvs_cfg.data_cfg.key_size);

      int expected_val_len =
         snprintf(expected_val, max_val_size, "val-%04d", i);
      ASSERT_TRUE(
         ((expected_val_len > 0) && (expected_val_len < max_val_size)),
         "expected_key_len = %d is not within expected range of [0 .. %d]. ",
         expected_key_len,
         data->kvs_cfg.data_cfg.key_size);

      kvstore_iterator_get_current(
         it, &current_key, &current_msg_len, &current_msg);
      const char *current_val =
         (const char *)(((const data_handle *)current_msg)->data);

      int memcmp_rv =
         memcmp(current_key, expected_key, data->kvs_cfg.data_cfg.key_size);
      ASSERT_EQUAL(0,
                   memcmp_rv,
                   "iteration %d, memcmp() failed, rv=%d: expected_key = '%.*s'"
                   ", current_key = '%.*s'.",
                   i,
                   memcmp_rv,
                   data->kvs_cfg.data_cfg.key_size,
                   expected_key,
                   data->kvs_cfg.data_cfg.key_size,
                   current_key);

      ASSERT_EQUAL(data->kvs_cfg.data_cfg.message_size,
                   current_msg_len,
                   "iteration %d, mismatched value length",
                   i);

      memcmp_rv = memcmp(current_val, expected_val, max_val_size);
      ASSERT_EQUAL(0,
                   memcmp_rv,
                   "iteration %d, memcmp() failed, rv=%d: expected_val = '%.*s'"
                   ", current_val = '%.*s'.",
                   i,
                   memcmp_rv,
                   max_val_size,
                   expected_val,
                   max_val_size,
                   current_val);
   }

   rc = kvstore_iterator_status(it);
   ASSERT_EQUAL(0, rc, "Iterator stopped with error status: %d. ", rc);

   ASSERT_EQUAL(
      num_inserts, i, "Iterator stopped at i=%d, expected %d ", i, num_inserts);

   bool is_valid = kvstore_iterator_valid(it);
   ASSERT_FALSE(is_valid, "Iterator is still valid, while it should not be. ");

   // Must deinit iterator before kvstore_close() is called as part of teardown
   kvstore_iterator_deinit(it);
}

/*
 * Verify that keys are still accessible after closing and reopening KVStore.
 */
CTEST2(kvstore, test_close_open_key_access)
{
   // Exercise INSERT of new key / value pair.
   memcpy(data->key, "foo", 3);
   data_handle *msg  = (data_handle *)data->msg_buffer;
   msg->message_type = MESSAGE_TYPE_INSERT;
   msg->ref_count    = 1;
   memcpy((void *)(msg->data), "bar", 3);

   int rc = kvstore_insert(data->kvs,
                           data->key,
                           data->kvs_cfg.data_cfg.message_size,
                           data->msg_buffer);

   ASSERT_EQUAL(
      0, rc, "kvstore_insert() of new key '%s' failed, rc=%d\n", data->key, rc);

   kvstore_lookup_result result;
   kvstore_lookup_result_init(data->kvs,
                              &result,
                              data->kvs_cfg.data_cfg.message_size,
                              data->msg_buffer);

   rc = kvstore_lookup(data->kvs, data->key, &result);

   ASSERT_EQUAL(
      0, rc, "kvstore_lookup() for key '%s' failed. rc=%d.", data->key, rc);

   bool found = kvstore_lookup_result_found(&result);
   ASSERT_TRUE(found,
               "kvstore_lookup() for key '%s' failed. found=%d.",
               data->key,
               found);

   kvstore_lookup_result_deinit(&result);

   kvstore_close(data->kvs);

   rc = kvstore_open(&data->kvs_cfg, &data->kvs);
   ASSERT_EQUAL(0, rc, "kvstore_open() failed, rc=%d ", rc);

   kvstore_lookup_result_init(data->kvs,
                              &result,
                              data->kvs_cfg.data_cfg.message_size,
                              data->msg_buffer);

   rc = kvstore_lookup(data->kvs, data->key, &result);

   ASSERT_EQUAL(
      0, rc, "kvstore_lookup() failed after close/re-open; rc=%d ", rc);

   found = kvstore_lookup_result_found(&result);
   ASSERT_TRUE(found,
               "Did not find expected key '%s' after re-opening store. ",
               data->key);

   kvstore_lookup_result_deinit(&result);
}

/*
 * Minions and helper functions defined here.
 */
static int
setup_kvstore(kvstore **kvs, kvstore_config *kvs_cfg)
{
   Platform_stdout_fh = fopen("/tmp/unit_test.stdout", "a+");
   Platform_stderr_fh = fopen("/tmp/unit_test.stderr", "a+");

   int rc;
   kvs_cfg->filename   = TEST_DB_NAME;
   kvs_cfg->cache_size = Giga;      // see config.c: cache_capacity
   kvs_cfg->disk_size  = 30 * Giga; // see config.c: allocator_capacity

   kvs_cfg->data_cfg = test_data_config;

   rc = kvstore_create(kvs_cfg, kvs);
   ASSERT_EQUAL(0, rc, "kvstore_create() failed, rc=%d. ", rc);
   return rc;
}

/* Helper routine to insert n-number of keys to KVStore */
static int
do_inserts(const int       num_inserts,
           kvstore        *kvs,
           kvstore_config *kvs_cfg,
           char           *key,
           char           *msg_buffer)
{
   data_handle *msg  = (data_handle *)msg_buffer;
   msg->message_type = MESSAGE_TYPE_INSERT;
   msg->ref_count    = 1;
   int rc            = 0;

   const int max_val_size =
      kvs_cfg->data_cfg.message_size - sizeof(data_handle);

   // insert keys backwards, just for kicks
   for (int i = num_inserts - 1; i >= 0; i--) {

      fprintf(stderr, ".");
      int key_len = snprintf(key, kvs_cfg->data_cfg.key_size, "key-%04d", i);

      ASSERT_TRUE(
         ((key_len > 0) && (key_len < kvs_cfg->data_cfg.key_size)),
         "Insert failed for key i=%d, key_len = %d should be within (0, %d). ",
         i,
         key_len,
         kvs_cfg->data_cfg.key_size);

      int val_len = snprintf((char *)(msg->data), max_val_size, "val-%04d", i);

      ASSERT_TRUE(
         ((val_len > 0) && (val_len < max_val_size)),
         "Insert failed for key i=%d"
         ", Unexpected generated val_len = %d, should be within (0, %d). ",
         i,
         val_len,
         max_val_size);

      rc = kvstore_insert(kvs, key, kvs_cfg->data_cfg.message_size, msg_buffer);
      ASSERT_EQUAL(0, rc, "Insert failed for i=%d, rc=%d. ", i, rc);
   }
   return rc;
}
