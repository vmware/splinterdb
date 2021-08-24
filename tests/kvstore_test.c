// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * kvstore_test.c --
 *
 *     tests for the simplified kvstore api
 */

#include "kvstore.h"
#include "test_data.h"

#define Kilo (1024UL)
#define Mega (Kilo * Kilo)
#define Giga (Kilo * Mega)

int
kvstore_test(int argc, char *argv[])
{
   fprintf(stderr, "kvstore_test: starting\n");

   int            rc;
   kvstore_config kvs_cfg = {0};
   kvstore *      kvs;

   kvs_cfg.filename   = "db";
   kvs_cfg.cache_size = Giga;      // see config.c: cache_capacity
   kvs_cfg.disk_size  = 30 * Giga; // see config.c: allocator_capacity

   kvs_cfg.data_cfg = *test_data_config;

   rc = kvstore_init(&kvs_cfg, &kvs);
   if (rc != 0) {
      fprintf(stderr, "kvstore_init error: %d\n", rc);
      return -1;
   }

   kvstore_register_thread(kvs);

   fprintf(stderr, "kvstore_test: initializing test data\n");
   char *key        = calloc(1, kvs_cfg.data_cfg.key_size);
   char *msg_buffer = calloc(1, kvs_cfg.data_cfg.message_size);
   if (msg_buffer == NULL) {
      fprintf(stderr, "calloc message buffer\n");
      goto cleanup;
   }
   bool found;
   memcpy(key, "foo", 3);

   fprintf(stderr, "kvstore_test: lookup non-existent key...");
   rc = kvstore_lookup(kvs, key, msg_buffer, &found);
   if (rc != 0) {
      fprintf(stderr, "kvstore_lookup: %d\n", rc);
      goto cleanup;
   }
   fprintf(stderr, "found=%d\n", found);
   if (found) {
      rc = -1;
      fprintf(stderr, "unexpectedly found a key we haven't set\n");
      goto cleanup;
   }

   data_handle *msg  = (data_handle *)msg_buffer;
   msg->message_type = MESSAGE_TYPE_INSERT;
   msg->ref_count    = 1;
   memcpy((void *)(msg->data), "bar", 3);

   fprintf(stderr, "inserting key with data = %.*s\n", 3, (char *)(msg->data));
   rc = kvstore_insert(kvs, key, msg_buffer);
   if (rc != 0) {
      fprintf(stderr, "kvstore_insert: %d\n", rc);
      goto cleanup;
   }

   // muck with the value, just to see that it gets reset
   msg->message_type = MESSAGE_TYPE_UPDATE;
   snprintf((void *)(msg->data),
            kvs_cfg.data_cfg.message_size - offsetof(data_handle, data),
            "zzz");
   fprintf(
      stderr,
      "after insert, we set local variable data = %.*s just to mess it up\n",
      3,
      (char *)(msg->data));

   fprintf(stderr, "kvstore_test: lookup #2...");
   rc = kvstore_lookup(kvs, key, msg_buffer, &found);
   if (rc != 0) {
      fprintf(stderr, "kvstore_lookup: %d\n", rc);
      goto cleanup;
   }
   fprintf(stderr, "found=%d\n", found);
   if (!found) {
      rc = -1;
      fprintf(stderr, "unexpectedly 'found' is false\n");
      goto cleanup;
   }
   if (memcmp(msg->data, "bar", 3) != 0) {
      rc = -1;
      fprintf(stderr,
              "lookup returned an unexpected value for data = %.*s\n",
              3,
              (char *)(msg->data));
   }

   fprintf(stderr, "kvstore_test: delete key\n");
   msg->message_type = MESSAGE_TYPE_DELETE;
   rc                = kvstore_insert(kvs, key, msg_buffer);
   if (rc != 0) {
      fprintf(stderr, "kvstore_insert (for delete): %d\n", rc);
      goto cleanup;
   }

   fprintf(stderr, "kvstore_test: lookup #3, for now-deleted key...");
   rc = kvstore_lookup(kvs, key, msg_buffer, &found);
   if (rc != 0) {
      fprintf(stderr, "kvstore_lookup: %d\n", rc);
      goto cleanup;
   }
   fprintf(stderr, "found=%d\n", found);
   if (found) {
      rc = -1;
      fprintf(stderr, "unexpectedly 'found' is true\n");
      goto cleanup;
   }

   const int max_val_size = kvs_cfg.data_cfg.message_size - sizeof(data_handle);

   const int num_inserts = 50;
   msg->message_type     = MESSAGE_TYPE_INSERT;
   fprintf(stderr, "kvstore_test: inserting %d keys", num_inserts);
   // insert keys backwards, just for kicks
   for (int i = num_inserts - 1; i >= 0; i--) {
      fprintf(stderr, ".");
      int key_len = snprintf(key, kvs_cfg.data_cfg.key_size, "key-%04d", i);
      if (!(key_len > 0 && key_len < kvs_cfg.data_cfg.key_size)) {
         fprintf(stderr, "expected: generated key fits in buffer\n");
         rc = -1;
         goto cleanup;
      }

      int val_len = snprintf((char *)(msg->data), max_val_size, "val-%04d", i);
      if (!(val_len > 0 && val_len < max_val_size)) {
         fprintf(stderr, "expected: generated value fits in buffer\n");
         rc = -1;
         goto cleanup;
      }

      rc = kvstore_insert(kvs, key, msg_buffer);
      if (rc != 0) {
         fprintf(stderr, "insert failed\n");
         rc = -1;
         goto cleanup;
      }
   }
   fprintf(stderr, "\nkvstore_test: inserts done, now using iterator:");

   kvstore_iterator *it;
   rc = kvstore_iterator_init(kvs, &it, NULL /* start key */);
   if (rc != 0) {
      fprintf(stderr, "initializing iterator: %d\n", rc);
      goto cleanup;
   }

   const char *current_key;
   const char *current_msg;
   int i = 0;
   for (; kvstore_iterator_valid(it); kvstore_iterator_next(it)) {
      char expected_key[24] = {0};
      char expected_val[24] = {0};
      int  expected_key_len =
         snprintf(expected_key, kvs_cfg.data_cfg.key_size, "key-%04d", i);
      platform_assert(expected_key_len > 0 &&
                      expected_key_len < kvs_cfg.data_cfg.key_size);
      int expected_val_len =
         snprintf(expected_val, max_val_size, "val-%04d", i);
      platform_assert(expected_val_len > 0 && expected_val_len < max_val_size);

      kvstore_iterator_get_current(it, &current_key, &current_msg);
      const char *current_val =
         (const char *)(((const data_handle *)current_msg)->data);

      if (memcmp(current_key, expected_key, kvs_cfg.data_cfg.key_size) != 0) {
         fprintf(stderr, "iteration %d: mismatched key\n", i);
         rc = -1;
         goto iter_cleanup;
      }

      if (memcmp(current_val, expected_val, max_val_size) != 0) {
         fprintf(stderr, "iteration %d: mismatched value\n", i);
         rc = -1;
         goto iter_cleanup;
      }
      fprintf(stderr, ".");
      i++;
   }
   rc = kvstore_iterator_status(it);
   if (rc != 0) {
      fprintf(stderr, "iterator stopped with error status: %d\n", rc);
      goto iter_cleanup;
   }

   if (i != num_inserts) {
      fprintf(stderr, "iterator stopped at %d, expected %d\n", i, num_inserts);
      rc = -1;
      goto iter_cleanup;
   }

   if (kvstore_iterator_valid(it)) {
      fprintf(stderr, "iterator still valid, this should not happen\n");
      rc = -1;
      goto iter_cleanup;
   }
   fprintf(stderr, "OK.  iterator test complete\n");


iter_cleanup:
   fprintf(stderr, "\nkvstore_test: de-init iterator\n");
   kvstore_iterator_deinit(it);
cleanup:
   kvstore_deinit(kvs);
   if (rc == 0) {
      fprintf(stderr, "kvstore_test: succeeded\n");
      return 0;
   } else {
      fprintf(stderr, "kvstore_test: FAILED\n");
      return -1;
   }
}
