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

   int            kv_result;
   kvstore_handle kvs_handle = {0};
   kvstore_config kvs_cfg    = {0};

   kvs_cfg.filename   = "db";
   kvs_cfg.cache_size = Giga;      // see config.c: cache_capacity
   kvs_cfg.disk_size  = 30 * Giga; // see config.c: allocator_capacity

   kvs_cfg.data_cfg = *test_data_config;

   kv_result = kvstore_init(&kvs_cfg, &kvs_handle);
   if (kv_result != 0) {
      fprintf(stderr, "kvstore_init error: %d\n", kv_result);
      return -1;
   }

   kvstore_register_thread(kvs_handle);

   fprintf(stderr, "kvstore_test: initializing test data\n");
   char *key   = calloc(1, kvs_cfg.data_cfg.key_size);
   char *value = calloc(1, kvs_cfg.data_cfg.message_size);
   if (value == NULL) {
      fprintf(stderr, "calloc value buffer\n");
   }
   bool found;
   memcpy(key, "foo", 3);

   fprintf(stderr, "kvstore_test: lookup non-existent key...");
   kv_result = kvstore_lookup(kvs_handle, key, value, &found);
   if (kv_result != 0) {
      fprintf(stderr, "kvstore_lookup: %d\n", kv_result);
      goto cleanup;
   }
   fprintf(stderr, "found=%d\n", found);
   if (found) {
      kv_result = -1;
      fprintf(stderr, "unexpectedly found a key we haven't set\n");
      goto cleanup;
   }

   data_handle *valueStruct  = (data_handle *)value;
   valueStruct->message_type = MESSAGE_TYPE_INSERT;
   valueStruct->ref_count    = 1;
   memcpy((void *)(valueStruct->data), "bar", 3);

   fprintf(stderr,
           "inserting key with data = %.*s\n",
           3,
           (char *)(valueStruct->data));
   kv_result = kvstore_insert(kvs_handle, key, value);
   if (kv_result != 0) {
      fprintf(stderr, "kvstore_insert: %d\n", kv_result);
      goto cleanup;
   }

   // muck with the value, just to see that it gets reset
   valueStruct->message_type = MESSAGE_TYPE_UPDATE;
   snprintf((void *)(valueStruct->data),
            kvs_cfg.data_cfg.message_size - offsetof(data_handle, data),
            "zzz");
   fprintf(
      stderr,
      "after insert, we set local variable data = %.*s just to mess it up\n",
      3,
      (char *)(valueStruct->data));

   fprintf(stderr, "kvstore_test: lookup #2...");
   kv_result = kvstore_lookup(kvs_handle, key, value, &found);
   if (kv_result != 0) {
      fprintf(stderr, "kvstore_lookup: %d\n", kv_result);
      goto cleanup;
   }
   fprintf(stderr, "found=%d\n", found);
   if (!found) {
      kv_result = -1;
      fprintf(stderr, "unexpectedly 'found' is false\n");
      goto cleanup;
   }
   if (memcmp(valueStruct->data, "bar", 3) != 0) {
      kv_result = -1;
      fprintf(stderr,
              "lookup returned an unexpected value for data = %.*s\n",
              3,
              (char *)(valueStruct->data));
   }

   fprintf(stderr, "kvstore_test: delete key\n");
   valueStruct->message_type = MESSAGE_TYPE_DELETE;
   kv_result                 = kvstore_insert(kvs_handle, key, value);
   if (kv_result != 0) {
      fprintf(stderr, "kvstore_insert (for delete): %d\n", kv_result);
      goto cleanup;
   }

   fprintf(stderr, "kvstore_test: lookup #3, for now-deleted key...");
   kv_result = kvstore_lookup(kvs_handle, key, value, &found);
   if (kv_result != 0) {
      fprintf(stderr, "kvstore_lookup: %d\n", kv_result);
      goto cleanup;
   }
   fprintf(stderr, "found=%d\n", found);
   if (found) {
      kv_result = -1;
      fprintf(stderr, "unexpectedly 'found' is true\n");
      goto cleanup;
   }


cleanup:
   kvstore_deinit(kvs_handle);
   if (kv_result == 0) {
      fprintf(stderr, "kvstore_test: succeeded\n");
      return 0;
   } else {
      fprintf(stderr, "kvstore_test: FAILED\n");
      return -1;
   }
}
