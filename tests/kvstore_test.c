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

   int            kvResult;
   kvstore_handle kvsHandle = {0};
   kvstore_config kvsCfg    = {0};

   kvsCfg.filename   = "db";
   kvsCfg.cache_size = Giga;      // see config.c: cache_capacity
   kvsCfg.disk_size  = 30 * Giga; // see config.c: allocator_capacity

   kvsCfg.data_cfg = *test_data_config;

   kvResult = kvstore_init(&kvsCfg, &kvsHandle);
   if (kvResult != 0) {
      fprintf(stderr, "kvstore_init error: %d\n", kvResult);
      return -1;
   }

   kvstore_register_thread(kvsHandle);

   fprintf(stderr, "kvstore_test: initializing test data\n");
   char *key   = calloc(1, kvsCfg.data_cfg.key_size);
   char *value = calloc(1, kvsCfg.data_cfg.message_size);
   if (value == NULL) {
      fprintf(stderr, "calloc value buffer\n");
   }
   bool found;
   memcpy(key, "foo", 3);

   fprintf(stderr, "kvstore_test: lookup non-existent key...");
   kvResult = kvstore_lookup(kvsHandle, key, value, &found);
   if (kvResult != 0) {
      fprintf(stderr, "kvstore_lookup: %d\n", kvResult);
      goto cleanup;
   }
   fprintf(stderr, "found=%d\n", found);
   if (found) {
      kvResult = -1;
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
   kvResult = kvstore_insert(kvsHandle, key, value);
   if (kvResult != 0) {
      fprintf(stderr, "kvstore_insert: %d\n", kvResult);
      goto cleanup;
   }

   // muck with the value, just to see that it gets reset
   valueStruct->message_type = MESSAGE_TYPE_UPDATE;
   snprintf((void *)(valueStruct->data),
            kvsCfg.data_cfg.message_size - offsetof(data_handle, data),
            "zzz");
   fprintf(
      stderr,
      "after insert, we set local variable data = %.*s just to mess it up\n",
      3,
      (char *)(valueStruct->data));

   fprintf(stderr, "kvstore_test: lookup #2...");
   kvResult = kvstore_lookup(kvsHandle, key, value, &found);
   if (kvResult != 0) {
      fprintf(stderr, "kvstore_lookup: %d\n", kvResult);
      goto cleanup;
   }
   fprintf(stderr, "found=%d\n", found);
   if (!found) {
      kvResult = -1;
      fprintf(stderr, "unexpectedly 'found' is false\n");
      goto cleanup;
   }
   if (memcmp(valueStruct->data, "bar", 3) != 0) {
      kvResult = -1;
      fprintf(stderr,
              "lookup returned an unexpected value for data = %.*s\n",
              3,
              (char *)(valueStruct->data));
   }

   fprintf(stderr, "kvstore_test: delete key\n");
   valueStruct->message_type = MESSAGE_TYPE_DELETE;
   kvResult                  = kvstore_insert(kvsHandle, key, value);
   if (kvResult != 0) {
      fprintf(stderr, "kvstore_insert (for delete): %d\n", kvResult);
      goto cleanup;
   }

   fprintf(stderr, "kvstore_test: lookup #3, for now-deleted key...");
   kvResult = kvstore_lookup(kvsHandle, key, value, &found);
   if (kvResult != 0) {
      fprintf(stderr, "kvstore_lookup: %d\n", kvResult);
      goto cleanup;
   }
   fprintf(stderr, "found=%d\n", found);
   if (found) {
      kvResult = -1;
      fprintf(stderr, "unexpectedly 'found' is true\n");
      goto cleanup;
   }

cleanup:
   kvstore_deinit(kvsHandle);
   if (kvResult == 0) {
      fprintf(stderr, "kvstore_test: succeeded\n");
      return 0;
   } else {
      fprintf(stderr, "kvstore_test: FAILED\n");
      return -1;
   }
}
