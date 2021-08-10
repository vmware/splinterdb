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

void
kvstore_set_config(kvstore_config *kvsCfg)
{
   *kvsCfg            = (kvstore_config){0}; // zero it out
   kvsCfg->filename  = "db";
   kvsCfg->cache_size = Giga; // see config.c: cache_capacity

   kvsCfg->key_size  = test_data_config->key_size;
   kvsCfg->data_size = test_data_config->message_size;

   kvsCfg->key_compare = test_data_config->key_compare;
   kvsCfg->key_hash    = test_data_config->key_hash;

   kvsCfg->merge_tuples       = test_data_config->merge_tuples;
   kvsCfg->merge_tuples_final = test_data_config->merge_tuples_final;

   kvsCfg->key_to_str     = test_data_config->key_to_string;
   kvsCfg->message_to_str = test_data_config->message_to_string;
   kvsCfg->message_class  = test_data_config->message_class;

   kvsCfg->disk_size = 30 * Giga; // see config.c: allocator_capacity
}

int
kvstore_test(int argc, char *argv[])
{
   fprintf(stderr, "kvstore_test: starting\n");

   int           kvResult;
   kvstore_handle kvsHandle = {0};
   kvstore_config kvsCfg    = {0};


   kvstore_set_config(&kvsCfg);

   kvResult = kvstore_init(&kvsCfg, &kvsHandle);
   if (kvResult != 0) {
      fprintf(stderr, "kvstore_init error: %d\n", kvResult);
      return -1;
   }

   kvstore_register_thread(kvsHandle);

   fprintf(stderr, "kvstore_test: initializing test data\n");
   char *key   = calloc(1, kvsCfg.key_size);
   char *value = calloc(1, kvsCfg.data_size);
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
            kvsCfg.data_size - offsetof(data_handle, data),
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
