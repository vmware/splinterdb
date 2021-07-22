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

void kvstore_set_config(KVStoreConfig *kvsCfg) {
   *kvsCfg = (KVStoreConfig){0}; // zero it out
   kvsCfg->filename = "db";
   kvsCfg->cacheSize = Giga; // see config.c: cache_capacity

   kvsCfg->keySize = test_data_config->key_size;
   kvsCfg->dataSize = test_data_config->message_size;

   kvsCfg->keyCompare = test_data_config->key_compare;
   kvsCfg->keyHash = test_data_config->key_hash;

   kvsCfg->mergeTuples = test_data_config->merge_tuples;
   kvsCfg->mergeTuplesFinal = test_data_config->merge_tuples_final;

   kvsCfg->keyToStr = test_data_config->key_to_string;
   kvsCfg->messageToStr = test_data_config->message_to_string;
   kvsCfg->messageClass = test_data_config->message_class;

   kvsCfg->diskSize = 30 * Giga; // see config.c: allocator_capacity
}

int kvstore_test(int argc, char *argv[])
{
   fprintf(stderr, "kvstore_test: starting\n");

   int kvResult;
   KVStoreHandle kvsHandle = {0};
   KVStoreConfig kvsCfg = {0};


   kvstore_set_config(&kvsCfg);

   kvResult = KVStore_Init(&kvsCfg, &kvsHandle);
   if (kvResult != 0) {
      fprintf(stderr, "kvstore_init error: %d\n", kvResult);
      return -1;
   }

   KVStore_RegisterThread(kvsHandle);

   fprintf(stderr, "kvstore_test: initializing test data\n");
   char key[24] = {0};
   char *value = calloc(1, kvsCfg.dataSize);
   if (value == NULL) {
      fprintf(stderr, "calloc value buffer\n");
   }
   bool found;
   memcpy(key, "foo", 3);

   fprintf(stderr, "kvstore_test: lookup non-existent key...");
   kvResult = KVStore_Lookup(kvsHandle, key, value, &found);
   if (kvResult != 0) {
      fprintf(stderr, "kvstore_lookup: %d\n", kvResult);
      goto cleanup;
   }
   fprintf(stderr, "found=%d\n", found);
   if (found)
   {
      kvResult = -1;
      fprintf(stderr, "unexpectedly found a key we haven't set\n");
      goto cleanup;
   }

   data_handle *valueStruct = (data_handle *)value;
   valueStruct->message_type = MESSAGE_TYPE_INSERT;
   valueStruct->ref_count = 1;
   memcpy((void *)(valueStruct->data), "bar", 3);

   fprintf(stderr, "inserting key with data = %.*s\n", 3, (char*)(valueStruct->data));
   kvResult = KVStore_Insert(kvsHandle, key, value);
   if (kvResult != 0) {
      fprintf(stderr, "kvstore_insert: %d\n", kvResult);
      goto cleanup;
   }

   // muck with the value, just to see that it gets reset
   valueStruct->message_type = MESSAGE_TYPE_UPDATE;
   strncpy((void *)(valueStruct->data), "zzz", 3);
   fprintf(stderr, "after insert, we set local variable data = %.*s just to mess it up\n", 3, (char*)(valueStruct->data));

   fprintf(stderr, "kvstore_test: lookup #2...");
   kvResult = KVStore_Lookup(kvsHandle, key, value, &found);
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
      fprintf(stderr, "lookup returned an unexpected value for data = %.*s\n", 3, (char*)(valueStruct->data));
   }

   fprintf(stderr, "kvstore_test: delete key\n");
   valueStruct->message_type = MESSAGE_TYPE_DELETE;
   kvResult = KVStore_Insert(kvsHandle, key, value);
   if (kvResult != 0) {
      fprintf(stderr, "kvstore_insert (for delete): %d\n", kvResult);
      goto cleanup;
   }

   fprintf(stderr, "kvstore_test: lookup #3, for now-deleted key...");
   kvResult = KVStore_Lookup(kvsHandle, key, value, &found);
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
   KVStore_Deinit(kvsHandle);
   if (kvResult == 0) {
      fprintf(stderr, "kvstore_test: succeeded\n");
      return 0;
   } else {
      fprintf(stderr, "kvstore_test: FAILED\n");
      return -1;
   }
}
