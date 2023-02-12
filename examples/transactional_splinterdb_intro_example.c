// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0
/*
 * SplinterDB "Hello World" example program. Demonstrate use of:
 *  - Basic SplinterDB configuration and create() interface
 *  - Insert, lookup and iterator interfaces
 *  - Close and reopen a SplinterDB db (instance)
 */
#include <stdio.h>
#include <string.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "splinterdb/transaction.h"
#include "transaction_internal.h"

#define DB_FILE_NAME    "transactional_splinterdb_intro_db"
#define DB_FILE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define CACHE_SIZE_MB   64   // Size of cache; can be changed across boots

/* Application declares the limit of key-sizes it intends to use */
#define USER_MAX_KEY_SIZE ((int)24)

#define NUM_KEYS  16
#define VALUE_LEN 1024

void
print_txn(const char *prefix, transaction *txn)
{
   printf("transaction [%s]:\n", prefix);
   // for (uint64 i = 0; i < txn->num_rw_entries; i++) {
   //    rw_entry *entry = txn->rw_entries[i];
   //    printf("  %s\n", (const char *)slice_data(entry->key));
   // }
   // print commit_wts and commit_rts
   printf("  commit_wts: %" PRIu64 "\n", txn->commit_wts);
   printf("  commit_rts: %" PRIu64 "\n", txn->commit_rts);
}

/*
 * -------------------------------------------------------------------------------
 * We, intentionally, do not check for errors or show error handling, as this is
 * mostly a sample program to illustrate how-to use the APIs.
 * -------------------------------------------------------------------------------
 */
int
main()
{
   printf("     **** Transactional SplinterDB Basic example program ****\n\n");

   // Initialize data configuration, using default key-comparison handling.
   data_config splinter_data_cfg;
   default_data_config_init(USER_MAX_KEY_SIZE, &splinter_data_cfg);

   // Basic configuration of a SplinterDB instance
   splinterdb_config splinterdb_cfg;
   memset(&splinterdb_cfg, 0, sizeof(splinterdb_cfg));
   splinterdb_cfg.filename   = DB_FILE_NAME;
   splinterdb_cfg.disk_size  = (DB_FILE_SIZE_MB * 1024 * 1024);
   splinterdb_cfg.cache_size = (CACHE_SIZE_MB * 1024 * 1024);
   splinterdb_cfg.data_cfg   = &splinter_data_cfg;

   transactional_splinterdb *spl_handle =
      NULL; // To a running SplinterDB instance

   int rc = transactional_splinterdb_create(&splinterdb_cfg, &spl_handle);
   printf("Created Transactional SplinterDB instance, dbname '%s'.\n\n",
          DB_FILE_NAME);

   transaction t1;
   transaction t2;

   slice keys[NUM_KEYS];
   for (int i = 0; i < NUM_KEYS; i++) {
      char key[USER_MAX_KEY_SIZE] = {'x'};
      sprintf(key, "key_%d", i);
      keys[i] = slice_create(USER_MAX_KEY_SIZE, key);
   }
   const char  buf[VALUE_LEN] = {1};
   const slice value          = slice_create(VALUE_LEN, buf);

   splinterdb_lookup_result result;
   transactional_splinterdb_lookup_result_init(spl_handle, &result, 0, NULL);

   transactional_splinterdb_begin(spl_handle, &t1);
   for (int i = 0; i < NUM_KEYS; i++) {
      transactional_splinterdb_insert(spl_handle, &t1, keys[i], value);
   }
   assert(transactional_splinterdb_commit(spl_handle, &t1) == 0);
   print_txn("t1", &t1);

   transactional_splinterdb_begin(spl_handle, &t1);
   transactional_splinterdb_lookup(spl_handle, &t1, keys[0], &result);
   {
      transactional_splinterdb_begin(spl_handle, &t2);
      transactional_splinterdb_insert(spl_handle, &t2, keys[0], value);
      assert(transactional_splinterdb_commit(spl_handle, &t2) == 0);
      print_txn("t2", &t2);
   }
   assert(transactional_splinterdb_commit(spl_handle, &t1) == 0);
   print_txn("t1", &t1);

   transactional_splinterdb_begin(spl_handle, &t1);
   transactional_splinterdb_lookup(spl_handle, &t1, keys[0], &result);
   transactional_splinterdb_insert(spl_handle, &t1, keys[0], value);
   {
      transactional_splinterdb_begin(spl_handle, &t2);
      transactional_splinterdb_insert(spl_handle, &t2, keys[0], value);
      assert(transactional_splinterdb_commit(spl_handle, &t2) == 0);
      print_txn("t2", &t2);
   }
   assert(transactional_splinterdb_commit(spl_handle, &t1) == -1);
   print_txn("t1 (abort)", &t1);

   transactional_splinterdb_begin(spl_handle, &t1);
   {
      transactional_splinterdb_begin(spl_handle, &t2);
      transactional_splinterdb_insert(spl_handle, &t2, keys[0], value);
      assert(transactional_splinterdb_commit(spl_handle, &t2) == 0);
      print_txn("t2", &t2);
   }
   transactional_splinterdb_lookup(spl_handle, &t1, keys[0], &result);
   transactional_splinterdb_insert(spl_handle, &t1, keys[0], value);
   assert(transactional_splinterdb_commit(spl_handle, &t1) == 0);
   print_txn("t1", &t1);

   for (int i = 0; i < 20000000; i++) {
      transactional_splinterdb_begin(spl_handle, &t1);
      transactional_splinterdb_insert(spl_handle, &t1, keys[0], value);
      assert(transactional_splinterdb_commit(spl_handle, &t1) == 0);
   }
   print_txn("t1", &t1);

   // const char *fruit = "apple";
   // const char *descr = "An apple a day keeps the doctor away!";
   // slice       key   = slice_create((size_t)strlen(fruit), fruit);
   // slice       value = slice_create((size_t)strlen(descr), descr);

   // transaction txn;
   // transactional_splinterdb_begin(spl_handle, &txn);

   // rc = transactional_splinterdb_insert(spl_handle, &txn, key, value);
   // if (!rc) {
   //    printf("Inserted key '%s'\n", fruit);
   // }

   // rc = transactional_splinterdb_commit(spl_handle, &txn);
   // if (rc == -1) {
   //    printf("%d: Transaction aborts\n", __LINE__);
   // }

   // transactional_splinterdb_begin(spl_handle, &txn);

   // fruit = "Orange";
   // descr = "Is a good source of vitamin-C.";
   // key   = slice_create((size_t)strlen(fruit), fruit);
   // value = slice_create((size_t)strlen(descr), descr);
   // rc    = transactional_splinterdb_insert(spl_handle, &txn, key, value);
   // if (!rc) {
   //    printf("Inserted key '%s'\n", fruit);
   // }

   // fruit = "Mango";
   // descr = "Mango is the king of fruits.";
   // key   = slice_create((size_t)strlen(fruit), fruit);
   // value = slice_create((size_t)strlen(descr), descr);
   // rc    = transactional_splinterdb_insert(spl_handle, &txn, key, value);
   // if (!rc) {
   //    printf("Inserted key '%s'\n", fruit);
   // }

   // rc = transactional_splinterdb_commit(spl_handle, &txn);
   // if (rc == -1) {
   //    printf("%d: Transaction aborts\n", __LINE__);
   // }

   // transactional_splinterdb_begin(spl_handle, &txn);

   // // Retrieve a key-value pair.
   // splinterdb_lookup_result result;
   // transactional_splinterdb_lookup_result_init(spl_handle, &result, 0, NULL);

   // fruit = "Orange";
   // key   = slice_create((size_t)strlen(fruit), fruit);
   // rc    = transactional_splinterdb_lookup(spl_handle, &txn, key, &result);
   // rc    = splinterdb_lookup_result_value(&result, &value);
   // if (!rc) {
   //    printf("Found key: '%s', value: '%.*s'\n",
   //           fruit,
   //           (int)slice_length(value),
   //           (char *)slice_data(value));
   // } else {
   //    printf("Key: '%s' not found. (rc=%d)\n", fruit, rc);
   // }

   // // Handling non-existent keys
   // fruit = "Banana";
   // key   = slice_create((size_t)strlen(fruit), fruit);
   // rc    = transactional_splinterdb_lookup(spl_handle, &txn, key, &result);
   // rc    = splinterdb_lookup_result_value(&result, &value);
   // if (rc) {
   //    printf("Key: '%s' not found. (rc=%d)\n", fruit, rc);
   // }
   // printf("\n");

   // rc = transactional_splinterdb_commit(spl_handle, &txn);
   // if (rc == -1) {
   //    printf("%d: Transaction aborts\n", __LINE__);
   // }

   transactional_splinterdb_close(&spl_handle);
   printf("Shutdown SplinterDB instance, dbname '%s'.\n\n", DB_FILE_NAME);

   return rc;
}
