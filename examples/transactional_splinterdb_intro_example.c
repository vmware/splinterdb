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

#define DB_FILE_NAME    "transactional_splinterdb_intro_db"
#define DB_FILE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define CACHE_SIZE_MB   64   // Size of cache; can be changed across boots

/* Application declares the limit of key-sizes it intends to use */
#define USER_MAX_KEY_SIZE ((int)100)

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

   // Insert a few kv-pairs, describing properties of fruits.
   const char *fruit = "apple";
   const char *descr = "An apple a day keeps the doctor away!";
   slice       key   = slice_create((size_t)strlen(fruit), fruit);
   slice       value = slice_create((size_t)strlen(descr), descr);

   transaction txn;
   transactional_splinterdb_begin(spl_handle, &txn);

   rc = transactional_splinterdb_insert(spl_handle, &txn, key, value);
   printf("Inserted key '%s'\n", fruit);

   transactional_splinterdb_commit(spl_handle, &txn);

   transactional_splinterdb_begin(spl_handle, &txn);

   fruit = "Orange";
   descr = "Is a good source of vitamin-C.";
   key   = slice_create((size_t)strlen(fruit), fruit);
   value = slice_create((size_t)strlen(descr), descr);
   rc    = transactional_splinterdb_insert(spl_handle, &txn, key, value);
   printf("Inserted key '%s'\n", fruit);

   fruit = "Mango";
   descr = "Mango is the king of fruits.";
   key   = slice_create((size_t)strlen(fruit), fruit);
   value = slice_create((size_t)strlen(descr), descr);
   rc    = transactional_splinterdb_insert(spl_handle, &txn, key, value);
   printf("Inserted key '%s'\n", fruit);

   transactional_splinterdb_commit(spl_handle, &txn);

   transactional_splinterdb_begin(spl_handle, &txn);

   // Retrieve a key-value pair.
   splinterdb_lookup_result result;
   transactional_splinterdb_lookup_result_init(spl_handle, &result, 0, NULL);

   fruit = "Orange";
   key   = slice_create((size_t)strlen(fruit), fruit);
   rc    = transactional_splinterdb_lookup(spl_handle, &txn, key, &result);
   rc =
      transactional_splinterdb_lookup_result_value(spl_handle, &result, &value);
   if (!rc) {
      printf("Found key: '%s', value: '%.*s'\n",
             fruit,
             (int)slice_length(value),
             (char *)slice_data(value));
   }

   // Handling non-existent keys
   fruit = "Banana";
   key   = slice_create((size_t)strlen(fruit), fruit);
   rc    = transactional_splinterdb_lookup(spl_handle, &txn, key, &result);
   rc =
      transactional_splinterdb_lookup_result_value(spl_handle, &result, &value);
   if (rc) {
      printf("Key: '%s' not found. (rc=%d)\n", fruit, rc);
   }
   printf("\n");

   transactional_splinterdb_commit(spl_handle, &txn);

   transactional_splinterdb_close(&spl_handle);
   printf("Shutdown SplinterDB instance, dbname '%s'.\n\n", DB_FILE_NAME);

   return rc;
}
