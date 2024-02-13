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
#include <stdlib.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "splinterdb/transaction.h"

#define DB_FILE_NAME    "transactional_splinterdb_intro_db"
#define DB_FILE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define CACHE_SIZE_MB   64   // Size of cache; can be changed across boots

/* Application declares the limit of key-sizes it intends to use */
#define USER_MAX_KEY_SIZE ((int)24)

#define NUM_KEYS  16
#define VALUE_LEN 1024

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
   transaction t3;

   printf("t1: %lu, t2: %lu, t3: %lu\n\n",
          ((unsigned long)&t1) % 100,
          ((unsigned long)&t2) % 100,
          ((unsigned long)&t3) % 100);

   slice keys[NUM_KEYS];
   for (int i = 0; i < NUM_KEYS; i++) {
      char *key = (char *)malloc(USER_MAX_KEY_SIZE);
      memset(key, 'x', USER_MAX_KEY_SIZE);
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

   // make keys[0] rts:1 and wts:3
   transactional_splinterdb_begin(spl_handle, &t1);
   transactional_splinterdb_lookup(spl_handle, &t1, keys[0], &result);
   transactional_splinterdb_update(spl_handle, &t1, keys[2], value);
   transactional_splinterdb_begin(spl_handle, &t2);
   if (transactional_splinterdb_update(spl_handle, &t2, keys[2], value) == 0) {
      assert(transactional_splinterdb_commit(spl_handle, &t2) == 0);
   } else {
      printf("t2 failed due to write-write conflict on %s\n",
             (char *)slice_data(keys[2]));
   }
   assert(transactional_splinterdb_commit(spl_handle, &t1) == 0);

   // make keys[1] rts:1 and wts:2
   transactional_splinterdb_begin(spl_handle, &t1);
   transactional_splinterdb_lookup(spl_handle, &t1, keys[1], &result);
   transactional_splinterdb_update(spl_handle, &t1, keys[3], value);
   assert(transactional_splinterdb_commit(spl_handle, &t1) == 0);

   // the example in the tictoc paper

   transactional_splinterdb_begin(spl_handle, &t1);
   transactional_splinterdb_begin(spl_handle, &t2);
   transactional_splinterdb_lookup(spl_handle, &t1, keys[0], &result);
   if (transactional_splinterdb_update(spl_handle, &t2, keys[0], value) == 0) {
      assert(transactional_splinterdb_commit(spl_handle, &t2) == 0);
   } else {
      printf("t2 failed\n");
   }
   if (transactional_splinterdb_update(spl_handle, &t1, keys[1], value) == 0) {
      assert(transactional_splinterdb_commit(spl_handle, &t1) == 0);
   } else {
      printf("t1 failed\n");
   }
   for (int i = 0; i < NUM_KEYS; i++) {
      free((char *)slice_data(keys[i]));
   }
   transactional_splinterdb_close(&spl_handle);
   printf("Shutdown SplinterDB instance, dbname '%s'.\n\n", DB_FILE_NAME);

   return rc;
}
