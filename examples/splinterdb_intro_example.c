// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * SplinterDB "Hello World" Example Program.
 */
#include <stdio.h>
#include <string.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "example_common.h"

/* Tag to identify messages from application program */
#define APP_ME "App-Basic"

/*
 * App-specific 'defaults' that can be parameterized, eventually.
 */
#define APP_DB_NAME "splinterdb_intro_db"

#define APP_DEVICE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define APP_CACHE_SIZE_MB  64   // Size of cache; can be changed across boots

/* Application declares the limit of key-sizes it intends to use */
#define APP_MAX_KEY_SIZE ((int)100)

/*
 * -------------------------------------------------------------------------------
 * main() Driver for basic SplinterDB example program.
 * We, intentionally, do not check or show-for error handling, as this is mostly
 * a sample program to illustrative how-to use the APIs.
 * -------------------------------------------------------------------------------
 */
int
main()
{
   printf("     **** SplinterDB Basic Example program ****\n\n");

   /*
    * Initialize data configuration, using default key-comparison handling.
    */
   data_config splinter_data_cfg;
   default_data_config_init(APP_MAX_KEY_SIZE, &splinter_data_cfg);

   /* Basic configuration of a SplinterDB instance */
   splinterdb_config splinterdb_cfg;
   memset(&splinterdb_cfg, 0, sizeof(splinterdb_cfg));
   splinterdb_cfg.filename   = APP_DB_NAME;
   splinterdb_cfg.disk_size  = (APP_DEVICE_SIZE_MB * K_MiB);
   splinterdb_cfg.cache_size = (APP_CACHE_SIZE_MB * K_MiB);
   splinterdb_cfg.data_cfg   = &splinter_data_cfg;

   splinterdb *spl_handle = NULL; // To a running SplinterDB instance

   int rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
   ex_msg("Created SplinterDB instance, dbname '%s'.\n\n", APP_DB_NAME);

   /* Insert a few kv-pairs, describing properties of fruits. */
   const char *fruit = "apple";
   const char *descr = "An apple a day keeps the doctor away!";
   slice       key   = slice_create((size_t)strlen(fruit), fruit);
   slice       value = slice_create((size_t)strlen(descr), descr);
   rc                = splinterdb_insert(spl_handle, key, value);
   ex_msg("Inserted key '%s'\n", fruit);

   fruit = "Orange";
   descr = "Is a good source of vitamin-C.";
   key   = slice_create((size_t)strlen(fruit), fruit);
   value = slice_create((size_t)strlen(descr), descr);
   rc    = splinterdb_insert(spl_handle, key, value);
   ex_msg("Inserted key '%s'\n", fruit);

   fruit = "Mango";
   descr = "Mango is the king of fruits.";
   key   = slice_create((size_t)strlen(fruit), fruit);
   value = slice_create((size_t)strlen(descr), descr);
   rc    = splinterdb_insert(spl_handle, key, value);
   ex_msg("Inserted key '%s'\n", fruit);

   /* Retrieve a key-value pair. */
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(spl_handle, &result, 0, NULL);

   fruit = "Orange";
   key   = slice_create((size_t)strlen(fruit), fruit);
   rc    = splinterdb_lookup(spl_handle, key, &result);
   rc    = splinterdb_lookup_result_value(spl_handle, &result, &value);
   if (!rc) {
      ex_msg("Found key: '%s', value: '%.*s'\n",
             fruit,
             (int)slice_length(value),
             (char *)slice_data(value));
   }

   /* Handling non-existent keys */
   fruit = "Banana";
   key   = slice_create((size_t)strlen(fruit), fruit);
   rc    = splinterdb_lookup(spl_handle, key, &result);
   rc    = splinterdb_lookup_result_value(spl_handle, &result, &value);
   if (rc) {
      ex_err("Key: '%s' not found.\n", fruit);
   }
   printf("\n");

   /* Retrieve all the key-value pairs from the database */
   ex_msg("Iterate through all the %d key-value pairs"
          " returning keys in lexicographic sort order:\n",
          2);

   splinterdb_iterator *it = NULL;
   rc = splinterdb_iterator_init(spl_handle, &it, NULL_SLICE);

   int i = 0;
   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      slice key, value;
      splinterdb_iterator_get_current(it, &key, &value);
      ex_msg("[%d] key='%.*s', value='%.*s'\n",
             i,
             (int)slice_length(key),
             (char *)slice_data(key),
             (int)slice_length(value),
             (char *)slice_data(value));
      i++;
   }
   rc = splinterdb_iterator_status(it);
   splinterdb_iterator_deinit(it);

   ex_msg("Found %d key-value pairs\n\n", i);

   splinterdb_close(&spl_handle);
   ex_msg("Shutdown SplinterDB instance, dbname '%s'.\n\n", APP_DB_NAME);

   return rc;
}
