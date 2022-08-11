// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * SplinterDB Wide values APIs example program.
 */
#include <stdio.h>
#include <string.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"

#define DB_FILE_NAME    "splinterdb_wide_values_example_db"
#define DB_FILE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define CACHE_SIZE_MB   64   // Size of cache; can be changed across boots

/* Application declares the limit of key-sizes it intends to use */
#define USER_MAX_KEY_SIZE ((int)100)

/* Avg value size and max value sizes we expect to deal with in this program */
#define USER_AVG_VALUE_SIZE ((int)128)
#define USER_MAX_VALUE_SIZE ((int)1024)

/*
 * -------------------------------------------------------------------------------
 * main() for SplinterDB program handling inserts & lookups of wide values.
 * -------------------------------------------------------------------------------
 */
int
main()
{
   printf("     **** SplinterDB Wide Values APIs example program ****\n\n");

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

   splinterdb *spl_handle = NULL; // To a running SplinterDB instance

   int rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
   printf("Created SplinterDB instance, dbname '%s'.\n\n", DB_FILE_NAME);

   // -- ACTION IS HERE --
   char key_buf[USER_MAX_KEY_SIZE];
   char val_buf[USER_MAX_VALUE_SIZE];

   int nrows = 0;
   // Insert few values doubling value-size for each new key inserted
   for (int val_len = 16; val_len <= USER_MAX_VALUE_SIZE;
        val_len <<= 1, nrows++) {
      snprintf(key_buf, sizeof(key_buf), "Key with val_len=%d", val_len);
      memset(val_buf, 'z', val_len);

      slice key   = slice_create(strlen(key_buf), key_buf);
      slice value = slice_create(val_len, val_buf);
      rc          = splinterdb_insert(spl_handle, key, value);
      if (rc) {
         break;
      }
   }
   printf("Inserted %d rows with wide-values\n", nrows);

   nrows = 0;

   // Initialize a result struct, providing an output buffer for use
   char outbuf[USER_AVG_VALUE_SIZE];

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(spl_handle, &result, sizeof(outbuf), outbuf);

   printf("Retrieve values of different lengths using output buffer of"
          " fixed size=%lu bytes:\n",
          sizeof(outbuf));

   // Lookup keys which have increasingly wider-values, using a small fixed size
   // output buffer. When necessary, memory will be allocated for wider values.
   for (int val_len = 16; val_len <= USER_MAX_VALUE_SIZE;
        val_len <<= 1, nrows++) {

      char key_buf[USER_MAX_KEY_SIZE];
      snprintf(key_buf, sizeof(key_buf), "Key with val_len=%d", val_len);
      size_t key_len = strlen(key_buf);

      slice key = slice_create(key_len, key_buf);
      rc        = splinterdb_lookup(spl_handle, key, &result);

      slice value;
      rc = splinterdb_lookup_result_value(&result, &value);
      if (!rc) {
         printf(
            "  [%d] Found key (key_len=%d): '%.*s', value length found = %lu\n",
            nrows,
            (int)key_len,
            (int)key_len,
            key_buf,
            slice_length(value));
      } else {
         printf("Did not find key '%.*s'.\n", (int)key_len, key_buf);
      }
   }
   splinterdb_lookup_result_deinit(&result);

   splinterdb_close(&spl_handle);
   printf("Shutdown SplinterDB instance, dbname '%s'.\n\n", DB_FILE_NAME);

   return rc;
}
