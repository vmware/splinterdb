// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * SplinterDB Basic APIs Example Program.
 */

#include <stdio.h>
#include <string.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "example_common.h"

/* Tag to identify messages from application program */
#define APP_ME "App-APIs"

/*
 * App-specific 'defaults' that can be parameterized, eventually.
 */
#define APP_DB_NAME "splinterdb_apis_example_db"

#define APP_DEVICE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define APP_CACHE_SIZE_MB  64   // Size of cache; can be changed across boots

/* Application declares the limit of key-sizes it intends to use */
#define APP_MAX_KEY_SIZE ((int)100)

/* Application declares its expected avg value size and max value size */
#define APP_AVG_VALUE_SIZE ((int)128)
#define APP_MAX_VALUE_SIZE ((int)1024)

// Declare a struct to build a key/value pair
typedef struct kv_pair {
   char *kv_key;
   char *kv_val;
} kv_pair;

// Function Prototypes
void
configure_splinter_instance(splinterdb_config *splinterdb_cfg,
                            data_config       *splinter_data_cfg,
                            const char        *filename,
                            uint64             dev_size,
                            uint64             cache_size);

void
do_inserts_and_lookups(splinterdb *spl_handle);

int
do_inserts_wide_values(splinterdb *spl_handle);

int
do_insert(splinterdb  *spl_handle,
          const char  *key_data,
          const size_t key_len,
          const char  *value_data,
          const size_t value_len);

int
do_lookup(splinterdb *spl_handle, const char *key_data, const size_t key_len);

int
do_lookups_with_output_buf(splinterdb *spl_handle);

/*
 * -------------------------------------------------------------------------------
 * main() Driver for SplinterDB program for insert / loookup / delete APIs.
 * -------------------------------------------------------------------------------
 */
int
main()
{
   printf("     **** SplinterDB Basic APIs Example program ****\n\n");

   // Initialize data configuration, describing your key-value properties
   data_config splinter_data_cfg;
   default_data_config_init(APP_MAX_KEY_SIZE, &splinter_data_cfg);

   // Basic configuration of a SplinterDB instance
   splinterdb_config splinterdb_cfg;
   configure_splinter_instance(&splinterdb_cfg,
                               &splinter_data_cfg,
                               APP_DB_NAME,
                               (APP_DEVICE_SIZE_MB * K_MiB),
                               (APP_CACHE_SIZE_MB * K_MiB));

   splinterdb *spl_handle = NULL; // To a running SplinterDB instance

   int rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
   if (rc) {
      ex_err("SplinterDB creation failed. (rc=%d)\n", rc);
      return rc;
   }
   ex_msg("Created SplinterDB instance, dbname '%s'.\n\n", APP_DB_NAME);

   // -- ACTION IS HERE --
   do_inserts_and_lookups(spl_handle);

   do_inserts_wide_values(spl_handle);

   // -- ACTION IS HERE --
   do_lookups_with_output_buf(spl_handle);

   splinterdb_close(&spl_handle);
   ex_msg("Shutdown SplinterDB instance, dbname '%s'.\n\n", APP_DB_NAME);

   return rc;
}

/*
 * -------------------------------------------------------------------------------
 * configure_splinter_instance()
 *
 * Basic configuration of a SplinterDB instance, specifying min parameters such
 * as the device's name, device and cache sizes.
 * -------------------------------------------------------------------------------
 */
void
configure_splinter_instance(splinterdb_config *splinterdb_cfg,
                            data_config       *splinter_data_cfg,
                            const char        *filename,
                            uint64             dev_size, // in bytes
                            uint64             cache_size)           // in bytes
{
   memset(splinterdb_cfg, 0, sizeof(*splinterdb_cfg));
   splinterdb_cfg->filename   = filename;
   splinterdb_cfg->disk_size  = dev_size;
   splinterdb_cfg->cache_size = cache_size;
   splinterdb_cfg->data_cfg   = splinter_data_cfg;
   return;
}

/*
 * ---------------------------------------------------------------------------
 * do_inserts_and_lookups()
 *
 * Insert a small number of hard-coded key-value pairs. Perform few lookups.
 */
void
do_inserts_and_lookups(splinterdb *spl_handle)
{
   int num_kv_pairs = 3;
   // clang-format off
   // Insert a set of key/value pairs
   kv_pair kv_pairs[] = {
                            { "k-1"       , "value-1" },
                            { "key-2"     , "value-2" },
                            { "key-third" , "3rd key's value: March" },
                        };
   // clang-format on

   for (int ictr = 0; ictr < num_kv_pairs; ictr++) {
      do_insert(spl_handle,
                kv_pairs[ictr].kv_key,
                strlen(kv_pairs[ictr].kv_key),
                kv_pairs[ictr].kv_val,
                strlen(kv_pairs[ictr].kv_val));
   }

   // Lookup a few keys that are known to exist
   for (int ictr = 0; ictr < num_kv_pairs; ictr++) {
      const char *key_data = kv_pairs[ictr].kv_key;
      size_t      key_len  = strlen(kv_pairs[ictr].kv_key);
      do_lookup(spl_handle, key_data, key_len);
   }

   do_lookup(spl_handle, "key-not-found", sizeof("key-not-found"));
}

/*
 * ---------------------------------------------------------------------------
 * do_insert()
 *
 * Insert a new key/value pair to a SplinterDB instance.
 * ----------------------------------------------------------------------------
 */
int
do_insert(splinterdb  *spl_handle,
          const char  *key_data,
          const size_t key_len,
          const char  *value_data,
          const size_t value_len)
{
   slice key   = slice_create(key_len, key_data);
   slice value = slice_create(value_len, value_data);
   int   rc    = splinterdb_insert(spl_handle, key, value);
   return rc;
}

/*
 * ---------------------------------------------------------------------------
 * do_lookup()
 *
 * Lookup a value given a key.
 * ---------------------------------------------------------------------------
 */
int
do_lookup(splinterdb *spl_handle, const char *key_data, const size_t key_len)
{
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(spl_handle, &result, 0, NULL);

   slice key = slice_create(key_len, key_data);
   int   rc  = splinterdb_lookup(spl_handle, key, &result);

   slice value;
   rc = splinterdb_lookup_result_value(spl_handle, &result, &value);
   if (!rc) {
      ex_msg("Found key: '%.*s', value: '%.*s'.\n",
             (int)key_len,
             key_data,
             (int)slice_length(value),
             (char *)slice_data(value));
   } else {
      ex_err("Did not find key '%.*s'.\n", (int)key_len, key_data);
   }

   splinterdb_lookup_result_deinit(&result);
   return rc;
}

/*
 * ---------------------------------------------------------------------------
 * do_inserts_wide_values()
 *
 * Insert few more key/value pairs with increasing length of the value.
 * This data will later be used to demonstrate use of output buffers during
 * lookups.
 */
int
do_inserts_wide_values(splinterdb *spl_handle)
{
   int  rc = 0;
   char key_buf[APP_MAX_KEY_SIZE];
   char val_buf[APP_MAX_VALUE_SIZE];

   // Insert few values with increasing sizes.
   int nrows = 0;
   for (int ictr = 16; ictr <= APP_MAX_VALUE_SIZE; ictr <<= 1, nrows++) {
      snprintf(key_buf, sizeof(key_buf), "Key: val_len=%d", ictr);
      memset(val_buf, 'z', ictr);

      rc = do_insert(spl_handle, key_buf, strlen(key_buf), val_buf, ictr);
      if (rc) {
         break;
      }
   }
   ex_msg("Inserted %d rows with wide-values\n", nrows);
   return rc;
}

/*
 * ---------------------------------------------------------------------------
 * do_lookups_with_output_buf()
 *
 * Lookup keys which have increasingly wider-values, using a fixed sized
 * output buffer. This example shows how we can start with an average sized
 * output buffer, to retrieve multiple key/value pairs. When necessary, memory
 * will be allocated for wider values.
 * ---------------------------------------------------------------------------
 */
int
do_lookups_with_output_buf(splinterdb *spl_handle)
{
   int  rc    = 0;
   int  nrows = 0;
   char outbuf[APP_AVG_VALUE_SIZE];

   /* Initialize a result struct, providing an output buffer for use */
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(spl_handle, &result, sizeof(outbuf), outbuf);

   for (int ictr = 16; ictr <= APP_MAX_VALUE_SIZE; ictr <<= 1, nrows++) {

      char key_buf[APP_MAX_KEY_SIZE];
      snprintf(key_buf, sizeof(key_buf), "Key: val_len=%d", ictr);
      size_t key_len = strlen(key_buf);

      slice key = slice_create(key_len, key_buf);
      rc        = splinterdb_lookup(spl_handle, key, &result);

      slice value;
      rc = splinterdb_lookup_result_value(spl_handle, &result, &value);
      if (!rc) {
         ex_msg("Found key: '%.*s', value length = %lu\n",
                (int)key_len,
                key_buf,
                slice_length(value));
      } else {
         ex_err("Did not find key '%.*s'.\n", (int)key_len, key_buf);
      }
   }
   splinterdb_lookup_result_deinit(&result);
   return rc;
}
