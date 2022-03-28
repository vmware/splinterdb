// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * SplinterDB Basic Iterators Example Program.
 */

#include <stdio.h>
#include <string.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "example_common.h"

/* Tag to identify messages from application program */
#define APP_ME "App-Iterators"

/*
 * App-specific 'defaults' that can be parameterized, eventually.
 */
#define APP_DB_NAME "splinterdb_apis_example_db"

#define APP_DEVICE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define APP_CACHE_SIZE_MB  64   // Size of cache; can be changed across boots

/* Application declares the limit of key-sizes it intends to use */
#define APP_MAX_KEY_SIZE ((int)100)

// Declare a struct to build a key/value pair
typedef struct kv_pair {
   char *kv_key;
   char *kv_val;
} kv_pair;

// clang-format off
// Define an array of key-value pairs to load
kv_pair inet_addr_info[] =
        {
              { "5.79.89.114"       , "www.acm.org, ttl=47 time=171.147 ms" }
            , { "208.80.154.232"    , "www.wikidpedia.org, ttl=52 time=99.427 ms" }
            , { "151.101.188.81"    , "www.bbc.com, ttl=57 time=28.620 ms" }
            , { "99.84.238.130"     , "www.worldbank.org, ttl=240 time=46.452 ms" }
            , { "10.113.78.20"      , "www.vmware.com, ttl=57 time=31.888 ms" }
            , { "34.102.136.180"    , "www.eiffeltower.com, ttl=116 time=33.266 ms" }
            , { "184.26.53.176"     , "www.rediff.com, ttl=56 time=33.587 ms" }
            , { "151.101.190.154"   , "www.cnet.com, ttl=58 time=37.691 ms" }
            , { "104.244.42.129"    , "www.twitter.com, ttl=52 time=74.215 ms" }
            , { "104.143.9.110"     , "www.hongkongair.com, ttl=49 time=91.059 ms" }
        };

// clang-format on
int num_inet_addrs = (sizeof(inet_addr_info) / sizeof(*inet_addr_info));

// Function Prototypes
static void
configure_splinter_instance(splinterdb_config *splinterdb_cfg,
                            data_config       *splinter_data_cfg,
                            const char        *filename,
                            uint64             dev_size,
                            uint64             cache_size);

static void
do_inserts(splinterdb *spl_handle, kv_pair *kv_pairs, int num_kv_pairs);

static int
do_insert(splinterdb  *spl_handle,
          const char  *key_data,
          const size_t key_len,
          const char  *value_data,
          const size_t value_len);

static int
do_iterate_all(splinterdb *spl_handle, int num_keys);

static int
do_iterate_from(splinterdb *spl_handle, const char *from_key);

/*
 * -----------------------------------------------------------------------------
 * main() Driver for SplinterDB program for iterators interfaces.
 * -----------------------------------------------------------------------------
 */
int
main()
{
   printf("     **** SplinterDB Iterators Example program ****\n\n");

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

   do_inserts(spl_handle, inet_addr_info, num_inet_addrs);

   do_iterate_all(spl_handle, num_inet_addrs);

   const char *start_key = "99.84.238.130";

   do_iterate_from(spl_handle, start_key);

   start_key = "5.79.89.114";
   do_iterate_from(spl_handle, start_key);

   start_key = "10.113.78.20";
   do_iterate_from(spl_handle, start_key);

   splinterdb_close(&spl_handle);
   ex_msg("Shutdown SplinterDB instance, dbname '%s'.\n\n", APP_DB_NAME);

   return rc;
}

/*
 * -----------------------------------------------------------------------------
 * configure_splinter_instance()
 *
 * Basic configuration of a SplinterDB instance, specifying min parameters such
 * as the device's name, device and cache sizes.
 * -----------------------------------------------------------------------------
 */
static void
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
 * -----------------------------------------------------------------------------
 * do_inserts()
 *
 * Insert a small number of key-value pairs.
 * -----------------------------------------------------------------------------
 */
static void
do_inserts(splinterdb *spl_handle, kv_pair *kv_pairs, int num_kv_pairs)
{
   int ictr = 0;
   for (; ictr < num_kv_pairs; ictr++) {
      do_insert(spl_handle,
                kv_pairs[ictr].kv_key,
                strlen(kv_pairs[ictr].kv_key),
                kv_pairs[ictr].kv_val,
                strlen(kv_pairs[ictr].kv_val));
   }
   ex_msg("Inserted %d key-value pairs for inet-addr ping times.\n\n", ictr);
}

/*
 * ---------------------------------------------------------------------------
 * do_insert()
 *
 * Insert a new key/value pair to a SplinterDB instance.
 * ----------------------------------------------------------------------------
 */
static int
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
 * do_iterate_all()
 *
 * Implement basic iterator interfaces to scan through all key-value pairs.
 * ----------------------------------------------------------------------------
 */
static int
do_iterate_all(splinterdb *spl_handle, int num_keys)
{
   ex_msg("Iterate through all the %d keys:\n", num_keys);

   splinterdb_iterator *it = NULL;

   int rc = splinterdb_iterator_init(spl_handle, &it, NULL_SLICE);

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
   return rc;
}

/*
 * ---------------------------------------------------------------------------
 * do_iterate_from()
 *
 * Implement basic iterator interfaces to scan through all key-value pairs,
 * starting from an initial search key..
 * ----------------------------------------------------------------------------
 */
static int
do_iterate_from(splinterdb *spl_handle, const char *from_key)
{
   ex_msg("Iterate through all the keys starting from '%s':\n", from_key);

   splinterdb_iterator *it = NULL;

   slice start_key = slice_create(strlen(from_key), from_key);
   int   rc        = splinterdb_iterator_init(spl_handle, &it, start_key);

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
   return rc;
}
