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
#define APP_DB_NAME "splinterdb_iterators_example_db"

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
// Mapping from www.address to inet-IP address etc.
kv_pair www_inet_info[] =
{
      { "www.acm.org"         , "5.79.89.114, ttl=47 time=171.147 ms" }
    , { "www.wikidpedia.org"  , "208.80.154.232, ttl=52 time=99.427 ms" }
    , { "www.bbc.com"         , "151.101.188.81, ttl=57 time=28.620 ms" }
    , { "www.worldbank.org"   , "99.84.238.130, ttl=240 time=46.452 ms" }
    , { "www.vmware.com"      , "10.113.78.20, ttl=57 time=31.888 ms" }
    , { "www.eiffeltower.com" , "34.102.136.180, ttl=116 time=33.266 ms" }
    , { "www.rediff.com"      , "184.26.53.176, ttl=56 time=33.587 ms" }
    , { "www.cnet.com"        , "151.101.190.154, ttl=58 time=37.691 ms" }
    , { "www.twitter.com"     , "104.244.42.129, ttl=52 time=74.215 ms" }
    , { "www.hongkongair.com" , "104.143.9.110, ttl=49 time=91.059 ms" }
};

int num_www_addrs = (sizeof(www_inet_info) / sizeof(*www_inet_info));

// clang-format on

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
   printf("     **** SplinterDB Iterators Example program: "
          "Lexicographic sort comparison  ****\n\n");

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

   ex_msg("Insert and iterate through %d {www-url} -> {ipaddr, metrics} "
          "mapping table:\n",
          num_www_addrs);
   do_inserts(spl_handle, www_inet_info, num_www_addrs);

   // This will return all kv-pairs, including the previous mapping table.
   do_iterate_all(spl_handle, num_www_addrs);

   const char *start_key = "www.eiffeltower.com";
   do_iterate_from(spl_handle, start_key);

   start_key = "www.twitter.com";
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
   ex_msg("NOTE: Watch the keys coming out in %s sorted order.\n\n",
          "lexicographic");
   ex_msg("Iterate through all the %d keys:\n", num_keys);

   splinterdb_iterator *it = NULL;

   // -- ACTION IS HERE --
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

   // -- ACTION IS HERE --
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
