// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * SplinterDB Iterators example program, using start-key for scan.
 */
#include <stdio.h>
#include <string.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"

#define DB_FILE_NAME    "splinterdb_iterators_example_db"
#define DB_FILE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define CACHE_SIZE_MB   64   // Size of cache; can be changed across boots

/* Application declares the limit of key-sizes it intends to use */
#define USER_MAX_KEY_SIZE ((int)100)

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
    , { "www.hongkongair.com" , "104.143.9.110, ttl=49 time=91.059 ms" }
};

int num_www_addrs = (sizeof(www_inet_info) / sizeof(*www_inet_info));

// clang-format on

// Function Prototypes
static void
do_inserts(splinterdb *spl_handle, kv_pair *kv_pairs, int num_kv_pairs);

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
   printf("     **** SplinterDB Iterators example program: "
          "Lexicographic sort comparison  ****\n\n");

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

   printf("Insert and iterate through %d {www-url} -> {ipaddr, metrics} "
          "mapping table:\n",
          num_www_addrs);
   do_inserts(spl_handle, www_inet_info, num_www_addrs);

   // Iterate through all key-value pairs, w/o specifying start-key
   do_iterate_from(spl_handle, NULL);

   const char *start_key = "www.eiffeltower.com";
   do_iterate_from(spl_handle, start_key);

   // Iteration should start from key past the non-existent start-key
   start_key = "www.twitter.com";
   do_iterate_from(spl_handle, start_key);

   splinterdb_close(&spl_handle);
   printf("Shutdown SplinterDB instance, dbname '%s'.\n\n", DB_FILE_NAME);

   return rc;
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
      slice key =
         slice_create(strlen(kv_pairs[ictr].kv_key), kv_pairs[ictr].kv_key);
      slice value =
         slice_create(strlen(kv_pairs[ictr].kv_val), kv_pairs[ictr].kv_val);
      int rc = splinterdb_insert(spl_handle, key, value);
      if (rc) {
         printf(
            "Insert for key='%s' failed, rc=%d\n", kv_pairs[ictr].kv_key, rc);
         return;
      }
   }
   printf("Inserted %d unordered key-value pairs for inet-addr ping times.\n\n",
          ictr);
}

/*
 * ---------------------------------------------------------------------------
 * do_iterate_from()
 *
 * Implement basic iterator interfaces to scan through all key-value pairs,
 * starting from an initial search key. If start key is NULL, return all pairs.
 * ----------------------------------------------------------------------------
 */
static int
do_iterate_from(splinterdb *spl_handle, const char *from_key)
{
   printf("Iterate through all the keys starting from '%s':\n", from_key);

   splinterdb_iterator *it = NULL;

   // -- ACTION IS HERE --
   // Initialize start key if initial search key was provided.
   slice start_key =
      (from_key ? slice_create(strlen(from_key), from_key) : NULL_SLICE);
   int rc = splinterdb_iterator_init(spl_handle, &it, start_key);

   int i = 0;

   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      slice key, value;
      splinterdb_iterator_get_current(it, &key, &value);
      printf("[%d] key='%.*s', value='%.*s'\n",
             i,
             (int)slice_length(key),
             (char *)slice_data(key),
             (int)slice_length(value),
             (char *)slice_data(value));
      i++;
   }
   rc = splinterdb_iterator_status(it);
   splinterdb_iterator_deinit(it);

   printf("Found %d key-value pairs\n\n", i);
   return rc;
}
