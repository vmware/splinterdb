// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * ----------------------------------------------------------------------------
 * SplinterDB Advanced Iterators Example Program with custom sort-comparison.
 *
 * What's new beyond previous splinterdb_iterators_example.c?
 *
 * In this program, we show the application of user-specified custom
 * key-comparison routines. The 'key' here is the 4-part IP-address, which
 * is stored as the string seen from 'ping'; i.e. "208.80.154.232" .
 * To illustrate the use of user-defined keys, we then provide a
 * sort-comparison routine, which splits up the IP-address to its
 * constituent parts, and does a numeric comparison of each 1-byte value.
 *
 * See:
 * - The definition of custom splinter_data_cfg.key_compare to the
 *   user-provided comparison function, custom_key_compare()
 * - The ip4_ipaddr_keycmp() and ip4_split() functions that show how one
 *   can deal with application-specific key formats.
 * ----------------------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"

/*
 * App-specific 'defaults' that can be parameterized, eventually.
 */
#define DB_FILE_NAME    "splinterdb_custom_ipv4_sortcmp_example_db"
#define DB_FILE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define CACHE_SIZE_MB   64   // Size of cache; can be changed across boots

// Describe the layout of fields in an IP4-address
#define IPV4_NUM_FIELDS 4
#define IPV4_NUM_DOTS   (IPV4_NUM_FIELDS - 1)

#define IP4_MIN_KEY_SIZE ((1 * IPV4_NUM_FIELDS) + IPV4_NUM_DOTS)

// Application declares the limit of key-sizes it intends to use
#define IP4_MAX_KEY_SIZE ((3 * IPV4_NUM_FIELDS) + IPV4_NUM_DOTS)

// Max # of chars in a well-formed IP4 address, including null-terminator byte
#define IPV4_MAX_KEY_BUF_SIZE (IP4_MAX_KEY_SIZE + 1)

// Key is a 4-part inet-address string, whose value is a description of
// the IP-address: its www-name, and some ping metrics.
typedef struct www_ping_metrics {
   uint32     ttl_ms; // Time-to-live
   uint32     rtt_ms; // Round trip time
   const char www_name[30];
} www_ping_metrics;

#define WWW_PING_SIZE(p)                                                       \
   (size_t)(offsetof(www_ping_metrics, www_name) + strlen((p)->www_name))

typedef struct kv_pair {
   char            *kv_key;
   www_ping_metrics kv_val;
} kv_pair;

// clang-format off
// Define a hard-coded array of key-value pairs to load
// Mapping from inet-IP address to www.address etc.
kv_pair inet_addr_info[] =
{
    //   ip-address             ttl  rtt   www-address
      { "5.79.89.114"       , {  47, 171, "www.acm.org" } }
    , { "208.80.154.232"    , {  52,  99, "www.wikidpedia.org" } }
    , { "151.101.188.81"    , {  57,  28, "www.bbc.com" } }
    , { "99.84.238.130"     , { 240,  46, "www.worldbank.org" } }
    , { "10.113.78.20"      , {  57,  32, "www.vmware.com" } }
    , { "34.102.136.180"    , {  116, 33, "www.eiffeltower.com" } }
    , { "184.26.53.176"     , {  56,  33, "www.rediff.com" } }
    , { "151.101.190.154"   , {  58,  37, "www.cnet.com" } }
    , { "104.244.42.129"    , {  52,  74, "www.twitter.com" } }
    , { "104.143.9.110"     , {  49,  91, "www.hongkongair.com" } }
};

int num_inet_addrs = (sizeof(inet_addr_info) / sizeof(*inet_addr_info));

// clang-format on

// Define min/max key values for user-defined IP4-addr keys
#define IP4ADDR_MIN_KEY "0.0.0.0"
#define IP4ADDR_MAX_KEY "255.255.255.255"

// Function Prototypes
static void
configure_splinter_instance(splinterdb_config *splinterdb_cfg,
                            data_config       *splinter_data_cfg,
                            const char        *filename,
                            uint64             dev_size,
                            uint64             cache_size);

int
custom_key_compare(const data_config *cfg, slice key1, slice key2);

int
ip4_ipaddr_keycmp(const char  *key1,
                  const size_t key1_len,
                  const char  *key2,
                  const size_t key2_len);

int
ip4_split(int *key_fields, const char *key, const size_t key_len);

static void
do_inserts(splinterdb *spl_handle, kv_pair *kv_pairs, int num_kv_pairs);

static int
do_iterate_from(splinterdb *spl_handle, const char *from_key);

static void
print_ping_metrics(int kctr, slice key, slice value);

/*
 * -----------------------------------------------------------------------------
 * main() Driver for SplinterDB program for iterators interfaces.
 * -----------------------------------------------------------------------------
 */
int
main()
{
   printf("     **** SplinterDB Iterators example program: "
          "Custom sort comparison  ****\n\n");

   // Initialize data configuration, describing your key-value properties
   data_config splinter_data_cfg;
   default_data_config_init(IP4_MAX_KEY_SIZE, &splinter_data_cfg);

   // -- ACTION IS HERE --
   // Customize key-comparison with our implementation for IP4 addresses
   // **** NOTE **** Custom key-comparision function needs to be provided
   // up-front. Every insert will invoke this method to insert the new key
   // in custom-sorted order.
   splinter_data_cfg.key_compare = custom_key_compare;

   // Basic configuration of a SplinterDB instance
   splinterdb_config splinterdb_cfg;
   configure_splinter_instance(&splinterdb_cfg,
                               &splinter_data_cfg,
                               DB_FILE_NAME,
                               (DB_FILE_SIZE_MB * 1024 * 1024),
                               (CACHE_SIZE_MB * 1024 * 1024));

   splinterdb *spl_handle = NULL; // To a running SplinterDB instance

   int rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
   if (rc) {
      printf("SplinterDB creation failed. (rc=%d)\n", rc);
      return rc;
   }

   printf("Insert and iterate through %d {ipaddr} -> {www-url, metrics} "
          "mapping table:\n",
          num_inet_addrs);
   do_inserts(spl_handle, inet_addr_info, num_inet_addrs);

   // NULL start_key => Iterate through all key-value pairs
   do_iterate_from(spl_handle, NULL);

   const char *start_key = "99.84.238.130";
   do_iterate_from(spl_handle, start_key);

   // Specify a non-existent start key; Scan should return from 104.143.9.110
   start_key = "100.101.102.103";
   do_iterate_from(spl_handle, start_key);

   splinterdb_close(&spl_handle);
   printf("Shutdown SplinterDB instance, dbname '%s'.\n\n", DB_FILE_NAME);

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
 * custom_key_compare() - Implement custom key-comparison function
 * -----------------------------------------------------------------------------
 */
int
custom_key_compare(const data_config *cfg, slice key1, slice key2)
{
   return ip4_ipaddr_keycmp((const char *)slice_data(key1),
                            slice_length(key1),
                            (const char *)slice_data(key2),
                            slice_length(key2));
}

/*
 * -----------------------------------------------------------------------------
 * ipaddr_keycmp() - Custom IPV4 IP-address key-comparison routine.
 *
 * -- ACTION IS HERE --
 * 'key1' and 'key2' are expected to be well-formed IP4 addresses.
 * - Extract each of the 4 parts of the IP-address
 * - Implement comparison by numerical sort-order of each part.
 *
 * Returns: See below:
 * -----------------------------------------------------------------------------
 */
/* Return value expected from key comparison routine */
#define KEYCMP_RV_KEY1_LT_KEY2 ((int)-1)
#define KEYCMP_RV_KEY1_EQ_KEY2 ((int)0)
#define KEYCMP_RV_KEY1_GT_KEY2 ((int)1)

int
ip4_ipaddr_keycmp(const char  *key1,
                  const size_t key1_len,
                  const char  *key2,
                  const size_t key2_len)
{
   int key1_fields[IPV4_NUM_FIELDS];
   int key2_fields[IPV4_NUM_FIELDS];

   ip4_split(key1_fields, key1, key1_len);
   ip4_split(key2_fields, key2, key2_len);

   // Do a field-by-field comparison to return in sorted order.
   int *key1p = key1_fields;
   int *key2p = key2_fields;

   // When we exit loop below, both keys are known to match.
   int rv   = KEYCMP_RV_KEY1_EQ_KEY2;
   int fctr = 0;
   do {
      if (*key1p < *key2p) {
         return KEYCMP_RV_KEY1_LT_KEY2;
      } else if (*key1p == *key2p) {
         // Advance to next field in address for both keys
         key1p++;
         key2p++;
         fctr++;
      } else {
         return KEYCMP_RV_KEY1_GT_KEY2;
      }
   } while (fctr < IPV4_NUM_FIELDS);

   return rv;
}

/*
 * -----------------------------------------------------------------------------
 * ip4_split() - Split a well-formed IPV4-address into its constituent parts.
 *
 * Returns: Output array key_fields[], populated with each piece of IP-address.
 * -----------------------------------------------------------------------------
 */
int
ip4_split(int *key_fields, const char *key, const size_t key_len)
{
   assert(key_len < IPV4_MAX_KEY_BUF_SIZE);

   // Process on-stack copy of key, so as to not trash user's data
   char keybuf[IPV4_MAX_KEY_BUF_SIZE];
   snprintf(keybuf, sizeof(keybuf), "%.*s", (int)key_len, key);

   char *cp   = (char *)keybuf;
   char *dot  = NULL;
   int   fctr = 0;

   // Split each ip-address into its constituent parts
   while ((dot = strchr(cp, '.')) != NULL) {
      *dot             = '\n';
      key_fields[fctr] = atoi(cp);
      fctr++;
      cp = (dot + 1);
   }
   key_fields[fctr] = atoi(cp);
   fctr++;

   return fctr;
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
      slice value = slice_create(WWW_PING_SIZE(&kv_pairs[ictr].kv_val),
                                 (const char *)&kv_pairs[ictr].kv_val);
      int   rc    = splinterdb_insert(spl_handle, key, value);
      if (rc) {
         printf(
            "Insert of key '%s' failed; rc=%d\n", kv_pairs[ictr].kv_key, rc);
         return;
      }
   }
   printf("Inserted %d key-value pairs for inet-addr ping times.\n\n", ictr);
}

/*
 * ---------------------------------------------------------------------------
 * do_iterate_from()
 *
 * Implement basic iterator interfaces to scan through all key-value pairs,
 * starting from an initial search key.
 * ----------------------------------------------------------------------------
 */
static int
do_iterate_from(splinterdb *spl_handle, const char *from_key)
{
   printf("Iterate through all the keys starting from '%s':\n", from_key);

   splinterdb_iterator *it = NULL;

   // Initialize start key if initial search key was provided.
   slice start_key =
      (from_key ? slice_create(strlen(from_key), from_key) : NULL_SLICE);
   int rc = splinterdb_iterator_init(spl_handle, &it, start_key);

   int i = 0;

   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      slice key, value;
      splinterdb_iterator_get_current(it, &key, &value);
      print_ping_metrics(i, key, value);
      i++;
   }
   rc = splinterdb_iterator_status(it);
   splinterdb_iterator_deinit(it);

   printf("Found %d key-value pairs\n\n", i);
   return rc;
}

/*
 * ---------------------------------------------------------------------------
 * print_ping_metrics()
 *
 * Decode a key/value pair and print ping-metrics.
 * ----------------------------------------------------------------------------
 */
static void
print_ping_metrics(int kctr, slice key, slice value)
{
   www_ping_metrics *ping_value;
   ping_value = ((www_ping_metrics *)slice_data(value));
   printf("[%d] key='%.*s', value=[ttl=%u, time=%u, name='%.*s']\n",
          kctr,
          (int)slice_length(key),
          (char *)slice_data(key),
          ping_value->ttl_ms,
          ping_value->rtt_ms,
          (int)(slice_length(value) - 8),
          ping_value->www_name);
}
