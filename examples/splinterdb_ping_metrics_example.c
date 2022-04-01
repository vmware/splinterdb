// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * ----------------------------------------------------------------------------
 * SplinterDB Advanced Iterators Example Program with custom sort-comparison.
 *
 * What's new beyond previous splinterdb_custom_ipv4_addr_sortcmp_example.c?
 *
 * In this program, we show the application of user-specified custom
 * key-comparison routines. The 'key' here the 4-part IP-address, which
 * is stored as the string seen from 'ping'; i.e. "208.80.154.232"
 * To illustrate the use of user-defined keys, we then provide a
 * sort-comparison routine, which splits up the IP-address to its
 * constituent parts, and does a numeric comparison of each 1-byte value.
 *
 * See:
 * - The definition of custom splinter_data_cfg.key_compare to the
 *   user-provided comparison function, custom_key_compare()
 * - The ip4_ipaddr_keycmp() and ip4_split() functions that show one
 *   can deal with application-specified key formats.
 *
 * Ref: https://www.geeksforgeeks.org/ping-in-c/
 * ----------------------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>

// Need following headers to get ping facility working.
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <netinet/ip_icmp.h>
#include <time.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "example_common.h"

/* Tag to identify messages from application program */
#define APP_ME "App-ping-metrics"

/*
 * App-specific 'defaults' that can be parameterized, eventually.
 */
#define APP_DB_NAME "splinterdb_custom_ipv4_sortcmp_example_db"

#define APP_DEVICE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define APP_CACHE_SIZE_MB  64   // Size of cache; can be changed across boots

// Describe the layout of fields in an IP4-address
#define APP_IPV4_NUM_FIELDS 4
#define APP_IPV4_NUM_DOTS   (APP_IPV4_NUM_FIELDS - 1)

// Application declares the limit of key-sizes it intends to use
#define APP_MAX_KEY_SIZE ((3 * APP_IPV4_NUM_FIELDS) + APP_IPV4_NUM_DOTS)

// Max # of chars in a well-formed IP4 address, including null-terminator byte
#define APP_IPV4_MAX_KEY_BUF_SIZE (APP_MAX_KEY_SIZE + 1)

#define APP_PING_EVERY_S 1 // interval between pings

// Key is a 4-part inet-address string, whose value is a description of
// the IP-address: its www-name, and some ping metrics.
typedef struct www_ping_metrics {
   uint32     ttl_ms;
   uint32     ping_ms;
   const char www_name[30];
} www_ping_metrics;

#define WWW_PING_SIZE(p)                                                       \
   (size_t)(offsetof(www_ping_metrics, www_name) + strlen((p)->www_name))

typedef struct kv_pair {
   char            *kv_key;
   www_ping_metrics kv_val;
} kv_pair;

// clang-format off
const char *www_sites[] = {  "www.acm.org"
                           , "www.wikidpedia.org"
                           , "www.vmware.com"
                           , "www.bbc.com"
                           , "www.worldbank.org"
                           , "www.eiffeltower.com"
                           , "www.rediff.com"
                           , "www.cnet.com"
                           , "www.twitter.com"
                           , "www.hongkongair.com"
                          };

// clang-format on

#define NUM_WWW_SITES ARRAY_LEN(www_sites)

/*
 * **************************************************************
 * Following definitions etc. are needed for 'ping' facility
 * **************************************************************
 */
// Consolidate stuff we need in order to do a ping
typedef struct www_conn_hdlr {
   struct sockaddr_in addr_conn;
   char               ip_addr[NI_MAXHOST];

   // Ping-metrics returned for each www-connection by one do_ping() call.
   uint64 ping_elapsed_ms;

} www_conn_hdlr;

// Ping packet size
#define PING_PKT_S 64

// Ping packet structure
struct ping_pkt {
   struct icmphdr hdr;
   char           msg[PING_PKT_S - sizeof(struct icmphdr)];
};

// Automatic port number
#define AUTO_PORT_NO 0

// Gives the timeout delay for receiving packets in seconds
#define RECV_TIMEOUT 1

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

/*
static void
do_inserts(splinterdb *spl_handle, kv_pair *kv_pairs, int num_kv_pairs);

static int
do_insert(splinterdb  *spl_handle,
          const char  *key_data,
          const size_t key_len,
          const char  *value_data,
          const size_t value_len);
*/

static int
do_iterate_all(splinterdb *spl_handle, int num_keys);

/*
static int
do_iterate_from(splinterdb *spl_handle, const char *from_key);
*/

static void
print_ping_metrics(int kctr, slice key, slice value);

static void
do_dns_lookups(www_conn_hdlr *conns, const char **www_sites, int num_sites);

static char *
dns_lookup(www_conn_hdlr *conn, const char *addr_host);

static void
ping_all_www_sites(www_conn_hdlr *conns, const char **www_sites, int num_sites);

static void
do_ping(int sockfd, int wctr, const char *www_addr, www_conn_hdlr *conn);

unsigned short
checksum(void *b, int len);

static uint64
get_elapsed_ns(struct timespec *start, struct timespec *end);

/*
 * -----------------------------------------------------------------------------
 * main() Driver for SplinterDB program for iterators interfaces.
 * -----------------------------------------------------------------------------
 */
int
main()
{
   printf("     **** SplinterDB Iterators Example program: "
          "Custom sort comparison  ****\n\n");

   // Initialize data configuration, describing your key-value properties
   data_config splinter_data_cfg;
   default_data_config_init(APP_MAX_KEY_SIZE, &splinter_data_cfg);

   // -- ACTION IS HERE --
   // Customize key-comparison with our implementation for IP4 addresses
   // **** NOTE **** Custom key-comparision function needs to be provided
   // up-front. Every insert will invoke this method to insert the new key
   // in custom-sorted order.
   sprintf(splinter_data_cfg.min_key, "%s", "0.0.0.0");
   sprintf(splinter_data_cfg.max_key, "%s", "255.255.255.255");
   splinter_data_cfg.key_compare = custom_key_compare;

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

   int           loopctr             = 0;
   int           max_loops           = 3;
   www_conn_hdlr conn[NUM_WWW_SITES] = {0};

   // Do DNS-lookups, and cache ip-addr for all www-sites we'll ping below
   do_dns_lookups(conn, www_sites, NUM_WWW_SITES);

   do {
      ping_all_www_sites(conn, www_sites, NUM_WWW_SITES);
      loopctr++;
      sleep(APP_PING_EVERY_S);
   } while (loopctr < max_loops);

   do_iterate_all(spl_handle, 0);

   splinterdb_close(&spl_handle);
   ex_msg("Shutdown SplinterDB instance, dbname '%s'.\n\n", APP_DB_NAME);

   return rc;
}

/*
 * -----------------------------------------------------------------------------
 * do_dns_lookups() - On an array of www-sites, caching in IP-addresses
 * -----------------------------------------------------------------------------
 */
static void
do_dns_lookups(www_conn_hdlr *conns, const char **www_sites, int num_sites)
{
   for (int wctr = 0; wctr < num_sites; wctr++) {
      if (!dns_lookup(&conns[wctr], www_sites[wctr])) {
         ex_err("DNS lookup failed for %s\n", www_sites[wctr]);
      }
   }
}


/*
 * -----------------------------------------------------------------------------
 * Performs a DNS on one www-addr, populating output www_conn_hdlr *conn handle
 * -----------------------------------------------------------------------------
 */
static char *
dns_lookup(www_conn_hdlr *conn, const char *addr_host)
{
   struct hostent *host_entity;
   if ((host_entity = gethostbyname(addr_host)) == NULL) {
      // No ip found for hostname
      return NULL;
   }

   // Filling up address structure
   char *ip = (char *)&conn->ip_addr;
   strcpy(ip, inet_ntoa(*(struct in_addr *)host_entity->h_addr));

   struct sockaddr_in *addr_conn = &conn->addr_conn;
   addr_conn->sin_family         = host_entity->h_addrtype;
   addr_conn->sin_port           = htons(AUTO_PORT_NO);
   addr_conn->sin_addr.s_addr    = *(long *)host_entity->h_addr;
   return ip;
}

/*
 * -----------------------------------------------------------------------------
 * ping_all_www_sites()
 *
 * Cycle thru a known list of www-sites (whose DNS-lookup has been done).
 * Ping each site, collect and return the ping metrics through conn_hdlr struct.
 * -----------------------------------------------------------------------------
 */
static void
ping_all_www_sites(www_conn_hdlr *conns, const char **www_sites, int num_sites)
{
   for (int wctr = 0; wctr < num_sites; wctr++) {

      // Establish a new socket fd for each www-site, first time
      int sockfd = socket(AF_INET, SOCK_RAW, IPPROTO_ICMP);

      int ttl_val = 64;
      // set socket options at ip to TTL and value to 64,
      // change to what you want by setting ttl_val
      if (setsockopt(sockfd, SOL_IP, IP_TTL, &ttl_val, sizeof(ttl_val)) != 0) {
         ex_err("\nSetting socket options for sockfd=%d to TTL failed!\n",
                sockfd);
         return;
      }
      struct timeval tv_out;
      tv_out.tv_sec  = RECV_TIMEOUT;
      tv_out.tv_usec = 0;

      // setting timeout of recv setting
      setsockopt(
         sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv_out, sizeof tv_out);

      do_ping(sockfd, wctr, www_sites[wctr], &conns[wctr]);

      close(sockfd);
   }
}

/*
 * -----------------------------------------------------------------------------
 * do_ping()
 *
 * Make a 'ping' request to one www-site. Return the ping-metrics
 * through ping-metrics fields in output www_conn_hdlr *conn struct.
 * -----------------------------------------------------------------------------
 */
static void
do_ping(int sockfd, int wctr, const char *www_addr, www_conn_hdlr *conn)
{
   struct sockaddr *ping_addr = (struct sockaddr *)&conn->addr_conn;

   // int    msg_count = 0;
   int i;

   struct ping_pkt    pckt;
   struct sockaddr_in r_addr;
   struct timespec    tfs;
   struct timespec    tfe;

   int ttl_val = 64;
   // set socket options at ip to TTL and value to 64,
   // change to what you want by setting ttl_val
   if (setsockopt(sockfd, SOL_IP, IP_TTL, &ttl_val, sizeof(ttl_val)) != 0) {
      ex_err("\nSetting TTL options for sockfd=%d failed!\n", sockfd);
      return;
   }
   struct timeval tv_out;
   tv_out.tv_sec  = RECV_TIMEOUT;
   tv_out.tv_usec = 0;

   // setting timeout of recv setting
   setsockopt(
      sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv_out, sizeof tv_out);

   // filling packet
   bzero(&pckt, sizeof(pckt));
   pckt.hdr.type       = ICMP_ECHO;
   pckt.hdr.un.echo.id = getpid();

   for (i = 0; i < sizeof(pckt.msg) - 1; i++) {
      pckt.msg[i] = i + '0';
   }

   pckt.msg[i]               = 0;
   pckt.hdr.un.echo.sequence = wctr;
   pckt.hdr.checksum         = checksum(&pckt, sizeof(pckt));

   uint32 addr_len         = sizeof(r_addr);
   size_t sizeof_ping_addr = sizeof(conn->addr_conn);

   // Clear out returned ping-metrics, from previous call
   conn->ping_elapsed_ms = 0;

   // send packet
   clock_gettime(CLOCK_MONOTONIC, &tfs);
   if (sendto(sockfd, &pckt, sizeof(pckt), 0, ping_addr, sizeof_ping_addr) <= 0)
   {
      ex_err("[%d] Ping to %s ... Packet Sending Failed!\n", wctr, www_addr);
   }

   // receive packet
   if (recvfrom(
          sockfd, &pckt, sizeof(pckt), 0, (struct sockaddr *)&r_addr, &addr_len)
       <= 0)
   {
      ex_err("[%d] Ping to %s ... Packet receive failed!\n", wctr, www_addr);
   }

   clock_gettime(CLOCK_MONOTONIC, &tfe);
   uint64 elapsed_ns = get_elapsed_ns(&tfs, &tfe);
   uint64 elapsed_ms = NSEC_TO_MSEC(elapsed_ns);

   conn->ping_elapsed_ms = elapsed_ms;
   ex_msg("[%d] Ping %d bytes to %s took %lu ns (%lu ms)\n",
          wctr,
          (int)sizeof(pckt),
          www_addr,
          elapsed_ns,
          conn->ping_elapsed_ms);
}

/* Compute the elapsed time delta in ns between two clock_gettime() values */
static uint64
get_elapsed_ns(struct timespec *start, struct timespec *end)
{
   // return(TIMESPEC_TO_NS(end) - TIMESPEC_TO_NS(start));
   uint64 end_ns   = TIMESPEC_TO_NS(end);
   uint64 start_ns = TIMESPEC_TO_NS(start);
   return (end_ns - start_ns);
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
 // -- ACTION IS HERE --
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
   int key1_fields[APP_IPV4_NUM_FIELDS];
   int key2_fields[APP_IPV4_NUM_FIELDS];

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
   } while (fctr < APP_IPV4_NUM_FIELDS);

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
   assert(key_len < APP_IPV4_MAX_KEY_BUF_SIZE);

   // Process on-stack copy of key, so as to not trash user's data
   char keybuf[APP_IPV4_MAX_KEY_BUF_SIZE];
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

#if UNDEF
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
                (const char *)&kv_pairs[ictr].kv_val,
                // strlen(kv_pairs[ictr].kv_val));
                WWW_PING_SIZE(&kv_pairs[ictr].kv_val));
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
#endif // UNDEF

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
      slice key;
      slice value;

      splinterdb_iterator_get_current(it, &key, &value);
      print_ping_metrics(i, key, value);
      i++;
   }
   rc = splinterdb_iterator_status(it);
   splinterdb_iterator_deinit(it);

   ex_msg("Found %d key-value pairs\n\n", i);
   return rc;
}

#if UNDEF
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
      print_ping_metrics(i, key, value);
      i++;
   }
   rc = splinterdb_iterator_status(it);
   splinterdb_iterator_deinit(it);

   ex_msg("Found %d key-value pairs\n\n", i);
   return rc;
}

#endif /* UNDEF */

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
   ex_msg("[%d] key='%.*s', value=[ttl=%u, time=%u, name='%.*s']\n",
          kctr,
          (int)slice_length(key),
          (char *)slice_data(key),
          ping_value->ttl_ms,
          ping_value->ping_ms,
          (int)(slice_length(value) - 8),
          ping_value->www_name);
}

// Calculating the Check Sum
unsigned short
checksum(void *b, int len)
{
   unsigned short *buf = b;
   unsigned int    sum = 0;
   unsigned short  result;

   for (sum = 0; len > 1; len -= 2)
      sum += *buf++;
   if (len == 1)
      sum += *(unsigned char *)buf;
   sum = (sum >> 16) + (sum & 0xFFFF);
   sum += (sum >> 16);
   result = ~sum;
   return result;
}
