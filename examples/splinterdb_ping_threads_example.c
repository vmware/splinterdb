// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * ----------------------------------------------------------------------------
 * SplinterDB Ping Example Program with threads:
 *
 * What's new beyond previous splinterdb_ping_metrics_example?
 *
 * This program is essentially the same as splinterdb_ping_metrics_example,
 * where we 'ping' a collection of www-URLs, to gather ping metrics.
 * Here, we show the use of thread support in SplinterDB. Two threads
 * are started up.
 *
 * - One, iterates through the set of www-URLs to generate
 *   ping metrics, which are stored in the database.
 * - Another thread periodically polls for all key-value pairs, computing
 *   aggregated ping metrics from the various messages inserted by thread-1.
 *
 * The actual logic of running a 'ping', and inserting messages to
 * SplinterDB is the same as the previous program. The new thing here is a
 * demo of concurrent threaded-execution.
 *
 * See: RESOLVE: Update this ...
 * ----------------------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

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
#define APP_ME "App-ping-thread"

/*
 * App-specific 'defaults' that can be parameterized, eventually.
 */
#define APP_DB_NAME "splinterdb_ping_threads_example_db"

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

/*
 * --------------------------------------------------------------------------
 * A new INSERT will be done for the 1st time a www-site is added to the
 * telemetry collection.
 *
 * Key is a 4-part inet-address string, whose value is a description of
 * the IP-address: its www-name, and some ping metrics.
 *
 * Here, the "ping metrics" are actually aggregated over a collection of
 * individual ping-time metrics. The value is really this:
 *  - # of pings done
 *  - [min, avg, max] ping metric
 *  - this ping's elapsed-time
 *  - www-site name to which ping was done.
 * --------------------------------------------------------------------------
 */
typedef struct www_ping_metrics {
   uint32 min_ping_ms;
   uint32 avg_ping_ms;
   uint32 max_ping_ms;
   uint32 num_pings;
   uint32 this_ping_ms;
   char   www_name[30];
} www_ping_metrics;

#define WWW_PING_METRICS_SIZE(p)                                               \
   (size_t)(offsetof(www_ping_metrics, www_name) + strlen((p)->www_name))

/*
 * When a ping-metrics structure is returned as a "value", the name field
 * is not null-terminated. This define gives the length of that field.
 */
#define WWW_PING_NAME_SIZE(val_length)                                         \
   ((val_length)-offsetof(www_ping_metrics, www_name))

/*
 * --------------------------------------------------------------------------
 * An UPDATE message will be inserted for every subsequent ping metric.
 * The key remains the same as for the INSERT; i.e. the ip-address.
 *
 * Here, the "value" is just the new ping-time gathered.
 * Over-time we will see multiple such new-metric UPDATE messages recorded
 * in the db. Upon a lookup, the user-specified merge-method will aggregate
 * the metrics, to return the consolidate metric, as in:
 * <key> - {min, avg, max}-ping-elapsed-ms, # pings-done, www-name
 * --------------------------------------------------------------------------
 */
typedef struct ping_metric {
   uint32 this_ping_ms;
} ping_metric;

#define WWW_PING_SIZE() sizeof(ping_metric)

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
   char               ip_addr[NI_MAXHOST + 1]; // Allow for null-termination

   // Ping-metrics returned for each www-connection by one do_ping() call.
   uint64 ping_elapsed_ms;

} www_conn_hdlr;

// We have 2 types of threads whose params structs are defined below
#define APP_PING_NUM_THREADS 2

/*
 * Structure to package-up parameters needed by 'ping' thread to run 'ping' on
 * all known www-sites to track.
 */
typedef struct ping_thread_params {
   splinterdb    *spl_handle;
   www_conn_hdlr *conns;         // Ptr to array of www-connection handlers
   const char   **www_sites;     // Ptr to array of www-url names
   int            num_www_sites; // Length of www_sites[] & conns[] arrays.
   int            max_loops;     // # of times to ping each site
} ping_thread_params;

/*
 * Structure to package-up parameters needed by 'lookup' thread to report the
 * aggregated 'ping' metrics from all known www-sites we track.
 */
typedef struct lookup_thread_params {
   splinterdb *spl_handle;
   int         num_www_sites; // Length of www_sites[] & conns[] arrays.
} lookup_thread_params;

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

static int
custom_key_compare(const data_config *cfg, slice key1, slice key2);

static int
ip4_ipaddr_keycmp(const char  *key1,
                  const size_t key1_len,
                  const char  *key2,
                  const size_t key2_len);

static int
ip4_split(int *key_fields, const char *key, const size_t key_len);

static int
aggregate_ping_metrics(const data_config *cfg,
                       slice              key,
                       message            old_raw_message,
                       merge_accumulator *new_data);

static int
ping_metrics_final(const data_config *cfg,
                   slice              key,
                   merge_accumulator *oldest_raw_data);

static int
do_insert(splinterdb  *spl_handle,
          const char  *key_data,
          const size_t key_len,
          const char  *value_data,
          const size_t value_len);

static int
do_update(splinterdb  *spl_handle,
          const char  *key_data,
          const size_t key_len,
          const char  *value_data,
          const size_t value_len);

static void *
do_iterate_all(void *arg);

static void
print_ping_metrics(int kctr, slice key, slice value);

static void
do_dns_lookups(www_conn_hdlr *conns, const char **www_sites, int num_sites);

static char *
dns_lookup(www_conn_hdlr *conn, const char *addr_host);

static void *
do_ping_all_www_sites(void *arg);

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
main(int argv, char *argc[])
{
   printf("     **** SplinterDB Example multi-threaded Ping program: "
          "Update telemetry metrics with threaded execution messages ****\n\n");

   // Initialize data configuration, describing your key-value properties
   data_config splinter_data_cfg;
   default_data_config_init(APP_MAX_KEY_SIZE, &splinter_data_cfg);

   // Customize key-comparison with our implementation for IP4 addresses
   // **** NOTE **** Custom key-comparision function needs to be provided
   // up-front. Every insert will invoke this method to insert the new key
   // in custom-sorted order.
   sprintf(splinter_data_cfg.min_key, "%s", "0.0.0.0");
   sprintf(splinter_data_cfg.max_key, "%s", "255.255.255.255");

   splinter_data_cfg.min_key_length = strlen(splinter_data_cfg.min_key);
   splinter_data_cfg.max_key_length = strlen(splinter_data_cfg.max_key);
   splinter_data_cfg.key_compare    = custom_key_compare;

   // -- ACTION IS HERE --
   // Provide user-defined merge methods, which will do the metrics aggregation.
   splinter_data_cfg.merge_tuples       = aggregate_ping_metrics;
   splinter_data_cfg.merge_tuples_final = ping_metrics_final;

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

   www_conn_hdlr conns[NUM_WWW_SITES] = {0};

   int max_loops = 3;
   // Extract max-loops arg, if provided.
   if (argv > 1) {
      max_loops = atoi(argc[1]);
   }

   // Do DNS-lookups, and cache ip-addr for all www-sites we'll ping below
   do_dns_lookups(conns, www_sites, NUM_WWW_SITES);

   pthread_t thread_ids[APP_PING_NUM_THREADS];

   lookup_thread_params lookup_param;
   lookup_param.spl_handle    = spl_handle;
   lookup_param.num_www_sites = NUM_WWW_SITES;

   // Process all key/value pairs, and examine the aggregated metrics.
   // do_iterate_all(&lookup_param);
   rc = pthread_create(&thread_ids[0], NULL, &do_iterate_all, &lookup_param);

   // Package up the structs needed by thread to 'ping' all www-sites in a loop.
   ping_thread_params ping_param;
   ping_param.spl_handle    = spl_handle;
   ping_param.conns         = conns;
   ping_param.www_sites     = www_sites;
   ping_param.num_www_sites = NUM_WWW_SITES;
   ping_param.max_loops     = max_loops;

   // do_ping_all_www_sites(&ping_param);
   rc =
      pthread_create(&thread_ids[1], NULL, &do_ping_all_www_sites, &ping_param);

   ex_msg("Waiting for %d threads to complete:\n", APP_PING_NUM_THREADS);
   for (int tctr = 0; tctr < APP_PING_NUM_THREADS; tctr++) {
      ex_msg(" Join thread ID=%lu ...\n", thread_ids[tctr]);
      void *thread_rc;
      int   rc = pthread_join(thread_ids[tctr], &thread_rc);
      if (rc || (thread_rc != NULL)) {
         ex_err("Thread[%d] ID=%lu had an error: %p\n",
                tctr,
                thread_ids[tctr],
                thread_rc);
      }
   }

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
      ex_err("Warning! No IP found by gethostbyname() for '%s'\n", addr_host);
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
 * do_ping_all_www_sites()
 *
 * Method to do 'ping' to all www-sites, in a loop, and insert / update the
 * ping metrics in SplinterDB.
 * This is a thread-handler method implementing the "ping" thread which runs a
 * 'ping' on each www-url, collects the metrics and inserts / updates it as a
 * message to the database.
 * -----------------------------------------------------------------------------
 */
static void *
do_ping_all_www_sites(void *arg)
{
   ping_thread_params *ping_param    = (ping_thread_params *)arg;
   splinterdb         *spl_handle    = ping_param->spl_handle;
   www_conn_hdlr      *conns         = ping_param->conns;
   const char        **www_sites     = ping_param->www_sites;
   int                 num_www_sites = ping_param->num_www_sites;
   int                 max_loops     = ping_param->max_loops;

   sleep(1);
   int loopctr = 0;

   // Ping all sites, and initialize the base key-value pair for 1st ping
   ping_all_www_sites(conns, www_sites, num_www_sites);
   ex_msg("-- Finished 1st ping to all sites, loop %d.\n\n", loopctr);

   // -- ACTION IS HERE --
   // Declare an array of ping-metrics for all www-sites probed
   // www_ping_metrics metrics[num_www_sites] = {0};
   size_t            nbytes  = (num_www_sites * sizeof(www_ping_metrics));
   www_ping_metrics *metrics = (www_ping_metrics *)malloc(nbytes);
   bzero(metrics, nbytes);

   // ---------------------------------------------------------------------
   // INSERT message: Register the base metric, definining the www-site's
   // name and associated ping metrics
   for (int wctr = 0; wctr < num_www_sites; wctr++) {
      www_ping_metrics *metric = &metrics[wctr];
      sprintf(metric->www_name, "%s", www_sites[wctr]);

      // Establish this www-site's connection handler
      www_conn_hdlr *conn  = &conns[wctr];
      metric->this_ping_ms = conn->ping_elapsed_ms;
      metric->num_pings    = 1;

      // Construct the key/value pair, to drive the INSERT into SplinterDB
      const char  *key_data   = conn->ip_addr;
      const size_t key_len    = strlen(conn->ip_addr);
      const char  *value_data = (const char *)metric;
      const size_t value_len  = WWW_PING_METRICS_SIZE(metric);
      int rc = do_insert(spl_handle, key_data, key_len, value_data, value_len);
      if (rc) {
         ex_err("Insert of base metric for '%s' failed, rc=%d\n",
                metric->www_name,
                rc);
      }
   }

   loopctr++;
   // ---------------------------------------------------------------------
   // Run n-more pings, collecting elapsed time for each ping. Store this
   // in SplinterDB as an UPDATE message, which slams-in just the new
   // elapsed-time metric, associated with www-site's IP-address as the key.
   // ---------------------------------------------------------------------
   while (loopctr < max_loops) {
      ping_all_www_sites(conns, www_sites, num_www_sites);
      ex_msg("-- Finished Ping to all sites, loop %d.\n\n", loopctr);

      // Register the new ping metric as an update message for the ipaddr
      for (int wctr = 0; wctr < num_www_sites; wctr++) {
         ping_metric metric = {0};

         // Establish this www-site's connection handler
         www_conn_hdlr *conn = &conns[wctr];
         metric.this_ping_ms = conn->ping_elapsed_ms;

         // Construct the key/value pair, to drive the UPDATE into SplinterDB
         const char  *key_data = conn->ip_addr;
         const size_t key_len  = strlen(conn->ip_addr);

         // NOTE: As we are only updating the single metric, the length of the
         //       value's data is shorter than that what was inserted
         //       previously.
         const char  *value_data = (const char *)&metric;
         const size_t value_len  = WWW_PING_SIZE();
         int          rc =
            do_update(spl_handle, key_data, key_len, value_data, value_len);
         if (rc) {
            ex_err("Update of new metric for ip-addr '%s' failed, rc=%d\n",
                   key_data,
                   rc);
         }
      }
      loopctr++;
      sleep(APP_PING_EVERY_S);
   }
   return 0;
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

   struct ping_pkt    pckt;
   struct sockaddr_in r_addr;
   struct timespec    tfs;
   struct timespec    tfe;

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

   int i;
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

   // Send packet
   clock_gettime(CLOCK_MONOTONIC, &tfs);
   if (sendto(sockfd, &pckt, sizeof(pckt), 0, ping_addr, sizeof_ping_addr) <= 0)
   {
      ex_err("[%d] Ping to %s ... Packet Sending Failed!\n", wctr, www_addr);
   }

   // Receive packet
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
   ex_msg("[%d] Ping %d bytes to %s (%s) took %lu ns (%lu ms)\n",
          wctr,
          (int)sizeof(pckt),
          www_addr,
          conn->ip_addr,
          elapsed_ns,
          conn->ping_elapsed_ms);
}

/* Compute the elapsed time delta in ns between two clock_gettime() values */
static uint64
get_elapsed_ns(struct timespec *start, struct timespec *end)
{
   /*
   uint64 end_ns   = TIMESPEC_TO_NS(end);
   uint64 start_ns = TIMESPEC_TO_NS(start);
   return (end_ns - start_ns);
   */
   return (TIMESPEC_TO_NS(end) - TIMESPEC_TO_NS(start));
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
static int
custom_key_compare(const data_config *cfg, slice key1, slice key2)
{
   // ex_msg("%s() ...\n", __FUNCTION__);
   return ip4_ipaddr_keycmp((const char *)slice_data(key1),
                            slice_length(key1),
                            (const char *)slice_data(key2),
                            slice_length(key2));
}


/*
 * -----------------------------------------------------------------------------
 * ipaddr_keycmp() - Custom IPV4 IP-address key-comparison routine.
 *
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

static int
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
static int
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

/*
 * -----------------------------------------------------------------------------
 * aggregate_ping_metrics()
 *
 * User-supplied merge-callback function, which understands the semantics of
 * the "value" -- which are ping-metrics. And implements the 'merge' operation
 * to aggregate ping-metrics across multiple messages.
 * -----------------------------------------------------------------------------
 */
static int
aggregate_ping_metrics(const data_config *cfg,
                       slice              key,
                       message            old_raw_message,
                       merge_accumulator *new_raw_message)
{
   message_type result_type = message_class(old_raw_message);
   uint64       old_msg_len = message_length(old_raw_message);
   uint64       new_msg_len = merge_accumulator_length(new_raw_message);

   // ex_msg("%s(), result_type=%d ...\n", __FUNCTION__, result_type);
   const char *msgtype = "UNKNOWN";

   if (result_type == MESSAGE_TYPE_INSERT) {
      msgtype                           = "MESSAGE_TYPE_INSERT";
      www_ping_metrics *old_metrics     = NULL;
      ping_metric      *new_ping_metric = NULL;

      old_metrics     = (www_ping_metrics *)slice_data(old_raw_message.data);
      new_ping_metric = (ping_metric *)merge_accumulator_data(new_raw_message);

      // Aggregate ping-metrics in a new output struct
      www_ping_metrics agg_metrics = {0};

      uint64 new_metric = new_ping_metric->this_ping_ms;
      if (old_metrics->num_pings == 1) {
         old_metrics->min_ping_ms = old_metrics->this_ping_ms;
         old_metrics->avg_ping_ms = old_metrics->this_ping_ms;
         old_metrics->max_ping_ms = old_metrics->this_ping_ms;
      }
      agg_metrics.min_ping_ms = MIN(old_metrics->min_ping_ms, new_metric);
      agg_metrics.max_ping_ms = MAX(old_metrics->max_ping_ms, new_metric);
      agg_metrics.num_pings   = (old_metrics->num_pings + 1);
      agg_metrics.avg_ping_ms =
         ((old_metrics->avg_ping_ms * old_metrics->num_pings) + new_metric)
         / agg_metrics.num_pings;

      agg_metrics.this_ping_ms = new_metric;

      // Move-over the www-name field over to new aggregated metrics struct
      size_t old_msg_len = slice_length(old_raw_message.data);
      MEMMOVE(agg_metrics.www_name,
              old_metrics->www_name,
              WWW_PING_NAME_SIZE(old_msg_len));

      // Merge the new message with the old (aggregated) message
      message newmsg;
      newmsg.type = message_class(old_raw_message);
      newmsg.data = slice_create(old_msg_len, (void *)&agg_metrics);

      merge_accumulator_copy_message(new_raw_message, newmsg);

   } else if (result_type == MESSAGE_TYPE_UPDATE) {
      msgtype = "MESSAGE_TYPE_UPDATE";
   }
   if (0)
      ex_msg("%s: %s: old_msg_len=%lu, new_msg_len=%lu\n",
             __FUNCTION__,
             msgtype,
             old_msg_len,
             new_msg_len);
   /*
   print_ping_metrics(0, key, old_raw_message.data);
   */
   return 0;
}

/*
 * -----------------------------------------------------------------------------
 * ping_metrics_final() -- RESOLVE: Document this.
 * -----------------------------------------------------------------------------
 */
static int
ping_metrics_final(const data_config *cfg,
                   slice              key,
                   merge_accumulator *oldest_raw_data) // IN/OUT
{
   ex_msg("%s() ...\n", __FUNCTION__);
   message_type result_type = merge_accumulator_message_class(oldest_raw_data);
   size_t       msg_len     = merge_accumulator_length(oldest_raw_data);
   if (result_type == MESSAGE_TYPE_INSERT) {
      ex_msg("[%d]: MESSAGE_TYPE_INSERT:\n", __LINE__);
   } else if (result_type == MESSAGE_TYPE_UPDATE) {
      ex_msg("[%d]: MESSAGE_TYPE_UPDATE: key='%.*s', msg_len=%lu\n",
             __LINE__,
             (int)slice_length(key),
             (char *)slice_data(key),
             msg_len);
   } else {
      ex_msg("[%d]: Unknown MESSAGE_TYPE=%d:\n", __LINE__, result_type);
   }
   // print_ping_metrics(0, key, old_raw_message.data);
   return 0;
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
 * do_update()
 *
 * Update the value portion for an existing key in a SplinterDB instance.
 * ----------------------------------------------------------------------------
 */
static int
do_update(splinterdb  *spl_handle,
          const char  *key_data,
          const size_t key_len,
          const char  *value_data,
          const size_t value_len)
{
   slice key   = slice_create(key_len, key_data);
   slice value = slice_create(value_len, value_data);
   int   rc    = splinterdb_update(spl_handle, key, value);
   return rc;
}

/*
 * ---------------------------------------------------------------------------
 * do_iterate_all()
 *
 * Implement basic iterator interfaces to scan through all key-value pairs.
 * This is a thread-handler method implementing the "lookup" thread which scans
 * for ping-metrics, aggregates them for each key (ip-addr) and returns one
 * aggregated value.
 * ----------------------------------------------------------------------------
 */
static void *
do_iterate_all(void *arg)
{
   lookup_thread_params *lookup_param = (lookup_thread_params *)arg;
   splinterdb           *spl_handle   = lookup_param->spl_handle;
   int                   num_keys     = lookup_param->num_www_sites;

   int ictr = 0;
   do {
      ex_msg("Iterate through all the %d sites:\n", num_keys);

      splinterdb_iterator *it = NULL;

      int rc = splinterdb_iterator_init(spl_handle, &it, NULL_SLICE);
      if (rc) {
         return 0;
      }

      for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
         slice key;
         slice value;

         splinterdb_iterator_get_current(it, &key, &value);
         print_ping_metrics(ictr, key, value);
         ictr++;
      }
      rc = splinterdb_iterator_status(it);
      splinterdb_iterator_deinit(it);

      ex_msg("Found %d key-value pairs\n\n", ictr);
      sleep(1);
   } while (ictr != num_keys);

   return 0;
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
   ex_msg("[%d] key='%.*s'"
          ", value=[count=%u, min=%u, avg=%u, max=%u, elapsed=%u"
          ", name='%.*s']\n",
          kctr,
          (int)slice_length(key),
          (char *)slice_data(key),
          ping_value->num_pings,
          ping_value->min_ping_ms,
          ping_value->avg_ping_ms,
          ping_value->max_ping_ms,
          ping_value->this_ping_ms,
          (int)WWW_PING_NAME_SIZE(slice_length(value)),
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
