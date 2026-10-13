// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/* Public-API comparison for predicate-filtered ranges. Compile the same source
 * against the baseline (scan + application predicate) and with
 * -DPREDICATE_RANGE_FILTER_API against the candidate. Run only under the
 * campaign's exclusive benchmark wrapper, with db_path on the verified disk
 * filesystem. */
#include <assert.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"

#define TENANTS 32

static bool hash_collisions;

typedef struct tenant_filter {
   uint64_t tenant;
   uint64_t calls;
} tenant_filter;

static uint64_t
read_be(const unsigned char *p)
{
   uint64_t v = 0;
   for (int i = 0; i < 8; i++) {
      v = (v << 8) | p[i];
   }
   return v;
}

static void
write_be(unsigned char *p, uint64_t v)
{
   for (int i = 7; i >= 0; i--) {
      p[i] = v;
      v >>= 8;
   }
}

static uint32_t
hash_tenant(const data_config *cfg, user_key k, uint32_t seed)
{
   const unsigned char *bytes = slice_data(k.key);
   assert(slice_length(k.key) == 16);
   uint32_t tenant = (uint32_t)read_be(bytes + 8);
   return (hash_collisions ? tenant / 2 : tenant) * 2654435761U ^ seed;
}

static bool
tenant_matches(void *context, slice key)
{
   tenant_filter *filter = context;
   filter->calls++;
   assert(slice_length(key) == 16);
   return read_be((const unsigned char *)slice_data(key) + 8) == filter->tenant;
}

static uint64_t
nanos(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

static void
check(int rc)
{
   if (rc != 0) {
      fprintf(stderr, "SplinterDB error: %s (%d)\n", strerror(rc), rc);
      exit(1);
   }
}

int
main(int argc, char **argv)
{
   if (argc < 2 || argc > 4) {
      fprintf(stderr,
              "usage: %s db_path [keys_per_tenant=16384] [queries=2000]\n",
              argv[0]);
      return 2;
   }
   uint64_t rows    = argc > 2 ? strtoull(argv[2], NULL, 10) : 16384;
   uint64_t queries = argc > 3 ? strtoull(argv[3], NULL, 10) : 2000;
   assert(rows > 100 && queries > 0);
   const char *collision_env = getenv("FILTER_HASH_COLLISIONS");
   hash_collisions           = collision_env && strcmp(collision_env, "1") == 0;
#ifdef PREDICATE_RANGE_FILTER_API
   const char *mode = getenv("FILTER_MODE");
   if (mode == NULL) {
      mode = "pruned";
   }
   if (strcmp(mode, "pruned") != 0 && strcmp(mode, "predicate") != 0) {
      fprintf(stderr, "FILTER_MODE must be pruned or predicate\n");
      return 2;
   }
#else
   const char *mode = "application";
#endif
   data_config data_cfg;
   default_data_config_init(&data_cfg);
   data_cfg.key_hash     = hash_tenant;
   splinterdb_config cfg = {.filename          = argv[1],
                            .io_flags          = O_RDWR | O_CREAT | O_DIRECT,
                            .cache_size        = 64ULL << 20,
                            .disk_size         = 2ULL << 30,
                            .memtable_capacity = 4ULL << 20,
                            .data_cfg          = &data_cfg};
   splinterdb       *db  = NULL;
   check(splinterdb_create(&cfg, &db));
   unsigned char key[16];
   unsigned char value[64] = {0};
   uint64_t      begin     = nanos();
   /* Grouped ingestion leaves some immutable branches selective by tenant;
    * queries run before close/reopen compacts away that arrangement. */
   for (uint64_t tenant = 0; tenant < TENANTS; tenant++) {
      write_be(key + 8, tenant);
      for (uint64_t i = 0; i < rows; i++) {
         write_be(key, i);
         memcpy(value, key, sizeof(key));
         check(splinterdb_insert(db,
                                 slice_create(sizeof(key), key),
                                 slice_create(sizeof(value), value),
                                 NULL));
      }
   }
   fprintf(stderr,
           "loaded=%" PRIu64 " elapsed_ns=%" PRIu64 "\n",
           rows * TENANTS,
           nanos() - begin);
   printf("mode,collisions,keys_per_tenant,limit,queries,ns_per_query,visited,"
          "predicate_calls,checksum\n");
   const int limits[] = {1, 2, 10, 100};
   for (unsigned int l = 0; l < sizeof(limits) / sizeof(limits[0]); l++) {
      uint64_t      seed = 42, visited = 0, checksum = 0;
      tenant_filter filter = {0};
      begin                = nanos();
      for (uint64_t q = 0; q < queries; q++) {
         seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
         uint64_t start             = (seed >> 32) % (rows - limits[l]);
         uint64_t tenant            = q % TENANTS;
         filter.tenant              = tenant;
         unsigned char hash_key[16] = {0};
         write_be(hash_key + 8, tenant);
         write_be(key, start);
         write_be(key + 8, 0);
         splinterdb_iterator *it = NULL;
#ifdef PREDICATE_RANGE_FILTER_API
         check(splinterdb_iterator_init_with_filter(
            db,
            &it,
            greater_than_or_equal,
            slice_create(sizeof(key), key),
            tenant_matches,
            &filter,
            strcmp(mode, "pruned") == 0
               ? slice_create(sizeof(hash_key), hash_key)
               : NULL_SLICE));
#else
         check(splinterdb_iterator_init(
            db, &it, greater_than_or_equal, slice_create(sizeof(key), key)));
#endif
         int count = 0;
         while (splinterdb_iterator_valid(it)) {
            slice k, v;
            splinterdb_iterator_get_current(it, &k, &v);
            visited++;
            const unsigned char *bytes = slice_data(k);
#ifdef PREDICATE_RANGE_FILTER_API
            bool matches = read_be(bytes + 8) == tenant;
#else
            bool matches = tenant_matches(&filter, k);
#endif
            if (matches) {
               assert(read_be(bytes) == start + count);
               assert(slice_length(v) == sizeof(value));
               assert(memcmp(slice_data(v), bytes, sizeof(key)) == 0);
               checksum += read_be(bytes) + tenant;
               if (++count == limits[l]) {
                  break;
               }
            }
#ifdef PREDICATE_RANGE_FILTER_API
            else
            {
               assert(0 && "nonmatching key from filtered iterator");
            }
#endif
            splinterdb_iterator_next(it);
         }
         check(splinterdb_iterator_status(it));
         assert(count == limits[l]);
         splinterdb_iterator_deinit(it);
      }
      printf("%s,%d,%" PRIu64 ",%d,%" PRIu64 ",%.1f,%" PRIu64 ",%" PRIu64
             ",%" PRIu64 "\n",
             mode,
             hash_collisions,
             rows,
             limits[l],
             queries,
             (double)(nanos() - begin) / queries,
             visited,
             filter.calls,
             checksum);
      fflush(stdout);
   }
   check(splinterdb_close(&db, false));
   return 0;
}
