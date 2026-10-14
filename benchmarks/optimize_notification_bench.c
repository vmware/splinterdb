/* Repeated blocking optimize after an initial full optimization. Full-range
 * calls still perform compaction CPU work. OPTIMIZE_EMPTY_RANGE=1 selects equal
 * bounds that complete inline to measure notification/API overhead separately.
 * Neither case measures compaction throughput under an update workload. */
#ifndef _GNU_SOURCE
#   define _GNU_SOURCE
#endif
#include <assert.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"

static uint64_t
now_ns(void)
{
   struct timespec t;
   clock_gettime(CLOCK_MONOTONIC, &t);
   return (uint64_t)t.tv_sec * 1000000000 + t.tv_nsec;
}

static void
checked(int rc, const char *what)
{
   if (rc) {
      fprintf(stderr, "%s failed: %d\n", what, rc);
      exit(1);
   }
}

static int
cmp_u64(const void *a, const void *b)
{
   uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
   return (x > y) - (x < y);
}

static void
make_key(uint64_t id, char key[24])
{
   for (unsigned i = 0; i < 8; i++)
      key[i] = id >> (56 - 8 * i);
   memset(key + 8, 'k', 16);
}

static void
optimize(splinterdb *db, bool empty_range)
{
   slice bound = empty_range ? slice_create(1, "k") : NULL_SLICE;
#ifdef HAVE_SENTINEL_NOTIFICATION
   checked(splinterdb_optimize(db, bound, bound, true, NULL), "optimize");
#else
   splinterdb_notification note;
   splinterdb_notification_init_blocking(&note);
   checked(splinterdb_optimize(db, bound, bound, true, &note), "optimize");
   splinterdb_notification_deinit(&note);
#endif
}

int
main(int argc, char **argv)
{
   if (argc != 6) {
      fprintf(stderr, "usage: %s DB CSV LABEL WORKERS REPETITIONS\n", argv[0]);
      return 2;
   }
   uint64_t workers = strtoull(argv[4], NULL, 10);
   uint64_t count   = strtoull(argv[5], NULL, 10);
   if (workers > 1 || !count || count > 1000000)
      return 2;
   const char *empty_env   = getenv("OPTIMIZE_EMPTY_RANGE");
   bool        empty_range = empty_env && strcmp(empty_env, "1") == 0;
   data_config dcfg;
   default_data_config_init(&dcfg);
   splinterdb_config cfg = {
      .filename                = argv[1],
      .cache_size              = 128 * 1024 * 1024,
      .disk_size               = (uint64_t)8 * 1024 * 1024 * 1024,
      .data_cfg                = &dcfg,
      .num_normal_bg_threads   = workers,
      .num_memtable_bg_threads = 0,
      .io_flags                = O_RDWR | O_CREAT | O_DIRECT,
   };
   splinterdb *db;
   checked(splinterdb_create(&cfg, &db), "create");
   const uint64_t keys = 20000;
   for (uint64_t id = 0; id < keys; id++) {
      char key[24], value[100];
      make_key(id, key);
      memset(value, 'v', sizeof(value));
      memcpy(value, &id, sizeof(id));
      checked(splinterdb_insert(db,
                                slice_create(sizeof(key), key),
                                slice_create(sizeof(value), value),
                                NULL),
              "insert");
   }
#ifdef HAVE_SENTINEL_NOTIFICATION
   checked(splinterdb_durable_barrier(db, NULL), "barrier");
#else
   checked(splinterdb_durable_barrier(db), "barrier");
#endif
   for (uint64_t i = 0; i < 100; i++)
      optimize(db, false);
   uint64_t *latencies = calloc(count, sizeof(*latencies));
   assert(latencies);
   struct rusage before, after;
   getrusage(RUSAGE_SELF, &before);
   uint64_t start = now_ns();
   for (uint64_t i = 0; i < count; i++) {
      uint64_t t = now_ns();
      optimize(db, empty_range);
      latencies[i] = now_ns() - t;
   }
   uint64_t elapsed = now_ns() - start;
   getrusage(RUSAGE_SELF, &after);
   qsort(latencies, count, sizeof(*latencies), cmp_u64);
   double cpu = (after.ru_utime.tv_sec - before.ru_utime.tv_sec)
                + (after.ru_stime.tv_sec - before.ru_stime.tv_sec)
                + ((after.ru_utime.tv_usec - before.ru_utime.tv_usec)
                   + (after.ru_stime.tv_usec - before.ru_stime.tv_usec))
                     / 1e6;
   checked(splinterdb_close(&db, false), "close");
   checked(splinterdb_open(&cfg, &db), "reopen");
   splinterdb_iterator *it;
   checked(splinterdb_iterator_init(db, &it), "iterator init");
   for (uint64_t id = 0; id < keys; id++) {
      assert(splinterdb_iterator_valid(it));
      slice k, v;
      splinterdb_iterator_get_current(it, &k, &v);
      char expected[24];
      make_key(id, expected);
      assert(slice_length(k) == sizeof(expected)
             && memcmp(slice_data(k), expected, sizeof(expected)) == 0);
      assert(slice_length(v) == 100
             && memcmp(slice_data(v), &id, sizeof(id)) == 0);
      splinterdb_iterator_next(it);
   }
   assert(!splinterdb_iterator_valid(it));
   checked(splinterdb_iterator_status(it), "iterator status");
   splinterdb_iterator_deinit(it);
   checked(splinterdb_close(&db, false), "verified close");
   FILE *csv = fopen(argv[2], "a+");
   assert(csv);
   fseek(csv, 0, SEEK_END);
   if (ftell(csv) == 0)
      fprintf(
         csv,
         "label,range,workers,repetitions,keys,seconds,cpu_s,mean_us,p50_us,"
         "p99_us,read_bytes,write_bytes,verified\n");
   fprintf(csv,
           "%s,%s,%" PRIu64 ",%" PRIu64 ",%" PRIu64
           ",%.6f,%.6f,%.3f,%.3f,%.3f,%" PRIu64 ",%" PRIu64 ",1\n",
           argv[3],
           empty_range ? "empty" : "full",
           workers,
           count,
           keys,
           elapsed / 1e9,
           cpu,
           elapsed / 1e3 / count,
           latencies[count / 2] / 1e3,
           latencies[(count - 1) * 99 / 100] / 1e3,
           (uint64_t)(after.ru_inblock - before.ru_inblock) * 512,
           (uint64_t)(after.ru_oublock - before.ru_oublock) * 512);
   fclose(csv);
   free(latencies);
   printf("verified %" PRIu64 " keys; %" PRIu64
          " optimize calls, %.3f us/call\n",
          keys,
          count,
          elapsed / 1e3 / count);
   return 0;
}
