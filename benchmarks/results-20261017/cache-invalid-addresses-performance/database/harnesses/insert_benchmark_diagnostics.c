// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/* Concurrent random insertion and point queries, sampled independently. */
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <time.h>
#include <unistd.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#ifndef KVBENCH_NO_TRACE
#   include "platform_trace.h"
#endif
#include "platform_threads.h"

#define GIB             (UINT64_C(1) << 30)
#define MAX_WORKERS     64
#define MAX_RECORD_PART 1024
#define HIST_SUB_BITS   5
#define HIST_SUB        (1u << HIST_SUB_BITS)
#define HIST_BUCKETS    (64 * HIST_SUB)

typedef struct latency_histogram {
   uint64_t buckets[HIST_BUCKETS];
   uint64_t count;
   uint64_t sum_ns;
   uint64_t max_ns;
} latency_histogram;

typedef struct options {
   const char *filename;
   const char *csv;
   const char *summary;
   const char *trace;
   uint64_t    records, data_gib, cache_mib, disk_gib, seed, id_base;
   uint64_t    inserts, queries, memtable_bg, normal_bg, memtable_mib;
   uint64_t    key_size, value_size, report_ms, seconds, min_free_gib;
   uint64_t    queue_scale, barrier_ms, target_insert_ops_s;
   uint64_t    log_group_size_pages;
   bool        full_query_range, direct, use_log, open_existing;
} options;

typedef struct benchmark benchmark;
typedef struct worker {
   _Alignas(64) atomic_uint_fast64_t completed;
   atomic_uint_fast64_t hits, max_ns, response_max_ns, active_since_ns;
   atomic_bool          done;
   pthread_t            thread;
   benchmark           *bench;
   uint64_t             index;
   bool                 query;
   latency_histogram    service_latency;
   latency_histogram    response_latency;
   uint64_t             last_start_ns, last_end_ns;
   uint64_t             loop_exit_ns, deregister_start_ns, deregister_end_ns;
} worker;

struct benchmark {
   options              opt;
   splinterdb          *db;
   atomic_bool          start, stop;
   uint64_t             start_ns;
   atomic_uint_fast64_t arrival_sequence;
   atomic_int           error;
   worker               workers[MAX_WORKERS];
   pthread_t            barrier_thread;
   atomic_bool          barrier_started, barrier_done;
   atomic_uint_fast64_t barrier_count, barrier_sum_ns, barrier_max_ns;
};

static volatile sig_atomic_t interrupted;

static void
handle_signal(int signo)
{
   interrupted = signo;
}

static uint64_t
now_ns(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

/* Boundary-only diagnostics. The original driver is kept separately. Reuse
 * its existing per-operation timestamps; no new clock calls enter that loop.
 * Print these snapshots only after close so diagnostic output is not timed.
 */
typedef struct diagnostic_snapshot {
   uint64_t wall_ns;
   uint64_t cpu_ns;
} diagnostic_snapshot;

static diagnostic_snapshot
take_diagnostic_snapshot(void)
{
   struct timespec cpu;
   uint64_t wall_ns = now_ns();
   if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu) != 0) {
      perror("CLOCK_PROCESS_CPUTIME_ID");
      exit(1);
   }
   return (diagnostic_snapshot){.wall_ns = wall_ns,
                                .cpu_ns = (uint64_t)cpu.tv_sec * 1000000000
                                          + cpu.tv_nsec};
}

static void
print_diagnostic_snapshot(const char *stage, diagnostic_snapshot snapshot)
{
   fprintf(stderr,
           "diagnostic_stage name=%s wall_ns=%lu cpu_ns=%lu\n",
           stage,
           snapshot.wall_ns,
           snapshot.cpu_ns);
}

/* A bijection on 64-bit integers, so the random insertion keys are unique. */
static uint64_t
mix(uint64_t x)
{
   x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
   x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
   return x ^ (x >> 31);
}

static uint64_t
next_random(uint64_t *state)
{
   *state += UINT64_C(0x9e3779b97f4a7c15);
   return mix(*state);
}

static void
make_key(const options *opt, uint64_t id, char *key)
{
   uint64_t x = mix(id + opt->seed);
   for (uint64_t i = 0; i < opt->key_size; i++) {
      key[i] = (char)(x >> (56 - 8 * (i % 8)));
   }
}

static void
record_max(atomic_uint_fast64_t *slot, uint64_t ns)
{
   uint64_t old = atomic_load_explicit(slot, memory_order_relaxed);
   while (old < ns
          && !atomic_compare_exchange_weak_explicit(
             slot, &old, ns, memory_order_relaxed, memory_order_relaxed))
   {
   }
}

static void
record_latency(worker *w, uint64_t ns)
{
   record_max(&w->max_ns, ns);
}

static unsigned
histogram_index(uint64_t value)
{
   if (value < HIST_SUB) {
      return (unsigned)value;
   }
   unsigned exponent = 63 - __builtin_clzll(value);
   unsigned sub =
      (unsigned)((value >> (exponent - HIST_SUB_BITS)) & (HIST_SUB - 1));
   return (exponent - HIST_SUB_BITS + 1) * HIST_SUB + sub;
}

static uint64_t
histogram_value(unsigned index)
{
   if (index < HIST_SUB) {
      return index;
   }
   unsigned exponent = index / HIST_SUB + HIST_SUB_BITS - 1;
   unsigned sub      = index % HIST_SUB;
   uint64_t low =
      (UINT64_C(1) << exponent) | ((uint64_t)sub << (exponent - HIST_SUB_BITS));
   uint64_t width = UINT64_C(1) << (exponent - HIST_SUB_BITS);
   return low + width / 2;
}

static void
histogram_add(latency_histogram *histogram, uint64_t value)
{
   histogram->buckets[histogram_index(value)]++;
   histogram->count++;
   histogram->sum_ns += value;
   histogram->max_ns = MAX(histogram->max_ns, value);
}

static void
histogram_merge(latency_histogram *result, const latency_histogram *source)
{
   for (unsigned i = 0; i < HIST_BUCKETS; i++) {
      result->buckets[i] += source->buckets[i];
   }
   result->count += source->count;
   result->sum_ns += source->sum_ns;
   result->max_ns = MAX(result->max_ns, source->max_ns);
}

static double
histogram_percentile_ms(const latency_histogram *histogram, double percentile)
{
   if (histogram->count == 0) {
      return 0;
   }
   uint64_t target = (uint64_t)(percentile * histogram->count);
   if (target == 0) {
      target = 1;
   }
   uint64_t cumulative = 0;
   for (unsigned i = 0; i < HIST_BUCKETS; i++) {
      cumulative += histogram->buckets[i];
      if (cumulative >= target) {
         return histogram_value(i) / 1e6;
      }
   }
   return histogram->max_ns / 1e6;
}

static void
wait_until(benchmark *b, uint64_t deadline)
{
   while (!atomic_load_explicit(&b->stop, memory_order_relaxed)) {
      uint64_t now = now_ns();
      if (now >= deadline) {
         return;
      }
      uint64_t remaining = deadline - now;
      if (remaining > UINT64_C(10000000)) {
         remaining = UINT64_C(10000000);
      }
      struct timespec delay = {.tv_sec  = remaining / UINT64_C(1000000000),
                               .tv_nsec = remaining % UINT64_C(1000000000)};
      nanosleep(&delay, NULL);
   }
}

static void *
run_worker(void *arg)
{
   worker        *w = arg;
   benchmark     *b = w->bench;
   const options *o = &b->opt;
   char           key[MAX_RECORD_PART], value[MAX_RECORD_PART];
   memset(value, 'v', o->value_size);
   int rc = platform_register_thread();
   if (rc) {
      atomic_store(&b->error, rc);
      atomic_store(&b->stop, true);
      atomic_store(&w->done, true);
      return NULL;
   }
   splinterdb_lookup_result result;
   fprintf(stderr,
           "worker=%lu kind=%s tid=%lu os_tid=%d\n",
           w->index,
           w->query ? "query" : "insert",
           platform_get_tid(),
           gettid());
   if (w->query) {
      splinterdb_lookup_result_init(
         b->db, &result, SPLINTERDB_LOOKUP_VALUE, sizeof(value), value);
   }
   while (!atomic_load_explicit(&b->start, memory_order_acquire)) {
      struct timespec delay = {.tv_nsec = 1000000};
      nanosleep(&delay, NULL);
   }
   uint64_t completed = 0, hits = 0;
   uint64_t random = o->seed + w->index + 1;
   uint64_t start = 0, end = 0;
   while (!atomic_load_explicit(&b->stop, memory_order_relaxed)) {
      uint64_t scheduled = 0;
      uint64_t arrival   = UINT64_MAX;
      if (!w->query && o->target_insert_ops_s != 0) {
         arrival = atomic_fetch_add_explicit(
            &b->arrival_sequence, 1, memory_order_relaxed);
         if (arrival >= o->records) {
            break;
         }
         scheduled = b->start_ns
                     + (uint64_t)((__uint128_t)arrival * UINT64_C(1000000000)
                                  / o->target_insert_ops_s);
         wait_until(b, scheduled);
         if (atomic_load_explicit(&b->stop, memory_order_relaxed)) {
            break;
         }
      }
      uint64_t id;
      if (!w->query) {
         uint64_t offset = o->target_insert_ops_s != 0
                              ? arrival
                              : completed * o->inserts + w->index;
         if (offset >= o->records) {
            break;
         }
         id = o->id_base + offset;
      } else if (o->full_query_range) {
         id = o->id_base + next_random(&random) % o->records;
      } else {
         uint64_t source    = next_random(&random) % o->inserts;
         uint64_t available = atomic_load_explicit(
            &b->workers[source].completed, memory_order_acquire);
         if (!available) {
            struct timespec delay = {.tv_nsec = 100000};
            nanosleep(&delay, NULL);
            continue;
         }
         id = o->id_base + (next_random(&random) % available) * o->inserts
              + source;
      }
      make_key(o, id, key);
      start = now_ns();
      atomic_store_explicit(&w->active_since_ns, start, memory_order_relaxed);
      if (w->query) {
         rc = splinterdb_lookup(b->db, slice_create(o->key_size, key), &result);
         if (!rc && splinterdb_lookup_found(&result)) {
            hits++;
         }
      } else {
         rc = splinterdb_insert(b->db,
                                slice_create(o->key_size, key),
                                slice_create(o->value_size, value),
                                NULL);
      }
      end = now_ns();
      record_latency(w, end - start);
      histogram_add(&w->service_latency, end - start);
      if (!w->query) {
         uint64_t response = scheduled == 0 ? end - start : end - scheduled;
         histogram_add(&w->response_latency, response);
         record_max(&w->response_max_ns, response);
      }
      atomic_store_explicit(&w->active_since_ns, 0, memory_order_relaxed);
      if (rc) {
         fprintf(stderr, "worker %lu: operation failed: %d\n", w->index, rc);
         atomic_store(&b->error, rc);
         atomic_store(&b->stop, true);
         break;
      }
      completed++;
      atomic_store_explicit(&w->hits, hits, memory_order_relaxed);
      atomic_store_explicit(&w->completed, completed, memory_order_release);
   }
   w->last_start_ns = start;
   w->last_end_ns   = end;
   w->loop_exit_ns  = now_ns();
   if (w->query) {
      splinterdb_lookup_result_deinit(&result);
   }
   w->deregister_start_ns = now_ns();
   platform_deregister_thread();
   w->deregister_end_ns = now_ns();
   atomic_store(&w->done, true);
   return NULL;
}

/*
 * Dedicated thread that calls splinterdb_durable_barrier() on a fixed period,
 * independent of insert/query progress. If a barrier call takes longer than
 * the period, the next call follows immediately (no attempt to catch up on
 * skipped slots).
 */
static void *
run_barrier(void *arg)
{
   benchmark     *b  = arg;
   const options *o  = &b->opt;
   int            rc = platform_register_thread();
   if (rc) {
      atomic_store(&b->error, rc);
      atomic_store(&b->stop, true);
      atomic_store(&b->barrier_done, true);
      return NULL;
   }
   while (!atomic_load_explicit(&b->start, memory_order_acquire)) {
      struct timespec delay = {.tv_nsec = 1000000};
      nanosleep(&delay, NULL);
   }
   uint64_t period_ns = o->barrier_ms * UINT64_C(1000000);
   uint64_t next      = now_ns() + period_ns;
   while (!atomic_load_explicit(&b->stop, memory_order_relaxed)) {
      uint64_t now = now_ns();
      if (now < next) {
         uint64_t        remaining = next - now;
         struct timespec delay     = {.tv_sec  = remaining / 1000000000,
                                      .tv_nsec = remaining % 1000000000};
         nanosleep(&delay, NULL);
         continue;
      }
      uint64_t start = now_ns();
#ifdef KVBENCH_NO_DURABLE_BARRIER
      rc = ENOTSUP;
#else
      rc = splinterdb_durable_barrier(b->db, SPLINTERDB_NOTIFICATION_BLOCKING);
#endif
      uint64_t elapsed = now_ns() - start;
      if (rc) {
         fprintf(stderr, "barrier: durable_barrier failed: %d\n", rc);
         atomic_store(&b->error, rc);
         atomic_store(&b->stop, true);
         break;
      }
      atomic_fetch_add_explicit(&b->barrier_count, 1, memory_order_relaxed);
      atomic_fetch_add_explicit(
         &b->barrier_sum_ns, elapsed, memory_order_relaxed);
      record_max(&b->barrier_max_ns, elapsed);
      next += period_ns;
   }
   platform_deregister_thread();
   atomic_store(&b->barrier_done, true);
   return NULL;
}

static void
usage(const char *name)
{
   fprintf(
      stderr,
      "Usage: %s --db NEW_IMAGE --csv NEW_CSV [options]\n"
      "  --open             open an existing image instead of creating one\n"
      "  --id-base N        first logical id inserted (default 0)\n"
      "  --data-gib N       logical key+value GiB (default 60)\n"
      "  --records N        override logical data target\n"
      "  --cache-mib N      default 3072\n"
      "  --disk-gib N       image capacity (default 512; grows lazily)\n"
      "  --inserters N      default 8\n"
      "  --readers N        default 8\n"
      "  --memtable-bg N    default 1\n"
      "  --normal-bg N      default 4\n"
      "  --memtable-mib N   default 24\n"
      "  --key-size N       default 16, minimum 8\n"
      "  --value-size N     default 16\n"
      "  --query-range inserted|all (default inserted)\n"
      "  --queue-scale N    foreground task threshold (default 100)\n"
      "  --report-ms N      default 1000\n"
      "  --seconds N        optional time limit (0 = no limit)\n"
      "  --target-insert-ops-s N  aggregate open-loop arrival rate; "
      "requires --seconds (default 0 = closed loop)\n"
      "  --summary NEW_CSV  write aggregate latency percentiles\n"
      "  --min-free-gib N   stop before filesystem fills (default 8)\n"
      "  --seed N           default 1\n"
      "  --barrier-ms N     call splinterdb_durable_barrier from a "
      "dedicated thread every N ms (default 0 = disabled)\n"
      "  --log-group-size-pages N  automatic durability threshold "
      "(0 = 1024 pages, UINT64_MAX = disabled)\n"
      "  --trace NEW_CSV    trace internal tasks and waits (optional)\n"
      "  --direct          use O_DIRECT (default buffered I/O)\n"
      "  --no-log          disable WAL (default enabled)\n"
      "Automatic checkpoints are disabled. Existing files are refused.\n"
      "Load measurements exclude create, final drain and close.\n",
      name);
}

static bool
parse_number(const char *s, uint64_t *out)
{
   char *end;
   errno                    = 0;
   unsigned long long value = strtoull(s, &end, 10);
   if (errno || s == end || *end || *s == '-') {
      return false;
   }
   *out = value;
   return true;
}

static bool
parse_options(int argc, char **argv, options *o)
{
   *o = (options){.data_gib     = 60,
                  .cache_mib    = 3072,
                  .disk_gib     = 512,
                  .seed         = 1,
                  .inserts      = 8,
                  .queries      = 8,
                  .memtable_bg  = 1,
                  .normal_bg    = 4,
                  .memtable_mib = 24,
                  .key_size     = 16,
                  .value_size   = 16,
                  .report_ms    = 1000,
                  .min_free_gib = 8,
                  .queue_scale  = 100,
                  .use_log      = true};
   for (int i = 1; i < argc; i++) {
      if (!strcmp(argv[i], "--direct")) {
         o->direct = true;
         continue;
      }
      if (!strcmp(argv[i], "--no-log")) {
         o->use_log = false;
         continue;
      }
      if (!strcmp(argv[i], "--open")) {
         o->open_existing = true;
         continue;
      }
      if (i + 1 >= argc) {
         return false;
      }
      const char *name = argv[i++], *value = argv[i];
      if (!strcmp(name, "--db")) {
         o->filename = value;
         continue;
      }
      if (!strcmp(name, "--csv")) {
         o->csv = value;
         continue;
      }
      if (!strcmp(name, "--summary")) {
         o->summary = value;
         continue;
      }
      if (!strcmp(name, "--trace")) {
         o->trace = value;
         continue;
      }
      if (!strcmp(name, "--query-range")) {
         if (strcmp(value, "inserted") && strcmp(value, "all")) {
            return false;
         }
         o->full_query_range = !strcmp(value, "all");
         continue;
      }
      struct {
         const char *name;
         uint64_t   *value;
      } numbers[] = {
         {"--data-gib", &o->data_gib},
         {"--records", &o->records},
         {"--cache-mib", &o->cache_mib},
         {"--disk-gib", &o->disk_gib},
         {"--inserters", &o->inserts},
         {"--readers", &o->queries},
         {"--memtable-bg", &o->memtable_bg},
         {"--normal-bg", &o->normal_bg},
         {"--memtable-mib", &o->memtable_mib},
         {"--key-size", &o->key_size},
         {"--value-size", &o->value_size},
         {"--report-ms", &o->report_ms},
         {"--seconds", &o->seconds},
         {"--min-free-gib", &o->min_free_gib},
         {"--seed", &o->seed},
         {"--id-base", &o->id_base},
         {"--queue-scale", &o->queue_scale},
         {"--barrier-ms", &o->barrier_ms},
         {"--log-group-size-pages", &o->log_group_size_pages},
         {"--target-insert-ops-s", &o->target_insert_ops_s},
      };
      bool found = false;
      for (size_t j = 0; j < sizeof(numbers) / sizeof(numbers[0]); j++) {
         if (!strcmp(name, numbers[j].name)) {
            if (!parse_number(value, numbers[j].value)) {
               return false;
            }
            found = true;
            break;
         }
      }
      if (!found) {
         return false;
      }
   }
   if (!o->filename || !o->csv || !o->inserts || o->inserts > MAX_WORKERS
       || o->queries > MAX_WORKERS - o->inserts || o->key_size < 8
       || o->key_size > MAX_RECORD_PART || o->value_size > MAX_RECORD_PART
       || !o->report_ms || o->report_ms > 60000 || !o->cache_mib
       || o->cache_mib > UINT64_MAX / (1 << 20) || !o->disk_gib
       || o->disk_gib > INT64_MAX / GIB || o->data_gib > UINT64_MAX / GIB
       || o->min_free_gib > UINT64_MAX / GIB || !o->memtable_mib
       || o->memtable_mib > UINT64_MAX / (1 << 20) || o->memtable_bg > 32
       || o->normal_bg > 32
       || o->inserts + o->queries + o->memtable_bg + o->normal_bg + 1
             > MAX_WORKERS
       || o->seconds > UINT64_MAX / 1000000000
       || o->barrier_ms > UINT64_MAX / 1000000
       || (o->target_insert_ops_s != 0 && (o->seconds == 0 || o->queries != 0)))
   {
      return false;
   }
#ifdef KVBENCH_NO_DURABLE_BARRIER
   if (o->barrier_ms != 0) {
      return false;
   }
#endif
#ifdef KVBENCH_NO_TRACE
   if (o->trace != NULL) {
      return false;
   }
#endif
   if (!o->records) {
      o->records = o->data_gib * GIB / (o->key_size + o->value_size);
   }
   return o->records > 0 && o->records <= UINT64_MAX / MAX_WORKERS
          && o->records <= UINT64_MAX / (o->key_size + o->value_size)
          && o->id_base <= UINT64_MAX - o->records;
}

static void
process_io(uint64_t *reads, uint64_t *writes)
{
   *reads = *writes = 0;
   FILE *file       = fopen("/proc/self/io", "r");
   if (!file) {
      return;
   }
   char     name[64];
   uint64_t value;
   while (fscanf(file, "%63s %" SCNu64, name, &value) == 2) {
      if (!strcmp(name, "read_bytes:")) {
         *reads = value;
      }
      if (!strcmp(name, "write_bytes:")) {
         *writes = value;
      }
   }
   fclose(file);
}

static int
write_summary(const benchmark *b, uint64_t inserted, double elapsed_s)
{
   const options *o = &b->opt;
   if (o->summary == NULL) {
      return 0;
   }

   latency_histogram insert_service = {0}, insert_response = {0};
   latency_histogram query_service = {0};
   for (uint64_t i = 0; i < o->inserts + o->queries; i++) {
      if (b->workers[i].query) {
         histogram_merge(&query_service, &b->workers[i].service_latency);
      } else {
         histogram_merge(&insert_service, &b->workers[i].service_latency);
         histogram_merge(&insert_response, &b->workers[i].response_latency);
      }
   }

   FILE *summary = fopen(o->summary, "wx");
   if (summary == NULL) {
      return errno;
   }
   fprintf(summary,
           "target_insert_ops_s,achieved_insert_ops_s,elapsed_s,inserted,"
           "insert_service_mean_ms,insert_service_p50_ms,insert_service_p99_ms,"
           "insert_service_p999_ms,insert_service_max_ms,"
           "insert_response_mean_ms,insert_response_p50_ms,"
           "insert_response_p99_ms,insert_response_p999_ms,"
           "insert_response_max_ms,query_service_mean_ms,query_service_p99_ms,"
           "query_service_max_ms\n");
   fprintf(
      summary,
      "%lu,%.6f,%.6f,%lu,%.6f,%.6f,%.6f,%.6f,%.6f,"
      "%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f\n",
      o->target_insert_ops_s,
      elapsed_s == 0 ? 0 : inserted / elapsed_s,
      elapsed_s,
      inserted,
      insert_service.count ? insert_service.sum_ns / 1e6 / insert_service.count
                           : 0,
      histogram_percentile_ms(&insert_service, 0.50),
      histogram_percentile_ms(&insert_service, 0.99),
      histogram_percentile_ms(&insert_service, 0.999),
      insert_service.max_ns / 1e6,
      insert_response.count
         ? insert_response.sum_ns / 1e6 / insert_response.count
         : 0,
      histogram_percentile_ms(&insert_response, 0.50),
      histogram_percentile_ms(&insert_response, 0.99),
      histogram_percentile_ms(&insert_response, 0.999),
      insert_response.max_ns / 1e6,
      query_service.count ? query_service.sum_ns / 1e6 / query_service.count
                          : 0,
      histogram_percentile_ms(&query_service, 0.99),
      query_service.max_ns / 1e6);
   int rc = ferror(summary) ? EIO : 0;
   if (fclose(summary) != 0 && rc == 0) {
      rc = errno;
   }
   return rc;
}

int
main(int argc, char **argv)
{
   benchmark b = {0};
   if (argc == 2 && !strcmp(argv[1], "--help")) {
      usage(argv[0]);
      return 0;
   }
   if (!parse_options(argc, argv, &b.opt)) {
      usage(argv[0]);
      return 2;
   }
   const options *o = &b.opt;
   if (o->trace) {
#ifdef KVBENCH_NO_TRACE
      return 2;
#else
      int trace_rc = platform_trace_open(o->trace);
      if (trace_rc) {
         fprintf(stderr, "trace open failed: %s\n", strerror(trace_rc));
         return 1;
      }
#endif
   }
   FILE *csv = fopen(o->csv, "wx");
   if (!csv) {
      perror(o->csv);
      return 1;
   }
   int fd = open(o->filename,
                 O_RDWR | O_CLOEXEC | (o->open_existing ? 0 : O_CREAT | O_EXCL),
                 0600);
   if (fd < 0) {
      perror(o->filename);
      fclose(csv);
      return 1;
   }
   struct statvfs space;
   if (fstatvfs(fd, &space) != 0
       || (uint64_t)space.f_bavail * space.f_frsize < o->min_free_gib * GIB)
   {
      fprintf(stderr, "Insufficient free space\n");
      close(fd);
      fclose(csv);
      return 1;
   }

   data_config data_cfg;
   default_data_config_init(&data_cfg);
   splinterdb_config cfg = {.filename   = o->filename,
                            .disk_size  = o->disk_gib * GIB,
                            .cache_size = o->cache_mib * (1 << 20),
                            .data_cfg   = &data_cfg,
                            .io_flags   = O_RDWR | (o->direct ? O_DIRECT : 0),
                            .num_memtable_bg_threads = o->memtable_bg,
                            .num_normal_bg_threads   = o->normal_bg,
                            .memtable_capacity   = o->memtable_mib * (1 << 20),
                            .queue_scale_percent = o->queue_scale,
                            .use_log             = o->use_log,
                            .checkpoint_log_size_bytes = UINT64_MAX,
                            .use_stats                 = true};
#ifndef KVBENCH_NO_LOG_GROUP_LIMIT
   cfg.log_group_size_pages = o->log_group_size_pages;
#else
   if (o->log_group_size_pages != 0) {
      fprintf(stderr, "This build has no automatic log-group limit\n");
      return 1;
   }
#endif
   platform_set_log_streams(stderr, stderr);
   fprintf(
      stderr,
      "version=%s records=%lu key=%lu value=%lu cache_mib=%lu "
      "disk_gib=%lu inserters=%lu readers=%lu memtable_bg=%lu normal_bg=%lu "
      "memtable_mib=%lu queue_scale=%lu log=%d checkpoints=disabled "
      "direct=%d query_range=%s seed=%lu report_ms=%lu barrier_ms=%lu "
      "target_insert_ops_s=%lu id_base=%lu mode=%s\n",
      splinterdb_get_version(),
      o->records,
      o->key_size,
      o->value_size,
      o->cache_mib,
      o->disk_gib,
      o->inserts,
      o->queries,
      o->memtable_bg,
      o->normal_bg,
      o->memtable_mib,
      o->queue_scale,
      o->use_log,
      o->direct,
      o->full_query_range ? "all" : "inserted",
      o->seed,
      o->report_ms,
      o->barrier_ms,
      o->target_insert_ops_s,
      o->id_base,
      o->open_existing ? "open" : "create");
   int rc = o->open_existing ? splinterdb_open(&cfg, &b.db)
                             : splinterdb_create(&cfg, &b.db);
   if (rc) {
      fprintf(
         stderr, "%s failed: %d\n", o->open_existing ? "open" : "create", rc);
      close(fd);
      fclose(csv);
      return 1;
   }

   signal(SIGINT, handle_signal);
   signal(SIGTERM, handle_signal);
   uint64_t started = 0;
   for (uint64_t i = 0; i < o->inserts + o->queries; i++) {
      worker *w = &b.workers[i];
      w->bench  = &b;
      w->index  = i;
      w->query  = i >= o->inserts;
      rc        = pthread_create(&w->thread, NULL, run_worker, w);
      if (rc) {
         atomic_store(&b.error, rc);
         atomic_store(&b.stop, true);
         break;
      }
      started++;
   }
   if (o->barrier_ms && !atomic_load(&b.stop)) {
      rc = pthread_create(&b.barrier_thread, NULL, run_barrier, &b);
      if (rc) {
         atomic_store(&b.error, rc);
         atomic_store(&b.stop, true);
      } else {
         atomic_store(&b.barrier_started, true);
      }
   }
   fprintf(
      csv,
      "elapsed_s,interval_s,inserted,logical_gib,insert_ops_s,query_ops_s,"
      "hit_pct,insert_max_ms,query_max_ms,active_max_ms,cpu_cores,max_rss_mib,"
      "read_mib_s,write_mib_s,image_gib,free_gib,inserters_running,"
      "barrier_ops_s,barrier_avg_ms,barrier_max_ms,target_insert_ops_s,"
      "offered_insert_ops_s,insert_backlog,response_max_ms");
   for (uint64_t i = 0; i < started; i++) {
      fprintf(csv, ",%s%lu_ops_s", b.workers[i].query ? "q" : "i", i);
   }
   fputc('\n', csv);
   fflush(csv);
   uint64_t prev_completed[MAX_WORKERS] = {0}, prev_hits = 0;
   uint64_t prev_barrier_count = 0, prev_barrier_sum_ns = 0;
   uint64_t prev_read, prev_write;
   process_io(&prev_read, &prev_write);
   struct rusage usage_now;
   getrusage(RUSAGE_SELF, &usage_now);
   double prev_cpu =
      usage_now.ru_utime.tv_sec + usage_now.ru_utime.tv_usec / 1e6
      + usage_now.ru_stime.tv_sec + usage_now.ru_stime.tv_usec / 1e6;
   uint64_t start = now_ns(), previous = start;
   b.start_ns = start;
   fprintf(
      stderr, "start_monotonic_ns=%lu main_address=%p\n", start, (void *)main);
   diagnostic_snapshot diagnostic_start = take_diagnostic_snapshot();
   atomic_store_explicit(&b.start, true, memory_order_release);
   const char *reason   = "complete";
   uint64_t    inserted = 0;
   while (true) {
      struct timespec delay = {.tv_sec  = o->report_ms / 1000,
                               .tv_nsec = (o->report_ms % 1000) * 1000000};
      nanosleep(&delay, NULL);
      uint64_t now = now_ns(), delta_insert = 0, delta_query = 0, hits = 0;
      uint64_t insert_max = 0, query_max = 0, response_max = 0, active_max = 0;
      uint64_t inserters_running  = 0;
      double   rates[MAX_WORKERS] = {0};
      double   elapsed = (now - start) / 1e9, interval = (now - previous) / 1e9;
      bool     all_inserts_done = true;
      inserted                  = 0;
      for (uint64_t i = 0; i < started; i++) {
         worker  *w         = &b.workers[i];
         uint64_t completed = atomic_load(&w->completed);
         uint64_t count     = completed - prev_completed[i];
         prev_completed[i]  = completed;
         rates[i]           = count / interval;
         uint64_t latency   = atomic_exchange(&w->max_ns, 0);
         uint64_t active    = atomic_load(&w->active_since_ns);
         if (active && active < now && active_max < now - active) {
            active_max = now - active;
         }
         if (w->query) {
            delta_query += count;
            hits += atomic_load(&w->hits);
            if (query_max < latency) {
               query_max = latency;
            }
         } else {
            inserted += completed;
            delta_insert += count;
            if (insert_max < latency) {
               insert_max = latency;
            }
            uint64_t response = atomic_exchange(&w->response_max_ns, 0);
            if (response_max < response) {
               response_max = response;
            }
            if (!atomic_load(&w->done)) {
               inserters_running++;
               all_inserts_done = false;
            }
         }
      }
      uint64_t reads, writes;
      process_io(&reads, &writes);
      getrusage(RUSAGE_SELF, &usage_now);
      double cpu = usage_now.ru_utime.tv_sec + usage_now.ru_utime.tv_usec / 1e6
                   + usage_now.ru_stime.tv_sec
                   + usage_now.ru_stime.tv_usec / 1e6;
      struct stat st = {0};
      if (fstat(fd, &st) != 0 || fstatvfs(fd, &space) != 0) {
         atomic_store(&b.error, errno);
         atomic_store(&b.stop, true);
      }
      double   free_gib       = (double)space.f_bavail * space.f_frsize / GIB;
      uint64_t barrier_count  = atomic_load(&b.barrier_count);
      uint64_t barrier_sum_ns = atomic_load(&b.barrier_sum_ns);
      uint64_t barrier_max_ns = atomic_exchange(&b.barrier_max_ns, 0);
      uint64_t delta_barrier_count = barrier_count - prev_barrier_count;
      uint64_t delta_barrier_sum   = barrier_sum_ns - prev_barrier_sum_ns;
      uint64_t offered             = inserted;
      if (o->target_insert_ops_s != 0) {
         __uint128_t possible = (__uint128_t)(now - start)
                                * o->target_insert_ops_s / UINT64_C(1000000000);
         offered = possible > o->records ? o->records : (uint64_t)possible;
      }
      fprintf(csv,
              "%.6f,%.6f,%lu,%.6f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,"
              "%.3f,%.6f,%.3f,%lu,%.3f,%.3f,%.3f,%lu,%.3f,%lu,%.3f",
              elapsed,
              interval,
              inserted,
              (double)inserted * (o->key_size + o->value_size) / GIB,
              delta_insert / interval,
              delta_query / interval,
              delta_query ? (hits - prev_hits) * 100.0 / delta_query : 0,
              insert_max / 1e6,
              query_max / 1e6,
              active_max / 1e6,
              (cpu - prev_cpu) / interval,
              usage_now.ru_maxrss / 1024.0,
              (reads - prev_read) / (1048576.0 * interval),
              (writes - prev_write) / (1048576.0 * interval),
              (double)st.st_size / GIB,
              free_gib,
              inserters_running,
              delta_barrier_count / interval,
              delta_barrier_count
                 ? delta_barrier_sum / 1e6 / delta_barrier_count
                 : 0,
              barrier_max_ns / 1e6,
              o->target_insert_ops_s,
              o->target_insert_ops_s ? (double)o->target_insert_ops_s : 0,
              offered > inserted ? offered - inserted : 0,
              response_max / 1e6);
      for (uint64_t i = 0; i < started; i++) {
         fprintf(csv, ",%.3f", rates[i]);
      }
      fputc('\n', csv);
      if (fflush(csv) != 0) {
         atomic_store(&b.error, errno);
         atomic_store(&b.stop, true);
      }
      prev_hits           = hits;
      prev_read           = reads;
      prev_write          = writes;
      prev_cpu            = cpu;
      prev_barrier_count  = barrier_count;
      prev_barrier_sum_ns = barrier_sum_ns;
      previous            = now;
      if (all_inserts_done || atomic_load(&b.stop)) {
         break;
      }
      if (interrupted) {
         reason = "signal";
         break;
      }
      if (o->seconds && elapsed >= o->seconds) {
         reason = "time_limit";
         break;
      }
      if (free_gib < o->min_free_gib) {
         reason = "free_space_limit";
         break;
      }
   }
   diagnostic_snapshot diagnostic_stop = take_diagnostic_snapshot();
   atomic_store(&b.stop, true);
   for (uint64_t i = 0; i < started; i++) {
      pthread_join(b.workers[i].thread, NULL);
   }
   diagnostic_snapshot diagnostic_joined = take_diagnostic_snapshot();
   if (atomic_load(&b.barrier_started)) {
      pthread_join(b.barrier_thread, NULL);
   }
   inserted = 0;
   for (uint64_t i = 0; i < o->inserts; i++) {
      inserted += atomic_load(&b.workers[i].completed);
   }
   double elapsed_s = (now_ns() - start) / 1e9;
   fprintf(stderr,
           "load_end reason=%s inserted=%lu elapsed_s=%.3f error=%d "
           "barrier_count=%lu barrier_avg_ms=%.3f barrier_max_ms=%.3f\n",
           reason,
           inserted,
           elapsed_s,
           atomic_load(&b.error),
           atomic_load(&b.barrier_count),
           atomic_load(&b.barrier_count) ? atomic_load(&b.barrier_sum_ns) / 1e6
                                              / atomic_load(&b.barrier_count)
                                         : 0,
           atomic_load(&b.barrier_max_ns) / 1e6);
   int summary_rc = write_summary(&b, inserted, elapsed_s);
   if (summary_rc != 0) {
      fprintf(stderr, "summary write failed: %s\n", strerror(summary_rc));
   }
   uint64_t close_start = now_ns();
   diagnostic_snapshot diagnostic_before_close = take_diagnostic_snapshot();
#ifdef KVBENCH_LEGACY_CLOSE
   splinterdb_close(&b.db);
   rc = 0;
#else
   rc = splinterdb_close(&b.db, false);
#endif
   diagnostic_snapshot diagnostic_after_close = take_diagnostic_snapshot();
   fprintf(
      stderr, "close_s=%.3f close_rc=%d\n", (now_ns() - close_start) / 1e9, rc);
   print_diagnostic_snapshot("worker_start", diagnostic_start);
   print_diagnostic_snapshot("stop_request", diagnostic_stop);
   print_diagnostic_snapshot("workers_joined", diagnostic_joined);
   print_diagnostic_snapshot("before_close", diagnostic_before_close);
   print_diagnostic_snapshot("after_close", diagnostic_after_close);
   for (uint64_t i = 0; i < started; i++) {
      worker *w = &b.workers[i];
      fprintf(stderr,
              "diagnostic_worker index=%lu completed=%lu last_start_ns=%lu "
              "last_end_ns=%lu loop_exit_ns=%lu deregister_start_ns=%lu "
              "deregister_end_ns=%lu\n",
              i,
              atomic_load(&w->completed),
              w->last_start_ns,
              w->last_end_ns,
              w->loop_exit_ns,
              w->deregister_start_ns,
              w->deregister_end_ns);
   }
   close(fd);
   fclose(csv);
   int trace_rc = 0;
#ifndef KVBENCH_NO_TRACE
   trace_rc = platform_trace_close();
#endif
   bool incomplete = !o->seconds && inserted < o->records;
   return rc || trace_rc || summary_rc || atomic_load(&b.error) || incomplete
             ? 1
             : 0;
}
