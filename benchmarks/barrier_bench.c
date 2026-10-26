/* Common-source public API durability benchmark. Databases must be fresh,
 * disk-backed files. CSV completion latency is observed at notification_wait,
 * so depth > 1 includes the application's intervening insert work. */
#ifndef _GNU_SOURCE
#   define _GNU_SOURCE
#endif
#include <assert.h>
#include <fcntl.h>
#include <inttypes.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#include <time.h>
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"

static uint64_t
now_ns(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

static int
cmp_u64(const void *a, const void *b)
{
   uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
   return (x > y) - (x < y);
}

static void
checked(int rc, const char *what)
{
   if (rc) {
      fprintf(stderr, "%s failed: %d\n", what, rc);
      exit(1);
   }
}

static void
make_key(uint64_t id, char key[24])
{
   for (unsigned i = 0; i < 8; i++)
      key[i] = id >> (56 - 8 * i);
   memset(key + 8, 'k', 16);
}

typedef struct pending_barrier {
   splinterdb_notification note;
   uint64_t                started;
   uint64_t                index;
   bool                    active;
} pending_barrier;

static void
finish(splinterdb *db, pending_barrier *slot, uint64_t *completion)
{
   if (!slot->active)
      return;
   checked(splinterdb_notification_wait(db, &slot->note), "notification wait");
   completion[slot->index] = now_ns() - slot->started;
   int status              = -1;
   assert(splinterdb_notification_poll(&slot->note, &status) && status == 0);
   splinterdb_notification_deinit(&slot->note);
   slot->active = false;
}

typedef struct barrier_metrics {
   uint64_t elapsed;
   double   cpu;
   uint64_t read_bytes, write_bytes;
   double   launch_mean, completion_mean;
   uint64_t launch_p50, launch_p99, completion_p50, completion_p99;
} barrier_metrics;

static barrier_metrics
run_workload(splinterdb_config *cfg,
             bool               async_mode,
             uint64_t           batch,
             uint64_t           count,
             uint64_t           depth)
{
   splinterdb *db;
   checked(splinterdb_create(cfg, &db), "create");
   uint64_t        *launch     = calloc(count, sizeof(*launch));
   uint64_t        *completion = calloc(count, sizeof(*completion));
   pending_barrier *pending    = calloc(depth, sizeof(*pending));
   assert(launch && completion && pending);
   struct rusage before, after;
   getrusage(RUSAGE_SELF, &before);
   uint64_t start = now_ns();
   for (uint64_t b = 0; b < count; b++) {
      pending_barrier *slot = &pending[b % depth];
      finish(db, slot, completion);
      for (uint64_t i = 0; i < batch; i++) {
         uint64_t id = b * batch + i;
         char     key[24], value[100];
         make_key(id, key);
         memset(value, 'v', sizeof(value));
         memcpy(value, &id, sizeof(id));
         checked(splinterdb_insert(db,
                                   slice_create(sizeof(key), key),
                                   slice_create(sizeof(value), value),
                                   NULL),
                 "insert");
      }
      uint64_t t = now_ns();
      if (async_mode) {
#ifdef HAVE_ASYNC_BARRIER
         splinterdb_notification_init_polling(&slot->note, NULL);
         slot->started = t;
         slot->index   = b;
#   ifdef HAVE_SENTINEL_NOTIFICATION
         checked(splinterdb_durable_barrier(db, &slot->note), "launch");
#   else
         checked(splinterdb_durable_barrier_async(db, &slot->note), "launch");
#   endif
         slot->active = true;
#endif
      } else {
#ifdef HAVE_SENTINEL_NOTIFICATION
         checked(splinterdb_durable_barrier(db, NULL), "durable barrier");
#else
         checked(splinterdb_durable_barrier(db), "durable barrier");
#endif
      }
      launch[b] = now_ns() - t;
      if (!async_mode)
         completion[b] = launch[b];
   }
   for (uint64_t i = 0; i < depth; i++)
      finish(db, &pending[i], completion);
   uint64_t elapsed = now_ns() - start;
   getrusage(RUSAGE_SELF, &after);
   double launch_sum = 0, completion_sum = 0;
   for (uint64_t i = 0; i < count; i++) {
      launch_sum += launch[i];
      completion_sum += completion[i];
   }
   qsort(launch, count, sizeof(*launch), cmp_u64);
   qsort(completion, count, sizeof(*completion), cmp_u64);
   double cpu = (after.ru_utime.tv_sec - before.ru_utime.tv_sec)
                + (after.ru_stime.tv_sec - before.ru_stime.tv_sec)
                + ((after.ru_utime.tv_usec - before.ru_utime.tv_usec)
                   + (after.ru_stime.tv_usec - before.ru_stime.tv_usec))
                     / 1e6;
   barrier_metrics result = {
      .elapsed         = elapsed,
      .cpu             = cpu,
      .read_bytes      = (after.ru_inblock - before.ru_inblock) * 512,
      .write_bytes     = (after.ru_oublock - before.ru_oublock) * 512,
      .launch_mean     = launch_sum / count,
      .completion_mean = completion_sum / count,
      .launch_p50      = launch[count / 2],
      .launch_p99      = launch[(count - 1) * 99 / 100],
      .completion_p50  = completion[count / 2],
      .completion_p99  = completion[(count - 1) * 99 / 100],
   };
   free(launch);
   free(completion);
   free(pending);
   return result;
}

int
main(int argc, char **argv)
{
   if (argc != 9) {
      fprintf(stderr,
              "usage: %s DB CSV LABEL sync|async LOG WORKERS BATCH BARRIERS\n"
              "BARRIER_DEPTH optionally controls outstanding async barriers "
              "(default 1).\n",
              argv[0]);
      return 2;
   }
   bool async_mode = strcmp(argv[4], "async") == 0;
   if (!async_mode && strcmp(argv[4], "sync") != 0)
      return 2;
#ifndef HAVE_ASYNC_BARRIER
   if (async_mode) {
      fprintf(stderr, "async API unavailable\n");
      return 2;
   }
#endif
   uint64_t    logging   = strtoull(argv[5], NULL, 10);
   uint64_t    workers   = strtoull(argv[6], NULL, 10);
   uint64_t    batch     = strtoull(argv[7], NULL, 10);
   uint64_t    count     = strtoull(argv[8], NULL, 10);
   const char *depth_env = getenv("BARRIER_DEPTH");
   uint64_t depth = async_mode && depth_env ? strtoull(depth_env, NULL, 10) : 1;
   if (logging > 1 || workers > 1 || !batch || !count || !depth || depth > 1024
       || count > 1000000 || batch > UINT64_MAX / count)
      return 2;
   data_config dcfg;
   default_data_config_init(&dcfg);
   splinterdb_config cfg = {
      .filename                = argv[1],
      .cache_size              = 128 * 1024 * 1024,
      .disk_size               = (uint64_t)8 * 1024 * 1024 * 1024,
      .data_cfg                = &dcfg,
      .use_log                 = logging,
      .num_normal_bg_threads   = workers,
      .num_memtable_bg_threads = 0,
      .io_flags                = O_RDWR | O_CREAT | O_DIRECT,
   };
   int pipefd[2];
   assert(pipe(pipefd) == 0);
   pid_t worker = fork();
   assert(worker >= 0);
   if (worker == 0) {
      close(pipefd[0]);
      barrier_metrics measured =
         run_workload(&cfg, async_mode, batch, count, depth);
      assert(write(pipefd[1], &measured, sizeof(measured)) == sizeof(measured));
      /* Exit without close/checkpoint; the parent validates crash recovery. */
      _exit(0);
   }
   close(pipefd[1]);
   barrier_metrics measured;
   size_t          received = 0;
   while (received < sizeof(measured)) {
      ssize_t n = read(
         pipefd[0], (char *)&measured + received, sizeof(measured) - received);
      if (n < 0 && errno == EINTR)
         continue;
      assert(n > 0);
      received += n;
   }
   close(pipefd[0]);
   int child_status;
   assert(waitpid(worker, &child_status, 0) == worker);
   assert(WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0);
   /* The child never cleanly closed: only completed barriers made these
    * updates durable. Recovery and full validation are outside timing. */
   splinterdb *db;
   checked(splinterdb_open(&cfg, &db), "open");
   splinterdb_iterator *it;
   checked(splinterdb_iterator_init(db, &it), "iterator init");
   for (uint64_t id = 0; id < batch * count; id++) {
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
         "label,mode,log,workers,batch,barriers,depth,seconds,updates_s,cpu_s,"
         "launch_mean_us,launch_p50_us,launch_p99_us,observed_completion_mean_"
         "us,observed_completion_p50_us,observed_completion_p99_us,verified,"
         "read_bytes,write_bytes\n");
   fprintf(csv,
           "%s,%s,%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64
           ",%.6f,%.3f,%.6f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,1,%" PRIu64
           ",%" PRIu64 "\n",
           argv[3],
           argv[4],
           logging,
           workers,
           batch,
           count,
           depth,
           measured.elapsed / 1e9,
           batch * count * 1e9 / measured.elapsed,
           measured.cpu,
           measured.launch_mean / 1e3,
           measured.launch_p50 / 1e3,
           measured.launch_p99 / 1e3,
           measured.completion_mean / 1e3,
           measured.completion_p50 / 1e3,
           measured.completion_p99 / 1e3,
           measured.read_bytes,
           measured.write_bytes);
   fclose(csv);
   printf("verified %" PRIu64 " updates, %.3f updates/s\n",
          batch * count,
          batch * count * 1e9 / measured.elapsed);
   return 0;
}
