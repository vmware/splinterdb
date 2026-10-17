// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/* Resident mutable clockcache probe, derived from controlled-final plus the
 * setup-only exact working-page override from controlled-hotset. Run only
 * under the campaign machine lock on real disk. All pages are preloaded;
 * cache_lock marks them dirty. There is no timed allocation or writeback.
 * Usage: FILE CACHE_MIB THREADS SECONDS [disjoint|shared] write
 * CACHE_PROBE_WORKING_PAGES selects an exact set (>=32, <=75% cache pages).
 * Each process runs exactly one phase, so verification includes every write
 * from warmup through stopping, without resetting live page counters.
 */
#include <errno.h>
#include <inttypes.h>
#include <linux/perf_event.h>
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include "clockcache.h"
#include "rc_allocator.h"
#include "platform_threads.h"
#include "platform_sleep.h"

#define PROBE_MAGIC UINT64_C(0xb79d2384c651ea0f)
/* Private FREE/LOADING bits, unchanged across the compared builds. A writer
 * may set WRITELOCKED while waiting for our read reference, so that bit alone
 * is not an invalid state for a page whose reference we already hold. */
#define PROBE_UNREADABLE_STATUS ((1u << 0) | (1u << 4))

typedef struct {
   uint64 identity;
   uint64 counter;
   uint64 counter_inverse;
} probe_page;

typedef struct {
   uint64 read_bytes, write_bytes, major_faults, minor_faults, cpu_ns;
} probe_counters;

enum { PROBE_PERF_EVENTS = 4 };

typedef struct {
   int    fd[PROBE_PERF_EVENTS];
   uint64 ids[PROBE_PERF_EVENTS];
   uint64 value[PROBE_PERF_EVENTS];
   uint64 enabled_ns;
   uint64 running_ns;
} probe_perf;

static const uint64 probe_perf_config[PROBE_PERF_EVENTS] = {
   PERF_COUNT_HW_CPU_CYCLES,
   PERF_COUNT_HW_INSTRUCTIONS,
   PERF_COUNT_HW_BRANCH_INSTRUCTIONS,
   PERF_COUNT_HW_BRANCH_MISSES,
};

static const char *probe_perf_name[PROBE_PERF_EVENTS] = {
   "cycles", "instructions", "branches", "branch_misses",
};

/* pid=0, cpu=-1 binds the event to the calling worker, not the control thread.
 * Keep all events in one pinned group: no independent event multiplexing or
 * ratio extrapolation is permitted. The caller must pin homogeneous CPUs. */
static void
probe_perf_open(probe_perf *perf)
{
   for (unsigned i = 0; i < PROBE_PERF_EVENTS; i++) {
      struct perf_event_attr attr = {
         .size           = sizeof(attr),
         .type           = PERF_TYPE_HARDWARE,
         .config         = probe_perf_config[i],
         .disabled       = i == 0,
         .pinned         = i == 0,
         .exclude_kernel = 1,
         .exclude_hv     = 1,
         .read_format    = PERF_FORMAT_GROUP | PERF_FORMAT_ID
                           | PERF_FORMAT_TOTAL_TIME_ENABLED
                           | PERF_FORMAT_TOTAL_TIME_RUNNING,
      };
      perf->fd[i] = syscall(__NR_perf_event_open,
                            &attr,
                            0,
                            -1,
                            i == 0 ? -1 : perf->fd[0],
                            PERF_FLAG_FD_CLOEXEC);
      if (perf->fd[i] < 0) {
         fprintf(stderr,
                 "perf_event_open(%s): %s; sample rejected\n",
                 probe_perf_name[i], strerror(errno));
         exit(1);
      }
      if (ioctl(perf->fd[i], PERF_EVENT_IOC_ID, &perf->ids[i]) != 0) {
         perror("PERF_EVENT_IOC_ID");
         exit(1);
      }
   }
}

static void
probe_perf_start(probe_perf *perf)
{
   if (ioctl(perf->fd[0], PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP) != 0
       || ioctl(perf->fd[0], PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP) != 0)
   {
      perror("perf group reset/enable");
      exit(1);
   }
}

static void
probe_perf_stop(probe_perf *perf)
{
   if (ioctl(perf->fd[0], PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP) != 0) {
      perror("perf group disable");
      exit(1);
   }
}

static void
probe_perf_read_close(probe_perf *perf, bool32 measured)
{
   struct {
      uint64 nr, time_enabled, time_running;
      struct { uint64 value, id; } events[PROBE_PERF_EVENTS];
   } result = {0};
   ssize_t length = read(perf->fd[0], &result, sizeof(result));
   if (length != sizeof(result) || result.nr != PROBE_PERF_EVENTS
       || (measured && (result.time_running == 0
                        || result.time_enabled != result.time_running)))
   {
      fprintf(stderr,
              "perf group unavailable or multiplexed: bytes=%zd events=%lu "
              "enabled=%lu running=%lu; sample rejected\n",
              length, result.nr, result.time_enabled, result.time_running);
      exit(1);
   }
   perf->enabled_ns = result.time_enabled;
   perf->running_ns = result.time_running;
   unsigned matched = 0;
   for (unsigned i = 0; i < PROBE_PERF_EVENTS; i++) {
      for (unsigned j = 0; j < PROBE_PERF_EVENTS; j++) {
         if (result.events[i].id == perf->ids[j]) {
            perf->value[j] = result.events[i].value;
            matched++;
         }
      }
   }
   if (matched != PROBE_PERF_EVENTS) {
      fprintf(stderr, "perf event IDs did not match; sample rejected\n");
      exit(1);
   }
   for (unsigned i = 0; i < PROBE_PERF_EVENTS; i++) {
      if (close(perf->fd[i]) != 0) {
         perror("perf event close");
         exit(1);
      }
   }
}

typedef struct probe_context {
   clockcache *clock;
   uint64     *extents;
   uint64      pages;
   uint64      page_size;
   uint64      pages_per_extent;
   uint32      phase; // 0 = starting, 1 = warmup, 2 = measurement, 3 = stop
   uint32      ready;
   bool32      shared;
   uint32      threads;
   bool32      perf_enabled;
   const char *api_name;
   uint64      warmup_seconds;
   int         worker_cpus[MAX_THREADS];
   uint32      num_worker_cpus;
   int         control_cpu;
} probe_context;

typedef struct {
   probe_context *context;
   uint64         worker;
   uint64         ops;
   uint64         checksum;
   uint64         errors;
   uint64         claim_failures;
   uint64         all_claim_failures;
   uint64         all_ops;
   uint64         finished;
   probe_perf     perf;
   uint64         cpu_ns;
   int            actual_start_cpu;
   int            actual_end_cpu;
   threadid       registered_tid;
} PLATFORM_CACHELINE_ALIGNED probe_worker;

static uint64
nanos(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

static uint64
thread_cpu_nanos(void)
{
   struct timespec ts;
   platform_assert(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0);
   return (uint64)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

static void
pin_current_thread(int cpu)
{
   platform_assert(cpu >= 0 && cpu < CPU_SETSIZE);
   cpu_set_t set;
   CPU_ZERO(&set);
   CPU_SET(cpu, &set);
   int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
   if (rc != 0) {
      fprintf(stderr, "pin cpu %d: %s; sample rejected\n", cpu, strerror(rc));
      exit(1);
   }
   platform_assert(sched_getcpu() == cpu);
}

static void
sleep_seconds(uint64 seconds)
{
   struct timespec remaining = {.tv_sec = seconds};
   while (nanosleep(&remaining, &remaining) != 0 && errno == EINTR) {
   }
}

static void
check(platform_status rc)
{
   if (!SUCCESS(rc)) {
      fprintf(stderr, "cache probe: %s\n", platform_status_to_string(rc));
      exit(1);
   }
}

static uint64
random64(uint64 *state)
{
   uint64 value = *state;
   value ^= value << 13;
   value ^= value >> 7;
   value ^= value << 17;
   return *state = value;
}

static uint64
page_address(const probe_context *ctx, uint64 index)
{
   return ctx->extents[index / ctx->pages_per_extent]
          + (index % ctx->pages_per_extent) * ctx->page_size;
}

static probe_counters
snapshot(void)
{
   probe_counters c      = {0};
   FILE          *stream = fopen("/proc/self/io", "r");
   platform_assert(stream != NULL);
   char     line[128];
   unsigned found = 0;
   while (fgets(line, sizeof(line), stream)) {
      unsigned long long value;
      if (sscanf(line, "read_bytes: %llu", &value) == 1) {
         c.read_bytes = value;
         found |= 1;
      } else if (sscanf(line, "write_bytes: %llu", &value) == 1) {
         c.write_bytes = value;
         found |= 2;
      }
   }
   platform_assert(fclose(stream) == 0 && found == 3);
   struct rusage usage;
   platform_assert(getrusage(RUSAGE_SELF, &usage) == 0);
   c.major_faults = usage.ru_majflt;
   c.minor_faults = usage.ru_minflt;
   struct timespec ts;
   platform_assert(clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts) == 0);
   c.cpu_ns = (uint64)ts.tv_sec * 1000000000 + ts.tv_nsec;
   return c;
}

/* Only the mode is specialized; the same executable drives every library.
 * On failed claim, discard the read reference before retrying the same page.
 * This lets a claimed writer drain readers instead of deadlocking. */
static inline __attribute__((always_inline)) void *
probe_run_worker_impl(void *arg, bool32 shared)
{
   probe_worker  *worker = arg;
   probe_context *ctx    = worker->context;
   cache         *cc     = (cache *)ctx->clock;
   pin_current_thread(ctx->worker_cpus[worker->worker]);
   platform_register_thread();
   worker->registered_tid = platform_get_tid();
   if (ctx->perf_enabled) {
      probe_perf_open(&worker->perf);
   }
   __atomic_add_fetch(&ctx->ready, 1, __ATOMIC_RELEASE);
   uint64 seed = 42 + worker->worker;
   /* Interleaved disjoint partitions cover every page exactly once. The
    * last workers may have one fewer page when pages % threads != 0. */
   uint64 choices = shared ? ctx->pages
                           : (ctx->pages - 1 - worker->worker) / ctx->threads + 1;
   uint64 steps = 0;
   bool32 measuring = FALSE;
   for (;;) {
      uint32 phase = __atomic_load_n(&ctx->phase, __ATOMIC_ACQUIRE);
      if (phase == 0) {
         platform_pause();
         continue;
      }
      if (phase == 3) {
         break;
      }
      if (phase == 2 && !measuring) {
         measuring = TRUE;
         seed = 42 + worker->worker;
         worker->actual_start_cpu = sched_getcpu();
         worker->cpu_ns = thread_cpu_nanos();
         if (ctx->perf_enabled) {
            probe_perf_start(&worker->perf);
         }
      }
      for (uint64 batch = 0; batch < 64; batch++, steps++) {
         uint64 index = random64(&seed) % choices;
         if (!shared) {
            index = index * ctx->threads + worker->worker;
         }
         uint64 addr = page_address(ctx, index);
         for (;;) {
            page_handle page;
            platform_status rc =
               cache_get(cc, addr, TRUE, PAGE_TYPE_MEMTABLE, &page);
            if (!SUCCESS(rc)) {
               worker->errors++;
               __atomic_store_n(&ctx->phase, 3, __ATOMIC_RELEASE);
               goto finished;
            }
            probe_page *stored = (probe_page *)page_handle_data(&page);
            bool32 valid = page_handle_addr(&page) == addr
                           && stored->identity == (addr ^ PROBE_MAGIC);
            if ((steps & 1023) == 0) {
               clockcache_entry *entry = (clockcache_entry *)page.page;
               uint32 status = __atomic_load_n(&entry->status, __ATOMIC_RELAXED);
               valid = valid && !(status & PROBE_UNREADABLE_STATUS)
                       && cache_get_read_ref(cc, &page) != 0;
            }
            if (!valid) {
               cache_unget(cc, &page);
               worker->errors++;
               __atomic_store_n(&ctx->phase, 3, __ATOMIC_RELEASE);
               goto finished;
            }
            if (!cache_try_claim(cc, &page)) {
               cache_unget(cc, &page);
               worker->all_claim_failures++;
               if (measuring) {
                  worker->claim_failures++;
               }
               if (__atomic_load_n(&ctx->phase, __ATOMIC_ACQUIRE) == 3) {
                  goto finished;
               }
               platform_pause();
               continue;
            }
            /* This upgrades the claim and marks dirty, exactly as B-tree
             * mutation does. Mutable words are inspected only under lock. */
            cache_lock(cc, &page);
            uint64 value = stored->counter;
            valid = stored->counter_inverse == ~value && value != UINT64_MAX;
            if (valid) {
               stored->counter = value + 1;
               stored->counter_inverse = ~(value + 1);
            }
            cache_unlock(cc, &page);
            cache_unclaim(cc, &page);
            cache_unget(cc, &page);
            if (!valid) {
               worker->errors++;
               __atomic_store_n(&ctx->phase, 3, __ATOMIC_RELEASE);
               goto finished;
            }
            worker->all_ops++;
            if (measuring) {
               worker->ops++;
               worker->checksum += addr ^ PROBE_MAGIC;
            }
            break;
         }
      }
   }
finished:
   if (measuring && ctx->perf_enabled) {
      probe_perf_stop(&worker->perf);
   }
   worker->finished = nanos();
   worker->actual_end_cpu = sched_getcpu();
   if (measuring) {
      worker->cpu_ns = thread_cpu_nanos() - worker->cpu_ns;
   }
   if (ctx->perf_enabled) {
      probe_perf_read_close(&worker->perf, measuring);
   }
   platform_deregister_thread();
   return NULL;
}

static void *
probe_run_disjoint(void *arg)
{
   return probe_run_worker_impl(arg, FALSE);
}

static void *
probe_run_shared(void *arg)
{
   return probe_run_worker_impl(arg, TRUE);
}

/* No writers remain. This scan and its logging are outside all measured
 * CPU/I/O snapshots. Compare every protected counter and their exact sum to
 * worker-local successful writes, including warmup and stopping batches. */
static uint64
verify_pages(probe_context *ctx, const probe_worker *workers, uint64 *observed)
{
   cache *cc = (cache *)ctx->clock;
   uint64 errors = 0, sums[MAX_THREADS] = {0}, expected = 0;
   *observed = 0;
   for (uint64 index = 0; index < ctx->pages; index++) {
      uint64 addr = page_address(ctx, index);
      page_handle page;
      platform_status rc = cache_get(cc, addr, TRUE, PAGE_TYPE_MEMTABLE, &page);
      if (!SUCCESS(rc)) {
         errors++;
         continue;
      }
      const probe_page *stored = (const probe_page *)page_handle_data(&page);
      if (page_handle_addr(&page) != addr
          || stored->identity != (addr ^ PROBE_MAGIC)
          || stored->counter_inverse != ~stored->counter) {
         errors++;
      }
      *observed += stored->counter;
      sums[index % ctx->threads] += stored->counter;
      cache_unget(cc, &page);
   }
   for (uint32 i = 0; i < ctx->threads; i++) {
      expected += workers[i].all_ops;
      if (!ctx->shared && sums[i] != workers[i].all_ops) {
         errors++;
      }
   }
   if (*observed != expected) {
      errors++;
   }
   fprintf(stderr, "VERIFY pages=%lu observed=%lu expected=%lu errors=%lu\n",
           ctx->pages, *observed, expected, errors);
   return errors;
}

static uint64
run_phase(probe_context *ctx,
          uint32         threads,
          uint64         seconds,
          const char    *mode,
          uint64         cache_mib,
          uint64         setup_ns)
{
   platform_assert(threads <= ctx->num_worker_cpus && threads <= ctx->pages);
   ctx->threads = threads;
   probe_worker workers[MAX_THREADS] = {0};
   pthread_t    handles[MAX_THREADS];
   void *(*worker_fn)(void *) = ctx->shared ? probe_run_shared
                                           : probe_run_disjoint;
   __atomic_store_n(&ctx->ready, 0, __ATOMIC_RELAXED);
   __atomic_store_n(&ctx->phase, 0, __ATOMIC_RELAXED);
   for (uint32 i = 0; i < threads; i++) {
      workers[i].context = ctx;
      workers[i].worker  = i;
      platform_assert(pthread_create(&handles[i], NULL, worker_fn, &workers[i])
                      == 0);
      /* Registration assigns the cache refcount stripe. Serialize only
       * startup so worker index -> CPU -> stripe stays stable across runs. */
      while (__atomic_load_n(&ctx->ready, __ATOMIC_ACQUIRE) != i + 1) {
         platform_pause();
      }
   }
   while (__atomic_load_n(&ctx->ready, __ATOMIC_ACQUIRE) != threads) {
      platform_pause();
   }
   __atomic_store_n(&ctx->phase, 1, __ATOMIC_RELEASE);
   sleep_seconds(ctx->warmup_seconds);
   probe_counters before = snapshot();
   uint64         begin  = nanos();
   if (__atomic_load_n(&ctx->phase, __ATOMIC_ACQUIRE) != 3) {
      __atomic_store_n(&ctx->phase, 2, __ATOMIC_RELEASE);
      sleep_seconds(seconds);
   }
   __atomic_store_n(&ctx->phase, 3, __ATOMIC_RELEASE);
   uint64 finish = begin, ops = 0, checksum = 0, errors = 0;
   uint64 claim_failures = 0, all_claim_failures = 0, all_ops = 0;
   uint64 perf_values[PROBE_PERF_EVENTS] = {0};
   uint64 perf_enabled_ns = 0, perf_running_ns = 0, worker_cpu_ns = 0;
   for (uint32 i = 0; i < threads; i++) {
      platform_assert(pthread_join(handles[i], NULL) == 0);
      ops += workers[i].ops;
      checksum += workers[i].checksum;
      errors += workers[i].errors;
      claim_failures += workers[i].claim_failures;
      all_claim_failures += workers[i].all_claim_failures;
      all_ops += workers[i].all_ops;
      worker_cpu_ns += workers[i].cpu_ns;
      if (workers[i].actual_start_cpu != ctx->worker_cpus[i]
          || workers[i].actual_end_cpu != ctx->worker_cpus[i])
      {
         errors++;
      }
      finish = MAX(finish, workers[i].finished);
      for (unsigned j = 0; j < PROBE_PERF_EVENTS; j++) {
         perf_values[j] += workers[i].perf.value[j];
      }
      perf_enabled_ns += workers[i].perf.enabled_ns;
      perf_running_ns += workers[i].perf.running_ns;
   }
   probe_counters after        = snapshot();
   /* Log only after the I/O/CPU snapshots: redirected stderr may dirty a
    * real file and otherwise masquerade as timed storage I/O. */
   for (uint32 i = 0; i < threads; i++) {
      fprintf(stderr,
              "WORKER worker=%u tid=%lu cpu=%d start_cpu=%d end_cpu=%d ops=%lu "
              "cpu_ns=%lu cycles=%lu instructions=%lu branches=%lu "
              "branch_misses=%lu all_ops=%lu claim_failures=%lu "
              "all_claim_failures=%lu\n",
              i, workers[i].registered_tid, ctx->worker_cpus[i], workers[i].actual_start_cpu,
              workers[i].actual_end_cpu, workers[i].ops, workers[i].cpu_ns,
              workers[i].perf.value[0], workers[i].perf.value[1],
              workers[i].perf.value[2], workers[i].perf.value[3],
              workers[i].all_ops, workers[i].claim_failures,
              workers[i].all_claim_failures);
   }
   uint64         read_bytes   = after.read_bytes - before.read_bytes;
   uint64         write_bytes  = after.write_bytes - before.write_bytes;
   uint64         major_faults = after.major_faults - before.major_faults;
   if (read_bytes || write_bytes || major_faults)
   {
      fprintf(stderr,
              "resident interval rejected: read=%lu write=%lu major=%lu\n",
              read_bytes,
              write_bytes,
              major_faults);
      errors++;
   }
   uint64 verified_updates = 0;
   errors += verify_pages(ctx, workers, &verified_updates);
   uint64 dirty_pages = cache_count_dirty((cache *)ctx->clock);
   if (all_ops == 0 || dirty_pages == 0) {
      errors++;
   }
   struct rusage usage;
   getrusage(RUSAGE_SELF, &usage);
   clockcache *cc      = ctx->clock;
   double      elapsed = (double)(finish - begin) / 1e9;
   printf("%s,%s,%lu,%u,%lu,%.9f,%.1f,%lu,%lu,%lu,%zu,%zu,%zu,%zu,%zu,%ld,%lu,%"
          "lu,%lu,%lu,%lu,%lu,%lu,%d,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%d,%lu,"
          "%lu,%lu,%lu,%lu,%lu,%lu\n",
          mode,
          ctx->api_name,
          cache_mib,
          threads,
          ctx->pages,
          elapsed,
          ops / elapsed,
          ops,
          checksum,
          errors,
          cc->lookup_bh.length,
          cc->entry_bh.length,
          cc->data_bh.length,
          cc->rc_bh.length,
          cc->pincount_bh.length,
          usage.ru_maxrss * 1024L,
          setup_ns,
          read_bytes,
          write_bytes,
          major_faults,
          after.minor_faults - before.minor_faults,
          after.cpu_ns - before.cpu_ns,
          (uint64)0,
          ctx->perf_enabled,
          perf_values[0],
          perf_values[1],
          perf_values[2],
          perf_values[3],
          perf_enabled_ns,
          perf_running_ns,
          ctx->warmup_seconds,
          ctx->control_cpu,
          worker_cpu_ns,
          claim_failures,
          all_claim_failures,
          all_ops,
          all_ops - ops,
          verified_updates,
          dirty_pages);
   fflush(stdout);
   return errors;
}

int
main(int argc, char **argv)
{
   if (argc != 7) {
      fprintf(stderr,
              "usage: %s FILE CACHE_MIB THREADS SECONDS "
              "[disjoint|shared] write\n", argv[0]);
      return 2;
   }
   uint64 cache_mib = strtoull(argv[2], NULL, 10);
   uint64 seconds = strtoull(argv[4], NULL, 10);
   const char *mode = argv[5];
   const char *api_name = argv[6];
   char *threads_end = NULL;
   uint64 threads = strtoull(argv[3], &threads_end, 10);
   platform_assert(*argv[3] >= '0' && *argv[3] <= '9' && *threads_end == '\0'
                   && threads > 0 && threads < MAX_THREADS);
   platform_assert(cache_mib >= 1 && cache_mib <= 32768 && seconds > 0);
   platform_assert(strcmp(mode, "disjoint") == 0 || strcmp(mode, "shared") == 0);
   platform_assert(strcmp(api_name, "write") == 0);
   const char *control_cpu_env = getenv("CACHE_PROBE_CONTROL_CPU");
   platform_assert(control_cpu_env != NULL);
   char *control_end = NULL;
   long control_cpu = strtol(control_cpu_env, &control_end, 10);
   platform_assert(*control_cpu_env && *control_end == '\0'
                   && control_cpu >= 0 && control_cpu < CPU_SETSIZE);
   pin_current_thread(control_cpu);
   platform_register_thread();
   platform_heap_id hid;
   check(
      platform_heap_create(platform_get_module_id(), 512 * MiB, FALSE, &hid));
   io_config io_cfg;
   io_config_init(&io_cfg,
                  IO_DEFAULT_PAGE_SIZE,
                  IO_DEFAULT_EXTENT_SIZE,
                  O_RDWR | O_CREAT | O_TRUNC | O_DIRECT,
                  IO_DEFAULT_PERMS,
                  IO_DEFAULT_ASYNC_QUEUE_DEPTH,
                  argv[1]);
   io_handle *io = io_handle_create(&io_cfg, hid);
   platform_assert(io != NULL);
   allocator_config al_cfg;
   const char      *ratio_env  = getenv("CACHE_PROBE_DISK_RATIO");
   uint64           disk_ratio = ratio_env ? strtoull(ratio_env, NULL, 10) : 16;
   platform_assert(disk_ratio >= 2 && disk_ratio <= 256);
   const char *layout = getenv("CACHE_PROBE_ADDRESS_LAYOUT");
   if (layout == NULL) {
      layout = "spread";
   }
   platform_assert(strcmp(layout, "spread") == 0
                   || strcmp(layout, "compact") == 0);
   allocator_config_init(
      &al_cfg, &io_cfg, MAX(1 * GiB, cache_mib * MiB * disk_ratio));
   rc_allocator al;
   check(rc_allocator_init(&al, &al_cfg, io, hid, platform_get_module_id()));
   clockcache_config cfg;
   clockcache_config_init(&cfg, &io_cfg, cache_mib * MiB, NULL, FALSE);
   clockcache cc;
   check(clockcache_init(&cc,
                         &cfg,
                         io,
                         (allocator *)&al,
                         "cache probe",
                         hid,
                         platform_get_module_id()));
   probe_context ctx = {.clock     = &cc,
                        .shared    = strcmp(mode, "shared") == 0,
                        .api_name  = api_name,
                        .page_size = io_cfg.page_size,
                        .pages_per_extent =
                           io_cfg.extent_size / io_cfg.page_size};
   const char *perf_env = getenv("CACHE_PROBE_PERF");
   platform_assert(perf_env == NULL || strcmp(perf_env, "0") == 0
                   || strcmp(perf_env, "1") == 0);
   ctx.perf_enabled = perf_env == NULL || strcmp(perf_env, "1") == 0;
   ctx.control_cpu = control_cpu;
   const char *warmup_env = getenv("CACHE_PROBE_WARMUP_SECONDS");
   ctx.warmup_seconds = warmup_env ? strtoull(warmup_env, NULL, 10) : 5;
   platform_assert(ctx.warmup_seconds > 0 && ctx.warmup_seconds <= 600);
   const char *worker_cpus_env = getenv("CACHE_PROBE_WORKER_CPUS");
   platform_assert(worker_cpus_env != NULL && *worker_cpus_env);
   char *cpu_list = strdup(worker_cpus_env), *cpu_save = NULL;
   platform_assert(cpu_list != NULL);
   for (char *token = strtok_r(cpu_list, ",", &cpu_save); token;
        token = strtok_r(NULL, ",", &cpu_save))
   {
      char *end = NULL;
      long cpu = strtol(token, &end, 10);
      platform_assert(*end == '\0' && cpu >= 0 && cpu < CPU_SETSIZE
                      && cpu != control_cpu && ctx.num_worker_cpus < MAX_THREADS);
      for (uint32 i = 0; i < ctx.num_worker_cpus; i++) {
         platform_assert(ctx.worker_cpus[i] != cpu);
      }
      ctx.worker_cpus[ctx.num_worker_cpus++] = cpu;
   }
   free(cpu_list);
   fprintf(stderr, "CONTROL worker_cpus=%s control_cpu=%ld control_tid=%lu warmup_seconds=%lu\n",
           worker_cpus_env, control_cpu, platform_get_tid(), ctx.warmup_seconds);
   ctx.pages = (uint64)cfg.page_capacity * 3 / 4;
   const char *working_pages_env = getenv("CACHE_PROBE_WORKING_PAGES");
   if (working_pages_env != NULL) {
      char *end = NULL;
      errno = 0;
      uint64 requested_pages = strtoull(working_pages_env, &end, 10);
      platform_assert(*working_pages_env >= '0'
                      && *working_pages_env <= '9' && *end == '\0'
                      && errno != ERANGE && requested_pages >= 32
                      && requested_pages <= (uint64)cfg.page_capacity * 3 / 4
                      && requested_pages <= UINT32_MAX,
                      "CACHE_PROBE_WORKING_PAGES must be >=32 and <=75%% "
                      "of cache pages");
      ctx.pages = requested_pages;
   }
   uint64 extents =
      (ctx.pages + ctx.pages_per_extent - 1) / ctx.pages_per_extent;
   if (working_pages_env == NULL) {
      ctx.pages = extents * ctx.pages_per_extent;
   }
   ctx.extents   = calloc(extents, sizeof(*ctx.extents));
   platform_assert(ctx.extents && ctx.pages <= UINT32_MAX
                   && ctx.pages >= threads);
   uint64 setup_start = nanos();
   if (strcmp(layout, "spread") == 0) {
      /* Allocate through the real allocator, then retain a deterministic
       * random subset. Only selected extents receive disk data. Contiguous
       * addresses would touch a tiny part of the direct directory even at
       * a large logical disk/cache ratio. All setup is outside timing. */
      uint64  available = al_cfg.extent_capacity - al.stats.curr_allocated;
      uint64 *addresses = malloc(available * sizeof(*addresses));
      platform_assert(addresses != NULL && extents <= available);
      for (uint64 i = 0; i < available; i++) {
         check(
            allocator_alloc((allocator *)&al, &addresses[i], PAGE_TYPE_MEMTABLE));
      }
      uint64 address_seed = 42;
      for (uint64 i = 0; i < extents; i++) {
         uint64 j       = i + random64(&address_seed) % (available - i);
         uint64 saved   = addresses[i];
         addresses[i]   = addresses[j];
         addresses[j]   = saved;
         ctx.extents[i] = addresses[i];
      }
      for (uint64 i = extents; i < available; i++) {
         platform_assert(
            allocator_dec_ref((allocator *)&al, addresses[i], PAGE_TYPE_MEMTABLE)
            == AL_NO_REFS);
         platform_assert(
            allocator_dec_ref((allocator *)&al, addresses[i], PAGE_TYPE_MEMTABLE)
            == AL_FREE);
      }
      free(addresses);
   } else {
      for (uint64 i = 0; i < extents; i++) {
         check(
            allocator_alloc((allocator *)&al, &ctx.extents[i], PAGE_TYPE_MEMTABLE));
      }
   }
   buffer_handle write_buffer;
   check(platform_buffer_init(&write_buffer, io_cfg.extent_size));
   char *buffer = platform_buffer_getaddr(&write_buffer);
   for (uint64 i = 0; i < extents; i++) {
      memset(buffer, 0, io_cfg.extent_size);
      for (uint64 p = 0; p < ctx.pages_per_extent; p++) {
         uint64      index     = i * ctx.pages_per_extent + p;
         if (index >= ctx.pages) {
            break;
         }
         probe_page *stored    = (probe_page *)(buffer + p * ctx.page_size);
         stored->identity      = page_address(&ctx, index) ^ PROBE_MAGIC;
         stored->counter = 0;
         stored->counter_inverse = UINT64_MAX;
      }
      check(io_write(io, buffer, io_cfg.extent_size, ctx.extents[i]));
   }
   check(io_durable_barrier(io));
   platform_buffer_deinit(&write_buffer);
   for (uint64 i = 0; i < ctx.pages; i++) {
      page_handle page;
      check(cache_get(
         (cache *)&cc, page_address(&ctx, i), TRUE, PAGE_TYPE_MEMTABLE, &page));
      platform_assert(!page_handle_is_null(&page)
                      && page_handle_addr(&page) == page_address(&ctx, i));
      cache_unget((cache *)&cc, &page);
   }
   io_wait_all(io);
   uint64 setup_ns = nanos() - setup_start;
   fprintf(stderr,
           "O_DIRECT address_layout=%s cache_mib=%lu disk_ratio=%lu "
           "resident_pages=%lu working_set_bytes=%lu disk_bytes=%lu "
           "setup_seconds=%.3f\n",
           layout,
           cache_mib,
           disk_ratio,
           ctx.pages,
           ctx.pages * ctx.page_size,
           extents * io_cfg.extent_size,
           setup_ns / 1e9);
   puts("mode,api,cache_mib,threads,working_pages,seconds,ops_s,ops,checksum,"
        "errors,directory_bytes,entry_bytes,data_bytes,refcount_bytes,pin_"
        "bytes,max_rss_bytes,setup_ns,read_bytes,write_bytes,major_faults,"
        "minor_faults,cpu_ns,async_waits,perf_enabled,perf_cycles,perf_instructions,"
        "perf_branches,perf_branch_misses,perf_enabled_ns,perf_running_ns,"
        "warmup_seconds,control_cpu,worker_cpu_ns,claim_failures,"
        "all_claim_failures,all_ops,warmup_ops,verified_updates,dirty_pages");
   uint64 errors = run_phase(&ctx, threads, seconds, mode, cache_mib, setup_ns);
   /* Dirty writeback, completion and eviction occur only after the final
    * verification and output. They are deliberately absent from timings. */
   cache_flush((cache *)&cc);
   io_wait_all(io);
   check(cache_evict((cache *)&cc, FALSE));
   for (uint64 i = 0; i < extents; i++) {
      platform_assert(
         allocator_dec_ref((allocator *)&al, ctx.extents[i], PAGE_TYPE_MEMTABLE)
         == AL_NO_REFS);
      platform_assert(
         allocator_dec_ref((allocator *)&al, ctx.extents[i], PAGE_TYPE_MEMTABLE)
         == AL_FREE);
   }
   free(ctx.extents);
   clockcache_deinit(&cc);
   rc_allocator_deinit(&al);
   io_handle_destroy(io);
   platform_heap_destroy(&hid);
   platform_deregister_thread();
   return errors != 0;
}
