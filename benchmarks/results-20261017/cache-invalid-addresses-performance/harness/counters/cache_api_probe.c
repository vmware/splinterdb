// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/* Real clockcache probe, shared unchanged with the baseline. All setup and
 * warmup are outside timing. Run only under the campaign benchmark lock on a
 * real disk. The file is opened O_DIRECT and contains the actual cached pages.
 * v2: per-worker userspace hardware counters cover the measured phase only.
 * CACHE_PROBE_PERF=0 disables hardware counters with the same executable.
 * Usage: FILE CACHE_MIB THREADS[,THREADS...] SECONDS
 * [both|independent|dependent|churn|miss] [blocking|nonblocking|async].
 * CACHE_PROBE_DISK_RATIO defaults to 16. miss requires nonblocking and checks
 * valid allocated but uncached pages (STATUS_BUSY), without timed disk I/O.
 */
#include <errno.h>
#include <inttypes.h>
#include <linux/perf_event.h>
#include <pthread.h>
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
   uint64 next_addr;
   uint64 next_identity;
} probe_page;

typedef enum {
   PROBE_BLOCKING,
   PROBE_NONBLOCKING,
   PROBE_ASYNC,
   PROBE_MISS,
} probe_api;

typedef struct {
   page_get_async_state_buffer state;
   uint32                      ready;
} probe_async;

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
   bool32      dependent;
   probe_api   api;
   bool32      perf_enabled;
   const char *api_name;
} probe_context;

typedef struct {
   probe_context *context;
   uint64         worker;
   uint64         ops;
   uint64         checksum;
   uint64         errors;
   uint64         async_waits;
   uint64         finished;
   probe_perf     perf;
} PLATFORM_CACHELINE_ALIGNED probe_worker;

static uint64
nanos(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64)ts.tv_sec * 1000000000 + ts.tv_nsec;
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

static void
probe_async_ready(void *arg)
{
   probe_async *state = arg;
   __atomic_store_n(&state->ready, 1, __ATOMIC_RELEASE);
}

static inline platform_status
probe_async_get(cache       *cc,
                uint64       addr,
                probe_async *state,
                page_handle *page,
                uint64      *waits)
{
   __atomic_store_n(&state->ready, 0, __ATOMIC_RELAXED);
   cache_get_async_state_init(
      &state->state, cc, addr, PAGE_TYPE_MISC, probe_async_ready, state);
   async_status status = cache_get_async(&state->state);
   while (status != ASYNC_STATUS_DONE) {
      (*waits)++;
      while (!__atomic_exchange_n(&state->ready, 0, __ATOMIC_ACQ_REL)) {
         cache_cleanup(cc);
         platform_pause();
      }
      status = cache_get_async(&state->state);
   }
   return cache_get_async_state_result(&state->state, page);
}

/* Each wrapper supplies a constant API, removing API dispatch from the hot
 * loop while keeping one identical loop body for both shared libraries. */
static inline __attribute__((always_inline)) void *
probe_run_worker_impl(void *arg, probe_api api)
{
   probe_worker  *worker = arg;
   probe_context *ctx    = worker->context;
   cache         *cc     = (cache *)ctx->clock;
   platform_register_thread();
   if (ctx->perf_enabled) {
      probe_perf_open(&worker->perf);
   }
   __atomic_add_fetch(&ctx->ready, 1, __ATOMIC_RELEASE);
   probe_async async_state;
   uint64      seed      = 42 + worker->worker;
   uint64      addr      = page_address(ctx, worker->worker % ctx->pages);
   uint64      steps     = 0;
   bool32      measuring = FALSE;
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
         seed      = 42 + worker->worker;
         addr      = page_address(ctx, worker->worker % ctx->pages);
         if (ctx->perf_enabled) {
            probe_perf_start(&worker->perf);
         }
      }
      for (uint64 batch = 0; batch < 64; batch++, steps++) {
         if (!ctx->dependent) {
            addr = page_address(ctx, random64(&seed) % ctx->pages);
         }
         page_handle     page;
         platform_status rc;
         if (api == PROBE_ASYNC) {
            rc = probe_async_get(
               cc, addr, &async_state, &page, &worker->async_waits);
         } else {
            rc = cache_get(
               cc, addr, api == PROBE_BLOCKING, PAGE_TYPE_MISC, &page);
         }
         if (api == PROBE_MISS) {
            if (!STATUS_IS_EQ(rc, STATUS_BUSY) || !page_handle_is_null(&page)) {
               if (SUCCESS(rc)) {
                  cache_unget(cc, &page);
               }
               worker->errors++;
               __atomic_store_n(&ctx->phase, 3, __ATOMIC_RELEASE);
               goto finished;
            }
            if (measuring) {
               worker->ops++;
               worker->checksum += addr ^ PROBE_MAGIC;
            }
            continue;
         }
         if (!SUCCESS(rc)) {
            worker->errors++;
            __atomic_store_n(&ctx->phase, 3, __ATOMIC_RELEASE);
            goto finished;
         }
         const probe_page *stored = (const probe_page *)page_handle_data(&page);
         bool32            valid =
            page_handle_addr(&page) == addr
            && stored->identity == (addr ^ PROBE_MAGIC)
            && stored->next_identity == (stored->next_addr ^ PROBE_MAGIC)
            && stored->next_addr % ctx->page_size == 0;
         if ((steps & 1023) == 0) {
            clockcache_entry *entry = (clockcache_entry *)page.page;
            uint32 status = __atomic_load_n(&entry->status, __ATOMIC_RELAXED);
            valid         = valid && !(status & PROBE_UNREADABLE_STATUS)
                    && cache_get_read_ref(cc, &page) != 0;
         }
         uint64 next  = stored->next_addr;
         uint64 value = stored->identity;
         cache_unget(cc, &page);
         if (!valid) {
            worker->errors++;
            __atomic_store_n(&ctx->phase, 3, __ATOMIC_RELEASE);
            goto finished;
         }
         if (measuring) {
            worker->ops++;
            worker->checksum += value;
         }
         if (ctx->dependent) {
            addr = next;
         }
      }
   }
finished:
   if (measuring && ctx->perf_enabled) {
      probe_perf_stop(&worker->perf);
   }
   worker->finished = nanos();
   if (ctx->perf_enabled) {
      probe_perf_read_close(&worker->perf, measuring);
   }
   platform_deregister_thread();
   return NULL;
}

static void *
probe_run_blocking(void *arg)
{
   return probe_run_worker_impl(arg, PROBE_BLOCKING);
}
static void *
probe_run_nonblocking(void *arg)
{
   return probe_run_worker_impl(arg, PROBE_NONBLOCKING);
}
static void *
probe_run_async(void *arg)
{
   return probe_run_worker_impl(arg, PROBE_ASYNC);
}
static void *
probe_run_miss(void *arg)
{
   return probe_run_worker_impl(arg, PROBE_MISS);
}

static uint64
run_phase(probe_context *ctx,
          uint32         threads,
          uint64         seconds,
          const char    *mode,
          uint64         cache_mib,
          uint64         setup_ns)
{
   probe_worker workers[MAX_THREADS] = {0};
   pthread_t    handles[MAX_THREADS];
   void *(*worker_fn)(void *) = ctx->api == PROBE_BLOCKING ? probe_run_blocking
                                : ctx->api == PROBE_NONBLOCKING
                                   ? probe_run_nonblocking
                                : ctx->api == PROBE_ASYNC ? probe_run_async
                                                          : probe_run_miss;
   __atomic_store_n(&ctx->ready, 0, __ATOMIC_RELAXED);
   __atomic_store_n(&ctx->phase, 0, __ATOMIC_RELAXED);
   for (uint32 i = 0; i < threads; i++) {
      workers[i].context = ctx;
      workers[i].worker  = i;
      platform_assert(pthread_create(&handles[i], NULL, worker_fn, &workers[i])
                      == 0);
   }
   while (__atomic_load_n(&ctx->ready, __ATOMIC_ACQUIRE) != threads) {
      platform_pause();
   }
   __atomic_store_n(&ctx->phase, 1, __ATOMIC_RELEASE);
   sleep_seconds(1);
   probe_counters before = snapshot();
   uint64         begin  = nanos();
   if (__atomic_load_n(&ctx->phase, __ATOMIC_ACQUIRE) != 3) {
      __atomic_store_n(&ctx->phase, 2, __ATOMIC_RELEASE);
      sleep_seconds(seconds);
   }
   __atomic_store_n(&ctx->phase, 3, __ATOMIC_RELEASE);
   uint64 finish = begin, ops = 0, checksum = 0, errors = 0, async_waits = 0;
   uint64 perf_values[PROBE_PERF_EVENTS] = {0};
   uint64 perf_enabled_ns = 0, perf_running_ns = 0;
   for (uint32 i = 0; i < threads; i++) {
      platform_assert(pthread_join(handles[i], NULL) == 0);
      ops += workers[i].ops;
      checksum += workers[i].checksum;
      errors += workers[i].errors;
      async_waits += workers[i].async_waits;
      finish = MAX(finish, workers[i].finished);
      for (unsigned j = 0; j < PROBE_PERF_EVENTS; j++) {
         perf_values[j] += workers[i].perf.value[j];
      }
      perf_enabled_ns += workers[i].perf.enabled_ns;
      perf_running_ns += workers[i].perf.running_ns;
   }
   probe_counters after        = snapshot();
   uint64         read_bytes   = after.read_bytes - before.read_bytes;
   uint64         write_bytes  = after.write_bytes - before.write_bytes;
   uint64         major_faults = after.major_faults - before.major_faults;
   if (strcmp(mode, "churn") != 0
       && (read_bytes || write_bytes || major_faults))
   {
      fprintf(stderr,
              "resident interval rejected: read=%lu write=%lu major=%lu\n",
              read_bytes,
              write_bytes,
              major_faults);
      errors++;
   }
   struct rusage usage;
   getrusage(RUSAGE_SELF, &usage);
   clockcache *cc      = ctx->clock;
   double      elapsed = (double)(finish - begin) / 1e9;
   printf("%s,%s,%lu,%u,%lu,%.9f,%.1f,%lu,%lu,%lu,%zu,%zu,%zu,%zu,%zu,%ld,%lu,%"
          "lu,%lu,%lu,%lu,%lu,%lu,%d,%lu,%lu,%lu,%lu,%lu,%lu\n",
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
          async_waits,
          ctx->perf_enabled,
          perf_values[0],
          perf_values[1],
          perf_values[2],
          perf_values[3],
          perf_enabled_ns,
          perf_running_ns);
   fflush(stdout);
   return errors;
}

int
main(int argc, char **argv)
{
   if (argc < 5 || argc > 7) {
      fprintf(stderr,
              "usage: %s FILE CACHE_MIB THREADS[,THREADS...] SECONDS "
              "[both|independent|dependent|churn|miss] "
              "[blocking|nonblocking|async]\n",
              argv[0]);
      return 2;
   }
   uint64      cache_mib = strtoull(argv[2], NULL, 10);
   uint64      seconds   = strtoull(argv[4], NULL, 10);
   const char *mode      = argc >= 6 ? argv[5] : "both";
   const char *api_name  = argc == 7 ? argv[6] : "blocking";
   bool32      missing   = strcmp(mode, "miss") == 0;
   probe_api   api       = strcmp(api_name, "blocking") == 0 ? PROBE_BLOCKING
                           : strcmp(api_name, "nonblocking") == 0 ? PROBE_NONBLOCKING
                                                                  : PROBE_ASYNC;
   platform_assert(strcmp(api_name, "blocking") == 0
                   || strcmp(api_name, "nonblocking") == 0
                   || strcmp(api_name, "async") == 0);
   platform_assert(!missing || api == PROBE_NONBLOCKING);
   if (missing)
      api = PROBE_MISS;
   bool32 churn = strcmp(mode, "churn") == 0;
   bool32 both  = strcmp(mode, "both") == 0;
   platform_assert(cache_mib >= 1 && cache_mib <= 32768 && seconds > 0);
   platform_assert(both || churn || missing || strcmp(mode, "independent") == 0
                   || strcmp(mode, "dependent") == 0);
   platform_assert(!churn || cache_mib <= 1024,
                   "churn is limited to <= 1 GiB caches");
   uint32 counts[MAX_THREADS], num_counts = 0;
   char  *thread_list = strdup(argv[3]), *save = NULL;
   for (char *token = strtok_r(thread_list, ",", &save); token;
        token       = strtok_r(NULL, ",", &save))
   {
      uint64 count = strtoull(token, NULL, 10);
      platform_assert(count > 0 && count < MAX_THREADS
                      && num_counts < MAX_THREADS);
      counts[num_counts++] = count;
   }
   free(thread_list);
   platform_assert(num_counts != 0);
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
                        .api       = api,
                        .api_name  = api_name,
                        .page_size = io_cfg.page_size,
                        .pages_per_extent =
                           io_cfg.extent_size / io_cfg.page_size};
   const char *perf_env = getenv("CACHE_PROBE_PERF");
   platform_assert(perf_env == NULL || strcmp(perf_env, "0") == 0
                   || strcmp(perf_env, "1") == 0);
   ctx.perf_enabled = perf_env == NULL || strcmp(perf_env, "1") == 0;
   ctx.pages =
      churn ? (uint64)cfg.page_capacity * 2 : (uint64)cfg.page_capacity * 3 / 4;
   uint64 extents =
      (ctx.pages + ctx.pages_per_extent - 1) / ctx.pages_per_extent;
   ctx.pages     = extents * ctx.pages_per_extent;
   ctx.extents   = calloc(extents, sizeof(*ctx.extents));
   uint32 *order = malloc(ctx.pages * sizeof(*order));
   uint32 *next  = malloc(ctx.pages * sizeof(*next));
   platform_assert(ctx.extents && order && next && ctx.pages <= UINT32_MAX);
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
            allocator_alloc((allocator *)&al, &addresses[i], PAGE_TYPE_MISC));
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
            allocator_dec_ref((allocator *)&al, addresses[i], PAGE_TYPE_MISC)
            == AL_NO_REFS);
         platform_assert(
            allocator_dec_ref((allocator *)&al, addresses[i], PAGE_TYPE_MISC)
            == AL_FREE);
      }
      free(addresses);
   } else {
      for (uint64 i = 0; i < extents; i++) {
         check(
            allocator_alloc((allocator *)&al, &ctx.extents[i], PAGE_TYPE_MISC));
      }
   }
   for (uint64 i = 0; i < ctx.pages; i++) {
      order[i] = i;
   }
   uint64 seed = 42;
   for (uint64 i = ctx.pages - 1; i > 0; i--) {
      uint64 j    = random64(&seed) % (i + 1);
      uint32 temp = order[i];
      order[i]    = order[j];
      order[j]    = temp;
   }
   for (uint64 i = 0; i < ctx.pages; i++) {
      next[order[i]] = order[(i + 1) % ctx.pages];
   }
   free(order);
   buffer_handle write_buffer;
   check(platform_buffer_init(&write_buffer, io_cfg.extent_size));
   char *buffer = platform_buffer_getaddr(&write_buffer);
   for (uint64 i = 0; i < extents; i++) {
      memset(buffer, 0, io_cfg.extent_size);
      for (uint64 p = 0; p < ctx.pages_per_extent; p++) {
         uint64      index     = i * ctx.pages_per_extent + p;
         probe_page *stored    = (probe_page *)(buffer + p * ctx.page_size);
         stored->identity      = page_address(&ctx, index) ^ PROBE_MAGIC;
         stored->next_addr     = page_address(&ctx, next[index]);
         stored->next_identity = stored->next_addr ^ PROBE_MAGIC;
      }
      check(io_write(io, buffer, io_cfg.extent_size, ctx.extents[i]));
   }
   check(io_durable_barrier(io));
   free(next);
   platform_buffer_deinit(&write_buffer);
   for (uint64 i = 0; i < ctx.pages; i++) {
      page_handle page;
      check(cache_get(
         (cache *)&cc, page_address(&ctx, i), TRUE, PAGE_TYPE_MISC, &page));
      platform_assert(!page_handle_is_null(&page)
                      && page_handle_addr(&page) == page_address(&ctx, i));
      cache_unget((cache *)&cc, &page);
   }
   if (missing) {
      check(cache_evict((cache *)&cc, FALSE));
   }
   io_wait_all(io);
   uint64 setup_ns = nanos() - setup_start;
   fprintf(stderr,
           "O_DIRECT address_layout=%s cache_mib=%lu disk_ratio=%lu "
           "resident_pages=%lu disk_bytes=%lu "
           "setup_seconds=%.3f\n",
           layout,
           cache_mib,
           disk_ratio,
           ctx.pages,
           ctx.pages * ctx.page_size,
           setup_ns / 1e9);
   puts("mode,api,cache_mib,threads,working_pages,seconds,ops_s,ops,checksum,"
        "errors,directory_bytes,entry_bytes,data_bytes,refcount_bytes,pin_"
        "bytes,max_rss_bytes,setup_ns,read_bytes,write_bytes,major_faults,"
        "minor_faults,cpu_ns,async_waits,perf_enabled,perf_cycles,perf_instructions,"
        "perf_branches,perf_branch_misses,perf_enabled_ns,perf_running_ns");
   uint64 errors = 0;
   for (uint32 i = 0; i < num_counts; i++) {
      for (uint32 p = 0; p < (both ? 2 : 1); p++) {
         ctx.dependent     = both ? p != 0 : strcmp(mode, "dependent") == 0;
         const char *phase = ctx.dependent ? "dependent"
                             : churn       ? "churn"
                             : missing     ? "miss"
                                           : "independent";
         errors +=
            run_phase(&ctx, counts[i], seconds, phase, cache_mib, setup_ns);
      }
   }
   check(cache_evict((cache *)&cc, FALSE));
   for (uint64 i = 0; i < extents; i++) {
      platform_assert(
         allocator_dec_ref((allocator *)&al, ctx.extents[i], PAGE_TYPE_MISC)
         == AL_NO_REFS);
      platform_assert(
         allocator_dec_ref((allocator *)&al, ctx.extents[i], PAGE_TYPE_MISC)
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
