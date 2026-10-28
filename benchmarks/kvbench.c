// kvbench.c -- SplinterDB public-API benchmark used to compare two versions.
//
// Uses only splinterdb/splinterdb.h + splinterdb/default_data_config.h + libc,
// so the same source builds against versions whose internals differ. The one
// public-API difference handled here is splinterdb_close():
//   -DKVBENCH_CLOSE_HAS_FORCE  => int splinterdb_close(splinterdb **, bool32)
//   otherwise                  => void splinterdb_close(splinterdb **)
//
// Key scheme: logical id in [0,N). Present key = 8-byte big-endian(2*id)
// followed by 16 bytes of fixed padding (24 bytes total), so memcmp order ==
// id order. Absent ("negative") keys use 2*id+1, interleaved with present
// keys. Value = 100 bytes: id (LE u64), generation (LE u64), filler.
//
// Each invocation: create/open the DB, run the listed phases in order, close.
// Phase syntax (optional "@T" overrides the thread count):
//   load:random | load:seq        fixed-count insert of all N ids, then close
//                                 (close timed separately as "drain")
//   warm                          parallel full scan; verifies every id once
//   point:uniform|zipf|seq|neg    fixed-duration lookups
//   range:uniform|zipf:L          fixed-duration forward range queries
//   ow:uniform|zipf               fixed-duration overwrites (insert existing)
// One CSV row per phase is appended to --csv.

#ifndef _GNU_SOURCE
#   define _GNU_SOURCE
#endif
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"

#define KEY_SIZE    24
#define VALUE_SIZE  100
#define MAX_PHASES  64
#define MAX_WORKERS 32

static const char KEY_PAD[16] = "kvbench-padding!";

/* ------------------------------------------------------------------------ */
/* Time, RNG, permutations, zipf                                            */
/* ------------------------------------------------------------------------ */

static inline uint64_t
now_ns(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

/* Bijective 64-bit mixer (splitmix64 finalizer). */
static inline uint64_t
mix(uint64_t x)
{
   x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
   x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
   return x ^ (x >> 31);
}

static inline uint64_t
rng_next(uint64_t *s)
{
   *s += UINT64_C(0x9e3779b97f4a7c15);
   return mix(*s);
}

static inline double
rng_unit(uint64_t *s)
{
   return (double)(rng_next(s) >> 11) * 0x1.0p-53;
}

/*
 * Pseudo-random bijection on [0,n): a 4-round balanced Feistel network over
 * 2*half bits (2^(2*half) >= n), with cycle-walking to stay inside [0,n).
 */
typedef struct {
   uint64_t n;
   unsigned half;
   uint64_t mask;
   uint64_t keys[4];
} perm_t;

static void
perm_init(perm_t *p, uint64_t n, uint64_t seed)
{
   unsigned bits = 1;
   while (bits < 64 && (UINT64_C(1) << bits) < n) {
      bits++;
   }
   p->n    = n;
   p->half = (bits + 1) / 2;
   if (p->half < 1) {
      p->half = 1;
   }
   p->mask = (UINT64_C(1) << p->half) - 1;
   for (int r = 0; r < 4; r++) {
      p->keys[r] = mix(seed * UINT64_C(0x632be59bd9b4e019) + r + 1);
   }
}

static inline uint64_t
feistel(const perm_t *p, uint64_t x)
{
   uint64_t l = x >> p->half, r = x & p->mask;
   for (int i = 0; i < 4; i++) {
      uint64_t t = l ^ (mix(r ^ p->keys[i]) & p->mask);
      l          = r;
      r          = t;
   }
   return (l << p->half) | r;
}

static inline uint64_t
perm_apply(const perm_t *p, uint64_t i)
{
   uint64_t x = i;
   do {
      x = feistel(p, x);
   } while (x >= p->n);
   return x;
}

/* YCSB-style zipfian over ranks [0,n). */
typedef struct {
   uint64_t n;
   double   theta, zetan, zeta2, alpha, eta, half_pow_theta;
} zipf_t;

static double
zeta_sum(uint64_t n, double theta)
{
   uint64_t m = n < 2000000 ? n : 2000000;
   double   s = 0;
   for (uint64_t i = 1; i <= m; i++) {
      s += pow((double)i, -theta);
   }
   if (n > m) {
      /* Euler-Maclaurin tail: sum_{k=m+1}^{n} k^-t */
      s += (pow((double)n, 1 - theta) - pow((double)m, 1 - theta)) / (1 - theta)
           + 0.5 * (pow((double)n, -theta) - pow((double)m, -theta));
   }
   return s;
}

static void
zipf_init(zipf_t *z, uint64_t n, double theta)
{
   z->n     = n;
   z->theta = theta;
   z->zetan = zeta_sum(n, theta);
   z->zeta2 = 1.0 + pow(0.5, theta);
   z->alpha = 1.0 / (1.0 - theta);
   z->eta =
      (1.0 - pow(2.0 / (double)n, 1.0 - theta)) / (1.0 - z->zeta2 / z->zetan);
   z->half_pow_theta = pow(0.5, theta);
}

static inline uint64_t
zipf_next(const zipf_t *z, uint64_t *rng)
{
   double u  = rng_unit(rng);
   double uz = u * z->zetan;
   if (uz < 1.0) {
      return 0;
   }
   if (uz < 1.0 + z->half_pow_theta) {
      return 1;
   }
   uint64_t r =
      (uint64_t)((double)z->n * pow(z->eta * u - z->eta + 1.0, z->alpha));
   return r >= z->n ? z->n - 1 : r;
}

/* ------------------------------------------------------------------------ */
/* Histogram: log2 buckets with 32 linear sub-buckets (~3% resolution)       */
/* ------------------------------------------------------------------------ */

#define HSUB_BITS 5
#define HSUB      (1u << HSUB_BITS)
#define HBUCKETS  (64 * HSUB)

typedef struct {
   uint64_t b[HBUCKETS];
   uint64_t count, sum, max;
} hist_t;

static inline unsigned
hidx(uint64_t v)
{
   if (v < HSUB) {
      return (unsigned)v;
   }
   unsigned e   = 63 - __builtin_clzll(v);
   unsigned sub = (unsigned)((v >> (e - HSUB_BITS)) & (HSUB - 1));
   return (e - HSUB_BITS + 1) * HSUB + sub;
}

static inline uint64_t
hval(unsigned idx)
{
   if (idx < HSUB) {
      return idx;
   }
   unsigned e     = idx / HSUB + HSUB_BITS - 1;
   unsigned sub   = idx % HSUB;
   uint64_t lo    = (UINT64_C(1) << e) | ((uint64_t)sub << (e - HSUB_BITS));
   uint64_t width = UINT64_C(1) << (e - HSUB_BITS);
   return lo + width / 2;
}

static inline void
hist_add(hist_t *h, uint64_t v)
{
   h->b[hidx(v)]++;
   h->count++;
   h->sum += v;
   if (v > h->max) {
      h->max = v;
   }
}

static void
hist_merge(hist_t *dst, const hist_t *src)
{
   for (unsigned i = 0; i < HBUCKETS; i++) {
      dst->b[i] += src->b[i];
   }
   dst->count += src->count;
   dst->sum += src->sum;
   if (src->max > dst->max) {
      dst->max = src->max;
   }
}

static double
hist_pct_us(const hist_t *h, double p)
{
   if (!h->count) {
      return NAN;
   }
   uint64_t rank = (uint64_t)ceil(p * (double)h->count);
   if (rank < 1) {
      rank = 1;
   }
   uint64_t acc = 0;
   for (unsigned i = 0; i < HBUCKETS; i++) {
      acc += h->b[i];
      if (acc >= rank) {
         uint64_t v = hval(i);
         return (double)(v > h->max ? h->max : v) / 1e3;
      }
   }
   return (double)h->max / 1e3;
}

static double
hist_mean_us(const hist_t *h)
{
   return h->count ? (double)h->sum / (double)h->count / 1e3 : NAN;
}

/* ------------------------------------------------------------------------ */
/* Options / global state                                                    */
/* ------------------------------------------------------------------------ */

enum phase_kind { PH_LOAD, PH_WARM, PH_POINT, PH_RANGE, PH_OW };
enum dist { D_UNIFORM, D_ZIPF, D_SEQ, D_NEG, D_RANDOM };

typedef struct {
   char            name[64];
   enum phase_kind kind;
   enum dist       dist;
   uint64_t        L;
   int             threads;
} phase_t;

typedef struct {
   const char *db, *csv, *hang_dir;
   const char *vtag, *stag, *otag;
   int         rep;
   bool        create;
   uint64_t    n, cache_mib, disk_gib, memtable_mib;
   int         use_log, threads, mt_bg, normal_bg;
   double      secs, warmup;
   uint64_t    sabotage;
   uint64_t    hang_secs;
   uint64_t    seed;
   int         nphases;
   phase_t     phases[MAX_PHASES];
} options;

static options     opt;
static splinterdb *kvs;
static data_config data_cfg;
static perm_t      load_perm, zipf_perm;
static zipf_t      zipf;
static uint64_t    generation; /* value generation for overwrites */

enum { ST_WAIT = 0, ST_WARMUP = 1, ST_MEASURE = 2, ST_STOP = 3 };
static _Atomic int      run_state;
static _Atomic uint64_t seq_counter;

/* Watchdog */
enum { WD_IDLE, WD_OPS, WD_OPENCLOSE };
static _Atomic int      wd_mode;
static _Atomic uint64_t wd_mode_since_ns;
static const char      *wd_what = "";

typedef struct worker {
   pthread_t        th;
   int              idx;
   const phase_t   *ph;
   hist_t           lat;
   hist_t           startup;
   uint64_t         ops, tuples, pt_ns, pt_div, deinit_ns, deinit_n;
   uint64_t         violations, errors, notfound, found;
   uint64_t         rng;
   uint64_t         measure_rng;
   bool             measure_rng_captured;
   _Atomic uint64_t progress;
   char             first_violation[256];
} worker_t;

static worker_t *workers;
static int       nworkers;

/* ------------------------------------------------------------------------ */
/* Keys / values                                                             */
/* ------------------------------------------------------------------------ */

static inline void
make_key(uint64_t k, char *key)
{
   for (int i = 0; i < 8; i++) {
      key[i] = (char)(k >> (56 - 8 * i));
   }
   memcpy(key + 8, KEY_PAD, 16);
}

static inline bool
decode_key(slice s, uint64_t *k)
{
   if (slice_length(s) != KEY_SIZE) {
      return false;
   }
   const unsigned char *p = slice_data(s);
   uint64_t             x = 0;
   for (int i = 0; i < 8; i++) {
      x = (x << 8) | p[i];
   }
   *k = x;
   return memcmp(p + 8, KEY_PAD, 16) == 0;
}

static inline void
make_value(uint64_t id, uint64_t gen, char *val)
{
   memcpy(val, &id, 8);
   memcpy(val + 8, &gen, 8);
   memset(val + 16, 'v', VALUE_SIZE - 16);
}

static inline bool
value_id_ok(slice v, uint64_t id)
{
   if (slice_length(v) != VALUE_SIZE) {
      return false;
   }
   uint64_t got;
   memcpy(&got, slice_data(v), 8);
   return got == id;
}

static inline bool
value_generation_ok(slice v, uint64_t id, uint64_t expected_generation)
{
   if (!value_id_ok(v, id)) {
      return false;
   }
   uint64_t got_generation;
   memcpy(&got_generation, (const char *)slice_data(v) + 8, 8);
   return got_generation == expected_generation;
}

static void
violation(worker_t *w, const char *fmt, ...)
   __attribute__((format(printf, 2, 3)));

static void
violation(worker_t *w, const char *fmt, ...)
{
   w->violations++;
   if (w->violations <= 3) {
      char    buf[256];
      va_list ap;
      va_start(ap, fmt);
      vsnprintf(buf, sizeof(buf), fmt, ap);
      va_end(ap);
      if (w->violations == 1) {
         snprintf(w->first_violation, sizeof(w->first_violation), "%s", buf);
      }
      fprintf(stderr, "VIOLATION [thread %d]: %s\n", w->idx, buf);
   }
}

/* ------------------------------------------------------------------------ */
/* Process metrics                                                           */
/* ------------------------------------------------------------------------ */

typedef struct {
   uint64_t t_ns;
   double   cpu_s;
   uint64_t rd, wr;
   uint64_t swpin, swpout;
   uint64_t majflt;
} snap_t;

static void
proc_io(uint64_t *rd, uint64_t *wr)
{
   *rd = *wr = 0;
   FILE *f   = fopen("/proc/self/io", "r");
   if (!f) {
      return;
   }
   char     name[64];
   uint64_t v;
   while (fscanf(f, "%63s %" SCNu64, name, &v) == 2) {
      if (!strcmp(name, "read_bytes:")) {
         *rd = v;
      } else if (!strcmp(name, "write_bytes:")) {
         *wr = v;
      }
   }
   fclose(f);
}

static void
vm_swap(uint64_t *in, uint64_t *out)
{
   *in = *out = 0;
   FILE *f    = fopen("/proc/vmstat", "r");
   if (!f) {
      return;
   }
   char     name[64];
   uint64_t v;
   while (fscanf(f, "%63s %" SCNu64, name, &v) == 2) {
      if (!strcmp(name, "pswpin")) {
         *in = v;
      } else if (!strcmp(name, "pswpout")) {
         *out = v;
      }
   }
   fclose(f);
}

static void
take_snap(snap_t *s)
{
   struct rusage ru;
   getrusage(RUSAGE_SELF, &ru);
   s->cpu_s = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6
              + ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;
   s->majflt = ru.ru_majflt;
   proc_io(&s->rd, &s->wr);
   vm_swap(&s->swpin, &s->swpout);
   s->t_ns = now_ns();
}

/* ------------------------------------------------------------------------ */
/* DB lifecycle                                                              */
/* ------------------------------------------------------------------------ */

static void
fill_config(splinterdb_config *cfg)
{
   *cfg = (splinterdb_config){
      .filename                = opt.db,
      .cache_size              = opt.cache_mib << 20,
      .disk_size               = opt.disk_gib << 30,
      .data_cfg                = &data_cfg,
      .io_flags                = O_RDWR | O_CREAT | O_DIRECT,
      .io_perms                = 0644,
      .use_log                 = opt.use_log ? 1 : 0,
      .memtable_capacity       = opt.memtable_mib << 20,
      .fanout                  = 8,
      .num_memtable_bg_threads = (uint64)opt.mt_bg,
      .num_normal_bg_threads   = (uint64)opt.normal_bg,
      .queue_scale_percent     = 100,
   };
#ifdef KVBENCH_SPACE_MODE
   cfg->use_stats = 1;
#endif
}

static double open_secs;

static void
db_open(bool create)
{
   splinterdb_config cfg;
   fill_config(&cfg);
   wd_what = create ? "create" : "open";
   atomic_store(&wd_mode_since_ns, now_ns());
   atomic_store(&wd_mode, WD_OPENCLOSE);
   uint64_t t0 = now_ns();
   int      rc =
      create ? splinterdb_create(&cfg, &kvs) : splinterdb_open(&cfg, &kvs);
   open_secs = (now_ns() - t0) / 1e9;
   atomic_store(&wd_mode, WD_IDLE);
   if (rc) {
      fprintf(stderr, "FATAL: splinterdb_%s failed rc=%d\n", wd_what, rc);
      exit(3);
   }
   fprintf(stderr, "%s took %.3f s\n", wd_what, open_secs);
}

static double
db_close(void)
{
#ifdef KVBENCH_SPACE_MODE
   /* Space/stats build only: dump insertion + allocator stats for the session.
    */
   if (getenv("KVBENCH_STATS_BEFORE_CLOSE")) {
      platform_set_log_streams(stdout, stderr);
      splinterdb_stats_print_insertion(kvs);
      fflush(stdout);
   }
#endif
   wd_what = "close";
   atomic_store(&wd_mode_since_ns, now_ns());
   atomic_store(&wd_mode, WD_OPENCLOSE);
   uint64_t t0 = now_ns();
#ifdef KVBENCH_CLOSE_HAS_FORCE
   int rc = splinterdb_close(&kvs, 0);
   if (rc) {
      fprintf(stderr,
              "FINDING: splinterdb_close(force=0) refused rc=%d; forcing\n",
              rc);
      rc = splinterdb_close(&kvs, 1);
      if (rc) {
         fprintf(stderr, "FINDING: splinterdb_close(force=1) rc=%d\n", rc);
      }
   }
#else
   splinterdb_close(&kvs);
#endif
   double s = (now_ns() - t0) / 1e9;
   atomic_store(&wd_mode, WD_IDLE);
   kvs = NULL;
   fprintf(stderr, "close took %.3f s\n", s);
   return s;
}

/* ------------------------------------------------------------------------ */
/* Watchdog                                                                  */
/* ------------------------------------------------------------------------ */

static void
hang_dump(const char *why)
{
   char path[512], cmd[1024];
   snprintf(path,
            sizeof(path),
            "%s/hang-%s-%s-%d-%ld.txt",
            opt.hang_dir ? opt.hang_dir : ".",
            opt.vtag,
            wd_what,
            (int)getpid(),
            (long)time(NULL));
   fprintf(stderr,
           "HANG DETECTED (%s) during %s; dumping stacks to %s\n",
           why,
           wd_what,
           path);
   snprintf(cmd,
            sizeof(cmd),
            "gdb -batch -ex 'info threads' -ex 'thread apply all bt' -p %d > "
            "'%s' 2>&1",
            (int)getpid(),
            path);
   int rc = system(cmd);
   fprintf(stderr, "gdb rc=%d; exiting with status 99\n", rc);
   fflush(stderr);
   _exit(99);
}

static void *
watchdog_main(void *arg)
{
   (void)arg;
   uint64_t last_prog = 0, last_change = now_ns(), last_report = now_ns();
   uint64_t last_report_prog = 0;
   double   last_cpu         = -1;
   uint64_t last_io          = 0;
   int      last_mode        = WD_IDLE;
   for (;;) {
      sleep(1);
      int      mode = atomic_load(&wd_mode);
      uint64_t now  = now_ns();
      if (mode != last_mode) {
         last_mode        = mode;
         last_change      = now;
         last_prog        = 0;
         last_report_prog = 0;
         last_report      = now;
         last_cpu         = -1;
      }
      if (mode == WD_OPS) {
         uint64_t prog = 0;
         for (int i = 0; i < nworkers; i++) {
            prog +=
               atomic_load_explicit(&workers[i].progress, memory_order_relaxed);
         }
         if (prog != last_prog) {
            last_prog   = prog;
            last_change = now;
         } else if (now - last_change > opt.hang_secs * 1000000000ull) {
            hang_dump("no op progress for hang_secs");
         }
         if (now - last_report >= 10ull * 1000000000ull) {
            fprintf(stderr,
                    "  [%s] progress %" PRIu64 " ops (+%.0f ops/s)\n",
                    wd_what,
                    prog,
                    (prog - last_report_prog) / ((now - last_report) / 1e9));
            last_report      = now;
            last_report_prog = prog;
         }
      } else if (mode == WD_OPENCLOSE) {
         snap_t s;
         take_snap(&s);
         uint64_t io = s.rd + s.wr;
         if (last_cpu < 0 || s.cpu_s - last_cpu > 0.05 || io != last_io) {
            last_cpu    = s.cpu_s;
            last_io     = io;
            last_change = now;
         } else if (now - last_change > opt.hang_secs * 1000000000ull) {
            hang_dump("no CPU or IO progress for hang_secs");
         }
         uint64_t since = atomic_load(&wd_mode_since_ns);
         if (now - since > 2400ull * 1000000000ull) {
            hang_dump("open/close exceeded 40 minutes");
         }
      }
   }
   return NULL;
}

/* ------------------------------------------------------------------------ */
/* Worker bodies                                                             */
/* ------------------------------------------------------------------------ */

static inline uint64_t
pick_id(worker_t *w, enum dist d, uint64_t *seqpos, uint64_t lo, uint64_t hi)
{
   switch (d) {
      case D_ZIPF:
         return perm_apply(&zipf_perm, zipf_next(&zipf, &w->rng));
      case D_SEQ:
      {
         uint64_t id = *seqpos;
         *seqpos     = (id + 1 >= hi) ? lo : id + 1;
         return id;
      }
      case D_UNIFORM:
      case D_NEG:
      default:
         return rng_next(&w->rng) % opt.n;
   }
}

static inline void
wait_start(void)
{
   while (atomic_load_explicit(&run_state, memory_order_acquire) == ST_WAIT) {
      struct timespec d = {.tv_nsec = 100000};
      nanosleep(&d, NULL);
   }
}

static void *
load_worker(void *arg)
{
   worker_t *w = arg;
   char      key[KEY_SIZE], val[VALUE_SIZE];
   uint64_t  T  = (uint64_t)nworkers;
   uint64_t  lo = opt.n * (uint64_t)w->idx / T,
            hi  = opt.n * (uint64_t)(w->idx + 1) / T;
   wait_start();
   uint64_t pos = lo, chunk_end = 0, cur = 0;
   for (;;) {
      uint64_t id;
      if (w->ph->dist == D_RANDOM) {
         if (pos >= hi) {
            break;
         }
         id = perm_apply(&load_perm, pos++);
      } else {
         if (cur >= chunk_end) {
            cur = atomic_fetch_add(&seq_counter, 64);
            if (cur >= opt.n) {
               break;
            }
            chunk_end = cur + 64 > opt.n ? opt.n : cur + 64;
         }
         id = cur++;
      }
      make_key(2 * id, key);
      make_value(id, 0, val);
      uint64_t t0 = now_ns();
      int      rc = splinterdb_insert(kvs,
                                 slice_create(KEY_SIZE, key),
                                 message_create(VALUE_SIZE, val),
                                 NULL);
      uint64_t t1 = now_ns();
      if (rc) {
         w->errors++;
         if (w->errors <= 3) {
            fprintf(stderr, "ERROR: insert id=%" PRIu64 " rc=%d\n", id, rc);
         }
      }
      hist_add(&w->lat, t1 - t0);
      w->ops++;
      atomic_store_explicit(&w->progress, w->ops, memory_order_relaxed);
   }
   return NULL;
}

static void *
warm_worker(void *arg)
{
   worker_t *w  = arg;
   uint64_t  T  = (uint64_t)nworkers;
   uint64_t  lo = opt.n * (uint64_t)w->idx / T,
            hi  = opt.n * (uint64_t)(w->idx + 1) / T;
   if ((opt.sabotage & 8) && w->idx == nworkers - 1) {
      hi++; /* sabotage: expect one more id than exists */
   }
   char key[KEY_SIZE];
   make_key(2 * lo, key);
   wait_start();
   splinterdb_iterator *it;
   int                  rc =
      splinterdb_iterator_init(kvs,
                               &it,
                               .start_position = splinterdb_start_at_or_after(
                                  slice_create(KEY_SIZE, key)));
   if (rc) {
      w->errors++;
      fprintf(stderr, "ERROR: warm iterator_init rc=%d\n", rc);
      return NULL;
   }
   uint64_t expect = lo;
   while (expect < hi && splinterdb_iterator_valid(it)) {
      slice    k, v;
      uint64_t kk;
      splinterdb_iterator_get_current(it, &k, &v);
      if (!decode_key(k, &kk) || kk != 2 * expect || !value_id_ok(v, expect)) {
         violation(w,
                   "warm: expected id %" PRIu64 " got key %" PRIu64
                   " (len %" PRIu64 ")",
                   expect,
                   kk,
                   slice_length(k));
         if (kk > 2 * expect && (kk & 1) == 0) {
            expect = kk / 2; /* resync after a gap */
         }
      }
      expect++;
      w->ops++;
      if ((w->ops & 1023) == 0) {
         atomic_store_explicit(&w->progress, w->ops, memory_order_relaxed);
      }
      if (expect < hi) {
         splinterdb_iterator_next(it);
      }
   }
   rc = splinterdb_iterator_status(it);
   if (rc) {
      w->errors++;
      fprintf(stderr, "ERROR: warm iterator status rc=%d\n", rc);
   }
   if (expect != hi) {
      violation(w,
                "warm: range [%" PRIu64 ",%" PRIu64 ") ended at %" PRIu64,
                lo,
                hi,
                expect);
   }
   splinterdb_iterator_deinit(it);
   atomic_store_explicit(&w->progress, w->ops, memory_order_relaxed);
   return NULL;
}

static void *
point_worker(void *arg)
{
   worker_t                *w = arg;
   char                     key[KEY_SIZE];
   char                     buf[256];
   splinterdb_lookup_result res;
   splinterdb_lookup_result_init(
      kvs, &res, .buffer = slice_create(sizeof(buf), buf));
   uint64_t T      = (uint64_t)nworkers;
   uint64_t lo     = opt.n * (uint64_t)w->idx / T,
            hi     = opt.n * (uint64_t)(w->idx + 1) / T;
   uint64_t seqpos = lo;
   bool     neg    = w->ph->dist == D_NEG;
   wait_start();
   uint64_t n = 0;
   for (;;) {
      int st = atomic_load_explicit(&run_state, memory_order_relaxed);
      if (st == ST_STOP) {
         break;
      }
      uint64_t id    = pick_id(w, w->ph->dist, &seqpos, lo, hi);
      bool     q_neg = neg;
      bool     e_neg = neg;
      if (opt.sabotage & 1) {
         q_neg = true; /* sabotage: query absent key, expect present */
      }
      if ((opt.sabotage & 16) && n > 1000) {
         sleep(100000); /* test hook: simulate a hang for the watchdog */
      }
      if (opt.sabotage & 2) {
         q_neg = false; /* sabotage: query present key, expect absent */
      }
      make_key(q_neg ? 2 * id + 1 : 2 * id, key);
      uint64_t t0 = now_ns();
      int      rc = splinterdb_lookup(kvs, slice_create(KEY_SIZE, key), &res);
      uint64_t t1 = now_ns();
      n++;
      atomic_store_explicit(&w->progress, n, memory_order_relaxed);
      if (st != ST_MEASURE) {
         continue;
      }
      w->ops++;
      hist_add(&w->lat, t1 - t0);
      if (rc) {
         w->errors++;
         if (w->errors <= 3) {
            fprintf(stderr, "ERROR: lookup id=%" PRIu64 " rc=%d\n", id, rc);
         }
         continue;
      }
      bool found = splinterdb_lookup_found(&res);
      if (found) {
         w->found++;
      } else {
         w->notfound++;
      }
      if (e_neg) {
         if (found) {
            violation(w, "negative key 2*%" PRIu64 "+1 found", id);
         }
      } else {
         slice v;
         if (!found) {
            violation(w, "present id %" PRIu64 " not found", id);
         } else if (splinterdb_lookup_result_value(&res, &v) != 0
                    || !value_id_ok(v, id))
         {
            violation(w, "present id %" PRIu64 " wrong value", id);
         }
      }
   }
   splinterdb_lookup_result_deinit(&res);
   return NULL;
}

static void *
range_worker(void *arg)
{
   worker_t *w = arg;
   char      key[KEY_SIZE];
   uint64_t  L      = w->ph->L;
   uint64_t  seqpos = 0;
   uint64_t  n      = 0;
   wait_start();
   for (;;) {
      int st = atomic_load_explicit(&run_state, memory_order_relaxed);
      if (st == ST_STOP) {
         break;
      }
      uint64_t start  = pick_id(w, w->ph->dist, &seqpos, 0, opt.n);
      uint64_t expect = opt.n - start < L ? opt.n - start : L;
      if (opt.sabotage & 4) {
         expect++; /* sabotage: expect one extra tuple */
      }
      make_key(2 * start, key);
      uint64_t             t0 = now_ns(), t1 = 0, tl = 0;
      splinterdb_iterator *it;
      int                  rc = splinterdb_iterator_init(
         kvs,
         &it,
         .start_position =
            splinterdb_start_at_or_after(slice_create(KEY_SIZE, key)));
      if (rc) {
         w->errors++;
         if (w->errors <= 3) {
            fprintf(stderr, "ERROR: iterator_init rc=%d\n", rc);
         }
         continue;
      }
      uint64_t got = 0;
      bool     bad = false;
      while (got < L && splinterdb_iterator_valid(it)) {
         slice    k, v;
         uint64_t kk = 0;
         splinterdb_iterator_get_current(it, &k, &v);
         if (!bad
             && (!decode_key(k, &kk) || kk != 2 * (start + got)
                 || !value_id_ok(v, start + got)))
         {
            bad = true;
            if (st == ST_MEASURE) {
               violation(w,
                         "range start %" PRIu64 " tuple %" PRIu64
                         ": expected key %" PRIu64 " got %" PRIu64,
                         start,
                         got,
                         2 * (start + got),
                         kk);
            }
         }
         got++;
         if (got == 1) {
            t1 = now_ns();
         }
         if (got == L) {
            tl = now_ns();
            break;
         }
         splinterdb_iterator_next(it);
      }
      int      status = splinterdb_iterator_status(it);
      uint64_t t2     = now_ns();
      splinterdb_iterator_deinit(it);
      uint64_t t3 = now_ns();
      n++;
      atomic_store_explicit(&w->progress, n, memory_order_relaxed);
      if (st != ST_MEASURE) {
         continue;
      }
      if (status) {
         w->errors++;
         if (w->errors <= 3) {
            fprintf(stderr, "ERROR: iterator status rc=%d\n", status);
         }
      }
      if (got != expect) {
         violation(w,
                   "range start %" PRIu64 " L=%" PRIu64 ": got %" PRIu64
                   " tuples, expected %" PRIu64,
                   start,
                   L,
                   got,
                   expect);
      }
      w->ops++;
      w->tuples += got;
      hist_add(&w->lat, t3 - t0);
      if (got >= 1) {
         hist_add(&w->startup, t1 - t0);
      }
      if (L > 1 && got == L) {
         w->pt_ns += tl - t1;
         w->pt_div += L - 1;
      }
      w->deinit_ns += t3 - t2;
      w->deinit_n++;
   }
   return NULL;
}

static void *
ow_worker(void *arg)
{
   worker_t *w = arg;
   char      key[KEY_SIZE], val[VALUE_SIZE];
   uint64_t  seqpos = 0, n = 0;
   wait_start();
   for (;;) {
      int st = atomic_load_explicit(&run_state, memory_order_relaxed);
      if (st == ST_STOP) {
         break;
      }
      if (st == ST_MEASURE && !w->measure_rng_captured) {
         w->measure_rng          = w->rng;
         w->measure_rng_captured = true;
      }
      uint64_t id = pick_id(w, w->ph->dist, &seqpos, 0, opt.n);
      make_key(2 * id, key);
      make_value(id, generation, val);
      uint64_t t0 = now_ns();
      int      rc = splinterdb_insert(kvs,
                                 slice_create(KEY_SIZE, key),
                                 message_create(VALUE_SIZE, val),
                                 NULL);
      uint64_t t1 = now_ns();
      n++;
      atomic_store_explicit(&w->progress, n, memory_order_relaxed);
      if (rc) {
         w->errors++;
         if (w->errors <= 3) {
            fprintf(stderr, "ERROR: overwrite id=%" PRIu64 " rc=%d\n", id, rc);
         }
      }
      if (st != ST_MEASURE) {
         continue;
      }
      w->ops++;
      hist_add(&w->lat, t1 - t0);
   }
   return NULL;
}

/*
 * Validate an untimed deterministic sample from the beginning of each
 * overwrite worker's measured stream. Replaying the saved RNG state keeps all
 * validation work out of the measured hot path.
 */
static void
verify_overwrite_sample(const phase_t *ph, int threads)
{
   enum { SAMPLES_PER_WORKER = 4096 };
   char                     key[KEY_SIZE], buffer[VALUE_SIZE];
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(
      kvs, &result, .buffer = slice_create(sizeof(buffer), buffer));

   for (int i = 0; i < threads; i++) {
      worker_t *original = &workers[i];
      if (original->ops == 0) {
         continue;
      }
      if (!original->measure_rng_captured) {
         original->errors++;
         fprintf(
            stderr,
            "ERROR: overwrite verifier has no measurement RNG for thread %d\n",
            i);
         continue;
      }
      worker_t replay  = {.rng = original->measure_rng};
      uint64_t seqpos  = 0;
      uint64_t samples = original->ops < SAMPLES_PER_WORKER
                            ? original->ops
                            : SAMPLES_PER_WORKER;
      for (uint64_t sample = 0; sample < samples; sample++) {
         uint64_t id = pick_id(&replay, ph->dist, &seqpos, 0, opt.n);
         make_key(2 * id, key);
         int rc = splinterdb_lookup(kvs, slice_create(KEY_SIZE, key), &result);
         if (rc != 0) {
            original->errors++;
            if (original->errors <= 3) {
               fprintf(stderr,
                       "ERROR: overwrite verification id=%" PRIu64 " rc=%d\n",
                       id,
                       rc);
            }
            continue;
         }
         slice value;
         if (!splinterdb_lookup_found(&result)
             || splinterdb_lookup_result_value(&result, &value) != 0
             || !value_generation_ok(value, id, generation))
         {
            violation(original,
                      "overwrite verification id=%" PRIu64
                      " did not return generation %" PRIu64,
                      id,
                      generation);
         }
      }
   }
   splinterdb_lookup_result_deinit(&result);
}

/* ------------------------------------------------------------------------ */
/* Phase driver                                                              */
/* ------------------------------------------------------------------------ */

static const char *
dist_name(enum dist d)
{
   switch (d) {
      case D_UNIFORM:
         return "uniform";
      case D_ZIPF:
         return "zipf";
      case D_SEQ:
         return "seq";
      case D_NEG:
         return "neg";
      case D_RANDOM:
         return "random";
   }
   return "?";
}

static void
sleep_secs(double s)
{
   uint64_t end = now_ns() + (uint64_t)(s * 1e9);
   for (;;) {
      uint64_t now = now_ns();
      if (now >= end) {
         break;
      }
      uint64_t        rem = end - now;
      struct timespec d   = {.tv_sec  = rem / 1000000000ull,
                             .tv_nsec = rem % 1000000000ull};
      nanosleep(&d, NULL);
   }
}

static void
file_sizes(uint64_t *apparent, uint64_t *alloc)
{
   struct stat st;
   if (stat(opt.db, &st) == 0) {
      *apparent = (uint64_t)st.st_size;
      *alloc    = (uint64_t)st.st_blocks * 512;
   } else {
      *apparent = *alloc = 0;
   }
}

static int csv_header_written;

static void
csv_row(const phase_t *ph,
        int            threads,
        hist_t        *lat,
        hist_t        *startup,
        uint64_t       ops,
        uint64_t       tuples,
        uint64_t       pt_ns,
        uint64_t       pt_div,
        uint64_t       deinit_ns,
        uint64_t       deinit_n,
        uint64_t       viol,
        uint64_t       errs,
        const snap_t  *a,
        const snap_t  *b,
        double         insert_s,
        double         drain_s,
        const snap_t  *da,
        const snap_t  *db_,
        uint64_t       app,
        uint64_t       alc)
{
   double   secs = (b->t_ns - a->t_ns) / 1e9;
   double   cpu  = b->cpu_s - a->cpu_s;
   uint64_t rd = b->rd - a->rd, wr = b->wr - a->wr;
   double   d_cpu = da ? db_->cpu_s - da->cpu_s : 0;
   uint64_t d_rd = da ? db_->rd - da->rd : 0, d_wr = da ? db_->wr - da->wr : 0;
   uint64_t swin  = b->swpin - a->swpin + (da ? db_->swpin - da->swpin : 0);
   uint64_t swout = b->swpout - a->swpout + (da ? db_->swpout - da->swpout : 0);
   uint64_t majflt  = b->majflt - a->majflt;
   double   logical = (double)ops * (KEY_SIZE + VALUE_SIZE);
   double   wamp    = (ph->kind == PH_LOAD || ph->kind == PH_OW) && ops
                         ? (double)(wr + d_wr) / logical
                         : NAN;
   double   pt      = pt_div ? (double)pt_ns / (double)pt_div : NAN;
   double   dmean = deinit_n ? (double)deinit_ns / (double)deinit_n / 1e3 : NAN;

   printf("== %-22s T=%-2d ops=%-10" PRIu64 " secs=%7.2f ops/s=%11.0f  "
          "lat us p50=%.2f p99=%.2f p99.9=%.2f max=%.1f mean=%.2f\n",
          ph->name,
          threads,
          ops,
          secs,
          ops / secs,
          hist_pct_us(lat, 0.5),
          hist_pct_us(lat, 0.99),
          hist_pct_us(lat, 0.999),
          (double)lat->max / 1e3,
          hist_mean_us(lat));
   printf("   cpu=%.2fs ops/cpu-s=%.0f rd=%.1f B/op wr=%.1f B/op "
          "violations=%" PRIu64 " errors=%" PRIu64 " swapin=%" PRIu64
          " swapout=%" PRIu64 " majflt=%" PRIu64 "\n",
          cpu,
          ops / cpu,
          ops ? (double)rd / ops : 0,
          ops ? (double)wr / ops : 0,
          viol,
          errs,
          swin,
          swout,
          majflt);
   if (ph->kind == PH_RANGE) {
      printf("   startup us p50=%.2f p99=%.2f mean=%.2f  per-tuple=%.1f ns  "
             "deinit mean=%.2f us  tuples/s=%.0f\n",
             hist_pct_us(startup, 0.5),
             hist_pct_us(startup, 0.99),
             hist_mean_us(startup),
             pt,
             dmean,
             tuples / secs);
   }
   if (da) {
      printf(
         "   insert/run=%.2fs drain(close)=%.2fs total=%.2fs drain_cpu=%.2fs "
         "drain_rd=%" PRIu64 " drain_wr=%" PRIu64 " wamp=%.3f "
         "db_apparent=%" PRIu64 " db_alloc=%" PRIu64 "\n",
         insert_s,
         drain_s,
         insert_s + drain_s,
         d_cpu,
         d_rd,
         d_wr,
         wamp,
         app,
         alc);
   }
   fflush(stdout);

   if (!opt.csv) {
      return;
   }
   FILE *f = fopen(opt.csv, "a");
   if (!f) {
      perror(opt.csv);
      return;
   }
   if (!csv_header_written) {
      fseek(f, 0, SEEK_END);
      if (ftell(f) == 0) {
         fprintf(f,
                 "version,size,order,log,phase,threads,rep,n,ops,secs,ops_s,"
                 "p50_us,p99_us,p999_us,max_us,mean_us,cpu_s,ops_per_cpu_s,"
                 "rd_bytes,wr_bytes,rd_B_op,wr_B_op,"
                 "startup_p50_us,startup_p99_us,startup_mean_us,per_tuple_ns,"
                 "deinit_mean_us,tuples,tuples_s,"
                 "insert_s,drain_s,total_s,drain_cpu_s,drain_rd,drain_wr,wamp,"
                 "db_apparent,db_alloc,open_s,violations,errors,swapin,swapout,"
                 "majflt\n");
      }
      csv_header_written = 1;
   }
   fprintf(f,
           "%s,%s,%s,%d,%s,%d,%d,%" PRIu64 ",%" PRIu64 ",%.3f,%.1f,"
           "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.1f,"
           "%" PRIu64 ",%" PRIu64 ",%.2f,%.2f,"
           "%.3f,%.3f,%.3f,%.2f,"
           "%.3f,%" PRIu64 ",%.1f,"
           "%.3f,%.3f,%.3f,%.3f,%" PRIu64 ",%" PRIu64 ",%.4f,"
           "%" PRIu64 ",%" PRIu64 ",%.3f,%" PRIu64 ",%" PRIu64 ",%" PRIu64
           ",%" PRIu64 ",%" PRIu64 "\n",
           opt.vtag,
           opt.stag,
           opt.otag,
           opt.use_log,
           ph->name,
           threads,
           opt.rep,
           opt.n,
           ops,
           secs,
           ops / secs,
           hist_pct_us(lat, 0.5),
           hist_pct_us(lat, 0.99),
           hist_pct_us(lat, 0.999),
           (double)lat->max / 1e3,
           hist_mean_us(lat),
           cpu,
           ops / cpu,
           rd,
           wr,
           ops ? (double)rd / ops : 0,
           ops ? (double)wr / ops : 0,
           hist_pct_us(startup, 0.5),
           hist_pct_us(startup, 0.99),
           hist_mean_us(startup),
           pt,
           dmean,
           tuples,
           tuples / secs,
           insert_s,
           drain_s,
           insert_s + drain_s,
           d_cpu,
           d_rd,
           d_wr,
           wamp,
           app,
           alc,
           open_secs,
           viol,
           errs,
           swin,
           swout,
           majflt);
   fclose(f);
}

static void
run_phase(const phase_t *ph, bool last)
{
   int T    = ph->threads > 0 ? ph->threads : opt.threads;
   nworkers = T;
   workers  = calloc((size_t)T, sizeof(worker_t));
   if (!workers) {
      fprintf(stderr, "FATAL: out of memory\n");
      exit(3);
   }
   void *(*fn)(void *) = NULL;
   switch (ph->kind) {
      case PH_LOAD:
         fn = load_worker;
         break;
      case PH_WARM:
         fn = warm_worker;
         break;
      case PH_POINT:
         fn = point_worker;
         break;
      case PH_RANGE:
         fn = range_worker;
         break;
      case PH_OW:
         fn = ow_worker;
         break;
   }
   bool fixed_count = ph->kind == PH_LOAD || ph->kind == PH_WARM;
   atomic_store(&run_state, ST_WAIT);
   atomic_store(&seq_counter, 0);
   for (int i = 0; i < T; i++) {
      workers[i].idx = i;
      workers[i].ph  = ph;
      workers[i].rng = mix(opt.seed * 1000003 + (uint64_t)i * 7919 + 17
                           + (uint64_t)ph->kind * 131 + (uint64_t)ph->dist);
      if (pthread_create(&workers[i].th, NULL, fn, &workers[i])) {
         fprintf(stderr, "FATAL: pthread_create\n");
         exit(3);
      }
   }
   fprintf(stderr,
           "phase %s: T=%d dist=%s %s\n",
           ph->name,
           T,
           dist_name(ph->dist),
           fixed_count ? "(fixed count)" : "(fixed duration)");
   wd_what = ph->name;
   atomic_store(&wd_mode, WD_OPS);
   snap_t a, b;
   if (fixed_count) {
      take_snap(&a);
      atomic_store_explicit(&run_state, ST_MEASURE, memory_order_release);
      for (int i = 0; i < T; i++) {
         pthread_join(workers[i].th, NULL);
      }
      take_snap(&b);
   } else {
      atomic_store_explicit(&run_state, ST_WARMUP, memory_order_release);
      sleep_secs(opt.warmup);
      take_snap(&a);
      atomic_store_explicit(&run_state, ST_MEASURE, memory_order_release);
      sleep_secs(opt.secs);
      atomic_store_explicit(&run_state, ST_STOP, memory_order_release);
      take_snap(&b);
      for (int i = 0; i < T; i++) {
         pthread_join(workers[i].th, NULL);
      }
   }
   atomic_store(&wd_mode, WD_IDLE);

   if (ph->kind == PH_OW) {
      verify_overwrite_sample(ph, T);
   }

   hist_t *lat     = calloc(1, sizeof(hist_t)),
          *startup = calloc(1, sizeof(hist_t));
   uint64_t ops = 0, tuples = 0, pt_ns = 0, pt_div = 0, dns = 0, dn = 0,
            viol = 0, errs = 0, found = 0, notfound = 0;
   for (int i = 0; i < T; i++) {
      worker_t *w = &workers[i];
      hist_merge(lat, &w->lat);
      hist_merge(startup, &w->startup);
      ops += w->ops;
      tuples += w->tuples;
      pt_ns += w->pt_ns;
      pt_div += w->pt_div;
      dns += w->deinit_ns;
      dn += w->deinit_n;
      viol += w->violations;
      errs += w->errors;
      found += w->found;
      notfound += w->notfound;
   }
   if (ph->kind == PH_WARM) {
      tuples = ops;
      if (ops != opt.n && !(opt.sabotage & 8)) {
         fprintf(stderr,
                 "VIOLATION: warm scanned %" PRIu64 " tuples, expected %" PRIu64
                 "\n",
                 ops,
                 opt.n);
         viol++;
      }
   }
   if (ph->kind == PH_POINT) {
      fprintf(stderr,
              "   found=%" PRIu64 " notfound=%" PRIu64 "\n",
              found,
              notfound);
   }

   double   insert_s = (b.t_ns - a.t_ns) / 1e9, drain_s = 0;
   snap_t   da, dbs;
   uint64_t app = 0, alc = 0;
   bool     closed = false;
   if (ph->kind == PH_LOAD || last) {
      take_snap(&da);
      drain_s = db_close();
      take_snap(&dbs);
      file_sizes(&app, &alc);
      closed = true;
   }
   csv_row(ph,
           T,
           lat,
           startup,
           ops,
           tuples,
           pt_ns,
           pt_div,
           dns,
           dn,
           viol,
           errs,
           &a,
           &b,
           insert_s,
           drain_s,
           closed ? &da : NULL,
           closed ? &dbs : NULL,
           app,
           alc);
   if (viol) {
      printf("   FINDING: %" PRIu64
             " correctness violations in %s (first: %s)\n",
             viol,
             ph->name,
             workers[0].first_violation[0] ? workers[0].first_violation
                                           : "see stderr");
   }
   free(lat);
   free(startup);
   free(workers);
   workers  = NULL;
   nworkers = 0;
   if (closed && !last) {
      db_open(false);
   }
}

/* ------------------------------------------------------------------------ */
/* CLI                                                                       */
/* ------------------------------------------------------------------------ */

static void
usage(void)
{
   fprintf(
      stderr,
      "usage: kvbench --db PATH (--create|--open) --n N --cache-mib M "
      "--disk-gib G --log 0|1\n"
      "   [--threads 6] [--mt-bg 1] [--normal-bg 4] [--memtable-mib 24]\n"
      "   [--secs 20] [--warmup 3] [--csv FILE] [--hang-dir DIR]\n"
      "   [--vtag S] [--stag S] [--otag S] [--rep R] [--seed S]\n"
      "   [--sabotage MASK]  (1: point present->query absent; 2: point "
      "neg->query present;\n"
      "                       4: range expects +1; 8: warm expects +1; 16: "
      "point threads hang)\n"
      "   [--hang-secs 120]  (no-progress timeout before gdb dump + exit 99\n"
      "   phase...   load:random|load:seq  warm  point:uniform|zipf|seq|neg\n"
      "              range:uniform|zipf:L  ow:uniform|zipf    (suffix @T = "
      "threads)\n");
   exit(2);
}

static bool
parse_phase(const char *s, phase_t *ph)
{
   memset(ph, 0, sizeof(*ph));
   snprintf(ph->name, sizeof(ph->name), "%s", s);
   char buf[64];
   snprintf(buf, sizeof(buf), "%s", s);
   char *at = strchr(buf, '@');
   if (at) {
      *at         = 0;
      ph->threads = atoi(at + 1);
   }
   char *a = strtok(buf, ":"), *b = strtok(NULL, ":"), *c = strtok(NULL, ":");
   if (!a) {
      return false;
   }
   enum dist d = D_UNIFORM;
   if (b) {
      if (!strcmp(b, "uniform"))
         d = D_UNIFORM;
      else if (!strcmp(b, "zipf"))
         d = D_ZIPF;
      else if (!strcmp(b, "seq"))
         d = D_SEQ;
      else if (!strcmp(b, "neg"))
         d = D_NEG;
      else if (!strcmp(b, "random"))
         d = D_RANDOM;
      else
         return false;
   }
   ph->dist = d;
   if (!strcmp(a, "load")) {
      ph->kind = PH_LOAD;
      return b && (d == D_RANDOM || d == D_SEQ);
   } else if (!strcmp(a, "warm")) {
      ph->kind = PH_WARM;
      return true;
   } else if (!strcmp(a, "point")) {
      ph->kind = PH_POINT;
      return b != NULL;
   } else if (!strcmp(a, "range")) {
      ph->kind = PH_RANGE;
      if (!b || !c || (d != D_UNIFORM && d != D_ZIPF)) {
         return false;
      }
      ph->L = strtoull(c, NULL, 10);
      return ph->L >= 1;
   } else if (!strcmp(a, "ow")) {
      ph->kind = PH_OW;
      return b && (d == D_UNIFORM || d == D_ZIPF);
   }
   return false;
}

int
main(int argc, char **argv)
{
   opt      = (options){.threads      = 6,
                        .mt_bg        = 1,
                        .normal_bg    = 4,
                        .memtable_mib = 24,
                        .secs         = 20,
                        .warmup       = 3,
                        .vtag         = "?",
                        .stag         = "?",
                        .otag         = "?",
                        .rep          = 1,
                        .seed         = 42,
                        .hang_secs    = 120,
                        .use_log      = -1};
   int mode = -1;
#ifdef KVBENCH_SPACE_MODE
   /*
    * Space-check build only: open with use_stats, print insertion stats
    * (which include allocator "Currently Allocated" extents, recomputed from
    * the persisted allocator map at open), close. No phases, no writes.
    */
   bool space_mode = false;
#endif
   for (int i = 1; i < argc; i++) {
      const char *s = argv[i];
#define ARG(name) (!strcmp(s, name) && i + 1 < argc)
      if (!strcmp(s, "--create"))
         mode = 1;
      else if (!strcmp(s, "--open"))
         mode = 0;
      else if (ARG("--db"))
         opt.db = argv[++i];
      else if (ARG("--csv"))
         opt.csv = argv[++i];
      else if (ARG("--hang-dir"))
         opt.hang_dir = argv[++i];
      else if (ARG("--vtag"))
         opt.vtag = argv[++i];
      else if (ARG("--stag"))
         opt.stag = argv[++i];
      else if (ARG("--otag"))
         opt.otag = argv[++i];
      else if (ARG("--rep"))
         opt.rep = atoi(argv[++i]);
      else if (ARG("--n"))
         opt.n = strtoull(argv[++i], NULL, 0);
      else if (ARG("--cache-mib"))
         opt.cache_mib = strtoull(argv[++i], NULL, 0);
      else if (ARG("--disk-gib"))
         opt.disk_gib = strtoull(argv[++i], NULL, 0);
      else if (ARG("--memtable-mib"))
         opt.memtable_mib = strtoull(argv[++i], NULL, 0);
      else if (ARG("--log"))
         opt.use_log = atoi(argv[++i]);
      else if (ARG("--threads"))
         opt.threads = atoi(argv[++i]);
      else if (ARG("--mt-bg"))
         opt.mt_bg = atoi(argv[++i]);
      else if (ARG("--normal-bg"))
         opt.normal_bg = atoi(argv[++i]);
      else if (ARG("--secs"))
         opt.secs = atof(argv[++i]);
      else if (ARG("--warmup"))
         opt.warmup = atof(argv[++i]);
      else if (ARG("--sabotage"))
         opt.sabotage = strtoull(argv[++i], NULL, 0);
      else if (ARG("--hang-secs"))
         opt.hang_secs = strtoull(argv[++i], NULL, 0);
      else if (ARG("--seed"))
         opt.seed = strtoull(argv[++i], NULL, 0);
#ifdef KVBENCH_SPACE_MODE
      else if (!strcmp(s, "space"))
         space_mode = true;
#endif
      else if (s[0] == '-')
         usage();
      else {
         if (opt.nphases >= MAX_PHASES
             || !parse_phase(s, &opt.phases[opt.nphases]))
         {
            fprintf(stderr, "bad phase '%s'\n", s);
            usage();
         }
         opt.nphases++;
      }
#undef ARG
   }
#ifdef KVBENCH_SPACE_MODE
   if (space_mode && opt.db && opt.cache_mib && opt.disk_gib
       && opt.use_log >= 0)
   {
      default_data_config_init(&data_cfg);
      platform_set_log_streams(stdout, stderr);
      db_open(false);
      splinterdb_stats_print_insertion(kvs);
      fflush(stdout);
      db_close();
      uint64_t app, alc;
      file_sizes(&app, &alc);
      printf("space: db_apparent=%" PRIu64 " db_alloc=%" PRIu64 "\n", app, alc);
      return 0;
   }
#endif
   if (!opt.db || mode < 0 || !opt.n || !opt.cache_mib || !opt.disk_gib
       || opt.use_log < 0 || !opt.nphases || opt.threads < 1
       || opt.threads > MAX_WORKERS)
   {
      usage();
   }
   for (int i = 0; i < opt.nphases; i++) {
      if (opt.phases[i].threads > MAX_WORKERS) {
         usage();
      }
   }
   prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
   setvbuf(stdout, NULL, _IOLBF, 0);

   default_data_config_init(&data_cfg);
   perm_init(&load_perm, opt.n, opt.seed * 2 + 1);
   perm_init(&zipf_perm, opt.n, opt.seed * 2 + 2);
   zipf_init(&zipf, opt.n, 0.99);
   generation = (uint64_t)time(NULL);

   printf("kvbench version=%s splinter=%s db=%s mode=%s n=%" PRIu64
          " cache_mib=%" PRIu64 " disk_gib=%" PRIu64
          " log=%d threads=%d mt_bg=%d normal_bg=%d memtable_mib=%" PRIu64
          " secs=%.1f warmup=%.1f rep=%d sabotage=%" PRIu64 "\n",
          opt.vtag,
          splinterdb_get_version(),
          opt.db,
          mode ? "create" : "open",
          opt.n,
          opt.cache_mib,
          opt.disk_gib,
          opt.use_log,
          opt.threads,
          opt.mt_bg,
          opt.normal_bg,
          opt.memtable_mib,
          opt.secs,
          opt.warmup,
          opt.rep,
          opt.sabotage);

   pthread_t wd;
   pthread_create(&wd, NULL, watchdog_main, NULL);

   db_open(mode == 1);
   for (int i = 0; i < opt.nphases; i++) {
      run_phase(&opt.phases[i], i == opt.nphases - 1);
   }
   if (kvs) {
      db_close();
   }
   return 0;
}
