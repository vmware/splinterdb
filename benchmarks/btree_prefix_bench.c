// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/* B-tree-only resident benchmark. Build against matching private headers and
 * libsplinterdb.a (see build_btree_prefix_bench.py). No trunk, WAL, background
 * tasks, or source-tree iterator participates in construction. All input bytes,
 * tuple descriptors, and operation orders are precomputed before timing.
 *
 * A pack sample measures the complete btree_pack() call. Request
 * initialization, full output verification and reclamation are outside the
 * measured interval. Multiple samples aggregate active time without retaining
 * multiple outputs. Read samples use a packed immutable branch; inserts use a
 * separate mutable B-tree. Runtime compression does not affect mutable insert
 * representation.
 *
 * PREFIX_PERF_CONTROL/PREFIX_PERF_ACK optionally name perf stat control FIFOs.
 * Counters must initially be disabled. Each active interval enables/disables
 * them; setup, verification and reclamation remain excluded. FIFO handshakes
 * make the perf window slightly wider than wall timing (clock reads only).
 * getrusage and /proc snapshots surround that window; process CPU/wall clocks
 * bracket the active work directly. No setup, verification or reclaim is in it.
 */
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#include "splinterdb/default_data_config.h"
#include "btree_private.h"
#include "clockcache.h"
#include "platform_threads.h"
#include "rc_allocator.h"

#ifndef BTREE_BENCH_PREFIX_SUPPORT
#   define BTREE_BENCH_PREFIX_SUPPORT 0
#endif

#define MIB          (UINT64_C(1) << 20)
#define PAGE_BYTES   4096
#define EXTENT_BYTES (32 * PAGE_BYTES)

typedef struct {
   const char *db, *operation, *pattern, *insert_order;
   uint64      keys, key_bytes, value_bytes, ops, scan_length, batches, seed;
   uint64      cache_mib, disk_mib;
   int         cpu;
   bool        compression, read_lexical, compression_set, read_lexical_set;
   bool        miss, full_scan, allow_io, ops_set;
} options;

typedef struct {
   key     tuple_key;
   message msg;
} tuple;

typedef struct {
   iterator     super;
   const tuple *tuples;
   uint64       count, pos;
} array_iterator;

typedef struct {
   key    tuple_key;
   uint64 id;
} generated_key;

typedef struct {
   uint64 wall_ns, cpu_ns, user_ns, system_ns;
   uint64 read_bytes, write_bytes, minor_faults, major_faults;
   uint64 io_reads, io_writes, io_async_reads, io_async_writes;
} counters;

typedef struct {
   options           opt;
   data_config       data_cfg;
   io_config         io_cfg;
   allocator_config  allocator_cfg;
   clockcache_config cache_cfg;
   btree_config      btree_cfg;
   platform_heap_id  hid;
   io_handle        *io;
   rc_allocator      al;
   clockcache        cc;
   btree_scratch    *scratch;
   tuple            *tuples;
   key              *misses;
   unsigned char    *key_storage, *value_storage, *replacement_storage;
   uint64           *insert_order, *query_order;
   counters          total;
   uint64 checksum, expected_checksum, verified_tuples, verified_values;
   uint64 packed_bytes, leaf_count, compressed_leaf_count, prefix_bytes;
   uint64 operations, returned_tuples, processed_tuples;
   FILE  *perf_control, *perf_ack;
} benchmark;

static const data_config *sort_config;
static const io_ops      *underlying_io_ops;
static io_ops             measured_io_ops;
static bool               active_io;
static counters           io_counts;

static void
fail(const char *what)
{
   fprintf(stderr, "btree_prefix_bench: %s\n", what);
   exit(1);
}

static void
check(platform_status status, const char *what)
{
   if (!SUCCESS(status)) {
      fprintf(stderr,
              "btree_prefix_bench: %s: %s\n",
              what,
              platform_status_to_string(status));
      exit(1);
   }
}

static void *
allocate(uint64 count, uint64 size)
{
   if (size == 0 || count > SIZE_MAX / size) {
      fail("allocation size overflow");
   }
   void *p = calloc(count, size);
   if (p == NULL) {
      fail("allocation failed");
   }
   return p;
}

static uint64
clock_ns(clockid_t clock)
{
   struct timespec t;
   if (clock_gettime(clock, &t) != 0) {
      fail("clock_gettime failed");
   }
   return (uint64)t.tv_sec * 1000000000 + t.tv_nsec;
}

static uint64
mix(uint64 x)
{
   x += UINT64_C(0x9e3779b97f4a7c15);
   x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
   x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
   return x ^ (x >> 31);
}

static void
put_be(unsigned char *out, uint64 n)
{
   for (int i = 7; i >= 0; i--) {
      out[i] = (unsigned char)n;
      n >>= 8;
   }
}

static uint64
get_be(const void *data)
{
   const unsigned char *p = data;
   uint64               n = 0;
   for (unsigned i = 0; i < 8; i++) {
      n = (n << 8) | p[i];
   }
   return n;
}

static int
nonlex_compare(const data_config *cfg, user_key a, user_key b)
{
   (void)cfg;
   if (slice_length(a.key) < 8 || slice_length(b.key) < 8) {
      fail("nonlexical comparator received a short key");
   }
   return memcmp((const char *)slice_data(a.key) + slice_length(a.key) - 8,
                 (const char *)slice_data(b.key) + slice_length(b.key) - 8,
                 8);
}

static int
generated_compare(const void *a, const void *b)
{
   const generated_key *x = a, *y = b;
   return data_key_compare(sort_config, x->tuple_key, y->tuple_key);
}

static void
set_lexical(benchmark *b, bool lexical)
{
#if BTREE_BENCH_PREFIX_SUPPORT
   b->data_cfg.key_compare_is_lexical = lexical;
#else
   (void)b;
   (void)lexical;
#endif
}

static void
array_curr(iterator *base, key *tuple_key, message *msg)
{
   array_iterator *it = (array_iterator *)base;
   *tuple_key         = it->tuples[it->pos].tuple_key;
   *msg               = it->tuples[it->pos].msg;
}

static bool32
array_can_curr(iterator *base)
{
   array_iterator *it = (array_iterator *)base;
   return it->pos < it->count;
}

static platform_status
array_next(iterator *base)
{
   array_iterator *it = (array_iterator *)base;
   it->pos++;
   return STATUS_OK;
}

static platform_status
array_prev(iterator *base)
{
   (void)base;
   return STATUS_NOTSUP;
}

static platform_status
array_seek(iterator *base, comparison c, key k)
{
   (void)base;
   (void)c;
   (void)k;
   return STATUS_NOTSUP;
}

static void
array_print(iterator *base)
{
   (void)base;
}

static const iterator_ops array_ops = {
   .curr     = array_curr,
   .can_prev = array_can_curr,
   .can_next = array_can_curr,
   .next     = array_next,
   .prev     = array_prev,
   .seek     = array_seek,
   .print    = array_print,
};

static platform_status
measured_read(io_handle *io, void *buf, uint64 bytes, uint64 addr)
{
   if (active_io) {
      io_counts.io_reads++;
   }
   return underlying_io_ops->read(io, buf, bytes, addr);
}

static platform_status
measured_write(io_handle *io, void *buf, uint64 bytes, uint64 addr)
{
   if (active_io) {
      io_counts.io_writes++;
   }
   return underlying_io_ops->write(io, buf, bytes, addr);
}

static platform_status
measured_async_init(io_async_state   *state,
                    io_handle        *io,
                    io_async_cmd      cmd,
                    uint64            addr,
                    async_callback_fn callback,
                    void             *arg)
{
   if (active_io) {
      if (cmd == io_async_preadv) {
         io_counts.io_async_reads++;
      } else {
         io_counts.io_async_writes++;
      }
   }
   return underlying_io_ops->async_state_init(
      state, io, cmd, addr, callback, arg);
}

static void
perf_command(benchmark *b, const char *command)
{
   if (b->perf_control == NULL) {
      return;
   }
   if (fprintf(b->perf_control, "%s\n", command) < 0
       || fflush(b->perf_control) != 0)
   {
      fail("perf control write failed");
   }
   /* Installed perf emits five bytes: "ack\n\0". A line reader leaves the
    * trailing NUL for the next acknowledgement and strcmp then sees an empty
    * string. Accept the optional one-byte separator, but validate every byte
    * of the acknowledgement itself; also accept perf versions without NUL. */
   int first = fgetc(b->perf_ack);
   if (first == '\0') {
      first = fgetc(b->perf_ack);
   }
   if (first != 'a' || fgetc(b->perf_ack) != 'c' || fgetc(b->perf_ack) != 'k'
       || fgetc(b->perf_ack) != '\n')
   {
      fail("perf control acknowledgement failed");
   }
}

static counters
snapshot(void)
{
   counters c    = io_counts;
   FILE    *file = fopen("/proc/self/io", "r");
   if (file == NULL) {
      fail("cannot read /proc/self/io");
   }
   char     line[128];
   unsigned found = 0;
   while (fgets(line, sizeof(line), file) != NULL) {
      unsigned long long value;
      if (sscanf(line, "read_bytes: %llu", &value) == 1) {
         c.read_bytes = value;
         found |= 1;
      } else if (sscanf(line, "write_bytes: %llu", &value) == 1) {
         c.write_bytes = value;
         found |= 2;
      }
   }
   if (fclose(file) != 0 || found != 3) {
      fail("incomplete /proc/self/io snapshot");
   }
   struct rusage r;
   if (getrusage(RUSAGE_SELF, &r) != 0) {
      fail("getrusage failed");
   }
   c.user_ns = (uint64)r.ru_utime.tv_sec * 1000000000
               + (uint64)r.ru_utime.tv_usec * 1000;
   c.system_ns = (uint64)r.ru_stime.tv_sec * 1000000000
                 + (uint64)r.ru_stime.tv_usec * 1000;
   c.minor_faults = r.ru_minflt;
   c.major_faults = r.ru_majflt;
   c.cpu_ns       = clock_ns(CLOCK_PROCESS_CPUTIME_ID);
   c.wall_ns      = clock_ns(CLOCK_MONOTONIC);
   return c;
}

static counters
begin_interval(benchmark *b)
{
   io_wait_all(b->io);
   counters c = snapshot();
   perf_command(b, "enable");
   c.cpu_ns  = clock_ns(CLOCK_PROCESS_CPUTIME_ID);
   c.wall_ns = clock_ns(CLOCK_MONOTONIC);
   active_io = true;
   return c;
}

static void
end_interval(benchmark *b, counters before)
{
   uint64 end_wall = clock_ns(CLOCK_MONOTONIC);
   uint64 end_cpu  = clock_ns(CLOCK_PROCESS_CPUTIME_ID);
   active_io       = false;
   perf_command(b, "disable");
   counters after = snapshot();
   after.wall_ns  = end_wall;
   after.cpu_ns   = end_cpu;
#define ACCUMULATE(field)                                                      \
   do {                                                                        \
      if (after.field < before.field)                                          \
         fail("resource counter decreased");                                   \
      b->total.field += after.field - before.field;                            \
   } while (0)
   ACCUMULATE(wall_ns);
   ACCUMULATE(cpu_ns);
   ACCUMULATE(user_ns);
   ACCUMULATE(system_ns);
   ACCUMULATE(read_bytes);
   ACCUMULATE(write_bytes);
   ACCUMULATE(minor_faults);
   ACCUMULATE(major_faults);
   ACCUMULATE(io_reads);
   ACCUMULATE(io_writes);
   ACCUMULATE(io_async_reads);
   ACCUMULATE(io_async_writes);
#undef ACCUMULATE
   if (!b->opt.allow_io
       && (after.read_bytes != before.read_bytes
           || after.write_bytes != before.write_bytes
           || after.io_reads != before.io_reads
           || after.io_writes != before.io_writes
           || after.io_async_reads != before.io_async_reads
           || after.io_async_writes != before.io_async_writes
           || after.major_faults != before.major_faults))
   {
      fail("measured interval issued I/O or major faults; resident benchmark "
           "rejected");
   }
}

static void
make_inputs(benchmark *b)
{
   const options *o = &b->opt;
   b->key_storage   = allocate(2 * o->keys, o->key_bytes);
   b->value_storage = allocate(o->keys, o->value_bytes);
   if (strcmp(o->operation, "overwrite") == 0)
      b->replacement_storage = allocate(o->keys, o->value_bytes);
   b->tuples                = allocate(o->keys, sizeof(*b->tuples));
   b->misses                = allocate(o->keys, sizeof(*b->misses));
   generated_key *generated = allocate(2 * o->keys, sizeof(*generated));
   for (uint64 i = 0; i < 2 * o->keys; i++) {
      unsigned char *k = b->key_storage + i * o->key_bytes;
      for (uint64 j = 0; j < o->key_bytes; j++) {
         k[j] = (unsigned char)(mix(i + o->seed + (j / 8) * 0x100000001b3ULL)
                                >> (8 * (j % 8)));
      }
      if (strcmp(o->pattern, "common") == 0) {
         memset(k, 'p', 48);
         put_be(k + 48, mix(i + o->seed));
      } else {
         /* mix is a permutation, so first words are random and unique. */
         put_be(k, mix(i + o->seed));
      }
      put_be(k + o->key_bytes - 8, i);
      generated[i] = (generated_key){
         .tuple_key = key_create(FALSE, o->key_bytes, k), .id = i};
   }
   sort_config = &b->data_cfg;
   qsort(generated, 2 * o->keys, sizeof(*generated), generated_compare);
   for (uint64 i = 1; i < 2 * o->keys; i++) {
      if (data_key_compare(
             &b->data_cfg, generated[i - 1].tuple_key, generated[i].tuple_key)
          >= 0)
      {
         fail("generated keys are not strictly sorted and unique");
      }
   }
   for (uint64 i = 0; i < o->keys; i++) {
      unsigned char *v  = b->value_storage + i * o->value_bytes;
      uint64         id = generated[2 * i].id;
      for (uint64 j = 0; j < o->value_bytes; j++) {
         v[j] = (unsigned char)(mix(id + o->seed + (j / 8) * 0x123456789ULL)
                                >> (8 * (j % 8)));
      }
      put_be(v, id);
      if (b->replacement_storage != NULL) {
         unsigned char *replacement =
            b->replacement_storage + i * o->value_bytes;
         for (uint64 j = 0; j < o->value_bytes; j++)
            replacement[j] = v[j] ^ 0xa5;
      }
      b->tuples[i] = (tuple){
         .tuple_key = generated[2 * i].tuple_key,
         .msg       = message_create_internal(
            MESSAGE_TYPE_INSERT, NULL, slice_create(o->value_bytes, v))};
      b->misses[i] = generated[2 * i + 1].tuple_key;
   }
   free(generated);
   b->insert_order = allocate(o->keys, sizeof(*b->insert_order));
   for (uint64 i = 0; i < o->keys; i++) {
      b->insert_order[i] = i;
   }
   if (strcmp(o->insert_order, "random") == 0) {
      uint64 state = o->seed;
      for (uint64 i = o->keys - 1; i > 0; i--) {
         state    = mix(state);
         uint64 j = state % (i + 1), tmp = b->insert_order[i];
         b->insert_order[i] = b->insert_order[j];
         b->insert_order[j] = tmp;
      }
   }
   b->query_order = allocate(o->ops, sizeof(*b->query_order));
   uint64 state   = o->seed;
   uint64 range   = strcmp(o->operation, "scan") == 0
                       ? o->keys - o->scan_length + 1
                       : o->keys;
   for (uint64 i = 0; i < o->ops; i++) {
      state             = mix(state);
      b->query_order[i] = state % range;
   }
}

static void
init_iterator(benchmark      *b,
              btree_iterator *it,
              uint64          root,
              page_type       type,
              key             start)
{
   check(
      btree_iterator_init((cache *)&b->cc,
                          &b->btree_cfg,
                          it,
                          root,
                          BTREE_BRANCH_FORMAT_VERSION,
                          type,
                          &(const btree_iterator_init_options){
                             .start_position = key_start_at_or_after(start)}),
      "iterator init");
}

static void
verify_tuple(benchmark *b, uint64 position, key actual_key, message actual_msg)
{
   tuple expected = b->tuples[position];
   if (key_length(actual_key) != b->opt.key_bytes
       || message_class(actual_msg) != MESSAGE_TYPE_INSERT
       || message_is_blob(actual_msg)
       || message_length(actual_msg) != b->opt.value_bytes
       || memcmp(key_data(actual_key),
                 key_data(expected.tuple_key),
                 b->opt.key_bytes)
             != 0
       || memcmp(message_data(actual_msg),
                 message_data(expected.msg),
                 b->opt.value_bytes)
             != 0)
   {
      fail("full key/value verification failed");
   }
   b->verified_tuples++;
   b->verified_values++;
}

static uint64
verify_tree(benchmark *b, uint64 root, page_type type, bool inspect)
{
   btree_iterator it;
   init_iterator(b, &it, root, type, NEGATIVE_INFINITY_KEY);
   uint64 count = 0, leaf = UINT64_MAX, checksum = 0;
   if (inspect) {
      b->leaf_count = b->compressed_leaf_count = b->prefix_bytes = 0;
   }
   while (iterator_can_curr(&it.super)) {
      if (count >= b->opt.keys) {
         fail("extra tuple after expected tree end");
      }
      key     k;
      message msg;
      iterator_curr(&it.super, &k, &msg);
      verify_tuple(b, count++, k, msg);
      checksum += get_be((const char *)key_data(k) + key_length(k) - 8)
                  + get_be(message_data(msg));
      if (inspect && leaf != it.curr.addr) {
         leaf = it.curr.addr;
         b->leaf_count++;
#if BTREE_BENCH_PREFIX_SUPPORT
         b->compressed_leaf_count += it.curr.hdr->prefix_length != 0;
         b->prefix_bytes += it.curr.hdr->prefix_length;
#endif
      }
      check(iterator_next(&it.super), "verification iterator next");
   }
   btree_iterator_deinit(&it);
   if (count != b->opt.keys) {
      fail("tree ended before expected tuple count");
   }
   if (inspect) {
      check(btree_space_use_bytes(
               (cache *)&b->cc, &b->btree_cfg, root, type, &b->packed_bytes),
            "tree space inspection");
   }
   return checksum;
}

static uint64
pack_once(benchmark *b, bool measured)
{
   set_lexical(b, b->opt.compression);
   array_iterator it = {
      .super = {.ops = &array_ops}, .tuples = b->tuples, .count = b->opt.keys};
   btree_pack_req *req = allocate(1, sizeof(*req));
   check(
      btree_pack_req_init(
         req, (cache *)&b->cc, &b->btree_cfg, &it.super, b->opt.keys, b->hid),
      "pack req init");
   counters before = {0};
   if (measured) {
      before = begin_interval(b);
   }
   platform_status status = btree_pack(req);
   if (measured) {
      end_interval(b, before);
   }
   check(status, "btree_pack");
   if (req->num_tuples != b->opt.keys || it.pos != b->opt.keys
       || req->key_bytes != b->opt.keys * b->opt.key_bytes
       || req->message_bytes != b->opt.keys * b->opt.value_bytes
       || req->root_addr == 0)
   {
      fail("pack output statistics mismatch");
   }
   uint64 root = req->root_addr;
   btree_pack_req_deinit(req, b->hid);
   free(req);
   set_lexical(b, b->opt.read_lexical);
   uint64 sum = verify_tree(b, root, PAGE_TYPE_BRANCH, true);
   if (measured) {
      b->checksum += sum;
      for (uint64 i = 0; i < b->opt.keys; i++)
         b->expected_checksum +=
            get_be(message_data(b->tuples[i].msg))
            + get_be((const char *)key_data(b->tuples[i].tuple_key)
                     + b->opt.key_bytes - 8);
   }
   return root;
}

static void
run_inserts(benchmark *b)
{
   bool overwrite = strcmp(b->opt.operation, "overwrite") == 0;
   /* First build/reclaim is an unmeasured warmup of this exact mutable path. */
   for (uint64 batch = 0; batch <= b->opt.batches; batch++) {
      bool           measured = batch != 0;
      mini_allocator mini;
      uint64         root;
      check(
         btree_create(
            (cache *)&b->cc, &b->btree_cfg, &mini, PAGE_TYPE_MEMTABLE, &root),
         "mutable tree create");
      btree_insert_results result;
      btree_insert_results_init(&result, NULL);
      if (overwrite) {
         for (uint64 i = 0; i < b->opt.keys; i++) {
            tuple t = b->tuples[b->insert_order[i]];
            check(btree_insert((cache *)&b->cc,
                               &b->btree_cfg,
                               b->hid,
                               b->scratch,
                               root,
                               &mini,
                               t.tuple_key,
                               t.msg,
                               &result),
                  "overwrite prefill");
         }
         verify_tree(b, root, PAGE_TYPE_MEMTABLE, false);
         for (uint64 i = 0; i < b->opt.keys; i++)
            b->tuples[i].msg = message_create_internal(
               MESSAGE_TYPE_INSERT,
               NULL,
               slice_create(b->opt.value_bytes,
                            b->replacement_storage + i * b->opt.value_bytes));
      }
      counters before = {0};
      if (measured)
         before = begin_interval(b);
      for (uint64 i = 0; i < b->opt.keys; i++) {
         tuple t = b->tuples[b->insert_order[i]];
         check(btree_insert((cache *)&b->cc,
                            &b->btree_cfg,
                            b->hid,
                            b->scratch,
                            root,
                            &mini,
                            t.tuple_key,
                            t.msg,
                            &result),
               "insert");
      }
      if (measured)
         end_interval(b, before);
      btree_insert_results_deinit(&result);
      uint64 sum = verify_tree(b, root, PAGE_TYPE_MEMTABLE, false);
      if (measured) {
         b->checksum += sum;
         for (uint64 i = 0; i < b->opt.keys; i++)
            b->expected_checksum +=
               get_be(message_data(b->tuples[i].msg))
               + get_be((const char *)key_data(b->tuples[i].tuple_key)
                        + b->opt.key_bytes - 8);
      }
      mini_release(&mini);
      btree_dec_ref((cache *)&b->cc, &b->btree_cfg, root, PAGE_TYPE_MEMTABLE);
      if (measured) {
         b->operations += b->opt.keys;
         b->processed_tuples += b->opt.keys;
      }
      if (overwrite) {
         for (uint64 i = 0; i < b->opt.keys; i++)
            b->tuples[i].msg = message_create_internal(
               MESSAGE_TYPE_INSERT,
               NULL,
               slice_create(b->opt.value_bytes,
                            b->value_storage + i * b->opt.value_bytes));
      }
   }
}

static uint64
run_lookups(benchmark *b, uint64 root, bool verify)
{
   char          buffer[MAX_INLINE_MESSAGE_SIZE(PAGE_BYTES)];
   lookup_result result;
   lookup_result_init(&result, &b->data_cfg, 0, sizeof(buffer), buffer);
   uint64 checksum = 0;
   for (uint64 q = 0; q < b->opt.ops; q++) {
      uint64 pos    = b->query_order[q];
      key    target = b->opt.miss ? b->misses[pos] : b->tuples[pos].tuple_key;
      check(btree_lookup((cache *)&b->cc,
                         &b->btree_cfg,
                         root,
                         BTREE_BRANCH_FORMAT_VERSION,
                         PAGE_TYPE_BRANCH,
                         target,
                         &result),
            "lookup");
      if (lookup_result_found(&result) == b->opt.miss) {
         fail("lookup hit/miss mismatch");
      }
      if (b->opt.miss) {
         checksum += pos + 1;
      } else {
         message msg = merge_accumulator_to_message(&result.value);
         if (message_length(msg) != b->opt.value_bytes || message_is_blob(msg))
         {
            fail("lookup value shape mismatch");
         }
         checksum += get_be(message_data(msg));
         if (verify) {
            verify_tuple(b, pos, target, msg);
         }
      }
   }
   lookup_result_deinit(&result);
   return checksum;
}

static uint64
run_scans(benchmark *b, uint64 root, bool verify)
{
   uint64 checksum = 0;
   for (uint64 q = 0; q < b->opt.ops; q++) {
      uint64         start = b->query_order[q];
      btree_iterator it;
      init_iterator(b, &it, root, PAGE_TYPE_BRANCH, b->tuples[start].tuple_key);
      for (uint64 i = 0; i < b->opt.scan_length; i++) {
         if (!iterator_can_curr(&it.super)) {
            fail("range ended before its requested length");
         }
         key     k;
         message msg;
         iterator_curr(&it.super, &k, &msg);
         if (key_length(k) != b->opt.key_bytes
             || message_length(msg) != b->opt.value_bytes
             || message_is_blob(msg))
         {
            fail("scan value shape mismatch");
         }
         checksum += get_be(message_data(msg))
                     + get_be((const char *)key_data(k) + key_length(k) - 8);
         if (verify) {
            verify_tuple(b, start + i, k, msg);
         }
         /* Do not fetch an unused tuple past the requested range. */
         if (i + 1 < b->opt.scan_length) {
            check(iterator_next(&it.super), "scan iterator next");
         }
      }
      btree_iterator_deinit(&it);
   }
   return checksum;
}

static uint64
expected_query_checksum(benchmark *b)
{
   uint64 sum   = 0;
   bool   scans = strcmp(b->opt.operation, "scan") == 0;
   for (uint64 q = 0; q < b->opt.ops; q++) {
      uint64 pos = b->query_order[q];
      if (!scans && b->opt.miss) {
         sum += pos + 1;
      } else {
         uint64 count = scans ? b->opt.scan_length : 1;
         for (uint64 j = 0; j < count; j++) {
            sum += get_be(message_data(b->tuples[pos + j].msg));
            if (scans)
               sum +=
                  get_be((const char *)key_data(b->tuples[pos + j].tuple_key)
                         + b->opt.key_bytes - 8);
         }
      }
   }
   return sum;
}

static uint64
parse_number(const char *value)
{
   char *end;
   errno                = 0;
   unsigned long long n = strtoull(value, &end, 10);
   if (errno != 0 || *value == '-' || end == value || *end != '\0') {
      fail("invalid numeric argument");
   }
   return n;
}

static bool
parse_switch(const char *value)
{
   if (strcmp(value, "on") == 0)
      return true;
   if (strcmp(value, "off") == 0)
      return false;
   fail("switch must be on or off");
   return false;
}

static options
parse_options(int argc, char **argv)
{
   options                    o      = {.operation    = "lookup",
                                        .pattern      = "common",
                                        .insert_order = "random",
                                        .keys         = 200000,
                                        .key_bytes    = 64,
                                        .value_bytes  = 100,
                                        .ops          = 1000000,
                                        .scan_length  = 10000,
                                        .batches      = 1,
                                        .seed         = 42,
                                        .cache_mib    = 512,
                                        .disk_mib     = 2048,
                                        .cpu          = 2,
                                        .compression  = true,
                                        .read_lexical = true};
   static const struct option opts[] = {
      {"db", required_argument, 0, 'd'},
      {"operation", required_argument, 0, 'o'},
      {"pattern", required_argument, 0, 'p'},
      {"compression", required_argument, 0, 'c'},
      {"read-lexical", required_argument, 0, 'r'},
      {"keys", required_argument, 0, 'n'},
      {"key-bytes", required_argument, 0, 'k'},
      {"value-bytes", required_argument, 0, 'v'},
      {"ops", required_argument, 0, 'q'},
      {"scan-length", required_argument, 0, 'l'},
      {"batches", required_argument, 0, 'b'},
      {"seed", required_argument, 0, 's'},
      {"cache-mib", required_argument, 0, 'a'},
      {"disk-mib", required_argument, 0, 'D'},
      {"insert-order", required_argument, 0, 'i'},
      {"miss", no_argument, 0, 'm'},
      {"cpu", required_argument, 0, 'C'},
      {"allow-io", no_argument, 0, 'I'},
      {"help", no_argument, 0, 'h'},
      {0, 0, 0, 0}};
   int c;
   while ((c = getopt_long(argc, argv, "", opts, NULL)) != -1) {
      switch (c) {
         case 'd':
            o.db = optarg;
            break;
         case 'o':
            o.operation = optarg;
            break;
         case 'p':
            o.pattern = optarg;
            break;
         case 'c':
            o.compression     = parse_switch(optarg);
            o.compression_set = true;
            break;
         case 'r':
            o.read_lexical     = parse_switch(optarg);
            o.read_lexical_set = true;
            break;
         case 'n':
            o.keys = parse_number(optarg);
            break;
         case 'k':
            o.key_bytes = parse_number(optarg);
            break;
         case 'v':
            o.value_bytes = parse_number(optarg);
            break;
         case 'q':
            o.ops     = parse_number(optarg);
            o.ops_set = true;
            break;
         case 'l':
            o.full_scan = strcmp(optarg, "full") == 0;
            if (!o.full_scan)
               o.scan_length = parse_number(optarg);
            break;
         case 'b':
            o.batches = parse_number(optarg);
            break;
         case 's':
            o.seed = parse_number(optarg);
            break;
         case 'a':
            o.cache_mib = parse_number(optarg);
            break;
         case 'D':
            o.disk_mib = parse_number(optarg);
            break;
         case 'i':
            o.insert_order = optarg;
            break;
         case 'm':
            o.miss = true;
            break;
         case 'C':
         {
            uint64 cpu = parse_number(optarg);
            if (cpu >= CPU_SETSIZE)
               fail("CPU index out of range");
            o.cpu = (int)cpu;
            break;
         }
         case 'I':
            o.allow_io = true;
            break;
         case 'h':
            puts("B-tree resident benchmark: --db NEW_PATH --operation "
                 "insert|overwrite|lookup|scan|pack\n"
                 "  --pattern common|random|nonlexical --compression on|off "
                 "--read-lexical on|off\n"
                 "  --keys N --key-bytes N --value-bytes N --ops N "
                 "--scan-length N|full\n"
                 "  --batches N --seed N --cache-mib N --disk-mib N "
                 "--insert-order random|sorted\n"
                 "  --miss --cpu N --allow-io");
            exit(0);
         default:
            fail("invalid command-line option");
      }
   }
   if (optind != argc || o.db == NULL)
      fail("--db NEW_PATH is required");
   if (strcmp(o.operation, "insert") && strcmp(o.operation, "overwrite")
       && strcmp(o.operation, "lookup") && strcmp(o.operation, "scan")
       && strcmp(o.operation, "pack"))
      fail("invalid operation");
   if (strcmp(o.pattern, "common") && strcmp(o.pattern, "random")
       && strcmp(o.pattern, "nonlexical"))
      fail("invalid pattern");
   if (strcmp(o.insert_order, "random") && strcmp(o.insert_order, "sorted"))
      fail("invalid insertion order");
   if (strcmp(o.pattern, "nonlexical") == 0) {
      if ((o.compression_set && o.compression)
          || (o.read_lexical_set && o.read_lexical))
         fail("nonlexical ordering cannot use a lexical flag");
      o.compression = o.read_lexical = false;
   }
#if !BTREE_BENCH_PREFIX_SUPPORT
   if (o.compression_set && o.compression) {
      fail("this baseline build does not support prefix compression");
   }
   o.compression  = false;
   o.read_lexical = false;
#endif
   if (o.keys < 2 || o.keys > UINT64_MAX / 2 || o.ops == 0 || o.batches == 0
       || o.key_bytes < 64 || o.key_bytes > MAX_INLINE_KEY_SIZE(PAGE_BYTES)
       || o.value_bytes < 8
       || o.value_bytes > MAX_INLINE_MESSAGE_SIZE(PAGE_BYTES)
       || o.cache_mib < 32 || o.disk_mib < 64 || o.cache_mib > UINT64_MAX / MIB
       || o.disk_mib > UINT64_MAX / MIB)
      fail("parameter outside supported bounds");
   if (o.full_scan)
      o.scan_length = o.keys;
   if (strcmp(o.operation, "scan") == 0) {
      if (!o.ops_set)
         o.ops = 1000;
      if (!o.scan_length || o.scan_length > o.keys)
         fail("scan exceeds key count");
   }
   if (o.miss && strcmp(o.operation, "lookup"))
      fail("--miss requires lookup");
   if (o.keys > UINT64_MAX / o.key_bytes || o.keys > UINT64_MAX / o.value_bytes
       || o.keys > UINT64_MAX / o.batches || o.ops > UINT64_MAX / o.batches
       || (strcmp(o.operation, "scan") == 0
           && o.ops * o.batches > UINT64_MAX / o.scan_length))
      fail("work count overflow");
   /* Mutable pages are pinned. Reject obviously oversized configurations before
    * a cache allocation can stall; actual zero-I/O checks remain authoritative.
    */
   long double estimate = 4.0L * o.keys * (o.key_bytes + o.value_bytes + 64);
   if (estimate > (long double)o.cache_mib * MIB / 2)
      fail("cache lacks conservative headroom for one resident tree");
   return o;
}

static void
prefault_buffer(buffer_handle *buffer)
{
   volatile unsigned char *pages = platform_buffer_getaddr(buffer);
   for (uint64 i = 0; i < buffer->length; i += PAGE_BYTES)
      pages[i] = pages[i];
}

static void
setup(benchmark *b)
{
   cpu_set_t cpus;
   CPU_ZERO(&cpus);
   CPU_SET(b->opt.cpu, &cpus);
   if (sched_setaffinity(0, sizeof(cpus), &cpus) != 0)
      fail("CPU pin failed");
   if (platform_register_thread() != 0)
      fail("thread registration failed");
   check(
      platform_heap_create(platform_get_module_id(), 512 * MIB, false, &b->hid),
      "heap create");
   default_data_config_init(&b->data_cfg);
   if (strcmp(b->opt.pattern, "nonlexical") == 0)
      b->data_cfg.key_compare = nonlex_compare;
   set_lexical(b, b->opt.compression);
   make_inputs(b);
   io_config_init(&b->io_cfg,
                  PAGE_BYTES,
                  EXTENT_BYTES,
                  O_RDWR | O_CREAT | O_EXCL | O_DIRECT,
                  0600,
                  256,
                  b->opt.db);
   allocator_config_init(&b->allocator_cfg, &b->io_cfg, b->opt.disk_mib * MIB);
   clockcache_config_init(
      &b->cache_cfg, &b->io_cfg, b->opt.cache_mib * MIB, NULL, false);
   btree_config_init(&b->btree_cfg, &b->cache_cfg.super, &b->data_cfg);
   b->io = io_handle_create(&b->io_cfg, b->hid);
   if (b->io == NULL)
      fail("I/O handle create failed (path must be new)");
   underlying_io_ops                = b->io->ops;
   measured_io_ops                  = *underlying_io_ops;
   measured_io_ops.read             = measured_read;
   measured_io_ops.write            = measured_write;
   measured_io_ops.async_state_init = measured_async_init;
   b->io->ops                       = &measured_io_ops;
   check(rc_allocator_init(
            &b->al, &b->allocator_cfg, b->io, b->hid, platform_get_module_id()),
         "allocator init");
   check(clockcache_init(&b->cc,
                         &b->cache_cfg,
                         b->io,
                         (allocator *)&b->al,
                         "btree-prefix-bench",
                         b->hid,
                         platform_get_module_id()),
         "cache init");
   /* Preserve contents while faulting each anonymous cache data page in. */
   prefault_buffer(&b->cc.data_bh);
   prefault_buffer(&b->cc.lookup_bh);
   prefault_buffer(&b->cc.entry_bh);
   prefault_buffer(&b->cc.rc_bh);
   prefault_buffer(&b->cc.pincount_bh);
   prefault_buffer(&b->cc.batch_bh);
   b->scratch = TYPED_MANUAL_ZALLOC(
      b->hid, b->scratch, btree_scratch_size(&b->btree_cfg));
   if (b->scratch == NULL)
      fail("scratch allocation failed");
   const char *control = getenv("PREFIX_PERF_CONTROL");
   const char *ack     = getenv("PREFIX_PERF_ACK");
   if ((control == NULL) != (ack == NULL))
      fail("both perf FIFO paths required");
   if (control != NULL) {
      b->perf_control = fopen(control, "w");
      b->perf_ack     = fopen(ack, "r");
      if (b->perf_control == NULL || b->perf_ack == NULL)
         fail("perf FIFO open failed");
   }
}

static void
teardown(benchmark *b)
{
   platform_free(b->hid, b->scratch);
   io_wait_all(b->io);
   clockcache_deinit(&b->cc);
   rc_allocator_deinit(&b->al);
   b->io->ops = underlying_io_ops;
   io_handle_destroy(b->io);
   platform_heap_destroy(&b->hid);
   platform_deregister_thread();
   free(b->tuples);
   free(b->misses);
   free(b->key_storage);
   free(b->value_storage);
   free(b->replacement_storage);
   free(b->insert_order);
   free(b->query_order);
   if (b->perf_control)
      fclose(b->perf_control);
   if (b->perf_ack)
      fclose(b->perf_ack);
   /* Only this successfully created, exclusively owned scratch file is removed.
    */
   if (unlink(b->opt.db) != 0)
      fail("scratch file removal failed");
}

int
main(int argc, char **argv)
{
   benchmark *b           = allocate(1, sizeof(*b));
   b->opt                 = parse_options(argc, argv);
   uint64 lifecycle_start = clock_ns(CLOCK_MONOTONIC);
   setup(b);
   if (strcmp(b->opt.operation, "insert") == 0
       || strcmp(b->opt.operation, "overwrite") == 0)
   {
      run_inserts(b);
   } else if (strcmp(b->opt.operation, "pack") == 0) {
      uint64 warm_root = pack_once(b, false);
      btree_dec_ref(
         (cache *)&b->cc, &b->btree_cfg, warm_root, PAGE_TYPE_BRANCH);
      for (uint64 batch = 0; batch < b->opt.batches; batch++) {
         uint64 root = pack_once(b, true);
         btree_dec_ref((cache *)&b->cc, &b->btree_cfg, root, PAGE_TYPE_BRANCH);
         b->operations++;
         b->processed_tuples += b->opt.keys;
      }
   } else {
      uint64 root     = pack_once(b, false);
      bool   scans    = strcmp(b->opt.operation, "scan") == 0;
      uint64 expected = expected_query_checksum(b);
      /* Full replay warms the exact access sequence and validates all bytes. */
      uint64 warm =
         scans ? run_scans(b, root, true) : run_lookups(b, root, true);
      if (warm != expected)
         fail("warmup checksum mismatch");
      for (uint64 batch = 0; batch < b->opt.batches; batch++) {
         counters before = begin_interval(b);
         uint64   sum =
            scans ? run_scans(b, root, false) : run_lookups(b, root, false);
         end_interval(b, before);
         if (sum != expected)
            fail("measured checksum mismatch");
         b->checksum += sum;
         b->expected_checksum += expected;
         b->operations += b->opt.ops;
         b->returned_tuples += scans ? b->opt.ops * b->opt.scan_length
                                     : (b->opt.miss ? 0 : b->opt.ops);
      }
      uint64 replay =
         scans ? run_scans(b, root, true) : run_lookups(b, root, true);
      if (replay != expected)
         fail("verification replay checksum mismatch");
      verify_tree(b, root, PAGE_TYPE_BRANCH, false);
      btree_dec_ref((cache *)&b->cc, &b->btree_cfg, root, PAGE_TYPE_BRANCH);
   }
   if (b->checksum != b->expected_checksum)
      fail("final checksum mismatch");
   teardown(b);
   uint64 lifecycle_ns = clock_ns(CLOCK_MONOTONIC) - lifecycle_start;
   bool   resident     = b->total.read_bytes == 0 && b->total.write_bytes == 0
                   && b->total.major_faults == 0 && b->total.io_reads == 0
                   && b->total.io_writes == 0 && b->total.io_async_reads == 0
                   && b->total.io_async_writes == 0;
   printf(
      "{\"schema_version\":1,\"operation\":\"%s\",\"pattern\":\"%s\","
      "\"insert_order\":\"%s\",\"keys\":%lu,\"key_bytes\":%lu,\"value_bytes\":%"
      "lu,"
      "\"requested_ops\":%lu,\"scan_length\":%lu,\"batches\":%lu,\"seed\":%lu,"
      "\"cache_mib\":%lu,\"disk_mib\":%lu,\"cpu\":%d,\"miss\":%s,"
      "\"prefix_supported\":%s,\"pack_lexical\":%s,\"read_lexical\":%s,"
      "\"operations\":%lu,\"tuples\":%lu,\"returned_tuples\":%lu,\"processed_"
      "tuples\":%lu,"
      "\"active_wall_ns\":%lu,\"process_cpu_ns\":%lu,"
      "\"user_cpu_ns\":%lu,\"system_cpu_ns\":%lu,"
      "\"active_read_bytes\":%lu,\"active_write_bytes\":%lu,"
      "\"minor_faults\":%lu,\"major_faults\":%lu,"
      "\"io_reads\":%lu,\"io_writes\":%lu,\"io_async_reads\":%lu,\"io_async_"
      "writes\":%lu,"
      "\"lifecycle_wall_ns\":%lu,\"checksum\":%lu,\"expected_checksum\":%lu,"
      "\"verified_tuples\":%lu,\"verified_values\":%lu,"
      "\"packed_bytes\":%lu,\"leaf_count\":%lu,\"compressed_leaf_count\":%lu,"
      "\"prefix_bytes\":%lu,\"resident\":%s,\"verified\":true}\n",
      b->opt.operation,
      b->opt.pattern,
      b->opt.insert_order,
      b->opt.keys,
      b->opt.key_bytes,
      b->opt.value_bytes,
      b->opt.ops,
      b->opt.scan_length,
      b->opt.batches,
      b->opt.seed,
      b->opt.cache_mib,
      b->opt.disk_mib,
      b->opt.cpu,
      b->opt.miss ? "true" : "false",
      BTREE_BENCH_PREFIX_SUPPORT ? "true" : "false",
      b->opt.compression ? "true" : "false",
      b->opt.read_lexical ? "true" : "false",
      b->operations,
      b->returned_tuples + b->processed_tuples,
      b->returned_tuples,
      b->processed_tuples,
      b->total.wall_ns,
      b->total.cpu_ns,
      b->total.user_ns,
      b->total.system_ns,
      b->total.read_bytes,
      b->total.write_bytes,
      b->total.minor_faults,
      b->total.major_faults,
      b->total.io_reads,
      b->total.io_writes,
      b->total.io_async_reads,
      b->total.io_async_writes,
      lifecycle_ns,
      b->checksum,
      b->expected_checksum,
      b->verified_tuples,
      b->verified_values,
      b->packed_bytes,
      b->leaf_count,
      b->compressed_leaf_count,
      b->prefix_bytes,
      resident ? "true" : "false");
   free(b);
   return 0;
}
