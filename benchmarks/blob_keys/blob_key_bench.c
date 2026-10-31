// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0
/* Experimental ordered blob keys over the real mutable and packed B-trees.
 * Each tree has a separately owned key arena. This is not a persisted format.
 */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "blob_key.h"
#include "blob_key_checks.h"
#include "btree.h"
#include "clockcache.h"
#include "platform_threads.h"
#include "rc_allocator.h"
#include "task.h"

#define MIB (UINT64_C(1) << 20)
#define GIB (UINT64_C(1) << 30)
#define CHECK(x)                                                               \
   do {                                                                        \
      platform_status check_rc = (x);                                          \
      if (!SUCCESS(check_rc)) {                                                \
         fprintf(stderr,                                                       \
                 "%s:%d: %s failed: %d\n",                                     \
                 __FILE__,                                                     \
                 __LINE__,                                                     \
                 #x,                                                           \
                 check_rc.r);                                                  \
         exit(2);                                                              \
      }                                                                        \
   } while (0)
#define REQUIRE(x)                                                             \
   do {                                                                        \
      if (!(x)) {                                                              \
         fprintf(stderr, "%s:%d: %s failed\n", __FILE__, __LINE__, #x);        \
         exit(2);                                                              \
      }                                                                        \
   } while (0)

typedef struct {
   const char *db;
   bool32      blob, variable, checks;
   uint64      n, queries, bytes, prefix, common, cache_mib;
} options;

typedef struct {
   options            opt;
   platform_heap_id   hid;
   io_config          io_cfg;
   allocator_config   al_cfg;
   clockcache_config  cc_cfg;
   task_system_config task_cfg;
   task_system        tasks;
   io_handle         *io;
   rc_allocator       al;
   clockcache         cc;
   blob_key_config    codec;
   btree_config       tree_cfg;
   btree_scratch     *scratch;
   unsigned char     *corpus;
   uint64            *lengths, *order;
   uint64             baseline_allocated;
   uint64             encoded_bytes;
} bench;

typedef struct {
   uint64         root, arena_root;
   page_type      type;
   mini_allocator mini, arena;
   bool32         mini_active, arena_active;
} owned_tree;

static uint64
now_ns(void)
{
   struct timespec ts;
   REQUIRE(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
   return (uint64)ts.tv_sec * 1000000000 + ts.tv_nsec;
}
static uint64
random_next(uint64 *s)
{
   uint64 x = (*s += UINT64_C(0x9e3779b97f4a7c15));
   x        = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
   x        = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
   return x ^ (x >> 31);
}
static slice
raw_key(bench *b, uint64 id, bool32 missing)
{
   return slice_create(b->lengths[id],
                       b->corpus + (2 * id + missing) * b->opt.bytes);
}
static uint64
expected_value(uint64 id)
{
   return id % 8 == 0 ? id + UINT64_C(0x100000000) : id;
}

static void
generate(bench *b)
{
   const options *o = &b->opt;
   REQUIRE(o->n <= SIZE_MAX / (2 * o->bytes));
   b->corpus  = malloc(2 * o->n * o->bytes);
   b->lengths = malloc(o->n * sizeof(*b->lengths));
   b->order   = malloc(o->n * sizeof(*b->order));
   REQUIRE(b->corpus && b->lengths && b->order);
   for (uint64 i = 0; i < o->n; i++) {
      uint64 minlen = o->common + 8;
      b->lengths[i] =
         o->variable ? minlen + i % (o->bytes - minlen + 1) : o->bytes;
      b->order[i] = i;
      for (uint64 missing = 0; missing < 2; missing++) {
         unsigned char *p = b->corpus + (2 * i + missing) * o->bytes;
         memset(p, 0, o->bytes);
         uint64 number = 2 * i + missing;
         for (unsigned j = 0; j < 8; j++)
            p[o->common + j] = number >> (56 - 8 * j);
         uint64 state = i + 1;
         for (uint64 j = minlen; j < b->lengths[i]; j++)
            p[j] = random_next(&state);
      }
   }
   uint64 state = 42;
   for (uint64 i = o->n - 1; i > 0; i--) {
      uint64 j = random_next(&state) % (i + 1), tmp = b->order[i];
      b->order[i] = b->order[j];
      b->order[j] = tmp;
   }
}

static void
setup(bench *b)
{
   platform_register_thread();
   CHECK(platform_heap_create(platform_get_module_id(), GIB, FALSE, &b->hid));
   io_config_init(&b->io_cfg,
                  4096,
                  128 * 1024,
                  O_RDWR | O_CREAT | O_TRUNC | O_DIRECT,
                  0600,
                  256,
                  b->opt.db);
   uint64 capacity = 4 * GIB;
   while (capacity < 8 * b->opt.n * b->opt.bytes)
      capacity *= 2;
   allocator_config_init(&b->al_cfg, &b->io_cfg, capacity);
   clockcache_config_init(
      &b->cc_cfg, &b->io_cfg, b->opt.cache_mib * MIB, "/dev/null", TRUE);
   uint64 workers[NUM_TASK_TYPES] = {0};
   CHECK(task_system_config_init(&b->task_cfg, TRUE, workers));
   b->io = io_handle_create(&b->io_cfg, b->hid);
   REQUIRE(b->io != NULL);
   CHECK(task_system_init(&b->tasks, b->hid, &b->task_cfg));
   CHECK(rc_allocator_init(
      &b->al, &b->al_cfg, b->io, b->hid, platform_get_module_id()));
   CHECK(clockcache_init(&b->cc,
                         &b->cc_cfg,
                         b->io,
                         (allocator *)&b->al,
                         "blob-keys",
                         b->hid,
                         platform_get_module_id()));
   CHECK(blob_key_config_init(
      &b->codec, (cache *)&b->cc, b->opt.blob, b->opt.prefix));
   btree_config_init(&b->tree_cfg, &b->cc_cfg.super, &b->codec.data_cfg);
   /* Bound key metadata plus prefix to the existing 512-byte inline limit. */
   b->tree_cfg.blob_cfg.blob_max_descriptor_size = 128;
   b->scratch = calloc(1, btree_scratch_size(&b->tree_cfg));
   REQUIRE(b->scratch);
   b->baseline_allocated = allocator_in_use((allocator *)&b->al);
   generate(b);
}

static void
tree_create_arena(bench *b, owned_tree *t)
{
   CHECK(btree_create((cache *)&b->cc,
                      &b->tree_cfg,
                      &t->arena,
                      PAGE_TYPE_BRANCH,
                      &t->arena_root));
   t->arena_active = TRUE;
}
static void
tree_release(bench *b, owned_tree *t)
{
   if (t->mini_active)
      mini_release(&t->mini);
   if (t->root)
      REQUIRE(btree_dec_ref((cache *)&b->cc, &b->tree_cfg, t->root, t->type));
   if (t->arena_active)
      mini_release(&t->arena);
   if (t->arena_root)
      REQUIRE(btree_dec_ref(
         (cache *)&b->cc, &b->tree_cfg, t->arena_root, PAGE_TYPE_BRANCH));
   memset(t, 0, sizeof(*t));
}
static uint64
phase_begin(bench *b)
{
   blob_key_reset_counters(&b->codec);
   cache_reset_stats((cache *)&b->cc);
   return now_ns();
}
static void
phase_end(bench *b, const char *stage, uint64 operations, uint64 start)
{
   double seconds = (now_ns() - start) / 1e9;
   uint64 read_bytes, write_bytes;
   cache_io_stats((cache *)&b->cc, &read_bytes, &write_bytes);
   blob_key_counters *c = &b->codec.counters;
   printf("{\"stage\":\"%s\",\"mode\":\"%s\",\"key_bytes\":%lu,\"prefix_"
          "bytes\":%lu,\"common_prefix\":%lu,\"records\":%lu,\"operations\":%"
          "lu,\"seconds\":%.9f,\"ops_per_sec\":%.3f,\"read_bytes\":%lu,\"write_"
          "bytes\":%lu,\"allocated_bytes\":%lu,\"corpus_bytes\":%lu,"
          "\"comparisons\":%lu,\"prefix_resolved\":%lu,\"blob_fallbacks\":%lu,"
          "\"blob_fragments\":%lu,\"blob_bytes\":%lu,\"hash_calls\":%lu}\n",
          stage,
          b->opt.blob ? "blob" : "inline",
          b->opt.bytes,
          b->opt.prefix,
          b->opt.common,
          b->opt.n,
          operations,
          seconds,
          operations / seconds,
          read_bytes,
          write_bytes,
          allocator_in_use((allocator *)&b->al) * b->io_cfg.extent_size,
          2 * b->opt.n * b->opt.bytes,
          c->comparisons,
          c->prefix_resolved,
          c->blob_fallbacks,
          c->blob_fragments,
          c->blob_bytes,
          c->hash_calls);
   fflush(stdout);
}

static void
insert(bench *b, owned_tree *t, bool32 overwrite)
{
   writable_buffer encoded;
   writable_buffer_init(&encoded, b->hid);
   btree_insert_results results;
   btree_insert_results_init(&results, NULL);
   uint64 operations = 0, start = phase_begin(b);
   for (uint64 j = 0; j < b->opt.n; j++) {
      uint64 i = b->order[j];
      if (overwrite && i % 8 != 0)
         continue;
      CHECK(blob_key_encode(&b->codec,
                            &b->tree_cfg.blob_cfg,
                            &t->arena,
                            raw_key(b, i, FALSE),
                            &encoded));
      if (!overwrite)
         b->encoded_bytes += writable_buffer_length(&encoded);
      key k = key_create_from_slice(FALSE, writable_buffer_to_slice(&encoded));
      uint64  value = overwrite ? expected_value(i) : i;
      message msg   = message_create_internal(
         MESSAGE_TYPE_INSERT, NULL, slice_create(sizeof(value), &value));
      CHECK(btree_insert((cache *)&b->cc,
                         &b->tree_cfg,
                         b->hid,
                         b->scratch,
                         t->root,
                         &t->mini,
                         k,
                         msg,
                         &results));
      operations++;
   }
   phase_end(b, overwrite ? "overwrite" : "insert", operations, start);
   btree_insert_results_deinit(&results);
   writable_buffer_deinit(&encoded);
}

static void
lookups(bench *b, owned_tree *t, const char *stage)
{
   lookup_result result;
   lookup_result_init(
      &result, &b->codec.data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   uint64 rng = 0x12345, start = phase_begin(b);
   for (uint64 j = 0; j < b->opt.queries; j++) {
      uint64 i       = random_next(&rng) % b->opt.n;
      bool32 missing = j & 1;
      lookup_result_reset(&result);
      CHECK(btree_lookup((cache *)&b->cc,
                         &b->tree_cfg,
                         t->root,
                         BTREE_DEFAULT_FORMAT,
                         t->type,
                         key_create_from_slice(TRUE, raw_key(b, i, missing)),
                         &result));
      REQUIRE(lookup_result_found(&result) == !missing);
      if (!missing) {
         slice value =
            merge_accumulator_to_value(lookup_result_accumulator(&result));
         REQUIRE(slice_length(value) == sizeof(uint64));
         uint64 got;
         memcpy(&got, slice_data(value), sizeof(got));
         REQUIRE(got == expected_value(i));
      }
   }
   phase_end(b, stage, b->opt.queries, start);
   lookup_result_deinit(&result);
}

/* A private owner-pair adapter. Cloning happens during initialization/next,
 * preserving the observation-only iterator_curr contract. */
typedef struct {
   iterator        super;
   btree_iterator  source;
   bench          *b;
   owned_tree     *destination;
   writable_buffer encoded;
   message         msg;
} clone_iterator;
static platform_status
clone_prepare(clone_iterator *it)
{
   if (!iterator_can_next(&it->source.super))
      return STATUS_OK;
   key k;
   iterator_curr(&it->source.super, &k, &it->msg);
   return blob_key_clone(&it->b->codec,
                         &it->b->tree_cfg.blob_cfg,
                         &it->destination->arena,
                         key_slice(k),
                         &it->encoded);
}
static void
clone_curr(iterator *base, key *k, message *m)
{
   clone_iterator *it = (clone_iterator *)base;
   *k = key_create_from_slice(FALSE, writable_buffer_to_slice(&it->encoded));
   *m = it->msg;
}
static bool32
clone_next_available(iterator *base)
{
   return iterator_can_next(&((clone_iterator *)base)->source.super);
}
static bool32
clone_prev_available(iterator *base)
{
   (void)base;
   return FALSE;
}
static platform_status
clone_next(iterator *base)
{
   clone_iterator *it = (clone_iterator *)base;
   platform_status rc = iterator_next(&it->source.super);
   return SUCCESS(rc) ? clone_prepare(it) : rc;
}
static const iterator_ops clone_ops = {.curr     = clone_curr,
                                       .can_next = clone_next_available,
                                       .can_prev = clone_prev_available,
                                       .next     = clone_next};

static void
pack(bench *b, owned_tree *source, owned_tree *destination, const char *stage)
{
   uint64 start = phase_begin(b);
   tree_create_arena(b, destination);
   destination->type = PAGE_TYPE_BRANCH;
   clone_iterator it = {
      .super = {.ops = &clone_ops}, .b = b, .destination = destination};
   writable_buffer_init(&it.encoded, b->hid);
   CHECK(btree_iterator_init((cache *)&b->cc,
                             &b->tree_cfg,
                             &it.source,
                             source->root,
                             BTREE_DEFAULT_FORMAT,
                             source->type,
                             &(btree_iterator_init_options){0}));
   CHECK(clone_prepare(&it));
   btree_pack_req req;
   CHECK(btree_pack_req_init(&req,
                             (cache *)&b->cc,
                             &b->tree_cfg,
                             &it.super,
                             b->opt.n,
                             b->hid,
                             .collect_fingerprints = TRUE));
   CHECK(btree_pack(&req));
   destination->root = req.root_addr;
   REQUIRE(req.num_tuples == b->opt.n);
   btree_iterator_deinit(&it.source);
   writable_buffer_deinit(&it.encoded);
   mini_release(&destination->arena);
   destination->arena_active = FALSE;
   phase_end(b, stage, b->opt.n, start);
   for (uint64 i = 0; i < b->opt.n; i++) {
      user_key raw = {.key = raw_key(b, i, FALSE), .is_query_key = TRUE};
      REQUIRE(req.fingerprint_arr[i]
              == b->codec.data_cfg.key_hash(&b->codec.data_cfg, raw, 0));
   }
   btree_pack_req_deinit(&req, b->hid);
}

static void
scan(bench *b, owned_tree *t, const char *stage)
{
   writable_buffer decoded;
   writable_buffer_init(&decoded, b->hid);
   btree_iterator it;
   uint64         start = phase_begin(b);
   CHECK(btree_iterator_init(
      (cache *)&b->cc,
      &b->tree_cfg,
      &it,
      t->root,
      BTREE_DEFAULT_FORMAT,
      t->type,
      &(btree_iterator_init_options){.prefetch_lookahead = 2}));
   uint64 i = 0;
   while (iterator_can_next(&it.super)) {
      key     k;
      message msg;
      iterator_curr(&it.super, &k, &msg);
      REQUIRE(i < b->opt.n);
      CHECK(
         blob_key_decode(&b->codec,
                         (user_key){.key = key_slice(k), .is_query_key = FALSE},
                         &decoded));
      if (slice_lex_cmp(writable_buffer_to_slice(&decoded),
                        raw_key(b, i, FALSE))
          != 0)
      {
         uint64 got_id = 0;
         memcpy(&got_id, message_data(msg), sizeof(got_id));
         fprintf(stderr,
                 "scan mismatch index=%lu value=%lu decoded_length=%lu "
                 "expected_length=%lu\n",
                 i,
                 got_id,
                 writable_buffer_length(&decoded),
                 b->lengths[i]);
         exit(2);
      }
      uint64 got;
      REQUIRE(message_length(msg) == sizeof(got));
      memcpy(&got, message_data(msg), sizeof(got));
      REQUIRE(got == expected_value(i));
      i++;
      CHECK(iterator_next(&it.super));
   }
   REQUIRE(i == b->opt.n);
   btree_iterator_deinit(&it);
   phase_end(b, stage, i, start);
   writable_buffer_deinit(&decoded);
}

static options
parse_options(int argc, char **argv)
{
   options o = {.db        = NULL,
                .blob      = TRUE,
                .n         = 1024,
                .queries   = 2048,
                .bytes     = 4096,
                .prefix    = 32,
                .cache_mib = 128};
   for (int i = 1; i < argc; i++) {
      if (!strcmp(argv[i], "--checks")) {
         o.checks = TRUE;
         continue;
      }
      if (!strcmp(argv[i], "--variable")) {
         o.variable = TRUE;
         continue;
      }
      REQUIRE(i + 1 < argc);
      const char *name = argv[i++], *value = argv[i];
      if (!strcmp(name, "--db")) {
         o.db = value;
         continue;
      }
      if (!strcmp(name, "--mode")) {
         REQUIRE(!strcmp(value, "inline") || !strcmp(value, "blob"));
         o.blob = !strcmp(value, "blob");
         continue;
      }
      char *end;
      errno         = 0;
      uint64 number = strtoull(value, &end, 10);
      REQUIRE(errno == 0 && *end == 0 && *value != 0);
      if (!strcmp(name, "--records"))
         o.n = number;
      else if (!strcmp(name, "--queries"))
         o.queries = number;
      else if (!strcmp(name, "--key-bytes"))
         o.bytes = number;
      else if (!strcmp(name, "--prefix"))
         o.prefix = number;
      else if (!strcmp(name, "--common-prefix"))
         o.common = number;
      else if (!strcmp(name, "--cache-mib"))
         o.cache_mib = number;
      else {
         fprintf(stderr, "Unknown argument: %s\n", name);
         exit(2);
      }
   }
   REQUIRE(o.db && o.n > 0 && o.queries > 0 && o.bytes >= 8
           && o.bytes <= 16 * MIB && o.common <= o.bytes - 8
           && o.prefix <= BLOB_KEY_MAX_PREFIX_BYTES && o.cache_mib >= 16);
   REQUIRE(o.blob || o.bytes <= 512);
   return o;
}

int
main(int argc, char **argv)
{
   bench b = {.opt = parse_options(argc, argv)};
   setup(&b);
   printf("{\"configuration\":true,\"cache_mib\":%lu,\"variable_keys\":%s,"
          "\"page_bytes\":4096,\"extent_bytes\":131072,\"fingerprints\":true,"
          "\"direct_io\":true}\n",
          b.opt.cache_mib,
          b.opt.variable ? "true" : "false");
   if (b.opt.checks) {
      owned_tree checks_arena = {0};
      tree_create_arena(&b, &checks_arena);
      CHECK(blob_key_checks(
         &b.codec, &b.tree_cfg.blob_cfg, &checks_arena.arena, b.hid));
      tree_release(&b, &checks_arena);
      goto cleanup;
   }
   owned_tree mutable = {.type = PAGE_TYPE_MEMTABLE}, branch = {0},
              repacked = {0};
   tree_create_arena(&b, &mutable);
   CHECK(btree_create(
      (cache *)&b.cc, &b.tree_cfg, &mutable.mini, mutable.type, &mutable.root));
   mutable.mini_active = TRUE;
   insert(&b, &mutable, FALSE);
   printf(
      "{\"encoded_key_bytes_total\":%lu,\"encoded_key_bytes_average\":%.3f}\n",
      b.encoded_bytes,
      (double)b.encoded_bytes / b.opt.n);
   insert(&b, &mutable, TRUE);
   lookups(&b, &mutable, "memtable_lookup");
   scan(&b, &mutable, "memtable_scan");
   pack(&b, &mutable, &branch, "pack");
   tree_release(&b, &mutable);
   /* The source arena is gone; branch key clones must independently survive. */
   CHECK(cache_writeback_dirty((cache *)&b.cc));
   CHECK(cache_durable_barrier((cache *)&b.cc));
   CHECK(cache_evict((cache *)&b.cc, FALSE));
   lookups(&b, &branch, "branch_cold_lookup");
   lookups(&b, &branch, "branch_warm_lookup");
   scan(&b, &branch, "branch_scan");
   pack(&b, &branch, &repacked, "repack");
   tree_release(&b, &branch);
   CHECK(cache_writeback_dirty((cache *)&b.cc));
   CHECK(cache_durable_barrier((cache *)&b.cc));
   CHECK(cache_evict((cache *)&b.cc, FALSE));
   lookups(&b, &repacked, "repacked_lookup");
   scan(&b, &repacked, "repacked_scan");
   tree_release(&b, &repacked);
cleanup:
   REQUIRE(allocator_in_use((allocator *)&b.al) == b.baseline_allocated);
   free(b.scratch);
   free(b.corpus);
   free(b.lengths);
   free(b.order);
   clockcache_deinit(&b.cc);
   rc_allocator_deinit(&b.al);
   task_system_deinit(&b.tasks);
   io_handle_destroy(b.io);
   platform_heap_destroy(&b.hid);
   platform_deregister_thread();
   if (b.opt.checks) {
      puts("{\"validation\":\"passed\",\"codec_checks\":true,\"allocation_"
           "baseline_restored\":true}");
      return 0;
   }
   puts("{\"validation\":\"passed\",\"source_release\":true,\"repack_source_"
        "release\":true,\"allocation_baseline_restored\":true}");
   return 0;
}
