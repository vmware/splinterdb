// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "test.h"
#include "platform_io.h"
#include "splinterdb/default_data_config.h"
#include "splinterdb_tests_private.h"

#include <errno.h>

#include "random.h"
#include "poison.h"

#define SCAN_BENCHMARK_KEY_SIZE       8
#define SCAN_BENCHMARK_MAX_MILESTONES 32

typedef enum scan_benchmark_mode {
   SCAN_BENCHMARK_LOAD_AND_SCAN,
   SCAN_BENCHMARK_INIT_ONLY,
   SCAN_BENCHMARK_SCAN_ONLY,
   SCAN_BENCHMARK_OPTIMIZE_ONLY,
} scan_benchmark_mode;

typedef struct scan_benchmark_options {
   scan_benchmark_mode mode;
   bool32              random_load_order;
   bool32              splinter_random_keys;
   bool32              random_scan_starts;
   bool32              backwards_scan;
   uint64              scan_length;
   uint64              scan_count;
} scan_benchmark_options;

typedef struct scan_benchmark_milestone_stats {
   uint64 tuples;
   uint64 samples;
   uint64 elapsed_ns_sum;
   uint64 logical_bytes_sum;
} scan_benchmark_milestone_stats;

static inline int
scan_benchmark_status_to_int(platform_status status)
{
   return status.r;
}

static void
scan_benchmark_usage(const char *prog)
{
   platform_error_log("Usage:\n");
   platform_error_log("\t%s [--init-only | --scan-only] [--random-load-order] "
                      "[--splinter-random-keys]\n",
                      prog);
   platform_error_log("\t   [--scan-length <count>] [--scan-count <count>] "
                      "[--random-scan-starts]\n");
   platform_error_log("\t   [--backwards-scan]\n");
   platform_error_log("\t   --num-inserts <count> [generic config options]\n");
   platform_error_log("\n");
   platform_error_log("Modes:\n");
   platform_error_log("\t(default) create/load database, close it, reopen it, "
                      "then scan once\n");
   platform_error_log("\t--init-only  create/load database and exit\n");
   platform_error_log("\t--scan-only  open existing database and scan once\n");
   platform_error_log("\n");
   platform_error_log("Benchmark options:\n");
   platform_error_log("\t--random-load-order   insert keys using a "
                      "deterministic permutation\n");
   platform_error_log("\t--splinter-random-keys use the same TEST_RANDOM key "
                      "mapping as splinter_test\n");
   platform_error_log("\t--scan-length         limit each scan to this many "
                      "returned tuples (0 = full scan)\n");
   platform_error_log(
      "\t--scan-count          number of scans to run (default 1)\n");
   platform_error_log("\t--random-scan-starts  choose a fresh random start key "
                      "for each scan\n");
   platform_error_log("\t--backwards-scan      scan toward smaller keys\n");
   config_usage();
}

static platform_status
scan_benchmark_parse_args(int                     argc,
                          char                   *argv[],
                          scan_benchmark_options *options,
                          int                    *config_argc,
                          char                 ***config_argv)
{
   *options = (scan_benchmark_options){
      .mode                 = SCAN_BENCHMARK_LOAD_AND_SCAN,
      .random_load_order    = FALSE,
      .splinter_random_keys = FALSE,
      .random_scan_starts   = FALSE,
      .backwards_scan       = FALSE,
      .scan_length          = 0,
      .scan_count           = 1,
   };

   char **filtered =
      TYPED_ARRAY_MALLOC(platform_get_heap_id(), filtered, MAX(argc - 1, 1));
   if (filtered == NULL) {
      return STATUS_NO_MEMORY;
   }

   int filtered_count = 0;
   for (int i = 1; i < argc; i++) {
      if (STRING_EQUALS_LITERAL(argv[i], "--init-only")) {
         if (options->mode == SCAN_BENCHMARK_SCAN_ONLY) {
            platform_error_log("scan_benchmark: choose only one of "
                               "--init-only or --scan-only\n");
            platform_free(platform_get_heap_id(), filtered);
            return STATUS_BAD_PARAM;
         }
         options->mode = SCAN_BENCHMARK_INIT_ONLY;
      } else if (STRING_EQUALS_LITERAL(argv[i], "--scan-only")) {
         if (options->mode == SCAN_BENCHMARK_INIT_ONLY) {
            platform_error_log("scan_benchmark: choose only one of "
                               "--init-only or --scan-only\n");
            platform_free(platform_get_heap_id(), filtered);
            return STATUS_BAD_PARAM;
         }
         options->mode = SCAN_BENCHMARK_SCAN_ONLY;
      } else if (STRING_EQUALS_LITERAL(argv[i], "--optimize-only")) {
         // Open an existing DB (cold cache) and time a blocking full-leaf
         // optimize -- a compaction-throughput benchmark.
         options->mode = SCAN_BENCHMARK_OPTIMIZE_ONLY;
      } else if (STRING_EQUALS_LITERAL(argv[i], "--random-load-order")) {
         options->random_load_order = TRUE;
      } else if (STRING_EQUALS_LITERAL(argv[i], "--splinter-random-keys")) {
         options->splinter_random_keys = TRUE;
      } else if (STRING_EQUALS_LITERAL(argv[i], "--random-scan-starts")) {
         options->random_scan_starts = TRUE;
      } else if (STRING_EQUALS_LITERAL(argv[i], "--backwards-scan")) {
         options->backwards_scan = TRUE;
      } else if (STRING_EQUALS_LITERAL(argv[i], "--scan-length")) {
         if (i + 1 == argc
             || !try_string_to_uint64(argv[++i], &options->scan_length))
         {
            platform_error_log(
               "scan_benchmark: failed to parse --scan-length\n");
            platform_free(platform_get_heap_id(), filtered);
            return STATUS_BAD_PARAM;
         }
      } else if (STRING_EQUALS_LITERAL(argv[i], "--scan-count")) {
         if (i + 1 == argc
             || !try_string_to_uint64(argv[++i], &options->scan_count)
             || options->scan_count == 0)
         {
            platform_error_log(
               "scan_benchmark: failed to parse --scan-count\n");
            platform_free(platform_get_heap_id(), filtered);
            return STATUS_BAD_PARAM;
         }
      } else {
         filtered[filtered_count++] = argv[i];
      }
   }

   *config_argc = filtered_count;
   *config_argv = filtered;
   return STATUS_OK;
}

static inline comparison
scan_benchmark_start_comparison(bool32 backwards_scan)
{
   return backwards_scan ? less_than_or_equal : greater_than_or_equal;
}

static inline bool32
scan_benchmark_iterator_can_advance(splinterdb_iterator *iter,
                                    bool32               backwards_scan)
{
   return backwards_scan ? splinterdb_iterator_can_prev(iter)
                         : splinterdb_iterator_can_next(iter);
}

static inline void
scan_benchmark_iterator_advance(splinterdb_iterator *iter,
                                bool32               backwards_scan)
{
   if (backwards_scan) {
      splinterdb_iterator_prev(iter);
   } else {
      splinterdb_iterator_next(iter);
   }
}

static inline void
scan_benchmark_encode_key(uint64 record_no,
                          uint8  keybuf[SCAN_BENCHMARK_KEY_SIZE])
{
   for (uint64 byte_no = 0; byte_no < SCAN_BENCHMARK_KEY_SIZE; byte_no++) {
      uint64 shift    = 8 * (SCAN_BENCHMARK_KEY_SIZE - 1 - byte_no);
      keybuf[byte_no] = (record_no >> shift) & 0xFF;
   }
}

static inline void
scan_benchmark_encode_splinter_random_key(uint64 record_no,
                                          uint8 keybuf[SCAN_BENCHMARK_KEY_SIZE])
{
   uint64 encoded = platform_checksum64(&record_no, sizeof(record_no), 42);
   memcpy(keybuf, &encoded, sizeof(encoded));
}

static inline void
scan_benchmark_encode_record_key(uint64 record_no,
                                 bool32 splinter_random_keys,
                                 uint8  keybuf[SCAN_BENCHMARK_KEY_SIZE])
{
   if (splinter_random_keys) {
      scan_benchmark_encode_splinter_random_key(record_no, keybuf);
   } else {
      scan_benchmark_encode_key(record_no, keybuf);
   }
}

static inline uint64
scan_benchmark_gcd(uint64 a, uint64 b)
{
   while (b != 0) {
      uint64 tmp = a % b;
      a          = b;
      b          = tmp;
   }
   return a;
}

static void
scan_benchmark_choose_permutation(uint64  num_records,
                                  uint64  seed,
                                  uint64 *multiplier,
                                  uint64 *offset)
{
   if (num_records <= 1) {
      *multiplier = 1;
      *offset     = 0;
      return;
   }

   random_state rs;
   random_init(&rs, seed, 0);

   uint64 candidate = 1 + (random_next_uint64(&rs) % (num_records - 1));
   while (scan_benchmark_gcd(candidate, num_records) != 1) {
      candidate++;
      if (candidate >= num_records) {
         candidate = 1;
      }
   }

   *multiplier = candidate;
   *offset     = random_next_uint64(&rs) % num_records;
}

static inline uint64
scan_benchmark_permute_record(uint64 position,
                              uint64 num_records,
                              uint64 multiplier,
                              uint64 offset)
{
   return (uint64)(((__uint128_t)multiplier * position + offset) % num_records);
}

static inline void
scan_benchmark_print_progress(const char *label,
                              uint64      completed,
                              uint64      total,
                              timestamp   start_time)
{
   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
   double elapsed_s  = (double)elapsed_ns / BILLION;
   double pct        = total == 0 ? 100.0 : (100.0 * completed) / total;
   double rate =
      elapsed_ns == 0 ? 0.0 : ((double)completed * BILLION) / elapsed_ns;

   platform_default_log(
      "%s progress: %lu / %lu (%.1f%%), %.2fs elapsed, %.2f ops/s\n",
      label,
      completed,
      total,
      pct,
      elapsed_s,
      rate);
}

static inline double
scan_benchmark_logical_mib_per_sec(uint64 logical_bytes_scanned,
                                   uint64 elapsed_ns)
{
   return elapsed_ns == 0 ? 0.0
                          : ((double)logical_bytes_scanned * BILLION)
                               / elapsed_ns / MiB_TO_B(1);
}

static void
scan_benchmark_print_milestone(uint64 tuples_scanned,
                               uint64 logical_bytes_scanned,
                               uint64 elapsed_ns)
{
   double elapsed_s = (double)elapsed_ns / BILLION;
   double tuples_per_sec =
      elapsed_ns == 0 ? 0.0 : ((double)tuples_scanned * BILLION) / elapsed_ns;
   double mib_per_sec =
      scan_benchmark_logical_mib_per_sec(logical_bytes_scanned, elapsed_ns);
   double ns_per_tuple =
      tuples_scanned == 0 ? 0.0 : (double)elapsed_ns / tuples_scanned;

   platform_default_log(
      "scan milestone: %10lu tuples, %8.3fs elapsed, "
      "%8.2f ns/tuple, %10.2f tuples/s, %8.2f MiB/s logical\n",
      tuples_scanned,
      elapsed_s,
      ns_per_tuple,
      tuples_per_sec,
      mib_per_sec);
}

static void
scan_benchmark_build_milestones(
   uint64  max_tuples,
   uint64  milestones[SCAN_BENCHMARK_MAX_MILESTONES],
   uint64 *milestone_count)
{
   *milestone_count = 0;
   if (max_tuples == 0) {
      return;
   }

   uint64 milestone = 1;
   while (*milestone_count < SCAN_BENCHMARK_MAX_MILESTONES
          && milestone < max_tuples)
   {
      milestones[(*milestone_count)++] = milestone;
      if (milestone > UINT64_MAX / 10) {
         break;
      }
      milestone *= 10;
   }

   if (*milestone_count == 0 || milestones[*milestone_count - 1] != max_tuples)
   {
      platform_assert(*milestone_count < SCAN_BENCHMARK_MAX_MILESTONES);
      milestones[(*milestone_count)++] = max_tuples;
   }
}

static void
scan_benchmark_print_average_milestones(
   uint64                                scans_completed,
   const scan_benchmark_milestone_stats *milestone_stats,
   uint64                                milestone_count)
{
   platform_default_log("average milestones after %lu scan%s:\n",
                        scans_completed,
                        scans_completed == 1 ? "" : "s");
   for (uint64 i = 0; i < milestone_count; i++) {
      const scan_benchmark_milestone_stats *stats = &milestone_stats[i];
      if (stats->samples == 0) {
         continue;
      }

      double tuples_per_sec =
         stats->elapsed_ns_sum == 0
            ? 0.0
            : ((double)stats->tuples * stats->samples * BILLION)
                 / stats->elapsed_ns_sum;
      double mib_per_sec = scan_benchmark_logical_mib_per_sec(
         stats->logical_bytes_sum, stats->elapsed_ns_sum);
      double ns_per_tuple =
         (stats->tuples == 0 || stats->samples == 0)
            ? 0.0
            : (double)stats->elapsed_ns_sum / (stats->tuples * stats->samples);

      platform_default_log("  %10lu tuples over %6lu scan%s: %8.2f ns/tuple, "
                           "%10.2f tuples/s, %8.2f MiB/s logical\n",
                           stats->tuples,
                           stats->samples,
                           stats->samples == 1 ? "" : "s",
                           ns_per_tuple,
                           tuples_per_sec,
                           mib_per_sec);
   }
}

static void
scan_benchmark_make_config(const master_config *master_cfg,
                           data_config         *data_cfg,
                           splinterdb_config   *cfg,
                           bool                 open_existing)
{
   *cfg = (splinterdb_config){
      .filename                 = master_cfg->io_filename,
      .cache_size               = master_cfg->cache_capacity,
      .disk_size                = master_cfg->allocator_capacity,
      .data_cfg                 = data_cfg,
      .use_shmem                = master_cfg->use_shmem,
      .shmem_size               = master_cfg->shmem_size,
      .page_size                = master_cfg->page_size,
      .extent_size              = master_cfg->extent_size,
      .io_flags                 = master_cfg->io_flags,
      .io_perms                 = master_cfg->io_perms,
      .io_async_queue_depth     = master_cfg->io_async_queue_depth,
      .cache_use_stats          = master_cfg->use_stats,
      .cache_logfile            = master_cfg->cache_logfile,
      .num_memtable_bg_threads  = master_cfg->num_memtable_bg_threads,
      .num_normal_bg_threads    = master_cfg->num_normal_bg_threads,
      .btree_rough_count_height = master_cfg->btree_rough_count_height,
      .filter_hash_size         = master_cfg->filter_hash_size,
      .filter_log_index_size    = master_cfg->filter_log_index_size,
      .use_log                  = master_cfg->use_log,
      .memtable_capacity        = master_cfg->memtable_capacity,
      .fanout                   = master_cfg->fanout,
      .use_stats                = master_cfg->use_stats,
      .reclaim_threshold        = master_cfg->reclaim_threshold,
      .queue_scale_percent      = master_cfg->queue_scale_percent,
      .prefetch_budget          = master_cfg->prefetch_budget,
   };

   if (open_existing) {
      cfg->io_flags &= ~O_CREAT;
   }
}

static int
scan_benchmark_load_database(const splinterdb_config *cfg,
                             uint64                   num_records,
                             uint64                   value_size,
                             bool32                   random_load_order,
                             bool32                   splinter_random_keys,
                             uint64                   seed)
{
   splinterdb *kvs = NULL;
   int         rc  = splinterdb_create(cfg, &kvs);
   if (rc != 0) {
      return rc;
   }

   uint8 *value_buf =
      TYPED_ARRAY_ZALLOC(platform_get_heap_id(), value_buf, value_size);
   if (value_buf == NULL) {
      splinterdb_close(&kvs);
      return scan_benchmark_status_to_int(STATUS_NO_MEMORY);
   }

   uint8     keybuf[SCAN_BENCHMARK_KEY_SIZE];
   slice     value                  = slice_create(value_size, value_buf);
   timestamp start_time             = platform_get_timestamp();
   uint64    progress_interval      = MAX(num_records / 10, 1);
   uint64    permutation_multiplier = 1;
   uint64    permutation_offset     = 0;

   if (random_load_order) {
      scan_benchmark_choose_permutation(num_records,
                                        seed ^ 0x9e3779b97f4a7c15ULL,
                                        &permutation_multiplier,
                                        &permutation_offset);
      platform_default_log("scan_benchmark: random load order enabled "
                           "(multiplier=%lu offset=%lu)\n",
                           permutation_multiplier,
                           permutation_offset);
   }

   platform_default_log("scan_benchmark: loading %lu records\n", num_records);

   for (uint64 record_no = 0; record_no < num_records; record_no++) {
      uint64 key_record_no =
         random_load_order
            ? scan_benchmark_permute_record(record_no,
                                            num_records,
                                            permutation_multiplier,
                                            permutation_offset)
            : record_no;
      scan_benchmark_encode_record_key(
         key_record_no, splinter_random_keys, keybuf);
      slice key = slice_create(sizeof(keybuf), keybuf);
      rc        = splinterdb_insert(kvs, key, value, NULL);
      if (rc != 0) {
         platform_error_log(
            "scan_benchmark: insert failed at record %lu: %d\n", record_no, rc);
         break;
      }

      if ((record_no + 1) % progress_interval == 0
          || record_no + 1 == num_records)
      {
         scan_benchmark_print_progress(
            "load", record_no + 1, num_records, start_time);
      }
   }

   platform_free(platform_get_heap_id(), value_buf);
   splinterdb_close(&kvs);
   return rc;
}

/*
 * Compaction-throughput benchmark: open an existing DB (cold cache) and time a
 * single blocking full-leaf optimize over the whole key range. With data > cache
 * and O_DIRECT, the branch reads done by compaction's merge iterators are cold,
 * so this exercises the btree-iterator prefetch path under compaction.
 */
static int
scan_benchmark_run_optimize(const splinterdb_config *cfg)
{
   splinterdb *kvs = NULL;
   int         rc  = splinterdb_open(cfg, &kvs);
   if (rc != 0) {
      return rc;
   }

   io_reset_stats((io_handle *)splinterdb_get_io_handle(kvs));

   splinterdb_notification notification;
   splinterdb_notification_init_blocking(&notification);

   platform_default_log("scan_benchmark: running blocking full-leaf optimize\n");
   timestamp start_time = platform_get_timestamp();
   rc = splinterdb_optimize(kvs, NULL_SLICE, NULL_SLICE, TRUE, &notification);
   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
   splinterdb_notification_deinit(&notification);

   platform_default_log("optimize complete: rc=%d, %.3fs elapsed\n",
                        rc,
                        (double)elapsed_ns / BILLION);
   io_print_stats((io_handle *)splinterdb_get_io_handle(kvs),
                  Platform_default_log_handle);

   splinterdb_close(&kvs);
   return rc;
}

static int
scan_benchmark_run_scan(const splinterdb_config *cfg,
                        bool                     print_lookup_stats,
                        uint64                   expected_records,
                        bool32                   backwards_scan)
{
   splinterdb *kvs = NULL;
   int         rc  = splinterdb_open(cfg, &kvs);
   if (rc != 0) {
      return rc;
   }

   splinterdb_stats_reset(kvs);
   io_reset_stats((io_handle *)splinterdb_get_io_handle(kvs));

   splinterdb_iterator *iter       = NULL;
   timestamp            start_time = platform_get_timestamp();
   rc                              = splinterdb_iterator_init(
      kvs, &iter, scan_benchmark_start_comparison(backwards_scan), NULL_SLICE);
   if (rc != 0) {
      splinterdb_close(&kvs);
      return rc;
   }

   uint64 next_milestone        = 1;
   uint64 tuples_scanned        = 0;
   uint64 logical_bytes_scanned = 0;

   while (splinterdb_iterator_valid(iter)) {
      slice key;
      slice value;
      splinterdb_iterator_get_current(iter, &key, &value);
      tuples_scanned++;
      logical_bytes_scanned += slice_length(key) + slice_length(value);

      if (tuples_scanned == next_milestone) {
         scan_benchmark_print_milestone(tuples_scanned,
                                        logical_bytes_scanned,
                                        platform_timestamp_elapsed(start_time));
         if (next_milestone <= UINT64_MAX / 10) {
            next_milestone *= 10;
         }
      }

      if (!scan_benchmark_iterator_can_advance(iter, backwards_scan)) {
         break;
      }
      scan_benchmark_iterator_advance(iter, backwards_scan);
   }

   rc = splinterdb_iterator_status(iter);
   if (rc == 0
       && (tuples_scanned == 0 || tuples_scanned != next_milestone / 10))
   {
      scan_benchmark_print_milestone(tuples_scanned,
                                     logical_bytes_scanned,
                                     platform_timestamp_elapsed(start_time));
   }

   uint64 total_elapsed_ns = platform_timestamp_elapsed(start_time);
   platform_default_log("scan complete: %lu tuples, %.2f MiB/s logical\n",
                        tuples_scanned,
                        scan_benchmark_logical_mib_per_sec(
                           logical_bytes_scanned, total_elapsed_ns));

   if (expected_records != 0 && expected_records != tuples_scanned) {
      platform_error_log(
         "scan_benchmark: expected %lu tuples but scanned %lu\n",
         expected_records,
         tuples_scanned);
      rc = EINVAL;
   }

   if (print_lookup_stats) {
      splinterdb_stats_print_lookup(kvs);
   }
   io_print_stats((io_handle *)splinterdb_get_io_handle(kvs),
                  Platform_default_log_handle);

   splinterdb_iterator_deinit(iter);
   splinterdb_close(&kvs);
   return rc;
}

static int
scan_benchmark_run_repeated_scans(const splinterdb_config *cfg,
                                  bool                     print_lookup_stats,
                                  uint64                   expected_records,
                                  uint64                   scan_length,
                                  uint64                   scan_count,
                                  bool32                   splinter_random_keys,
                                  bool32                   random_scan_starts,
                                  bool32                   backwards_scan,
                                  uint64                   seed)
{
   splinterdb *kvs = NULL;
   int         rc  = splinterdb_open(cfg, &kvs);
   if (rc != 0) {
      return rc;
   }

   splinterdb_stats_reset(kvs);
   io_reset_stats((io_handle *)splinterdb_get_io_handle(kvs));

   uint64 effective_scan_length =
      scan_length == 0 ? expected_records : scan_length;
   if (effective_scan_length == 0) {
      platform_error_log("scan_benchmark: repeated scans require a non-zero "
                         "scan length or --num-inserts\n");
      splinterdb_close(&kvs);
      return EINVAL;
   }

   uint64 milestones[SCAN_BENCHMARK_MAX_MILESTONES];
   uint64 milestone_count = 0;
   scan_benchmark_build_milestones(
      effective_scan_length, milestones, &milestone_count);

   scan_benchmark_milestone_stats
      milestone_stats[SCAN_BENCHMARK_MAX_MILESTONES];
   ZERO_ARRAY(milestone_stats);
   for (uint64 i = 0; i < milestone_count; i++) {
      milestone_stats[i].tuples = milestones[i];
   }

   random_state rs;
   random_init(&rs, seed ^ 0xd1b54a32d192ed03ULL, 0);

   uint64 report_interval             = MAX(scan_count / 10, 1);
   uint64 total_elapsed_ns            = 0;
   uint64 total_tuples_scanned        = 0;
   uint64 total_logical_bytes_scanned = 0;
   uint8  keybuf[SCAN_BENCHMARK_KEY_SIZE];

   platform_default_log(
      "scan_benchmark: running %lu %s scan%s of up to %lu tuple%s%s\n",
      scan_count,
      backwards_scan ? "backwards" : "forward",
      scan_count == 1 ? "" : "s",
      effective_scan_length,
      effective_scan_length == 1 ? "" : "s",
      random_scan_starts ? " from random starting points" : "");

   for (uint64 scan_no = 0; scan_no < scan_count; scan_no++) {
      slice  start_key       = NULL_SLICE;
      uint64 start_record_no = 0;
      if (random_scan_starts) {
         platform_assert(expected_records > 0);
         start_record_no = random_next_uint64(&rs) % expected_records;
         scan_benchmark_encode_record_key(
            start_record_no, splinter_random_keys, keybuf);
         start_key = slice_create(sizeof(keybuf), keybuf);
      }

      splinterdb_iterator *iter       = NULL;
      timestamp            start_time = platform_get_timestamp();
      rc                              = splinterdb_iterator_init(
         kvs,
         &iter,
         scan_benchmark_start_comparison(backwards_scan),
         start_key);
      if (rc != 0) {
         splinterdb_close(&kvs);
         return rc;
      }

      uint64 tuples_scanned        = 0;
      uint64 logical_bytes_scanned = 0;
      uint64 milestone_idx         = 0;
      uint64 target_tuples         = effective_scan_length;

      while (splinterdb_iterator_valid(iter) && tuples_scanned < target_tuples)
      {
         slice key;
         slice value;
         splinterdb_iterator_get_current(iter, &key, &value);
         tuples_scanned++;
         logical_bytes_scanned += slice_length(key) + slice_length(value);

         while (milestone_idx < milestone_count
                && tuples_scanned == milestones[milestone_idx])
         {
            uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
            milestone_stats[milestone_idx].samples++;
            milestone_stats[milestone_idx].elapsed_ns_sum += elapsed_ns;
            milestone_stats[milestone_idx].logical_bytes_sum +=
               logical_bytes_scanned;
            milestone_idx++;
         }

         if (tuples_scanned >= target_tuples
             || !scan_benchmark_iterator_can_advance(iter, backwards_scan))
         {
            break;
         }
         scan_benchmark_iterator_advance(iter, backwards_scan);
      }

      rc = splinterdb_iterator_status(iter);
      if (rc != 0) {
         splinterdb_iterator_deinit(iter);
         splinterdb_close(&kvs);
         return rc;
      }

      uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
      total_elapsed_ns += elapsed_ns;
      total_tuples_scanned += tuples_scanned;
      total_logical_bytes_scanned += logical_bytes_scanned;

      splinterdb_iterator_deinit(iter);

      bool32 should_report = (scan_no + 1 <= 3) || (scan_no + 1 == scan_count)
                             || ((scan_no + 1) % report_interval == 0);
      if (should_report) {
         double avg_ns_per_tuple =
            total_tuples_scanned == 0
               ? 0.0
               : (double)total_elapsed_ns / total_tuples_scanned;
         double logical_mib_per_sec = scan_benchmark_logical_mib_per_sec(
            total_logical_bytes_scanned, total_elapsed_ns);
         platform_default_log("scan progress: %lu / %lu scans complete, "
                              "last_start=%lu, last_tuples=%lu, "
                              "cumulative %.2f ns/tuple, "
                              "%.2f MiB/s logical\n",
                              scan_no + 1,
                              scan_count,
                              start_record_no,
                              tuples_scanned,
                              avg_ns_per_tuple,
                              logical_mib_per_sec);
         scan_benchmark_print_average_milestones(
            scan_no + 1, milestone_stats, milestone_count);
      }
   }

   platform_default_log("scan complete: %lu scans, %lu tuples, "
                        "%.2f MiB/s logical\n",
                        scan_count,
                        total_tuples_scanned,
                        scan_benchmark_logical_mib_per_sec(
                           total_logical_bytes_scanned, total_elapsed_ns));

   if (print_lookup_stats) {
      splinterdb_stats_print_lookup(kvs);
   }
   io_print_stats((io_handle *)splinterdb_get_io_handle(kvs),
                  Platform_default_log_handle);

   splinterdb_close(&kvs);
   return rc;
}

int
scan_benchmark(int argc, char *argv[])
{
   platform_status        status;
   scan_benchmark_options options;
   int                    config_argc = 0;
   char                 **config_argv = NULL;
   master_config          master_cfg;
   test_workload_config   workload_cfg;
   data_config            default_data_cfg;
   splinterdb_config      cfg;
   int                    rc = 0;

   if (argc > 1 && STRING_EQUALS_LITERAL(argv[1], "--help")) {
      scan_benchmark_usage(argv[0]);
      return 0;
   }

   platform_register_thread();
   config_set_defaults(&master_cfg);

   status = scan_benchmark_parse_args(
      argc, argv, &options, &config_argc, &config_argv);
   if (!SUCCESS(status)) {
      rc = scan_benchmark_status_to_int(status);
      goto out;
   }

   status = config_parse(&master_cfg, 1, config_argc, config_argv);
   if (!SUCCESS(status)) {
      rc = scan_benchmark_status_to_int(status);
      goto out;
   }
   test_workload_config_init(&workload_cfg, &master_cfg);

   if (workload_cfg.key_size < SCAN_BENCHMARK_KEY_SIZE) {
      platform_error_log("scan_benchmark: key-size must be at least %u bytes\n",
                         SCAN_BENCHMARK_KEY_SIZE);
      rc = EINVAL;
      goto out;
   }

   if (options.mode != SCAN_BENCHMARK_SCAN_ONLY
       && options.mode != SCAN_BENCHMARK_OPTIMIZE_ONLY
       && master_cfg.num_inserts == 0)
   {
      platform_error_log(
         "scan_benchmark: --num-inserts must be set for load modes\n");
      rc = EINVAL;
      goto out;
   }

   if (options.random_scan_starts && master_cfg.num_inserts == 0) {
      platform_error_log("scan_benchmark: --random-scan-starts requires "
                         "--num-inserts to describe the keyspace\n");
      rc = EINVAL;
      goto out;
   }

   default_data_config_init(&default_data_cfg);

   platform_default_log(
      "scan_benchmark: db=%s mode=%d num_inserts=%lu "
      "cache=%lu extent=%lu value=%lu "
      "random_load=%d splinter_random_keys=%d scan_length=%lu scan_count=%lu "
      "random_starts=%d backwards_scan=%d seed=%lu\n",
      master_cfg.io_filename,
      options.mode,
      master_cfg.num_inserts,
      master_cfg.cache_capacity,
      master_cfg.extent_size,
      workload_cfg.message_size,
      options.random_load_order,
      options.splinter_random_keys,
      options.scan_length,
      options.scan_count,
      options.random_scan_starts,
      options.backwards_scan,
      master_cfg.seed);

   if (options.mode == SCAN_BENCHMARK_LOAD_AND_SCAN
       || options.mode == SCAN_BENCHMARK_INIT_ONLY)
   {
      scan_benchmark_make_config(&master_cfg, &default_data_cfg, &cfg, FALSE);
      rc = scan_benchmark_load_database(&cfg,
                                        master_cfg.num_inserts,
                                        workload_cfg.message_size,
                                        options.random_load_order,
                                        options.splinter_random_keys,
                                        master_cfg.seed);
      if (rc != 0 || options.mode == SCAN_BENCHMARK_INIT_ONLY) {
         goto out;
      }
   }

   scan_benchmark_make_config(&master_cfg, &default_data_cfg, &cfg, TRUE);
   if (options.mode == SCAN_BENCHMARK_OPTIMIZE_ONLY) {
      rc = scan_benchmark_run_optimize(&cfg);
   } else if (options.scan_count == 1 && options.scan_length == 0
              && !options.random_scan_starts)
   {
      rc = scan_benchmark_run_scan(&cfg,
                                   master_cfg.use_stats,
                                   master_cfg.num_inserts,
                                   options.backwards_scan);
   } else {
      rc = scan_benchmark_run_repeated_scans(&cfg,
                                             master_cfg.use_stats,
                                             master_cfg.num_inserts,
                                             options.scan_length,
                                             options.scan_count,
                                             options.splinter_random_keys,
                                             options.random_scan_starts,
                                             options.backwards_scan,
                                             master_cfg.seed);
   }

out:
   if (config_argv != NULL) {
      platform_free(platform_get_heap_id(), config_argv);
   }
   platform_deregister_thread();
   return rc;
}
