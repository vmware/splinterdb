// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * Open an existing SplinterDB disk image and optimize a key range.
 *
 * This is intended as a small utility for preparing a database image before
 * point/range-query benchmarking.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"

#define DEFAULT_CACHE_SIZE_MIB      1024
#define DEFAULT_NORMAL_BG_THREADS   2
#define DEFAULT_MEMTABLE_BG_THREADS 1
#define BYTES_PER_MIB               (1024ULL * 1024ULL)
#define OPTIMIZE_HELP_REQUESTED     (-1)

typedef struct optimize_options {
   const char *filename;
   const char *min_key;
   const char *max_key;
   uint64      cache_size;
   uint64      disk_size;
   uint64      num_normal_bg_threads;
   uint64      num_memtable_bg_threads;
} optimize_options;

static void
usage(const char *progname);

static int
parse_uint64(const char *arg, uint64 *result);

static int
parse_options(int argc, char **argv, optimize_options *opts);

int
main(int argc, char **argv)
{
   optimize_options opts = {
      .cache_size              = DEFAULT_CACHE_SIZE_MIB * BYTES_PER_MIB,
      .num_normal_bg_threads   = DEFAULT_NORMAL_BG_THREADS,
      .num_memtable_bg_threads = DEFAULT_MEMTABLE_BG_THREADS,
   };

   int rc = parse_options(argc, argv, &opts);
   if (rc == OPTIMIZE_HELP_REQUESTED) {
      return 0;
   }
   if (rc != 0) {
      return rc;
   }

   data_config data_cfg;
   default_data_config_init(&data_cfg);

   splinterdb_config splinterdb_cfg;
   memset(&splinterdb_cfg, 0, sizeof(splinterdb_cfg));
   splinterdb_cfg.filename                = opts.filename;
   splinterdb_cfg.cache_size              = opts.cache_size;
   splinterdb_cfg.disk_size               = opts.disk_size;
   splinterdb_cfg.data_cfg                = &data_cfg;
   splinterdb_cfg.num_normal_bg_threads   = opts.num_normal_bg_threads;
   splinterdb_cfg.num_memtable_bg_threads = opts.num_memtable_bg_threads;

   splinterdb *spl = NULL;
   rc              = splinterdb_open(&splinterdb_cfg, &spl);
   if (rc != 0) {
      fprintf(stderr, "splinterdb_open(%s) failed: %d\n", opts.filename, rc);
      return rc;
   }

   slice min_key = opts.min_key == NULL
                      ? NULL_SLICE
                      : slice_create(strlen(opts.min_key), opts.min_key);
   slice max_key = opts.max_key == NULL
                      ? NULL_SLICE
                      : slice_create(strlen(opts.max_key), opts.max_key);

   splinterdb_notification notification;
   splinterdb_notification_init_blocking(&notification);

   printf("Optimizing %s in [%s, %s)...\n",
          opts.filename,
          opts.min_key == NULL ? "-infinity" : opts.min_key,
          opts.max_key == NULL ? "+infinity" : opts.max_key);

   rc = splinterdb_optimize(spl, min_key, max_key, &notification);
   splinterdb_notification_deinit(&notification);

   if (rc == 0) {
      printf("Optimize completed successfully.\n");
   } else {
      fprintf(stderr, "splinterdb_optimize failed: %d\n", rc);
   }

   splinterdb_close(&spl);
   return rc;
}

static void
usage(const char *progname)
{
   fprintf(stderr,
           "Usage: %s [options] <splinterdb-image>\n"
           "\n"
           "Options:\n"
           "  --min-key <key>              Inclusive lower bound\n"
           "  --max-key <key>              Exclusive upper bound\n"
           "  --cache-mib <mib>            Cache size in MiB (default %u)\n"
           "  --disk-size-mib <mib>        Check image size in MiB\n"
           "  --normal-bg-threads <count>  Normal task threads (default %u)\n"
           "  --memtable-bg-threads <cnt>  Memtable task threads (default %u)\n"
           "  --help                       Show this help\n",
           progname,
           DEFAULT_CACHE_SIZE_MIB,
           DEFAULT_NORMAL_BG_THREADS,
           DEFAULT_MEMTABLE_BG_THREADS);
}

static int
parse_uint64(const char *arg, uint64 *result)
{
   char *end = NULL;

   errno   = 0;
   *result = strtoull(arg, &end, 10);
   if (errno != 0 || end == arg || *end != '\0') {
      return EINVAL;
   }

   return 0;
}

static int
parse_options(int argc, char **argv, optimize_options *opts)
{
   for (int i = 1; i < argc; i++) {
      if (strcmp(argv[i], "--help") == 0) {
         usage(argv[0]);
         return OPTIMIZE_HELP_REQUESTED;
      } else if (strcmp(argv[i], "--min-key") == 0) {
         if (++i == argc) {
            usage(argv[0]);
            return EINVAL;
         }
         opts->min_key = argv[i];
      } else if (strcmp(argv[i], "--max-key") == 0) {
         if (++i == argc) {
            usage(argv[0]);
            return EINVAL;
         }
         opts->max_key = argv[i];
      } else if (strcmp(argv[i], "--cache-mib") == 0) {
         uint64 mib;
         if (++i == argc || parse_uint64(argv[i], &mib) != 0) {
            usage(argv[0]);
            return EINVAL;
         }
         opts->cache_size = mib * BYTES_PER_MIB;
      } else if (strcmp(argv[i], "--disk-size-mib") == 0) {
         uint64 mib;
         if (++i == argc || parse_uint64(argv[i], &mib) != 0) {
            usage(argv[0]);
            return EINVAL;
         }
         opts->disk_size = mib * BYTES_PER_MIB;
      } else if (strcmp(argv[i], "--normal-bg-threads") == 0) {
         if (++i == argc
             || parse_uint64(argv[i], &opts->num_normal_bg_threads) != 0)
         {
            usage(argv[0]);
            return EINVAL;
         }
      } else if (strcmp(argv[i], "--memtable-bg-threads") == 0) {
         if (++i == argc
             || parse_uint64(argv[i], &opts->num_memtable_bg_threads) != 0)
         {
            usage(argv[0]);
            return EINVAL;
         }
      } else if (opts->filename == NULL) {
         opts->filename = argv[i];
      } else {
         usage(argv[0]);
         return EINVAL;
      }
   }

   if (opts->filename == NULL) {
      usage(argv[0]);
      return EINVAL;
   }
   if (opts->num_normal_bg_threads == 0) {
      fprintf(stderr,
              "--normal-bg-threads must be greater than 0 for blocking "
              "optimize completion.\n");
      return EINVAL;
   }

   return 0;
}
