// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "config.h"
#include "util.h"

/*
 * --------------------------------------------------------------------------
 * Default test configuration settings. These will be used by
 * config_set_defaults() to initialize test-execution configuration in the
 * master_config used to run tests. See also config.h, where few default
 * config limits used outside this file are defined.
 * --------------------------------------------------------------------------
 */
// Determined empirically ... nothing scientific here
#define TEST_CONFIG_DEFAULT_IO_ASYNC_Q_DEPTH 256

// Provide sufficient disk, cache and memtable capacity to get somewhat
// realistic configuration for most tests.
#define TEST_CONFIG_DEFAULT_DISK_SIZE_GB         30
#define TEST_CONFIG_DEFAULT_CACHE_SIZE_GB        1
#define TEST_CONFIG_DEFAULT_MEMTABLE_CAPACITY_MB 24
#define TEST_CONFIG_DEFAULT_SHMEM_SIZE_GB        2

// Setup reasonable BTree and branch tree configurations
#define TEST_CONFIG_DEFAULT_FILTER_HASH_SIZE      26
#define TEST_CONFIG_DEFAULT_FILTER_LOG_INDEX_SIZE 8
#define TEST_CONFIG_DEFAULT_FANOUT                8

// Deal with reasonable key / message sizes for tests
// There are open issues in some tests for smaller key-sizes.
// For now, restrict tests to use this minimum key-size.
#define TEST_CONFIG_MIN_KEY_SIZE         ((int)sizeof(uint64))
#define TEST_CONFIG_DEFAULT_KEY_SIZE     24
#define TEST_CONFIG_DEFAULT_MESSAGE_SIZE 100

// Configs that are usually changed by different tests
#define TEST_CONFIG_DEFAULT_SEED        0
#define TEST_CONFIG_DEFAULT_NUM_INSERTS 0

// By default, background threads are disabled in Splinter task system.
// Most tests run w/o background threads. Very small # of tests exercise
// background threads through the --num-normal-bg-threads and
// --num-memtable-bg-threads options.
#define TEST_CONFIG_DEFAULT_NUM_NORMAL_BG_THREADS   0
#define TEST_CONFIG_DEFAULT_NUM_MEMTABLE_BG_THREADS 0

#define TEST_CONFIG_DEFAULT_QUEUE_SCALE_PERCENT (100)

// clang-format off
/*
 * ---------------------------------------------------------------------------
 * Helper function to initialize master_config{} used to run tests with some
 * useful default values. The expectation is that the input 'cfg' is zero'ed
 * out before calling this initializer, so that all other fields will have
 * some reasonable 0-defaults.
 *
 * ******************* EXPERIMENTAL FEATURES ********************
 *  - use_shmem: Support for shared memory segments.
 *    This functionality is solely meant for internal development uses.
 *    We don't support free(), so your test / usage will likely run into
 *    shared-memory OOMs errors.
 *
 * ---------------------------------------------------------------------------
 */
void
config_set_defaults(master_config *cfg)
{
   *cfg = (master_config){
      .io_filename              = "db",
      .cache_logfile            = "cache_log",
      .page_size                = TEST_CONFIG_DEFAULT_PAGE_SIZE,
      .extent_size              = TEST_CONFIG_DEFAULT_EXTENT_SIZE,
      .io_flags                 = O_RDWR | O_CREAT,
      .io_perms                 = 0755,
      .io_async_queue_depth     = TEST_CONFIG_DEFAULT_IO_ASYNC_Q_DEPTH,
      .allocator_capacity       = GiB_TO_B(TEST_CONFIG_DEFAULT_DISK_SIZE_GB),
      .cache_capacity           = GiB_TO_B(TEST_CONFIG_DEFAULT_CACHE_SIZE_GB),
      .btree_rough_count_height = 1,
      .filter_hash_size    = TEST_CONFIG_DEFAULT_FILTER_HASH_SIZE,
      .filter_log_index_size        = TEST_CONFIG_DEFAULT_FILTER_LOG_INDEX_SIZE,
      .use_log                  = FALSE,
      .num_normal_bg_threads    = TEST_CONFIG_DEFAULT_NUM_NORMAL_BG_THREADS,
      .num_memtable_bg_threads  = TEST_CONFIG_DEFAULT_NUM_MEMTABLE_BG_THREADS,
      .memtable_capacity        = MiB_TO_B(TEST_CONFIG_DEFAULT_MEMTABLE_CAPACITY_MB),
      .fanout                   = TEST_CONFIG_DEFAULT_FANOUT,
      .use_stats                = FALSE,
      .reclaim_threshold        = UINT64_MAX,
      .queue_scale_percent      = TEST_CONFIG_DEFAULT_QUEUE_SCALE_PERCENT,
      .verbose_logging_enabled  = FALSE,
      .verbose_progress         = FALSE,

      .use_shmem                = FALSE,
      // Default shared-memory sze if it is configured
     .shmem_size                = GiB_TO_B(TEST_CONFIG_DEFAULT_SHMEM_SIZE_GB),
     .wait_for_gdb              = FALSE,

      .log_handle               = NULL,
      .max_key_size             = TEST_CONFIG_DEFAULT_KEY_SIZE,
      .message_size             = TEST_CONFIG_DEFAULT_MESSAGE_SIZE,
      .num_inserts              = TEST_CONFIG_DEFAULT_NUM_INSERTS,
      .seed                     = TEST_CONFIG_DEFAULT_SEED,
   };
}
// clang-format on

void
config_usage()
{
   platform_error_log("\nConfiguration: (default)\n");
   platform_error_log("\t--page-size (%d)\n", TEST_CONFIG_DEFAULT_PAGE_SIZE);
   platform_error_log("\t--extent-size (%d)\n",
                      TEST_CONFIG_DEFAULT_EXTENT_SIZE);
   platform_error_log("\t--set-hugetlb\n");
   platform_error_log("\t--unset-hugetlb\n");
   platform_error_log("\t--set-mlock\n");
   platform_error_log("\t--unset-mlock\n");
   platform_error_log("\t--db-location\n");
   platform_error_log("\t--set-O_DIRECT\n");
   platform_error_log("\t--unset-O_DIRECT\n");
   platform_error_log("\t--set-O_CREAT\n");
   platform_error_log("\t--unset-O_CREAT\n");
   platform_error_log("\t--db-perms\n");
   platform_error_log("\t--db-capacity-gib (%d)\n",
                      TEST_CONFIG_DEFAULT_DISK_SIZE_GB);
   platform_error_log("\t--db-capacity-mib (%d)\n",
                      (int)(TEST_CONFIG_DEFAULT_DISK_SIZE_GB * KiB));
   platform_error_log("\t--libaio-queue-depth\n");
   platform_error_log("\t--cache-capacity-gib (%d)\n",
                      TEST_CONFIG_DEFAULT_CACHE_SIZE_GB);
   platform_error_log("\t--cache-capacity-mib (%d)\n",
                      (int)(TEST_CONFIG_DEFAULT_CACHE_SIZE_GB * KiB));
   platform_error_log("\t--cache-debug-log\n");
   platform_error_log("\t--queue-scale-percent (%d)\n",
                      TEST_CONFIG_DEFAULT_QUEUE_SCALE_PERCENT);
   platform_error_log("\t--memtable-capacity-gib\n");
   platform_error_log("\t--memtable-capacity-mib (%d)\n",
                      TEST_CONFIG_DEFAULT_MEMTABLE_CAPACITY_MB);
   platform_error_log("\t--rough-count-height\n");
   platform_error_log("\t--filter-remainder-size\n");
   platform_error_log("\t--fanout (%d)\n", TEST_CONFIG_DEFAULT_FANOUT);

   platform_error_log("\t--num-normal-bg-threads (%d)\n",
                      TEST_CONFIG_DEFAULT_NUM_NORMAL_BG_THREADS);
   platform_error_log("\t--num-memtable-bg-threads (%d)\n",
                      TEST_CONFIG_DEFAULT_NUM_MEMTABLE_BG_THREADS);

   platform_error_log("\t--stats\n");
   platform_error_log("\t--no-stats\n");
   platform_error_log("\t--log\n");
   platform_error_log("\t--no-log\n");
   platform_error_log("\t--verbose-logging\n");
   platform_error_log("\t--no-verbose-logging\n");
   platform_error_log("\t--verbose-progress\n");

   platform_error_log(
      "\t--use-shmem           **** Experimental feature ****\n");
   // clang-format off
   platform_error_log("\t       [ --trace-shmem | --trace-shmem-allocs | --trace-shmem-frees ]\n");
   platform_error_log("\t       [ --shmem-capacity-mib <mb> (%lu) | --shmem-capacity-gib <gb> (%d) ]\n",
                      (TEST_CONFIG_DEFAULT_SHMEM_SIZE_GB * KiB),
                      TEST_CONFIG_DEFAULT_SHMEM_SIZE_GB);
   // clang-format on

   platform_error_log("\t--key-size (%d)\n", TEST_CONFIG_DEFAULT_KEY_SIZE);
   platform_error_log("\t--data-size (%d)\n", TEST_CONFIG_DEFAULT_MESSAGE_SIZE);
   platform_error_log("\t--num-inserts (%d)\n",
                      TEST_CONFIG_DEFAULT_NUM_INSERTS);
   platform_error_log("\t--seed (%d)\n", TEST_CONFIG_DEFAULT_SEED);
}

/*
 * config_parse_use_shmem() - Check if --use-shmem argument was supplied on
 * the cmdline. Some tests need to know this to setup the shared memory heap
 * before other test configuration is done.
 */
bool
config_parse_use_shmem(int argc, char *argv[])
{
   master_config master_cfg;
   config_set_defaults(&master_cfg);
   platform_status rc = config_parse(&master_cfg, 1, argc, argv);
   platform_assert(SUCCESS(rc), "Failed to parse config arguments.");
   return master_cfg.use_shmem;
}

/*
 * config_parse() --
 *
 * Rudimentary command-line argument parser used by tests. --<config> options
 * are sourced into a master_config structure, which then gets used to setup
 * the configuration of various sub-systems.
 */
platform_status
config_parse(master_config *cfg, const uint8 num_config, int argc, char *argv[])
{
   uint64 i;
   for (i = 0; i < argc; i++) {
      // Don't be mislead; this is not dead-code. See the config macro expansion
      if (0) {
         config_set_uint64("page-size", cfg, page_size)
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               if (cfg[cfg_idx].page_size != TEST_CONFIG_DEFAULT_PAGE_SIZE) {
                  platform_error_log("Currently, configuration parameter '%s' "
                                     "is restricted to %d bytes.\n",
                                     "--page-size",
                                     TEST_CONFIG_DEFAULT_PAGE_SIZE);
                  platform_error_log("config: failed to parse page-size\n");
                  return STATUS_BAD_PARAM;
               }
               // Really dead-code for now; Leave it for future enablement.
               if (!IS_POWER_OF_2(cfg[cfg_idx].page_size)) {
                  platform_error_log("Configuration parameter '%s' must be "
                                     "a power of 2.\n",
                                     "--page-size");
                  platform_error_log("config: failed to parse page-size\n");
                  return STATUS_BAD_PARAM;
               }
            }
         }
         config_set_uint64("extent-size", cfg, extent_size)
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               if (!IS_POWER_OF_2(cfg[cfg_idx].extent_size)) {
                  platform_error_log("Configuration parameter '%s' must be "
                                     "a power of 2.\n",
                                     "--extent-size");
                  platform_error_log("config: failed to parse page-size\n");
                  return STATUS_BAD_PARAM;
               }
            }
         }
         config_has_option("set-hugetlb")
         {
            platform_use_hugetlb = TRUE;
         }
         config_has_option("unset-hugetlb")
         {
            platform_use_hugetlb = FALSE;
         }
         config_has_option("set-mlock")
         {
            platform_use_mlock = TRUE;
         }
         config_has_option("unset-mlock")
         {
            platform_use_mlock = FALSE;
         }
         config_set_string("db-location", cfg, io_filename) {}
         config_has_option("set-O_DIRECT")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].io_flags |= O_DIRECT;
            }
         }
         config_has_option("unset-O_DIRECT")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].io_flags &= ~O_DIRECT;
            }
         }
         config_has_option("set-O_CREAT")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].io_flags |= O_CREAT;
            }
         }
         config_has_option("unset-O_CREAT")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].io_flags &= ~O_CREAT;
            }
         }
         config_set_uint32("db-perms", cfg, io_perms) {}
         config_set_mib("db-capacity", cfg, allocator_capacity) {}
         config_set_gib("db-capacity", cfg, allocator_capacity) {}
         config_set_uint64("libaio-queue-depth", cfg, io_async_queue_depth) {}
         config_set_mib("cache-capacity", cfg, cache_capacity) {}
         config_set_gib("cache-capacity", cfg, cache_capacity) {}
         config_set_string("cache-debug-log", cfg, cache_logfile) {}
         config_set_uint64("queue-scale-percent", cfg, queue_scale_percent) {}
         config_set_mib("memtable-capacity", cfg, memtable_capacity) {}
         config_set_gib("memtable-capacity", cfg, memtable_capacity) {}
         config_set_uint64("rough-count-height", cfg, btree_rough_count_height)
         {
         }
         config_set_uint64("filter-hash-size", cfg, filter_hash_size) {}
         config_set_uint64("fanout", cfg, fanout) {}
         config_set_mib("reclaim-threshold", cfg, reclaim_threshold) {}
         config_set_gib("reclaim-threshold", cfg, reclaim_threshold) {}

         /*
          * These arguments will be passed through to Splinter initialization
          * to setup Splinter task system to use background threads.
          */
         config_set_uint64("num-normal-bg-threads", cfg, num_normal_bg_threads);
         config_set_uint64(
            "num-memtable-bg-threads", cfg, num_memtable_bg_threads);

         config_has_option("stats")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].use_stats = TRUE;
            }
         }
         config_has_option("no-stats")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].use_stats = FALSE;
            }
         }
         config_has_option("log")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].use_log = TRUE;
            }
         }
         config_has_option("no-log")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].use_log = FALSE;
            }
         }
         config_has_option("verbose-logging")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].verbose_logging_enabled = TRUE;
               cfg[cfg_idx].log_handle = Platform_default_log_handle;
            }
         }
         config_has_option("no-verbose-logging")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].verbose_logging_enabled = FALSE;
               cfg[cfg_idx].log_handle              = NULL;
            }
         }
         config_has_option("verbose-progress")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].verbose_progress = TRUE;
            }
         }
         /*
          * Arguments to run Splinter configured with shared memory.
          */
         config_has_option("use-shmem")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].use_shmem = TRUE;
            }
         }
         config_set_mib("shmem-capacity", cfg, shmem_size) {}
         config_set_gib("shmem-capacity", cfg, shmem_size) {}
         config_has_option("trace-shmem-allocs")
         {
            platform_enable_tracing_shm_allocs();
         }
         config_has_option("trace-shmem-frees")
         {
            platform_enable_tracing_shm_frees();
         }
         config_has_option("trace-shmem")
         {
            // Trace both allocations & frees from shared memory segment.
            platform_enable_tracing_shm_ops();
         }
         // Parameter should only be used with --use-shmem argument.
         config_has_option("fork-child")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].fork_child = TRUE;
            }
         }

         // Some tests that fork multiple child processes may need
         // debugging. Use this arg to wait-for-gdb looping behaviour.
         config_has_option("wait-for-gdb")
         {
            for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].wait_for_gdb = TRUE;
            }
         }

         config_set_uint64("key-size", cfg, max_key_size) {}
         config_set_uint64("data-size", cfg, message_size) {}

         // Test-execution configuration parameters
         config_set_uint64("seed", cfg, seed) {}
         config_set_uint64("num-inserts", cfg, num_inserts) {}
         config_set_uint64("num-processes", cfg, num_processes) {}

         config_set_else
         {
            platform_error_log("config: invalid option: %s\n", argv[i]);
            return STATUS_BAD_PARAM;
         }
      }

      // Validate consistency of config parameters provided.
      for (uint8 cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
         if (cfg[cfg_idx].extent_size % cfg[cfg_idx].page_size != 0) {
            platform_error_log("Configured extent-size, %lu, is not a multiple "
                               "of page-size, %lu bytes.\n",
                               cfg[cfg_idx].extent_size,
                               cfg[cfg_idx].page_size);
            return STATUS_BAD_PARAM;
         }
         if (cfg[cfg_idx].extent_size / cfg[cfg_idx].page_size
             != MAX_PAGES_PER_EXTENT)
         {
            int npages = (cfg[cfg_idx].extent_size / cfg[cfg_idx].page_size);
            platform_error_log("For the configured page-size, %lu bytes, "
                               "the '%s' argument, %lu, results in %d "
                               "pages per extent which is not the "
                               "supported value, %lu. "
                               "Valid value for %s is %lu.\n",
                               cfg[cfg_idx].page_size,
                               "--extent-size",
                               cfg[cfg_idx].extent_size,
                               npages,
                               MAX_PAGES_PER_EXTENT,
                               "--extent-size",
                               (MAX_PAGES_PER_EXTENT * cfg[cfg_idx].page_size));
            return STATUS_BAD_PARAM;
         }
         if (cfg[cfg_idx].max_key_size < TEST_CONFIG_MIN_KEY_SIZE) {
            platform_error_log("Configured key-size, %lu, should be at least "
                               "%d bytes. Support for smaller key-sizes is "
                               "experimental.\n",
                               cfg[cfg_idx].max_key_size,
                               TEST_CONFIG_MIN_KEY_SIZE);
            return STATUS_BAD_PARAM;
         }
      }
      return STATUS_OK;
   }
