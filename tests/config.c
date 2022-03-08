// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "config.h"

/*
 * --------------------------------------------------------------------------
 * Default test configuration settings. These will be used by
 * config_set_defaults() to initialize test-execution configuration in the
 * master_config used to run tests.
 * --------------------------------------------------------------------------
 */
#define TEST_CONFIG_DEFAULT_PAGE_SIZE 4096 // bytes

#define TEST_CONFIG_DEFAULT_PAGES_PER_EXTENT 32
_Static_assert(TEST_CONFIG_DEFAULT_PAGES_PER_EXTENT <= MAX_PAGES_PER_EXTENT,
               "Invalid TEST_CONFIG_DEFAULT_PAGES_PER_EXTENT value");

#define TEST_CONFIG_DEFAULT_EXTENT_SIZE                                        \
   (TEST_CONFIG_DEFAULT_PAGES_PER_EXTENT * TEST_CONFIG_DEFAULT_PAGE_SIZE)

// Determined empirically ... nothing scientific here
#define TEST_CONFIG_DEFAULT_IO_ASYNC_Q_DEPTH 256

// Provide sufficient disk, cache and memtable capacity to get somewhat
// realistic configuration for most tests.
#define TEST_CONFIG_DEFAULT_DISK_SIZE_GB         30
#define TEST_CONFIG_DEFAULT_CACHE_SIZE_GB        1
#define TEST_CONFIG_DEFAULT_MEMTABLE_CAPACITY_MB 24

// Setup reasonable BTree and branch tree configurations
#define TEST_CONFIG_DEFAULT_FILTER_INDEX_SIZE     256
#define TEST_CONFIG_DEFAULT_FANOUT                8
#define TEST_CONFIG_DEFAULT_MAX_BRANCHES_PER_NODE 24

// Deal with reasonable key / message sizes for tests
#define TEST_CONFIG_DEFAULT_KEY_SIZE     24
#define TEST_CONFIG_DEFAULT_MESSAGE_SIZE 100

// Configs that are usually changed by different tests
#define TEST_CONFIG_DEFAULT_SEED        0
#define TEST_CONFIG_DEFAULT_NUM_INSERTS 0

// clang-format off
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
      .filter_remainder_size    = 6,
      .filter_index_size        = TEST_CONFIG_DEFAULT_FILTER_INDEX_SIZE,
      .use_log                  = FALSE,
      .memtable_capacity        = MiB_TO_B(TEST_CONFIG_DEFAULT_MEMTABLE_CAPACITY_MB),
      .fanout                   = TEST_CONFIG_DEFAULT_FANOUT,
      .max_branches_per_node    = TEST_CONFIG_DEFAULT_MAX_BRANCHES_PER_NODE,
      .use_stats                = FALSE,
      .reclaim_threshold        = UINT64_MAX,
      .key_size                 = TEST_CONFIG_DEFAULT_KEY_SIZE,
      .message_size             = TEST_CONFIG_DEFAULT_MESSAGE_SIZE,
      .num_inserts              = TEST_CONFIG_DEFAULT_NUM_INSERTS,
      .seed                     = TEST_CONFIG_DEFAULT_SEED,
   };
}
// clang-format on

void
config_usage()
{
   platform_error_log("Configuration: (default)\n");
   platform_error_log("\t--page_size (%d)\n", TEST_CONFIG_DEFAULT_PAGE_SIZE);
   platform_error_log("\t--extent_size (%d)\n",
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
   platform_error_log("\t--memtable-capacity-gib\n");
   platform_error_log("\t--memtable-capacity-mib (%d)\n",
                      TEST_CONFIG_DEFAULT_MEMTABLE_CAPACITY_MB);
   platform_error_log("\t--rough-count-height\n");
   platform_error_log("\t--filter-remainder-size\n");
   platform_error_log("\t--fanout (%d)\n", TEST_CONFIG_DEFAULT_FANOUT);
   platform_error_log("\t--max-branches-per-node (%d)\n",
                      TEST_CONFIG_DEFAULT_MAX_BRANCHES_PER_NODE);
   platform_error_log("\t--stats\n");
   platform_error_log("\t--no-stats\n");
   platform_error_log("\t--log\n");
   platform_error_log("\t--no-log\n");
   platform_error_log("\t--key-size (%d)\n", TEST_CONFIG_DEFAULT_KEY_SIZE);
   platform_error_log("\t--data-size (%d)\n", TEST_CONFIG_DEFAULT_MESSAGE_SIZE);
   platform_error_log("\t--num-inserts (%d)\n",
                      TEST_CONFIG_DEFAULT_NUM_INSERTS);
   platform_error_log("\t--seed (%d)\n", TEST_CONFIG_DEFAULT_SEED);
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
   uint8  cfg_idx;
   for (i = 0; i < argc; i++) {
      // Don't be mislead; this is not dead-code. See the config macro expansion
      if (0) {
         config_set_uint64("page-size", cfg, page_size)
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               if (cfg[cfg_idx].page_size != TEST_CONFIG_DEFAULT_PAGE_SIZE) {
                  platform_error_log("page_size must be %d for now\n",
                                     TEST_CONFIG_DEFAULT_PAGE_SIZE);
                  platform_error_log("config: failed to parse page-size\n");
                  return STATUS_BAD_PARAM;
               }
               if (!IS_POWER_OF_2(cfg[cfg_idx].page_size)) {
                  platform_error_log("page_size must be a power of 2\n");
                  platform_error_log("config: failed to parse page-size\n");
                  return STATUS_BAD_PARAM;
               }
            }
         }
         config_set_uint64("extent-size", cfg, extent_size) {}
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
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].io_flags |= O_DIRECT;
            }
         }
         config_has_option("unset-O_DIRECT")
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].io_flags &= ~O_DIRECT;
            }
         }
         config_has_option("set-O_CREAT")
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].io_flags |= O_CREAT;
            }
         }
         config_has_option("unset-O_CREAT")
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
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
         config_set_mib("memtable-capacity", cfg, memtable_capacity) {}
         config_set_gib("memtable-capacity", cfg, memtable_capacity) {}
         config_set_uint64("rough-count-height", cfg, btree_rough_count_height)
         {}
         config_set_uint64("filter-remainder-size", cfg, filter_remainder_size)
         {}
         config_set_uint64("fanout", cfg, fanout) {}
         config_set_uint64("max-branches-per-node", cfg, max_branches_per_node)
         {}
         config_set_mib("reclaim-threshold", cfg, reclaim_threshold) {}
         config_set_gib("reclaim-threshold", cfg, reclaim_threshold) {}
         config_has_option("stats")
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].use_stats = TRUE;
            }
         }
         config_has_option("no-stats")
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].use_stats = FALSE;
            }
         }
         config_has_option("log")
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].use_log = TRUE;
            }
         }
         config_set_uint64("key-size", cfg, key_size) {}
         config_set_uint64("data-size", cfg, message_size) {}

         // Test-execution configuration parameters
         config_set_uint64("seed", cfg, seed) {}
         config_set_uint64("num-inserts", cfg, num_inserts) {}

         config_set_else
         {
            platform_error_log("config: invalid option: %s\n", argv[i]);
            return STATUS_BAD_PARAM;
         }
      }
      for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
         if (cfg[cfg_idx].extent_size % cfg[cfg_idx].page_size != 0) {
            platform_error_log(
               "config: extent_size is not a multiple of page_size\n");
            return STATUS_BAD_PARAM;
         }
         if (cfg[cfg_idx].extent_size / cfg[cfg_idx].page_size
             > MAX_PAGES_PER_EXTENT) {
            platform_error_log("config: pages per extent too high: %lu > %lu\n",
                               cfg[cfg_idx].extent_size
                                  / cfg[cfg_idx].page_size,
                               MAX_PAGES_PER_EXTENT);
            return STATUS_BAD_PARAM;
         }
         if (cfg[cfg_idx].key_size > MAX_KEY_SIZE) {
            platform_error_log(
               "config: key-size is larger than MAX_KEY_SIZE: %lu > %d\n",
               cfg[cfg_idx].key_size,
               MAX_KEY_SIZE);
            return STATUS_BAD_PARAM;
         }
      }
      return STATUS_OK;
   }
