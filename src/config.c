// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "config.h"

void
config_set_defaults(master_config *cfg)
{
   *cfg = (master_config) {
      .io_filename = "db",
      .cache_logfile = "cache_log",

      .page_size                = 4096,
      .extent_size              = 128 * 1024,

      .io_flags                 = O_RDWR | O_CREAT,
      .io_perms                 = 0755,
      .io_async_queue_depth     = 256,

      .allocator_capacity       = GiB_TO_B(30),

      .cache_capacity           = GiB_TO_B(1),

      .btree_rough_count_height = 1,

      .filter_remainder_size    = 6,
      .filter_index_size        = 256,

      .use_log                  = FALSE,

      .memtable_capacity        = MiB_TO_B(24),
      .fanout                   = 8,
      .max_branches_per_node    = 24,
      .use_stats                = FALSE,
      .reclaim_threshold        = UINT64_MAX,

      .key_size                 = 24,
      .message_size             = 100,

      .seed                     = 0,
   };
}

void config_usage()
{
   platform_error_log("Configuration:\n");
   platform_error_log("\t--page_size\n");
   platform_error_log("\t--extent_size\n");
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
   platform_error_log("\t--db-capacity-gib\n");
   platform_error_log("\t--db-capacity-mib\n");
   platform_error_log("\t--libaio-queue-depth\n");
   platform_error_log("\t--cache-capacity-gib\n");
   platform_error_log("\t--cache-capacity-mib\n");
   platform_error_log("\t--cache-debug-log\n");
   platform_error_log("\t--memtable-capacity-gib\n");
   platform_error_log("\t--memtable-capacity-mib\n");
   platform_error_log("\t--rough-count-height\n");
   platform_error_log("\t--filter-remainder-size\n");
   platform_error_log("\t--fanout\n");
   platform_error_log("\t--max-branches-per-node\n");
   platform_error_log("\t--stats\n");
   platform_error_log("\t--no-stats\n");
   platform_error_log("\t--log\n");
   platform_error_log("\t--no-log\n");
   platform_error_log("\t--key-size\n");
   platform_error_log("\t--data-size\n");
   platform_error_log("\t--seed\n");
}

platform_status
config_parse(master_config *cfg,
             const uint8    num_config,
             int            argc,
             char          *argv[])
{
   uint64 i;
   uint8 cfg_idx;
   for (i = 0; i < argc; i++) {
      if (0) {
      config_set_uint64("page-size", cfg, page_size) {
         for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
            if (cfg[cfg_idx].page_size != 4096) {
               platform_error_log("page_size must be 4096 for now\n");
               platform_error_log("config: failed to parse page-size\n");
               return STATUS_BAD_PARAM;
            }
            if (!IS_POWER_OF_2(cfg[cfg_idx].page_size)) {
               platform_error_log("page_size must be a power of 2\n");
               platform_error_log("config: failed to parse page-size\n");
               return STATUS_BAD_PARAM;
            }
         }
      } config_set_uint64("extent-size", cfg, extent_size) {
      } config_has_option("set-hugetlb") {
         platform_use_hugetlb = TRUE;
      } config_has_option("unset-hugetlb") {
         platform_use_hugetlb = FALSE;
      } config_has_option("set-mlock") {
         platform_use_mlock = TRUE;
      } config_has_option("unset-mlock") {
         platform_use_mlock = FALSE;
      } config_set_string("db-location", cfg, io_filename) {
      } config_has_option("set-O_DIRECT") {
         for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
            cfg[cfg_idx].io_flags |= O_DIRECT;
         }
      } config_has_option("unset-O_DIRECT") {
         for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
            cfg[cfg_idx].io_flags &= ~O_DIRECT;
         }
      } config_has_option("set-O_CREAT") {
         for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
            cfg[cfg_idx].io_flags |= O_CREAT;
         }
      } config_has_option("unset-O_CREAT") {
         for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
            cfg[cfg_idx].io_flags &= ~O_CREAT;
         }
      } config_set_uint32("db-perms", cfg, io_perms) {
      } config_set_mib("db-capacity", cfg, allocator_capacity) {
      } config_set_gib("db-capacity", cfg, allocator_capacity) {
      } config_set_uint64("libaio-queue-depth", cfg, io_async_queue_depth) {
      } config_set_mib("cache-capacity", cfg, cache_capacity) {
      } config_set_gib("cache-capacity", cfg, cache_capacity) {
      } config_set_string("cache-debug-log", cfg, cache_logfile) {
      } config_set_mib("memtable-capacity", cfg, memtable_capacity) {
      } config_set_gib("memtable-capacity", cfg, memtable_capacity) {
      } config_set_uint64("rough-count-height", cfg, btree_rough_count_height) {
      } config_set_uint64("filter-remainder-size", cfg, filter_remainder_size) {
      } config_set_uint64("fanout", cfg, fanout) {
      } config_set_uint64("max-branches-per-node", cfg, max_branches_per_node) {
      } config_set_mib("reclaim-threshold", cfg, reclaim_threshold) {
      } config_set_gib("reclaim-threshold", cfg, reclaim_threshold) {
      } config_has_option("stats") {
         for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
            cfg[cfg_idx].use_stats = TRUE;
         }
      } config_has_option("no-stats") {
         for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
            cfg[cfg_idx].use_stats = FALSE;
         }
      } config_has_option("log") {
         for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
            cfg[cfg_idx].use_log = TRUE;
         }
      } config_set_uint64("key-size", cfg, key_size) {
      } config_set_uint64("data-size", cfg, message_size) {
      } config_set_uint64("seed", cfg, seed) {
      } config_set_else {
         platform_error_log("config: invalid option: %s\n", argv[i]);
         return STATUS_BAD_PARAM;
      }
   }
   for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
      if (cfg[cfg_idx].extent_size % cfg[cfg_idx].page_size != 0) {
         platform_error_log("config: extent_size is not a multiple of page_size\n");
         return STATUS_BAD_PARAM;
      }
      if (cfg[cfg_idx].extent_size / cfg[cfg_idx].page_size > MAX_PAGES_PER_EXTENT) {
         platform_error_log("config: pages per extent too high: %lu > %lu\n",
                            cfg[cfg_idx].extent_size / cfg[cfg_idx].page_size,
                            MAX_PAGES_PER_EXTENT);
         return STATUS_BAD_PARAM;
      }
   }
   return STATUS_OK;
}
