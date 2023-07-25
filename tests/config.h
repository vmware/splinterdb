// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * config.h --
 *
 *     This file contains functions for config parsing.
 */

#pragma once

#include "clockcache.h"
#include "splinterdb/data.h"
#include "io.h"
#include "rc_allocator.h"
#include "shard_log.h"
#include "trunk.h"
#include "util.h"

extern const char *BUILD_VERSION;

/*
 * --------------------------------------------------------------------------
 * Default test configuration settings. These will be used by
 * config_set_defaults() to initialize test-execution configuration in the
 * master_config used to run tests.
 * --------------------------------------------------------------------------
 */
#define TEST_CONFIG_DEFAULT_PAGE_SIZE LAIO_DEFAULT_PAGE_SIZE // bytes

#define TEST_CONFIG_DEFAULT_PAGES_PER_EXTENT LAIO_DEFAULT_PAGES_PER_EXTENT
_Static_assert(TEST_CONFIG_DEFAULT_PAGES_PER_EXTENT <= MAX_PAGES_PER_EXTENT,
               "Invalid TEST_CONFIG_DEFAULT_PAGES_PER_EXTENT value");

#define TEST_CONFIG_DEFAULT_EXTENT_SIZE                                        \
   (TEST_CONFIG_DEFAULT_PAGES_PER_EXTENT * TEST_CONFIG_DEFAULT_PAGE_SIZE)

/*
 * --------------------------------------------------------------------------
 * Convenience structure to hold configuration options for all sub-systems.
 * Command-line parsing routines parse config params into these structs.
 * Mostly needed for testing interfaces.
 * --------------------------------------------------------------------------
 */
typedef struct master_config {
   uint64 page_size;
   uint64 extent_size;

   // io
   char   io_filename[MAX_STRING_LENGTH];
   int    io_flags;
   uint32 io_perms;
   uint64 io_async_queue_depth;

   // allocator
   uint64 allocator_capacity;

   // cache
   uint64 cache_capacity;
   bool32 cache_use_stats;
   char   cache_logfile[MAX_STRING_LENGTH];

   // btree
   uint64 btree_rough_count_height;

   // routing filter
   uint64 filter_remainder_size;
   uint64 filter_index_size;

   // log
   bool32 use_log;

   // task system
   uint64 num_normal_bg_threads;   // Both bg_threads fields have to be non-zero
   uint64 num_memtable_bg_threads; // for background threads to be enabled

   // splinter
   uint64               memtable_capacity;
   uint64               fanout;
   uint64               max_branches_per_node;
   uint64               use_stats;
   uint64               reclaim_threshold;
   uint64               queue_scale_percent;
   bool32               verbose_logging_enabled;
   bool32               verbose_progress;
   platform_log_handle *log_handle;

   // data
   uint64 max_key_size;
   uint64 message_size;

   // Test-execution configuration parameters
   uint64 seed;
   uint64 num_inserts;
} master_config;


void
config_set_defaults(master_config *cfg);

void
config_usage();

platform_status
config_parse(master_config *cfg,
             const uint8    num_config,
             int            argc,
             char          *argv[]);


/*
 * Config option parsing macros
 *
 * They are meant to be used inside config_parse and test_config_parse.
 * They can be used as regular conditions, and the code block after will be
 * executed when the condition satisfies.
 *
 * So config_set_*(...) { stmt; } gets expanded to:
 *
 *    } else if (...) {
 *       ...boilerplate parsing code...
 *       {
 *         stmt;
 *       }
 *    }
 *
 * config_set_string, config_set_uint* and config_set_*ib can be used for
 * parsing a single value or comma-separated multiple values.
 * When parsing a single value, each cfg will receive the same parsed value.
 * When parsing multiple values, if number of tokens doesn't match num_config,
 * will print error msg and fail.
 */

#define config_has_option(str)                                                 \
   }                                                                           \
   else if (STRING_EQUALS_LITERAL(argv[i], "--" str)) {

#define config_set_string(name, var, field)                                    \
   config_has_option(name) if (i + 1 == argc)                                  \
   {                                                                           \
      platform_error_log("config: failed to parse %s\n", name);                \
      return STATUS_BAD_PARAM;                                                 \
   }                                                                           \
   uint8               _idx;                                                   \
   platform_strtok_ctx _ctx = {                                                \
      .token_str = NULL, .last_token = NULL, .last_token_len = 0};             \
                                                                               \
   if (strchr(argv[++i], ',')) {                                               \
      char *_token = platform_strtok_r(argv[i], ",", &_ctx);                   \
      for (_idx = 0; _token != NULL; _idx++) {                                 \
         if (_idx > num_config - 1) {                                          \
            platform_error_log("config: more %s than num_tables\n", name);     \
            return STATUS_BAD_PARAM;                                           \
         }                                                                     \
         int _rc = snprintf(var[_idx].field, MAX_STRING_LENGTH, "%s", _token); \
         if (_rc >= MAX_STRING_LENGTH) {                                       \
            platform_error_log("config: %s too long\n", name);                 \
            return STATUS_BAD_PARAM;                                           \
         }                                                                     \
                                                                               \
         _token = platform_strtok_r(NULL, ",", &_ctx);                         \
      }                                                                        \
      if (_idx < num_config) {                                                 \
         platform_error_log("config: less %s than num_tables\n", name);        \
         return STATUS_BAD_PARAM;                                              \
      }                                                                        \
   } else {                                                                    \
      for (_idx = 0; _idx < num_config; _idx++) {                              \
         int _rc =                                                             \
            snprintf(var[_idx].field, MAX_STRING_LENGTH, "%s", argv[i]);       \
         if (_rc >= MAX_STRING_LENGTH) {                                       \
            platform_error_log("config: %s too long\n", name);                 \
            return STATUS_BAD_PARAM;                                           \
         }                                                                     \
      }                                                                        \
   }

#define _config_set_numerical(name, var, field, type)                          \
   config_has_option(name) if (i + 1 == argc)                                  \
   {                                                                           \
      platform_error_log("config: failed to parse %s\n", name);                \
      return STATUS_BAD_PARAM;                                                 \
   }                                                                           \
   uint8               _idx;                                                   \
   platform_strtok_ctx _ctx = {                                                \
      .token_str = NULL, .last_token = NULL, .last_token_len = 0};             \
                                                                               \
   if (strchr(argv[++i], ',')) {                                               \
      char *_token = platform_strtok_r(argv[i], ",", &_ctx);                   \
      for (_idx = 0; _token != NULL; _idx++) {                                 \
         if (_idx > num_config - 1) {                                          \
            platform_error_log("config: more %s than num_tables\n", name);     \
            return STATUS_BAD_PARAM;                                           \
         }                                                                     \
         if (!try_string_to_##type(_token, &var[_idx].field)) {                \
            platform_error_log("config: failed to parse %s\n", name);          \
            return STATUS_BAD_PARAM;                                           \
         }                                                                     \
                                                                               \
         _token = platform_strtok_r(NULL, ",", &_ctx);                         \
      }                                                                        \
      if (_idx < num_config) {                                                 \
         platform_error_log("config: less %s than num_tables\n", name);        \
         return STATUS_BAD_PARAM;                                              \
      }                                                                        \
   } else {                                                                    \
      for (_idx = 0; _idx < num_config; _idx++) {                              \
         if (!try_string_to_##type(argv[i], &var[_idx].field)) {               \
            platform_error_log("config: failed to parse %s\n", name);          \
            return STATUS_BAD_PARAM;                                           \
         }                                                                     \
      }                                                                        \
   }

#define config_set_uint8(name, var, field)                                     \
   _config_set_numerical(name, var, field, uint8)

#define config_set_uint32(name, var, field)                                    \
   _config_set_numerical(name, var, field, uint32)

#define config_set_uint64(name, var, field)                                    \
   _config_set_numerical(name, var, field, uint64)

#define config_set_mib(name, var, field)                                       \
   config_set_uint64(name "-mib", var, field)                                  \
   {                                                                           \
      for (uint8 _i = 0; _i < num_config; _i++) {                              \
         var[_i].field = MiB_TO_B(var[_i].field);                              \
      }                                                                        \
   }

#define config_set_gib(name, var, field)                                       \
   config_set_uint64(name "-gib", var, field)                                  \
   {                                                                           \
      for (uint8 _i = 0; _i < num_config; _i++) {                              \
         var[_i].field = GiB_TO_B(var[_i].field);                              \
      }                                                                        \
   }

#define config_set_else                                                        \
   }                                                                           \
   else
