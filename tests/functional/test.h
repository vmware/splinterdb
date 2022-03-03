// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * test.h --
 *
 *     This file contains constants and functions that pertain to tests.
 */

#ifndef __TEST_H
#define __TEST_H

#include "cache.h"
#include "clockcache.h"
#include "../config.h"
#include "splinterdb/data.h"
#include "rc_allocator.h"
#include "shard_log.h"
#include "trunk.h"
#include "../test_data.h"

typedef enum test_key_type {
   TEST_RANDOM,
   TEST_PERIODIC,
   TEST_SEQ,
   TEST_SEMISEQ
} test_key_type;

#define TEST_STUCK_IO_TIMEOUT SEC_TO_NSEC(60)

int
test_dispatcher(int argc, char *argv[]);

int
btree_test(int argc, char *argv[]);

int
filter_test(int argc, char *argv[]);

int
splinter_test(int argc, char *argv[]);

int
log_test(int argc, char *argv[]);

int
cache_test(int argc, char *argv[]);

int
ycsb_test(int argc, char *argv[]);

/*
 * Initialization for using splinter, need to be called at the start of the test
 * main function. This initializes SplinterDB's task sub-system.
 */
static inline platform_status
test_init_task_system(platform_heap_id    hid,
                      platform_io_handle *ioh,
                      task_system       **system,
                      bool                use_stats,
                      bool                use_bg_threads,
                      uint8               num_bg_threads[NUM_TASK_TYPES])
{
   // splinter initialization
   return task_system_create(hid,
                             ioh,
                             system,
                             use_stats,
                             use_bg_threads,
                             num_bg_threads,
                             trunk_get_scratch_size());
}

static inline void
test_deinit_task_system(platform_heap_id hid, task_system *ts)
{
   task_system_destroy(hid, ts);
}

static inline void
test_key(char         *key,
         test_key_type key_type,
         uint64        idx,
         uint64        thread_id,
         uint64        semiseq_freq,
         uint64        key_size,
         uint64        period)
{
   memset(key, 0, key_size);
   switch (key_type) {
      case TEST_RANDOM:
         *(uint64 *)key = platform_checksum64(&idx, sizeof(idx), 42);
         break;
      case TEST_SEQ:
         *(uint64 *)key = htobe64(
            platform_checksum64(&thread_id, sizeof(thread_id), 42) + idx);
         break;
      case TEST_SEMISEQ:
         if (idx % semiseq_freq == 0) {
            *(uint64 *)key = platform_checksum64(&idx, sizeof(idx), 42);
         } else {
            *(uint64 *)key = htobe64(
               platform_checksum64(&thread_id, sizeof(thread_id), 42) + idx);
         }
         break;
      case TEST_PERIODIC:
      {
         uint64 period_idx = idx % period;
         *(uint64 *)key    = platform_checksum64(&period_idx, sizeof(idx), 42);
         break;
      }
   }
}

static inline bool
test_period_complete(uint64 idx, uint64 period)
{
   return idx % period == 0;
}

static inline uint64
test_range(uint64 idx, uint64 range_min, uint64 range_max)
{
   debug_assert(range_max > range_min);
   return range_min
          + platform_checksum64(&idx, sizeof(idx), 43)
               % (range_max - range_min);
}

static inline void
test_int_to_key(char *key, uint64 idx, uint64 key_size)
{
   memset(key, 0, key_size);
   *(uint64 *)key = htobe64(idx);
}

static inline void
test_insert_data(void        *raw_data,
                 int8         ref_count,
                 char        *val,
                 uint64       input_size,
                 uint64       data_size,
                 message_type type)
{
   memset(raw_data, 0, data_size);
   data_handle *data  = raw_data;
   data->message_type = type;
   data->ref_count    = ref_count;
   memmove(data->data, val, input_size);
}

/*
 * test_config_init() --
 *
 * Initialize the configuration sub-structures for various sub-systems, using an
 * input master configuration, master_cfg. A few command-line config parameters
 * may have been used to setup master_cfg beyond its initial defaults.
 */
static inline void
test_config_init(trunk_config        *splinter_cfg,  // OUT
                 data_config         *data_cfg,      // OUT
                 shard_log_config    *log_cfg,       // OUT
                 clockcache_config   *cache_cfg,     // OUT
                 rc_allocator_config *allocator_cfg, // OUT
                 io_config           *io_cfg,        // OUT
                 master_config       *master_cfg     // IN
)
{
   *data_cfg              = test_data_config;
   data_cfg->key_size     = master_cfg->key_size;
   data_cfg->message_size = master_cfg->message_size;

   io_config_init(io_cfg,
                  master_cfg->page_size,
                  master_cfg->extent_size,
                  master_cfg->io_flags,
                  master_cfg->io_perms,
                  master_cfg->io_async_queue_depth,
                  master_cfg->io_filename);

   rc_allocator_config_init(
      allocator_cfg, io_cfg, master_cfg->allocator_capacity);

   clockcache_config_init(cache_cfg,
                          io_cfg,
                          master_cfg->cache_capacity,
                          master_cfg->cache_logfile,
                          master_cfg->use_stats);

   shard_log_config_init(log_cfg, &cache_cfg->super, data_cfg);

   trunk_config_init(splinter_cfg,
                     &cache_cfg->super,
                     data_cfg,
                     (log_config *)log_cfg,
                     master_cfg->memtable_capacity,
                     master_cfg->fanout,
                     master_cfg->max_branches_per_node,
                     master_cfg->btree_rough_count_height,
                     master_cfg->filter_remainder_size,
                     master_cfg->filter_index_size,
                     master_cfg->reclaim_threshold,
                     master_cfg->use_log,
                     master_cfg->use_stats);
}

/*
 * Some command-line [config] arguments become test execution parameters.
 * Define a structure to hold these when parsing command-line arguments.
 */
typedef struct test_exec_config {
   uint64 seed;
   uint64 num_inserts;
} test_exec_config;

/*
 * test_parse_args_n() --
 *
 * Driver routine to parse command-line configuration arguments, to setup the
 * config sub-structures for all sub-systems in up to n-SplinterDB instances,
 * given by the num_config parameter.
 *
 * NOTE: test_exec_cfg{} contains test-specific configuration parameters.
 * Not all tests may need these, so this arg is optional, and can be NULL.
 */
static inline platform_status
test_parse_args_n(trunk_config        *splinter_cfg,  // OUT
                  data_config         *data_cfg,      // OUT
                  io_config           *io_cfg,        // OUT
                  rc_allocator_config *allocator_cfg, // OUT
                  clockcache_config   *cache_cfg,     // OUT
                  shard_log_config    *log_cfg,       // OUT
                  test_exec_config    *test_exec_cfg, // OUT
                  uint8                num_config,    // IN
                  int                  argc,          // IN
                  char                *argv[]         // IN
)
{
   platform_status rc;
   uint8           i;

   // Allocate memory and setup default configs for up to n-instances
   master_config *master_cfg =
      TYPED_ARRAY_MALLOC(platform_get_heap_id(), master_cfg, num_config);
   for (i = 0; i < num_config; i++) {
      config_set_defaults(&master_cfg[i]);
   }

   rc = config_parse(master_cfg, num_config, argc, argv);
   if (!SUCCESS(rc)) {
      return rc;
   }

   for (i = 0; i < num_config; i++) {
      test_config_init(&splinter_cfg[i],
                       &data_cfg[i],
                       log_cfg,
                       &cache_cfg[i],
                       allocator_cfg,
                       io_cfg,
                       &master_cfg[i]);
   }

   // All the n-SplinterDB instances will work with the same set of
   // test execution parameters.
   if (test_exec_cfg) {
      test_exec_cfg->seed        = master_cfg[0].seed;
      test_exec_cfg->num_inserts = master_cfg[0].num_inserts;
   }

   platform_free(platform_get_heap_id(), master_cfg);

   return STATUS_OK;
}

/*
 * test_parse_args() --
 *
 * Parse the command-line configuration arguments to setup the config
 * sub-structures for individual SplinterDB sub-systems.
 */
static inline platform_status
test_parse_args(trunk_config        *splinter_cfg,
                data_config         *data_cfg,
                io_config           *io_cfg,
                rc_allocator_config *allocator_cfg,
                clockcache_config   *cache_cfg,
                shard_log_config    *log_cfg,
                uint64              *seed,
                int                  argc,
                char                *argv[])
{
   test_exec_config test_exec_cfg;
   ZERO_STRUCT(test_exec_cfg);

   platform_status rc;
   rc = test_parse_args_n(splinter_cfg,
                          data_cfg,
                          io_cfg,
                          allocator_cfg,
                          cache_cfg,
                          log_cfg,
                          &test_exec_cfg,
                          1,
                          argc,
                          argv);
   if (!SUCCESS(rc)) {
      return rc;
   }
   // Most tests that parse cmdline args are only interested in this one.
   *seed = test_exec_cfg.seed;
   return rc;
}

// monotonically increasing counter to generate splinter id for tests.
static allocator_root_id counter = 1;

static inline allocator_root_id
test_generate_allocator_root_id()
{
   return __sync_fetch_and_add(&counter, 1);
}

#endif
