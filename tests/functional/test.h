// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * test.h --
 *
 *     This file contains constants and functions that pertain to tests.
 */

#pragma once

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

int
splinter_io_apis_test(int argc, char *argv[]);

/*
 * Initialization for using splinter, need to be called at the start of the test
 * main function. This initializes SplinterDB's task sub-system.
 */
static inline platform_status
test_init_task_system(platform_heap_id          hid,
                      platform_io_handle       *ioh,
                      task_system             **system,
                      const task_system_config *cfg)
{
   // splinter initialization
   return task_system_create(hid, ioh, system, cfg);
}

static inline void
test_deinit_task_system(platform_heap_id hid, task_system **ts)
{
   task_system_destroy(hid, ts);
}

static inline key
test_key(key_buffer   *keywb,
         test_key_type key_type,
         uint64        idx,
         uint64        thread_id,
         uint64        semiseq_freq,
         uint64        key_size,
         uint64        period)
{
   key_buffer_resize(keywb, key_size);
   char *keybuffer = key_buffer_data(keywb);
   memset(keybuffer, 0, key_size);
   switch (key_type) {
      case TEST_RANDOM:
         *(uint64 *)keybuffer = platform_checksum64(&idx, sizeof(idx), 42);
         break;
      case TEST_SEQ:
         *(uint64 *)keybuffer = htobe64(
            platform_checksum64(&thread_id, sizeof(thread_id), 42) + idx);
         break;
      case TEST_SEMISEQ:
         if (idx % semiseq_freq == 0) {
            *(uint64 *)keybuffer = platform_checksum64(&idx, sizeof(idx), 42);
         } else {
            *(uint64 *)keybuffer = htobe64(
               platform_checksum64(&thread_id, sizeof(thread_id), 42) + idx);
         }
         break;
      case TEST_PERIODIC:
      {
         uint64 period_idx = idx % period;
         *(uint64 *)keybuffer =
            platform_checksum64(&period_idx, sizeof(idx), 42);
         break;
      }
   }
   return key_buffer_key(keywb);
}

static inline bool32
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
test_int_to_key(key_buffer *kb, uint64 idx, uint64 key_size)
{
   key_buffer_resize(kb, key_size);
   uint64 *keybytes = key_buffer_data(kb);
   memset(keybytes, 0, key_size);
   *keybytes = htobe64(idx);
}

/*
 * The intention is that we can shove all our different algorithms for
 * generating sequences of messages into this structure (e.g. via
 * tagged union or whatever).
 */
typedef struct test_message_generator {
   message_type type;
   uint64       min_payload_size;
   uint64       max_payload_size;
} test_message_generator;

#define GENERATOR_MIN_PAYLOAD_SIZE (sizeof(uint64))

/*
 * Generate the ith test message in msg.  Currently, test messages are
 * data_handles with a ref_count of 1 and a payload of "random" size
 * between min and max payload-size.  The min payload must be long
 * enough to store a uint64.  We put the idx at the front of the payload.
 * The rest of the payload is filled with a "random" byte.
 */
static inline void
generate_test_message(const test_message_generator *generator,
                      uint64                        idx,
                      merge_accumulator            *msg)
{
   debug_assert(generator->min_payload_size <= generator->max_payload_size,
                "generator min_payload_size=%lu should be less than generator "
                "max_payload_size=%lu",
                generator->min_payload_size,
                generator->max_payload_size);
   debug_assert(GENERATOR_MIN_PAYLOAD_SIZE <= generator->min_payload_size,
                "generator min_payload_size=%lu should be at "
                "least %lu bytes ",
                generator->min_payload_size,
                GENERATOR_MIN_PAYLOAD_SIZE);
   uint64 payload_size =
      generator->min_payload_size
      + (idx % (generator->max_payload_size - generator->min_payload_size + 1));
   uint64 total_size = sizeof(data_handle) + payload_size;
   merge_accumulator_set_class(msg, generator->type);
   merge_accumulator_resize(msg, total_size);
   data_handle *raw_data = merge_accumulator_data(msg);
   memset(raw_data, idx, total_size);
   raw_data->ref_count = 1;
   memcpy(raw_data->data, &idx, sizeof(idx));
}

/* Set the message_type for messages generated by this generator. */
static inline void
message_generate_set_message_type(test_message_generator *gen,
                                  message_type            type)
{
   gen->type = type;
}

static inline uint64
generator_average_message_size(test_message_generator *gen)
{
   return sizeof(data_handle)
          + (gen->min_payload_size + gen->max_payload_size) / 2;
}

/*
 * test_config_init() --
 *
 * Initialize the configuration sub-structures for various sub-systems, using an
 * input master configuration, master_cfg. A few command-line config parameters
 * may have been used to setup master_cfg beyond its initial defaults.
 */
static inline platform_status
test_config_init(trunk_config           *splinter_cfg,  // OUT
                 data_config           **data_cfg,      // OUT
                 shard_log_config       *log_cfg,       // OUT
                 task_system_config     *task_cfg,      // OUT
                 clockcache_config      *cache_cfg,     // OUT
                 allocator_config       *allocator_cfg, // OUT
                 io_config              *io_cfg,        // OUT
                 test_message_generator *gen,
                 master_config          *master_cfg // IN
)
{
   *data_cfg                 = test_data_config;
   (*data_cfg)->max_key_size = master_cfg->max_key_size;

   io_config_init(io_cfg,
                  master_cfg->page_size,
                  master_cfg->extent_size,
                  master_cfg->io_flags,
                  master_cfg->io_perms,
                  master_cfg->io_async_queue_depth,
                  master_cfg->io_filename);

   allocator_config_init(allocator_cfg, io_cfg, master_cfg->allocator_capacity);

   clockcache_config_init(cache_cfg,
                          io_cfg,
                          master_cfg->cache_capacity,
                          master_cfg->cache_logfile,
                          master_cfg->use_stats);

   shard_log_config_init(log_cfg, &cache_cfg->super, *data_cfg);

   uint64 num_bg_threads[NUM_TASK_TYPES] = {0};
   num_bg_threads[TASK_TYPE_NORMAL]      = master_cfg->num_normal_bg_threads;
   num_bg_threads[TASK_TYPE_MEMTABLE]    = master_cfg->num_memtable_bg_threads;
   platform_status rc                    = task_system_config_init(task_cfg,
                                                master_cfg->use_stats,
                                                num_bg_threads,
                                                trunk_get_scratch_size());
   platform_assert_status_ok(rc);

   rc = trunk_config_init(splinter_cfg,
                          &cache_cfg->super,
                          *data_cfg,
                          (log_config *)log_cfg,
                          master_cfg->memtable_capacity,
                          master_cfg->fanout,
                          master_cfg->max_branches_per_node,
                          master_cfg->btree_rough_count_height,
                          master_cfg->filter_remainder_size,
                          master_cfg->filter_index_size,
                          master_cfg->reclaim_threshold,
                          master_cfg->queue_scale_percent,
                          master_cfg->use_log,
                          master_cfg->use_stats,
                          master_cfg->verbose_logging_enabled,
                          master_cfg->log_handle);
   if (!SUCCESS(rc)) {
      return rc;
   }

   gen->type             = MESSAGE_TYPE_INSERT;
   gen->min_payload_size = GENERATOR_MIN_PAYLOAD_SIZE;
   gen->max_payload_size = master_cfg->message_size;
   return rc;
}

/*
 * Some command-line [config] arguments become test execution parameters.
 * Define a structure to hold these when parsing command-line arguments.
 * This is shared by both functional and unit-test methods.
 */
typedef struct test_exec_config {
   uint64 seed;
   uint64 num_inserts;
   bool32 verbose_progress; // --verbose-progress: During test execution
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
test_parse_args_n(trunk_config           *splinter_cfg,  // OUT
                  data_config           **data_cfg,      // OUT
                  io_config              *io_cfg,        // OUT
                  allocator_config       *allocator_cfg, // OUT
                  clockcache_config      *cache_cfg,     // OUT
                  shard_log_config       *log_cfg,       // OUT
                  task_system_config     *task_cfg,      // OUT
                  test_exec_config       *test_exec_cfg, // OUT
                  test_message_generator *gen,           // OUT
                  uint8                   num_config,    // IN
                  int                     argc,          // IN
                  char                   *argv[]         // IN
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
      goto out;
   }

   for (i = 0; i < num_config; i++) {
      rc = test_config_init(&splinter_cfg[i],
                            &data_cfg[i],
                            log_cfg,
                            task_cfg,
                            &cache_cfg[i],
                            allocator_cfg,
                            io_cfg,
                            gen,
                            &master_cfg[i]);
      if (!SUCCESS(rc)) {
         goto out;
      }
   }

   // All the n-SplinterDB instances will work with the same set of
   // test execution parameters.
   if (test_exec_cfg) {
      test_exec_cfg->seed             = master_cfg[0].seed;
      test_exec_cfg->num_inserts      = master_cfg[0].num_inserts;
      test_exec_cfg->verbose_progress = master_cfg[0].verbose_progress;
   }

out:
   platform_free(platform_get_heap_id(), master_cfg);

   return rc;
}

/*
 * test_parse_args() --
 *
 * Parse the command-line configuration arguments to setup the config
 * sub-structures for individual SplinterDB sub-systems.
 */
static inline platform_status
test_parse_args(trunk_config           *splinter_cfg,
                data_config           **data_cfg,
                io_config              *io_cfg,
                allocator_config       *allocator_cfg,
                clockcache_config      *cache_cfg,
                shard_log_config       *log_cfg,
                task_system_config     *task_cfg,
                uint64                 *seed,
                test_message_generator *gen,
                uint64                 *num_memtable_bg_threads,
                uint64                 *num_normal_bg_threads,
                int                     argc,
                char                   *argv[])
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
                          task_cfg,
                          &test_exec_cfg,
                          gen,
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
