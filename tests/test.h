// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * test.h --
 *
 *     This file contains constants and functions the pertain to
 *     tests.
 */

#ifndef __TEST_H
#define __TEST_H

#include "btree.h"
#include "cache.h"
#include "clockcache.h"
#include "config.h"
#include "data.h"
#include "rc_allocator.h"
#include "shard_log.h"
#include "splinter.h"
#include "test_data.h"

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
kvstore_test(int argc, char *argv[]);

int kvstore_basic_test(int argc, char *argv[]);

int util_test(int argc, char *argv[]);

int
ycsb_test(int argc, char *argv[]);

// FIXME: [tjiaheng 2020-03-23] Consider move to mainTestDispatcher.
/*
 * Initialization for using splinter, need to be called at the
 * start of the test main function.
 */
static inline platform_status
test_init_splinter(platform_heap_id     hid,
                   platform_io_handle  *ioh,
                   task_system        **system,
                   bool                 use_stats,
                   bool                 use_bg_threads,
                   uint8                num_bg_threads[NUM_TASK_TYPES])
{
   // splinter initialization
   return task_system_create(hid, ioh, system, use_stats, use_bg_threads,
         num_bg_threads, splinter_get_scratch_size());
}

static inline void
test_deinit_splinter(platform_heap_id hid, task_system *ts)
{
   task_system_destroy(hid, ts);
}

static inline void
test_key(char          *key,
         test_key_type  key_type,
         uint64         idx,
         uint64         thread_id,
         uint64         semiseq_freq,
         uint64         key_size,
         uint64         period)
{
   memset(key, 0, key_size);
   switch(key_type) {
      case TEST_RANDOM:
         *(uint64 *)key = platform_checksum64(&idx, sizeof(idx), 42);
         break;
      case TEST_SEQ:
         *(uint64 *)key =
            htobe64(platform_checksum64(&thread_id, sizeof(thread_id), 42) +
                    idx);
         break;
      case TEST_SEMISEQ:
         if (idx % semiseq_freq == 0)
            *(uint64 *)key = platform_checksum64(&idx, sizeof(idx), 42);
         else
            *(uint64 *)key =
               htobe64(platform_checksum64(&thread_id, sizeof(thread_id), 42)
                       + idx);
      case TEST_PERIODIC:
      {
         uint64 period_idx = idx % period;
         *(uint64 *)key = platform_checksum64(&period_idx, sizeof(idx), 42);
         break;
      }
   }
}

static inline bool
test_period_complete(uint64 idx,
                     uint64 period)
{
   return idx % period == 0;
}

static inline uint64
test_range(uint64 idx,
           uint64 range_min,
           uint64 range_max)
{
   debug_assert(range_max > range_min);
   return range_min + platform_checksum64(&idx, sizeof(idx), 43) %
      (range_max - range_min);
}

static inline void
test_int_to_key(char *key,
                uint64 idx,
                uint64 key_size)
{
   memset(key, 0, key_size);
   *(uint64 *)key = htobe64(idx);
}

static inline void
test_insert_data(void   *raw_data,
                 int8    ref_count,
                 char   *val,
                 uint64  input_size,
                 uint64  data_size,
                 message_type type)
{
   memset(raw_data, 0, data_size);
   data_handle *data = raw_data;
   data->message_type = type;
   data->ref_count = ref_count;
   memmove(data->data, val, input_size);
}

static inline platform_status
test_count_tuples_in_range(cache        *cc,
                           btree_config *cfg,
                           uint64       *root_addr,
                           page_type     type,
                           uint64        num_trees,
                           char         *low_key,
                           char         *high_key,
                           uint64       *count)     // OUTPUT
{
   btree_iterator itor;
   uint64 i;
   *count = 0;
   for (i = 0; i < num_trees; i++) {
      if (!btree_verify_tree(cc, cfg, root_addr[i], type)) {
         btree_print_tree(cc, cfg, root_addr[i]);
         platform_assert(0);
      }
      btree_iterator_init(
         cc, cfg, &itor, root_addr[i], type, low_key, high_key, TRUE, FALSE, 0);
      bool at_end;
      iterator_at_end(&itor.super, &at_end);
      while (!at_end) {
         char *key = NULL, *data, *last_key = NULL;
         last_key = key;
         iterator_get_curr(&itor.super, &key, &data);
         if (last_key != NULL && btree_key_compare(cfg, last_key, key) > 0) {
            char last_key_str[128], key_str[128];
            btree_key_to_string(cfg, last_key, last_key_str);
            btree_key_to_string(cfg, key, key_str);
            btree_print_tree(cc, cfg, root_addr[i]);
            platform_log("test_count_tuples_in_range: key out of order\n");
            platform_log("last %s\nkey %s\n", last_key_str, key_str);
            platform_assert(0);
         }
         if (btree_key_compare(cfg, low_key, key) > 0) {
            char low_key_str[128], key_str[128], high_key_str[128];
            btree_key_to_string(cfg, low_key, low_key_str);
            btree_key_to_string(cfg, key, key_str);
            btree_key_to_string(cfg, high_key, high_key_str);
            btree_print_tree(cc, cfg, root_addr[i]);
            platform_log("test_count_tuples_in_range: key out of range\n");
            platform_log("low %s\nkey %s\nmax %s\n", low_key_str, key_str, high_key_str);
            platform_assert(0);
         }
         if (high_key && btree_key_compare(cfg, key, high_key) > 0) {
            char low_key_str[128], key_str[128], high_key_str[128];
            btree_key_to_string(cfg, low_key, low_key_str);
            btree_key_to_string(cfg, key, key_str);
            btree_key_to_string(cfg, high_key, high_key_str);
            btree_print_tree(cc, cfg, root_addr[i]);
            platform_log("test_count_tuples_in_range: key out of range\n");
            platform_log("low %s\nkey %s\nmax %s\n", low_key_str, key_str, high_key_str);
            platform_assert(0);
         }
         (*count)++;
         iterator_advance(&itor.super);
         iterator_at_end(&itor.super, &at_end);
      }
      btree_iterator_deinit(&itor);
   }

   return STATUS_OK;
}

static inline int
test_btree_print_all_keys(cache        *cc,
                          btree_config *cfg,
                          uint64       *root_addr,
                          page_type     type,
                          uint64        num_trees,
                          char         *low_key,
                          char         *high_key)
{
   btree_iterator itor;
   uint64 i;
   for (i = 0; i < num_trees; i++) {
      platform_log("tree number %lu\n", i);
      btree_iterator_init(
         cc, cfg, &itor, root_addr[i], type, low_key, high_key, TRUE, FALSE, 0);
      bool at_end;
      iterator_at_end(&itor.super, &at_end);
      while (!at_end) {
         char *key = NULL, *data;
         iterator_get_curr(&itor.super, &key, &data);
         char key_str[128];
         btree_key_to_string(cfg, key, key_str);
         platform_log("%s\n", key_str);
         iterator_advance(&itor.super);
         iterator_at_end(&itor.super, &at_end);
      }
      btree_iterator_deinit(&itor);
   }
   return 0;
}

static inline void
test_config_init(splinter_config     *splinter_cfg,
                 data_config         *data_cfg,
                 shard_log_config    *log_cfg,
                 clockcache_config   *cache_cfg,
                 rc_allocator_config *allocator_cfg,
                 io_config           *io_cfg,
                 master_config       *master_cfg)
{
   *data_cfg = *test_data_config;
   data_cfg->key_size           = master_cfg->key_size;
   data_cfg->message_size       = master_cfg->message_size;

   io_config_init(io_cfg, master_cfg->page_size, master_cfg->extent_size,
                  master_cfg->io_flags, master_cfg->io_perms,
                  master_cfg->io_async_queue_depth, master_cfg->io_filename);

   rc_allocator_config_init(allocator_cfg, master_cfg->page_size,
                            master_cfg->extent_size,
                            master_cfg->allocator_capacity);

   clockcache_config_init(cache_cfg, master_cfg->page_size,
                          master_cfg->extent_size, master_cfg->cache_capacity,
                          master_cfg->cache_logfile, master_cfg->use_stats);

   shard_log_config_init(log_cfg, data_cfg, master_cfg->page_size,
                         master_cfg->extent_size);

   splinter_config_init(splinter_cfg, data_cfg, (log_config *)log_cfg,
                        master_cfg->memtable_capacity,
                        master_cfg->fanout, master_cfg->max_branches_per_node,
                        master_cfg->btree_rough_count_height,
                        master_cfg->page_size, master_cfg->extent_size,
                        master_cfg->filter_remainder_size,
                        master_cfg->filter_index_size,
                        master_cfg->reclaim_threshold,
                        master_cfg->use_log, master_cfg->use_stats);
}

static inline platform_status
test_parse_args_n(splinter_config     *splinter_cfg,
                  data_config         *data_cfg,
                  io_config           *io_cfg,
                  rc_allocator_config *allocator_cfg,
                  clockcache_config   *cache_cfg,
                  shard_log_config    *log_cfg,
                  uint64              *seed,
                  uint8                num_config,
                  int                  argc,
                  char                *argv[])
{
   platform_status rc;
   uint8 i;

   master_config *master_cfg = TYPED_ARRAY_MALLOC(platform_get_heap_id(),
                                                  master_cfg, num_config);
   for (i = 0; i < num_config; i++) {
      config_set_defaults(&master_cfg[i]);
   }

   rc = config_parse(master_cfg, num_config, argc, argv);
   if (!SUCCESS(rc)) {
      return rc;
   }

   for (i = 0; i < num_config; i++) {
      test_config_init(&splinter_cfg[i], &data_cfg[i], log_cfg, &cache_cfg[i],
                       allocator_cfg, io_cfg, &master_cfg[i]);
   }
   *seed = master_cfg[0].seed;
   platform_free(platform_get_heap_id(), master_cfg);

   return STATUS_OK;
}

static inline platform_status
test_parse_args(splinter_config     *splinter_cfg,
                data_config         *data_cfg,
                io_config           *io_cfg,
                rc_allocator_config *allocator_cfg,
                clockcache_config   *cache_cfg,
                shard_log_config    *log_cfg,
                uint64              *seed,
                int                  argc,
                char                *argv[])
{
   return test_parse_args_n(splinter_cfg, data_cfg, io_cfg, allocator_cfg,
                            cache_cfg, log_cfg, seed, 1, argc, argv);
}

// monotonically increasing counter to generate splinter id for tests.
static allocator_root_id counter = 1;

static inline allocator_root_id
test_generate_allocator_root_id()
{
   return __sync_fetch_and_add(&counter, 1);
}

#endif
