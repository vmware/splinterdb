// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0
/*
 * btree_test_common.c:
 *  Shared methods used by different BTree unit-test modules.
 */
#include "btree_test_common.h"

int
init_data_config_from_master_config(data_config   *data_cfg,
                                    master_config *master_cfg)
{
   data_cfg->max_key_size = master_cfg->max_key_size;
   return 1;
}

int
init_io_config_from_master_config(io_config *io_cfg, master_config *master_cfg)
{
   io_config_init(io_cfg,
                  master_cfg->page_size,
                  master_cfg->extent_size,
                  master_cfg->io_flags,
                  master_cfg->io_perms,
                  master_cfg->io_async_queue_depth,
                  master_cfg->io_filename);
   return 1;
}

int
init_rc_allocator_config_from_master_config(allocator_config *allocator_cfg,
                                            master_config    *master_cfg,
                                            io_config        *io_cfg)
{
   allocator_config_init(allocator_cfg, io_cfg, master_cfg->allocator_capacity);
   return 1;
}

int
init_clockcache_config_from_master_config(clockcache_config *cache_cfg,
                                          master_config     *master_cfg,
                                          io_config         *io_cfg)
{
   clockcache_config_init(cache_cfg,
                          io_cfg,
                          master_cfg->cache_capacity,
                          master_cfg->cache_logfile,
                          master_cfg->use_stats);
   return 1;
}

int
init_task_config_from_master_config(task_system_config  *task_cfg,
                                    const master_config *master_cfg,
                                    uint64               scratch_size)
{
   platform_status rc;
   uint64          num_bg_threads[NUM_TASK_TYPES] = {0};
   num_bg_threads[TASK_TYPE_NORMAL]   = master_cfg->num_normal_bg_threads;
   num_bg_threads[TASK_TYPE_MEMTABLE] = master_cfg->num_memtable_bg_threads;

   rc = task_system_config_init(
      task_cfg, master_cfg->use_stats, num_bg_threads, scratch_size);
   return SUCCESS(rc);
}

int
init_btree_config_from_master_config(btree_config  *dbtree_cfg,
                                     master_config *master_cfg,
                                     cache_config  *cache_cfg,
                                     data_config   *data_cfg)
{
   btree_config_init(
      dbtree_cfg, cache_cfg, data_cfg, master_cfg->btree_rough_count_height);
   return 1;
}
