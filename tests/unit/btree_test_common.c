// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0
/*
 * btree_test_common.c:
 *  Shared methods used by different BTree unit-test modules.
 */
#include "btree_test_common.h"

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
