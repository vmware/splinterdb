// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0
/*
 * btree_test_common.c:
 *  Shared methods used by different BTree unit-test modules.
 */
#include "btree_test_common.h"

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
