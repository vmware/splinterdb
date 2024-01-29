// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0
/*
 * btree_test_common.h: Shared header file with protoypes etc. used
 * by different BTree unit-test modules.
 */

#pragma once

#include "../config.h"
#include "io.h"
#include "rc_allocator.h"
#include "clockcache.h"
#include "task.h"
#include "btree.h"

// Function Prototypes
int
init_data_config_from_master_config(data_config   *data_cfg,
                                    master_config *master_cfg);

int
init_io_config_from_master_config(io_config *io_cfg, master_config *master_cfg);


int
init_rc_allocator_config_from_master_config(allocator_config *allocator_cfg,
                                            master_config    *master_cfg,
                                            io_config        *io_cfg);

int
init_clockcache_config_from_master_config(clockcache_config *cache_cfg,
                                          master_config     *master_cfg,
                                          io_config         *io_cfg);

int
init_task_config_from_master_config(task_system_config  *task_cfg,
                                    const master_config *master_cfg,
                                    uint64               scratch_size);

int
init_btree_config_from_master_config(btree_config  *dbtree_cfg,
                                     master_config *master_cfg,
                                     cache_config  *cache_cfg,
                                     data_config   *data_cfg);
