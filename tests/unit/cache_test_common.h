// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0
/*
 * cache_test_common.h: Shared header file with protoypes etc. used
 * by different unit-test modules that use the cache.
 */

#pragma once

#include "../config.h"
#include "io.h"
#include "rc_allocator.h"
#include "clockcache.h"

// Function Prototypes
int
init_data_config_from_master_config(data_config   *data_cfg,
                                    master_config *master_cfg);

int
init_io_config_from_master_config(io_config *io_cfg, master_config *master_cfg);


int
init_task_config_from_master_config(task_system_config  *task_cfg,
                                    const master_config *master_cfg,
                                    uint64               scratch_size);

int
init_allocator_config_from_master_config(allocator_config *allocator_cfg,
                                         master_config    *master_cfg,
                                         io_config        *io_cfg);

int
init_clockcache_config_from_master_config(clockcache_config *cache_cfg,
                                          master_config     *master_cfg,
                                          io_config         *io_cfg);
