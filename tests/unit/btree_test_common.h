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

int
init_btree_config_from_master_config(btree_config  *dbtree_cfg,
                                     master_config *master_cfg,
                                     cache_config  *cache_cfg,
                                     data_config   *data_cfg);
