// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * allocator.c --
 *
 *     This file implements the abstract base behavior shared by allocators.
 */

#include "allocator.h"
#include "poison.h"

void
allocator_config_init(allocator_config *allocator_cfg,
                      io_config        *io_cfg,
                      uint64            capacity)
{
   ZERO_CONTENTS(allocator_cfg);

   allocator_cfg->io_cfg   = io_cfg;
   allocator_cfg->capacity = capacity;

   allocator_cfg->page_capacity   = capacity / io_cfg->page_size;
   allocator_cfg->extent_capacity = capacity / io_cfg->extent_size;
   uint64 log_extent_size         = 63 - __builtin_clzll(io_cfg->extent_size);
   allocator_cfg->extent_mask     = ~((1ULL << log_extent_size) - 1);
}
