// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "allocator.h"
#include "cache.h"
#include "trunk.h"
#include "platform.h"

platform_status
test_functionality(allocator           *al,
                   io_handle           *io,
                   cache               *cc[],
                   trunk_config        *cfg,
                   uint64               seed,
                   uint64               num_inserts,
                   uint64               correctness_check_frequency,
                   task_system         *ts,
                   platform_heap_handle hh,
                   platform_heap_id     hid,
                   uint8                num_tables,
                   uint8                num_caches,
                   uint32               max_async_inflight);
