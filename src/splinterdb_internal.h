// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinterdb_internal.h --
 *
 *     This file contains the functions used internally in splinterdb.
 */

#pragma once

#include "data_internal.h"
#include "splinterdb/splinterdb.h"
#include "clockcache.h"
#include "trunk.h"
#include "rc_allocator.h"
#include "shard_log.h"

typedef struct splinterdb {
   task_system       *task_sys;
   io_config          io_cfg;
   platform_io_handle io_handle;
   allocator_config   allocator_cfg;
   rc_allocator       allocator_handle;
   clockcache_config  cache_cfg;
   clockcache         cache_handle;
   shard_log_config   log_cfg;
   task_system_config task_cfg;
   allocator_root_id  trunk_id;
   trunk_config       trunk_cfg;
   trunk_handle      *spl;
   platform_heap_id   heap_id;
   data_config       *data_cfg;
   bool               we_created_heap;
} splinterdb;

/*
 *-----------------------------------------------------------------------------
 * _splinterdb_lookup_result structure --
 *-----------------------------------------------------------------------------
 */

typedef struct {
   merge_accumulator value;
} _splinterdb_lookup_result;

_Static_assert(sizeof(_splinterdb_lookup_result)
                  <= sizeof(splinterdb_lookup_result),
               "sizeof(splinterdb_lookup_result) is too small");

_Static_assert(alignof(splinterdb_lookup_result)
                  == alignof(_splinterdb_lookup_result),
               "mismatched alignment for splinterdb_lookup_result");


int
splinterdb_create_or_open(const splinterdb_config *kvs_cfg,      // IN
                          splinterdb             **kvs_out,      // OUT
                          bool                     open_existing // IN
);
