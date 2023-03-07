// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * shard_log.h --
 *
 *     This file contains the interface for a sharded write-ahead log.
 */

#pragma once

#include "log.h"
#include "cache.h"
#include "iterator.h"
#include "splinterdb/data.h"
#include "mini_allocator.h"

/*
 * Configuration structure to set up the sharded log sub-system.
 */
typedef struct shard_log_config {
   cache_config *cache_cfg;
   data_config  *data_cfg;
   uint64        seed;
   // data config of point message tree
} shard_log_config;

typedef struct shard_log_thread_data {
   uint64 addr;
   uint64 offset;
} PLATFORM_CACHELINE_ALIGNED shard_log_thread_data;

/*
 * Sharded log context structure.
 */
typedef struct shard_log {
   log_handle            super; // handle to log I/O ops abstraction.
   cache                *cc;
   shard_log_config     *cfg;
   shard_log_thread_data thread_data[MAX_THREADS];
   mini_allocator        mini;
   uint64                addr;
   uint64                meta_head;
   uint64                magic;
} shard_log;

typedef struct log_entry log_entry;

typedef struct shard_log_iterator {
   iterator          super;
   shard_log_config *cfg;
   char             *contents;
   log_entry       **entries;
   uint64            num_entries;
   uint64            pos;
   size_t            contents_size; // # bytes allocate to contents array
   size_t            entries_size;  // # bytes allocate to entries array
} shard_log_iterator;

/*
 * ---------------------------------------------------------------
 * Sharded log page header stucture: Disk-resident structure.
 * Page Type == PAGE_TYPE_LOG
 * ---------------------------------------------------------------
 */
typedef struct ONDISK shard_log_hdr {
   checksum128 checksum;
   uint64      magic;
   uint64      next_extent_addr;
   uint16      num_entries;
} shard_log_hdr;

platform_status
shard_log_init(shard_log *log, cache *cc, shard_log_config *cfg);

void
shard_log_zap(shard_log *log);

platform_status
shard_log_iterator_init(cache              *cc,
                        shard_log_config   *cfg,
                        platform_heap_id    hid,
                        uint64              addr,
                        uint64              magic,
                        shard_log_iterator *itor);

void
shard_log_iterator_deinit(platform_heap_id hid, shard_log_iterator *itor);

void
shard_log_config_init(shard_log_config *log_cfg,
                      cache_config     *cache_cfg,
                      data_config      *data_cfg);
void
shard_log_print(shard_log *log);
