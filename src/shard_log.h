// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * shard_log.h --
 *
 *     This file contains the interface for a sharded write-ahead log.
 */

#pragma once

#include "log.h"
#include "platform_threads.h"
#include "platform_hash.h"
#include "cache.h"
#include "iterator.h"
#include "splinterdb/data.h"
#include "blob_build.h"
#include "mini_allocator.h"

/*
 * Configuration structure to set up the sharded log sub-system.
 */
typedef struct shard_log_config {
   cache_config     *cache_cfg;
   data_config      *data_cfg;
   uint64            seed;
   blob_build_config blob_cfg;
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
   platform_heap_id      heap_id;
   shard_log_thread_data thread_data[MAX_THREADS];
   mini_allocator        mini;
   uint64                addr;
   uint64                meta_head;
   uint64                magic;
} shard_log;

typedef struct log_entry log_entry;

typedef struct shard_log_iterator {
   log_iterator      super; // IS-A log_iterator IS-A generic iterator
   platform_heap_id  heap_id;
   cache            *cc;
   shard_log_config *cfg;
   char             *contents;
   log_entry       **entries;
   uint64            num_entries;
   uint64            pos;
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

/*
 * Create a fresh sharded write-ahead log stream.  Returns an abstract
 * log_handle (or NULL on failure) to be driven through the log.h interface and
 * retired with log_seal().
 */
log_handle *
shard_log_create(cache *cc, shard_log_config *cfg, platform_heap_id hid);

/*
 * Create an iterator over the sharded log segment identified by `segment`,
 * reading its records in generation order.  Returns an abstract log_iterator
 * (or NULL on failure) to be driven through the log.h interface and freed with
 * log_iterator_deinit().
 */
log_iterator *
shard_log_iterator_create(cache           *cc,
                          shard_log_config *cfg,
                          platform_heap_id  hid,
                          log_segment_info  segment);

void
shard_log_config_init(shard_log_config *log_cfg,
                      cache_config     *cache_cfg,
                      data_config      *data_cfg);
void
shard_log_print(shard_log *log);
