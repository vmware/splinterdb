// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * shard_log.h --
 *
 *     This file contains the interface for a sharded write-ahead log.
 */

#ifndef __SHARD_LOG_H
#define __SHARD_LOG_H

#include "log.h"
#include "cache.h"
#include "iterator.h"
#include "data.h"
#include "mini_allocator.h"
#include "task.h"

typedef struct shard_log_config {
   uint64       page_size;
   uint64       extent_size;
   uint64       entries_per_page;
   uint64       seed;
   // data config of point message tree
   data_config *data_cfg;
   // FIXME: [yfogel 2020-07-01] If we ever need to log range_delete
   //        in shard log, need to have separate point and rangedelete
   //        data_cfgs (data_cfg above is the point-message config)
} shard_log_config;

typedef struct shard_log_thread_data {
   uint64 addr;
   uint64 pos;
   uint64 offset;
} PLATFORM_CACHELINE_ALIGNED shard_log_thread_data;

typedef struct shard_log {
   log_handle             super;
   cache                 *cc;
   shard_log_config      *cfg;
   shard_log_thread_data  thread_data[MAX_THREADS];
   mini_allocator         mini;
   uint64                 addr;
   uint64                 meta_head;
   uint64                 magic;
} shard_log;

typedef struct shard_log_iterator {
   iterator          super;
   shard_log_config *cfg;
   char             *contents;
   char             *cursor;
   uint64            pos;
   uint64            total;
} shard_log_iterator;

typedef struct shard_log_hdr {
   checksum128   checksum;
   uint64        magic;
   uint64        next_extent_addr;
} shard_log_hdr;

platform_status
shard_log_init(shard_log        *log,
               cache            *cc,
               shard_log_config *cfg);

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

void shard_log_config_init(shard_log_config *log_cfg,
                           data_config      *data_cfg,
                           uint64            page_size,
                           uint64            extent_size);
void
shard_log_print(shard_log *log);

#endif //__SHARD_LOG_H
