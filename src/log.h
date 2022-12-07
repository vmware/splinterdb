// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * log.h --
 *
 *     This file contains the abstract interface for a write-ahead log.
 */

#ifndef __LOG_H
#define __LOG_H

#include "platform.h"
#include "cache.h"
#include "util.h"
#include "splinterdb/data.h"

typedef enum node_type {
   NODE_TYPE_INVALID = 0,
   NODE_TYPE_TRUNK,
   NODE_TYPE_SUPERBLOCK,
   NODE_TYPE_BTREE,
   NODE_TYPE_PIVOT_DATA          = 1000
} node_type;

typedef struct log_handle   log_handle;
typedef struct log_iterator log_iterator;
typedef struct log_config   log_config;

typedef int (*log_write_fn)(log_handle *log,
                            slice       key,
                            message     data,
                            uint64      generation,
                            node_type   nt,
                            uint64      page_addr,
                            uint64*     lsn);
typedef void (*log_release_fn)(log_handle *log);
typedef uint64 (*log_addr_fn)(log_handle *log);
typedef uint64 (*flush_lsn_fn)(log_handle *log);
typedef uint64 (*log_magic_fn)(log_handle *log);

typedef struct log_ops {
   log_write_fn   write;
   log_release_fn release;
   log_addr_fn    addr;
   flush_lsn_fn   flush_lsn;
   log_addr_fn    meta_addr;
   log_magic_fn   magic;
} log_ops;

// to sub-class log, make a log_handle your first field
struct log_handle {
   const log_ops *ops;
};

static inline int
log_write(log_handle *log, slice key, message data, uint64 generation, node_type nt,  uint64 page_addr, uint64* lsn)
{
   return log->ops->write(log, key, data, generation, nt, page_addr, lsn);
}

static inline void
log_release(log_handle *log)
{
   log->ops->release(log);
}

static inline uint64
log_addr(log_handle *log)
{
   return log->ops->addr(log);
}

static inline uint64
flush_lsn(log_handle *log)
{
   return log->ops->flush_lsn(log);
}

static inline uint64
log_meta_addr(log_handle *log)
{
   return log->ops->meta_addr(log);
}

static inline uint64
log_magic(log_handle *log)
{
   return log->ops->magic(log);
}

log_handle *
log_create(cache *cc, log_config *cfg, platform_heap_id hid);

#endif //__LOG_H
