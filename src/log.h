// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * log.h --
 *
 *     This file contains the abstract interface for a write-ahead log.
 */

#pragma once

#include "platform.h"
#include "cache.h"
#include "util.h"
#include "data_internal.h"

typedef struct log_handle   log_handle;
typedef struct log_iterator log_iterator;
typedef struct log_config   log_config;

typedef int (*log_write_fn)(log_handle *log,
                            key         tuple_key,
                            message     data,
                            uint64      generation);
typedef void (*log_release_fn)(log_handle *log);
typedef uint64 (*log_addr_fn)(log_handle *log);
typedef uint64 (*log_magic_fn)(log_handle *log);

typedef struct log_ops {
   log_write_fn   write;
   log_release_fn release;
   log_addr_fn    addr;
   log_addr_fn    meta_addr;
   log_magic_fn   magic;
} log_ops;

// to sub-class log, make a log_handle your first field
struct log_handle {
   const log_ops *ops;
};

static inline int
log_write(log_handle *log, key tuple_key, message data, uint64 generation)
{
   return log->ops->write(log, tuple_key, data, generation);
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
