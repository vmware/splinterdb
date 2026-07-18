// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * log.h --
 *
 *     This file contains the abstract interface for a write-ahead log.
 */

#pragma once

#include "cache.h"
#include "data_internal.h"

typedef struct log_handle   log_handle;
typedef struct log_iterator log_iterator;
typedef struct log_config   log_config;

/*
 * Identity of one mini-allocator-backed log stream.  It is sufficient for a
 * higher-level checkpoint record to describe a stream, but not by itself a
 * durable descriptor: core will later persist this information in an
 * independently checksummed log-segment record.
 */
typedef struct log_segment_info {
   uint64 addr;
   uint64 meta_addr;
   uint64 magic;
} log_segment_info;

typedef int (*log_write_fn)(log_handle *log,
                            key         tuple_key,
                            message     data,
                            uint64      memtable_generation,
                            uint64      leaf_generation);
/*
 * Finalize the current append pages into checksummed, immutable pages.
 *
 * The caller must exclude concurrent log_write() calls until it has taken the
 * cache writeback fence that is to make the pages durable.  It must also
 * serialize concurrent log_seal() calls.  seal() itself does not issue I/O or
 * a durable barrier.
 *
 * This is deliberately not a persisted replay-boundary descriptor.  A log
 * implementation whose metadata can grow after sealing needs an additional
 * boundary in the checkpoint record to exclude those later entries.
 */
typedef platform_status (*log_seal_fn)(log_handle *log);
/*
 * Detach the current stream after sealing it and prepare a distinct fresh
 * stream. The caller must exclude writes throughout the operation and make
 * the returned identities durable before allowing writes to the fresh stream.
 * A zero sealed.meta_addr means the old stream was empty and discarded.
 * Rotation alone does not advance the logical durable-log tail: that requires
 * a separate, durable tail/manifest publication by the caller.
 */
typedef platform_status (*log_rotate_fn)(log_handle       *log,
                                         log_segment_info *sealed,
                                         log_segment_info *fresh);
typedef void (*log_release_fn)(log_handle *log);
typedef uint64 (*log_addr_fn)(log_handle *log);
typedef uint64 (*log_magic_fn)(log_handle *log);

typedef struct log_ops {
   log_write_fn   write;
   log_seal_fn    seal;
   log_rotate_fn  rotate;
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
log_write(log_handle *log,
          key         tuple_key,
          message     data,
          uint64      memtable_generation,
          uint64      leaf_generation)
{
   return log->ops->write(
      log, tuple_key, data, memtable_generation, leaf_generation);
}

/*
 * Finalize the log's current append pages.  See log_seal_fn for the required
 * exclusion, boundary, and durability ordering.
 */
static inline platform_status
log_seal(log_handle *log)
{
   return log->ops->seal(log);
}

static inline platform_status
log_rotate(log_handle *log, log_segment_info *sealed, log_segment_info *fresh)
{
   return log->ops->rotate(log, sealed, fresh);
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
