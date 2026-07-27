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
 * Finalize and retire the log stream, terminally.  Finalizes the current
 * append pages into checksummed, immutable pages, releases in-memory
 * resources, and frees the handle (which is invalid afterward).
 *
 * The caller must exclude concurrent log_write() and log_seal() calls.  seal()
 * itself issues no writeback or durable barrier: to make the sealed pages
 * durable, the caller takes the cache writeback fence + a durable barrier
 * afterward.  The stream's identity is fixed at log_create() and obtained then
 * via log_get_segment_info(), so seal needs no identity out-parameter; the
 * caller frees the on-disk extents later via log_dec_ref().
 */
typedef platform_status (*log_seal_fn)(log_handle *log);
/*
 * The stream's durable identity, fixed at creation.  The caller records it
 * (e.g. in the superblock) as soon as the log is created, so that a crash
 * mid-stream can find the stream for replay.
 */
typedef log_segment_info (*log_segment_info_fn)(log_handle *log);

typedef struct log_ops {
   log_write_fn        write;
   log_seal_fn         seal;
   log_segment_info_fn segment_info;
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
 * Finalize and retire the log, freeing the handle.  See log_seal_fn for the
 * required exclusion and durability ordering; the handle is invalid after this
 * returns.  Capture the identity via log_get_segment_info() beforehand (it is
 * fixed at creation).
 */
static inline platform_status
log_seal(log_handle *log)
{
   return log->ops->seal(log);
}

/* The stream's durable identity (fixed at creation).  See log_segment_info_fn.
 */
static inline log_segment_info
log_get_segment_info(log_handle *log)
{
   return log->ops->segment_info(log);
}

log_handle *
log_create(cache *cc, log_config *cfg, platform_heap_id hid);

/*
 * Release a sealed log segment identified by its log_segment_info: drop the
 * reference its metadata extent holds, freeing the segment's on-disk extents.
 * Takes no handle -- the handle was freed by log_seal(); the caller retained
 * only the identity (log_get_segment_info(), captured at creation).
 */
void
log_dec_ref(cache *cc, const log_segment_info *segment);
