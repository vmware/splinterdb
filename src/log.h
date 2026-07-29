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
#include "iterator.h"

typedef struct log_handle   log_handle;
typedef struct log_iterator log_iterator;
typedef struct log_config   log_config;

/*
 * The on-disk head of one mini-allocator-backed log stream: the data head
 * (where replay begins), the metadata head (which owns the stream's extents),
 * and a per-stream magic that validates its pages.  Fixed at creation; a
 * higher-level checkpoint record stores it to later find the stream for replay
 * or reclaim it via log_dec_ref().
 */
typedef struct log_head {
   uint64 addr;      // data head: first log page, where replay begins
   uint64 meta_addr; // mini-allocator metadata head; owns the stream's extents
   uint64 magic;     // per-stream magic; validates the stream's pages
} log_head;

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
 * afterward.  The stream's head is fixed at creation and obtained then via
 * log_get_head(), so seal needs no out-parameter; the caller frees the on-disk
 * extents later via log_dec_ref().
 */
typedef platform_status (*log_seal_fn)(log_handle *log);
/*
 * The stream's durable head, fixed at creation.  The caller records it
 * (e.g. in the superblock) as soon as the log is created, so that a crash
 * mid-stream can find the stream for replay.
 */
typedef log_head (*log_head_fn)(log_handle *log);
/*
 * Bytes appended to the stream so far, so a caller can decide when to retire it.
 * Excludes the implementation's fixed per-stream overhead: a stream that has had
 * nothing written to it reports 0, which keeps a size-triggered policy from
 * firing on a brand-new stream no matter how small its threshold.  A
 * conservative measure otherwise -- space is counted as it is reserved, so this
 * rounds up to whatever allocation unit the implementation uses.
 */
typedef uint64 (*log_size_fn)(log_handle *log);

typedef struct log_ops {
   log_write_fn write;
   log_seal_fn  seal;
   log_head_fn  head;
   log_size_fn  size;
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
 * returns.  Capture the head via log_get_head() beforehand (it is fixed at
 * creation).
 */
static inline platform_status
log_seal(log_handle *log)
{
   return log->ops->seal(log);
}

/* The stream's durable head (fixed at creation).  See log_head_fn. */
static inline log_head
log_get_head(log_handle *log)
{
   return log->ops->head(log);
}

/* Bytes the stream currently occupies on disk.  See log_size_fn. */
static inline uint64
log_get_size(log_handle *log)
{
   return log->ops->size(log);
}

/*
 * A log_handle is created by the concrete log implementation -- e.g.
 * shard_log_create() -- and then driven through the abstract ops above; it is
 * freed by log_seal().
 */

/*
 * Release a sealed log identified by its log_head: drop the reference its
 * metadata head holds, freeing the stream's on-disk extents.  Takes no handle
 * -- the handle was freed by log_seal(); the caller retained only the head
 * (log_get_head(), captured at creation).
 */
void
log_dec_ref(cache *cc, const log_head *head);

/*
 * ---- Abstract log iteration ----
 *
 * A log_iterator reads a sealed log's records in generation order (used by
 * crash recovery to replay a stream onto the durable root).  It is a generic
 * iterator (curr/can_next/next, via the embedded `super`) plus the log-specific
 * ops below.  To sub-class, make a log_iterator your first field.
 */
typedef void (*log_iterator_curr_generations_fn)(log_iterator *itor,
                                                 uint64 *memtable_generation,
                                                 uint64 *leaf_generation);
typedef void (*log_iterator_deinit_fn)(log_iterator *itor);

typedef struct log_iterator_ops {
   log_iterator_curr_generations_fn curr_generations;
   log_iterator_deinit_fn           deinit;
} log_iterator_ops;

struct log_iterator {
   iterator                super; // generic iteration: curr / can_next / next
   const log_iterator_ops *ops;
};

/*
 * A log_iterator is created by the concrete log implementation -- e.g.
 * shard_log_iterator_create() -- which fills in the ops below; callers then
 * drive it through this abstract interface and free it with
 * log_iterator_deinit().
 */

/* Whether a current record exists (safe to call curr / curr_generations). */
static inline bool32
log_iterator_can_next(log_iterator *itor)
{
   return iterator_can_next(&itor->super);
}

/* The current record's key and message.  Requires a current record. */
static inline void
log_iterator_curr(log_iterator *itor, key *curr_key, message *msg)
{
   iterator_curr(&itor->super, curr_key, msg);
}

/* The current record's generation metadata.  Requires a current record. */
static inline void
log_iterator_curr_generations(log_iterator *itor,
                              uint64       *memtable_generation,
                              uint64       *leaf_generation)
{
   itor->ops->curr_generations(itor, memtable_generation, leaf_generation);
}

/* Advance to the next record. */
static inline platform_status
log_iterator_next(log_iterator *itor)
{
   return iterator_next(&itor->super);
}

/* Free the iterator and its resources; the handle is invalid afterward. */
static inline void
log_iterator_deinit(log_iterator *itor)
{
   itor->ops->deinit(itor);
}
