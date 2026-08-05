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
#include "log_data.h"

typedef struct log_handle   log_handle;
typedef struct log_iterator log_iterator;
typedef struct log_config   log_config;

typedef int (*log_write_fn)(log_handle *log,
                            key         tuple_key,
                            message     data,
                            uint64      memtable_generation,
                            uint64      leaf_generation);
/*
 * Make everything written so far durable, and leave the stream open.
 *
 * Closes the current group -- writing out whatever each writer still has
 * staged, and marking the result as a complete unit for replay -- then waits
 * for those writes to land and takes a durable barrier.  Subsequent writes
 * begin a new group.
 *
 * The caller must exclude concurrent log_write() calls for the duration, for
 * the same reason log_seal() requires it and a stronger one besides: a group
 * boundary is only correct if no writer holds a record that is already visible
 * to readers but not yet staged.  Such a record would land in the *next* group
 * while records that happened after it sit in this one, so a crash that kept
 * this group and lost the next would recover a state that never existed.
 *
 * On failure nothing new is guaranteed durable.  The implementation retains
 * the exact close/writeback state. The implementation must complete that retry
 * before accepting another record (whether through an explicit retry here or
 * on the next append); once it succeeds, the stream is open on the next group.
 */
typedef platform_status (*log_make_durable_fn)(log_handle *log);

/*
 * Finish the stream: write out everything still staged, and mark the last of it
 * as the end of the stream so that replay can tell a complete stream from one a
 * crash truncated.  The stream is immutable afterward, but the handle remains
 * valid and must still be released with log_deinit().
 *
 * Makes the stream durable as it finishes it, exactly as log_make_durable()
 * does -- there is no point completing a stream a crash could still lose -- so
 * callers need not follow with a barrier of their own.
 *
 * The caller must exclude concurrent log_write() and log_seal() calls.  The
 * stream's head is fixed at creation and obtained then via log_get_head(), so
 * seal needs no out-parameter; the caller asks the concrete implementation to
 * free the on-disk extents later.
 *
 * A caller that is about to discard the stream outright should skip this and
 * call log_deinit() alone: there is no point writing a terminator onto extents
 * that are about to be freed.
 *
 * On failure nothing new is guaranteed durable, but seal may simply be called
 * again: the implementation retains the exact structural/writeback state and
 * resumes from where it stopped.  Calling seal again after success is
 * idempotent.
 */
typedef platform_status (*log_seal_fn)(log_handle *log);
/*
 * Release the stream's in-memory resources and free the handle, which is
 * invalid afterward.  Writes nothing, so it cannot fail.
 *
 * Separate from seal because the two are wanted independently: a stream being
 * discarded needs only this, and a stream being finished needs seal's
 * durability guarantees before its handle goes away.
 */
typedef void (*log_deinit_fn)(log_handle *log);
/*
 * The stream's durable head, fixed at creation.  The caller records it
 * (e.g. in the superblock) as soon as the log is created, so that a crash
 * mid-stream can find the stream for replay.
 */
typedef log_head (*log_head_fn)(log_handle *log);
/*
 * Whether the stream has ever accepted a record.  The caller must exclude
 * concurrent log_write() calls while inspecting this state.
 */
typedef bool32 (*log_is_empty_fn)(log_handle *log);
/*
 * Bytes appended to the stream so far, so a caller can decide when to retire
 * it. Excludes the implementation's fixed per-stream overhead: a stream that
 * has had nothing written to it reports 0, which keeps a size-triggered policy
 * from firing on a brand-new stream no matter how small its threshold.  A
 * conservative measure otherwise -- space is counted as it is reserved, so this
 * rounds up to whatever allocation unit the implementation uses.
 */
typedef uint64 (*log_size_fn)(log_handle *log);

typedef struct log_ops {
   log_write_fn        write;
   log_make_durable_fn make_durable;
   log_seal_fn         seal;
   log_deinit_fn       deinit;
   log_head_fn         head;
   log_is_empty_fn     is_empty;
   log_size_fn         size;
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
 * Make everything written so far durable, leaving the stream open.  See
 * log_make_durable_fn for the required exclusion.
 */
static inline platform_status
log_make_durable(log_handle *log)
{
   return log->ops->make_durable(log);
}

/*
 * Finish the stream, durably.  See log_seal_fn for the required exclusion.  The
 * handle stays valid; release it with log_deinit().  Capture the head via
 * log_get_head() beforehand (it is fixed at creation).
 */
static inline platform_status
log_seal(log_handle *log)
{
   return log->ops->seal(log);
}

/* Free the handle, which is invalid afterward.  See log_deinit_fn. */
static inline void
log_deinit(log_handle *log)
{
   log->ops->deinit(log);
}

/* The stream's durable head (fixed at creation).  See log_head_fn. */
static inline log_head
log_get_head(log_handle *log)
{
   return log->ops->head(log);
}

/* Whether this stream has accepted any records. */
static inline bool32
log_is_empty(log_handle *log)
{
   return log->ops->is_empty(log);
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
 * freed by log_deinit().
 */

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
typedef bool32 (*log_iterator_stream_complete_fn)(log_iterator *itor);

typedef struct log_iterator_ops {
   log_iterator_curr_generations_fn curr_generations;
   log_iterator_deinit_fn           deinit;
   log_iterator_stream_complete_fn  stream_complete;
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

/*
 * Whether the records this iterator yields run all the way to the end of a
 * sealed stream, as opposed to stopping early because the stream was truncated
 * by a crash.
 *
 * Recovery needs this to decide whether it may go on to the next log.  The
 * records of a truncated stream are still a valid prefix on their own, but
 * anything written after it must not be replayed on top of them: doing so would
 * skip whatever was lost in between and produce a state that never existed.
 *
 * Normally FALSE for a live stream, which has no end yet.  It can be TRUE when
 * sealing succeeded but publication of the log cut failed, leaving the sealed
 * physical stream in the durable record's live slot.
 */
static inline bool32
log_iterator_stream_complete(log_iterator *itor)
{
   return itor->ops->stream_complete(itor);
}

/* Free the iterator and its resources; the handle is invalid afterward. */
static inline void
log_iterator_deinit(log_iterator *itor)
{
   itor->ops->deinit(itor);
}
