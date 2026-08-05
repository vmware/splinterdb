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

/*
 * Opaque ownership of one in-flight write in a concrete log group.  Callers
 * allocate this on their stack, initialize it with log_write_reserve(), and
 * consume it exactly once with log_write_reserved().  The fields are private
 * to the log implementation.
 */
typedef struct log_write_token {
   log_handle *log;
   void       *internal;
} log_write_token;

/*
 * Opaque durability cut returned by log_make_durable_begin().  A successful
 * begin holds one reference on the in-memory log handle until the matching
 * log_make_durable_wait(). Once the owner has otherwise quiesced and retired
 * the stream, that pin lets it deinit the stream between the two calls.
 */
typedef uint64 log_durable_ticket;

/*
 * Reserve the accepting durability group for a future append.  This operation
 * is infallible for a live log: it performs no allocation or I/O and does not
 * reject a group poisoned by an earlier append.  It only takes the log's short
 * group-state mutex.
 *
 * A layer which publishes an update before writing its log record can invoke
 * this while holding the lock which protects that update's linearization.  A
 * durability cut which follows the reservation will then wait for its eventual
 * append, without excluding other writers while it performs I/O.
 *
 * The caller must already have excluded seal/deinit and must consume the token
 * exactly once with log_write_reserved().  There is deliberately no cancel:
 * reserve belongs at a point after which the logical update cannot fail.
 */
typedef void (*log_write_reserve_fn)(log_handle *log, log_write_token *token);

/* Append through, and always consume, a prior reservation. */
typedef int (*log_write_reserved_fn)(log_write_token *token,
                                     key              tuple_key,
                                     message          data,
                                     uint64           memtable_generation,
                                     uint64           leaf_generation);
/*
 * Split-phase durability operation. Begin atomically closes the group which
 * accepted every log_write_reserve() that selected it before the cut, installs
 * a fresh group, and returns without waiting for those reservations or for I/O.
 * Writers therefore need not be excluded from either phase.
 *
 * Wait drains the selected group and every predecessor, takes a durable
 * barrier, and leaves the stream open. Concurrent begins and waits may pipeline
 * groups; a later wait may make several earlier tickets durable at once.
 * Transient graduation/writeback failures retain the exact state for a later
 * begin/wait pair to retry. If log_write_reserved() failed after its update
 * became visible, that group is permanently poisoned instead: every wait
 * whose cut includes it returns the original append error and no later group
 * may become complete on disk.
 */
typedef platform_status (
   *log_make_durable_begin_fn)(log_handle *log, log_durable_ticket *ticket_out);
typedef platform_status (*log_make_durable_wait_fn)(log_handle        *log,
                                                    log_durable_ticket ticket);

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
 * The caller must exclude concurrent reservations and log_seal() calls, and
 * wait for every existing reservation to be consumed. The stream's head is
 * fixed at creation and obtained then via log_get_head(), so seal needs no
 * out-parameter; the caller asks the concrete implementation to free the
 * on-disk extents later.
 *
 * A caller that is about to discard the stream outright should skip this and
 * call log_deinit() alone: there is no point writing a terminator onto extents
 * that are about to be freed.
 *
 * On a transient failure nothing new is guaranteed durable, but seal may
 * simply be called again: the implementation retains the exact
 * structural/writeback state and resumes from where it stopped. A group
 * poisoned by an earlier append failure is permanent, so seal continues to
 * return that append error. Calling seal again after success is idempotent.
 */
typedef platform_status (*log_seal_fn)(log_handle *log);
/*
 * Release the stream owner's reference. Before calling this, the owner must
 * exclude new reservations, log_make_durable_begin(), and log_seal() calls and
 * wait for every reserved write to be consumed. It need not wait for
 * log_make_durable_wait() calls consuming tickets issued before deinit: those
 * tickets defer the actual free, and the final matching wait may free the
 * handle as it returns. The handle is otherwise invalid as soon as deinit is
 * called. Deinit writes nothing, so it cannot fail.
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
 * concurrent reserved writes while inspecting this state.
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
   log_write_reserve_fn      write_reserve;
   log_write_reserved_fn     write_reserved;
   log_make_durable_begin_fn make_durable_begin;
   log_make_durable_wait_fn  make_durable_wait;
   log_seal_fn               seal;
   log_deinit_fn             deinit;
   log_head_fn               head;
   log_is_empty_fn           is_empty;
   log_size_fn               size;
} log_ops;

// to sub-class log, make a log_handle your first field
struct log_handle {
   const log_ops *ops;
};

static inline void
log_write_reserve(log_handle *log, log_write_token *token)
{
   platform_assert(log != NULL);
   platform_assert(token != NULL);
   log->ops->write_reserve(log, token);
}

/* Append a reserved record and consume token on every return path. */
static inline int
log_write_reserved(log_write_token *token,
                   key              tuple_key,
                   message          data,
                   uint64           memtable_generation,
                   uint64           leaf_generation)
{
   platform_assert(token != NULL);
   platform_assert(token->log != NULL,
                   "log write token is absent or has already been consumed");
   return token->log->ops->write_reserved(
      token, tuple_key, data, memtable_generation, leaf_generation);
}

/* Convenience for callers whose reservation and append are adjacent. */
static inline int
log_write(log_handle *log,
          key         tuple_key,
          message     data,
          uint64      memtable_generation,
          uint64      leaf_generation)
{
   log_write_token token;
   log_write_reserve(log, &token);
   return log_write_reserved(
      &token, tuple_key, data, memtable_generation, leaf_generation);
}

/*
 * Take a quick cut of everything written so far and return a ticket for it.
 * Writers may run concurrently with this operation.  A successful begin must
 * be paired with exactly one log_make_durable_wait(), even when ticket_out is
 * zero or a later operation makes the cut durable first.
 *
 * The ticket pins the handle until wait consumes it. This permits a caller to
 * release whatever external lock protects the live-log pointer before doing
 * the slow wait. After separately excluding every new operation as required by
 * log_deinit_fn, the owner may also deinit the stream before this wait.
 */
static inline platform_status
log_make_durable_begin(log_handle *log, log_durable_ticket *ticket_out)
{
   return log->ops->make_durable_begin(log, ticket_out);
}

/* Wait for the cut and consume the handle pin acquired by begin. */
static inline platform_status
log_make_durable_wait(log_handle *log, log_durable_ticket ticket)
{
   return log->ops->make_durable_wait(log, ticket);
}

/* Convenience wrapper for callers that do not need to release a lock early. */
static inline platform_status
log_make_durable(log_handle *log)
{
   log_durable_ticket ticket;
   platform_status    rc = log_make_durable_begin(log, &ticket);
   if (!SUCCESS(rc)) {
      return rc;
   }
   return log_make_durable_wait(log, ticket);
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

/*
 * Release the quiesced owner; only already-issued ticket waits remain legal
 * afterward. See log_deinit_fn for the required exclusion.
 */
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
