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
 * In order to support high concurrency while ensuring that the log makes
 * updates durable in their linearization order, log writes are performed in two
 * steps.  First, at the linearization point of an update, the caller uses
 * log_write_reserve() to reserve a spot in the log. The log_write_token is
 * the reservation receipt.  Then, they use log_write_reserved() to actually
 * write the log entry. We separate the process into two steps because, in order
 * to ensure correct linearization ordering of log durability, callers may need
 * to reserve their slot in the log while holding locks on other data structures
 * that they are updating (e.g. the btree leaf of the memtable).  The actual
 * write, which may require performing I/O, memory allocation, etc, can occur
 * later, outside of any critical section.
 *
 * There is no way to cancel a reservation, so make the reservation only once
 * you know that you want to perform the write.
 *
 * If the write fails, then the log will not satisfy subsequent make_durable
 * calls.
 */
typedef struct log_write_token {
   log_handle *log;
   void       *internal;
   /*
    * Reservations are thread-affine and may not be nested.  The concrete log
    * records both the originating thread and its reservation ticket here so
    * write_reserved() can validate the receipt before consuming it.
    */
   threadid owner_tid;
   uint64   internal_ticket;
} log_write_token;

typedef void (*log_write_reserve_fn)(log_handle *log, log_write_token *token);

/* Append through, and always consume, a prior reservation. */
typedef int (*log_write_reserved_fn)(log_write_token *token,
                                     key              tuple_key,
                                     message          data,
                                     uint64           memtable_generation,
                                     uint64           leaf_generation);

/*
 * make_durable_{begin,wait}() are used to ensure that all log writes whose
 * _reservation_ _completed_ before the _beginning_ of make_durable_begin() will
 * be durable before the _end_ of make_durable_wait().
 *
 * Opaque durability cut returned by log_make_durable_begin().  A successful
 * begin holds one reference on the in-memory log handle until the matching
 * log_make_durable_wait(). Once the owner has otherwise quiesced and retired
 * the stream, that pin lets it deinit the stream between the two calls.
 *
 * The log_durable_ticket identifies the set of writes covered by the
 * make_durable_begin request. (i.e. all writes whose reservation completed
 * before the beginning of make_durable_begin())
 *
 * If a covered write fails, then make_durable_wait will return an error -- the
 * log can never ensure that all covered writes have been made durable.
 *
 * A thread may not call make_durable_begin() while it owns a write reservation
 * on this log. Writers, other make_durable_begin() calls, and log_seal() may
 * run concurrently, subject to log_seal()'s exclusion of new reservations.
 */
typedef uint64 log_durable_ticket;

typedef platform_status (
   *log_make_durable_begin_fn)(log_handle *log, log_durable_ticket *ticket_out);
typedef platform_status (*log_make_durable_wait_fn)(log_handle        *log,
                                                    log_durable_ticket ticket);

/*
 * Finish the log: ensure that everything in the log (including
 * reserved-but-not-yet-written items) is durably written to disk and mark the
 * last of it as the end of the log so that replay can tell a complete log from
 * one that a crash truncated.  The log is immutable afterward, but the handle
 * remains valid and must still be released with log_deinit().
 *
 * The caller must exclude concurrent execution of log_write_reserve() and
 * log_seal(). Tokens returned by earlier reservations may remain outstanding
 * and are included in the sealed stream, but the sealing thread must not itself
 * own one because seal waits for all such reservations to complete.
 *
 * A caller that is about to discard the log outright can skip this and
 * call log_deinit() alone.
 *
 * On a transient failure nothing new is guaranteed durable, but seal may
 * simply be called again. Note, however, that an earlier failure in a log_write
 * means that the log is corrupted (from the point of that write onward) and
 * hence can never sealed.
 *
 * Calling seal again after success is idempotent.
 */
typedef platform_status (*log_seal_fn)(log_handle *log);

/*
 * Release the stream owner's reference. Before calling this, the owner must
 * exclude concurrent reservations, log_make_durable_begin(), and log_seal()
 * calls, including waiting for any already executing calls to return. It need
 * not wait for log_make_durable_wait() calls consuming tickets issued before
 * deinit. The handle is otherwise invalid as soon as deinit is called. Deinit
 * writes nothing, so it cannot fail.
 */
typedef void (*log_deinit_fn)(log_handle *log);

/*
 * The log's head, fixed at creation.  The caller records it (e.g. in the
 * superblock) so that crash recovery can find the log for replay.
 */
typedef log_head (*log_head_fn)(log_handle *log);

/*
 * Whether the log has ever accepted a record.
 */
typedef bool32 (*log_is_empty_fn)(log_handle *log);

/*
 * Rough approximation of the log's current on-disk size.
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
 * Take a quick cut of everything reserved for write so far and return a ticket
 * for it. Writers may run concurrently with this operation.  A successful begin
 * must be paired with exactly one log_make_durable_wait(), even when ticket_out
 * is zero or a later operation makes the cut durable first.
 *
 * The ticket prevents the handle from being freed until wait consumes it. This
 * permits a caller to release whatever external lock protects the live-log
 * pointer before doing the slow wait. After separately excluding every new
 * operation as required by log_deinit_fn, the owner may also deinit the stream
 * before this wait.
 */
static inline platform_status
log_make_durable_begin(log_handle *log, log_durable_ticket *ticket_out)
{
   platform_assert(log != NULL);
   platform_assert(ticket_out != NULL);
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
 * Finish the log, durably.  See log_seal_fn for the required exclusion.  The
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
 * A log_iterator reads a log's records in generation order (used by
 * crash recovery to replay a log onto the durable root).  It is a generic
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
 * sealed log, as opposed to stopping early because the log was truncated
 * by a crash.
 *
 * Recovery needs this to decide whether it may go on to the next log.  The
 * records of a truncated log, log_A, are still a valid prefix on their own, but
 * anything written to a subsequent log, log_B, must not be replayed on top of
 * them: doing so would skip whatever was lost at the end of log_A.
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
