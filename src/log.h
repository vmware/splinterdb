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
 * Log writes have three phases.
 *
 * prepare() performs every fallible or potentially slow operation: validating
 * and normalizing the record, constructing any log-owned blob, enrolling its
 * writebacks, and making room in per-thread staging.  prepared_message()
 * returns the normalized message so another data structure can share the
 * immutable blob extents rather than blobifying the input again.  A caller
 * which does not proceed to reserve() must abort() the prepared write.
 * The prepared object and the key and message storage passed to prepare() must
 * remain valid until finish() or abort() consumes the preparation.
 *
 * reserve() runs at the update's logical linearization point.  It assigns the
 * prepared write to a durability group and records an unused offset in that
 * group's thread-private staging buffer.  It performs no allocation or I/O.
 * Once reserve() succeeds it cannot be cancelled: the caller must perform its
 * guaranteed update and call finish().
 *
 * finish() copies and publishes the already-prepared record after the caller's
 * update, when its generation numbers are known.  It is infallible.  This
 * means an update is never made visible and then reported as failed merely
 * because its log append ran out of memory, space, or I/O resources.
 */
#define LOG_WRITE_PREPARED_INTERNAL_WORDS 32

typedef struct log_write_prepared {
   log_handle *log;
   /*
    * Concrete logs keep preparation state here.  Inline storage avoids a heap
    * allocation on every write while preserving the abstract log interface.
    */
   union {
      uint64      words[LOG_WRITE_PREPARED_INTERNAL_WORDS];
      long double alignment;
      void       *pointer_alignment;
   } internal;
} log_write_prepared;

typedef struct log_write_token {
   log_handle         *log;
   void               *internal;
   log_write_prepared *prepared;
   /*
    * Reservations are thread-affine and may not be nested.  The concrete log
    * records both the originating thread and its reservation ticket here so
    * finish() can validate the receipt before consuming it.
    */
   threadid owner_tid;
   uint64   internal_ticket;
   uint64   internal_offset;
} log_write_token;

typedef platform_status (*log_write_prepare_fn)(log_handle         *log,
                                                key                 tuple_key,
                                                message             data,
                                                bool32              force_blob,
                                                log_write_prepared *prepared);
typedef message (*log_write_prepared_message_fn)(
   const log_write_prepared *prepared);
typedef void (*log_write_abort_fn)(log_write_prepared *prepared);
typedef platform_status (*log_write_reserve_fn)(log_write_prepared *prepared,
                                                log_write_token    *token);
typedef void (*log_write_finish_fn)(log_write_token *token,
                                    uint64           memtable_generation,
                                    uint64           leaf_generation);

/*
 * make_durable_{begin,wait}() are used to ensure that all log writes whose
 * _reservation_ _completed_ before the _beginning_ of make_durable_begin() will
 * be durable before the _end_ of make_durable_wait().
 *
 * Opaque durability cut returned by log_make_durable_begin().  A successful
 * begin holds one reference on the in-memory log handle until the matching
 * log_make_durable_wait() or log_make_durable_release(). Once the owner has
 * otherwise quiesced and retired the stream, that pin lets it deinit the stream
 * between the two calls.
 *
 * The log_durable_ticket identifies the set of writes covered by the
 * make_durable_begin request. (i.e. all writes whose reservation completed
 * before the beginning of make_durable_begin())
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
typedef void (*log_make_durable_release_fn)(log_handle        *log,
                                            log_durable_ticket ticket);

/*
 * Finish the log: ensure that everything in the log (including reserved writes
 * whose finish() has not yet run) is durably written to disk and mark the last
 * of it as the end of the log so that replay can tell a complete log from one
 * that a crash truncated.  The log is immutable afterward, but the handle
 * remains valid and must still be released with log_deinit().
 *
 * The caller must exclude log_write_prepare(), log_write_reserve(), and other
 * log_seal() calls, and must ensure that no unreserved prepared write remains.
 * Tokens returned by earlier reservations may remain outstanding and are
 * included in the sealed stream, but the sealing thread must not itself own
 * one because seal waits for all such reservations to complete.
 *
 * A caller that is about to discard the log outright can skip this and
 * call log_deinit() alone.
 *
 * On a transient failure nothing new is guaranteed durable, but seal may
 * simply be called again.
 *
 * Calling seal again after success is idempotent.
 */
typedef platform_status (*log_seal_fn)(log_handle *log);

/*
 * Release the stream owner's reference. Before calling this, the owner must
 * exclude concurrent preparations, reservations, log_make_durable_begin(),
 * and log_seal() calls, including waiting for any already executing calls to
 * return, and no prepared write may remain. It need not wait for
 * log_make_durable_wait() calls consuming tickets issued before deinit. The
 * handle is otherwise invalid as soon as deinit is called. Deinit writes
 * nothing, so it cannot fail.
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
   log_write_prepare_fn          write_prepare;
   log_write_prepared_message_fn write_prepared_message;
   log_write_abort_fn            write_abort;
   log_write_reserve_fn          write_reserve;
   log_write_finish_fn           write_finish;
   log_make_durable_begin_fn     make_durable_begin;
   log_make_durable_wait_fn      make_durable_wait;
   log_make_durable_release_fn   make_durable_release;
   log_seal_fn                   seal;
   log_deinit_fn                 deinit;
   log_head_fn                   head;
   log_is_empty_fn               is_empty;
   log_size_fn                   size;
   bool32 (*group_size_reached)(log_handle *log);
} log_ops;

// to sub-class log, make a log_handle your first field
struct log_handle {
   const log_ops *ops;
};

static inline platform_status
log_write_prepare(log_handle         *log,
                  key                 tuple_key,
                  message             data,
                  bool32              force_blob,
                  log_write_prepared *prepared)
{
   platform_assert(log != NULL);
   platform_assert(prepared != NULL);
   ZERO_CONTENTS(prepared);
   return log->ops->write_prepare(log, tuple_key, data, force_blob, prepared);
}

static inline message
log_write_prepared_message(const log_write_prepared *prepared)
{
   platform_assert(prepared != NULL);
   platform_assert(prepared->log != NULL);
   return prepared->log->ops->write_prepared_message(prepared);
}

static inline void
log_write_abort(log_write_prepared *prepared)
{
   platform_assert(prepared != NULL);
   platform_assert(prepared->log != NULL);
   prepared->log->ops->write_abort(prepared);
   platform_assert(prepared->log == NULL);
}

static inline platform_status
log_write_reserve(log_write_prepared *prepared, log_write_token *token)
{
   platform_assert(prepared != NULL);
   platform_assert(prepared->log != NULL);
   platform_assert(token != NULL);
   ZERO_CONTENTS(token);
   return prepared->log->ops->write_reserve(prepared, token);
}

/* Publish a reserved record and consume both the token and preparation. */
static inline void
log_write_finish(log_write_token *token,
                 uint64           memtable_generation,
                 uint64           leaf_generation)
{
   platform_assert(token != NULL);
   platform_assert(token->log != NULL,
                   "log write token is absent or has already been consumed");
   token->log->ops->write_finish(token, memtable_generation, leaf_generation);
   platform_assert(token->log == NULL);
}

/* Best-effort policy hint. The caller protects the log lifetime; no record
 * reservation is needed. Raw log_write() only appends: the database schedules
 * automatic durability after releasing its insert epoch. */
static inline bool32
log_group_size_reached(log_handle *log)
{
   return log->ops->group_size_reached(log);
}

/* Convenience for callers whose three phases are adjacent. */
static inline int
log_write(log_handle *log,
          key         tuple_key,
          message     data,
          uint64      memtable_generation,
          uint64      leaf_generation)
{
   log_write_prepared prepared;
   platform_status    rc =
      log_write_prepare(log, tuple_key, data, FALSE, &prepared);
   if (!SUCCESS(rc)) {
      return rc.r;
   }
   log_write_token token;
   rc = log_write_reserve(&prepared, &token);
   if (!SUCCESS(rc)) {
      log_write_abort(&prepared);
      return rc.r;
   }
   log_write_finish(&token, memtable_generation, leaf_generation);
   return STATUS_OK.r;
}

/*
 * Take a quick cut of everything reserved for write so far and return a ticket
 * for it. Writers may run concurrently with this operation.  A successful begin
 * must be paired with exactly one log_make_durable_wait() or
 * log_make_durable_release(), even when ticket_out
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

/* Abandon a ticket without making it durable. This only releases its handle
 * pin: the group cut remains and no durability guarantee is made. Keep the
 * log's owner reference alive when a strict no-I/O release is required;
 * releasing the last pin on a retired stream may reclaim allocation metadata.
 */
static inline void
log_make_durable_release(log_handle *log, log_durable_ticket ticket)
{
   log->ops->make_durable_release(log, ticket);
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
