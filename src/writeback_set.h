// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * writeback_set.h --
 *
 *     A set of pages and extents to be written back together and then made
 *     durable as a unit.
 *
 *     The cache can issue a writeback and later tell you whether it completed
 *     (cache_writeback_page(), cache_writeback_get_status()), but it deals in
 *     one page at a time. Making a group of pages durable means issuing all of
 *     their writes, waiting for all of them, and then taking one barrier -- in
 *     that order, because the barrier is what costs, and it is O(1) however
 *     many pages precede it. This module holds the receipts in between.
 *
 *     The two phases are deliberately separate calls. Issuing every write
 *     before waiting for any is what lets the I/Os pipeline; an interface that
 *     let a caller interleave them would quietly serialize the group at device
 *     latency per page.
 *
 *     Completion and durability are likewise separate: writeback_set_wait()
 *     establishes that the writes reached the device, and only
 *     writeback_set_make_durable() makes them survive power loss. Splitting
 *     them keeps the durability boundary visible at the call site rather than
 *     buried inside a function that mostly does something else.
 *
 *     This module knows nothing about any particular cache implementation; it
 *     is written entirely against cache.h.
 */

#pragma once

#include "cache.h"
#include "vector.h"

typedef struct writeback_set_entry {
   cache_writeback_request request;
   page_type               type;
   /* The request could not cover every dirty page and must be reissued. */
   bool32 needs_retry;
} writeback_set_entry;

typedef VECTOR(writeback_set_entry) writeback_set_entry_vector;

typedef struct writeback_set {
   cache                     *cc;
   writeback_set_entry_vector entries;
} writeback_set;

/* Prepare an empty set. Allocates nothing until the first add. */
void
writeback_set_init(writeback_set *set, cache *cc, platform_heap_id hid);

/* Release the set's memory. Does not wait for any outstanding writes: a caller
 * that abandons a set without waiting leaves its writes in flight, which is
 * safe but means it learns nothing about whether they landed. */
void
writeback_set_deinit(writeback_set *set);

/*
 * Issue writeback of a page (or of every page of an extent) and add it to the
 * set. Non-blocking.
 *
 * Once the vector reservation succeeds, the member is enrolled even if it is
 * temporarily locked or claimed.  Such an entry records that it needs a retry,
 * allowing the caller to finish assembling the set without losing either a
 * partially issued extent request or the identity of a page still needing a
 * write.  Consequently these calls fail only if the member could not be
 * enrolled at all (normally allocation failure).
 */
platform_status
writeback_set_add_page(writeback_set *set, page_handle *page, page_type type);

platform_status
writeback_set_add_extent(writeback_set *set, uint64 addr, page_type type);

/*
 * Wait until every member's write has completed. Polls rather than blocking on
 * a condition: the poll reaps I/O completions on the calling thread, so it is
 * doing the work rather than merely waiting for someone else to.
 *
 * This is completion, NOT durability -- follow with
 * writeback_set_make_durable().
 *
 * Returns STATUS_IO_ERROR if a write failed, or STATUS_BUSY if a member still
 * needs to be reissued (including a page re-dirtied during its write). It still
 * waits until every issued request has finished, so no I/O belonging to the set
 * remains in flight when it returns.
 */
platform_status
writeback_set_wait(writeback_set *set);

/* Reissue every member not completely covered by its previous request. */
platform_status
writeback_set_retry_incomplete(writeback_set *set);

/*
 * Make the completed writes durable. Must follow a successful
 * writeback_set_wait(): the barrier covers writes that have completed, so
 * calling it with members still in flight would not cover them.
 *
 * Today this is one cache-wide barrier, whose cost is O(1) in the size of the
 * set and is shared with anything else that happens to have completed. The
 * separate entry point leaves room for a narrower mechanism later without
 * changing callers.
 */
platform_status
writeback_set_make_durable(writeback_set *set);

/*
 * Drop every member, keeping the allocation for reuse.  Only sound once the
 * members have been waited for: the requests are the only record that those
 * writes are outstanding, so discarding them earlier loses the ability to tell
 * when they land.
 */
void
writeback_set_reset(writeback_set *set);

/* Number of members added so far. */
uint64
writeback_set_num_requests(const writeback_set *set);
