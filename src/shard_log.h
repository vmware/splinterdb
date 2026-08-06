// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * shard_log.h --
 *
 *     This file contains the interface for a sharded write-ahead log.
 */

#pragma once

#include "log.h"
#include "platform_threads.h"
#include "platform_hash.h"
#include "cache.h"
#include "iterator.h"
#include "splinterdb/data.h"
#include "blob_build.h"
#include "mini_allocator.h"
#include "platform_mutex.h"
#include "writeback_set.h"

/*
 * Configuration structure to set up the sharded log sub-system.
 */
typedef struct shard_log_config {
   cache_config     *cache_cfg;
   data_config      *data_cfg;
   uint64            seed;
   blob_build_config blob_cfg;
} shard_log_config;

typedef enum shard_log_close_mode {
   SHARD_LOG_CLOSE_NONE,   // an ordinary page; the group stays open
   SHARD_LOG_CLOSE_GROUP,  // last page of its group
   SHARD_LOG_CLOSE_STREAM, // last page of its group and of the stream
} shard_log_close_mode;

typedef enum shard_log_buffer_state {
   SHARD_LOG_BUFFER_OPEN,
   SHARD_LOG_BUFFER_INCACHE,
} shard_log_buffer_state;

typedef enum shard_log_group_state {
   SHARD_LOG_GROUP_OPEN,
   SHARD_LOG_GROUP_CLOSING,
   SHARD_LOG_GROUP_TERMINATING,
   SHARD_LOG_GROUP_DURABILITY_PENDING,
   SHARD_LOG_GROUP_DURABLE,
} shard_log_group_state;

/*
 * Per-thread staging for log appends.
 *
 * A thread assembles a whole page image in `buf` and only then copies it into a
 * freshly allocated log page ("graduating" it). Appending is therefore a bare
 * memcpy into thread-private memory, rather than a cache_get / try_claim /
 * lock / unlock / unclaim / unget round trip per record against a shared page.
 *
 * It also means a log page is written exactly once, when it is complete: there
 * is no partially-filled page on disk to be rewritten later.
 */
typedef struct shard_log_thread_data {
   char                  *buf;    // page-sized image under construction
   uint64                 offset; // append cursor within buf
   shard_log_buffer_state state;
   /* Held from cache_alloc() until writeback-set enrollment succeeds. */
   page_handle *incache_page;
} PLATFORM_CACHELINE_ALIGNED shard_log_thread_data;

/*
 * A reservation or durability cut writes only its calling thread's slot.
 * Cache-line separation keeps unrelated operations from bouncing the same
 * line merely to publish their group-selection hazards.
 */
typedef struct shard_log_reservation_slot {
   uint64 ticket; // group id + 1, or zero when this thread has no reservation
} PLATFORM_CACHELINE_ALIGNED shard_log_reservation_slot;

_Static_assert(sizeof(shard_log_reservation_slot) == PLATFORM_CACHELINE_SIZE,
               "reservation slot must occupy exactly one cache line");

typedef struct shard_log_group shard_log_group;

/* Read together by every operation, separate from the contended cut claim. */
typedef struct shard_log_accepting_frontier {
   shard_log_group *group;
   uint64           id;
   /* Atomic, set once after the first record reaches a staging buffer. */
   bool32 has_records;
} PLATFORM_CACHELINE_ALIGNED shard_log_accepting_frontier;

_Static_assert(sizeof(shard_log_accepting_frontier) == PLATFORM_CACHELINE_SIZE,
               "accepting frontier must occupy exactly one cache line");

typedef struct shard_log_install_claim {
   uint64 state;
} PLATFORM_CACHELINE_ALIGNED shard_log_install_claim;

_Static_assert(sizeof(shard_log_install_claim) == PLATFORM_CACHELINE_SIZE,
               "installation claim must occupy exactly one cache line");

/* The high bit turns the versioned installation claim into a terminal claim. */
#define SHARD_LOG_INSTALL_SEALING_BIT (1ULL << 63)
#define SHARD_LOG_INSTALL_ID_MASK     (SHARD_LOG_INSTALL_SEALING_BIT - 1)

/*
 * One independently staged and written-back durability group.  A cut closes
 * this object and immediately installs another one for new writers; the slow
 * graduation, writeback wait, and device barrier happen afterward.
 */
struct shard_log_group {
   uint64                id; // on-disk id; tickets use id + 1
   shard_log_group_state state;
   shard_log_close_mode  close;
   uint64 page_count; // Number of disk pages allocated by this group
   /* Atomic, set once by the first reservation in this incarnation. */
   bool32 ever_used;
   bool32 emergency; // Is this group from the pool of emergency groups?
   /*
    * First failed append after this group was selected. A non-success value
    * permanently poisons the group: it must never receive a commit terminator.
    * The first error is installed atomically by a completing reservation.
    */
   platform_status append_error;

   shard_log_thread_data *thread_data;
   char                  *thread_buffers;

   platform_mutex wbset_lock;
   writeback_set  wbset;

   shard_log_group *next;
   shard_log_group *pool_next;
};

#define SHARD_LOG_NUM_EMERGENCY_GROUPS 2

/*
 * Sharded log context structure.
 */
typedef struct shard_log {
   log_handle        super; // handle to log I/O ops abstraction.
   cache            *cc;
   shard_log_config *cfg;
   platform_heap_id  heap_id;
   mini_allocator    mini;

   /* Immutable state, once the log is inited. */
   uint64    addr;
   uint64    meta_head;
   log_nonce nonce;

   /*
    * group_lock protects the group list, durability frontiers, stream state,
    * emergency pool, and ticket_refs. It is never held while allocating,
    * waiting for cache I/O, or issuing a durable barrier. Reservations use the
    * cache-line-private slots below and do not acquire it.
    */
   platform_mutex   group_lock;
   shard_log_group *groups_head;
   /*
    * Atomically published current group and its ticket. The pointer is
    * published before its ticket. accepting.ticket names the published group
    * as id + 1. install.state is the accepting group's id at rest and one
    * greater while its successor is being installed. Once sealing wins the
    * same claim, its high bit remains set and its low bits name the final
    * group.
    */
   shard_log_accepting_frontier accepting;
   shard_log_install_claim      install;
   shard_log_reservation_slot   reservation_slots[MAX_THREADS];

   shard_log_group *emergency_pool;

   uint64 last_cut_ticket;
   uint64 graduated_ticket;
   uint64 durable_ticket;
   uint64 seal_ticket;

   uint64 ticket_refs;
   bool32 sealing;
   bool32 sealed;
   bool32 deinit_requested;
   bool32 destroying;
   /*
    * Graduation is serialized separately from durability.  This preserves
    * physical group order while allowing later groups to stage records and
    * issue their writebacks while an earlier group waits at the device.
    */
   platform_mutex graduate_lock;
   platform_mutex durability_lock;

   /*
    * Extents the mini-allocator held once the stream was initialized -- its
    * fixed per-stream overhead (a metadata extent plus one per batch).
    * shard_log_get_size() subtracts it so a fresh stream reports zero bytes
    * appended.
    */
   uint64 initial_extents;
} shard_log;

typedef struct log_entry log_entry;

/*
 * Flag bit stolen from the top of shard_log_hdr::pages_in_group, marking the
 * final group of a sealed stream.
 *
 * Without it, a stream that lost a whole trailing group is indistinguishable
 * from one that simply ended: every group present is intact and contiguous, so
 * nothing on disk says more was supposed to follow.  Replaying up to that point
 * and then moving on to the next log would skip the missing records.
 */
#define SHARD_LOG_END_OF_STREAM       (1u << 31)
#define SHARD_LOG_PAGES_IN_GROUP_MASK (SHARD_LOG_END_OF_STREAM - 1)

typedef struct shard_log_iterator {
   log_iterator      super; // IS-A log_iterator IS-A generic iterator
   platform_heap_id  heap_id;
   cache            *cc;
   shard_log_config *cfg;
   char             *contents;
   log_entry       **entries;
   uint64            num_entries;
   uint64            pos;
   // Whether the replayable records run all the way to an end-of-stream marker.
   bool32 stream_complete;
} shard_log_iterator;

/*
 * ---------------------------------------------------------------
 * Sharded log page header stucture: Disk-resident structure.
 * Page Type == PAGE_TYPE_LOG
 * ---------------------------------------------------------------
 */
typedef struct ONDISK shard_log_hdr {
   checksum128 checksum;
   log_nonce   nonce;
   uint64      next_extent_addr;
   /*
    * The group this page belongs to.  A group is the unit of replay: either all
    * of its pages are present and it is replayed, or it is discarded whole.
    * That is what lets recovery reconstruct a prefix of the writes rather than
    * an arbitrary subset, which contiguity of the generation tags cannot
    * establish (splits advance generations without emitting a record).
    */
   uint64 group_id;
   /*
    * Non-zero only on the group's final page, giving its total page count, with
    * SHARD_LOG_END_OF_STREAM set when this is also the end of a sealed stream.
    * Zero on every earlier page. The final page normally carries packed data;
    * an empty group uses an empty final page.
    *
    * The count cannot be stamped on earlier pages because the final group size
    * is not known yet. Close therefore reserves one mutable thread buffer for
    * the final page and graduates it only after every ordinary page has a
    * permanent address. A failed close retries frozen images without modifying
    * or duplicating them.
    */
   uint32 pages_in_group;
   uint16 num_entries;
} shard_log_hdr;

/*
 * Create a fresh sharded write-ahead log stream.  On success, returns an
 * abstract log_handle through `log_out` to be driven through the log.h
 * interface and released with log_deinit().
 */
platform_status
shard_log_create(cache            *cc,
                 shard_log_config *cfg,
                 platform_heap_id  hid,
                 log_handle      **log_out);

/*
 * Create an iterator over the sharded log identified by `head`, reading its
 * records in generation order. Blob checksums are required for records at or
 * above `first_needed_generation`; older records are already represented by
 * the checkpoint root and their value pages need not survive replay. Returns
 * an abstract log_iterator through `itor_out` to be driven through the log.h
 * interface and freed with log_iterator_deinit().
 */
platform_status
shard_log_iterator_create(cache            *cc,
                          shard_log_config *cfg,
                          platform_heap_id  hid,
                          log_head          head,
                          uint64            first_needed_generation,
                          log_iterator    **itor_out);

/*
 * Release a stream identified by its log_head: drop the owner's reference from
 * its metadata head. This normally frees the stream's on-disk extents. A
 * split-phase durability ticket may keep them alive until its matching wait,
 * so the release is not required to be the final reference. Takes no handle --
 * the owner has called log_deinit() and retained only the head captured at
 * creation.
 *
 * Do not use this for a stream left behind by a crash: its mini-allocator
 * metadata was not made durable.  Crash recovery rebuilds the allocator map
 * through shard_log_recover_allocations() instead.
 */
void
shard_log_dec_ref(cache *cc, const log_head *head);

/*
 * ---- Recovering a stream's extents ----
 *
 * A shard log's mini-allocator metadata is deliberately never made durable --
 * keeping it safe to read after a crash would cost a write per allocation, and
 * nothing in normal operation needs it.  Crash recovery therefore walks the
 * stream itself, following the next-extent links in its page headers, to
 * reconstruct the allocator references.  It queries which individual backing
 * pages are readable before issuing cache reads, so a partly written final
 * extent and backends with holes need no process-wide relaxed-read mode.
 *
 * Record one allocator reference for every extent the durable stream can still
 * reach.  Call between allocator_recovery_begin() and
 * allocator_recovery_finish().  A zero head.addr (an absent log slot) is not an
 * error and records nothing.
 * The initial extent and a wholly unreadable linked successor are still
 * recorded: both remain reachable from the durable log identity during replay,
 * so neither may be reused until the root-only rebuild drops the log.
 *
 * The metadata head and stream extents are recorded first.  Every individually
 * valid log page is then scanned to recover the separate storage of each blob
 * it names, including pages in a trailing incomplete group.  Replay validates
 * those blobs before it can decide that the group is incomplete, and cache
 * reads require their extents not to have been reused meanwhile.  Blob recovery
 * therefore has to precede any replay allocation; the conservative suffix-only
 * references disappear in the root-only rebuild below.
 *
 * The recovered references need no matching release pass.  After replay is
 * folded into the tree, recovery publishes a root naming no logs and rebuilds
 * the allocator map again from that root.  The old streams are freed by being
 * absent from the second map.
 */
platform_status
shard_log_recover_allocations(cache            *cc,
                              shard_log_config *cfg,
                              platform_heap_id  hid,
                              log_head          head);

void
shard_log_config_init(shard_log_config *log_cfg,
                      cache_config     *cache_cfg,
                      data_config      *data_cfg);
void
shard_log_print(shard_log *log);
