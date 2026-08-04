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
   // data config of point message tree
} shard_log_config;

typedef enum shard_log_close {
   SHARD_LOG_CLOSE_NONE,   // an ordinary page; the group stays open
   SHARD_LOG_CLOSE_GROUP,  // last page of its group
   SHARD_LOG_CLOSE_STREAM, // last page of its group and of the stream
} shard_log_close;

typedef enum shard_log_buffer_state {
   SHARD_LOG_BUFFER_OPEN,
   SHARD_LOG_BUFFER_TERMINATED,
   SHARD_LOG_BUFFER_INCACHE,
} shard_log_buffer_state;

typedef enum shard_log_group_state {
   SHARD_LOG_GROUP_OPEN,
   SHARD_LOG_GROUP_CLOSING,
   SHARD_LOG_GROUP_TERMINATING,
   SHARD_LOG_GROUP_DURABILITY_PENDING,
   SHARD_LOG_GROUP_SEALED,
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
   shard_log_close        close;
   /* Held from cache_alloc() until writeback-set enrollment succeeds. */
   page_handle *incache_page;
   bool32 has_records; // sticky for this thread over the stream's lifetime
} PLATFORM_CACHELINE_ALIGNED shard_log_thread_data;

/*
 * Sharded log context structure.
 */
typedef struct shard_log {
   log_handle            super; // handle to log I/O ops abstraction.
   cache                *cc;
   shard_log_config     *cfg;
   platform_heap_id      heap_id;
   shard_log_thread_data thread_data[MAX_THREADS];
   mini_allocator        mini;
   // Backing block for thread_data[*].buf, one page per thread.
   char *thread_buffers;
   /*
    * The group currently accepting pages, and how many it holds so far.  Groups
    * never span log streams, so numbering is per-stream and starts at 0: the
    * exclusive insert lock held across a log cut guarantees every record
    * destined for this stream is already staged by the time it is sealed.
    */
   uint64                group_id;
   uint64                group_page_count;
   shard_log_group_state group_state;
   shard_log_close       group_close;
   /*
    * Normal writers are sharded and never take this lock.  It serializes the
    * rare recovery path after a failed group close with later close attempts:
    * no new group becomes OPEN until the previous one's writeback and durable
    * barrier have both succeeded.
    */
   platform_mutex close_lock;
   /*
    * Receipts for every page handed over since the group opened, so that
    * closing it can wait for exactly those writes rather than flushing the
    * whole cache.  They have to be collected as pages graduate, not at close
    * time: a page issued early is long gone by then and there would be no way
    * left to tell whether it landed.
    *
    * Guarded by wbset_lock, since graduation is concurrent.  It includes log
    * pages as well as blob pages/extents and is bounded by the group, which in
    * turn is bounded by the log-cut policy.
    */
   platform_mutex wbset_lock;
   writeback_set  wbset;
   uint64         addr;
   uint64         meta_head;
   log_nonce      nonce;
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
    * Non-zero only on the group's dedicated final terminator page, giving the
    * total number of data-plus-terminator pages, with
    * SHARD_LOG_END_OF_STREAM set when this is also the end of a sealed stream.
    * Zero on every data page.
    *
    * The count cannot be stamped on data pages as they fill because the final
    * group size is not known yet.  A separate empty terminator is a small space
    * cost in exchange for a simple retry rule: no failed close ever has to
    * modify or duplicate a frozen data page.
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
 * Release a stream identified by its log_head: drop the reference its metadata
 * head holds, freeing the stream's on-disk extents.  Takes no handle -- the
 * handle was freed by log_deinit(); the caller retained only the head
 * (log_get_head(), captured at creation).
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
