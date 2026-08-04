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
   char  *buf;         // page-sized image under construction
   uint64 offset;      // append cursor within buf
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
   uint64 group_id;
   uint64 group_page_count;
   /*
    * Receipts for every page handed over since the group opened, so that
    * closing it can wait for exactly those writes rather than flushing the
    * whole cache.  They have to be collected as pages graduate, not at close
    * time: a page issued early is long gone by then and there would be no way
    * left to tell whether it landed.
    *
    * Guarded by wbset_lock, since graduation is concurrent.  Its size is
    * therefore bounded by the group -- which today means by the log-cut policy,
    * at 24 bytes per page.
    */
   platform_mutex wbset_lock;
   writeback_set  wbset;
   uint64         addr;
   uint64         meta_head;
   uint64         magic;
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
   uint64      magic;
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
    * Non-zero on exactly one page per group -- the last one written -- giving
    * the number of pages the group contains, with SHARD_LOG_END_OF_STREAM set
    * if that group is also the last of a sealed stream.  Zero on every other
    * page.
    *
    * The count cannot be stamped when a page is written, because a group's
    * pages are written as they fill, long before it closes.  Marking only the
    * final page sidesteps that and costs nothing: it rides along on a page that
    * had to be written anyway.
    *
    * A terminator always counts at least itself, so the field stays a reliable
    * "is this a terminator" test even with the flag bit set.
    */
   uint32 pages_in_group;
   uint16 num_entries;
} shard_log_hdr;

/*
 * Create a fresh sharded write-ahead log stream.  Returns an abstract
 * log_handle (or NULL on failure) to be driven through the log.h interface and
 * released with log_deinit().
 */
log_handle *
shard_log_create(cache *cc, shard_log_config *cfg, platform_heap_id hid);

/*
 * Create an iterator over the sharded log identified by `head`, reading its
 * records in generation order.  Returns an abstract log_iterator (or NULL on
 * failure) to be driven through the log.h interface and freed with
 * log_iterator_deinit().
 */
log_iterator *
shard_log_iterator_create(cache            *cc,
                          shard_log_config *cfg,
                          platform_heap_id  hid,
                          log_head          head);

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
 * reconstruct the allocator references.
 *
 * Record one allocator reference for every extent belonging to the stream.
 * Call between allocator_recovery_begin() and allocator_recovery_finish().  A
 * zero head.addr (an absent log slot) is not an error and records nothing.
 *
 * The metadata head and stream extents are recorded first.  The stream's
 * replayable records are then walked to recover the separate storage of every
 * blob they name.  Blob recovery has to happen before replay because replay
 * allocates disk space; otherwise it could reuse an extent belonging to a blob
 * whose record it has not reached yet.  A blob reachable only from an
 * incomplete group is deliberately left unmarked because that group will not
 * be replayed.
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
