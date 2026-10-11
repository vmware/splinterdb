// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * clockcache.h --
 *
 *     This file contains interface for a concurrent clock cache.
 */

#pragma once

#include "platform_buffer.h"
#include "platform_threads.h"
#include "allocator.h"
#include "cache.h"
#include "platform_io.h"

// #define ADDR_TRACING
#define TRACE_ADDR  (UINT64_MAX - 1)
#define TRACE_ENTRY (UINT32_MAX - 1)

// #define RECORD_ACQUISITION_STACKS

/* how distributed the rw locks are */
#define CC_RC_WIDTH 4

/*
 * Configuration struct to setup the clock cache sub-system.
 */
typedef struct clockcache_config {
   cache_config super;
   io_config   *io_cfg;
   uint64       capacity;
   bool32       use_stats;
   char         logfile[MAX_STRING_LENGTH];

   // computed
   uint64 log_page_size;
   uint64 extent_mask;
   uint32 page_capacity;
   uint64 batch_capacity;
   uint64 cacheline_capacity;
   uint64 pages_per_extent;
} clockcache_config;

typedef struct clockcache       clockcache;
typedef struct clockcache_entry clockcache_entry;
typedef struct async_io_state   async_io_state;

#ifdef RECORD_ACQUISITION_STACKS

// Provide a small number of backtrace history records.
#   define NUM_HISTORY_RECORDS 32

typedef struct history_record {
   uint32 status;
   int    refcount;
   void  *backtrace[NUM_HISTORY_RECORDS];
} history_record;
#endif // RECORD_ACQUISITION_STACKS


typedef uint32 entry_status; // Saved in clockcache_entry->status

/*
 *-----------------------------------------------------------------------------
 * clockcache_entry --
 *
 *     The meta data entry in the cache. Each entry has the underlying
 *     page_handle together with some flags.
 *-----------------------------------------------------------------------------
 */
struct clockcache_entry {
   page_handle page;
   // Accessed atomically; see clockcache_get_status() and the flag helpers.
   volatile entry_status status;
   /* Published before CC_LOADING is cleared; protected by the read reference.
    */
   platform_status read_status;
   // Generation in which this page's current dirty interval began; 0 when the
   // page is clean or free. A writeback_dirty() drains every entry whose
   // generation is at or below the cut it took (see
   // clockcache_writeback_dirty).
   // Accessed with __atomic builtins and ordered by status transitions.
   volatile uint64  dirty_generation;
   page_type        type;
   async_wait_queue waiters;
#ifdef RECORD_ACQUISITION_STACKS
   int            next_history_record;
   history_record history[NUM_HISTORY_RECORDS];
#endif
};

// Allocators acquire and release this flag for every slot allocation. Keep
// flags for independent batches on separate cache lines.
typedef struct {
   bool32 busy;
} PLATFORM_CACHELINE_ALIGNED clockcache_batch;

_Static_assert(sizeof(clockcache_batch) == PLATFORM_CACHELINE_SIZE,
               "clockcache_batch must occupy one cache line");

/*
 *----------------------------------------------------------------------
 * clockcache -- A multi-threaded cache using a clock algorithm for eviction
 *
 *      Pages are indexed by a direct mapping, cc->lookup, which is an array.
 *      For a given address, cc->lookup[addr / page_size] returns an
 *      entry_number which can be used to access the metadata and data of the
 *      page.
 *
 *      Each page in the cache has an entry cc->entry[entry_number] with:
 *         --status: flags, e.g. free, write locked, flushing, etc.
 *         --page: disk address and pointer to the page data
 *         --type: used for stats
 *
 *      Each page has a distributed ref count, accessed by
 *      clockcache_[get,inc,dec]_ref(cc, entry_number, tid) and stored in
 *      cc->refcount (it is striped to avoid false sharing in certain
 *      workloads)
 *         -- a read lock is obtained by speculatively incrementing the ref
 *         count before checking the write lock bit, so the ref count should
 *         generally be treated as a lower bound.
 *
 *      Each thread remembers a preferred batch in per_thread[tid].free_hand.
 *      This is a hint, not ownership: batches are locked only while allocating,
 *      evicting, or cleaning, so idle threads cannot reserve unused capacity.
 *      When a thread doesn't find a free page in its batch, it obtains a pair
 *      of new batches: one to evict (from cc->evict_hand) and one to clean.
 *      The batch to clean is
 *      cc->cleaner_gap batches ahead of the current evictor head, so that
 *      cleaned pages have time to flush before eviction. Both cleaning and
 *      eviction use cc->batches[].busy to avoid conflicts and contention.
 *----------------------------------------------------------------------
 */
struct clockcache {
   cache              super;
   clockcache_config *cfg;
   allocator         *al;
   io_handle         *io;

   buffer_handle        lookup_bh;
   uint32              *lookup; // Convenience pointer for lookup_bh
   buffer_handle        entry_bh;
   clockcache_entry    *entry;   // Convenience pointer for entry_bh
   buffer_handle        data_bh; // actual memory for pages
   char                *data;    // convenience pointer for bh
   platform_log_handle *logfile;
   platform_heap_id     heap_id;

   // Distributed locks (the write bit is in the status uint32 of the entry)
   buffer_handle    rc_bh;
   volatile uint16 *refcount;
   buffer_handle    pincount_bh;
   volatile uint8  *pincount; // Convenience pointer for pincount_bh

   // Clock hands and related metadata
   volatile uint32   evict_hand;
   volatile uint32   free_hand;
   buffer_handle     batch_bh;
   clockcache_batch *batches; // Convenience pointer for batch_bh
   uint64            cleaner_gap;

   // Monotonic generation counter. A writeback_dirty() atomically increments it
   // to take a "cut", then drains every entry stamped with a generation at or
   // below that cut.
   uint64 dirty_generation;

   /*
    * A reserve of async I/O states, so that a writeback can always be issued
    * even when the heap is exhausted -- which matters because writing pages
    * back is how memory pressure gets relieved in the first place. Drawn from
    * only when a heap allocation fails; see clockcache_io_state_acquire().
    * io_state_pool_free is a bitvector, one bit per slot, set when free.
    */
   async_io_state *io_state_pool;
   volatile uint64 io_state_pool_free;

   volatile struct {
      volatile uint32 free_hand;
      bool32          enable_sync_get;
   } PLATFORM_CACHELINE_ALIGNED per_thread[MAX_THREADS];

   // Stats
   cache_stats stats[MAX_THREADS];
};

_Static_assert(MAX_READ_REFCOUNT
                  < 1ULL << (8 * sizeof(((clockcache *)NULL)->refcount[0])),
               "MAX_READ_REFCOUNT too large");

/*
 *-----------------------------------------------------------------------------
 * Function declarations
 *-----------------------------------------------------------------------------
 */

void
clockcache_config_init(clockcache_config *cache_config,
                       io_config         *io_cfg,
                       uint64             capacity,
                       const char        *cache_logfile,
                       uint64             use_stats);

platform_status
clockcache_init(clockcache        *cc,   // OUT
                clockcache_config *cfg,  // IN
                io_handle         *io,   // IN
                allocator         *al,   // IN
                char              *name, // IN
                platform_heap_id   hid,  // IN
                platform_module_id mid); // IN

void
clockcache_deinit(clockcache *cc); // IN
