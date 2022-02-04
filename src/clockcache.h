// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * clockcache.h --
 *
 *     This file contains interface for a concurrent clock cache.
 */

#ifndef __CLOCKCACHE_H
#define __CLOCKCACHE_H

#include "allocator.h"
#include "cache.h"
#include "io.h"
#include "task.h"

//#define ADDR_TRACING
#define TRACE_ADDR  (UINT64_MAX - 1)
#define TRACE_ENTRY (UINT32_MAX - 1)

// #define RECORD_ACQUISITION_STACKS

/* how distributed the rw locks are */
#define CC_RC_WIDTH 4

/*
 * Configuration struct to setup the clock cache sub-system.
 */
typedef struct clockcache_config {
   uint64 page_size;
   uint64 log_page_size;
   uint64 extent_size;
   uint64 capacity;
   uint32 page_capacity;
   bool   use_stats;
   char   logfile[MAX_STRING_LENGTH];

   // computed
   uint64 batch_capacity;
   uint64 cacheline_capacity;
   uint64 pages_per_extent;
} clockcache_config;

#ifdef RECORD_ACQUISITION_STACKS

// Provide a small number of backtrace history records.
#   define NUM_HISTORY_RECORDS 32

typedef struct history_record {
   uint32 status;
   int    refcount;
   void * backtrace[NUM_HISTORY_RECORDS];
} history_record;
#endif // RECORD_ACQUISITION_STACKS


/*
 *-----------------------------------------------------------------------------
 * clockcache_entry --
 *
 *     The meta data entry in the cache. Each entry has the underlying
 *     page_handle together with some flags.
 *-----------------------------------------------------------------------------
 */
typedef struct clockcache_entry {
   page_handle     page;
   volatile uint32 status;
   page_type       type;
#ifdef RECORD_ACQUISITION_STACKS
   int            next_history_record;
   history_record history[NUM_HISTORY_RECORDS];
#endif
} clockcache_entry;

typedef uint32 entry_status; // Saved in clockcache_entry->status

_Static_assert(sizeof(entry_status) == FSIZEOF(clockcache_entry, status),
               "Size of clockcache_entry->status is incorrect");

/*
 * Definitions for entry_status (clockcache_entry->status)
 */
#define CC_FREE        (1u << 0) // entry is free
#define CC_ACCESSED    (1u << 1) // access bit prevents eviction for one cycle
#define CC_CLEAN       (1u << 2) // page has no new changes
#define CC_WRITEBACK   (1u << 3) // page is actively in writeback
#define CC_LOADING     (1u << 4) // page is actively being read from disk
#define CC_WRITELOCKED (1u << 5) // write lock is held
#define CC_CLAIMED     (1u << 6) // claim is held

/* Common status flag combinations */
// free entry
#define CC_FREE_STATUS (0 | CC_FREE)

// evictable unlocked page
#define CC_EVICTABLE_STATUS (0 | CC_CLEAN)

// evictable locked page
#define CC_LOCKED_EVICTABLE_STATUS (0 | CC_CLEAN | CC_CLAIMED | CC_WRITELOCKED)

// accessed, but otherwise evictable page
#define CC_ACCESSED_STATUS (0 | CC_ACCESSED | CC_CLEAN)

// newly allocated page (dirty, writelocked)
#define CC_ALLOC_STATUS (0 | CC_WRITELOCKED | CC_CLAIMED)

// eligible for writeback (unaccessed)
#define CC_CLEANABLE1_STATUS /* dirty */ (0)

// eligible for writeback (accessed)
#define CC_CLEANABLE2_STATUS /* dirty */ (0 | CC_ACCESSED)

// actively in writeback (unaccessed)
#define CC_WRITEBACK1_STATUS (0 | CC_WRITEBACK)

// actively in writeback (accessed)
#define CC_WRITEBACK2_STATUS (0 | CC_ACCESSED | CC_WRITEBACK)

// loading for read
#define CC_READ_LOADING_STATUS (0 | CC_ACCESSED | CC_CLEAN | CC_LOADING)

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
 *      Each thread has a batch of pages indicated by cc->thread_free_hand[tid]
 *      from which it draws free pages. When a thread doesn't find a free page
 *      in its batch, it obtains a pair of new batches: one to evict
 *      (from cc->free_hand) and one to clean. The batch to clean is
 *      cc->cleaner_gap batches ahead of the current evictor head, so that
 *      cleaned pages have time to flush before eviction. Both cleaning and
 *      eviction use cc->batch_busy to avoid conflicts and contention.
 *----------------------------------------------------------------------
 */
typedef struct clockcache {
   cache              super;
   clockcache_config *cfg;
   allocator         *al;
   io_handle         *io;

   uint32              *lookup;
   clockcache_entry    *entry;
   buffer_handle       *bh;   // actual memory for pages
   char                *data; // convenience pointer for bh
   platform_log_handle  logfile;
   platform_heap_handle heap_handle;
   platform_heap_id     heap_id;

   // Distributed locks (the write bit is in the status uint32 of the entry)
   buffer_handle  *rc_bh;
   volatile uint8 *refcount;
   volatile uint8 *pincount;

   // Clock hands and related metadata
   volatile uint32 evict_hand;
   volatile uint32 free_hand;
   volatile bool  *batch_busy;
   uint64          cleaner_gap;

   volatile struct {
      volatile uint32 free_hand;
      bool            enable_sync_get;
   } PLATFORM_CACHELINE_ALIGNED per_thread[MAX_THREADS];

   // Stats
   cache_stats stats[MAX_THREADS];

   task_system *ts;
} clockcache;


/*
 *-----------------------------------------------------------------------------
 * Function declarations
 *-----------------------------------------------------------------------------
 */

void
clockcache_config_init(clockcache_config *cache_config,
                       uint64             page_size,
                       uint64             extent_size,
                       uint64             capacity,
                       char              *cache_logfile,
                       uint64             use_stats);

platform_status
clockcache_init(clockcache          *cc,   // OUT
                clockcache_config   *cfg,  // IN
                io_handle           *io,   // IN
                allocator           *al,   // IN
                char                *name, // IN
                task_system         *ts,   // IN
                platform_heap_handle hh,   // IN
                platform_heap_id     hid,  // IN
                platform_module_id   mid);   // IN

void
clockcache_deinit(clockcache *cc); // IN

#endif // __CLOCKCACHE_H
