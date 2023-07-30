// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * cache.h --
 *
 *     This file contains the abstract interface for a cache.
 */

#pragma once

#include "platform.h"
#include "allocator.h"
#include "io.h"

typedef struct page_handle {
   char  *data;
   uint64 disk_addr;
} page_handle;

typedef struct cache_config cache_config;
typedef struct cache        cache;

/*
 * Cache usage statistics structure, for different page types in the cache.
 * An array of this structure, one for each thread configured, is stored in
 * the global clockcache structure.
 */
typedef struct cache_stats {
   uint64 cache_hits[NUM_PAGE_TYPES];
   uint64 cache_misses[NUM_PAGE_TYPES];
   uint64 cache_miss_time_ns[NUM_PAGE_TYPES];
   uint64 page_writes[NUM_PAGE_TYPES];
   uint64 page_reads[NUM_PAGE_TYPES];
   uint64 prefetches_issued[NUM_PAGE_TYPES];
   uint64 writes_issued;
   uint64 syncs_issued;
} PLATFORM_CACHELINE_ALIGNED cache_stats;

/*
 * By defining a maximum for pages_per_extent, we can easily avoid a bunch of
 * variable-length arrays.
 * Our default is 128k extents and 4k pages, and 32 (usually pointers) easily
 * fits on the stack so we can avoid mallocs in addition to VLAs.
 * If we need to increase this, if we make the stack too large we'll need to
 * either allocate some scratch in a larger buffer, or add a lot of mallocs.
 */
#define MAX_PAGES_PER_EXTENT 32lu

/*
 * The Maximum ref count on a single page that a thread is allowed to
 * have. The sum of all threads' ref counts is MAX_THREADS times this.
 * See cache_get_read_ref() below.
 */
#define MAX_READ_REFCOUNT UINT16_MAX

// This is probably necessary:
_Static_assert(IS_POWER_OF_2(MAX_PAGES_PER_EXTENT),
               "MAX_PAGES_PER_EXTENT not a power of 2");

typedef enum {
   // Success without needing async IO because of cache hit.
   async_success = 0xc0ffee,
   /*
    * Locked it's write-locked, or raced with eviction or
    * another thread was loading the page. Caller needs to retry.
    */
   async_locked,
   // Retry or throttle ingress lookups because we're out of io reqs.
   async_no_reqs,
   // Started async IO and caller will be notified via callback.
   async_io_started
} cache_async_result;

struct cache_async_ctxt;
typedef void (*cache_async_cb)(struct cache_async_ctxt *ctxt);

/*
 * Context structure to manage async access through the cache.
 * User can embed this within a user-specific context
 */
typedef struct cache_async_ctxt {
   cache          *cc;     // IN cache
   cache_async_cb  cb;     // IN callback for async_io_started
   void           *cbdata; // IN opaque callback data
   platform_status status; // IN status of async IO
   page_handle    *page;   // OUT page handle
   // Internal stats
   struct {
      timestamp issue_ts; // issue time
      timestamp compl_ts; // completion time
   } stats;
} cache_async_ctxt;

typedef uint64 (*cache_config_generic_uint64_fn)(const cache_config *cfg);

typedef struct cache_config_ops {
   cache_config_generic_uint64_fn page_size;
   cache_config_generic_uint64_fn extent_size;
} cache_config_ops;

typedef struct cache_config {
   const cache_config_ops *ops;
} cache_config;

static inline uint64
cache_config_page_size(const cache_config *cfg)
{
   return cfg->ops->page_size(cfg);
}

static inline uint64
cache_config_extent_size(const cache_config *cfg)
{
   return cfg->ops->extent_size(cfg);
}

static inline uint64
cache_config_pages_per_extent(const cache_config *cfg)
{
   uint64 page_size   = cache_config_page_size(cfg);
   uint64 extent_size = cache_config_extent_size(cfg);
   return extent_size / page_size;
}

static inline uint64
cache_config_extent_page(const cache_config *cfg, uint64 extent_addr, uint64 i)
{
   return extent_addr + i * cache_config_page_size(cfg);
}

typedef void (*cache_generic_fn)(cache *cc);
typedef uint64 (*cache_generic_uint64_fn)(cache *cc);
typedef void (*page_generic_fn)(cache *cc, page_handle *page);

typedef page_handle *(*page_alloc_fn)(cache *cc, uint64 addr, page_type type);
typedef void (*extent_discard_fn)(cache *cc, uint64 addr, page_type type);
typedef page_handle *(*page_get_fn)(cache    *cc,
                                    uint64    addr,
                                    bool32    blocking,
                                    page_type type);
typedef cache_async_result (*page_get_async_fn)(cache            *cc,
                                                uint64            addr,
                                                page_type         type,
                                                cache_async_ctxt *ctxt);
typedef void (*page_async_done_fn)(cache            *cc,
                                   page_type         type,
                                   cache_async_ctxt *ctxt);
typedef bool32 (*page_try_claim_fn)(cache *cc, page_handle *page);
typedef void (*page_sync_fn)(cache       *cc,
                             page_handle *page,
                             bool32       is_blocking,
                             page_type    type);
typedef void (*extent_sync_fn)(cache  *cc,
                               uint64  addr,
                               uint64 *pages_outstanding);
typedef void (*page_prefetch_fn)(cache *cc, uint64 addr, page_type type);
typedef int (*evict_fn)(cache *cc, bool32 ignore_pinned);
typedef void (*assert_ungot_fn)(cache *cc, uint64 addr);
typedef void (*validate_page_fn)(cache *cc, page_handle *page, uint64 addr);
typedef void (*io_stats_fn)(cache *cc, uint64 *read_bytes, uint64 *write_bytes);
typedef uint32 (*count_dirty_fn)(cache *cc);
typedef uint16 (*page_get_read_ref_fn)(cache *cc, page_handle *page);
typedef bool32 (*cache_present_fn)(cache *cc, page_handle *page);
typedef void (*enable_sync_get_fn)(cache *cc, bool32 enabled);
typedef allocator *(*get_allocator_fn)(const cache *cc);
typedef cache_config *(*cache_config_fn)(const cache *cc);
typedef void (*cache_print_fn)(platform_log_handle *log_handle, cache *cc);

/*
 * Cache Operations structure:
 * Defines an abstract collection of "cache operation"-function pointers
 * for a caching system.
 */
typedef struct cache_ops {
   page_alloc_fn        page_alloc;
   extent_discard_fn    extent_discard;
   page_get_fn          page_get;
   page_get_async_fn    page_get_async;
   page_async_done_fn   page_async_done;
   page_generic_fn      page_unget;
   page_try_claim_fn    page_try_claim;
   page_generic_fn      page_unclaim;
   page_generic_fn      page_lock;
   page_generic_fn      page_unlock;
   page_prefetch_fn     page_prefetch;
   page_generic_fn      page_mark_dirty;
   page_generic_fn      page_pin;
   page_generic_fn      page_unpin;
   page_sync_fn         page_sync;
   extent_sync_fn       extent_sync;
   cache_generic_fn     flush;
   evict_fn             evict;
   cache_generic_fn     cleanup;
   assert_ungot_fn      assert_ungot;
   cache_generic_fn     assert_free;
   validate_page_fn     validate_page;
   cache_present_fn     cache_present;
   cache_print_fn       print;
   cache_print_fn       print_stats;
   io_stats_fn          io_stats;
   cache_generic_fn     reset_stats;
   count_dirty_fn       count_dirty;
   page_get_read_ref_fn page_get_read_ref;
   enable_sync_get_fn   enable_sync_get;
   get_allocator_fn     get_allocator;
   cache_config_fn      get_config;
} cache_ops;

// To sub-class cache, make a cache your first field;
struct cache {
   const cache_ops *ops;
};

/*
 *----------------------------------------------------------------------
 * cache_alloc
 *
 * Allocate a slot in the cache for the given page address.
 *
 * The page is assumed to be unallocated, so the backing data is not read from
 * the disk.  The contents of the in-memory page is undefined. It is the
 * responsibility of the caller to set every byte before allowing it to be
 * write back to the disk to avoid a security bug that might leak unexpected
 * data to the persistent store.
 *
 * `addr` is a byte offset from the beginning of the disk. It should be aligned
 * to cache_page_size().
 *
 * `type` marks the page as being used for the given purpose for debugging
 * and statistical accounting purposes.
 *
 * Returns a pointer to the page_handle for the page with address addr,
 * with thread holding the write lock on the page.
 *----------------------------------------------------------------------
 */
static inline page_handle *
cache_alloc(cache *cc, uint64 addr, page_type type)
{
   return cc->ops->page_alloc(cc, addr, type);
}

/*
 *----------------------------------------------------------------------
 * cache_extent_discard
 *
 * Evicts all the pages in the extent. Dirty pages are discarded.
 * This call may block on I/O (to complete writebacks initiated before
 * this call).
 *
 * Once an extent is freed, this function is used to evict all of its pages
 * from the cache.  This function is used to maintain an invariant that the
 * cache only contains only contains pages allocated in the RC allocator; if
 * this function were not called, and this extent were to be reallocated,
 * subsequent calls to cache_alloc could create duplicate entries in the cache
 * for the same backing page.
 *----------------------------------------------------------------------
 */
static inline void
cache_extent_discard(cache *cc, uint64 addr, page_type type)
{
   cc->ops->extent_discard(cc, addr, type);
}

/*
 *----------------------------------------------------------------------
 * cache_get
 *
 * Returns a pointer to the page_handle for the page with address addr.
 *
 * If blocking is set, then it blocks until the page is unlocked as well.
 * If blocking is TRUE, always returns with a read lock held.
 * If blocking is FALSE, returns non-NULL if and only if the thread now holds a
 * read lock on the page at addr.
 *
 * addr is a byte offset from the beginning of the disk. It should be aligned
 * to cache_page_size().
 *----------------------------------------------------------------------
 */
static inline page_handle *
cache_get(cache *cc, uint64 addr, bool32 blocking, page_type type)
{
   return cc->ops->page_get(cc, addr, blocking, type);
}

/*
 *----------------------------------------------------------------------
 * cache_ctxt_init
 *
 * Initialize an async context, preparing it for use with cache_get_async.
 *----------------------------------------------------------------------
 */
static inline void
cache_ctxt_init(cache            *cc,
                cache_async_cb    cb,
                void             *cbdata,
                cache_async_ctxt *ctxt)
{
   ctxt->cc     = cc;
   ctxt->cb     = cb;
   ctxt->cbdata = cbdata;
   ctxt->page   = NULL;
}

/*
 *----------------------------------------------------------------------
 * cache_get_async
 *
 * Schedules an asynchronous page get. See cache_async_result for results.
 *----------------------------------------------------------------------
 */
static inline cache_async_result
cache_get_async(cache *cc, uint64 addr, page_type type, cache_async_ctxt *ctxt)
{
   return cc->ops->page_get_async(cc, addr, type, ctxt);
}

/*
 *----------------------------------------------------------------------
 * cache_async_done
 *
 * Perform callbacks on the thread that made the async call after an async
 * operation completes.
 *----------------------------------------------------------------------
 */
static inline void
cache_async_done(cache *cc, page_type type, cache_async_ctxt *ctxt)
{
   return cc->ops->page_async_done(cc, type, ctxt);
}

/*
 *----------------------------------------------------------------------
 * cache_unget
 *
 * Drop a reference to a page.
 * The page must be read-locked by the calling thread (and not claimed or
 * write-locked) before making this call.
 *----------------------------------------------------------------------
 */
static inline void
cache_unget(cache *cc, page_handle *page)
{
   return cc->ops->page_unget(cc, page);
}

/*
 *----------------------------------------------------------------------
 * cache_claim
 *
 * Attempts to upgrade a read lock to claim.
 *
 * A claim means the lock is still held read-only, but that this
 * thread will be able to upgrade this read lock to write lock later
 * via get_lock(). (Any other reading thread that fails to get the claim
 * will have to abandon its read lock and try again later, after which
 * the locked value may have been changed by the thread that secured
 * the claim and used the write lock.)
 *
 * NOTE: If cache_claim returns false, the caller must release the
 * read lock before attempting cache_claim again to avoid deadlock.
 *
 * returns:
 * - TRUE if a claim was obtained
 * - FALSE if another thread holds a claim (or write lock)
 *
 * Does not block.
 *----------------------------------------------------------------------
 */
static inline bool32
cache_try_claim(cache *cc, page_handle *page)
{
   return cc->ops->page_try_claim(cc, page);
}

/*
 *----------------------------------------------------------------------
 * cache_unclaim
 *
 * Release a claim.
 * The handle must hold a claim when making this call.
 * The handle is changed to the read-locked state.
 *
 * Does not block.
 *----------------------------------------------------------------------
 */
static inline void
cache_unclaim(cache *cc, page_handle *page)
{
   return cc->ops->page_unclaim(cc, page);
}

/*
 *----------------------------------------------------------------------
 * cache_lock
 *
 * Upgrade a claim to a write lock.
 * The handle must hold a claim when making this call.
 * The handle is changed to the write-locked state.
 *
 * Blocks until outstanding read locks are released by other threads.
 *
 * If you call this method, you almost certainly want to call
 * cache_mark_dirty() immediately afterward.
 *----------------------------------------------------------------------
 */
static inline void
cache_lock(cache *cc, page_handle *page)
{
   return cc->ops->page_lock(cc, page);
}

/*
 *----------------------------------------------------------------------
 * cache_unlock
 *
 * Release a write lock.
 * The handle must hold a write lock when making this call.
 * The handle is changed to the claimed state.
 *
 * Does not block.
 *----------------------------------------------------------------------
 */
static inline void
cache_unlock(cache *cc, page_handle *page)
{
   return cc->ops->page_unlock(cc, page);
}

/*
 *----------------------------------------------------------------------
 * cache_prefetch
 *
 * Asynchronously load the extent with given base address. No notification is
 * provided to the calling thread; it may call cache_get when it's ready
 * to block on the arrival of the page.
 *
 *----------------------------------------------------------------------
 */
static inline void
cache_prefetch(cache *cc, uint64 addr, page_type type)
{
   return cc->ops->page_prefetch(cc, addr, type);
}

/*
 *----------------------------------------------------------------------
 * cache_mark_dirty
 *
 * Marks a page changed, to be written back.
 * The caller had better have the write lock on the page via cache_lock()
 * before changing its value.
 *
 * TODO This method should be removed; its effect should come automatically
 * with the acquisition of a write lock. @robj reports lots of bugs
 * due to forgetting to call this method. And we can't think of a case
 * where we'd want the "optimization" of taking a write lock but then
 * decide not to dirty it.
 *----------------------------------------------------------------------
 */
static inline void
cache_mark_dirty(cache *cc, page_handle *page)
{
   return cc->ops->page_mark_dirty(cc, page);
}

/*
 *----------------------------------------------------------------------
 * cache_pin
 *
 * Pin the page in the cache, disallowing eviction.
 *
 * This is a performance optimization used by memtable, where the caller knows
 * this page will be needed again very soon.
 *----------------------------------------------------------------------
 */
static inline void
cache_pin(cache *cc, page_handle *page)
{
   return cc->ops->page_pin(cc, page);
}

/*
 *----------------------------------------------------------------------
 * cache_unpin
 *
 * Release the pin from a cache page, allowing it to be evicted.
 *
 * The pin value is reference counted, so multiple threads may pin
 * and unpin, and the page is only eligible for eviction when no threads
 * have it pinned.
 *
 *----------------------------------------------------------------------
 */
static inline void
cache_unpin(cache *cc, page_handle *page)
{
   return cc->ops->page_unpin(cc, page);
}

/*
 *-----------------------------------------------------------------------------
 * cache_page_sync
 *
 * Asynchronously writes the page back to disk.
 *
 * This is used to sync log pages opportunistically. The current API doesn't
 * inform the user when this happens. "It's not the ideal API." -- @aconway
 *-----------------------------------------------------------------------------
 */
static inline void
cache_page_sync(cache       *cc,
                page_handle *page,
                bool32       is_blocking,
                page_type    type)
{
   return cc->ops->page_sync(cc, page, is_blocking, type);
}

/*
 *-----------------------------------------------------------------------------
 * cache_extent_sync
 *
 * Asynchronously syncs the extent beginning at addr.
 *
 * *pages_outstanding is immediately incremented by the number of pages
 * issued for writeback (the non-clean pages of the extent); as writebacks
 * complete, *pages_outstanding is decremented atomically.
 *
 * Assumes pages_outstanding is an aligned uint64, so (on x86) the caller
 * can access it and observe its value atomically.
 *
 * TODO: What happens if two callers call cache_extent_sync on the same extent?
 *
 * All pages in the extent must be clean or cleanable.
 * The page may not be in writeback, loading, or locked, or claimed, otherwise
 * undefined behavior will occur.
 *-----------------------------------------------------------------------------
 */
static inline void
cache_extent_sync(cache *cc, uint64 addr, uint64 *pages_outstanding)
{
   cc->ops->extent_sync(cc, addr, pages_outstanding);
}

/*
 *-----------------------------------------------------------------------------
 * cache_flush
 *
 * Issues writeback for all pages in the cache.
 *
 * Asserts that there are no pins, read locks, claims or write locks.
 *-----------------------------------------------------------------------------
 */
static inline void
cache_flush(cache *cc)
{
   cc->ops->flush(cc);
}

/*
 *-----------------------------------------------------------------------------
 * cache_evict
 *
 * Evicts all the pages.
 * Asserts that there are no pins (if ignore_pinned_pages is false), read
 * locks, claims or write locks.
 * Always returns 0.
 *
 * TODO: Does ignore_pinned_pages ignore the pages or the pinnedness of the
 *pages?
 *
 * Test facility.
 * This method is only used for testing, specifically in cache_test.
 * TODO Should be deleted and replaced with destructing and constructing
 * a fresh cache.
 *-----------------------------------------------------------------------------
 */
static inline int
cache_evict(cache *cc, bool32 ignore_pinned_pages)
{
   return cc->ops->evict(cc, ignore_pinned_pages);
}

/*
 *-----------------------------------------------------------------------------
 * cache_cleanup
 *
 * Ensures all pending cache callbacks are called.
 *
 * Test facility.
 * Used in tests to process pending IO completions during test shutdowns.
 *-----------------------------------------------------------------------------
 */
static inline void
cache_cleanup(cache *cc)
{
   return cc->ops->cleanup(cc);
}

/*
 *-----------------------------------------------------------------------------
 * cache_assert_ungot
 *
 * Debugging facility.
 * Asserts that no threads have a reference to the page at addr.
 *-----------------------------------------------------------------------------
 */
static inline void
cache_assert_ungot(cache *cc, uint64 addr)
{
   return cc->ops->assert_ungot(cc, addr);
}

/*
 *-----------------------------------------------------------------------------
 * cache_assert_free
 *
 * TODO(aconway): rename
 *
 * Debugging facility.
 * Asserts that no thread has a write lock on the page at addr.
 *-----------------------------------------------------------------------------
 */
static inline void
cache_assert_free(cache *cc)
{
   return cc->ops->assert_free(cc);
}

/*
 *-----------------------------------------------------------------------------
 * cache_print
 *
 * Debugging facility.
 * Prints a bitmap representation of the cache.
 *-----------------------------------------------------------------------------
 */
static inline void
cache_print(platform_log_handle *log_handle, cache *cc)
{
   return cc->ops->print(log_handle, cc);
}

/*
 *-----------------------------------------------------------------------------
 * cache_print_stats
 *
 * Analysis facility.
 * Prints out performance statistics.
 *-----------------------------------------------------------------------------
 */
static inline void
cache_print_stats(platform_log_handle *log_handle, cache *cc)
{
   return cc->ops->print_stats(log_handle, cc);
}

/*
 *-----------------------------------------------------------------------------
 * cache_reset_stats
 *
 * Analysis facility.
 * Resets performance statistics counters.
 *-----------------------------------------------------------------------------
 */
static inline void
cache_reset_stats(cache *cc)
{
   return cc->ops->reset_stats(cc);
}

/*
 *-----------------------------------------------------------------------------
 * cache_io_stats
 *
 * Analysis facility.
 * Returns performance statistics counts.
 *-----------------------------------------------------------------------------
 */
static inline void
cache_io_stats(cache *cc, uint64 *read_bytes, uint64 *write_bytes)
{
   return cc->ops->io_stats(cc, read_bytes, write_bytes);
}

/*
 *-----------------------------------------------------------------------------
 * cache_validate_page
 *
 * Debugging facility.
 * Asserts cache_page_valid, that the addr matches what page points to,
 * and that the page isn't free.
 *-----------------------------------------------------------------------------
 */
static inline void
cache_validate_page(cache *cc, page_handle *page, uint64 addr)
{
   cc->ops->validate_page(cc, page, addr);
}

/*
 *-----------------------------------------------------------------------------
 * cache_count_dirty
 *
 * Debugging facility.
 * Returns the number of dirty pages in the cache.
 *-----------------------------------------------------------------------------
 */
static inline uint32
cache_count_dirty(cache *cc)
{
   return cc->ops->count_dirty(cc);
}

/*
 *-----------------------------------------------------------------------------
 * cache_get_read_ref
 *
 * Testing facility.
 * Returns the number of threads with references to page.
 *-----------------------------------------------------------------------------
 */
static inline uint32
cache_get_read_ref(cache *cc, page_handle *page)
{
   return cc->ops->page_get_read_ref(cc, page);
}

/*
 *-----------------------------------------------------------------------------
 * cache_present
 *
 * Testing facility.
 * Returns TRUE if page is present in the cache.
 *-----------------------------------------------------------------------------
 */
static inline bool32
cache_present(cache *cc, page_handle *page)
{
   return cc->ops->cache_present(cc, page);
}

/*
 *-----------------------------------------------------------------------------
 * cache_enable_sync_get
 *
 * Debugging facility.
 * When set to FALSE, cache_get() is disallowed; cache_get_async must be
 * used instead.
 * (This facility was used when introducing async behavior to enforce that all
 * callers use only the async interface.)
 *-----------------------------------------------------------------------------
 */
static inline void
cache_enable_sync_get(cache *cc, bool32 enabled)
{
   cc->ops->enable_sync_get(cc, enabled);
}

/*
 *-----------------------------------------------------------------------------
 * cache_allocator
 *
 * Vestigial.
 * Returns an allocator associated with the cache.
 * TODO: Remove; callers should get their own darn allocators.
 *-----------------------------------------------------------------------------
 */
static inline allocator *
cache_get_allocator(const cache *cc)
{
   return cc->ops->get_allocator(cc);
}

/*
 *-----------------------------------------------------------------------------
 * cache_get_config
 *
 * Returns the cache configuration structure.
 * TODO Weird that the calls below this are top-level cache calls; seems like
 * callers should get the config and call the corresponding config-level entry
 * points.
 *-----------------------------------------------------------------------------
 */
static inline cache_config *
cache_get_config(const cache *cc)
{
   return cc->ops->get_config(cc);
}

/*
 *-----------------------------------------------------------------------------
 * cache_page_size
 *
 * Returns the page size from the cache configuration.
 *-----------------------------------------------------------------------------
 */
static inline uint64
cache_page_size(const cache *cc)
{
   return cache_config_page_size(cache_get_config(cc));
}

/*
 *-----------------------------------------------------------------------------
 * cache_extent_size
 *
 * Returns the extent size from the cache configuration.
 *-----------------------------------------------------------------------------
 */
static inline uint64
cache_extent_size(const cache *cc)
{
   return cache_config_extent_size(cache_get_config(cc));
}

/*
 *-----------------------------------------------------------------------------
 * cache_pages_per_extent
 *
 * Returns the number of cache pages in an extent according to the cache
 * configuration.
 *-----------------------------------------------------------------------------
 */
static inline uint64
cache_pages_per_extent(const cache *cc)
{
   cache_config *cfg = cache_get_config(cc);
   return cache_config_pages_per_extent(cfg);
}
