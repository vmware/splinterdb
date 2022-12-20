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
typedef uint64 (*base_addr_fn)(const cache_config *cfg, uint64 addr);

typedef struct cache_config_ops {
   cache_config_generic_uint64_fn page_size;
   cache_config_generic_uint64_fn extent_size;
   base_addr_fn                   extent_base_addr;
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
cache_config_extent_base_addr(const cache_config *cfg, uint64 addr)
{
   return cfg->ops->extent_base_addr(cfg, addr);
}

static inline uint64
cache_config_pages_per_extent(const cache_config *cfg)
{
   uint64 page_size   = cache_config_page_size(cfg);
   uint64 extent_size = cache_config_extent_size(cfg);
   return extent_size / page_size;
}

static inline uint64
cache_config_pages_share_extent(const cache_config *cfg,
                                uint64              addr1,
                                uint64              addr2)
{
   return cache_config_extent_base_addr(cfg, addr1)
          == cache_config_extent_base_addr(cfg, addr2);
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
typedef void (*extent_hard_evict_fn)(cache *cc, uint64 addr, page_type type);
typedef uint8 (*page_get_ref_fn)(cache *cc, uint64 addr);
typedef page_handle *(*page_get_fn)(cache    *cc,
                                    uint64    addr,
                                    bool      blocking,
                                    page_type type);
typedef cache_async_result (*page_get_async_fn)(cache            *cc,
                                                uint64            addr,
                                                page_type         type,
                                                cache_async_ctxt *ctxt);
typedef void (*page_async_done_fn)(cache            *cc,
                                   page_type         type,
                                   cache_async_ctxt *ctxt);
typedef bool (*page_claim_fn)(cache *cc, page_handle *page);
typedef void (*page_sync_fn)(cache       *cc,
                             page_handle *page,
                             bool         is_blocking,
                             page_type    type);
typedef void (*extent_sync_fn)(cache  *cc,
                               uint64  addr,
                               uint64 *pages_outstanding);
typedef void (*page_prefetch_fn)(cache *cc, uint64 addr, page_type type);
typedef int (*evict_fn)(cache *cc, bool ignore_pinned);
typedef void (*assert_ungot_fn)(cache *cc, uint64 addr);
typedef bool (*page_valid_fn)(cache *cc, uint64 addr);
typedef void (*validate_page_fn)(cache *cc, page_handle *page, uint64 addr);
typedef void (*io_stats_fn)(cache *cc, uint64 *read_bytes, uint64 *write_bytes);
typedef uint32 (*count_dirty_fn)(cache *cc);
typedef uint16 (*page_get_read_ref_fn)(cache *cc, page_handle *page);
typedef bool (*cache_present_fn)(cache *cc, page_handle *page);
typedef void (*enable_sync_get_fn)(cache *cc, bool enabled);
typedef allocator *(*cache_allocator_fn)(const cache *cc);
typedef cache_config *(*cache_config_fn)(const cache *cc);
typedef void (*cache_print_fn)(platform_log_handle *log_handle, cache *cc);

/*
 * Cache Operations structure:
 * Defines an abstract collection of "cache operation"-function pointers
 * for a caching system.
 */
typedef struct cache_ops {
   page_alloc_fn        page_alloc;
   extent_hard_evict_fn extent_hard_evict;
   page_get_ref_fn      page_get_ref;
   page_get_fn          page_get;
   page_get_async_fn    page_get_async;
   page_async_done_fn   page_async_done;
   page_generic_fn      page_unget;
   page_claim_fn        page_claim;
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
   page_valid_fn        page_valid;
   validate_page_fn     validate_page;
   cache_present_fn     cache_present;
   cache_print_fn       print;
   cache_print_fn       print_stats;
   io_stats_fn          io_stats;
   cache_generic_fn     reset_stats;
   count_dirty_fn       count_dirty;
   page_get_read_ref_fn page_get_read_ref;
   enable_sync_get_fn   enable_sync_get;
   cache_allocator_fn   cache_allocator;
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
 * Returns a pointer to the page_handle for the page with address addr,
 * with thread holding the write lock on the page.
 * The page is assumed to be unallocated, so the backing data is not read
 * from the disk.
 * XXX The in-memory page is initialized to zeros OR
 * XXX The in-memory page is uninitialized garbage; it is the responsibility
 * of the caller to set every byte before allowing it to be writte back
 * to the disk.
 *
 * The page is marked as `type`.
 * XXX Where is that type information stored,
 * and is it persistent? Is it updated synchronously with the page?
 *----------------------------------------------------------------------
 */
static inline page_handle *
cache_alloc(cache *cc, uint64 addr, page_type type)
{
   return cc->ops->page_alloc(cc, addr, type);
}

/*
 *----------------------------------------------------------------------
 * cache_hard_evict_extent
 *
 * Attempts to evict all the pages in the extent. Will wait for writeback,
 * but will evict and discard dirty pages.
 * XXX Why would discarding dirty pages ever be desirable?
 *----------------------------------------------------------------------
 */
static inline void
cache_hard_evict_extent(cache *cc, uint64 addr, page_type type)
{
   cc->ops->extent_hard_evict(cc, addr, type);
}

/*
 *----------------------------------------------------------------------
 * cache_get_ref
 *
 * Returns the count of outstanding references to the page.
 * XXX Why would calling threads care about this? Seems internal.
 *----------------------------------------------------------------------
 */
static inline uint8
cache_get_ref(cache *cc, uint64 addr)
{
   return cc->ops->page_get_ref(cc, addr);
}

/*
 *----------------------------------------------------------------------
 * cache_get
 *
 * Returns a pointer to the page_handle for the page with address addr.
 * If blocking is set, then it blocks until the page is unlocked as well.
 *
 * If blocking is TRUE, always returns with a read lock held.
 * If blocking is FALSE, returns non-NULL if and only if the thread now holds a
 * read lock on the page at addr.
 *----------------------------------------------------------------------
 */
static inline page_handle *
cache_get(cache *cc, uint64 addr, bool blocking, page_type type)
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
 * XXX clockcache_async_done docs makes me think this is internal; why
 * is it exposed in the public API?
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
 * XXX The page must not be locked or claimed?
 * 
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
 *      Attempts to upgrade a read lock to claim.
 *
 *      A claim means the lock is still held read-only, but that this
 *      thread will be able to upgrade this read lock to write lock later
 *      via get_lock(). (Any other reading thread that fails to get the claim
 *      will have to abandon its read lock and try again later, after which
 *      the locked value may have been changed by the thread that secured
 *      the claim and used the write lock.)
 *
 *      NOTE: If cache_claim returns false, the caller must release the
 *      read lock before attempting cache_claim again to avoid deadlock.
 *
 *      returns:
 *      - TRUE if a claim was obtained
 *      - FALSE if another thread holds a claim (or write lock)
 *
 *      does not block
 *----------------------------------------------------------------------
 */
static inline bool
cache_claim(cache *cc, page_handle *page)
{
   return cc->ops->page_claim(cc, page);
}

/*
 *----------------------------------------------------------------------
 * cache_unclaim
 *
 * Abandon a claim without upgrading to a write lock.
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
 * Upgrade a claim to a write lock. Blocks until outstanding read locks
 * are released by other threads.
 * 
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
 * XXX Downgrade a write lock to ... a read-lock with a claim?
 * 
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
 * XXX Why is this a separate step from cache_lock()? I'd assume that,
 * by the time a thread upgrades to a write lock, it has already decided
 * to certainly update the page value. So why add another step to potentially
 * omit?
 * 
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
 * XXX Is this purely a performance optimization: we know this page will
 * be needed again very soon? Or is this ever used for safety, somehow?
 * Some weird one-big-lock argument?
 *
 * XXX I note that clockcache_evict_all has an !ignore_pinned_pages mode.
 * Do users of this interface need to know how that interacts?
 * 
 *----------------------------------------------------------------------
 */
static inline void
cache_pin(cache *cc, page_handle *page)
{
   return cc->ops->page_pin(cc, page);
}

/*
 *----------------------------------------------------------------------
 * cache_pin
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
 * Asynchronously writes the page back to disk. Currently there is no way to
 * check when the writeback has completed,
 * XXX thus this call is only useful to prepare a page for eviction?
 *
 *-----------------------------------------------------------------------------
 */
static inline void
cache_page_sync(cache *cc, page_handle *page, bool is_blocking, page_type type)
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
 * complete, *pages_outstanding is decremented. The caller may use the
 * XXX atomically-read? Because it's an aligned uint64?
 * value of *pages_outstanding to determine when the extent writeback is
 * complete.
 *
 * All pages in the extent must be clean or cleanable.
 * XXX What other states might there be, and what would happen if you
 * called this with a page in such a state?
 * XXX should we copy this remark from flush?
 * XXX Asserts that there are no pins, read locks, claims or write locks.
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
 * Issues writeback for all page in the cache.
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
 * Asserts that there are no pins, read locks, claims or write locks.
 * Always returns 0.
 *-----------------------------------------------------------------------------
 */
static inline int
cache_evict(cache *cc, bool ignore_pinned_pages)
{
   return cc->ops->evict(cc, ignore_pinned_pages);
}

/*
 *-----------------------------------------------------------------------------
 * cache_cleanup
 *
 * XXX ?
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
 * Debugging facility.
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
 * Debugging facility.
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
 * cache_reset_stats
 *
 * Debugging facility.
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
 * cache_page_valid
 *
 * Debugging facility.
 * Returns false if addr is unaligned or out-of-bounds, or if addr has no get
 * references (presumably a call-graph ancestor caller holds at least one).
 *-----------------------------------------------------------------------------
 */
static inline bool
cache_page_valid(cache *cc, uint64 addr)
{
   return cc->ops->page_valid(cc, addr);
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
 * Debugging facility.
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
 * Debugging facility.
 * Returns TRUE if page is present in the cache.
 *-----------------------------------------------------------------------------
 */
static inline bool
cache_present(cache *cc, page_handle *page)
{
   return cc->ops->cache_present(cc, page);
}

/*
 *-----------------------------------------------------------------------------
 * cache_enable_sync_get
 *
 * XXX No idea.
 *-----------------------------------------------------------------------------
 */
static inline void
cache_enable_sync_get(cache *cc, bool enabled)
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
cache_allocator(const cache *cc)
{
   return cc->ops->cache_allocator(cc);
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
 * cache_extent_base_addr
 *
 * Returns the base addr of the extent containing addr.
 *-----------------------------------------------------------------------------
 */
static inline uint64
cache_extent_base_addr(const cache *cc, uint64 addr)
{
   return cache_config_extent_base_addr(cache_get_config(cc), addr);
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

/*
 *-----------------------------------------------------------------------------
 * cache_pages_share_extent
 *
 * Returns TRUE if addr1 and addr2 belong to the same extent.
 *-----------------------------------------------------------------------------
 */
static inline uint64
cache_pages_share_extent(const cache *cc, uint64 addr1, uint64 addr2)
{
   cache_config *cfg = cache_get_config(cc);
   return cache_config_pages_share_extent(cfg, addr1, addr2);
}
