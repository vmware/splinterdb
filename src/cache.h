// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * cache.h --
 *
 *     This file contains the abstract interface for a cache.
 */

#pragma once

#include "allocator.h"
#include "async.h"

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

#define PAGE_GET_ASYNC_STATE_BUFFER_SIZE (2048)
typedef union page_get_async_state_payload {
   uint8       bytes[PAGE_GET_ASYNC_STATE_BUFFER_SIZE];
   uint64      align_uint64;
   void       *align_ptr;
   long double align_long_double;
} page_get_async_state_payload;

typedef struct page_get_async_state_buffer {
   cache                       *cc;
   page_get_async_state_payload payload;
} page_get_async_state_buffer;

typedef void (*cache_generic_void_fn)(cache *cc);
typedef uint64 (*cache_generic_uint64_fn)(cache *cc);
typedef platform_status (*cache_generic_status_fn)(cache *cc);
typedef void (*page_generic_fn)(cache *cc, page_handle *page);

typedef void (*page_get_async_state_init_fn)(void             *payload,
                                             cache            *cc,
                                             uint64            addr,
                                             page_type         type,
                                             async_callback_fn callback,
                                             void             *callback_arg);
typedef async_status (*page_get_async_fn)(void *payload);
typedef page_handle *(*page_get_async_state_result_fn)(void *payload);

typedef page_handle *(*page_alloc_fn)(cache *cc, uint64 addr, page_type type);
typedef void (*extent_discard_fn)(cache *cc, uint64 addr, page_type type);
typedef page_handle *(*page_get_fn)(cache    *cc,
                                    uint64    addr,
                                    bool32    blocking,
                                    page_type type);
typedef bool32 (*page_try_claim_fn)(cache *cc, page_handle *page);

/*
 * ---- Writeback requests ----
 *
 * A writeback request is the receipt for one issued writeback: it names what
 * was written and identifies the page's dirty interval at the moment the write
 * was handed to the I/O layer.  Present it to cache_writeback_get_status() to
 * learn whether that write has completed.
 *
 * The interval identity is what makes a request meaningful, so it is produced
 * by the call that issues the write -- where it can be read atomically with the
 * CC_WRITEBACK transition -- rather than by a free-standing query.
 *
 * A request holds only the address, so it survives eviction of the page it
 * names and the caller need not hold a reference: a page that is no longer
 * resident was necessarily written back before it was evicted.
 *
 * gen == 0 means "nothing to wait for": the page was already clean when the
 * writeback was requested.
 *
 * A caller that only wants the write issued, and will never ask whether it
 * completed, may pass NULL instead of a request.
 */
typedef struct cache_writeback_request {
   uint64 addr;      // page addr, or the base addr of an extent
   uint64 gen;       // dirty interval the write covers; 0 == nothing issued
   bool32 is_extent; // whether addr names an extent rather than one page
} cache_writeback_request;

typedef enum cache_writeback_status {
   /* The write is still outstanding.  Poll cache_cleanup() and re-check. */
   CACHE_WRITEBACK_PENDING,
   /* The write completed: the contents as of the request reached the device.
    * Note this is completion, NOT durability -- see cache_durable_barrier(). */
   CACHE_WRITEBACK_COMPLETE,
   /* The write completed, but the page has since been dirtied again.  The
    * request is satisfied; callers that do not expect concurrent writers to
    * their pages should treat this as a bug in their own locking. */
   CACHE_WRITEBACK_REDIRTIED,
   /*
    * The write FAILED: these contents did not reach the device, and the request
    * will never be satisfied without a successful retry.  Polling again does
    * not help -- the cache retries failed writes on its own schedule, so a
    * caller that wants to keep waiting must be prepared to wait indefinitely;
    * one that needs durability now must propagate the failure.
    *
    * For an extent request this outranks REDIRTIED but not PENDING, so by the
    * time it is reported no write on the extent is still in flight.
    */
   CACHE_WRITEBACK_FAILED,
} cache_writeback_status;

typedef platform_status (*page_writeback_fn)(cache                   *cc,
                                             page_handle             *page,
                                             page_type                type,
                                             cache_writeback_request *req);
typedef platform_status (*extent_writeback_fn)(cache                   *cc,
                                               uint64                   addr,
                                               page_type                type,
                                               cache_writeback_request *req);
typedef cache_writeback_status (
   *writeback_get_status_fn)(cache *cc, const cache_writeback_request *req);
typedef void (*page_prefetch_fn)(cache *cc, uint64 addr, page_type type);
typedef int (*evict_fn)(cache *cc, bool32 ignore_pinned);
typedef bool32 (*page_addr_pred_fn)(cache *cc, uint64 addr);
typedef void (*page_addr_fn)(cache *cc, uint64 addr);
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
   page_alloc_fn     page_alloc;
   extent_discard_fn extent_discard;
   page_get_fn       page_get;

   page_get_async_state_init_fn   page_get_async_state_init;
   page_get_async_fn              page_get_async;
   page_get_async_state_result_fn page_get_async_result;

   page_generic_fn         page_unget;
   page_try_claim_fn       page_try_claim;
   page_generic_fn         page_unclaim;
   page_generic_fn         page_lock;
   page_generic_fn         page_unlock;
   page_prefetch_fn        page_prefetch;
   page_prefetch_fn        page_prefetch_page;
   page_generic_fn         page_pin;
   page_generic_fn         page_unpin;
   page_writeback_fn       page_writeback;
   extent_writeback_fn     extent_writeback;
   writeback_get_status_fn writeback_get_status;
   cache_generic_void_fn   flush;
   cache_generic_status_fn writeback_dirty;
   cache_generic_status_fn durable_barrier;
   evict_fn                evict;
   cache_generic_void_fn   cleanup;
   page_addr_pred_fn       in_use;
   page_addr_fn            assert_ungot;
   cache_generic_void_fn   assert_free;
   validate_page_fn        validate_page;
   cache_present_fn        cache_present;
   cache_print_fn          print;
   cache_print_fn          print_stats;
   io_stats_fn             io_stats;
   cache_generic_void_fn   reset_stats;
   count_dirty_fn          count_dirty;
   page_get_read_ref_fn    page_get_read_ref;
   enable_sync_get_fn      enable_sync_get;
   get_allocator_fn        get_allocator;
   cache_config_fn         get_config;
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

static inline void
cache_get_async_state_init(page_get_async_state_buffer *buffer,
                           cache                       *cc,
                           uint64                       addr,
                           page_type                    type,
                           async_callback_fn            callback,
                           void                        *callback_arg)
{
   buffer->cc = cc;
   return cc->ops->page_get_async_state_init(
      &buffer->payload, cc, addr, type, callback, callback_arg);
}

static inline async_status
cache_get_async(page_get_async_state_buffer *buffer)
{
   return buffer->cc->ops->page_get_async(&buffer->payload);
}

static inline page_handle *
cache_get_async_state_result(page_get_async_state_buffer *buffer)
{
   return buffer->cc->ops->page_get_async_result(&buffer->payload);
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
 * Acquiring the write lock marks the page dirty.
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
 * cache_prefetch_page
 *
 * Like cache_prefetch, but loads only the single page at addr rather than the
 * whole extent that contains it. Use this for sparse reads (e.g. a single
 * mini_allocator meta page) where pulling in the surrounding extent would waste
 * bandwidth. No notification is provided to the calling thread; it may call
 * cache_get when it's ready to block on the arrival of the page.
 *
 *----------------------------------------------------------------------
 */
static inline void
cache_prefetch_page(cache *cc, uint64 addr, page_type type)
{
   return cc->ops->page_prefetch_page(cc, addr, type);
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
 * cache_writeback_page
 *
 * Asynchronously issues writeback of the page and, if req is non-NULL, fills
 * in *req, the receipt for that write. This does NOT make the page durable; it
 * only hands the write to the I/O layer (no device-cache flush). Use
 * cache_writeback_get_status() to learn when it completes and
 * cache_durable_barrier() to make it durable.
 *
 * req may be NULL if the caller will never ask whether the write completed.
 * The returned status is still worth checking even then; see below.
 *
 * Does not block. If a writeback of this page is already in flight -- issued by
 * the pressure cleaner, say -- nothing further is issued and *req names that
 * in-flight interval, so the caller waits on it exactly as it would on its own
 * write. If the page is already clean, req->gen is 0: nothing to wait for.
 *
 * Returns STATUS_BUSY if the page is dirty but not writeback-able (locked or
 * claimed). Callers must treat that as a failure to make the page durable: no
 * write was issued and *req cannot report one.
 *
 * Returns STATUS_IO_ERROR if an earlier write of this page failed and no retry
 * has yet succeeded. Unlike STATUS_BUSY this is not transient: it persists
 * until the cache retries the write successfully.
 *-----------------------------------------------------------------------------
 */
static inline platform_status
cache_writeback_page(cache                   *cc,
                     page_handle             *page,
                     page_type                type,
                     cache_writeback_request *req)
{
   // Absorb the optional request here so that every cache implementation may
   // assume it was given somewhere to write the receipt.
   cache_writeback_request scratch;
   return cc->ops->page_writeback(cc, page, type, req ? req : &scratch);
}

/*
 *-----------------------------------------------------------------------------
 * cache_writeback_extent
 *
 * As cache_writeback_page(), but for every page of the extent beginning at
 * addr, coalesced into as few larger I/Os as the extent's residency allows.
 * One request covers the whole extent: req->gen is the newest dirty interval
 * among its pages, which is safe to compare every page against. As above, req
 * may be NULL.
 *
 * Pages of the extent that are clean or not resident need no write and are
 * skipped. Returns STATUS_BUSY if any page is dirty but not writeback-able, or
 * STATUS_IO_ERROR if an earlier write of any page failed; as with
 * cache_writeback_page(), the caller must treat either as a failure, since that
 * page's contents will not reach the device.
 *
 * Concurrent callers on the same extent are safe: only one can win a page's
 * writeback, and the other's request names that same in-flight interval, so
 * both observe its completion.
 *-----------------------------------------------------------------------------
 */
static inline platform_status
cache_writeback_extent(cache                   *cc,
                       uint64                   addr,
                       page_type                type,
                       cache_writeback_request *req)
{
   cache_writeback_request scratch;
   return cc->ops->extent_writeback(cc, addr, type, req ? req : &scratch);
}

/*
 *-----------------------------------------------------------------------------
 * cache_writeback_get_status
 *
 * Whether the write named by *req has completed. Never blocks; a caller
 * waiting for completion loops on this and cache_cleanup(), which reaps I/O
 * completions on the calling thread and is therefore what makes the loop
 * progress rather than merely spin.
 *
 * Completion is not durability: follow with cache_durable_barrier().
 *-----------------------------------------------------------------------------
 */
static inline cache_writeback_status
cache_writeback_get_status(cache *cc, const cache_writeback_request *req)
{
   return cc->ops->writeback_get_status(cc, req);
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
 * cache_writeback_dirty
 *
 * Issues and wait for completion of writebacks for all pages that are dirty
 * but not locked at the time of the call.  May writeback other pages, as well.
 *-----------------------------------------------------------------------------
 */
static inline platform_status
cache_writeback_dirty(cache *cc)
{
   return cc->ops->writeback_dirty(cc);
}

/*
 *-----------------------------------------------------------------------------
 * cache_durable_barrier
 *
 * Ensure that writeback completed before this call is durable across a power
 * loss. Callers normally use this after cache_writeback_dirty(), and again
 * after publishing a checkpoint superblock.
 *-----------------------------------------------------------------------------
 */
static inline platform_status
cache_durable_barrier(cache *cc)
{
   return cc->ops->durable_barrier(cc);
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
 * cache_in_use
 *
 * Returns TRUE if there is a reader or writer holding a reference
 * to the page at addr.
 *-----------------------------------------------------------------------------
 */
static inline bool32
cache_in_use(cache *cc, uint64 addr)
{
   return cc->ops->in_use(cc, addr);
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
