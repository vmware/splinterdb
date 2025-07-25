// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *-----------------------------------------------------------------------------
 * clockcache.c --
 *
 *     This file contains the implementation for a concurrent clock cache.
 *-----------------------------------------------------------------------------
 */
#include "platform.h"

#include "allocator.h"
#include "clockcache.h"
#include "io.h"
#include <stddef.h>
#include "util.h"

#include "poison.h"

/*
 *-----------------------------------------------------------------------------
 * Constants and Fixed Parameters
 *-----------------------------------------------------------------------------
 */

/* invalid "pointers" used to indicate that the given page or lookup is
 * unmapped
 */
#define CC_UNMAPPED_ENTRY UINT32_MAX
#define CC_UNMAPPED_ADDR  UINT64_MAX

// Number of entries to clean/evict/get_free in a per-thread batch
#define CC_ENTRIES_PER_BATCH 64

// Number of batches that the cleaner hand is ahead of the evictor hand
#define CC_CLEANER_GAP 512

/* number of events to poll for during clockcache_wait */
#define CC_DEFAULT_MAX_IO_EVENTS 1

/*
 *-----------------------------------------------------------------------------
 * Clockcache Operations Logging and Address Tracing
 *
 *      clockcache_log, etc. are used to write an output of cache operations to
 *      a log file for debugging purposes. If CC_LOG is set, then all output is
 *      written. If ADDR_TRACING is set, then only operations which affect
 *      entries with either entry_number TRACE_ENTRY or address TRACE_ADDR are
 *      written.
 *
 *      clockcache_log_stream should be called between platform_open_log_stream
 *      and platform_close_log_stream.
 *
 *      Note: these are debug functions, so calling platform_get_tid()
 *      potentially repeatedly is ok.
 *-----------------------------------------------------------------------------
 */

void
clockcache_print(platform_log_handle *log_handle, clockcache *cc);

#ifdef ADDR_TRACING
#   define clockcache_log(addr, entry, message, ...)                           \
      do {                                                                     \
         if (addr == TRACE_ADDR || entry == TRACE_ENTRY) {                     \
            platform_handle_log(cc->logfile,                                   \
                                "(%lu) " message,                              \
                                platform_get_tid(),                            \
                                ##__VA_ARGS__);                                \
         }                                                                     \
      } while (0)
#   define clockcache_log_stream(addr, entry, message, ...)                    \
      do {                                                                     \
         if (addr == TRACE_ADDR || entry == TRACE_ENTRY) {                     \
            platform_log_stream(                                               \
               "(%lu) " message, platform_get_tid(), ##__VA_ARGS__);           \
         }                                                                     \
      } while (0)
#else
#   ifdef CC_LOG
#      define clockcache_log(addr, entry, message, ...)                        \
         do {                                                                  \
            (void)(addr);                                                      \
            platform_handle_log(cc->logfile,                                   \
                                "(%lu) " message,                              \
                                platform_get_tid(),                            \
                                ##__VA_ARGS__);                                \
         } while (0)

#      define clockcache_log_stream(addr, entry, message, ...)                 \
         platform_log_stream(                                                  \
            "(%lu) " message, platform_get_tid(), ##__VA_ARGS__);
#   else
#      define clockcache_log(addr, entry, message, ...)                        \
         do {                                                                  \
            (void)(addr);                                                      \
            (void)(entry);                                                     \
            (void)(message);                                                   \
         } while (0)
#      define clockcache_log_stream(addr, entry, message, ...)                 \
         do {                                                                  \
            (void)(addr);                                                      \
            (void)(entry);                                                     \
            (void)(message);                                                   \
         } while (0)
#   endif
#endif

#if defined CC_LOG || defined ADDR_TRACING
#   define clockcache_open_log_stream() platform_open_log_stream()
#else
#   define clockcache_open_log_stream()
#endif

#if defined CC_LOG || defined ADDR_TRACING
#   define clockcache_close_log_stream() platform_close_log_stream(cc->logfile)
#else
#   define clockcache_close_log_stream()
#endif

/*
 *-----------------------------------------------------------------------------
 * clockcache_entry --
 *
 *     The meta data entry in the cache. Each entry has the underlying
 *     page_handle together with some flags.
 *-----------------------------------------------------------------------------
 */

/*
 *-----------------------------------------------------------------------------
 * Definitions for entry_status (clockcache_entry->status)
 *-----------------------------------------------------------------------------
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
 *-----------------------------------------------------------------------------
 * Clock cache Functions
 *-----------------------------------------------------------------------------
 */
/*-----------------------------------------------------------------------------
 * clockcache_{set/clear/test}_flag --
 *
 *      Atomically sets, clears or tests the given flag in the entry.
 *-----------------------------------------------------------------------------
 */

/* Validate entry_number, and return addr of clockcache_entry slot */
static inline clockcache_entry *
clockcache_get_entry(clockcache *cc, uint32 entry_number)
{
   debug_assert(entry_number < cc->cfg->page_capacity,
                "entry_number=%u is out-of-bounds. Should be < %d.",
                entry_number,
                cc->cfg->page_capacity);
   return (&cc->entry[entry_number]);
}

static inline entry_status
clockcache_get_status(clockcache *cc, uint32 entry_number)
{
   return clockcache_get_entry(cc, entry_number)->status;
}
static inline entry_status
clockcache_set_flag(clockcache *cc, uint32 entry_number, entry_status flag)
{
   return flag
          & __sync_fetch_and_or(&clockcache_get_entry(cc, entry_number)->status,
                                flag);
}

static inline uint32
clockcache_clear_flag(clockcache *cc, uint32 entry_number, entry_status flag)
{
   clockcache_entry *entry = clockcache_get_entry(cc, entry_number);
   return flag & __sync_fetch_and_and(&entry->status, ~flag);
}

static inline uint32
clockcache_test_flag(clockcache *cc, uint32 entry_number, entry_status flag)
{
   return flag & clockcache_get_status(cc, entry_number);
}

#ifdef RECORD_ACQUISITION_STACKS
static void
clockcache_record_backtrace(clockcache *cc, uint32 entry_number)
{
   // clang-format off
   int myhistindex = __sync_fetch_and_add(
                            &clockcache_get_entry(cc, entry_number)->next_history_record,
                            1);
   // clang-format on
   myhistindex = myhistindex % NUM_HISTORY_RECORDS;

   // entry_number is now known to be valid; offset into slot directly.
   clockcache_entry *myEntry = &cc->entry[entry_number];

   myEntry->history[myhistindex].status   = myEntry->status;
   myEntry->history[myhistindex].refcount = 0;
   for (threadid i = 0; i < MAX_THREADS; i++) {
      myEntry->history[myhistindex].refcount +=
         cc->refcount[i * cc->cfg->page_capacity + entry_number];
   }
   backtrace(myEntry->history[myhistindex].backtrace, NUM_HISTORY_RECORDS);
}
#else
#   define clockcache_record_backtrace(a, b)
#endif

/*
 *----------------------------------------------------------------------
 *
 * Utility functions
 *
 *----------------------------------------------------------------------
 */

static inline uint64
clockcache_config_page_size(const clockcache_config *cfg)
{
   return cfg->io_cfg->page_size;
}

static inline uint64
clockcache_config_extent_size(const clockcache_config *cfg)
{
   return cfg->io_cfg->extent_size;
}

static inline uint64
clockcache_multiply_by_page_size(const clockcache *cc, uint64 addr)
{
   return addr << cc->cfg->log_page_size;
}

static inline uint64
clockcache_divide_by_page_size(const clockcache *cc, uint64 addr)
{
   return addr >> cc->cfg->log_page_size;
}

static inline uint32
clockcache_lookup(const clockcache *cc, uint64 addr)
{
   uint64 lookup_no    = clockcache_divide_by_page_size(cc, addr);
   uint32 entry_number = cc->lookup[lookup_no];

   debug_assert(((entry_number < cc->cfg->page_capacity)
                 || (entry_number == CC_UNMAPPED_ENTRY)),
                "entry_number=%u is out-of-bounds. "
                " Should be either CC_UNMAPPED_ENTRY,"
                " or should be < %d.",
                entry_number,
                cc->cfg->page_capacity);
   return entry_number;
}

static inline clockcache_entry *
clockcache_lookup_entry(const clockcache *cc, uint64 addr)
{
   return &cc->entry[clockcache_lookup(cc, addr)];
}

static inline clockcache_entry *
clockcache_page_to_entry(const clockcache *cc, page_handle *page)
{
   return (clockcache_entry *)((char *)page - offsetof(clockcache_entry, page));
}

static inline uint32
clockcache_page_to_entry_number(const clockcache *cc, page_handle *page)
{
   return clockcache_page_to_entry(cc, page) - cc->entry;
}

static inline uint32
clockcache_data_to_entry_number(const clockcache *cc, char *data)
{
   return clockcache_divide_by_page_size(cc, data - cc->data);
}

debug_only static inline clockcache_entry *
clockcache_data_to_entry(const clockcache *cc, char *data)
{
   return &cc->entry[clockcache_data_to_entry_number(cc, data)];
}

static inline uint64
clockcache_page_size(const clockcache *cc)
{
   return clockcache_config_page_size(cc->cfg);
}

static inline uint64
clockcache_extent_size(const clockcache *cc)
{
   return clockcache_config_extent_size(cc->cfg);
}

/*
 *-----------------------------------------------------------------------------
 * clockcache_wait --
 *
 *      Does some work while waiting. Currently just polls for async IO
 *      completion.
 *
 *      This function needs to poll for async IO callback completion to avoid
 *      deadlock.
 *-----------------------------------------------------------------------------
 */
void
clockcache_wait(clockcache *cc)
{
   io_cleanup(cc->io, CC_DEFAULT_MAX_IO_EVENTS);
}


/*
 *-----------------------------------------------------------------------------
 * ref counts
 *
 *      Each entry has a distributed ref count. This ref count is striped
 *      across cache lines, so the ref count for entry 0 tid 0 is on a
 *      different cache line from both the ref count for entry 1 tid 0 and
 *      entry 0 tid 1. This reduces false sharing.
 *
 *      get_ref_internal converts an entry_number and tid to the index in
 *      cc->refcount where the ref count is stored.
 *-----------------------------------------------------------------------------
 */

static inline uint32
clockcache_get_ref_internal(clockcache *cc, uint32 entry_number)
{
   return entry_number % cc->cfg->cacheline_capacity * PLATFORM_CACHELINE_SIZE
          + entry_number / cc->cfg->cacheline_capacity;
}

static inline uint16
clockcache_get_ref(clockcache *cc, uint32 entry_number, uint64 counter_no)
{
   counter_no %= CC_RC_WIDTH;
   uint64 rc_number = clockcache_get_ref_internal(cc, entry_number);
   debug_assert(rc_number < cc->cfg->page_capacity);
   return cc->refcount[counter_no * cc->cfg->page_capacity + rc_number];
}

static inline void
clockcache_inc_ref(clockcache *cc, uint32 entry_number, threadid counter_no)
{
   counter_no %= CC_RC_WIDTH;
   uint64 rc_number = clockcache_get_ref_internal(cc, entry_number);
   debug_assert(rc_number < cc->cfg->page_capacity);

   debug_only uint16 refcount = __sync_fetch_and_add(
      &cc->refcount[counter_no * cc->cfg->page_capacity + rc_number], 1);
   debug_assert(refcount != MAX_READ_REFCOUNT);
}

static inline void
clockcache_dec_ref(clockcache *cc, uint32 entry_number, threadid counter_no)
{
   debug_only threadid input_counter_no = counter_no;

   counter_no %= CC_RC_WIDTH;
   uint64 rc_number = clockcache_get_ref_internal(cc, entry_number);
   debug_assert((rc_number < cc->cfg->page_capacity),
                "Entry number, %lu, is out of allocator "
                "page capacity range, %u.\n",
                rc_number,
                cc->cfg->page_capacity);

   debug_only uint16 refcount = __sync_fetch_and_sub(
      &cc->refcount[counter_no * cc->cfg->page_capacity + rc_number], 1);
   debug_assert((refcount != 0),
                "Invalid refcount, %u, after decrement."
                " input counter_no=%lu, rc_number=%lu, counter_no=%lu\n",
                refcount,
                input_counter_no,
                rc_number,
                counter_no);
}

static inline uint8
clockcache_get_pin(clockcache *cc, uint32 entry_number)
{
   uint64 rc_number = clockcache_get_ref_internal(cc, entry_number);
   debug_assert(rc_number < cc->cfg->page_capacity);
   return cc->pincount[rc_number];
}

static inline void
clockcache_inc_pin(clockcache *cc, uint32 entry_number)
{
   uint64 rc_number = clockcache_get_ref_internal(cc, entry_number);
   debug_assert(rc_number < cc->cfg->page_capacity);
   debug_only uint8 refcount =
      __sync_fetch_and_add(&cc->pincount[rc_number], 1);
   debug_assert(refcount != UINT8_MAX);
}

static inline void
clockcache_dec_pin(clockcache *cc, uint32 entry_number)
{
   uint64 rc_number = clockcache_get_ref_internal(cc, entry_number);
   debug_assert(rc_number < cc->cfg->page_capacity);
   debug_only uint8 refcount =
      __sync_fetch_and_sub(&cc->pincount[rc_number], 1);
   debug_assert(refcount != 0);
}

static inline void
clockcache_reset_pin(clockcache *cc, uint32 entry_number)
{
   uint64 rc_number = clockcache_get_ref_internal(cc, entry_number);
   debug_assert(rc_number < cc->cfg->page_capacity);
   if (cc->pincount[rc_number] != 0) {
      __sync_lock_test_and_set(&cc->pincount[rc_number], 0);
   }
}

void
clockcache_assert_no_refs(clockcache *cc)
{
   threadid        i;
   volatile uint32 j;
   for (i = 0; i < MAX_THREADS; i++) {
      for (j = 0; j < cc->cfg->page_capacity; j++) {
         if (clockcache_get_ref(cc, j, i) != 0) {
            clockcache_get_ref(cc, j, i);
         }
         platform_assert(clockcache_get_ref(cc, j, i) == 0);
      }
   }
}

void
clockcache_assert_no_refs_and_pins(clockcache *cc)
{
   threadid i;
   uint32   j;
   for (i = 0; i < MAX_THREADS; i++) {
      for (j = 0; j < cc->cfg->page_capacity; j++) {
         platform_assert(clockcache_get_ref(cc, j, i) == 0);
      }
   }
}

void
clockcache_assert_no_locks_held(clockcache *cc)
{
   uint64 i;
   clockcache_assert_no_refs_and_pins(cc);
   for (i = 0; i < cc->cfg->page_capacity; i++) {
      debug_assert(!clockcache_test_flag(cc, i, CC_WRITELOCKED));
   }
}

bool32
clockcache_assert_clean(clockcache *cc)
{
   uint64 i;
   for (i = 0; (i < cc->cfg->page_capacity)
               && (clockcache_test_flag(cc, i, CC_FREE)
                   || clockcache_test_flag(cc, i, CC_CLEAN));
        i++)
   { /* Do nothing */
   }
   return (i == cc->cfg->page_capacity);
}

/*
 *----------------------------------------------------------------------
 *
 * page locking functions
 *
 *----------------------------------------------------------------------
 */

typedef enum {
   GET_RC_SUCCESS = 0,
   GET_RC_CONFLICT,
   GET_RC_EVICTED,
   GET_RC_FLUSHING,
} get_rc;

/*
 *----------------------------------------------------------------------
 * clockcache_try_get_read
 *
 *      returns:
 *      - GET_RC_SUCCESS if a read lock was obtained
 *      - GET_RC_EVICTED if the entry was evicted
 *      - GET_RC_CONFLICT if another thread holds a write lock
 *
 *      does not block
 *----------------------------------------------------------------------
 */
static get_rc
clockcache_try_get_read(clockcache *cc, uint32 entry_number, bool32 set_access)
{
   const threadid tid = platform_get_tid();

   // first check if write lock is held
   uint32 cc_writing = clockcache_test_flag(cc, entry_number, CC_WRITELOCKED);
   if (UNLIKELY(cc_writing)) {
      return GET_RC_CONFLICT;
   }

   // then obtain the read lock
   clockcache_inc_ref(cc, entry_number, tid);

   // clockcache_test_flag returns 32 bits, not 1 (cannot use bool)
   uint32 cc_free = clockcache_test_flag(cc, entry_number, CC_FREE);
   cc_writing     = clockcache_test_flag(cc, entry_number, CC_WRITELOCKED);
   if (LIKELY(!cc_free && !cc_writing)) {
      // test and test and set to reduce contention
      if (set_access && !clockcache_test_flag(cc, entry_number, CC_ACCESSED)) {
         clockcache_set_flag(cc, entry_number, CC_ACCESSED);
      }
      clockcache_record_backtrace(cc, entry_number);
      return GET_RC_SUCCESS;
   }

   // cannot hold the read lock (either write lock is held or entry has been
   // evicted), dec ref and return
   clockcache_dec_ref(cc, entry_number, tid);

   if (cc_free) {
      return GET_RC_EVICTED;
   }

   // must be cc_writing
   debug_assert(cc_writing);
   return GET_RC_CONFLICT;
}

/*
 *----------------------------------------------------------------------
 * clockcache_get_read
 *
 *      returns:
 *      - GET_RC_SUCCESS if a read lock was obtained
 *      - GET_RC_EVICTED if the entry was evicted
 *
 *      blocks if another thread holds a write lock
 *----------------------------------------------------------------------
 */
static get_rc
clockcache_get_read(clockcache *cc, uint32 entry_number)
{
   get_rc rc = clockcache_try_get_read(cc, entry_number, TRUE);

   uint64 wait = 1;
   while (rc == GET_RC_CONFLICT) {
      platform_sleep_ns(wait);
      wait = wait > 1024 ? wait : 2 * wait;
      rc   = clockcache_try_get_read(cc, entry_number, TRUE);
   }

   return rc;
}

/*
 *----------------------------------------------------------------------
 * clockcache_try_get_claim
 *
 *      Attempts to upgrade a read lock to claim.
 *
 *      NOTE: A caller must release the read lock on GET_RC_CONFLICT before
 *      attempting try_get_claim again to avoid deadlock.
 *
 *      returns:
 *      - GET_RC_SUCCESS if a claim was obtained
 *      - GET_RC_CONFLICT if another thread holds a claim (or write lock)
 *
 *      does not block
 *----------------------------------------------------------------------
 */
static get_rc
clockcache_try_get_claim(clockcache *cc, uint32 entry_number)
{
   clockcache_log(0,
                  entry_number,
                  "try_get_claim: entry_number %u claimed: %u\n",
                  entry_number,
                  clockcache_test_flag(cc, entry_number, CC_CLAIMED));

   if (clockcache_set_flag(cc, entry_number, CC_CLAIMED)) {
      clockcache_log(0, entry_number, "return false\n", NULL);
      return GET_RC_CONFLICT;
   }

   clockcache_record_backtrace(cc, entry_number);

   return GET_RC_SUCCESS;
}

/*
 *----------------------------------------------------------------------
 * clockcache_get_write
 *
 *      Upgrades a claim to a write lock.
 *
 *      blocks:
 *      - while read locks are released
 *      - while write back completes
 *
 *      cannot fail
 *
 *      Note: does not wait on CC_LOADING. Caller must either ensure that
 *      CC_LOADING is not set prior to calling (e.g. via a prior call to
 *      clockcache_get).
 *----------------------------------------------------------------------
 */
static void
clockcache_get_write(clockcache *cc, uint32 entry_number)
{
   const threadid tid = platform_get_tid();

   debug_assert(clockcache_test_flag(cc, entry_number, CC_CLAIMED));
   debug_only uint32 was_writing =
      clockcache_set_flag(cc, entry_number, CC_WRITELOCKED);
   debug_assert(!was_writing);
   debug_assert(!clockcache_test_flag(cc, entry_number, CC_LOADING));

   /*
    * If the thread that wants a write lock holds > 1 refs, it means
    * it has some async lookups which have yielded after taking refs.
    * This is currently not allowed; because such a thread would
    * easily be able to upgrade to write lock and modify the page
    * under it's own yielded lookup.
    *
    * If threads do async lookups, they must leave the
    * compaction+incorporation (that needs write locking) to
    * background threads.
    */
   debug_assert(clockcache_get_ref(cc, entry_number, tid) >= 1);
   // Wait for flushing to finish
   while (clockcache_test_flag(cc, entry_number, CC_WRITEBACK)) {
      clockcache_wait(cc);
   }

   // Wait for readers to finish
   for (threadid thr_i = 0; thr_i < CC_RC_WIDTH; thr_i++) {
      if (tid % CC_RC_WIDTH != thr_i) {
         while (clockcache_get_ref(cc, entry_number, thr_i)) {
            platform_sleep_ns(1);
         }
      } else {
         // we have a single ref, so wait for others to drop
         while (clockcache_get_ref(cc, entry_number, thr_i) > 1) {
            platform_sleep_ns(1);
         }
      }
   }

   clockcache_record_backtrace(cc, entry_number);
}

/*
 *----------------------------------------------------------------------
 * clockcache_try_get_write
 *
 *      Attempts to upgrade a claim to a write lock.
 *
 *      returns:
 *      - GET_RC_SUCCESS if the write lock was obtained
 *      - GET_RC_CONFLICT if another thread holds a read lock
 *
 *      blocks on write back
 *
 *      Note: does not wait on CC_LOADING. Caller must either ensure that
 *      CC_LOADING is not set prior to calling (e.g. via a prior call to
 *      clockcache_get).
 *----------------------------------------------------------------------
 */
static get_rc
clockcache_try_get_write(clockcache *cc, uint32 entry_number)
{
   threadid thr_i;
   threadid tid = platform_get_tid();
   get_rc   rc;

   debug_assert(clockcache_test_flag(cc, entry_number, CC_CLAIMED));
   debug_only uint32 was_writing =
      clockcache_set_flag(cc, entry_number, CC_WRITELOCKED);
   debug_assert(!was_writing);
   debug_assert(!clockcache_test_flag(cc, entry_number, CC_LOADING));

   // if flushing, then bail
   if (clockcache_test_flag(cc, entry_number, CC_WRITEBACK)) {
      rc = GET_RC_FLUSHING;
      goto failed;
   }

   // check for readers
   for (thr_i = 0; thr_i < CC_RC_WIDTH; thr_i++) {
      if (tid % CC_RC_WIDTH != thr_i) {
         if (clockcache_get_ref(cc, entry_number, thr_i)) {
            // there is a reader, so bail
            rc = GET_RC_CONFLICT;
            goto failed;
         }
      } else {
         // we have a single ref, so if > 1 bail
         if (clockcache_get_ref(cc, entry_number, thr_i) > 1) {
            // there is a reader, so bail
            rc = GET_RC_CONFLICT;
            goto failed;
         }
      }
   }

   clockcache_record_backtrace(cc, entry_number);

   return GET_RC_SUCCESS;

failed:
   was_writing = clockcache_clear_flag(cc, entry_number, CC_WRITELOCKED);
   debug_assert(was_writing);
   return rc;
}

/*
 *----------------------------------------------------------------------
 *
 * writeback functions
 *
 *----------------------------------------------------------------------
 */

/*
 *----------------------------------------------------------------------
 * clockcache_ok_to_writeback
 *
 *      Tests the entry to see if write back is possible. Used for test and
 *      test and set.
 *----------------------------------------------------------------------
 */
static inline bool32
clockcache_ok_to_writeback(clockcache *cc,
                           uint32      entry_number,
                           bool32      with_access)
{
   uint32 status = clockcache_get_status(cc, entry_number);
   return ((status == CC_CLEANABLE1_STATUS)
           || (with_access && status == CC_CLEANABLE2_STATUS));
}

/*
 *----------------------------------------------------------------------
 * clockcache_try_set_writeback
 *
 *      Atomically sets the CC_WRITEBACK flag if the status permits; current
 *      status must be:
 *         -- CC_CLEANABLE1_STATUS (= 0)                  // dirty
 *         -- CC_CLEANABLE2_STATUS (= 0 | CC_ACCESSED)    // dirty
 *----------------------------------------------------------------------
 */
static inline bool32
clockcache_try_set_writeback(clockcache *cc,
                             uint32      entry_number,
                             bool32      with_access)
{
   // Validate first, as we need access to volatile status * below.
   debug_assert(entry_number < cc->cfg->page_capacity,
                "entry_number=%u is out-of-bounds. Should be < %d.",
                entry_number,
                cc->cfg->page_capacity);

   platform_assert(cc->entry[entry_number].waiters.head == NULL);

   volatile uint32 *status = &cc->entry[entry_number].status;
   if (__sync_bool_compare_and_swap(
          status, CC_CLEANABLE1_STATUS, CC_WRITEBACK1_STATUS))
   {
      return TRUE;
   }

   if (with_access
       && __sync_bool_compare_and_swap(
          status, CC_CLEANABLE2_STATUS, CC_WRITEBACK2_STATUS))
   {
      return TRUE;
   }
   return FALSE;
}

typedef struct async_io_state {
   clockcache           *cc;
   uint64               *outstanding_pages;
   io_async_state_buffer iostate;
} async_io_state;

static void
clockcache_write_callback(void *wbs)
{
   async_io_state *state = (async_io_state *)wbs;
   clockcache     *cc    = state->cc;

   if (io_async_run(state->iostate) != ASYNC_STATUS_DONE) {
      return;
   }

   platform_assert_status_ok(io_async_state_get_result(state->iostate));

   const struct iovec *iovec;
   uint64              count;
   iovec = io_async_state_get_iovec(state->iostate, &count);

   platform_assert(count > 0);
   platform_assert(count <= cc->cfg->pages_per_extent);


   uint64            i;
   uint32            entry_number;
   clockcache_entry *entry;
   uint64            addr;
   debug_only uint32 debug_status;

   for (i = 0; i < count; i++) {
      entry_number =
         clockcache_data_to_entry_number(cc, (char *)iovec[i].iov_base);
      entry = clockcache_get_entry(cc, entry_number);
      addr  = entry->page.disk_addr;

      clockcache_log(addr,
                     entry_number,
                     "write_callback i %lu entry %u addr %lu\n",
                     i,
                     entry_number,
                     addr);

      debug_status = clockcache_set_flag(cc, entry_number, CC_CLEAN);
      debug_assert(!debug_status);
      debug_status = clockcache_clear_flag(cc, entry_number, CC_WRITEBACK);
      debug_assert(debug_status);
   }

   if (state->outstanding_pages) {
      __sync_fetch_and_sub(state->outstanding_pages, count);
   }

   io_async_state_deinit(state->iostate);
   platform_free(cc->heap_id, state);
}


/*
 *----------------------------------------------------------------------
 * clockcache_batch_start_writeback --
 *
 *      Iterates through all pages in the batch and issues writeback for any
 *      which are cleanable.
 *
 *      Where possible, the write is extended to the extent, including pages
 *      outside the batch.
 *
 *      If is_urgent is set, pages with CC_ACCESSED are written back, otherwise
 *      they are not.
 *----------------------------------------------------------------------
 */
void
clockcache_batch_start_writeback(clockcache *cc, uint64 batch, bool32 is_urgent)
{
   uint32         entry_no, next_entry_no;
   uint64         addr, first_addr, end_addr, i;
   const threadid tid            = platform_get_tid();
   uint64         start_entry_no = batch * CC_ENTRIES_PER_BATCH;
   uint64         end_entry_no   = start_entry_no + CC_ENTRIES_PER_BATCH;

   clockcache_entry *entry, *next_entry;

   debug_assert((tid < MAX_THREADS), "Invalid tid=%lu\n", tid);
   debug_assert(cc != NULL);
   debug_assert(batch < cc->cfg->page_capacity / CC_ENTRIES_PER_BATCH);

   clockcache_open_log_stream();
   clockcache_log_stream(0,
                         0,
                         "batch_start_writeback: %lu, entries %lu-%lu\n",
                         batch,
                         start_entry_no,
                         end_entry_no - 1);

   uint64 page_size = clockcache_page_size(cc);

   allocator_config *allocator_cfg = allocator_get_config(cc->al);
   // Iterate through the entries in the batch and try to write out the extents.
   for (entry_no = start_entry_no; entry_no < end_entry_no; entry_no++) {
      entry = &cc->entry[entry_no];
      addr  = entry->page.disk_addr;
      // test and test and set in the if condition
      if (clockcache_ok_to_writeback(cc, entry_no, is_urgent)
          && clockcache_try_set_writeback(cc, entry_no, is_urgent))
      {
         debug_assert(clockcache_lookup(cc, addr) == entry_no);
         first_addr = entry->page.disk_addr;
         // walk backwards through extent to find first cleanable entry
         do {
            first_addr -= page_size;
            if (allocator_config_pages_share_extent(
                   allocator_cfg, first_addr, addr))
               next_entry_no = clockcache_lookup(cc, first_addr);
            else
               next_entry_no = CC_UNMAPPED_ENTRY;
         } while (
            next_entry_no != CC_UNMAPPED_ENTRY
            && clockcache_try_set_writeback(cc, next_entry_no, is_urgent));
         first_addr += page_size;
         end_addr = entry->page.disk_addr;
         // walk forwards through extent to find last cleanable entry
         do {
            end_addr += page_size;
            if (allocator_config_pages_share_extent(
                   allocator_cfg, end_addr, addr))
               next_entry_no = clockcache_lookup(cc, end_addr);
            else
               next_entry_no = CC_UNMAPPED_ENTRY;
         } while (
            next_entry_no != CC_UNMAPPED_ENTRY
            && clockcache_try_set_writeback(cc, next_entry_no, is_urgent));


         async_io_state *state;
         while ((state = TYPED_MALLOC(cc->heap_id, state)) == NULL) {
            clockcache_wait(cc);
         }

         state->cc                = cc;
         state->outstanding_pages = NULL;
         io_async_state_init(state->iostate,
                             cc->io,
                             io_async_pwritev,
                             first_addr,
                             clockcache_write_callback,
                             state);

         uint64 req_count =
            clockcache_divide_by_page_size(cc, end_addr - first_addr);

         if (cc->cfg->use_stats) {
            cc->stats[tid].page_writes[entry->type] += req_count;
            cc->stats[tid].writes_issued++;
         }

         for (i = 0; i < req_count; i++) {
            addr       = first_addr + clockcache_multiply_by_page_size(cc, i);
            next_entry = clockcache_lookup_entry(cc, addr);
            next_entry_no = clockcache_lookup(cc, addr);

            clockcache_log_stream(addr,
                                  next_entry_no,
                                  "flush: entry %u addr %lu\n",
                                  next_entry_no,
                                  addr);
            io_async_state_append_page(state->iostate, next_entry->page.data);
         }

         io_async_run(state->iostate);
      }
   }
   clockcache_close_log_stream();
}

/*
 *----------------------------------------------------------------------
 *
 * eviction functions
 *
 *----------------------------------------------------------------------
 */

/*
 *----------------------------------------------------------------------
 * clockcache_try_evict
 *
 *      Attempts to evict the page if it is evictable
 *----------------------------------------------------------------------
 */
static void
clockcache_try_evict(clockcache *cc, uint32 entry_number)
{
   clockcache_entry *entry = clockcache_get_entry(cc, entry_number);
   const threadid    tid   = platform_get_tid();

   /* store status for testing, then clear CC_ACCESSED */
   uint32 status = entry->status;
   /* T&T&S */
   if (clockcache_test_flag(cc, entry_number, CC_ACCESSED)) {
      clockcache_clear_flag(cc, entry_number, CC_ACCESSED);
   }

   /*
    * perform fast tests and quit if they fail */
   /* Note: this implicitly tests for:
    * CC_ACCESSED, CC_CLAIMED, CC_WRITELOCK, CC_WRITEBACK
    * Note: here is where we check that the evicting thread doesn't hold a read
    * lock itself.
    */
   if (status != CC_EVICTABLE_STATUS
       || clockcache_get_ref(cc, entry_number, tid)
       || clockcache_get_pin(cc, entry_number))
   {
      goto out;
   }

   /* try to evict:
    * 1. try to read lock
    * 2. try to claim
    * 3. try to write lock
    * 4. verify still evictable
    * 5. clear lookup, disk_addr
    * 6. set status to CC_FREE_STATUS (clears claim and write lock)
    * 7. release read lock */

   /* 1. try to read lock */
   if (clockcache_try_get_read(cc, entry_number, FALSE) != GET_RC_SUCCESS) {
      goto out;
   }

   /* 2. try to claim */
   if (clockcache_try_get_claim(cc, entry_number) != GET_RC_SUCCESS) {
      goto release_ref;
   }

   /*
    * 3. try to write lock
    *      -- first check if loading
    */
   if (clockcache_test_flag(cc, entry_number, CC_LOADING)
       || clockcache_try_get_write(cc, entry_number) != GET_RC_SUCCESS)
   {
      goto release_claim;
   }

   /* 4. verify still evictable
    * redo fast tests in case another thread has changed the status before we
    * obtained the lock
    * note: do not re-check the ref count for the active thread, because
    * it acquired a read lock in order to lock the entry.
    */
   status = entry->status;
   if (status != CC_LOCKED_EVICTABLE_STATUS
       || clockcache_get_pin(cc, entry_number))
   {
      goto release_write;
   }

   /* 5. clear lookup, disk addr */
   uint64 addr = entry->page.disk_addr;
   if (addr != CC_UNMAPPED_ADDR) {
      uint64 lookup_no      = clockcache_divide_by_page_size(cc, addr);
      cc->lookup[lookup_no] = CC_UNMAPPED_ENTRY;
      entry->page.disk_addr = CC_UNMAPPED_ADDR;
   }
   debug_only uint32 debug_status =
      clockcache_test_flag(cc, entry_number, CC_WRITELOCKED | CC_CLAIMED);
   debug_assert(debug_status);

   /* 6. set status to CC_FREE_STATUS (clears claim and write lock) */
   platform_assert(entry->waiters.head == NULL);
   entry->type   = PAGE_TYPE_INVALID;
   entry->status = CC_FREE_STATUS;
   clockcache_log(
      addr, entry_number, "evict: entry %u addr %lu\n", entry_number, addr);

   /* 7. release read lock */
   goto release_ref;

release_write:
   debug_status = clockcache_clear_flag(cc, entry_number, CC_WRITELOCKED);
   debug_assert(debug_status);
release_claim:
   debug_status = clockcache_clear_flag(cc, entry_number, CC_CLAIMED);
   debug_assert(debug_status);
release_ref:
   clockcache_dec_ref(cc, entry_number, tid);
out:
   return;
}

/*
 *----------------------------------------------------------------------
 * clockcache_evict_batch --
 *
 *      Evicts all evictable pages in the batch.
 *----------------------------------------------------------------------
 */
void
clockcache_evict_batch(clockcache *cc, uint32 batch)
{
   debug_assert(cc != NULL);
   debug_assert(batch < cc->cfg->page_capacity / CC_ENTRIES_PER_BATCH);

   uint32 start_entry_no = batch * CC_ENTRIES_PER_BATCH;
   uint32 end_entry_no   = start_entry_no + CC_ENTRIES_PER_BATCH;

   clockcache_log(0,
                  0,
                  "evict_batch: %u, entries %u-%u\n",
                  batch,
                  start_entry_no,
                  end_entry_no - 1);

   for (uint32 entry_no = start_entry_no; entry_no < end_entry_no; entry_no++) {
      clockcache_try_evict(cc, entry_no);
   }
}

/*
 *----------------------------------------------------------------------
 * clockcache_move_hand --
 *
 *      Moves the clock hand forward cleaning and evicting a batch. Cleans
 *      "accessed" pages if is_urgent is set, for example when get_free_page
 *      has cycled through the cache already.
 *----------------------------------------------------------------------
 */
void
clockcache_move_hand(clockcache *cc, bool32 is_urgent)
{
   const threadid   tid = platform_get_tid();
   volatile bool32 *evict_batch_busy;
   volatile bool32 *clean_batch_busy;
   uint64           cleaner_hand;

   /* move the hand a batch forward */
   uint64            evict_hand = cc->per_thread[tid].free_hand;
   debug_only bool32 was_busy   = TRUE;
   if (evict_hand != CC_UNMAPPED_ENTRY) {
      evict_batch_busy = &cc->batch_busy[evict_hand];
      was_busy = __sync_bool_compare_and_swap(evict_batch_busy, TRUE, FALSE);
      debug_assert(was_busy);
   }
   do {
      evict_hand =
         __sync_add_and_fetch(&cc->evict_hand, 1) % cc->cfg->batch_capacity;
      evict_batch_busy = &cc->batch_busy[evict_hand];
      // clean the batch ahead
      cleaner_hand = (evict_hand + cc->cleaner_gap) % cc->cfg->batch_capacity;
      clean_batch_busy = &cc->batch_busy[cleaner_hand];
      if (__sync_bool_compare_and_swap(clean_batch_busy, FALSE, TRUE)) {
         clockcache_batch_start_writeback(cc, cleaner_hand, is_urgent);
         was_busy = __sync_bool_compare_and_swap(clean_batch_busy, TRUE, FALSE);
         debug_assert(was_busy);
      }
   } while (!__sync_bool_compare_and_swap(evict_batch_busy, FALSE, TRUE));

   clockcache_evict_batch(cc, evict_hand % cc->cfg->batch_capacity);
   cc->per_thread[tid].free_hand = evict_hand % cc->cfg->batch_capacity;
}


/*
 *----------------------------------------------------------------------
 * clockcache_get_free_page --
 *
 *      returns a free page with given status and ref count.
 *----------------------------------------------------------------------
 */
uint32
clockcache_get_free_page(clockcache *cc,
                         uint32      status,
                         page_type   type,
                         bool32      refcount,
                         bool32      blocking)
{
   uint32            entry_no;
   uint64            num_passes = 0;
   const threadid    tid        = platform_get_tid();
   uint64            max_hand   = cc->per_thread[tid].free_hand;
   clockcache_entry *entry;
   timestamp         wait_start;

   debug_assert((tid < MAX_THREADS), "Invalid tid=%lu\n", tid);
   if (cc->per_thread[tid].free_hand == CC_UNMAPPED_ENTRY) {
      clockcache_move_hand(cc, FALSE);
   }

   /*
    * Debug builds can run on very high latency storage eg. Nimbus. Do
    * not give up after 3 passes on the cache. At least wait for the
    * max latency of an IO and keep making passes.
    */
   while (num_passes < 3
          || (blocking && !io_max_latency_elapsed(cc->io, wait_start)))
   {
      uint64 start_entry = cc->per_thread[tid].free_hand * CC_ENTRIES_PER_BATCH;
      uint64 end_entry   = start_entry + CC_ENTRIES_PER_BATCH;
      for (entry_no = start_entry; entry_no < end_entry; entry_no++) {
         entry = &cc->entry[entry_no];
         if (entry->status == CC_FREE_STATUS
             && __sync_bool_compare_and_swap(
                &entry->status, CC_FREE_STATUS, CC_ALLOC_STATUS))
         {
            if (refcount) {
               clockcache_inc_ref(cc, entry_no, tid);
            }
            platform_assert(entry->waiters.head == NULL);
            entry->status = status;
            entry->type   = type;
            debug_assert(entry->page.disk_addr == CC_UNMAPPED_ADDR);
            clockcache_record_backtrace(cc, entry_no);
            return entry_no;
         }
      }

      clockcache_move_hand(cc, num_passes != 0);
      if (cc->per_thread[tid].free_hand < max_hand) {
         num_passes++;
         /*
          * The first pass doesn't really have a fair chance at having
          * looked at the entire cache, still it's ok to start
          * reckoning start time for max latency. Since it runs into
          * seconds, we'll make another complete pass in a tiny
          * fraction of the max latency.
          */
         if (num_passes == 1) {
            wait_start = platform_get_timestamp();
         } else {
            platform_yield();
         }
         clockcache_wait(cc);
      }
      max_hand = cc->per_thread[tid].free_hand;
   }
   if (blocking) {
      platform_default_log("cache locked (num_passes=%lu time=%lu nsecs)\n",
                           num_passes,
                           platform_timestamp_elapsed(wait_start));
      clockcache_print(Platform_default_log_handle, cc);
      platform_assert(0);
   }

   return CC_UNMAPPED_ENTRY;
}
/*
 *-----------------------------------------------------------------------------
 * clockcache_flush --
 *
 *      Issues writeback for all page in the cache.
 *
 *      Asserts that there are no pins, read locks, claims or write locks.
 *-----------------------------------------------------------------------------
 */
void
clockcache_flush(clockcache *cc)
{
   // make sure all aio is complete first
   io_wait_all(cc->io);

   // there can be no references or pins or things won't flush
   // clockcache_assert_no_locks_held(cc); // take out for performance

   // clean all the pages
   for (uint32 flush_hand = 0;
        flush_hand < cc->cfg->page_capacity / CC_ENTRIES_PER_BATCH;
        flush_hand++)
   {
      clockcache_batch_start_writeback(cc, flush_hand, TRUE);
   }

   // make sure all aio is complete again
   io_wait_all(cc->io);

   debug_assert(clockcache_assert_clean(cc));
}

/*
 *-----------------------------------------------------------------------------
 * clockcache_evict_all --
 *
 *      evicts all the pages.
 *-----------------------------------------------------------------------------
 */
int
clockcache_evict_all(clockcache *cc, bool32 ignore_pinned_pages)
{
   uint32 evict_hand;
   uint32 i;

   if (!ignore_pinned_pages) {
      // there can be no references or pins or locks or it will block eviction
      clockcache_assert_no_locks_held(cc); // take out for performance
   }

   // evict all the pages
   for (evict_hand = 0; evict_hand < cc->cfg->batch_capacity; evict_hand++) {
      clockcache_evict_batch(cc, evict_hand);
      // Do it again for access bits
      clockcache_evict_batch(cc, evict_hand);
   }

   for (i = 0; i < cc->cfg->page_capacity; i++) {
      debug_only uint32 entry_no =
         clockcache_page_to_entry_number(cc, &cc->entry->page);
      // Every page should either be evicted or pinned.
      debug_assert(
         cc->entry[i].status == CC_FREE_STATUS
         || (ignore_pinned_pages && clockcache_get_pin(cc, entry_no)));
   }

   return 0;
}

/*
 *----------------------------------------------------------------------
 * clockcache_alloc --
 *
 *      Given a disk_addr, allocate entry in the cache and return its page with
 *      a write lock.
 *----------------------------------------------------------------------
 */
page_handle *
clockcache_alloc(clockcache *cc, uint64 addr, page_type type)
{
   uint32            entry_no = clockcache_get_free_page(cc,
                                              CC_ALLOC_STATUS,
                                              type,
                                              TRUE,  // refcount
                                              TRUE); // blocking
   clockcache_entry *entry    = &cc->entry[entry_no];
   entry->page.disk_addr      = addr;
   entry->type                = type;
   uint64 lookup_no = clockcache_divide_by_page_size(cc, entry->page.disk_addr);
   // bool32 rc        = __sync_bool_compare_and_swap(
   //    &cc->lookup[lookup_no], CC_UNMAPPED_ENTRY, entry_no);
   // platform_assert(rc);
   cc->lookup[lookup_no] = entry_no;
   clockcache_record_backtrace(cc, entry_no);

   clockcache_log(entry->page.disk_addr,
                  entry_no,
                  "alloc: entry %u addr %lu\n",
                  entry_no,
                  entry->page.disk_addr);
   return &entry->page;
}

/*
 *----------------------------------------------------------------------
 * clockcache_try_page_discard --
 *
 *      Evicts the page with address addr if it is in cache.
 *----------------------------------------------------------------------
 */
void
clockcache_try_page_discard(clockcache *cc, uint64 addr)
{
   const threadid tid = platform_get_tid();
   while (TRUE) {
      uint32 entry_number = clockcache_lookup(cc, addr);
      if (entry_number == CC_UNMAPPED_ENTRY) {
         clockcache_log(addr,
                        entry_number,
                        "try_discard_page (uncached): entry %u addr %lu\n",
                        entry_number,
                        addr);
         return;
      }

      /*
       * in cache, so evict:
       * 1. read lock
       * 2. wait for loading
       * 3. claim
       * 4. write lock
       * 5. clear lookup, disk_addr
       * 6. set status to CC_FREE_STATUS (clears claim and write lock)
       * 7. reset pincount to zero
       * 8. release read lock
       */

      // platform_assert(clockcache_get_ref(cc, entry_number, tid) == 0);

      /* 1. read lock */
      if (clockcache_get_read(cc, entry_number) == GET_RC_EVICTED) {
         // raced with eviction, try again
         continue;
      }

      /* 2. wait for loading */
      while (clockcache_test_flag(cc, entry_number, CC_LOADING)) {
         clockcache_wait(cc);
      }

      clockcache_entry *entry = clockcache_get_entry(cc, entry_number);

      if (entry->page.disk_addr != addr) {
         // raced with eviction, try again
         clockcache_dec_ref(cc, entry_number, tid);
         continue;
      }

      /* 3. claim */
      if (clockcache_try_get_claim(cc, entry_number) != GET_RC_SUCCESS) {
         // failed to get claim, try again
         clockcache_dec_ref(cc, entry_number, tid);
         continue;
      }

      /* log only after steps that can fail */
      clockcache_log(addr,
                     entry_number,
                     "try_discard_page (cached): entry %u addr %lu\n",
                     entry_number,
                     addr);

      /* 4. write lock */
      clockcache_get_write(cc, entry_number);

      /* 5. clear lookup and disk addr; set status to CC_FREE_STATUS */
      uint64 lookup_no      = clockcache_divide_by_page_size(cc, addr);
      cc->lookup[lookup_no] = CC_UNMAPPED_ENTRY;
      debug_assert(entry->page.disk_addr == addr);
      entry->page.disk_addr = CC_UNMAPPED_ADDR;

      /* 6. set status to CC_FREE_STATUS (clears claim and write lock) */
      platform_assert(entry->waiters.head == NULL);
      entry->type   = PAGE_TYPE_INVALID;
      entry->status = CC_FREE_STATUS;

      /* 7. reset pincount */
      clockcache_reset_pin(cc, entry_number);

      /* 8. release read lock */
      clockcache_dec_ref(cc, entry_number, tid);
      return;
   }
}

/*
 *----------------------------------------------------------------------
 * clockcache_extent_discard --
 *
 *      Attempts to evict all the pages in the extent. Will wait for writeback,
 *      but will evict and discard dirty pages.
 *----------------------------------------------------------------------
 */
void
clockcache_extent_discard(clockcache *cc, uint64 addr, page_type type)
{
   debug_assert(addr % clockcache_extent_size(cc) == 0);
   debug_assert(allocator_get_refcount(cc->al, addr) == AL_NO_REFS);

   clockcache_log(addr, 0, "hard evict extent: addr %lu\n", addr);
   for (uint64 i = 0; i < cc->cfg->pages_per_extent; i++) {
      uint64 page_addr = addr + clockcache_multiply_by_page_size(cc, i);
      clockcache_try_page_discard(cc, page_addr);
   }
}

/*
 * Get addr if addr is at entry_number.  Returns TRUE if successful.
 */
static bool32
clockcache_get_in_cache(clockcache   *cc,           // IN
                        uint64        addr,         // IN
                        bool32        blocking,     // IN
                        page_type     type,         // IN
                        uint32        entry_number, // IN
                        page_handle **page)         // OUT
{
   threadid tid = platform_get_tid();

   if (blocking) {
      if (clockcache_get_read(cc, entry_number) != GET_RC_SUCCESS) {
         // this means we raced with eviction, start over
         clockcache_log(addr,
                        entry_number,
                        "get (eviction race): entry %u addr %lu\n",
                        entry_number,
                        addr);
         return TRUE;
      }
      if (clockcache_get_entry(cc, entry_number)->page.disk_addr != addr) {
         // this also means we raced with eviction and really lost
         clockcache_dec_ref(cc, entry_number, tid);
         return TRUE;
      }
   } else {
      switch (clockcache_try_get_read(cc, entry_number, TRUE)) {
         case GET_RC_CONFLICT:
            clockcache_log(addr,
                           entry_number,
                           "get (locked -- non-blocking): entry %u addr %lu\n",
                           entry_number,
                           addr);
            *page = NULL;
            return FALSE;
         case GET_RC_EVICTED:
            clockcache_log(addr,
                           entry_number,
                           "get (eviction race): entry %u addr %lu\n",
                           entry_number,
                           addr);
            return TRUE;
         case GET_RC_SUCCESS:
            if (clockcache_get_entry(cc, entry_number)->page.disk_addr != addr)
            {
               // this also means we raced with eviction and really lost
               clockcache_dec_ref(cc, entry_number, tid);
               return TRUE;
            }
            break;
         default:
            platform_assert(0);
      }
   }

   while (clockcache_test_flag(cc, entry_number, CC_LOADING)) {
      clockcache_wait(cc);
   }
   clockcache_entry *entry = clockcache_get_entry(cc, entry_number);

   platform_assert(entry->type == type,
                   "entry %u type %d != %d",
                   entry_number,
                   entry->type,
                   type);

   if (cc->cfg->use_stats) {
      cc->stats[tid].cache_hits[type]++;
   }
   clockcache_log(addr,
                  entry_number,
                  "get (cached): entry %u addr %lu rc %u\n",
                  entry_number,
                  addr,
                  clockcache_get_ref(cc, entry_number, tid));
   *page = &entry->page;
   return FALSE;
}

static uint64
clockcache_acquire_entry_for_load(clockcache *cc, // IN
                                  uint64      addr,
                                  page_type   type) // OUT
{
   threadid          tid          = platform_get_tid();
   uint64            lookup_no    = clockcache_divide_by_page_size(cc, addr);
   uint32            entry_number = clockcache_get_free_page(cc,
                                                  CC_READ_LOADING_STATUS,
                                                  type,
                                                  TRUE,  // refcount
                                                  TRUE); // blocking
   clockcache_entry *entry        = clockcache_get_entry(cc, entry_number);
   /*
    * If someone else is loading the page and has reserved the lookup, let them
    * do it.
    */
   if (!__sync_bool_compare_and_swap(
          &cc->lookup[lookup_no], CC_UNMAPPED_ENTRY, entry_number))
   {
      clockcache_dec_ref(cc, entry_number, tid);
      platform_assert(entry->waiters.head == NULL);
      entry->type   = PAGE_TYPE_INVALID;
      entry->status = CC_FREE_STATUS;
      clockcache_log(addr,
                     entry_number,
                     "get abort: entry: %u addr: %lu\n",
                     entry_number,
                     addr);
      return CC_UNMAPPED_ENTRY;
   }

   /* Set up the page */
   entry->page.disk_addr = addr;
   return entry_number;
}

static void
clockcache_finish_load(clockcache *cc,      // IN
                       uint64      addr,    // IN
                       uint32      entry_number) // OUT
{
   clockcache_log(addr,
                  entry_number,
                  "finish_load): entry %u addr %lu\n",
                  entry_number,
                  addr);

   /* Clear the loading flag */
   debug_only uint32 was_loading =
      clockcache_clear_flag(cc, entry_number, CC_LOADING);
   debug_assert(was_loading);

   clockcache_entry *entry = clockcache_get_entry(cc, entry_number);
   async_wait_queue_release_all(&entry->waiters);
}

static bool32
clockcache_get_from_disk(clockcache   *cc,   // IN
                         uint64        addr, // IN
                         page_type     type, // IN
                         page_handle **page) // OUT
{
   threadid tid       = platform_get_tid();
   uint64   page_size = clockcache_page_size(cc);

   uint64 entry_number = clockcache_acquire_entry_for_load(cc, addr, type);
   if (entry_number == CC_UNMAPPED_ENTRY) {
      return TRUE;
   }
   clockcache_entry *entry = clockcache_get_entry(cc, entry_number);

   uint64 start, elapsed;
   if (cc->cfg->use_stats) {
      start = platform_get_timestamp();
   }

   platform_status status = io_read(cc->io, entry->page.data, page_size, addr);
   platform_assert_status_ok(status);

   if (cc->cfg->use_stats) {
      elapsed = platform_timestamp_elapsed(start);
      cc->stats[tid].cache_misses[type]++;
      cc->stats[tid].page_reads[type]++;
      cc->stats[tid].cache_miss_time_ns[type] += elapsed;
   }

   clockcache_finish_load(cc, addr, entry_number);

   *page = &entry->page;

   return FALSE;
}

/*
 *----------------------------------------------------------------------
 * clockcache_get_internal --
 *
 *      Attempts to get a pointer to the page_handle for the page with
 *      address addr. If successful returns FALSE indicating no retries
 *      are needed, else TRUE indicating the caller needs to retry.
 *      Updates the "page" argument to the page_handle on success.
 *
 *      Will ask the caller to retry if we race with the eviction or if
 *      we have to evict an entry and race with someone else loading the
 *      entry.
 *      Blocks while the page is loaded into cache if necessary.
 *----------------------------------------------------------------------
 */
debug_only static bool32
clockcache_get_internal(clockcache   *cc,       // IN
                        uint64        addr,     // IN
                        bool32        blocking, // IN
                        page_type     type,     // IN
                        page_handle **page)     // OUT
{
   debug_only uint64 page_size = clockcache_page_size(cc);
   debug_assert(
      ((addr % page_size) == 0), "addr=%lu, page_size=%lu\n", addr, page_size);

#if SPLINTER_DEBUG
   uint64 base_addr =
      allocator_config_extent_base_addr(allocator_get_config(cc->al), addr);
   refcount extent_ref_count = allocator_get_refcount(cc->al, base_addr);

   // Dump allocated extents info for deeper debugging.
   if (extent_ref_count == AL_FREE) {
      allocator_print_allocated(cc->al);
   }
   debug_assert((extent_ref_count != AL_FREE),
                "Attempt to get a buffer for page addr=%lu"
                ", page type=%d ('%s'),"
                " from extent addr=%lu, (extent number=%lu)"
                ", which is an unallocated extent, extent_ref_count=%u.",
                addr,
                type,
                page_type_str[type],
                base_addr,
                (base_addr / clockcache_extent_size(cc)),
                extent_ref_count);
#endif // SPLINTER_DEBUG

   // We expect entry_number to be valid, but it's still validated below
   // in case some arithmetic goes wrong.
   uint32 entry_number = clockcache_lookup(cc, addr);

   if (entry_number != CC_UNMAPPED_ENTRY) {
      return clockcache_get_in_cache(
         cc, addr, blocking, type, entry_number, page);
   } else if (blocking) {
      return clockcache_get_from_disk(cc, addr, type, page);
   } else {
      return FALSE;
   }
}

/*
 *----------------------------------------------------------------------
 * clockcache_get --
 *
 *      Returns a pointer to the page_handle for the page with address addr.
 *      Calls clockcachge_get_int till a retry is needed.
 *
 *      If blocking is set, then it blocks until the page is unlocked as
 *well.
 *
 *      Returns with a read lock held.
 *----------------------------------------------------------------------
 */
page_handle *
clockcache_get(clockcache *cc, uint64 addr, bool32 blocking, page_type type)
{
   bool32       retry;
   page_handle *handle;

   debug_assert(cc->per_thread[platform_get_tid()].enable_sync_get
                || type == PAGE_TYPE_MEMTABLE);
   while (1) {
      retry = clockcache_get_internal(cc, addr, blocking, type, &handle);
      if (!retry) {
         return handle;
      }
   }
}

/*
 * Get addr if addr is at entry_number.  Returns TRUE if successful.
 */

// clang-format off
DEFINE_ASYNC_STATE(clockcache_get_async_state, 3,
   param, clockcache *, cc,
   param, uint64, addr,
   param, page_type, type,
   param, async_callback_fn, callback,
   param, void *, callback_arg,
   local, page_handle *, __async_result,
   local, bool32, succeeded,
   local, threadid, tid,
   local, uint64, entry_number,
   local, clockcache_entry *, entry,
   local, uint64, page_size,
   local, uint64, base_addr,
   local, refcount, extent_ref_count,
   local, platform_status, rc,
   local, io_async_state_buffer, iostate,
   local, async_waiter, wait_node)
// clang-format on

_Static_assert(sizeof(clockcache_get_async_state)
                  <= PAGE_GET_ASYNC_STATE_BUFFER_SIZE,
               "clockcache_get_async_state is too large");


/*
 * Result is FALSE if we failed to find the page in cache and hence need to
 * retry the get from the beginning, TRUE if we succeeded.
 */
static async_status
clockcache_get_in_cache_async(clockcache_get_async_state *state, uint64 depth)
{
   async_begin(state, depth);

   state->tid = platform_get_tid();

   // We don't bother yielding for writers because they are expected to be
   // fast.  We do yield (below) if someone else is loading the page.
   if (clockcache_get_read(state->cc, state->entry_number) != GET_RC_SUCCESS) {
      // this means we raced with eviction, start over
      clockcache_log(state->addr,
                     state->entry_number,
                     "get (eviction race): entry %u addr %lu\n",
                     state->entry_number,
                     state->addr);
      state->succeeded = FALSE;
      async_return(state);
   }

   state->entry = clockcache_get_entry(state->cc, state->entry_number);
   if (state->entry->page.disk_addr != state->addr) {
      // this also means we raced with eviction and really lost
      clockcache_dec_ref(state->cc, state->entry_number, state->tid);
      state->succeeded = FALSE;
      async_return(state);
   }

   platform_assert(state->entry->type == state->type,
                   "entry->type %d != state->type %d\n",
                   state->entry->type,
                   state->type);

   async_wait_on_queue_until(
      !clockcache_test_flag(state->cc, state->entry_number, CC_LOADING),
      state,
      &state->entry->waiters,
      &state->wait_node,
      state->callback,
      state->callback_arg);

   if (state->cc->cfg->use_stats) {
      state->cc->stats[state->tid].cache_hits[state->type]++;
   }
   clockcache_log(
      state->addr,
      state->entry_number,
      "get (cached): entry %u addr %lu rc %u\n",
      state->entry_number,
      state->addr,
      clockcache_get_ref(state->cc, state->entry_number, state->tid));
   state->__async_result = &state->entry->page;
   state->succeeded      = TRUE;
   async_return(state);
}

void
clockcache_get_from_disk_async_callback(void *arg)
{
   clockcache_get_async_state *state = (clockcache_get_async_state *)arg;
   clockcache_finish_load(state->cc, state->addr, state->entry_number);
   state->callback(state->callback_arg);
}

static async_status
clockcache_get_from_disk_async(clockcache_get_async_state *state, uint64 depth)
{
   async_begin(state, depth);

   state->entry_number =
      clockcache_acquire_entry_for_load(state->cc, state->addr, state->type);
   if (state->entry_number == CC_UNMAPPED_ENTRY) {
      state->succeeded = FALSE;
      async_return(state);
   }
   state->entry = clockcache_get_entry(state->cc, state->entry_number);

   // The normal idiom for async functions is to just pass the callback to the
   // async child, but we pass a wrapper function so that we can always clear
   // the CC_LOADING flag, even if our caller abandoned us.
   state->rc = io_async_state_init(state->iostate,
                                   state->cc->io,
                                   io_async_preadv,
                                   state->addr,
                                   clockcache_get_from_disk_async_callback,
                                   state);
   // FIXME: I'm not sure if the cache state machine allows us to bail out once
   // we've acquired an entry, because other threads could now be waiting on the
   // load to finish, and there is no way for them to handle our failure to load
   // the page.
   platform_assert_status_ok(state->rc);

   state->rc =
      io_async_state_append_page(state->iostate, state->entry->page.data);
   platform_assert_status_ok(state->rc);

   while (io_async_run(state->iostate) != ASYNC_STATUS_DONE) {
      async_yield(state);
   }
   platform_assert_status_ok(io_async_state_get_result(state->iostate));
   io_async_state_deinit(state->iostate);

   state->__async_result = &state->entry->page;
   state->succeeded      = TRUE;
   async_return(state);
}

// Result is TRUE if successful, FALSE otherwise
static async_status
clockcache_get_internal_async(clockcache_get_async_state *state, uint64 depth)
{
   async_begin(state, depth);

   state->tid = platform_get_tid();

   state->page_size = clockcache_page_size(state->cc);
   debug_assert(((state->addr % state->page_size) == 0),
                "addr=%lu, page_size=%lu\n",
                state->addr,
                state->page_size);

#if SPLINTER_DEBUG
   state->base_addr = allocator_config_extent_base_addr(
      allocator_get_config(state->cc->al), state->addr);
   state->extent_ref_count =
      allocator_get_refcount(state->cc->al, state->base_addr);

   // Dump allocated extents info for deeper debugging.
   if (state->extent_ref_count == AL_FREE) {
      allocator_print_allocated(state->cc->al);
   }
   debug_assert((state->extent_ref_count != AL_FREE),
                "Attempt to get a buffer for page addr=%lu"
                ", page type=%d ('%s'),"
                " from extent addr=%lu, (extent number=%lu)"
                ", which is an unallocated extent, extent_ref_count=%u.",
                state->addr,
                state->type,
                page_type_str[state->type],
                state->base_addr,
                (state->base_addr / clockcache_extent_size(state->cc)),
                state->extent_ref_count);
#endif // SPLINTER_DEBUG

   // We expect entry_number to be valid, but it's still validated below
   // in case some arithmetic goes wrong.
   state->entry_number = clockcache_lookup(state->cc, state->addr);

   if (state->entry_number != CC_UNMAPPED_ENTRY) {
      async_await_subroutine(state, clockcache_get_in_cache_async);
   } else {
      async_await_subroutine(state, clockcache_get_from_disk_async);
   }
   async_return(state);
}

async_status
clockcache_get_async(clockcache_get_async_state *state)
{
   async_begin(state, 0);

   debug_assert(state->cc->per_thread[platform_get_tid()].enable_sync_get
                || state->type == PAGE_TYPE_MEMTABLE);

   state->succeeded = FALSE;
   while (!state->succeeded) {
      async_await_subroutine(state, clockcache_get_internal_async);
   }
   async_return(state);
}

void
clockcache_unget(clockcache *cc, page_handle *page)
{
   uint32         entry_number = clockcache_page_to_entry_number(cc, page);
   const threadid tid          = platform_get_tid();

   clockcache_record_backtrace(cc, entry_number);

   // T&T&S reduces contention
   if (!clockcache_test_flag(cc, entry_number, CC_ACCESSED)) {
      clockcache_set_flag(cc, entry_number, CC_ACCESSED);
   }

   clockcache_log(page->disk_addr,
                  entry_number,
                  "unget: entry %u addr %lu rc %u\n",
                  entry_number,
                  page->disk_addr,
                  clockcache_get_ref(cc, entry_number, tid) - 1);
   clockcache_dec_ref(cc, entry_number, tid);
}


/*
 *----------------------------------------------------------------------
 * clockcache_try_claim --
 *
 *      Upgrades a read lock to a claim. This function does not block and
 *      returns TRUE if the claim was successfully obtained.
 *
 *      A claimed node has the CC_CLAIMED bit set in its status vector.
 *
 *      NOTE: When a call to claim fails, the caller must drop and reobtain
 *the readlock before trying to claim again to avoid deadlock.
 *----------------------------------------------------------------------
 */
bool32
clockcache_try_claim(clockcache *cc, page_handle *page)
{
   uint32 entry_number = clockcache_page_to_entry_number(cc, page);

   clockcache_record_backtrace(cc, entry_number);
   clockcache_log(page->disk_addr,
                  entry_number,
                  "claim: entry %u addr %lu\n",
                  entry_number,
                  page->disk_addr);

   return clockcache_try_get_claim(cc, entry_number) == GET_RC_SUCCESS;
}

void
clockcache_unclaim(clockcache *cc, page_handle *page)
{
   uint32 entry_number = clockcache_page_to_entry_number(cc, page);

   clockcache_record_backtrace(cc, entry_number);
   clockcache_log(page->disk_addr,
                  entry_number,
                  "unclaim: entry %u addr %lu\n",
                  entry_number,
                  page->disk_addr);

   debug_only uint32 status =
      clockcache_clear_flag(cc, entry_number, CC_CLAIMED);
   debug_assert(status);
}


/*
 *----------------------------------------------------------------------
 * clockcache_lock --
 *
 *     Write locks a claimed page and blocks while any read locks are
 *released.
 *
 *     The write lock is indicated by having the CC_WRITELOCKED flag set in
 *     addition to the CC_CLAIMED flag.
 *----------------------------------------------------------------------
 */
void
clockcache_lock(clockcache *cc, page_handle *page)
{
   uint32 entry_number = clockcache_page_to_entry_number(cc, page);

   clockcache_record_backtrace(cc, entry_number);
   clockcache_log(page->disk_addr,
                  entry_number,
                  "lock: entry %u addr %lu\n",
                  entry_number,
                  page->disk_addr);
   clockcache_get_write(cc, entry_number);
}

void
clockcache_unlock(clockcache *cc, page_handle *page)
{
   uint32 entry_number = clockcache_page_to_entry_number(cc, page);

   clockcache_record_backtrace(cc, entry_number);
   clockcache_log(page->disk_addr,
                  entry_number,
                  "unlock: entry %u addr %lu\n",
                  entry_number,
                  page->disk_addr);
   debug_only uint32 was_writing =
      clockcache_clear_flag(cc, entry_number, CC_WRITELOCKED);
   debug_assert(was_writing);
}


/*----------------------------------------------------------------------
 * clockcache_mark_dirty --
 *
 *      Marks the entry dirty.
 *----------------------------------------------------------------------
 */
void
clockcache_mark_dirty(clockcache *cc, page_handle *page)
{
   debug_only clockcache_entry *entry = clockcache_page_to_entry(cc, page);
   uint32 entry_number = clockcache_page_to_entry_number(cc, page);

   clockcache_log(entry->page.disk_addr,
                  entry_number,
                  "mark_dirty: entry %u addr %lu\n",
                  entry_number,
                  entry->page.disk_addr);
   clockcache_clear_flag(cc, entry_number, CC_CLEAN);
   return;
}

/*
 *----------------------------------------------------------------------
 * clockcache_pin --
 *
 *      Functionally equivalent to an anonymous read lock. Implemented using
 *a special ref count.
 *
 *      A write lock must be held while pinning to avoid a race with
 *eviction.
 *----------------------------------------------------------------------
 */
void
clockcache_pin(clockcache *cc, page_handle *page)
{
   debug_only clockcache_entry *entry = clockcache_page_to_entry(cc, page);
   uint32 entry_number = clockcache_page_to_entry_number(cc, page);
   debug_assert(clockcache_test_flag(cc, entry_number, CC_WRITELOCKED));
   clockcache_inc_pin(cc, entry_number);

   clockcache_log(entry->page.disk_addr,
                  entry_number,
                  "pin: entry %u addr %lu\n",
                  entry_number,
                  entry->page.disk_addr);
}

void
clockcache_unpin(clockcache *cc, page_handle *page)
{
   debug_only clockcache_entry *entry = clockcache_page_to_entry(cc, page);
   uint32 entry_number = clockcache_page_to_entry_number(cc, page);
   clockcache_dec_pin(cc, entry_number);

   clockcache_log(entry->page.disk_addr,
                  entry_number,
                  "unpin: entry %u addr %lu\n",
                  entry_number,
                  entry->page.disk_addr);
}

/*
 *-----------------------------------------------------------------------------
 * clockcache_page_sync --
 *
 *      Asynchronously syncs the page. Currently there is no way to check
 *when the writeback has completed.
 *-----------------------------------------------------------------------------
 */
void
clockcache_page_sync(clockcache  *cc,
                     page_handle *page,
                     bool32       is_blocking,
                     page_type    type)
{
   uint32          entry_number = clockcache_page_to_entry_number(cc, page);
   async_io_state *state;
   uint64          addr = page->disk_addr;
   const threadid  tid  = platform_get_tid();
   platform_status status;

   if (!clockcache_try_set_writeback(cc, entry_number, TRUE)) {
      platform_assert(clockcache_test_flag(cc, entry_number, CC_CLEAN));
      return;
   }

   if (cc->cfg->use_stats) {
      cc->stats[tid].page_writes[type]++;
      cc->stats[tid].syncs_issued++;
   }

   if (!is_blocking) {
      state = TYPED_MALLOC(cc->heap_id, state);
      platform_assert(state);
      state->cc                = cc;
      state->outstanding_pages = NULL;
      io_async_state_init(state->iostate,
                          cc->io,
                          io_async_pwritev,
                          addr,
                          clockcache_write_callback,
                          state);
      io_async_state_append_page(state->iostate, page->data);
      io_async_run(state->iostate);
   } else {
      status = io_write(cc->io, page->data, clockcache_page_size(cc), addr);
      platform_assert_status_ok(status);
      clockcache_log(addr,
                     entry_number,
                     "page_sync write entry %u addr %lu\n",
                     entry_number,
                     addr);
      debug_only uint8 rc;
      rc = clockcache_set_flag(cc, entry_number, CC_CLEAN);
      debug_assert(!rc);
      rc = clockcache_clear_flag(cc, entry_number, CC_WRITEBACK);
      debug_assert(rc);
   }
}

/*
 *-----------------------------------------------------------------------------
 * clockcache_extent_sync --
 *
 *      Asynchronously syncs the extent.
 *
 *      Adds the number of pages issued writeback to the counter pointed to
 *      by pages_outstanding. When the writes complete, a callback subtracts
 *      them off, so that the caller may track how many pages are in
 *writeback.
 *
 *      Assumes all pages in the extent are clean or cleanable
 *-----------------------------------------------------------------------------
 */
void
clockcache_extent_sync(clockcache *cc, uint64 addr, uint64 *pages_outstanding)
{
   async_io_state *state = NULL;
   uint64          i;
   uint32          entry_number;
   uint64          req_count = 0;
   uint64          req_addr;
   uint64          page_addr;

   for (i = 0; i < cc->cfg->pages_per_extent; i++) {
      page_addr    = addr + clockcache_multiply_by_page_size(cc, i);
      entry_number = clockcache_lookup(cc, page_addr);
      if (entry_number != CC_UNMAPPED_ENTRY
          && clockcache_try_set_writeback(cc, entry_number, TRUE))
      {
         if (state == NULL) {
            req_addr = page_addr;
            state    = TYPED_MALLOC(cc->heap_id, state);
            platform_assert(state);
            state->cc                = cc;
            state->outstanding_pages = pages_outstanding;
            io_async_state_init(state->iostate,
                                cc->io,
                                io_async_pwritev,
                                req_addr,
                                clockcache_write_callback,
                                state);
         }
         io_async_state_append_page(
            state->iostate, clockcache_get_entry(cc, entry_number)->page.data);
         req_count++;
      } else {
         // ALEX: There is maybe a race with eviction with this assertion
         debug_assert(entry_number == CC_UNMAPPED_ENTRY
                      || clockcache_test_flag(cc, entry_number, CC_CLEAN));
         if (state != NULL) {
            __sync_fetch_and_add(pages_outstanding, req_count);
            io_async_run(state->iostate);
            state     = NULL;
            req_count = 0;
         }
      }
   }
   if (state != NULL) {
      __sync_fetch_and_add(pages_outstanding, req_count);
      io_async_run(state->iostate);
   }
}

/*
 * Clockcache prefetching
 *
 * The main trickiness here is that we call io_async_read() from the callback we
 * get from io_async_read().  The callback will actually come from io_cleanup,
 * but Sometimes the callback will occur before the first invocation of
 * io_async_read has even finished, so we need to avoid running two instances of
 * io_async_read() at the same time on the same state structure.  We accomplish
 * this by using a lock in the state structure.
 *
 * The other trickiness is that we need to free the state structure in the
 * callback, but only once we are done, and we need to ensure that there is not
 * another callback in progress when we free the state structure.  Because of
 * the lock, we get to execute only once our parent (and hence all ancestors)
 * has finished, so we don't have to worry about our parents.  And we spawn a
 * child callback only if our call to io_async_read() returns that the read is
 * not done, and we only free the state structure if the read is done.
 *
 * Hence we free the state structure only when we are the only callback in
 * progress.
 */

/*
 *----------------------------------------------------------------------
 * clockcache_prefetch_callback --
 *
 *      Internal callback function to clean up after prefetching a collection
 *      of pages from the device.
 *----------------------------------------------------------------------
 */
static void
clockcache_prefetch_callback(void *pfs)
{
   async_io_state *state = (async_io_state *)pfs;

   // Check whether we are done.  If not, this will enqueue us for a future
   // callback so we can check again.
   if (io_async_run(state->iostate) != ASYNC_STATUS_DONE) {
      return;
   }

   platform_assert_status_ok(io_async_state_get_result(state->iostate));

   const struct iovec *iovec;
   uint64              count;
   iovec = io_async_state_get_iovec(state->iostate, &count);

   clockcache          *cc        = state->cc;
   debug_only page_type type      = PAGE_TYPE_INVALID;
   debug_only uint64    last_addr = CC_UNMAPPED_ADDR;

   platform_assert(count > 0);
   platform_assert(count <= cc->cfg->pages_per_extent);

   debug_code(uint64 page_size = clockcache_page_size(cc));
   for (uint64 page_off = 0; page_off < count; page_off++) {
      uint32 entry_no =
         clockcache_data_to_entry_number(cc, (char *)iovec[page_off].iov_base);
      clockcache_entry *entry = &cc->entry[entry_no];
      if (page_off != 0) {
         debug_assert(type == entry->type);
      } else {
         type = entry->type;
      }

      uint64 addr = entry->page.disk_addr;
      debug_assert(addr != CC_UNMAPPED_ADDR);
      debug_assert(last_addr == CC_UNMAPPED_ADDR
                   || addr == last_addr + page_size);
      debug_code(last_addr = addr);
      debug_assert(entry_no == clockcache_lookup(cc, addr));

      clockcache_finish_load(cc, addr, entry_no);
   }

   io_async_state_deinit(state->iostate);
   platform_free(cc->heap_id, state);
}

/*
 *-----------------------------------------------------------------------------
 * clockcache_prefetch --
 *
 *      prefetch asynchronously loads the extent with given base address
 *-----------------------------------------------------------------------------
 */
void
clockcache_prefetch(clockcache *cc, uint64 base_addr, page_type type)
{
   async_io_state *state            = NULL;
   uint64          pages_per_extent = cc->cfg->pages_per_extent;
   threadid        tid              = platform_get_tid();

   debug_assert(base_addr % clockcache_extent_size(cc) == 0);

   for (uint64 page_off = 0; page_off < pages_per_extent; page_off++) {
      uint64 addr = base_addr + clockcache_multiply_by_page_size(cc, page_off);
      uint32 entry_no = clockcache_lookup(cc, addr);
      get_rc get_read_rc;
      if (entry_no != CC_UNMAPPED_ENTRY) {
         get_read_rc = clockcache_try_get_read(cc, entry_no, TRUE);
      } else {
         get_read_rc = GET_RC_EVICTED;
      }

      switch (get_read_rc) {
         case GET_RC_SUCCESS:
            clockcache_dec_ref(cc, entry_no, tid);
            // fallthrough
         case GET_RC_CONFLICT:
            // in cache, issue IO req if started
            if (state != NULL) {
               if (cc->cfg->use_stats) {
                  threadid tid = platform_get_tid();
                  uint64   count;
                  io_async_state_get_iovec(state->iostate, &count);
                  cc->stats[tid].page_reads[type] += count;
                  cc->stats[tid].prefetches_issued[type]++;
               }
               io_async_run(state->iostate);
               state = NULL;
            }
            clockcache_log(addr,
                           entry_no,
                           "prefetch (cached): entry %u addr %lu\n",
                           entry_no,
                           addr);
            break;
         case GET_RC_EVICTED:
         {
            // need to prefetch
            uint32 free_entry_no = clockcache_get_free_page(
               cc, CC_READ_LOADING_STATUS, type, FALSE, TRUE);
            clockcache_entry *entry = &cc->entry[free_entry_no];
            entry->page.disk_addr   = addr;
            entry->type             = type;
            uint64 lookup_no        = clockcache_divide_by_page_size(cc, addr);
            if (__sync_bool_compare_and_swap(
                   &cc->lookup[lookup_no], CC_UNMAPPED_ENTRY, free_entry_no))
            {
               if (state == NULL) {
                  // start a new IO req
                  state = TYPED_MALLOC(cc->heap_id, state);
                  platform_assert(state);
                  state->cc = cc;
                  io_async_state_init(state->iostate,
                                      cc->io,
                                      io_async_preadv,
                                      addr,
                                      clockcache_prefetch_callback,
                                      state);
               }
               platform_status rc =
                  io_async_state_append_page(state->iostate, entry->page.data);
               platform_assert_status_ok(rc);
               clockcache_log(addr,
                              entry_no,
                              "prefetch (load): entry %u addr %lu\n",
                              entry_no,
                              addr);
            } else {
               /*
                * someone else is already loading this page, release the free
                * entry and retry
                */
               entry->page.disk_addr = CC_UNMAPPED_ADDR;
               entry->type           = PAGE_TYPE_INVALID;
               platform_assert(entry->waiters.head == NULL);
               entry->status = CC_FREE_STATUS;
               page_off--;
            }
            break;
         }
         default:
            platform_assert(0);
      }
   }
   // issue IO req if started
   if (state != NULL) {
      if (cc->cfg->use_stats) {
         threadid tid = platform_get_tid();
         uint64   count;
         io_async_state_get_iovec(state->iostate, &count);
         cc->stats[tid].page_reads[type] += count;
         cc->stats[tid].prefetches_issued[type]++;
      }
      io_async_run(state->iostate);
      state = NULL;
   }
}

/*
 *----------------------------------------------------------------------
 * clockcache_print --
 *
 *      Prints a bitmap representation of the cache.
 *----------------------------------------------------------------------
 */
void
clockcache_print(platform_log_handle *log_handle, clockcache *cc)
{
   uint64   i;
   uint32   status;
   uint16   refcount;
   threadid thr_i;

   platform_log(log_handle,
                "************************** CACHE CONTENTS "
                "**************************\n");
   for (i = 0; i < cc->cfg->page_capacity; i++) {
      if (i != 0 && i % 16 == 0) {
         platform_log(log_handle, "\n");
      }
      if (i % CC_ENTRIES_PER_BATCH == 0) {
         platform_log(log_handle,
                      "Word %lu entries %lu-%lu\n",
                      (i / CC_ENTRIES_PER_BATCH),
                      i,
                      i + 63);
      }
      status   = cc->entry[i].status;
      refcount = 0;
      for (thr_i = 0; thr_i < CC_RC_WIDTH; thr_i++) {
         refcount += clockcache_get_ref(cc, i, thr_i);
      }
      platform_log(log_handle, "0x%02x-%u ", status, refcount);
   }

   platform_log(log_handle, "\n\n");
   return;
}

void
clockcache_validate_page(clockcache *cc, page_handle *page, uint64 addr)
{
   debug_assert(allocator_page_valid(cc->al, addr));
   debug_assert(page->disk_addr == addr);
   debug_assert(!clockcache_test_flag(
      cc, clockcache_page_to_entry_number(cc, page), CC_FREE));
}

void
clockcache_assert_ungot(clockcache *cc, uint64 addr)
{
   uint32 entry_number = clockcache_lookup(cc, addr);

   if (entry_number != CC_UNMAPPED_ENTRY) {
      for (threadid tid = 0; tid < CC_RC_WIDTH; tid++) {
         debug_only uint16 ref_count =
            clockcache_get_ref(cc, entry_number, tid);
         debug_assert(ref_count == 0,
                      "Entry %u addr %lu has ref count %u for thread %lu",
                      entry_number,
                      addr,
                      ref_count,
                      tid);
      }
   }
}

void
clockcache_io_stats(clockcache *cc, uint64 *read_bytes, uint64 *write_bytes)
{
   *read_bytes  = 0;
   *write_bytes = 0;

   if (!cc->cfg->use_stats) {
      return;
   }

   uint64 read_pages  = 0;
   uint64 write_pages = 0;
   for (uint64 i = 0; i < MAX_THREADS; i++) {
      for (page_type type = 0; type < NUM_PAGE_TYPES; type++) {
         write_pages += cc->stats[i].page_writes[type];
         read_pages += cc->stats[i].page_reads[type];
      }
   }

   *write_bytes = write_pages * 4 * KiB;
   *read_bytes  = read_pages * 4 * KiB;
}

void
clockcache_print_stats(platform_log_handle *log_handle, clockcache *cc)
{
   uint64      i;
   page_type   type;
   cache_stats global_stats;

   if (!cc->cfg->use_stats) {
      return;
   }

   uint64 page_writes = 0;
   ZERO_CONTENTS(&global_stats);
   for (i = 0; i < MAX_THREADS; i++) {
      for (type = 0; type < NUM_PAGE_TYPES; type++) {
         global_stats.cache_hits[type] += cc->stats[i].cache_hits[type];
         global_stats.cache_misses[type] += cc->stats[i].cache_misses[type];
         global_stats.cache_miss_time_ns[type] +=
            cc->stats[i].cache_miss_time_ns[type];
         global_stats.page_writes[type] += cc->stats[i].page_writes[type];
         page_writes += cc->stats[i].page_writes[type];
         global_stats.page_reads[type] += cc->stats[i].page_reads[type];
         global_stats.prefetches_issued[type] +=
            cc->stats[i].prefetches_issued[type];
      }
      global_stats.writes_issued += cc->stats[i].writes_issued;
      global_stats.syncs_issued += cc->stats[i].syncs_issued;
   }

   fraction miss_time[NUM_PAGE_TYPES];
   fraction avg_prefetch_pages[NUM_PAGE_TYPES];
   fraction avg_write_pages;

   for (type = 0; type < NUM_PAGE_TYPES; type++) {
      miss_time[type] =
         init_fraction(global_stats.cache_miss_time_ns[type], SEC_TO_NSEC(1));
      avg_prefetch_pages[type] = init_fraction(
         global_stats.page_reads[type] - global_stats.cache_misses[type],
         global_stats.prefetches_issued[type]);
   }
   avg_write_pages = init_fraction(page_writes - global_stats.syncs_issued,
                                   global_stats.writes_issued);

   // clang-format off
   platform_log(log_handle, "Cache Statistics\n");
   platform_log(log_handle, "-----------------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "page type       |      trunk |     branch |   memtable |     filter |        log |       misc |\n");
   platform_log(log_handle, "----------------|------------|------------|------------|------------|------------|------------|\n");
   platform_log(log_handle, "cache hits      | %10lu | %10lu | %10lu | %10lu | %10lu | %10lu |\n",
         global_stats.cache_hits[PAGE_TYPE_TRUNK],
         global_stats.cache_hits[PAGE_TYPE_BRANCH],
         global_stats.cache_hits[PAGE_TYPE_MEMTABLE],
         global_stats.cache_hits[PAGE_TYPE_FILTER],
         global_stats.cache_hits[PAGE_TYPE_LOG],
         global_stats.cache_hits[PAGE_TYPE_SUPERBLOCK]);
   platform_log(log_handle, "cache misses    | %10lu | %10lu | %10lu | %10lu | %10lu | %10lu |\n",
         global_stats.cache_misses[PAGE_TYPE_TRUNK],
         global_stats.cache_misses[PAGE_TYPE_BRANCH],
         global_stats.cache_misses[PAGE_TYPE_MEMTABLE],
         global_stats.cache_misses[PAGE_TYPE_FILTER],
         global_stats.cache_misses[PAGE_TYPE_LOG],
         global_stats.cache_misses[PAGE_TYPE_SUPERBLOCK]);
   platform_log(log_handle, "cache miss time | " FRACTION_FMT(9, 2)"s | "
                FRACTION_FMT(9, 2)"s | "FRACTION_FMT(9, 2)"s | "
                FRACTION_FMT(9, 2)"s | "FRACTION_FMT(9, 2)"s | "
                FRACTION_FMT(9, 2)"s |\n",
                FRACTION_ARGS(miss_time[PAGE_TYPE_TRUNK]),
                FRACTION_ARGS(miss_time[PAGE_TYPE_BRANCH]),
                FRACTION_ARGS(miss_time[PAGE_TYPE_MEMTABLE]),
                FRACTION_ARGS(miss_time[PAGE_TYPE_FILTER]),
                FRACTION_ARGS(miss_time[PAGE_TYPE_LOG]),
                FRACTION_ARGS(miss_time[PAGE_TYPE_SUPERBLOCK]));
   platform_log(log_handle, "pages written   | %10lu | %10lu | %10lu | %10lu | %10lu | %10lu |\n",
         global_stats.page_writes[PAGE_TYPE_TRUNK],
         global_stats.page_writes[PAGE_TYPE_BRANCH],
         global_stats.page_writes[PAGE_TYPE_MEMTABLE],
         global_stats.page_writes[PAGE_TYPE_FILTER],
         global_stats.page_writes[PAGE_TYPE_LOG],
         global_stats.page_writes[PAGE_TYPE_SUPERBLOCK]);
   platform_log(log_handle, "pages read      | %10lu | %10lu | %10lu | %10lu | %10lu | %10lu |\n",
         global_stats.page_reads[PAGE_TYPE_TRUNK],
         global_stats.page_reads[PAGE_TYPE_BRANCH],
         global_stats.page_reads[PAGE_TYPE_MEMTABLE],
         global_stats.page_reads[PAGE_TYPE_FILTER],
         global_stats.page_reads[PAGE_TYPE_LOG],
         global_stats.page_reads[PAGE_TYPE_SUPERBLOCK]);
   platform_log(log_handle, "avg prefetch pg |  " FRACTION_FMT(9, 2)" |  "
                FRACTION_FMT(9, 2)" |  "FRACTION_FMT(9, 2)" |  "
                FRACTION_FMT(9, 2)" |  "FRACTION_FMT(9, 2)" |  "
                FRACTION_FMT(9, 2)" |\n",
                FRACTION_ARGS(avg_prefetch_pages[PAGE_TYPE_TRUNK]),
                FRACTION_ARGS(avg_prefetch_pages[PAGE_TYPE_BRANCH]),
                FRACTION_ARGS(avg_prefetch_pages[PAGE_TYPE_MEMTABLE]),
                FRACTION_ARGS(avg_prefetch_pages[PAGE_TYPE_FILTER]),
                FRACTION_ARGS(avg_prefetch_pages[PAGE_TYPE_LOG]),
                FRACTION_ARGS(avg_prefetch_pages[PAGE_TYPE_SUPERBLOCK]));
   platform_log(log_handle, "-----------------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "avg write pgs: "FRACTION_FMT(9,2)"\n",
                FRACTION_ARGS(avg_write_pages));
   // clang-format on

   allocator_print_stats(cc->al);
}

void
clockcache_reset_stats(clockcache *cc)
{
   uint64 i;

   for (i = 0; i < MAX_THREADS; i++) {
      cache_stats *stats = &cc->stats[i];

      memset(stats->cache_hits, 0, sizeof(stats->cache_hits));
      memset(stats->cache_misses, 0, sizeof(stats->cache_misses));
      memset(stats->cache_miss_time_ns, 0, sizeof(stats->cache_miss_time_ns));
      memset(stats->page_writes, 0, sizeof(stats->page_writes));
   }
}

/*
 *----------------------------------------------------------------------
 *
 * verification functions for cache_test
 *
 *----------------------------------------------------------------------
 */

uint32
clockcache_count_dirty(clockcache *cc)
{
   uint32 entry_no;
   uint32 dirty_count = 0;
   for (entry_no = 0; entry_no < cc->cfg->page_capacity; entry_no++) {
      if (!clockcache_test_flag(cc, entry_no, CC_CLEAN)
          && !clockcache_test_flag(cc, entry_no, CC_FREE))
      {
         dirty_count++;
      }
   }
   return dirty_count;
}

bool32
clockcache_in_use(clockcache *cc, uint64 addr)
{
   uint32 entry_no = clockcache_lookup(cc, addr);
   if (entry_no == CC_UNMAPPED_ENTRY) {
      return FALSE;
   }
   for (threadid thr_i = 0; thr_i < CC_RC_WIDTH; thr_i++) {
      if (clockcache_get_ref(cc, entry_no, thr_i) > 0) {
         return TRUE;
      }
   }
   return clockcache_test_flag(cc, entry_no, CC_WRITELOCKED);
}

uint16
clockcache_get_read_ref(clockcache *cc, page_handle *page)
{
   uint32 entry_no = clockcache_page_to_entry_number(cc, page);
   platform_assert(entry_no != CC_UNMAPPED_ENTRY);
   uint16 ref_count = 0;
   for (threadid thr_i = 0; thr_i < CC_RC_WIDTH; thr_i++) {
      ref_count += clockcache_get_ref(cc, entry_no, thr_i);
   }
   return ref_count;
}

bool32
clockcache_present(clockcache *cc, page_handle *page)
{
   return clockcache_lookup(cc, page->disk_addr) != CC_UNMAPPED_ENTRY;
}

static void
clockcache_enable_sync_get(clockcache *cc, bool32 enabled)
{
   cc->per_thread[platform_get_tid()].enable_sync_get = enabled;
}

static allocator *
clockcache_get_allocator(const clockcache *cc)
{
   return cc->al;
}

/*
 *-----------------------------------------------------------------------------
 *
 * Virtual Functions
 *
 *      Here we define virtual functions for cache_ops
 *
 *      These are just boilerplate polymorph trampolines that cast the
 *      interface type to the concrete (clockcache-specific type) and then call
 *      into the clockcache_ method, so that the clockcache_ method signature
 *      can contain concrete types. These trampolines disappear in link-time
 *      optimization.
 *
 *-----------------------------------------------------------------------------
 */

uint64
clockcache_config_page_size_virtual(const cache_config *cfg)
{
   clockcache_config *ccfg = (clockcache_config *)cfg;
   return clockcache_config_page_size(ccfg);
}

uint64
clockcache_config_extent_size_virtual(const cache_config *cfg)
{
   clockcache_config *ccfg = (clockcache_config *)cfg;
   return clockcache_config_extent_size(ccfg);
}

cache_config_ops clockcache_config_ops = {
   .page_size   = clockcache_config_page_size_virtual,
   .extent_size = clockcache_config_extent_size_virtual,
};

page_handle *
clockcache_alloc_virtual(cache *c, uint64 addr, page_type type)
{
   clockcache *cc = (clockcache *)c;
   return clockcache_alloc(cc, addr, type);
}

void
clockcache_extent_discard_virtual(cache *c, uint64 addr, page_type type)
{
   clockcache *cc = (clockcache *)c;
   return clockcache_extent_discard(cc, addr, type);
}

page_handle *
clockcache_get_virtual(cache *c, uint64 addr, bool32 blocking, page_type type)
{
   clockcache *cc = (clockcache *)c;
   return clockcache_get(cc, addr, blocking, type);
}

void
clockcache_unget_virtual(cache *c, page_handle *page)
{
   clockcache *cc = (clockcache *)c;
   clockcache_unget(cc, page);
}

bool32
clockcache_try_claim_virtual(cache *c, page_handle *page)
{
   clockcache *cc = (clockcache *)c;
   return clockcache_try_claim(cc, page);
}

void
clockcache_unclaim_virtual(cache *c, page_handle *page)
{
   clockcache *cc = (clockcache *)c;
   clockcache_unclaim(cc, page);
}

void
clockcache_lock_virtual(cache *c, page_handle *page)
{
   clockcache *cc = (clockcache *)c;
   clockcache_lock(cc, page);
}

void
clockcache_unlock_virtual(cache *c, page_handle *page)
{
   clockcache *cc = (clockcache *)c;
   clockcache_unlock(cc, page);
}

void
clockcache_prefetch_virtual(cache *c, uint64 addr, page_type type)
{
   clockcache *cc = (clockcache *)c;
   clockcache_prefetch(cc, addr, type);
}

void
clockcache_mark_dirty_virtual(cache *c, page_handle *page)
{
   clockcache *cc = (clockcache *)c;
   clockcache_mark_dirty(cc, page);
}

void
clockcache_pin_virtual(cache *c, page_handle *page)
{
   clockcache *cc = (clockcache *)c;
   clockcache_pin(cc, page);
}

void
clockcache_unpin_virtual(cache *c, page_handle *page)
{
   clockcache *cc = (clockcache *)c;
   clockcache_unpin(cc, page);
}

static void
clockcache_get_async_state_init_virtual(page_get_async_state_buffer buffer,
                                        cache                      *cc,
                                        uint64                      addr,
                                        page_type                   type,
                                        async_callback_fn           callback,
                                        void *callback_arg)
{
   clockcache_get_async_state_init((clockcache_get_async_state *)buffer,
                                   (clockcache *)cc,
                                   addr,
                                   type,
                                   callback,
                                   callback_arg);
}

static async_status
clockcache_get_async_virtual(page_get_async_state_buffer buffer)
{
   return clockcache_get_async((clockcache_get_async_state *)buffer);
}

static page_handle *
clockcache_get_async_state_result_virtual(page_get_async_state_buffer buffer)
{
   clockcache_get_async_state *state = (clockcache_get_async_state *)buffer;
   return state->__async_result;
}

void
clockcache_page_sync_virtual(cache       *c,
                             page_handle *page,
                             bool32       is_blocking,
                             page_type    type)
{
   clockcache *cc = (clockcache *)c;
   clockcache_page_sync(cc, page, is_blocking, type);
}

void
clockcache_extent_sync_virtual(cache *c, uint64 addr, uint64 *pages_outstanding)
{
   clockcache *cc = (clockcache *)c;
   clockcache_extent_sync(cc, addr, pages_outstanding);
}

void
clockcache_flush_virtual(cache *c)
{
   clockcache *cc = (clockcache *)c;
   clockcache_flush(cc);
}

int
clockcache_evict_all_virtual(cache *c, bool32 ignore_pinned)
{
   clockcache *cc = (clockcache *)c;
   return clockcache_evict_all(cc, ignore_pinned);
}

void
clockcache_wait_virtual(cache *c)
{
   clockcache *cc = (clockcache *)c;
   return clockcache_wait(cc);
}

bool32
clockcache_in_use_virtual(cache *c, uint64 addr)
{
   clockcache *cc = (clockcache *)c;
   return clockcache_in_use(cc, addr);
}

void
clockcache_assert_ungot_virtual(cache *c, uint64 addr)
{
   clockcache *cc = (clockcache *)c;
   clockcache_assert_ungot(cc, addr);
}

void
clockcache_assert_no_locks_held_virtual(cache *c)
{
   clockcache *cc = (clockcache *)c;
   clockcache_assert_no_locks_held(cc);
}

void
clockcache_print_virtual(platform_log_handle *log_handle, cache *c)
{
   clockcache *cc = (clockcache *)c;
   clockcache_print(log_handle, cc);
}

void
clockcache_validate_page_virtual(cache *c, page_handle *page, uint64 addr)
{
   clockcache *cc = (clockcache *)c;
   clockcache_validate_page(cc, page, addr);
}

void
clockcache_print_stats_virtual(platform_log_handle *log_handle, cache *c)
{
   clockcache *cc = (clockcache *)c;
   clockcache_print_stats(log_handle, cc);
}

void
clockcache_io_stats_virtual(cache *c, uint64 *read_bytes, uint64 *write_bytes)
{
   clockcache *cc = (clockcache *)c;
   clockcache_io_stats(cc, read_bytes, write_bytes);
}

void
clockcache_reset_stats_virtual(cache *c)
{
   clockcache *cc = (clockcache *)c;
   clockcache_reset_stats(cc);
}

uint32
clockcache_count_dirty_virtual(cache *c)
{
   clockcache *cc = (clockcache *)c;
   return clockcache_count_dirty(cc);
}

uint16
clockcache_get_read_ref_virtual(cache *c, page_handle *page)
{
   clockcache *cc = (clockcache *)c;
   return clockcache_get_read_ref(cc, page);
}

bool32
clockcache_present_virtual(cache *c, page_handle *page)
{
   clockcache *cc = (clockcache *)c;
   return clockcache_present(cc, page);
}

void
clockcache_enable_sync_get_virtual(cache *c, bool32 enabled)
{
   clockcache *cc = (clockcache *)c;
   clockcache_enable_sync_get(cc, enabled);
}

allocator *
clockcache_get_allocator_virtual(const cache *c)
{
   clockcache *cc = (clockcache *)c;
   return clockcache_get_allocator(cc);
}

cache_config *
clockcache_get_config_virtual(const cache *c)
{
   clockcache *cc = (clockcache *)c;
   return &cc->cfg->super;
}

static cache_ops clockcache_ops = {
   .page_alloc     = clockcache_alloc_virtual,
   .extent_discard = clockcache_extent_discard_virtual,
   .page_get       = clockcache_get_virtual,

   .page_get_async_state_init = clockcache_get_async_state_init_virtual,
   .page_get_async            = clockcache_get_async_virtual,
   .page_get_async_result     = clockcache_get_async_state_result_virtual,

   .page_unget        = clockcache_unget_virtual,
   .page_try_claim    = clockcache_try_claim_virtual,
   .page_unclaim      = clockcache_unclaim_virtual,
   .page_lock         = clockcache_lock_virtual,
   .page_unlock       = clockcache_unlock_virtual,
   .page_prefetch     = clockcache_prefetch_virtual,
   .page_mark_dirty   = clockcache_mark_dirty_virtual,
   .page_pin          = clockcache_pin_virtual,
   .page_unpin        = clockcache_unpin_virtual,
   .page_sync         = clockcache_page_sync_virtual,
   .extent_sync       = clockcache_extent_sync_virtual,
   .flush             = clockcache_flush_virtual,
   .evict             = clockcache_evict_all_virtual,
   .cleanup           = clockcache_wait_virtual,
   .in_use            = clockcache_in_use_virtual,
   .assert_ungot      = clockcache_assert_ungot_virtual,
   .assert_free       = clockcache_assert_no_locks_held_virtual,
   .print             = clockcache_print_virtual,
   .print_stats       = clockcache_print_stats_virtual,
   .io_stats          = clockcache_io_stats_virtual,
   .reset_stats       = clockcache_reset_stats_virtual,
   .validate_page     = clockcache_validate_page_virtual,
   .count_dirty       = clockcache_count_dirty_virtual,
   .page_get_read_ref = clockcache_get_read_ref_virtual,
   .cache_present     = clockcache_present_virtual,
   .enable_sync_get   = clockcache_enable_sync_get_virtual,
   .get_allocator     = clockcache_get_allocator_virtual,
   .get_config        = clockcache_get_config_virtual,
};

/*
 *-----------------------------------------------------------------------------
 * clockcache_config_init --
 *
 *      Initialize clockcache config values
 *-----------------------------------------------------------------------------
 */
void
clockcache_config_init(clockcache_config *cache_cfg,
                       io_config         *io_cfg,
                       uint64             capacity,
                       const char        *cache_logfile,
                       uint64             use_stats)
{
   int rc;
   ZERO_CONTENTS(cache_cfg);

   cache_cfg->super.ops     = &clockcache_config_ops;
   cache_cfg->io_cfg        = io_cfg;
   cache_cfg->capacity      = capacity;
   cache_cfg->log_page_size = 63 - __builtin_clzll(io_cfg->page_size);
   cache_cfg->page_capacity = capacity / io_cfg->page_size;
   cache_cfg->use_stats     = use_stats;

   rc = snprintf(cache_cfg->logfile, MAX_STRING_LENGTH, "%s", cache_logfile);
   platform_assert(rc < MAX_STRING_LENGTH);
}

platform_status
clockcache_init(clockcache        *cc,   // OUT
                clockcache_config *cfg,  // IN
                io_handle         *io,   // IN
                allocator         *al,   // IN
                char              *name, // IN
                platform_heap_id   hid,  // IN
                platform_module_id mid)  // IN
{
   int      i;
   threadid thr_i;

   platform_assert(cc != NULL);
   ZERO_CONTENTS(cc);

   cc->cfg       = cfg;
   cc->super.ops = &clockcache_ops;

   uint64 allocator_page_capacity =
      clockcache_divide_by_page_size(cc, allocator_get_capacity(al));
   uint64 debug_capacity =
      clockcache_multiply_by_page_size(cc, cc->cfg->page_capacity);
   cc->cfg->batch_capacity = cc->cfg->page_capacity / CC_ENTRIES_PER_BATCH;
   cc->cfg->cacheline_capacity =
      cc->cfg->page_capacity / PLATFORM_CACHELINE_SIZE;
   cc->cfg->pages_per_extent =
      clockcache_divide_by_page_size(cc, clockcache_extent_size(cc));

   platform_assert(cc->cfg->page_capacity % PLATFORM_CACHELINE_SIZE == 0);
   platform_assert(cc->cfg->capacity == debug_capacity);
   platform_assert(cc->cfg->page_capacity % CC_ENTRIES_PER_BATCH == 0);

   cc->cleaner_gap = CC_CLEANER_GAP;

#if defined(CC_LOG) || defined(ADDR_TRACING)
   cc->logfile = platform_open_log_file(cfg->logfile, "w");
#else
   cc->logfile = NULL;
#endif
   clockcache_log(
      0, 0, "init: capacity %lu name %s\n", cc->cfg->capacity, name);

   cc->al      = al;
   cc->io      = io;
   cc->heap_id = hid;

   /* lookup maps addrs to entries, entry contains the entries themselves */
   cc->lookup =
      TYPED_ARRAY_MALLOC(cc->heap_id, cc->lookup, allocator_page_capacity);
   if (!cc->lookup) {
      goto alloc_error;
   }
   for (i = 0; i < allocator_page_capacity; i++) {
      cc->lookup[i] = CC_UNMAPPED_ENTRY;
   }

   cc->entry =
      TYPED_ARRAY_ZALLOC(cc->heap_id, cc->entry, cc->cfg->page_capacity);
   if (!cc->entry) {
      goto alloc_error;
   }

   platform_status rc = STATUS_NO_MEMORY;

   /* data must be aligned because of O_DIRECT */
   rc = platform_buffer_init(&cc->bh, cc->cfg->capacity);
   if (!SUCCESS(rc)) {
      goto alloc_error;
   }
   cc->data = platform_buffer_getaddr(&cc->bh);

   /* Set up the entries */
   for (i = 0; i < cc->cfg->page_capacity; i++) {
      cc->entry[i].page.data =
         cc->data + clockcache_multiply_by_page_size(cc, i);
      cc->entry[i].page.disk_addr = CC_UNMAPPED_ADDR;
      cc->entry[i].status         = CC_FREE_STATUS;
      cc->entry[i].type           = PAGE_TYPE_INVALID;
      async_wait_queue_init(&cc->entry[i].waiters);
   }

   /* Entry per-thread ref counts */
   size_t refcount_size =
      cc->cfg->page_capacity * CC_RC_WIDTH * sizeof(cc->refcount[0]);

   rc = platform_buffer_init(&cc->rc_bh, refcount_size);
   if (!SUCCESS(rc)) {
      goto alloc_error;
   }
   cc->refcount = platform_buffer_getaddr(&cc->rc_bh);

   /* Separate ref counts for pins */
   cc->pincount =
      TYPED_ARRAY_ZALLOC(cc->heap_id, cc->pincount, cc->cfg->page_capacity);
   if (!cc->pincount) {
      goto alloc_error;
   }

   /* The hands and associated page */
   cc->free_hand  = 0;
   cc->evict_hand = 1;
   for (thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      cc->per_thread[thr_i].free_hand       = CC_UNMAPPED_ENTRY;
      cc->per_thread[thr_i].enable_sync_get = TRUE;
   }
   cc->batch_busy =
      TYPED_ARRAY_ZALLOC(cc->heap_id,
                         cc->batch_busy,
                         cc->cfg->page_capacity / CC_ENTRIES_PER_BATCH);
   if (!cc->batch_busy) {
      goto alloc_error;
   }

   return STATUS_OK;

alloc_error:
   clockcache_deinit(cc);
   return STATUS_NO_MEMORY;
}

/*
 * De-init the resources allocated to initialize a clockcache.
 * This function may be called to deal with error situations, or a failed
 * clockcache_init(). So check for non-NULL handles before trying to release
 * resources.
 */
void
clockcache_deinit(clockcache *cc) // IN/OUT
{
   platform_assert(cc != NULL);

   if (cc->logfile) {
      clockcache_log(0, 0, "deinit %s\n", "");
#if defined(CC_LOG) || defined(ADDR_TRACING)
      platform_close_log_file(cc->logfile);
#endif
   }

   if (cc->lookup) {
      platform_free(cc->heap_id, cc->lookup);
   }
   if (cc->entry) {
      for (int i = 0; i < cc->cfg->page_capacity; i++) {
         async_wait_queue_deinit(&cc->entry[i].waiters);
      }
      platform_free(cc->heap_id, cc->entry);
   }

   debug_only platform_status rc = STATUS_TEST_FAILED;
   if (cc->data) {
      rc = platform_buffer_deinit(&cc->bh);

      // We expect above to succeed. Anyway, we are in the process of
      // dismantling the clockcache, hence, for now, can't do much by way
      // of reporting errors further upstream.
      debug_assert(SUCCESS(rc), "rc=%s", platform_status_to_string(rc));
      cc->data = NULL;
   }
   if (cc->refcount) {
      rc = platform_buffer_deinit(&cc->rc_bh);
      debug_assert(SUCCESS(rc), "rc=%s", platform_status_to_string(rc));
      cc->refcount = NULL;
   }

   if (cc->pincount) {
      platform_free_volatile(cc->heap_id, cc->pincount);
   }
   if (cc->batch_busy) {
      platform_free_volatile(cc->heap_id, cc->batch_busy);
   }
}
