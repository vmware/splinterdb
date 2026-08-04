// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *-----------------------------------------------------------------------------
 * shard_log.c --
 *
 *     This file contains the implementation for a sharded write-ahead log.
 *-----------------------------------------------------------------------------
 */

#include "shard_log.h"
#include "data_blob_build.h"
#include "data_internal.h"
#include "platform_sleep.h"
#include "platform_hash.h"
#include "platform_typed_alloc.h"
#include "platform_assert.h"
#include "platform_threads.h"
#include "platform_sort.h"
#include "poison.h"

static uint64 shard_log_magic_idx = 0;

static const page_type shard_log_page_type_table[NUM_BLOB_BATCHES + 1] = {
   PAGE_TYPE_LOG,
   [1 ... NUM_BLOB_BATCHES] = PAGE_TYPE_BLOB,
};

static inline uint64
shard_log_page_size(shard_log_config *cfg)
{
   return cache_config_page_size(cfg->cache_cfg);
}

static inline uint64
shard_log_pages_per_extent(shard_log_config *cfg)
{
   return cache_config_pages_per_extent(cfg->cache_cfg);
}

static inline uint64
shard_log_extent_size(shard_log_config *cfg)
{
   return cache_config_extent_size(cfg->cache_cfg);
}

static inline checksum128
shard_log_checksum(shard_log_config *cfg, page_handle *page)
{
   return platform_checksum128(
      page->data + 16, shard_log_page_size(cfg) - 16, cfg->seed);
}

static inline shard_log_thread_data *
shard_log_get_thread_data(shard_log *log, threadid thr_id)
{
   return &log->thread_data[thr_id];
}

page_handle *
shard_log_alloc(shard_log *log, uint64 *next_extent)
{
   uint64 addr = mini_alloc_page(&log->mini, 0, next_extent);
   if (addr == 0) {
      return NULL;
   }
   return cache_alloc(log->cc, addr, PAGE_TYPE_LOG);
}

/*
 * -------------------------------------------------------------------------
 * Header for a key/message pair stored in the sharded log: Disk-resident
 * structure. Appears on pages of page type == PAGE_TYPE_LOG
 * -------------------------------------------------------------------------
 */
struct ONDISK log_entry {
   uint64       memtable_generation;
   uint64       leaf_generation;
   ondisk_tuple tuple;
};

#define INVALID_LOG_GENERATION ((uint64) - 1)

static key
log_entry_key(log_entry *le)
{
   return ondisk_tuple_key(&le->tuple);
}

static bool32
log_entry_message_is_blob(log_entry *le)
{
   return ondisk_tuple_message_is_blob(&le->tuple);
}

static message
log_entry_message(cache *cc, log_entry *le)
{
   return ondisk_tuple_message(cc, &le->tuple);
}

static uint64
log_entry_required_capacity(key tuple_key, message msg)
{
   debug_assert(key_is_user_key(tuple_key));
   return sizeof(log_entry)
          + ondisk_tuple_required_data_capacity(tuple_key, msg);
}

static uint64
sizeof_log_entry(log_entry *le)
{
   return sizeof(log_entry) + sizeof_ondisk_tuple_data(&le->tuple);
}

static log_entry *
first_log_entry(char *page)
{
   return (log_entry *)(page + sizeof(shard_log_hdr));
}

static bool32
terminal_log_entry(shard_log_config *cfg, char *page, log_entry *le)
{
   return page + shard_log_page_size(cfg) - (char *)le < sizeof(log_entry)
          || le->memtable_generation == INVALID_LOG_GENERATION;
}

static inline void
log_entry_set_terminal(log_entry *le)
{
   /*
    * terminal_log_entry() tests memtable_generation.  Mark both generation
    * fields invalid so the terminator cannot be confused with a record by
    * diagnostics or a future format validator.
    */
   le->leaf_generation     = INVALID_LOG_GENERATION;
   le->memtable_generation = INVALID_LOG_GENERATION;
}

static log_entry *
log_entry_next(log_entry *le)
{
   return (log_entry *)((char *)le + sizeof_log_entry(le));
}

/* Reset a staging buffer to an empty page image. */
static void
shard_log_reset_buffer(shard_log *log, shard_log_thread_data *thread_data)
{
   shard_log_hdr *hdr = (shard_log_hdr *)thread_data->buf;
   hdr->magic         = log->magic;
   hdr->num_entries   = 0;
   // next_extent_addr and checksum are only knowable once a page has been
   // allocated for this image; see shard_log_graduate_buffer().
   thread_data->offset = sizeof(shard_log_hdr);
}

/*
 * Turn a thread's staged image into an on-disk log page: allocate the page,
 * copy the image in, and hand the write to the cache. A no-op if nothing has
 * been staged.
 *
 * This is the only place a log page is written, and it writes each page exactly
 * once, in full -- so there is never a partially-filled log page on disk to be
 * rewritten later.
 */
typedef enum shard_log_close {
   SHARD_LOG_CLOSE_NONE,   // an ordinary page; the group stays open
   SHARD_LOG_CLOSE_GROUP,  // last page of its group
   SHARD_LOG_CLOSE_STREAM, // last page of its group and of the stream
} shard_log_close;

static platform_status
shard_log_graduate_buffer(shard_log             *log,
                          shard_log_thread_data *thread_data,
                          shard_log_close        close)
{
   bool32 close_group = (close != SHARD_LOG_CLOSE_NONE);

   uint64 page_size = shard_log_page_size(log->cfg);

   debug_assert(thread_data->offset >= sizeof(shard_log_hdr));
   debug_assert(thread_data->offset <= page_size);
   /*
    * An empty buffer normally has nothing to contribute, but closing the group
    * still needs a page to carry the terminator, so we emit an otherwise empty
    * one.
    */
   if (thread_data->offset == sizeof(shard_log_hdr) && !close_group) {
      return STATUS_OK;
   }

   /*
    * Terminate the record stream where there is room for a marker. A tail too
    * short to hold one needs none: terminal_log_entry() treats it as the end.
    */
   uint64 free_space = page_size - thread_data->offset;
   if (sizeof(log_entry) <= free_space) {
      log_entry_set_terminal(
         (log_entry *)(thread_data->buf + thread_data->offset));
   }

   uint64       next_extent;
   page_handle *page = shard_log_alloc(log, &next_extent);
   if (page == NULL) {
      platform_error_log("shard_log_graduate_buffer: out of log space\n");
      return STATUS_NO_SPACE;
   }

   shard_log_hdr *staged    = (shard_log_hdr *)thread_data->buf;
   staged->next_extent_addr = next_extent;
   staged->group_id         = log->group_id;
   /*
    * Counted as it is handed over, so that the closing page -- the last to be
    * counted -- sees the group's final size and can record it.
    */
   uint64 pages = __sync_add_and_fetch(&log->group_page_count, 1);
   if (close_group) {
      platform_assert(pages <= SHARD_LOG_PAGES_IN_GROUP_MASK,
                      "group %lu is too large to terminate: %lu pages",
                      log->group_id,
                      pages);
      staged->pages_in_group =
         (uint32)pages
         | (close == SHARD_LOG_CLOSE_STREAM ? SHARD_LOG_END_OF_STREAM : 0);
   } else {
      staged->pages_in_group = 0;
   }

   memcpy(page->data, thread_data->buf, page_size);
   // Computed on the page over everything but the checksum field itself, so
   // the stale bytes just copied over it do not matter.
   ((shard_log_hdr *)page->data)->checksum = shard_log_checksum(log->cfg, page);

   cache_unlock(log->cc, page);
   cache_unclaim(log->cc, page);
   /*
    * Keep the receipt: closing the group has to wait for this write, and by
    * then the page will be long out of reach.
    */
   platform_mutex_lock(&log->wbset_lock);
   platform_status rc =
      writeback_set_add_page(&log->wbset, page, PAGE_TYPE_LOG);
   platform_mutex_unlock(&log->wbset_lock);
   cache_unget(log->cc, page);

   shard_log_reset_buffer(log, thread_data);
   return rc;
}

int
shard_log_write(log_handle *logh,
                key         tuple_key,
                message     msg,
                uint64      memtable_generation,
                uint64      leaf_generation)
{
   debug_assert(key_is_user_key(tuple_key));
   debug_assert(memtable_generation != INVALID_LOG_GENERATION);
   debug_assert(leaf_generation != INVALID_LOG_GENERATION);

   shard_log        *log = (shard_log *)logh;
   cache            *cc  = log->cc;
   merge_accumulator log_blob;
   bool32            log_blob_inited = FALSE;

   uint64 max_entry_size =
      shard_log_page_size(log->cfg) - sizeof(shard_log_hdr);
   if (message_is_blob(msg)
       || max_entry_size < log_entry_required_capacity(tuple_key, msg))
   {
      merge_accumulator_init(&log_blob, platform_get_heap_id());
      platform_status rc;
      if (message_is_blob(msg)) {
         rc =
            message_clone(&log->cfg->blob_cfg, cc, &log->mini, msg, &log_blob);
      } else {
         rc = message_to_blob(
            &log->cfg->blob_cfg, cc, &log->mini, msg, &log_blob);
      }
      if (!SUCCESS(rc)) {
         merge_accumulator_deinit(&log_blob);
         return rc.r;
      }
      msg             = merge_accumulator_to_message(&log_blob);
      log_blob_inited = TRUE;
   }

   shard_log_thread_data *thread_data =
      shard_log_get_thread_data(log, platform_get_tid());

   uint64 page_size      = shard_log_page_size(log->cfg);
   uint64 new_entry_size = log_entry_required_capacity(tuple_key, msg);
   debug_assert(new_entry_size <= page_size - sizeof(shard_log_hdr));

   // Full: turn the staged image into a page and start a fresh one.
   if (page_size - thread_data->offset < new_entry_size) {
      platform_status rc =
         shard_log_graduate_buffer(log, thread_data, SHARD_LOG_CLOSE_NONE);
      if (!SUCCESS(rc)) {
         if (log_blob_inited) {
            merge_accumulator_deinit(&log_blob);
         }
         return rc.r;
      }
   }

   log_entry *cursor = (log_entry *)(thread_data->buf + thread_data->offset);
   cursor->memtable_generation = memtable_generation;
   cursor->leaf_generation     = leaf_generation;
   copy_tuple_to_ondisk_tuple(&cursor->tuple, tuple_key, msg);

   ((shard_log_hdr *)thread_data->buf)->num_entries++;

   thread_data->offset += new_entry_size;
   thread_data->has_records = TRUE;
   debug_assert(thread_data->offset <= page_size);

   if (log_blob_inited) {
      /*
       * The blob holds this record's value, so it belongs to the same group as
       * the record: closing the group has to wait for these pages too, or the
       * group could be declared durable while the value it refers to is not.
       *
       * No close can slip in between staging the record above and recording the
       * blob here, because a close excludes writers for its whole duration.
       */
      platform_mutex_lock(&log->wbset_lock);
      platform_status rc = blob_writeback(cc, message_slice(msg), &log->wbset);
      platform_mutex_unlock(&log->wbset_lock);
      merge_accumulator_deinit(&log_blob);
      if (!SUCCESS(rc)) {
         return rc.r;
      }
   }

   return 0;
}

/*
 * shard_log_close_group --
 *
 *     Hand over every currently staged per-thread page (bounded by MAX_THREADS;
 *     it does not walk the historical log) and end the group, with `how`
 *     deciding whether the stream ends with it.  A terminal record (where there
 *     is room) and a checksum make each page readable by
 *     shard_log_iterator_init().
 *
 *     Issues the writes but does not wait for them; callers that need
 *     durability follow with the stream's writeback set.
 *
 *     The caller must prevent concurrent shard_log_write() and close calls.
 */
static platform_status
shard_log_close_group(shard_log *log, shard_log_close how)
{
   platform_status result = STATUS_OK;

   /*
    * Flush whatever each thread had staged but not yet graduated, and close the
    * group.  The caller guarantees the stream is quiescent, so no thread can be
    * mid-append and none of these buffers can grow under us -- which is also
    * what makes the group boundary exact, with no waiting: a log cut runs under
    * the memtable insert lock held exclusively, so every record that will ever
    * belong to this stream has already been staged by now.
    *
    * The terminator has to ride on the *last* page written, so find that page
    * up front rather than discovering it as we go.
    *
    * Safe to re-enter after a failed attempt, which callers rely on: buffers
    * already handed over were reset and are skipped, the one that failed and
    * everything after it are untouched, and the terminator is only written once
    * every buffer is out -- so a retry resumes rather than duplicating.
    */
   threadid last = MAX_THREADS;
   for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      if (shard_log_get_thread_data(log, thr_i)->offset > sizeof(shard_log_hdr))
      {
         last = thr_i;
      }
   }

   for (threadid thr_i = 0; thr_i < last; thr_i++) {
      platform_status rc = shard_log_graduate_buffer(
         log, shard_log_get_thread_data(log, thr_i), SHARD_LOG_CLOSE_NONE);
      if (!SUCCESS(rc)) {
         /*
          * Stop here.  One failure already dooms the group -- it will not be
          * terminated below, so replay discards it whole -- which makes writing
          * the rest pointless.  Worse, the only way the hand-over fails is that
          * the stream is out of space, so pressing on would spend what little
          * remains on pages nobody will ever read.
          */
         platform_error_log("shard_log_close_group: failed to flush the staged "
                            "log page of thread %lu: %s\n",
                            thr_i,
                            platform_status_to_string(rc));
         result = rc;
         break;
      }
   }

   /*
    * Close the group -- but only if every page of it was written.  A failed
    * flush above leaves the group short of some thread's records, while
    * group_page_count (bumped only on a successful hand-over) still matches the
    * pages that did make it.  Terminating now would therefore declare a count
    * that replay could satisfy, and the group would be accepted with a hole in
    * it.  Leaving it unterminated gets it rejected whole, which is the outcome
    * we want: losing a group beats replaying a broken one.
    *
    * If nothing was staged, a page of its own carries the terminator.  That is
    * needed when the group has pages to account for, and also whenever the
    * stream is ending: the end-of-stream mark has to go *somewhere*, and after
    * a make_durable the final group is routinely empty, with no page of its own
    * to ride on.  Without this the stream would look truncated to replay.
    *
    * The remaining case -- an empty group that is not ending the stream -- has
    * nothing to record, so it writes nothing and the group does not advance.
    */
   if (last == MAX_THREADS
       && (log->group_page_count > 0 || how == SHARD_LOG_CLOSE_STREAM))
   {
      // Use thread 0's buffer to write a closing page.
      last = 0;
   }
   if (!SUCCESS(result)) {
      platform_error_log("shard_log_close_group: leaving group %lu "
                         "unterminated after a failed flush; it will not be "
                         "replayed\n",
                         log->group_id);
   } else if (last != MAX_THREADS) {
      platform_status rc = shard_log_graduate_buffer(
         log, shard_log_get_thread_data(log, last), how);
      if (!SUCCESS(rc)) {
         platform_error_log("shard_log_close_group: failed to close group %lu: "
                            "%s\n",
                            log->group_id,
                            platform_status_to_string(rc));
         result = rc;
      }
   }

   return result;
}

/*
 *-----------------------------------------------------------------------------
 * shard_log_close_group_durably --
 *
 *      Close the current group, wait for its pages to reach the device, and
 *      take a durable barrier.  Shared by make_durable and seal, which differ
 *      only in whether the stream ends with the group.
 *-----------------------------------------------------------------------------
 */
static platform_status
shard_log_close_group_durably(shard_log *log, shard_log_close how)
{
   platform_status rc = shard_log_close_group(log, how);
   if (!SUCCESS(rc)) {
      // Left unclosed, so nothing can be promised durable; the group stays open
      // and a later attempt can finish it.
      return rc;
   }

   platform_mutex_lock(&log->wbset_lock);
   rc = writeback_set_wait(&log->wbset);
   if (SUCCESS(rc)) {
      rc = writeback_set_make_durable(&log->wbset);
   }
   if (SUCCESS(rc)) {
      writeback_set_reset(&log->wbset);
   }
   platform_mutex_unlock(&log->wbset_lock);

   return rc;
}

/*
 *-----------------------------------------------------------------------------
 * shard_log_make_durable --
 *
 *      Make everything written so far durable and leave the stream open; the
 *      next write starts a new group.
 *
 *      See log_make_durable_fn for the exclusion the caller must provide.
 *-----------------------------------------------------------------------------
 */
static platform_status
shard_log_make_durable(log_handle *logh)
{
   shard_log      *log = (shard_log *)logh;
   platform_status rc =
      shard_log_close_group_durably(log, SHARD_LOG_CLOSE_GROUP);
   if (!SUCCESS(rc)) {
      return rc;
   }

   /*
    * Durable.  Start the next group -- unless this one never had any pages, in
    * which case no group was closed and advancing would leave a hole in the
    * numbering that replay reads as a lost group.
    *
    * Safe to do unlocked: the caller excludes writers, so nothing is
    * graduating, and the receipts were consumed above.
    */
   if (log->group_page_count != 0) {
      log->group_id++;
      log->group_page_count = 0;
   }
   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 * shard_log_seal --
 *
 *      Finish the stream durably: close its final group, mark it as the end of
 *      the stream so replay can tell a complete stream from a truncated one,
 *      and make all of it durable.
 *
 *      Sealing makes its pages durable for the same reason make_durable does --
 *      there is no point finishing a stream that a crash could still lose --
 *      and callers no longer pair it with a barrier of their own.
 *
 *      The handle remains valid and must be released with shard_log_deinit().
 *-----------------------------------------------------------------------------
 */
platform_status
shard_log_seal(log_handle *logh)
{
   shard_log *log = (shard_log *)logh;
   return shard_log_close_group_durably(log, SHARD_LOG_CLOSE_STREAM);
}

/*
 *-----------------------------------------------------------------------------
 * shard_log_deinit --
 *
 *      Release the stream's in-memory resources and free the handle.  Writes
 *      nothing, so it cannot fail.
 *
 *      Releasing the mini-allocator's unused per-batch reserve ensures no
 *      future allocation touches this stream.  The caller already holds the
 *      stream's identity (captured at creation) and frees the on-disk extents
 *      separately via shard_log_dec_ref().
 *-----------------------------------------------------------------------------
 */
static void
shard_log_deinit(log_handle *logh)
{
   shard_log *log = (shard_log *)logh;

   mini_release(&log->mini);
   writeback_set_deinit(&log->wbset);
   platform_mutex_destroy(&log->wbset_lock);
   platform_free(log->heap_id, log->thread_buffers);
   platform_free(log->heap_id, log);
}

void
shard_log_dec_ref(cache *cc, const log_head *segment)
{
   if (segment->meta_addr == 0) {
      return;
   }
   refcount ref = mini_dec_ref(cc, segment->meta_addr, PAGE_TYPE_LOG);
   platform_assert(ref == 0);
}

log_head
shard_log_get_head(log_handle *logh)
{
   shard_log *log = (shard_log *)logh;
   return (log_head){
      .addr      = log->addr,
      .meta_addr = log->meta_head,
      .magic     = log->magic,
   };
}

bool32
shard_log_valid(shard_log_config *cfg, page_handle *page, uint64 magic)
{
   shard_log_hdr *hdr = (shard_log_hdr *)page->data;
   return hdr->magic == magic
          && platform_checksum_is_equal(hdr->checksum,
                                        shard_log_checksum(cfg, page));
}

uint64
shard_log_next_extent_addr(shard_log_config *cfg, page_handle *page)
{
   shard_log_hdr *hdr = (shard_log_hdr *)page->data;
   return hdr->next_extent_addr;
}

/* The base address of the extent holding addr. */
static uint64
shard_log_extent_base(cache *cc, uint64 addr)
{
   return allocator_config_extent_base_addr(
      allocator_get_config(cache_get_allocator(cc)), addr);
}

/* Visitor for shard_log_for_each_extent(); a failure abandons the walk. */
typedef platform_status (*shard_log_extent_fn)(void *arg, uint64 extent_addr);

/*
 * Could addr be the base address of a log extent on this device?
 *
 * next_extent_addr is read out of a page that a crash may have left holding
 * anything at all, so it is checked against the device geometry before it is
 * followed; once the walk lands on a page, its magic and checksum are what
 * vouch for the contents.
 *
 * Deliberately not a refcount test.  Crash recovery walks a stream precisely in
 * order to rebuild the refcount map, so the walk must not consult the map it is
 * about to populate -- and before allocator_recovery_begin() there is no map to
 * consult: rc_allocator leaves it NULL until then, so asking would fault.
 */
static bool32
shard_log_valid_extent_addr(cache *cc, shard_log_config *cfg, uint64 addr)
{
   uint64 extent_size = shard_log_extent_size(cfg);
   uint64 capacity    = allocator_get_capacity(cache_get_allocator(cc));

   return addr != 0 && addr % extent_size == 0
          && addr <= capacity - extent_size;
}

/*
 * Snapshot which complete pages of an extent may be read from the backing
 * store.  Query every page rather than assuming a readable prefix: the current
 * file backend has prefix-shaped EOF, but the abstract interface also permits
 * backends with holes.
 */
static platform_status
shard_log_extent_readable_pages(cache            *cc,
                                shard_log_config *cfg,
                                uint64            extent_addr,
                                bool32 readable_pages[MAX_PAGES_PER_EXTENT])
{
   uint64 pages_per_extent = shard_log_pages_per_extent(cfg);
   uint64 page_size        = shard_log_page_size(cfg);
   platform_assert(pages_per_extent <= MAX_PAGES_PER_EXTENT);

   for (uint64 i = 0; i < pages_per_extent; i++) {
      uint64          page_addr = extent_addr + i * page_size;
      platform_status rc =
         cache_range_is_readable(cc, page_addr, page_size, &readable_pages[i]);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   return STATUS_OK;
}

/* Issue every safe prefetch before the caller starts waiting on cache_get(). */
static void
shard_log_prefetch_readable_pages(
   cache            *cc,
   shard_log_config *cfg,
   uint64            extent_addr,
   const bool32      readable_pages[MAX_PAGES_PER_EXTENT])
{
   uint64 pages_per_extent = shard_log_pages_per_extent(cfg);
   uint64 page_size        = shard_log_page_size(cfg);
   bool32 all_readable     = TRUE;

   for (uint64 i = 0; i < pages_per_extent; i++) {
      all_readable &= readable_pages[i];
   }
   if (all_readable) {
      cache_prefetch(cc, extent_addr, PAGE_TYPE_LOG);
      return;
   }

   for (uint64 i = 0; i < pages_per_extent; i++) {
      if (readable_pages[i]) {
         cache_prefetch_page(cc, extent_addr + i * page_size, PAGE_TYPE_LOG);
      }
   }
}

/*
 * The next-extent link of the extent at extent_addr, or 0 if the chain ends
 * here.
 *
 * Taken from the last page of the extent that validates, not the first: the
 * link is stamped into each page as that page is written, and a page written
 * early can predate the allocation of the extent that follows, so only the
 * latest page's copy is guaranteed to name it.  Backing-store readability only
 * decides whether it is safe to issue a read; magic and checksum still decide
 * whether that page contributes a link.
 */
static platform_status
shard_log_extent_next_link(cache            *cc,
                           shard_log_config *cfg,
                           uint64            extent_addr,
                           uint64            magic,
                           uint64           *next_extent_addr)
{
   uint64 pages_per_extent = shard_log_pages_per_extent(cfg);
   uint64 page_size        = shard_log_page_size(cfg);
   *next_extent_addr       = 0;
   bool32 readable_pages[MAX_PAGES_PER_EXTENT];

   platform_status rc =
      shard_log_extent_readable_pages(cc, cfg, extent_addr, readable_pages);
   if (!SUCCESS(rc)) {
      return rc;
   }

   for (uint64 i = 0; i < pages_per_extent; i++) {
      uint64 page_addr = extent_addr + i * page_size;
      if (!readable_pages[i]) {
         continue;
      }

      page_handle *page = cache_get(cc, page_addr, TRUE, PAGE_TYPE_LOG);
      if (shard_log_valid(cfg, page, magic)) {
         *next_extent_addr = shard_log_next_extent_addr(cfg, page);
      }
      cache_unget(cc, page);
   }
   return STATUS_OK;
}

/*
 * Visit every data extent of a stream, in order.  See
 * shard_log_valid_extent_addr() for why this consults no refcounts, and
 * shard_log_recover_allocations() in shard_log.h for what recovery does with
 * it.
 *
 * The visit budget is not a policy limit but a corruption backstop: a garbled
 * link that happens to name an earlier extent of this same stream would carry
 * the stream's own magic, so the per-page checks cannot rule out a cycle. A
 * stream cannot hold more extents than the device has.
 */
static platform_status
shard_log_for_each_extent(cache              *cc,
                          shard_log_config   *cfg,
                          log_head            head,
                          shard_log_extent_fn fn,
                          void               *arg)
{
   uint64 budget = allocator_get_capacity(cache_get_allocator(cc))
                   / shard_log_extent_size(cfg);
   uint64 extent_addr = head.addr;

   while (shard_log_valid_extent_addr(cc, cfg, extent_addr)) {
      if (budget-- == 0) {
         platform_error_log("shard_log_for_each_extent: stream from %lu has "
                            "more extents than the device holds; its "
                            "next-extent chain is corrupt\n",
                            head.addr);
         return STATUS_INVALID_STATE;
      }

      /*
       * Visited before the read that follows the chain onwards, not after:
       * cache_get() requires a page's extent to be allocated, and to a map
       * being rebuilt it is not yet.  Recording the reference is what makes the
       * extent readable.  Same reason mini_recover_allocations() has a "before"
       * hook.
       *
       * This deliberately records an entirely unreadable successor too.  The
       * last written log page names the mini allocator's unused reserve, and
       * that stale link remains reachable until replay is finished.  Protecting
       * the reserve prevents replay allocations from reusing it as a non-log
       * extent before an iterator follows the link and rejects its pages.  The
       * root-only rebuild after replay reclaims it.
       */
      platform_status rc = fn(arg, extent_addr);
      if (!SUCCESS(rc)) {
         return rc;
      }
      rc = shard_log_extent_next_link(
         cc, cfg, extent_addr, head.magic, &extent_addr);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   return STATUS_OK;
}

static platform_status
shard_log_record_extent_reference(void *arg, uint64 extent_addr)
{
   return allocator_recovery_record_reference(
      (allocator *)arg, extent_addr, PAGE_TYPE_LOG);
}

static platform_status
shard_log_recover_blob_allocations(cache            *cc,
                                   shard_log_config *cfg,
                                   platform_heap_id  hid,
                                   log_head          head)
{
   if (head.addr == 0) {
      return STATUS_OK; // no such log
   }

   log_iterator *itor = shard_log_iterator_create(cc, cfg, hid, head);
   if (itor == NULL) {
      platform_error_log(
         "shard_log_recover_blob_allocations: could not read the stream at "
         "%lu\n",
         head.addr);
      return STATUS_NO_MEMORY;
   }

   platform_status rc = STATUS_OK;
   while (SUCCESS(rc) && log_iterator_can_next(itor)) {
      key     tuple_key;
      message msg;
      log_iterator_curr(itor, &tuple_key, &msg);
      if (message_is_blob(msg)) {
         rc = blob_recover_allocations(cc, message_slice(msg));
      }
      if (SUCCESS(rc)) {
         rc = log_iterator_next(itor);
      }
   }

   log_iterator_deinit(itor);
   return rc;
}

platform_status
shard_log_recover_allocations(cache            *cc,
                              shard_log_config *cfg,
                              platform_heap_id  hid,
                              log_head          head)
{
   if (head.addr == 0) {
      return STATUS_OK; // no such log
   }

   /*
    * The metadata head sits in an extent of its own, allocated before the mini
    * allocator that owns the data extents (see shard_log_init()), so the
    * page-header chain never reaches it.  It has to be recorded on its own or
    * the extent is left looking free while the durable record still names it.
    */
   allocator      *al        = cache_get_allocator(cc);
   uint64          meta_base = shard_log_extent_base(cc, head.meta_addr);
   platform_status rc        = shard_log_record_extent_reference(al, meta_base);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = shard_log_for_each_extent(
      cc, cfg, head, shard_log_record_extent_reference, al);
   if (!SUCCESS(rc)) {
      return rc;
   }

   return shard_log_recover_blob_allocations(cc, cfg, hid, head);
}

/*
 * Bytes appended to the stream so far.  The mini-allocator already tracks the
 * extents it has handed out across all of the stream's batches (data and blob),
 * counting each as it is reserved -- the same measure memtable_is_full() uses
 * for a memtable.  Subtracting the fixed overhead recorded at init means a
 * fresh stream reports 0, so a caller comparing against a threshold cannot be
 * tricked into rotating a stream that has had nothing written to it.
 */
uint64
shard_log_get_size(log_handle *logh)
{
   shard_log *log = (shard_log *)logh;
   return (mini_num_extents(&log->mini) - log->initial_extents)
          * shard_log_extent_size(log->cfg);
}

/*
 * Exact emptiness without a shared counter on the append path.  Each writer
 * owns its thread-local flag, and the log interface requires callers to exclude
 * writers while taking this snapshot.
 */
static bool32
shard_log_is_empty(log_handle *logh)
{
   shard_log *log = (shard_log *)logh;
   for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      if (shard_log_get_thread_data(log, thr_i)->has_records) {
         return FALSE;
      }
   }
   return TRUE;
}

static log_ops shard_log_ops = {
   .write        = shard_log_write,
   .make_durable = shard_log_make_durable,
   .seal         = shard_log_seal,
   .deinit       = shard_log_deinit,
   .head         = shard_log_get_head,
   .is_empty     = shard_log_is_empty,
   .size         = shard_log_get_size,
};

static platform_status
shard_log_init(shard_log        *log,
               cache            *cc,
               shard_log_config *cfg,
               platform_heap_id  hid)
{
   memset(log, 0, sizeof(shard_log));
   log->cc        = cc;
   log->cfg       = cfg;
   log->heap_id   = hid;
   log->super.ops = &shard_log_ops;

   uint64 magic_idx = __sync_fetch_and_add(&shard_log_magic_idx, 1);
   log->magic = platform_checksum64(&magic_idx, sizeof(uint64), cfg->seed);

   /*
    * One page-sized staging buffer per thread. Allocated before anything that
    * would need undoing, so a failure here can simply return.
    */
   uint64 page_size = shard_log_page_size(cfg);
   log->thread_buffers =
      TYPED_ARRAY_MALLOC(hid, log->thread_buffers, MAX_THREADS * page_size);
   if (log->thread_buffers == NULL) {
      platform_error_log("shard_log_init: failed to allocate %lu bytes of log "
                         "staging buffers\n",
                         MAX_THREADS * page_size);
      return STATUS_NO_MEMORY;
   }
   log->group_id         = 0;
   log->group_page_count = 0;
   platform_status mrc   = platform_mutex_init(&log->wbset_lock, 0, hid);
   if (!SUCCESS(mrc)) {
      platform_error_log("shard_log_init: failed to init the writeback-set "
                         "lock: %s\n",
                         platform_status_to_string(mrc));
      platform_free(hid, log->thread_buffers);
      return mrc;
   }
   writeback_set_init(&log->wbset, cc, hid);
   for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      shard_log_thread_data *thread_data =
         shard_log_get_thread_data(log, thr_i);
      thread_data->buf = log->thread_buffers + thr_i * page_size;
      shard_log_reset_buffer(log, thread_data);
   }

   allocator      *al = cache_get_allocator(cc);
   platform_status rc = allocator_alloc(al, &log->meta_head, PAGE_TYPE_LOG);
   platform_assert_status_ok(rc);

   log->addr = mini_init_with_types(&log->mini,
                                    cc,
                                    log->meta_head,
                                    0,
                                    NUM_BLOB_BATCHES + 1,
                                    PAGE_TYPE_LOG,
                                    shard_log_page_type_table);
   // platform_default_log("addr: %lu meta_head: %lu\n", log->addr,
   // log->meta_head);

   // Baseline for shard_log_get_size(): the stream's fixed overhead.
   log->initial_extents = mini_num_extents(&log->mini);

   return STATUS_OK;
}

log_handle *
shard_log_create(cache *cc, shard_log_config *cfg, platform_heap_id hid)
{
   shard_log *slog = TYPED_MALLOC(hid, slog);
   if (slog == NULL) {
      platform_error_log("shard_log_create: failed to allocate shard_log\n");
      return NULL;
   }
   // The heap is remembered in the log so that log_deinit() can free the handle
   // and its staging buffers without the caller touching platform_free().
   platform_status rc = shard_log_init(slog, cc, cfg, hid);
   if (!SUCCESS(rc)) {
      platform_error_log("shard_log_create: shard_log_init failed: %s\n",
                         platform_status_to_string(rc));
      platform_free(hid, slog);
      return NULL;
   }
   return (log_handle *)slog;
}

int
shard_log_compare(const void *p1, const void *p2, void *unused)
{
   log_entry **le1 = (log_entry **)p1;
   log_entry **le2 = (log_entry **)p2;

   if ((*le1)->memtable_generation < (*le2)->memtable_generation) {
      return -1;
   }
   if ((*le1)->memtable_generation > (*le2)->memtable_generation) {
      return 1;
   }
   if ((*le1)->leaf_generation < (*le2)->leaf_generation) {
      return -1;
   }
   if ((*le1)->leaf_generation > (*le2)->leaf_generation) {
      return 1;
   }
   return 0;
}

void
shard_log_iterator_curr(iterator *itorh, key *curr_key, message *msg)
{
   shard_log_iterator *itor = (shard_log_iterator *)itorh;
   *curr_key                = log_entry_key(itor->entries[itor->pos]);
   *msg = log_entry_message(itor->cc, itor->entries[itor->pos]);
}

static void
shard_log_iterator_curr_generations(log_iterator *itorh,
                                    uint64       *memtable_generation,
                                    uint64       *leaf_generation)
{
   shard_log_iterator *itor = (shard_log_iterator *)itorh;
   platform_assert(itor->pos < itor->num_entries);
   *memtable_generation = itor->entries[itor->pos]->memtable_generation;
   *leaf_generation     = itor->entries[itor->pos]->leaf_generation;
}

bool32
shard_log_iterator_can_prev(iterator *itorh)
{
   shard_log_iterator *itor = (shard_log_iterator *)itorh;
   return itor->pos >= 0;
}

bool32
shard_log_iterator_can_next(iterator *itorh)
{
   shard_log_iterator *itor = (shard_log_iterator *)itorh;
   return itor->pos < itor->num_entries;
}

platform_status
shard_log_iterator_next(iterator *itorh)
{
   shard_log_iterator *itor = (shard_log_iterator *)itorh;
   itor->pos++;
   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 * shard_log_config_init --
 *
 *      Initialize shard_log config values
 *-----------------------------------------------------------------------------
 */
void
shard_log_config_init(shard_log_config *log_cfg,
                      cache_config     *cache_cfg,
                      data_config      *data_cfg)
{
   ZERO_CONTENTS(log_cfg);
   log_cfg->cache_cfg = cache_cfg;
   log_cfg->data_cfg  = data_cfg;
   log_cfg->seed      = HASH_SEED;
   log_cfg->blob_cfg  = (blob_build_config){
       .extent_batch  = 1,
       .page_batch    = 2,
       .subpage_batch = 3,
       .alignment     = cache_config_page_size(cache_cfg),
   };
}

void
shard_log_print(shard_log *log)
{
   cache            *cc               = log->cc;
   uint64            extent_addr      = log->addr;
   shard_log_config *cfg              = log->cfg;
   uint64            magic            = log->magic;
   data_config      *dcfg             = cfg->data_cfg;
   uint64            pages_per_extent = shard_log_pages_per_extent(cfg);
   allocator        *al               = cache_get_allocator(cc);

   while (extent_addr != 0 && allocator_get_refcount(al, extent_addr) > 0) {
      cache_prefetch(cc, extent_addr, PAGE_TYPE_LOG);
      uint64 next_extent_addr = 0;
      for (uint64 i = 0; i < pages_per_extent; i++) {
         uint64       page_addr = extent_addr + i * shard_log_page_size(cfg);
         page_handle *page      = cache_get(cc, page_addr, TRUE, PAGE_TYPE_LOG);
         if (shard_log_valid(cfg, page, magic)) {
            next_extent_addr = shard_log_next_extent_addr(cfg, page);
            for (log_entry *le = first_log_entry(page->data);
                 !terminal_log_entry(cfg, page->data, le);
                 le = log_entry_next(le))
            {
               platform_default_log(
                  "%s -- %s%s : memtable=%lu leaf=%lu\n",
                  key_string(dcfg, log_entry_key(le)),
                  log_entry_message_is_blob(le) ? "(blob) " : "",
                  message_string(dcfg, log_entry_message(cc, le)),
                  le->memtable_generation,
                  le->leaf_generation);
            }
         }
         cache_unget(cc, page);
      }
      extent_addr = next_extent_addr;
   }
}

static void
shard_log_iterator_deinit(log_iterator *itorh)
{
   shard_log_iterator *itor = (shard_log_iterator *)itorh;
   platform_heap_id    hid  = itor->heap_id;
   if (itor->contents != NULL) {
      platform_free(hid, itor->contents);
   }
   if (itor->entries != NULL) {
      platform_free(hid, itor->entries);
   }
   platform_free(hid, itor); // the handle, from shard_log_iterator_create()
}

const static iterator_ops shard_log_iterator_ops = {
   .curr     = shard_log_iterator_curr,
   .can_prev = shard_log_iterator_can_prev,
   .can_next = shard_log_iterator_can_next,
   .next     = shard_log_iterator_next,
   .print    = NULL,
};

/*
 * TRUE only when the accepted records run to a page marked as ending a sealed
 * stream.  FALSE for a live stream, and for one whose tail was lost -- in which
 * case the records yielded are still a valid prefix, but nothing written after
 * this stream may be replayed on top of them.
 */
static bool32
shard_log_iterator_stream_complete(log_iterator *itorh)
{
   shard_log_iterator *itor = (shard_log_iterator *)itorh;
   return itor->stream_complete;
}

const static log_iterator_ops shard_log_log_iterator_ops = {
   .curr_generations = shard_log_iterator_curr_generations,
   .deinit           = shard_log_iterator_deinit,
   .stream_complete  = shard_log_iterator_stream_complete,
};

static platform_status
shard_log_iterator_init(cache              *cc,
                        shard_log_config   *cfg,
                        platform_heap_id    hid,
                        uint64              addr,
                        uint64              magic,
                        shard_log_iterator *itor)
{
   page_handle    *page;
   uint64          i;
   uint64          pages_per_extent = shard_log_pages_per_extent(cfg);
   uint64          page_addr;
   uint64          num_valid_pages = 0;
   uint64          extent_addr;
   uint64          next_extent_addr;
   uint64          contents_size;
   platform_status rc;
   bool32          readable_pages[MAX_PAGES_PER_EXTENT];

   memset(itor, 0, sizeof(shard_log_iterator));
   itor->super.super.ops = &shard_log_iterator_ops;     // generic iterator
   itor->super.ops       = &shard_log_log_iterator_ops; // log_iterator
   itor->heap_id         = hid;
   itor->cc              = cc;
   itor->cfg             = cfg;
   allocator *al         = cache_get_allocator(cc);

   /*
    * First pass: work out how much of the stream is replayable.
    *
    * Only whole groups may be replayed, and only an unbroken run of them from
    * the start: a group that is intact but follows a broken one cannot be
    * applied, because the records in between are missing and the result would
    * not be a prefix of anything that happened.
    *
    * A group is intact when the page count declared by its terminator matches
    * the number of its pages actually present.  Groups close before the next
    * one opens and pages are allocated in order, so a group's pages are
    * contiguous in the traversal and the replayable portion is a prefix of it.
    * We therefore only have to track a run at a time, and count pages.
    */
   uint64 group_id       = 0; // the run currently being tallied
   uint64 expect_group   = 0; // ids must run 0, 1, 2, ... with no gaps
   bool32 in_group       = FALSE;
   uint64 group_pages    = 0; // pages of it seen
   uint64 group_entries  = 0;
   uint64 group_declared = 0; // pages its terminator claims, 0 if unseen
   // whether its terminator also says the stream ends here
   bool32 group_ends_stream = FALSE;
   bool32 broken            = FALSE; // hit a group we cannot replay

   /*
    * The refcount gate is what stops the walk, and it is load-bearing rather
    * than defensive: the last page of a finished stream names a next extent
    * that shard_log_deinit() then released (mini_release() drops the unused
    * per-batch reserve), so the chain outlives the extent it points at.
    * Reading there would break cache_get()'s rule that a page belong to an
    * allocated extent.
    *
    * It works during crash recovery too, even though the map is rebuilt from
    * scratch: the rebuild walk (shard_log_recover_allocations()) runs first and
    * records the stream's extents plus its still-reachable successor reserve.
    * The range bitmap below prevents reads of absent pages in that reserve (or
    * in a partial extent); magic and checksum reject readable non-log contents.
    */
   extent_addr = addr;
   while (!broken && extent_addr != 0
          && allocator_get_refcount(al, extent_addr) > 0)
   {
      rc =
         shard_log_extent_readable_pages(cc, cfg, extent_addr, readable_pages);
      if (!SUCCESS(rc)) {
         return rc;
      }
      shard_log_prefetch_readable_pages(cc, cfg, extent_addr, readable_pages);

      next_extent_addr = 0;
      for (i = 0; i < pages_per_extent; i++) {
         page_addr = extent_addr + i * shard_log_page_size(cfg);
         if (!readable_pages[i]) {
            continue;
         }
         page = cache_get(cc, page_addr, TRUE, PAGE_TYPE_LOG);
         if (!shard_log_valid(cfg, page, magic)) {
            /*
             * A page that was never written, or whose write was lost.  Keep
             * scanning the extent rather than stopping here: the group this
             * page belongs to is now short of its declared count and will be
             * rejected on that basis, which is the check that matters.
             */
            cache_unget(cc, page);
            continue;
         }
         shard_log_hdr *hdr = (shard_log_hdr *)page->data;

         if (in_group && hdr->group_id != group_id) {
            // The run ended; judge it before starting the next.
            if (group_declared != 0 && group_pages == group_declared) {
               num_valid_pages += group_pages;
               itor->num_entries += group_entries;
               expect_group          = group_id + 1;
               itor->stream_complete = group_ends_stream;
            } else {
               broken = TRUE;
            }
            in_group = FALSE;
         }
         if (broken) {
            cache_unget(cc, page);
            break;
         }
         if (!in_group) {
            /*
             * Group ids are dense, so a jump means a whole group left no trace
             * on disk -- every one of its pages was lost.  Its own count cannot
             * report that (there is nothing left to count), so the sequence has
             * to.  Replaying across such a hole would skip records and produce
             * a state that never existed.
             */
            if (hdr->group_id != expect_group) {
               platform_error_log("shard_log_iterator_init: log skips from "
                                  "group %lu to %lu; discarding the rest\n",
                                  expect_group,
                                  hdr->group_id);
               cache_unget(cc, page);
               broken = TRUE;
               break;
            }
            in_group          = TRUE;
            group_id          = hdr->group_id;
            group_pages       = 0;
            group_entries     = 0;
            group_declared    = 0;
            group_ends_stream = FALSE;
         }
         group_pages++;
         group_entries += hdr->num_entries;
         if (hdr->pages_in_group != 0) {
            debug_assert(
               group_declared == 0, "group %lu has two terminators", group_id);
            group_declared =
               hdr->pages_in_group & SHARD_LOG_PAGES_IN_GROUP_MASK;
            group_ends_stream =
               (hdr->pages_in_group & SHARD_LOG_END_OF_STREAM) != 0;
         }
         next_extent_addr = shard_log_next_extent_addr(cfg, page);
         cache_unget(cc, page);
      }
      extent_addr = next_extent_addr;
   }
   if (!broken && in_group) {
      if (group_declared != 0 && group_pages == group_declared) {
         num_valid_pages += group_pages;
         itor->num_entries += group_entries;
         itor->stream_complete = group_ends_stream;
      }
      // Otherwise the stream ends in an unclosed group: discard it.
   }

   contents_size = num_valid_pages * shard_log_page_size(cfg);
   if (contents_size != 0) {
      itor->contents = TYPED_ARRAY_MALLOC(hid, itor->contents, contents_size);
      if (itor->contents == NULL) {
         platform_error_log("shard_log_iterator_init: failed to allocate "
                            "contents buffer of %lu bytes\n",
                            contents_size);
         return STATUS_NO_MEMORY;
      }
   }
   if (itor->num_entries != 0) {
      itor->entries = TYPED_ARRAY_MALLOC(hid, itor->entries, itor->num_entries);
      if (itor->entries == NULL) {
         platform_error_log("shard_log_iterator_init: failed to allocate "
                            "entries array for %lu entries\n",
                            itor->num_entries);
         platform_free(hid, itor->contents);
         return STATUS_NO_MEMORY;
      }
   }

   /*
    * Second pass: copy the records out of the pages the first pass accepted.
    * Those are the first num_valid_pages valid pages of the traversal, since
    * the replayable portion is a prefix.
    */
   log_entry *cursor      = (log_entry *)itor->contents;
   uint64     entry_idx   = 0;
   uint64     pages_taken = 0;
   extent_addr            = addr;
   while (pages_taken < num_valid_pages && extent_addr != 0
          && allocator_get_refcount(al, extent_addr) > 0)
   {
      rc =
         shard_log_extent_readable_pages(cc, cfg, extent_addr, readable_pages);
      if (!SUCCESS(rc)) {
         platform_free(hid, itor->entries);
         platform_free(hid, itor->contents);
         itor->entries  = NULL;
         itor->contents = NULL;
         return rc;
      }
      shard_log_prefetch_readable_pages(cc, cfg, extent_addr, readable_pages);

      next_extent_addr = 0;
      for (i = 0; i < pages_per_extent && pages_taken < num_valid_pages; i++) {
         page_addr = extent_addr + i * shard_log_page_size(cfg);
         if (!readable_pages[i]) {
            continue;
         }
         page = cache_get(cc, page_addr, TRUE, PAGE_TYPE_LOG);
         if (!shard_log_valid(cfg, page, magic)) {
            cache_unget(cc, page);
            continue;
         }
         pages_taken++;
         for (log_entry *le = first_log_entry(page->data);
              !terminal_log_entry(cfg, page->data, le);
              le = log_entry_next(le))
         {
            memmove(cursor, le, sizeof_log_entry(le));
            itor->entries[entry_idx] = cursor;
            entry_idx++;
            cursor = log_entry_next(cursor);
         }
         next_extent_addr = shard_log_next_extent_addr(cfg, page);
         cache_unget(cc, page);
      }
      extent_addr = next_extent_addr;
   }

   debug_assert(entry_idx == itor->num_entries);

   // sort by generation
   if (itor->num_entries != 0) {
      log_entry *tmp;
      platform_sort_slow(itor->entries,
                         itor->num_entries,
                         sizeof(log_entry *),
                         shard_log_compare,
                         NULL,
                         &tmp);
   }

   return STATUS_OK;
}

log_iterator *
shard_log_iterator_create(cache            *cc,
                          shard_log_config *cfg,
                          platform_heap_id  hid,
                          log_head          head)
{
   shard_log_iterator *itor = TYPED_MALLOC(hid, itor);
   if (itor == NULL) {
      platform_error_log("shard_log_iterator_create: failed to allocate "
                         "shard_log_iterator\n");
      return NULL;
   }
   platform_status rc =
      shard_log_iterator_init(cc, cfg, hid, head.addr, head.magic, itor);
   if (!SUCCESS(rc)) {
      platform_error_log("shard_log_iterator_create: shard_log_iterator_init "
                         "failed: %s\n",
                         platform_status_to_string(rc));
      platform_free(hid, itor);
      return NULL;
   }
   return &itor->super;
}
