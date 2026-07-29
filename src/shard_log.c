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

#define SHARD_WAIT     1
#define SHARD_UNMAPPED UINT64_MAX

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

static int
get_new_page_for_thread(shard_log             *log,
                        shard_log_thread_data *thread_data,
                        page_handle          **page)
{
   uint64 next_extent;

   *page = shard_log_alloc(log, &next_extent);
   if (*page == NULL) {
      return -1;
   }
   thread_data->addr     = (*page)->disk_addr;
   shard_log_hdr *hdr    = (shard_log_hdr *)(*page)->data;
   hdr->magic            = log->magic;
   hdr->next_extent_addr = next_extent;
   hdr->num_entries      = 0;
   thread_data->offset   = sizeof(shard_log_hdr);
   return 0;
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

   page_handle *page;
   if (thread_data->addr == SHARD_UNMAPPED) {
      if (get_new_page_for_thread(log, thread_data, &page)) {
         if (log_blob_inited) {
            merge_accumulator_deinit(&log_blob);
         }
         return -1;
      }
   } else {
      page        = cache_get(cc, thread_data->addr, TRUE, PAGE_TYPE_LOG);
      uint64 wait = 1;
      while (!cache_try_claim(cc, page)) {
         cache_unget(cc, page);
         platform_sleep_ns(wait);
         wait = wait > 1024 ? wait : 2 * wait;
         page = cache_get(cc, thread_data->addr, TRUE, PAGE_TYPE_LOG);
      }
      cache_lock(cc, page);
   }

   shard_log_hdr *hdr    = (shard_log_hdr *)page->data;
   log_entry     *cursor = (log_entry *)(page->data + thread_data->offset);
   uint64         new_entry_size = log_entry_required_capacity(tuple_key, msg);
   uint64 free_space = shard_log_page_size(log->cfg) - thread_data->offset;
   debug_assert(new_entry_size
                <= shard_log_page_size(log->cfg) - sizeof(shard_log_hdr));

   if (free_space < new_entry_size) {
      if (sizeof(log_entry) <= free_space) {
         log_entry_set_terminal(cursor);
      }
      hdr->checksum = shard_log_checksum(log->cfg, page);

      cache_unlock(cc, page);
      cache_unclaim(cc, page);
      cache_page_writeback(cc, page, FALSE, PAGE_TYPE_LOG);
      cache_unget(cc, page);

      if (get_new_page_for_thread(log, thread_data, &page)) {
         if (log_blob_inited) {
            merge_accumulator_deinit(&log_blob);
         }
         return -1;
      }
      cursor = (log_entry *)(page->data + thread_data->offset);
      hdr    = (shard_log_hdr *)page->data;
   }

   cursor->memtable_generation = memtable_generation;
   cursor->leaf_generation     = leaf_generation;
   copy_tuple_to_ondisk_tuple(&cursor->tuple, tuple_key, msg);

   hdr->num_entries++;

   thread_data->offset += new_entry_size;
   debug_assert(thread_data->offset <= shard_log_page_size(log->cfg));

   cache_unlock(cc, page);
   cache_unclaim(cc, page);
   cache_unget(cc, page);

   if (log_blob_inited) {
      platform_status rc = blob_sync(cc, message_slice(msg));
      merge_accumulator_deinit(&log_blob);
      if (!SUCCESS(rc)) {
         return rc.r;
      }
   }

   return 0;
}

/*
 * shard_log_seal --
 *
 *     Finalize and retire a log stream, terminally.  Finalizes every currently
 *     active per-thread append page (bounded by MAX_THREADS; it does not walk
 *     the historical log): a terminal record (where there is room) and checksum
 *     make each page readable by shard_log_iterator_init().  Then it releases
 *     the mini-allocator's unused reserve and frees the handle.  After seal the
 *     handle is invalid; the caller retains the identity it captured earlier
 *     (shard_log_get_head(), fixed at creation) to reopen the stream
 *     for replay and, eventually, to free its extents via log_dec_ref().
 *
 *     The caller must prevent concurrent shard_log_write() and seal calls.
 *     seal itself issues no writeback or durable barrier: to make the sealed
 *     pages durable, the caller takes cache_writeback_dirty() followed by a
 *     durable barrier.
 */
platform_status
shard_log_seal(log_handle *logh)
{
   shard_log *log = (shard_log *)logh;
   cache     *cc  = log->cc;

   for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      shard_log_thread_data *thread_data =
         shard_log_get_thread_data(log, thr_i);
      uint64 addr = thread_data->addr;
      if (addr == SHARD_UNMAPPED) {
         continue;
      }

      page_handle *page = cache_get(cc, addr, TRUE, PAGE_TYPE_LOG);
      uint64       wait = 1;
      while (!cache_try_claim(cc, page)) {
         /*
          * Even though the stream is quiescent (no concurrent writers/seals),
          * the background cache evictor can transiently hold the claim on a
          * cleaned log page before it drains our read-ref, so we still retry
          * rather than assert.
          */
         cache_unget(cc, page);
         platform_sleep_ns(wait);
         wait = wait > 1024 ? wait : 2 * wait;
         page = cache_get(cc, addr, TRUE, PAGE_TYPE_LOG);
      }
      cache_lock(cc, page);

      debug_assert(thread_data->addr == addr);
      debug_assert(thread_data->offset >= sizeof(shard_log_hdr));
      debug_assert(thread_data->offset <= shard_log_page_size(log->cfg));

      shard_log_hdr *hdr    = (shard_log_hdr *)page->data;
      log_entry     *cursor = (log_entry *)(page->data + thread_data->offset);
      uint64 free_space = shard_log_page_size(log->cfg) - thread_data->offset;
      if (sizeof(log_entry) <= free_space) {
         log_entry_set_terminal(cursor);
      }
      hdr->checksum = shard_log_checksum(log->cfg, page);

      cache_unlock(cc, page);
      cache_unclaim(cc, page);
      cache_unget(cc, page);

      /* Subsequent writes must allocate a new append page. */
      thread_data->addr   = SHARD_UNMAPPED;
      thread_data->offset = 0;
   }

   /*
    * The stream is now immutable.  Release the mini-allocator's unused
    * per-batch reserve so no future allocation touches this stream, and free
    * the handle.  The caller already holds the stream's identity (captured at
    * creation) and later frees the on-disk extents via log_dec_ref().
    */
   mini_release(&log->mini);
   platform_free(log->heap_id, log);
   return STATUS_OK;
}

void
log_dec_ref(cache *cc, const log_head *segment)
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

/*
 * Bytes appended to the stream so far.  The mini-allocator already tracks the
 * extents it has handed out across all of the stream's batches (data and blob),
 * counting each as it is reserved -- the same measure memtable_is_full() uses for
 * a memtable.  Subtracting the fixed overhead recorded at init means a fresh
 * stream reports 0, so a caller comparing against a threshold cannot be tricked
 * into rotating a stream that has had nothing written to it.
 */
uint64
shard_log_get_size(log_handle *logh)
{
   shard_log *log = (shard_log *)logh;
   return (mini_num_extents(&log->mini) - log->initial_extents)
          * shard_log_extent_size(log->cfg);
}

static log_ops shard_log_ops = {
   .write = shard_log_write,
   .seal  = shard_log_seal,
   .head  = shard_log_get_head,
   .size  = shard_log_get_size,
};

static platform_status
shard_log_init(shard_log *log, cache *cc, shard_log_config *cfg)
{
   memset(log, 0, sizeof(shard_log));
   log->cc        = cc;
   log->cfg       = cfg;
   log->super.ops = &shard_log_ops;

   uint64 magic_idx = __sync_fetch_and_add(&shard_log_magic_idx, 1);
   log->magic = platform_checksum64(&magic_idx, sizeof(uint64), cfg->seed);

   allocator      *al = cache_get_allocator(cc);
   platform_status rc = allocator_alloc(al, &log->meta_head, PAGE_TYPE_LOG);
   platform_assert_status_ok(rc);

   for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      shard_log_thread_data *thread_data =
         shard_log_get_thread_data(log, thr_i);
      thread_data->addr   = SHARD_UNMAPPED;
      thread_data->offset = 0;
   }

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
   platform_status rc = shard_log_init(slog, cc, cfg);
   if (!SUCCESS(rc)) {
      platform_error_log("shard_log_create: shard_log_init failed: %s\n",
                         platform_status_to_string(rc));
      platform_free(hid, slog);
      return NULL;
   }
   // Remember the heap so log_seal() can free the handle without the caller
   // touching platform_free() directly.
   slog->heap_id = hid;
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

const static log_iterator_ops shard_log_log_iterator_ops = {
   .curr_generations = shard_log_iterator_curr_generations,
   .deinit           = shard_log_iterator_deinit,
};

static platform_status
shard_log_iterator_init(cache              *cc,
                        shard_log_config   *cfg,
                        platform_heap_id    hid,
                        uint64              addr,
                        uint64              magic,
                        shard_log_iterator *itor)
{
   page_handle *page;
   uint64       i;
   uint64       pages_per_extent = shard_log_pages_per_extent(cfg);
   uint64       page_addr;
   uint64       num_valid_pages = 0;
   uint64       extent_addr;
   uint64       next_extent_addr;
   uint64       contents_size;

   memset(itor, 0, sizeof(shard_log_iterator));
   itor->super.super.ops = &shard_log_iterator_ops;     // generic iterator
   itor->super.ops       = &shard_log_log_iterator_ops; // log_iterator
   itor->heap_id         = hid;
   itor->cc              = cc;
   itor->cfg             = cfg;
   allocator *al         = cache_get_allocator(cc);

   // traverse the log extents and calculate the required space
   extent_addr = addr;
   while (extent_addr != 0 && allocator_get_refcount(al, extent_addr) > 0) {
      cache_prefetch(cc, extent_addr, PAGE_TYPE_LOG);
      next_extent_addr = 0;
      for (i = 0; i < pages_per_extent; i++) {
         page_addr = extent_addr + i * shard_log_page_size(cfg);
         page      = cache_get(cc, page_addr, TRUE, PAGE_TYPE_LOG);
         if (!shard_log_valid(cfg, page, magic)) {
            cache_unget(cc, page);
            goto finished_first_pass;
         }
         num_valid_pages++;
         itor->num_entries += ((shard_log_hdr *)page->data)->num_entries;
         next_extent_addr = shard_log_next_extent_addr(cfg, page);
         cache_unget(cc, page);
      }
      extent_addr = next_extent_addr;
   }

finished_first_pass:

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

   // traverse the log extents again and copy the kv pairs
   log_entry *cursor    = (log_entry *)itor->contents;
   uint64     entry_idx = 0;
   extent_addr          = addr;
   while (extent_addr != 0 && allocator_get_refcount(al, extent_addr) > 0) {
      cache_prefetch(cc, extent_addr, PAGE_TYPE_LOG);
      next_extent_addr = 0;
      for (i = 0; i < pages_per_extent; i++) {
         page_addr = extent_addr + i * shard_log_page_size(cfg);
         page      = cache_get(cc, page_addr, TRUE, PAGE_TYPE_LOG);
         if (!shard_log_valid(cfg, page, magic)) {
            cache_unget(cc, page);
            goto finished_second_pass;
         }
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
finished_second_pass:
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
