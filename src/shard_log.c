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
#include "platform_random.h"
#include "platform_typed_alloc.h"
#include "platform_assert.h"
#include "platform_threads.h"
#include "platform_sort.h"
#include "poison.h"

static const page_type shard_log_page_type_table[NUM_BLOB_BATCHES + 1] = {
   PAGE_TYPE_LOG,
   [1 ... NUM_BLOB_BATCHES] = PAGE_TYPE_BLOB,
};

static platform_status
shard_log_iterator_create_internal(cache            *cc,
                                   shard_log_config *cfg,
                                   platform_heap_id  hid,
                                   log_head          head,
                                   uint64            first_needed_generation,
                                   log_iterator    **itor_out);

static platform_status
shard_log_graduate_through(shard_log *log, log_durable_ticket target);

static platform_status
shard_log_wait_for_ticket(shard_log *log, log_durable_ticket target);

static void
shard_log_destroy(shard_log *log);

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
shard_log_get_thread_data(shard_log_group *group, threadid thr_id)
{
   return &group->thread_data[thr_id];
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

static platform_status
shard_log_validate_page_blobs(cache            *cc,
                              shard_log_config *cfg,
                              page_handle      *page,
                              uint64            first_needed_generation)
{
   for (log_entry *le = first_log_entry(page->data);
        !terminal_log_entry(cfg, page->data, le);
        le = log_entry_next(le))
   {
      if (le->memtable_generation < first_needed_generation
          || !log_entry_message_is_blob(le))
      {
         continue;
      }
      message         msg = log_entry_message(cc, le);
      platform_status rc  = message_validate(msg);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   return STATUS_OK;
}

/* Reset a staging buffer to an empty page image. */
static void
shard_log_reset_buffer(shard_log *log, shard_log_thread_data *thread_data)
{
   platform_assert(thread_data->incache_page == NULL);
   shard_log_hdr *hdr = (shard_log_hdr *)thread_data->buf;
   hdr->nonce         = log->nonce;
   hdr->num_entries   = 0;
   // next_extent_addr and checksum are only knowable once a page has been
   // allocated for this image; see shard_log_graduate_buffer().
   thread_data->offset = sizeof(shard_log_hdr);
   thread_data->state  = SHARD_LOG_BUFFER_OPEN;
   thread_data->close  = SHARD_LOG_CLOSE_NONE;
}

static void
shard_log_group_reset(shard_log *log, shard_log_group *group, uint64 id)
{
   group->id                  = id;
   group->state               = SHARD_LOG_GROUP_OPEN;
   group->close               = SHARD_LOG_CLOSE_NONE;
   group->active_reservations = 0;
   group->page_count          = 0;
   group->ever_used           = FALSE;
   group->append_error        = STATUS_OK;
   group->next                = NULL;
   group->pool_next           = NULL;
   writeback_set_reset(&group->wbset);

   uint64 page_size = shard_log_page_size(log->cfg);
   memset(group->thread_buffers, 0, MAX_THREADS * page_size);
   for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      shard_log_thread_data *thread_data =
         shard_log_get_thread_data(group, thr_i);
      thread_data->buf          = group->thread_buffers + thr_i * page_size;
      thread_data->incache_page = NULL;
      shard_log_reset_buffer(log, thread_data);
   }
}

static void
shard_log_group_free(shard_log *log, shard_log_group *group)
{
   for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      shard_log_thread_data *thread_data =
         shard_log_get_thread_data(group, thr_i);
      if (thread_data->incache_page != NULL) {
         cache_unget(log->cc, thread_data->incache_page);
         thread_data->incache_page = NULL;
      }
   }
   writeback_set_deinit(&group->wbset);
   platform_status rc = platform_mutex_destroy(&group->wbset_lock);
   platform_assert_status_ok(rc);
   platform_free(log->heap_id, group->thread_buffers);
   platform_free(log->heap_id, group->thread_data);
   platform_free(log->heap_id, group);
}

static platform_status
shard_log_group_alloc(shard_log        *log,
                      bool32            emergency,
                      shard_log_group **group_out)
{
   *group_out = NULL;

   shard_log_group *group = TYPED_MALLOC(log->heap_id, group);
   if (group == NULL) {
      return STATUS_NO_MEMORY;
   }
   ZERO_CONTENTS(group);

   group->thread_data =
      TYPED_ARRAY_MALLOC(log->heap_id, group->thread_data, MAX_THREADS);
   if (group->thread_data == NULL) {
      platform_free(log->heap_id, group);
      return STATUS_NO_MEMORY;
   }

   uint64 page_size      = shard_log_page_size(log->cfg);
   group->thread_buffers = TYPED_ARRAY_MALLOC(
      log->heap_id, group->thread_buffers, MAX_THREADS * page_size);
   if (group->thread_buffers == NULL) {
      platform_free(log->heap_id, group->thread_data);
      platform_free(log->heap_id, group);
      return STATUS_NO_MEMORY;
   }

   platform_status rc =
      platform_mutex_init(&group->wbset_lock, 0, log->heap_id);
   if (!SUCCESS(rc)) {
      platform_free(log->heap_id, group->thread_buffers);
      platform_free(log->heap_id, group->thread_data);
      platform_free(log->heap_id, group);
      return rc;
   }
   writeback_set_init(&group->wbset, log->cc, log->heap_id);
   group->emergency = emergency;
   shard_log_group_reset(log, group, 0);
   group->emergency = emergency;
   *group_out       = group;
   return STATUS_OK;
}

/* Return an unused candidate group to its allocation source. */
static void
shard_log_put_unused_group(shard_log *log, shard_log_group *group)
{
   if (group == NULL) {
      return;
   }
   if (!group->emergency) {
      shard_log_group_free(log, group);
      return;
   }

   platform_mutex_lock(&log->group_lock);
   group->pool_next    = log->emergency_pool;
   log->emergency_pool = group;
   platform_mutex_unlock(&log->group_lock);
}

/*
 * Allocate normally first.  The two preallocated groups are only consumed
 * when the heap cannot provide a group, so normal bursts scale with device
 * latency while low-memory progress retains a bounded reserve.
 */
static platform_status
shard_log_get_candidate_group(shard_log *log, shard_log_group **group_out)
{
   platform_status rc = shard_log_group_alloc(log, FALSE, group_out);
   if (SUCCESS(rc)) {
      return STATUS_OK;
   }

   platform_mutex_lock(&log->group_lock);
   shard_log_group *group = log->emergency_pool;
   if (group != NULL) {
      log->emergency_pool = group->pool_next;
      group->pool_next    = NULL;
   }
   platform_mutex_unlock(&log->group_lock);
   if (group == NULL) {
      return rc;
   }
   shard_log_group_reset(log, group, 0);
   group->emergency = TRUE;
   *group_out       = group;
   return STATUS_OK;
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
static platform_status
shard_log_graduate_buffer(shard_log             *log,
                          shard_log_group       *group,
                          shard_log_thread_data *thread_data,
                          shard_log_close        close)
{
   bool32 close_group = (close != SHARD_LOG_CLOSE_NONE);
   uint64 page_size   = shard_log_page_size(log->cfg);

   debug_assert(thread_data->offset >= sizeof(shard_log_hdr));
   debug_assert(thread_data->offset <= page_size);

   if (thread_data->state == SHARD_LOG_BUFFER_OPEN) {
      /*
       * An empty buffer normally has nothing to contribute. Group termination
       * deliberately uses an otherwise empty page as an explicit commit marker.
       */
      if (thread_data->offset == sizeof(shard_log_hdr) && !close_group) {
         return STATUS_OK;
      }

      uint64 free_space = page_size - thread_data->offset;
      if (sizeof(log_entry) <= free_space) {
         log_entry_set_terminal(
            (log_entry *)(thread_data->buf + thread_data->offset));
      }
      thread_data->close = close;
      thread_data->state = SHARD_LOG_BUFFER_TERMINATED;
   } else {
      /* A retry must finish exactly the image the first attempt froze. */
      platform_assert(thread_data->close == close);
   }

   if (thread_data->state == SHARD_LOG_BUFFER_TERMINATED) {
      uint64       next_extent;
      page_handle *page = shard_log_alloc(log, &next_extent);
      if (page == NULL) {
         platform_error_log("shard_log_graduate_buffer: out of log space\n");
         return STATUS_NO_SPACE;
      }

      shard_log_hdr *staged    = (shard_log_hdr *)thread_data->buf;
      staged->next_extent_addr = next_extent;
      staged->group_id         = group->id;
      /*
       * Count exactly once, when the frozen image obtains its permanent cache
       * page. If writeback enrollment fails, INCACHE retains both the page and
       * this ordinal for a retry.
       */
      uint64 pages = __sync_add_and_fetch(&group->page_count, 1);
      if (close_group) {
         platform_assert(pages <= SHARD_LOG_PAGES_IN_GROUP_MASK,
                         "group %lu is too large to terminate: %lu pages",
                         group->id,
                         pages);
         staged->pages_in_group =
            (uint32)pages
            | (close == SHARD_LOG_CLOSE_STREAM ? SHARD_LOG_END_OF_STREAM : 0);
      } else {
         staged->pages_in_group = 0;
      }

      memcpy(page->data, thread_data->buf, page_size);
      ((shard_log_hdr *)page->data)->checksum =
         shard_log_checksum(log->cfg, page);

      cache_unlock(log->cc, page);
      cache_unclaim(log->cc, page);
      thread_data->incache_page = page; // retain our cache reference for retry
      thread_data->state        = SHARD_LOG_BUFFER_INCACHE;
   }

   platform_assert(thread_data->state == SHARD_LOG_BUFFER_INCACHE);
   platform_mutex_lock(&group->wbset_lock);
   platform_status rc = writeback_set_add_page(
      &group->wbset, thread_data->incache_page, PAGE_TYPE_LOG);
   platform_mutex_unlock(&group->wbset_lock);
   if (!SUCCESS(rc)) {
      return rc;
   }

   cache_unget(log->cc, thread_data->incache_page);
   thread_data->incache_page = NULL;
   shard_log_reset_buffer(log, thread_data);
   return STATUS_OK;
}

static void
shard_log_write_reserve(log_handle *logh, log_write_token *token)
{
   shard_log *log = (shard_log *)logh;

   platform_status rc = platform_mutex_lock(&log->group_lock);
   platform_assert_status_ok(rc);

   shard_log_group *group = log->accepting;
   platform_assert(group != NULL);
   platform_assert(!log->deinit_requested);
   platform_assert(group->state == SHARD_LOG_GROUP_OPEN);
   platform_assert(group->active_reservations != UINT64_MAX);
   group->active_reservations++;
   group->ever_used = TRUE;
   token->log       = logh;
   token->internal  = group;

   rc = platform_mutex_unlock(&log->group_lock);
   platform_assert_status_ok(rc);
}

static platform_status
shard_log_end_append(shard_log       *log,
                     shard_log_group *group,
                     bool32           accepted_record,
                     platform_status  append_rc)
{
   platform_mutex_lock(&log->group_lock);
   platform_assert(group->active_reservations != 0);
   if (!SUCCESS(append_rc) && SUCCESS(group->append_error)) {
      /*
       * The caller's update was already visible before it entered the log.
       * Remember the first missing append permanently; a later retry or error
       * must not obscure the reason this recovery suffix is unusable.
       */
      group->append_error = append_rc;
   }
   group->active_reservations--;
   if (accepted_record) {
      log->has_records = TRUE;
   }
   platform_status result = group->append_error;
   platform_mutex_unlock(&log->group_lock);
   return result;
}

static int
shard_log_write_reserved(log_write_token *token,
                         key              tuple_key,
                         message          msg,
                         uint64           memtable_generation,
                         uint64           leaf_generation)
{
   debug_assert(key_is_user_key(tuple_key));
   debug_assert(memtable_generation != INVALID_LOG_GENERATION);
   debug_assert(leaf_generation != INVALID_LOG_GENERATION);

   platform_assert(token != NULL);
   platform_assert(token->log != NULL);
   platform_assert(token->internal != NULL);

   shard_log       *log   = (shard_log *)token->log;
   shard_log_group *group = token->internal;

   /* Ownership is consumed on every return path from this point onward. */
   token->log      = NULL;
   token->internal = NULL;

   cache            *cc = log->cc;
   platform_status   rc = STATUS_OK;
   merge_accumulator log_blob;
   bool32            log_blob_inited = FALSE;
   bool32            accepted_record = FALSE;

   uint64 max_entry_size =
      shard_log_page_size(log->cfg) - sizeof(shard_log_hdr);
   if (message_is_blob(msg)
       || max_entry_size < log_entry_required_capacity(tuple_key, msg))
   {
      merge_accumulator_init(&log_blob, platform_get_heap_id());
      if (message_is_blob(msg)) {
         rc =
            message_clone(&log->cfg->blob_cfg, cc, &log->mini, msg, &log_blob);
      } else {
         rc = message_to_blob(
            &log->cfg->blob_cfg, cc, &log->mini, msg, &log_blob);
      }
      if (!SUCCESS(rc)) {
         merge_accumulator_deinit(&log_blob);
         goto out;
      }
      msg             = merge_accumulator_to_message(&log_blob);
      log_blob_inited = TRUE;
   }

   if (log_blob_inited) {
      /*
       * Enroll the value before publishing the record into a staging buffer.
       * A partial blob-writeback enrollment may leave harmless extra receipts,
       * but it can no longer leave behind a record whose value was not covered.
       */
      platform_mutex_lock(&group->wbset_lock);
      rc = blob_writeback(cc, message_slice(msg), &group->wbset);
      platform_mutex_unlock(&group->wbset_lock);
      if (!SUCCESS(rc)) {
         goto out;
      }
   }

   shard_log_thread_data *thread_data =
      shard_log_get_thread_data(group, platform_get_tid());

   uint64 page_size      = shard_log_page_size(log->cfg);
   uint64 new_entry_size = log_entry_required_capacity(tuple_key, msg);
   debug_assert(new_entry_size <= page_size - sizeof(shard_log_hdr));

   // Full, or retrying an earlier hand-over: finish that frozen image first.
   if (thread_data->state != SHARD_LOG_BUFFER_OPEN
       || page_size - thread_data->offset < new_entry_size)
   {
      /*
       * Log pages must remain physically grouped.  A later group may stage
       * records immediately, but before it allocates its first page it helps
       * graduate every predecessor (including each predecessor terminator).
       */
      rc = shard_log_graduate_through(log, group->id);
      if (SUCCESS(rc)) {
         rc = shard_log_graduate_buffer(
            log, group, thread_data, SHARD_LOG_CLOSE_NONE);
      }
      if (!SUCCESS(rc)) {
         goto out;
      }
   }

   log_entry *cursor = (log_entry *)(thread_data->buf + thread_data->offset);
   cursor->memtable_generation = memtable_generation;
   cursor->leaf_generation     = leaf_generation;
   copy_tuple_to_ondisk_tuple(&cursor->tuple, tuple_key, msg);

   ((shard_log_hdr *)thread_data->buf)->num_entries++;

   thread_data->offset += new_entry_size;
   accepted_record = TRUE;
   debug_assert(thread_data->offset <= page_size);

out:
   if (log_blob_inited) {
      merge_accumulator_deinit(&log_blob);
   }
   rc = shard_log_end_append(log, group, accepted_record, rc);
   return rc.r;
}

static shard_log_group *
shard_log_find_group_locked(shard_log *log, log_durable_ticket ticket)
{
   platform_assert(ticket != 0);
   for (shard_log_group *group = log->groups_head; group != NULL;
        group                  = group->next)
   {
      if (group->id + 1 == ticket) {
         return group;
      }
   }
   return NULL;
}

/* Wait for every reservation which selected this group before its cut. */
static void
shard_log_wait_for_reservations(shard_log *log, log_durable_ticket ticket)
{
   uint64 wait = 100;
   while (TRUE) {
      platform_mutex_lock(&log->group_lock);
      if (log->graduated_ticket >= ticket) {
         platform_mutex_unlock(&log->group_lock);
         return;
      }
      shard_log_group *group = shard_log_find_group_locked(log, ticket);
      platform_assert(group != NULL);
      uint64 active = group->active_reservations;
      platform_mutex_unlock(&log->group_lock);
      if (active == 0) {
         return;
      }
      platform_sleep_ns(wait);
      wait = wait > 2048 ? wait : 2 * wait;
   }
}

/* graduate_lock must be held. */
static platform_status
shard_log_graduate_group(shard_log *log, shard_log_group *group)
{
   platform_assert(group->close != SHARD_LOG_CLOSE_NONE);
   platform_mutex_lock(&log->group_lock);
   platform_assert(group->active_reservations == 0);
   platform_status append_error = group->append_error;
   platform_mutex_unlock(&log->group_lock);

   /*
    * A terminator is the on-disk commit record for the whole group. Emitting
    * one after any append failed would make recovery skip a visible update and
    * then accept the suffix as complete. Data pages already graduated before
    * the failure remain harmless because recovery discards an unterminated
    * group and everything after it.
    */
   if (!SUCCESS(append_error)) {
      return append_error;
   }

   if (group->state == SHARD_LOG_GROUP_CLOSING) {
      /*
       * Freeze every writer's partial page as ordinary data first.  The
       * dedicated empty terminator is emitted only after all of them succeed,
       * so a retry never has to alter a previously frozen page.
       */
      for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
         platform_status rc =
            shard_log_graduate_buffer(log,
                                      group,
                                      shard_log_get_thread_data(group, thr_i),
                                      SHARD_LOG_CLOSE_NONE);
         if (!SUCCESS(rc)) {
            platform_error_log("shard_log_graduate_group: failed to flush "
                               "thread %lu in group %lu: %s\n",
                               thr_i,
                               group->id,
                               platform_status_to_string(rc));
            return rc;
         }
      }
      group->state = SHARD_LOG_GROUP_TERMINATING;
   }

   if (group->state == SHARD_LOG_GROUP_TERMINATING) {
      platform_status rc = shard_log_graduate_buffer(
         log, group, shard_log_get_thread_data(group, 0), group->close);
      if (!SUCCESS(rc)) {
         platform_error_log("shard_log_graduate_group: failed to terminate "
                            "group %lu: %s\n",
                            group->id,
                            platform_status_to_string(rc));
         return rc;
      }
      group->state = SHARD_LOG_GROUP_DURABILITY_PENDING;
   }

   platform_assert(group->state == SHARD_LOG_GROUP_DURABILITY_PENDING
                   || group->state == SHARD_LOG_GROUP_DURABLE);
   return STATUS_OK;
}

/*
 * Allocate log pages in group-id order.  Later groups may already contain
 * private staged records, but no page of N+1 is allocated until N's terminator
 * has obtained its permanent address and writeback receipt.
 */
static platform_status
shard_log_graduate_through(shard_log *log, log_durable_ticket target)
{
   if (target == 0) {
      return STATUS_OK;
   }

   platform_status rc = platform_mutex_lock(&log->graduate_lock);
   if (!SUCCESS(rc)) {
      return rc;
   }

   while (log->graduated_ticket < target) {
      platform_mutex_lock(&log->group_lock);
      log_durable_ticket next_ticket = log->graduated_ticket + 1;
      shard_log_group   *group = shard_log_find_group_locked(log, next_ticket);
      platform_assert(group != NULL);
      platform_assert(group->state != SHARD_LOG_GROUP_OPEN);
      uint64 active = group->active_reservations;
      platform_mutex_unlock(&log->group_lock);

      if (active != 0) {
         /*
          * Do not hold graduate_lock while draining the closed group. One of
          * its reserved writers may be blocked in this function helping finish
          * its predecessor before it can graduate a full buffer and consume
          * its reservation. Closed groups cannot acquire new reservations, so
          * it is safe to drop the driver lock, wait, and re-evaluate the
          * frontier.
          */
         platform_mutex_unlock(&log->graduate_lock);
         shard_log_wait_for_reservations(log, next_ticket);
         rc = platform_mutex_lock(&log->graduate_lock);
         if (!SUCCESS(rc)) {
            return rc;
         }
         continue;
      }

      rc = shard_log_graduate_group(log, group);
      if (!SUCCESS(rc)) {
         break;
      }

      platform_mutex_lock(&log->group_lock);
      platform_assert(log->graduated_ticket + 1 == next_ticket);
      log->graduated_ticket = next_ticket;
      platform_mutex_unlock(&log->group_lock);
   }

   platform_status unlock_rc = platform_mutex_unlock(&log->graduate_lock);
   if (SUCCESS(rc) && !SUCCESS(unlock_rc)) {
      rc = unlock_rc;
   }
   return rc;
}

/* Remove and release the durable prefix. durability_lock is held. */
static void
shard_log_reclaim_durable_groups(shard_log *log)
{
   platform_mutex_lock(&log->graduate_lock);
   platform_mutex_lock(&log->group_lock);

   shard_log_group *reclaim_head = NULL;
   shard_log_group *reclaim_tail = NULL;
   while (log->groups_head != NULL
          && log->groups_head->state == SHARD_LOG_GROUP_DURABLE)
   {
      shard_log_group *group = log->groups_head;
      log->groups_head       = group->next;
      group->next            = NULL;
      if (reclaim_tail == NULL) {
         reclaim_head = group;
      } else {
         reclaim_tail->next = group;
      }
      reclaim_tail = group;
   }
   if (log->groups_head == NULL) {
      log->groups_tail = NULL;
   }

   platform_mutex_unlock(&log->group_lock);
   platform_mutex_unlock(&log->graduate_lock);

   while (reclaim_head != NULL) {
      shard_log_group *group = reclaim_head;
      reclaim_head           = group->next;
      group->next            = NULL;
      shard_log_put_unused_group(log, group);
   }
}

/* Wait/writeback/barrier half shared by ticket wait and seal. */
static platform_status
shard_log_wait_for_ticket(shard_log *log, log_durable_ticket target)
{
   platform_status rc = shard_log_graduate_through(log, target);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = platform_mutex_lock(&log->durability_lock);
   if (!SUCCESS(rc)) {
      return rc;
   }

   platform_status result = STATUS_OK;
   if (log->durable_ticket < target) {
      /*
       * Issue every needed retry before waiting for any group, preserving the
       * same all-I/O-first pipelining as writeback_set itself.
       */
      for (log_durable_ticket ticket = log->durable_ticket + 1;
           ticket <= target;
           ticket++)
      {
         platform_mutex_lock(&log->group_lock);
         shard_log_group *group = shard_log_find_group_locked(log, ticket);
         platform_assert(group != NULL);
         platform_assert(group->state == SHARD_LOG_GROUP_DURABILITY_PENDING);
         platform_mutex_unlock(&log->group_lock);

         platform_status retry_rc =
            writeback_set_retry_incomplete(&group->wbset);
         if (SUCCESS(result) && !SUCCESS(retry_rc)) {
            result = retry_rc;
         }
      }

      for (log_durable_ticket ticket = log->durable_ticket + 1;
           ticket <= target;
           ticket++)
      {
         platform_mutex_lock(&log->group_lock);
         shard_log_group *group = shard_log_find_group_locked(log, ticket);
         platform_assert(group != NULL);
         platform_mutex_unlock(&log->group_lock);

         platform_status wait_rc = writeback_set_wait(&group->wbset);
         if (SUCCESS(result) && !SUCCESS(wait_rc)) {
            result = wait_rc;
         }
      }

      if (SUCCESS(result)) {
         result = cache_durable_barrier(log->cc);
      }

      if (SUCCESS(result)) {
         platform_mutex_lock(&log->group_lock);
         for (log_durable_ticket ticket = log->durable_ticket + 1;
              ticket <= target;
              ticket++)
         {
            shard_log_group *group = shard_log_find_group_locked(log, ticket);
            platform_assert(group != NULL);
            writeback_set_reset(&group->wbset);
            group->state = SHARD_LOG_GROUP_DURABLE;
         }
         log->durable_ticket = target;
         platform_mutex_unlock(&log->group_lock);
         shard_log_reclaim_durable_groups(log);
      }
   }

   platform_status unlock_rc = platform_mutex_unlock(&log->durability_lock);
   if (SUCCESS(result) && !SUCCESS(unlock_rc)) {
      result = unlock_rc;
   }
   return result;
}

/* Install a fresh accepting group and return the closed group's ticket. */
static platform_status
shard_log_make_durable_begin(log_handle *logh, log_durable_ticket *ticket_out)
{
   if (ticket_out == NULL) {
      return STATUS_BAD_PARAM;
   }
   *ticket_out    = 0;
   shard_log *log = (shard_log *)logh;

   shard_log_group *candidate = NULL;
   while (TRUE) {
      platform_mutex_lock(&log->group_lock);
      if (log->deinit_requested) {
         platform_mutex_unlock(&log->group_lock);
         shard_log_put_unused_group(log, candidate);
         return STATUS_INVALID_STATE;
      }

      shard_log_group *current = log->accepting;
      if (current == NULL || !current->ever_used) {
         log_durable_ticket target = log->last_cut_ticket;
         log->ticket_refs++;
         (void)mini_inc_ref(log->cc, log->meta_head);
         platform_mutex_unlock(&log->group_lock);
         shard_log_put_unused_group(log, candidate);
         *ticket_out = target;
         return STATUS_OK;
      }

      if (candidate != NULL) {
         platform_assert(current->state == SHARD_LOG_GROUP_OPEN);
         platform_assert(current->id != UINT64_MAX);
         current->state = SHARD_LOG_GROUP_CLOSING;
         current->close = SHARD_LOG_CLOSE_GROUP;

         candidate->id    = current->id + 1;
         current->next    = candidate;
         log->groups_tail = candidate;
         log->accepting   = candidate;
         candidate        = NULL;

         log->last_cut_ticket = current->id + 1;
         log->ticket_refs++;
         log_durable_ticket target = log->last_cut_ticket;
         (void)mini_inc_ref(log->cc, log->meta_head);
         platform_mutex_unlock(&log->group_lock);

         *ticket_out = target;
         return STATUS_OK;
      }
      platform_mutex_unlock(&log->group_lock);

      platform_status rc = shard_log_get_candidate_group(log, &candidate);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
}

static platform_status
shard_log_make_durable_wait(log_handle *logh, log_durable_ticket ticket)
{
   shard_log      *log = (shard_log *)logh;
   platform_status rc  = shard_log_wait_for_ticket(log, ticket);

   /*
    * Capture these before dropping the in-memory pin.  A concurrent deinit may
    * free the handle as soon as ticket_refs reaches zero, but it runs
    * mini_release() before this ticket drops its independent on-disk ref.
    */
   cache *cc        = log->cc;
   uint64 meta_head = log->meta_head;
   bool32 destroy   = FALSE;
   platform_mutex_lock(&log->group_lock);
   platform_assert(log->ticket_refs != 0,
                   "log durability ticket consumed more than once");
   log->ticket_refs--;
   if (log->deinit_requested && log->ticket_refs == 0 && !log->destroying) {
      log->destroying = TRUE;
      destroy         = TRUE;
   }
   platform_mutex_unlock(&log->group_lock);

   if (destroy) {
      shard_log_destroy(log);
   }
   (void)mini_dec_ref(cc, meta_head, PAGE_TYPE_LOG);
   return rc;
}

platform_status
shard_log_seal(log_handle *logh)
{
   shard_log *log = (shard_log *)logh;

   platform_mutex_lock(&log->group_lock);
   if (log->deinit_requested) {
      platform_mutex_unlock(&log->group_lock);
      return STATUS_INVALID_STATE;
   }
   if (!log->sealing) {
      shard_log_group *current = log->accepting;
      platform_assert(current != NULL);
      platform_assert(current->state == SHARD_LOG_GROUP_OPEN);
      current->state       = SHARD_LOG_GROUP_CLOSING;
      current->close       = SHARD_LOG_CLOSE_STREAM;
      log->accepting       = NULL;
      log->last_cut_ticket = current->id + 1;
      log->seal_ticket     = log->last_cut_ticket;
      log->sealing         = TRUE;
   }
   log_durable_ticket target = log->seal_ticket;
   platform_mutex_unlock(&log->group_lock);

   platform_status rc = shard_log_wait_for_ticket(log, target);
   if (SUCCESS(rc)) {
      platform_mutex_lock(&log->group_lock);
      log->sealed = TRUE;
      platform_mutex_unlock(&log->group_lock);
   }
   return rc;
}

/* Final destruction, called once ticket_refs is zero. */
static void
shard_log_destroy(shard_log *log)
{
   mini_release(&log->mini);

   shard_log_group *group = log->groups_head;
   while (group != NULL) {
      shard_log_group *next = group->next;
      shard_log_group_free(log, group);
      group = next;
   }
   group = log->emergency_pool;
   while (group != NULL) {
      shard_log_group *next = group->pool_next;
      shard_log_group_free(log, group);
      group = next;
   }

   platform_status rc = platform_mutex_destroy(&log->durability_lock);
   platform_assert_status_ok(rc);
   rc = platform_mutex_destroy(&log->graduate_lock);
   platform_assert_status_ok(rc);
   rc = platform_mutex_destroy(&log->group_lock);
   platform_assert_status_ok(rc);
   platform_free(log->heap_id, log);
}

static void
shard_log_deinit(log_handle *logh)
{
   shard_log *log     = (shard_log *)logh;
   bool32     destroy = FALSE;

   platform_mutex_lock(&log->group_lock);
   platform_assert(!log->deinit_requested);
   for (shard_log_group *group = log->groups_head; group != NULL;
        group                  = group->next)
   {
      platform_assert(group->active_reservations == 0,
                      "log_deinit with an unconsumed write reservation");
   }
   log->deinit_requested = TRUE;
   if (log->ticket_refs == 0) {
      log->destroying = TRUE;
      destroy         = TRUE;
   }
   platform_mutex_unlock(&log->group_lock);

   if (destroy) {
      shard_log_destroy(log);
   }
}

void
shard_log_dec_ref(cache *cc, const log_head *segment)
{
   if (segment->meta_addr == 0) {
      return;
   }
   /*
    * A split make-durable ticket holds its own mini-allocator reference.  The
    * stream owner may therefore release the head while a ticket is waiting;
    * the final ticket consumes the remaining reference after its barrier.
    */
   (void)mini_dec_ref(cc, segment->meta_addr, PAGE_TYPE_LOG);
}

log_head
shard_log_get_head(log_handle *logh)
{
   shard_log *log = (shard_log *)logh;
   return (log_head){
      .addr      = log->addr,
      .meta_addr = log->meta_head,
      .nonce     = log->nonce,
   };
}

bool32
shard_log_valid(shard_log_config *cfg, page_handle *page, log_nonce nonce)
{
   shard_log_hdr *hdr = (shard_log_hdr *)page->data;
   return log_nonce_is_equal(hdr->nonce, nonce)
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
 * followed; once the walk lands on a page, its nonce and checksum are what
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
 * decides whether it is safe to issue a read; nonce and checksum still decide
 * whether that page contributes a link.
 */
static platform_status
shard_log_extent_next_link(cache            *cc,
                           shard_log_config *cfg,
                           uint64            extent_addr,
                           log_nonce         nonce,
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
      if (shard_log_valid(cfg, page, nonce)) {
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
 * the stream's own nonce, so the per-page checks cannot rule out a cycle. A
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
         cc, cfg, extent_addr, head.nonce, &extent_addr);
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

typedef struct shard_log_recover_blob_state {
   cache            *cc;
   shard_log_config *cfg;
   log_nonce         nonce;
} shard_log_recover_blob_state;

static platform_status
shard_log_recover_blob_extent(void *arg, uint64 extent_addr)
{
   shard_log_recover_blob_state *state = arg;

   cache            *cc               = state->cc;
   shard_log_config *cfg              = state->cfg;
   uint64            pages_per_extent = shard_log_pages_per_extent(cfg);
   uint64            page_size        = shard_log_page_size(cfg);
   bool32            readable_pages[MAX_PAGES_PER_EXTENT];

   platform_status rc =
      shard_log_extent_readable_pages(cc, cfg, extent_addr, readable_pages);
   if (!SUCCESS(rc)) {
      return rc;
   }
   shard_log_prefetch_readable_pages(cc, cfg, extent_addr, readable_pages);

   for (uint64 i = 0; i < pages_per_extent; i++) {
      if (!readable_pages[i]) {
         continue;
      }

      uint64       page_addr = extent_addr + i * page_size;
      page_handle *page      = cache_get(cc, page_addr, TRUE, PAGE_TYPE_LOG);
      if (!shard_log_valid(cfg, page, state->nonce)) {
         cache_unget(cc, page);
         continue;
      }

      for (log_entry *le = first_log_entry(page->data);
           !terminal_log_entry(cfg, page->data, le);
           le = log_entry_next(le))
      {
         if (log_entry_message_is_blob(le)) {
            message msg = log_entry_message(cc, le);
            rc          = blob_recover_allocations(cc, message_slice(msg));
            if (!SUCCESS(rc)) {
               cache_unget(cc, page);
               return rc;
            }
         }
      }
      cache_unget(cc, page);
   }
   return STATUS_OK;
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
   (void)hid;

   /*
    * Scan every individually valid page, not only complete groups.  The replay
    * validator examines a trailing incomplete group before it knows that the
    * terminator is absent, and cache_get() requires those blob extents to be
    * protected from allocator reuse first.  The later root-only rebuild drops
    * these conservative references along with the rest of the old log.
    */
   shard_log_recover_blob_state state = {
      .cc = cc, .cfg = cfg, .nonce = head.nonce};
   return shard_log_for_each_extent(
      cc, cfg, head, shard_log_recover_blob_extent, &state);
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

static bool32
shard_log_is_empty(log_handle *logh)
{
   shard_log *log = (shard_log *)logh;
   platform_mutex_lock(&log->group_lock);
   bool32 empty = !log->has_records;
   platform_mutex_unlock(&log->group_lock);
   return empty;
}

static log_ops shard_log_ops = {
   .write_reserve      = shard_log_write_reserve,
   .write_reserved     = shard_log_write_reserved,
   .make_durable_begin = shard_log_make_durable_begin,
   .make_durable_wait  = shard_log_make_durable_wait,
   .seal               = shard_log_seal,
   .deinit             = shard_log_deinit,
   .head               = shard_log_get_head,
   .is_empty           = shard_log_is_empty,
   .size               = shard_log_get_size,
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

   platform_status rc = platform_random_bytes(&log->nonce, sizeof(log->nonce));
   if (!SUCCESS(rc)) {
      platform_error_log("shard_log_init: failed to generate log nonce: %s\n",
                         platform_status_to_string(rc));
      return rc;
   }

   rc = platform_mutex_init(&log->group_lock, 0, hid);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = platform_mutex_init(&log->graduate_lock, 0, hid);
   if (!SUCCESS(rc)) {
      platform_mutex_destroy(&log->group_lock);
      return rc;
   }
   rc = platform_mutex_init(&log->durability_lock, 0, hid);
   if (!SUCCESS(rc)) {
      platform_mutex_destroy(&log->graduate_lock);
      platform_mutex_destroy(&log->group_lock);
      return rc;
   }

   shard_log_group *current = NULL;
   rc                       = shard_log_group_alloc(log, FALSE, &current);
   if (!SUCCESS(rc)) {
      platform_mutex_destroy(&log->durability_lock);
      platform_mutex_destroy(&log->graduate_lock);
      platform_mutex_destroy(&log->group_lock);
      return rc;
   }
   log->groups_head = current;
   log->groups_tail = current;
   log->accepting   = current;

   for (uint64 i = 0; i < SHARD_LOG_NUM_EMERGENCY_GROUPS; i++) {
      shard_log_group *emergency = NULL;
      rc                         = shard_log_group_alloc(log, TRUE, &emergency);
      if (!SUCCESS(rc)) {
         shard_log_group_free(log, current);
         while (log->emergency_pool != NULL) {
            emergency            = log->emergency_pool;
            log->emergency_pool  = emergency->pool_next;
            emergency->pool_next = NULL;
            shard_log_group_free(log, emergency);
         }
         platform_mutex_destroy(&log->durability_lock);
         platform_mutex_destroy(&log->graduate_lock);
         platform_mutex_destroy(&log->group_lock);
         return rc;
      }
      emergency->pool_next = log->emergency_pool;
      log->emergency_pool  = emergency;
   }

   allocator *al = cache_get_allocator(cc);
   rc            = allocator_alloc(al, &log->meta_head, PAGE_TYPE_LOG);
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

platform_status
shard_log_create(cache            *cc,
                 shard_log_config *cfg,
                 platform_heap_id  hid,
                 log_handle      **log_out)
{
   if (log_out == NULL) {
      return STATUS_BAD_PARAM;
   }
   *log_out = NULL;

   shard_log *slog = TYPED_MALLOC(hid, slog);
   if (slog == NULL) {
      platform_error_log("shard_log_create: failed to allocate shard_log\n");
      return STATUS_NO_MEMORY;
   }
   // The heap is remembered in the log so that log_deinit() can free the handle
   // and its staging buffers without the caller touching platform_free().
   platform_status rc = shard_log_init(slog, cc, cfg, hid);
   if (!SUCCESS(rc)) {
      platform_error_log("shard_log_create: shard_log_init failed: %s\n",
                         platform_status_to_string(rc));
      platform_free(hid, slog);
      return rc;
   }
   *log_out = (log_handle *)slog;
   return STATUS_OK;
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
   log_nonce         nonce            = log->nonce;
   data_config      *dcfg             = cfg->data_cfg;
   uint64            pages_per_extent = shard_log_pages_per_extent(cfg);
   allocator        *al               = cache_get_allocator(cc);

   while (extent_addr != 0 && allocator_get_refcount(al, extent_addr) > 0) {
      cache_prefetch(cc, extent_addr, PAGE_TYPE_LOG);
      uint64 next_extent_addr = 0;
      for (uint64 i = 0; i < pages_per_extent; i++) {
         uint64       page_addr = extent_addr + i * shard_log_page_size(cfg);
         page_handle *page      = cache_get(cc, page_addr, TRUE, PAGE_TYPE_LOG);
         if (shard_log_valid(cfg, page, nonce)) {
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
                        log_nonce           nonce,
                        uint64              first_needed_generation,
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
    * the number of its pages actually present. Later groups may already be
    * staging records, but physical graduation is serialized: a group's
    * terminator obtains its address before any page of the next group. A
    * group's pages are therefore contiguous in the traversal and the
    * replayable portion is a prefix of it. We only have to track a run at a
    * time, and count pages.
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
   bool32 finished          = FALSE; // accepted an end-of-stream group

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
    * in a partial extent); nonce and checksum reject readable non-log contents.
    */
   extent_addr = addr;
   while (!broken && !finished && extent_addr != 0
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
         if (!shard_log_valid(cfg, page, nonce)) {
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
               if (group_ends_stream) {
                  /* EOS is authoritative: later pages are not in this stream.
                   */
                  cache_unget(cc, page);
                  finished = TRUE;
                  break;
               }
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

         rc = shard_log_validate_page_blobs(
            cc, cfg, page, first_needed_generation);
         if (!SUCCESS(rc)) {
            platform_error_log("shard_log_iterator_init: blob validation "
                               "failed in group %lu at log page %lu: %s; "
                               "discarding this group and the rest\n",
                               group_id,
                               page_addr,
                               platform_status_to_string(rc));
            cache_unget(cc, page);
            if (STATUS_IS_EQ(rc, STATUS_NO_MEMORY)) {
               return rc;
            }
            broken = TRUE;
            break;
         }
         group_pages++;
         group_entries += hdr->num_entries;
         if (hdr->pages_in_group != 0) {
            if (group_declared != 0) {
               platform_error_log("shard_log_iterator_init: group %lu has two "
                                  "terminators; discarding it and the rest\n",
                                  group_id);
               cache_unget(cc, page);
               broken = TRUE;
               break;
            }
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
   if (!broken && !finished && in_group) {
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
         if (!shard_log_valid(cfg, page, nonce)) {
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

static platform_status
shard_log_iterator_create_internal(cache            *cc,
                                   shard_log_config *cfg,
                                   platform_heap_id  hid,
                                   log_head          head,
                                   uint64            first_needed_generation,
                                   log_iterator    **itor_out)
{
   if (itor_out == NULL) {
      return STATUS_BAD_PARAM;
   }
   *itor_out = NULL;

   shard_log_iterator *itor = TYPED_MALLOC(hid, itor);
   if (itor == NULL) {
      platform_error_log("shard_log_iterator_create: failed to allocate "
                         "shard_log_iterator\n");
      return STATUS_NO_MEMORY;
   }
   platform_status rc = shard_log_iterator_init(
      cc, cfg, hid, head.addr, head.nonce, first_needed_generation, itor);
   if (!SUCCESS(rc)) {
      platform_error_log("shard_log_iterator_create: shard_log_iterator_init "
                         "failed: %s\n",
                         platform_status_to_string(rc));
      platform_free(hid, itor);
      return rc;
   }
   *itor_out = &itor->super;
   return STATUS_OK;
}

platform_status
shard_log_iterator_create(cache            *cc,
                          shard_log_config *cfg,
                          platform_heap_id  hid,
                          log_head          head,
                          uint64            first_needed_generation,
                          log_iterator    **itor_out)
{
   return shard_log_iterator_create_internal(
      cc, cfg, hid, head, first_needed_generation, itor_out);
}
