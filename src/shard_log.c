// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * shard_log.c --
 *
 *     This file contains the implementation for a sharded write-ahead log.
 */

#include "platform.h"

#include "shard_log.h"

#include "poison.h"

#define SHARD_WAIT 1
#define SHARD_UNMAPPED UINT64_MAX

static uint64 shard_log_magic_idx = 0;

int
       shard_log_write(log_handle *log, char *key, char *data, uint64 generation);
uint64 shard_log_addr      (log_handle *log);
uint64 shard_log_meta_addr (log_handle *log);
uint64 shard_log_magic     (log_handle *log);

static log_ops shard_log_ops = {
   .write     = shard_log_write,
   .addr      = shard_log_addr,
   .meta_addr = shard_log_meta_addr,
   .magic     = shard_log_magic,
};

void
shard_log_iterator_get_curr(iterator *itor, char **key, char **data);
platform_status
shard_log_iterator_at_end(iterator *itor, bool *at_end);
platform_status
shard_log_iterator_advance(iterator *itor);

const static iterator_ops shard_log_iterator_ops = {
   .get_curr = shard_log_iterator_get_curr,
   .at_end   = shard_log_iterator_at_end,
   .advance  = shard_log_iterator_advance,
   .print    = NULL,
};

static inline checksum128
shard_log_checksum(shard_log_config *cfg,
                   page_handle      *page)
{
   return platform_checksum128(page->data + 16, cfg->page_size - 16,
                               cfg->seed);
}

static inline shard_log_thread_data *
shard_log_get_thread_data(shard_log *log,
                          threadid   thr_id)
{
   return &log->thread_data[thr_id];
}

page_handle *
shard_log_alloc(shard_log *log, uint64 *next_extent)
{
   uint64 addr = mini_allocator_alloc(&log->mini, 0, NULL, next_extent);
   return cache_alloc(log->cc, addr, PAGE_TYPE_LOG);
}

platform_status
shard_log_init(shard_log        *log,
               cache            *cc,
               shard_log_config *cfg)
{
   memset(log, 0, sizeof(shard_log));
   log->cc = cc;
   log->cfg = cfg;
   log->super.ops = &shard_log_ops;

   uint64 magic_idx = __sync_fetch_and_add(&shard_log_magic_idx, 1);
   log->magic = platform_checksum64(&magic_idx, sizeof(uint64), cfg->seed);

   allocator *     al = cache_allocator(cc);
   platform_status rc = allocator_alloc_extent(al, &log->meta_head);
   platform_assert_status_ok(rc);

   for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      shard_log_thread_data *thread_data
         = shard_log_get_thread_data(log, thr_i);
      thread_data->addr   = SHARD_UNMAPPED;
      thread_data->pos    = 0;
      thread_data->offset = 0;
   }

   log->addr = mini_allocator_init(&log->mini, cc, log->cfg->data_cfg,
         log->meta_head, 0, 1, PAGE_TYPE_LOG);
   //platform_log("addr: %lu meta_head: %lu\n", log->addr, log->meta_head);

   return STATUS_OK;
}

void
shard_log_zap(shard_log *log)
{
   cache            *cc = log->cc;

   for (threadid i = 0; i < MAX_THREADS; i++) {
      shard_log_thread_data *thread_data = shard_log_get_thread_data(log, i);
      thread_data->addr = SHARD_UNMAPPED;
      thread_data->pos = 0;
      thread_data->offset = 0;
   }

   mini_allocator_zap(cc, NULL, log->meta_head, NULL, NULL, PAGE_TYPE_LOG);
}

int
shard_log_write(log_handle *logh,
                char       *key,
                char       *data,
                uint64      generation)
{
   shard_log *log = (shard_log *)logh;
   cache *    cc  = log->cc;

   page_handle *          page;
   shard_log_thread_data *thread_data =
      shard_log_get_thread_data(log, platform_get_tid());
   if (thread_data->addr == SHARD_UNMAPPED) {
      uint64 next_extent;
      page                  = shard_log_alloc(log, &next_extent);
      thread_data->addr     = page->disk_addr;
      shard_log_hdr *hdr    = (shard_log_hdr *)page->data;
      hdr->magic            = log->magic;
      hdr->next_extent_addr = next_extent;
      thread_data->offset   = sizeof(shard_log_hdr);
      thread_data->pos      = 0;
   } else {
      page        = cache_get(cc, thread_data->addr, TRUE, PAGE_TYPE_LOG);
      uint64 wait = 1;
      while (!cache_claim(cc, page)) {
         platform_sleep(wait);
         wait = wait > 1024 ? wait : 2 * wait;
      }
      cache_lock(cc, page);
   }
   char *cursor = page->data + thread_data->offset;

   uint64 *generation_cursor = (uint64 *)cursor;
   cursor += sizeof(uint64);
   thread_data->offset += sizeof(uint64);

   char *key_cursor = cursor;
   cursor += log->cfg->data_cfg->key_size;
   thread_data->offset += log->cfg->data_cfg->key_size;

   char *data_cursor = cursor;
   cursor += log->cfg->data_cfg->message_size;
   thread_data->offset += log->cfg->data_cfg->message_size;
   debug_assert(cursor - page->data < log->cfg->page_size);

   *generation_cursor = generation;
   memmove(key_cursor, key, log->cfg->data_cfg->key_size);
   memmove(data_cursor, data, log->cfg->data_cfg->message_size);

   thread_data->pos++;
   if (thread_data->pos == log->cfg->entries_per_page) {
      shard_log_hdr *hdr = (shard_log_hdr *)page->data;
      hdr->checksum = shard_log_checksum(log->cfg, page);

      cache_unlock(cc, page);
      cache_unclaim(cc, page);
      cache_page_sync(cc, page, FALSE, PAGE_TYPE_LOG);
      cache_unget(cc, page);

      thread_data->addr = SHARD_UNMAPPED;
      thread_data->pos = 0;
      thread_data->offset = 0;
   } else {
      cache_unlock(cc, page);
      cache_unclaim(cc, page);
      cache_unget(cc, page);
   }

   return 0;
}

uint64
shard_log_addr(log_handle *logh)
{
   shard_log *log = (shard_log *)logh;
   return log->addr;
}

uint64
shard_log_meta_addr(log_handle *logh)
{
   shard_log *log = (shard_log *)logh;
   return log->meta_head;
}

uint64
shard_log_magic(log_handle *logh)
{
   shard_log *log = (shard_log *)logh;
   return log->magic;
}

bool
shard_log_valid(shard_log_config *cfg,
                page_handle      *page,
                uint64            magic)
{
   shard_log_hdr *hdr = (shard_log_hdr *)page->data;
   return hdr->magic == magic
      && platform_checksum_is_equal(hdr->checksum, shard_log_checksum(cfg, page));
}

uint64
shard_log_next_extent_addr(shard_log_config *cfg,
                           page_handle      *page)
{
   shard_log_hdr *hdr = (shard_log_hdr *)page->data;
   return hdr->next_extent_addr;
}

int
shard_log_compare(const void *p1,
                  const void *p2,
                  void *unused)
{
   return *(uint64 *)p1 - *(uint64 *)p2;
}

log_handle *
log_create(cache            *cc,
           log_config       *lcfg,
           platform_heap_id  hid)
{
   shard_log_config *cfg = (shard_log_config *)lcfg;
   shard_log *slog = TYPED_MALLOC(hid, slog);
   platform_status rc = shard_log_init(slog, cc, cfg);
   platform_assert(SUCCESS(rc));
   return (log_handle *)slog;
}

platform_status
shard_log_iterator_init(cache              *cc,
                        shard_log_config   *cfg,
                        platform_heap_id    hid,
                        uint64              addr,
                        uint64              magic,
                        shard_log_iterator *itor)
{
   page_handle *page;
   uint64       i;
   uint64       pages_per_extent = cfg->extent_size / cfg->page_size;
   uint64       page_addr;
   uint64       num_valid_pages = 0;
   uint64       extent_addr;
   uint64       next_extent_addr;
   uint64       entry_size;
   char        *cursor, *temp;

   memset(itor, 0, sizeof(shard_log_iterator));
   itor->super.ops = &shard_log_iterator_ops;
   itor->cfg = cfg;

   // traverse the log extents and calculate the required space
   extent_addr = addr;
   while (extent_addr != 0 && cache_get_ref(cc, extent_addr) > 0) {
      cache_prefetch(cc, extent_addr, PAGE_TYPE_FILTER);
      next_extent_addr = 0;
      for (i = 0; i < pages_per_extent; i++) {
         page_addr = extent_addr + i * cfg->page_size;
         page = cache_get(cc, page_addr, TRUE, PAGE_TYPE_LOG);
         if (shard_log_valid(cfg, page, magic)) {
            num_valid_pages++;
            next_extent_addr = shard_log_next_extent_addr(cfg, page);
         }
         cache_unget(cc, page);
      }
      extent_addr = next_extent_addr;
   }

   itor->total = num_valid_pages * cfg->entries_per_page;
   entry_size = sizeof(uint64) + cfg->data_cfg->key_size
         + cfg->data_cfg->message_size;
   itor->contents = TYPED_ARRAY_MALLOC(hid, itor->contents,
                                       itor->total * entry_size);
   itor->cursor = itor->contents;

   // traverse the log extents again and copy the kv pairs
   extent_addr = addr;
   while (extent_addr != 0
         && cache_get_ref(cc, extent_addr) > 0) {
      cache_prefetch(cc, extent_addr, PAGE_TYPE_FILTER);
      next_extent_addr = 0;
      for (i = 0; i < pages_per_extent; i++) {
         page_addr = extent_addr + i * cfg->page_size;
         page = cache_get(cc, page_addr, TRUE, PAGE_TYPE_LOG);
         if (shard_log_valid(cfg, page, magic)) {
            cursor = page->data + sizeof(shard_log_hdr);
            memmove(itor->cursor, cursor,
                  cfg->entries_per_page * entry_size);
            itor->cursor += cfg->entries_per_page * entry_size;
            next_extent_addr = shard_log_next_extent_addr(cfg, page);
         }
         cache_unget(cc, page);
      }
      extent_addr = next_extent_addr;
   }

   itor->cursor = itor->contents;
   // sort by generation
   temp = TYPED_ARRAY_MALLOC(hid, temp, entry_size);
   if (temp == NULL) {
      platform_free(hid, itor->contents);
      return STATUS_NO_MEMORY;
   }
   platform_sort_slow(itor->cursor, itor->total, entry_size,
                      shard_log_compare, NULL, temp);
   platform_free(hid, temp);

   return STATUS_OK;
}

void
shard_log_iterator_deinit(platform_heap_id hid, shard_log_iterator *itor)
{
   platform_free(hid, itor->contents);
}

// FIXME: [nsarmicanic 2020-07-12]
//    Talk to alex and/or reverse engineer what
//    shard log actually does (what are the types?)
//    Do we also need a key_type?
void
shard_log_iterator_get_curr(iterator *itorh, char **key, char **data)
{
   shard_log_iterator *itor = (shard_log_iterator *)itorh;
   char *cursor = itor->cursor;

   // skip over generation
   cursor += sizeof(uint64);
   *key = cursor;
   cursor += itor->cfg->data_cfg->key_size;
   *data = cursor;
}

platform_status
shard_log_iterator_at_end(iterator *itorh,
                          bool     *at_end)
{
   shard_log_iterator *itor = (shard_log_iterator *)itorh;
   *at_end = itor->pos == itor->total;

   return STATUS_OK;
}

platform_status
shard_log_iterator_advance(iterator *itorh)
{
   shard_log_iterator *itor = (shard_log_iterator *)itorh;
   uint64 entry_size = sizeof(uint64) + itor->cfg->data_cfg->key_size
      + itor->cfg->data_cfg->message_size;

   itor->pos++;
   itor->cursor += entry_size;

   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 *
 * shard_log_config_init --
 *
 *      Initialize shard_log config values
 *
 *-----------------------------------------------------------------------------
 */

void shard_log_config_init(shard_log_config *log_cfg,
                           data_config      *data_cfg,
                           uint64            page_size,
                           uint64            extent_size)
{
   uint64 log_entry_size;

   ZERO_CONTENTS(log_cfg);

   log_cfg->page_size = page_size;
   log_cfg->extent_size = extent_size;

   log_entry_size
      = data_cfg->key_size + data_cfg->message_size + sizeof(uint64);
   log_cfg->entries_per_page
      = (log_cfg->page_size - sizeof(shard_log_hdr)) / log_entry_size;
   log_cfg->seed = HASH_SEED;
   log_cfg->data_cfg = data_cfg;
}

void
shard_log_print(shard_log *log) {
   uint64 i,j;
   cache *cc = log->cc;
   uint64 extent_addr = log->addr;
   uint64 next_extent_addr;
   uint64 page_addr;
   page_handle *page;
   shard_log_config *cfg = log->cfg;
   uint64 magic = log->magic;
   uint64 pages_per_extent = cfg->extent_size / cfg->page_size;
   char *cursor;

   char *key;
   char *data;
   uint64 generation;

   char key_str[128];
   char data_str[128];

   while (extent_addr != 0 && cache_get_ref(cc, extent_addr) > 0) {
      cache_prefetch(cc, extent_addr, PAGE_TYPE_FILTER);
      next_extent_addr = 0;
      for (i = 0; i < pages_per_extent; i++) {
         page_addr = extent_addr + i * cfg->page_size;
         page = cache_get(cc, page_addr, TRUE, PAGE_TYPE_LOG);
         if (shard_log_valid(cfg, page, magic)) {
            next_extent_addr = shard_log_next_extent_addr(cfg, page);
            cursor = page->data + sizeof(shard_log_hdr);
            for (j = 0; j < log->cfg->entries_per_page; j++) {
               generation = *(uint64 *)cursor;
               cursor += sizeof(uint64);
               key = cursor;
               cursor += log->cfg->data_cfg->key_size;
               data = cursor;
               cursor += log->cfg->data_cfg->message_size;
               data_key_to_string(cfg->data_cfg, key, key_str, 128);
               data_message_to_string(cfg->data_cfg, data, data_str, 128);
               platform_log("%s -- %s : %lu\n", key_str, data_str, generation);
            }
         }
         cache_unget(cc, page);
      }
      extent_addr = next_extent_addr;
   }
}
