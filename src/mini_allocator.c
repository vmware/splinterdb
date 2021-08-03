// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * mini_allocator.c --
 *
 *     This file contains the implementation for an allocator which
 *     allocates individual pages from extents.
 */

#include "platform.h"

#include "util.h"
#include "mini_allocator.h"
#include "allocator.h"
#include "cache.h"
#include "data.h"

#include "poison.h"

#define MINI_WAIT 1

typedef struct mini_allocator_meta_entry {
  uint64 extent_addr;
  bool zapped;
  char start_key[MAX_KEY_SIZE];
  char end_key[MAX_KEY_SIZE];
} mini_allocator_meta_entry;

typedef struct mini_allocator_meta_hdr {
  uint64 next_meta_addr;
  uint64 pos;
  mini_allocator_meta_entry entry[];
} mini_allocator_meta_hdr;

uint64 mini_allocator_init(mini_allocator *mini, cache *cc,
                           data_config *data_cfg, uint64 meta_head,
                           uint64 meta_tail, uint64 num_batches,
                           page_type type) {
  page_handle *meta_page;
  const uint64 pages_per_extent = cache_extent_size(cc) / cache_page_size(cc);
  page_handle *new_pages[MAX_PAGES_PER_EXTENT];
  uint64 i;
  uint64 batch;
  uint64 wait = 1;

  memset(mini, 0, sizeof(mini_allocator));
  mini->cc = cc;
  mini->data_cfg = data_cfg;
  mini->meta_head = meta_head;
  if (meta_tail == 0) {
    mini->meta_tail = meta_head;
  } else {
    mini->meta_tail = meta_tail;
  }
  mini->type = type;
  mini->num_batches = num_batches;
  platform_assert(num_batches <= MINI_MAX_BATCHES);

  meta_page = cache_get(cc, mini->meta_tail, TRUE, type);
  while (!cache_claim(cc, meta_page)) {
    // should never happen
    platform_sleep(wait);
    wait = wait > 1024 ? wait : 2 * wait;
  }
  wait = 1;
  cache_lock(cc, meta_page);
  mini_allocator_meta_hdr *hdr = (mini_allocator_meta_hdr *)meta_page->data;
  if (meta_tail == 0) {
    hdr->next_meta_addr = 0;
    hdr->pos = 0;
  }

  for (batch = 0; batch < num_batches; batch++) {
    cache_extent_alloc(cc, new_pages, type);
    mini->next_extent[batch] = new_pages[0]->disk_addr;
    // platform_log("mini_allocator_alloc %lu-%lu.%lu : %lu\n",
    //      mini->meta_head, mini->meta_tail, hdr->pos,
    //      mini->next_extent[batch]);
    if (hdr->pos ==
        (cache_page_size(mini->cc) - sizeof(mini_allocator_meta_hdr)) /
            sizeof(uint64)) {
      // need a new meta page
      mini->meta_tail += cache_page_size(mini->cc);
      if (mini->meta_tail % cache_extent_size(mini->cc) == 0) {
        // need to allocate the next meta extent
        page_handle *new_pages[MAX_PAGES_PER_EXTENT];
        cache_extent_alloc(mini->cc, new_pages, type);
        mini->meta_tail = new_pages[0]->disk_addr;
        for (i = 0; i < pages_per_extent; i++) {
          cache_unlock(mini->cc, new_pages[i]);
          cache_unclaim(mini->cc, new_pages[i]);
          cache_unget(mini->cc, new_pages[i]);
        }
      }
      hdr->next_meta_addr = mini->meta_tail;
      page_handle *last_meta_page = meta_page;
      meta_page = cache_get(mini->cc, mini->meta_tail, TRUE, type);
      while (!cache_claim(mini->cc, meta_page)) {
        platform_sleep(wait);
        wait = wait > 1024 ? wait : 2 * wait;
      }
      wait = 1;
      cache_lock(mini->cc, meta_page);
      cache_mark_dirty(mini->cc, last_meta_page);
      cache_unlock(mini->cc, last_meta_page);
      cache_unclaim(mini->cc, last_meta_page);
      cache_unget(mini->cc, last_meta_page);
      hdr = (mini_allocator_meta_hdr *)meta_page->data;
      hdr->pos = 0;
      hdr->next_meta_addr = 0;
    }
    for (i = 0; i < pages_per_extent; i++) {
      cache_unlock(cc, new_pages[i]);
      cache_unclaim(cc, new_pages[i]);
      cache_unget(cc, new_pages[i]);
    }
  }

  cache_mark_dirty(cc, meta_page);
  cache_unlock(cc, meta_page);
  cache_unclaim(cc, meta_page);
  cache_unget(cc, meta_page);

  return mini->next_extent[0];
}

uint64 mini_allocator_alloc(mini_allocator *mini, uint64 batch, char *key,
                            uint64 *next_extent) {
  uint64 next_addr = mini->next_addr[batch];
  uint64 next_extent_addr;
  page_handle *meta_page;
  uint64 i;
  mini_allocator_meta_hdr *hdr;
  uint64 new_meta_tail;
  uint64 wait = 1;
  platform_status rc = STATUS_OK;

  platform_assert(batch < mini->num_batches);

  // wait until we hold the lock for our batch
  while (0 || next_addr == MINI_WAIT ||
         !__sync_bool_compare_and_swap(&mini->next_addr[batch], next_addr,
                                       MINI_WAIT)) {
    platform_sleep(wait);
    wait = wait > 1024 ? wait : 2 * wait;
    next_addr = mini->next_addr[batch];
  }
  wait = 1;

  if (next_addr % cache_extent_size(mini->cc) == 0) {
    // need to allocate the next extent
    uint64 pages_per_extent =
        cache_extent_size(mini->cc) / cache_page_size(mini->cc);
    page_handle *new_pages[MAX_PAGES_PER_EXTENT];
    rc = cache_extent_alloc(mini->cc, new_pages, mini->type);
    platform_assert_status_ok(rc);
    // platform_log("meta_head %lu next_extent %lu new_extent %lu\n",
    //      mini->meta_head, mini->next_extent[batch], new_pages[0]->disk_addr);
    debug_assert(mini->next_extent[batch] != new_pages[0]->disk_addr);
    next_extent_addr = mini->next_extent[batch];
    mini->next_extent[batch] = new_pages[0]->disk_addr;
    next_addr = next_extent_addr;
    if (next_extent) {
      *next_extent = mini->next_extent[batch];
    }
    mini->next_addr[batch] = next_extent_addr + cache_page_size(mini->cc);
    for (i = 0; i < pages_per_extent; i++) {
      cache_unlock(mini->cc, new_pages[i]);
      cache_unclaim(mini->cc, new_pages[i]);
      cache_unget(mini->cc, new_pages[i]);
    }

    while (1) {
      meta_page = cache_get(mini->cc, mini->meta_tail, TRUE, mini->type);
      if (cache_claim(mini->cc, meta_page)) {
        if (meta_page->disk_addr == mini->meta_tail)
          break;
        else
          cache_unclaim(mini->cc, meta_page);
      }
      cache_unget(mini->cc, meta_page);
      platform_sleep(wait);
      wait = wait > 1024 ? wait : 2 * wait;
    }
    wait = 1;
    cache_lock(mini->cc, meta_page);
    debug_assert(meta_page->disk_addr == mini->meta_tail);

    hdr = (mini_allocator_meta_hdr *)meta_page->data;
    if (hdr->pos ==
        (cache_page_size(mini->cc) - sizeof(mini_allocator_meta_hdr)) /
            sizeof(mini_allocator_meta_entry)) {
      // need a new meta page
      new_meta_tail = mini->meta_tail + cache_page_size(mini->cc);
      if (new_meta_tail % cache_extent_size(mini->cc) == 0) {
        // need to allocate the next meta extent
        uint64 pages_per_extent =
            cache_extent_size(mini->cc) / cache_page_size(mini->cc);
        page_handle *new_pages[MAX_PAGES_PER_EXTENT];
        cache_extent_alloc(mini->cc, new_pages, mini->type);
        new_meta_tail = new_pages[0]->disk_addr;
        for (i = 0; i < pages_per_extent; i++) {
          cache_unlock(mini->cc, new_pages[i]);
          cache_unclaim(mini->cc, new_pages[i]);
          cache_unget(mini->cc, new_pages[i]);
        }
      }
      hdr->next_meta_addr = new_meta_tail;
      page_handle *last_meta_page = meta_page;
      meta_page = cache_get(mini->cc, new_meta_tail, TRUE, mini->type);
      while (!cache_claim(mini->cc, meta_page)) {
        // should never happen
        platform_sleep(wait);
        wait = wait > 1024 ? wait : 2 * wait;
      }
      wait = 1;
      cache_lock(mini->cc, meta_page);
      mini->meta_tail = new_meta_tail;
      cache_mark_dirty(mini->cc, last_meta_page);
      cache_unlock(mini->cc, last_meta_page);
      cache_unclaim(mini->cc, last_meta_page);
      cache_unget(mini->cc, last_meta_page);
      hdr = (mini_allocator_meta_hdr *)meta_page->data;
      hdr->pos = 0;
      hdr->next_meta_addr = 0;
    }

    // platform_log("mini_allocator_alloc %lu-%lu.%lu : %lu\n",
    //      mini->meta_head, mini->meta_tail, hdr->pos, new_extent_addr);

    platform_assert(hdr == (mini_allocator_meta_hdr *)meta_page->data);
    uint64 new_pos = hdr->pos;
    uint64 new_meta_addr = meta_page->disk_addr;
    mini_allocator_meta_entry *entry = &hdr->entry[hdr->pos++];
    if (key != NULL) {
      data_key_copy(mini->data_cfg, entry->start_key, key);

      // Set the end_key of the last extent from this batch
      if (mini->last_meta_addr[batch] != 0) {
        page_handle *last_meta_page = NULL;
        if (mini->last_meta_addr[batch] == mini->meta_tail) {
          last_meta_page = meta_page;
        } else {
          last_meta_page = cache_get(mini->cc, mini->last_meta_addr[batch],
                                     TRUE, mini->type);
          while (!cache_claim(mini->cc, last_meta_page)) {
            // should never happen
            platform_sleep(wait);
            wait = wait > 1024 ? wait : 2 * wait;
          }
          wait = 1;
          cache_lock(mini->cc, last_meta_page);
        }
        mini_allocator_meta_hdr *last_hdr =
            (mini_allocator_meta_hdr *)last_meta_page->data;
        mini_allocator_meta_entry *last_entry =
            &last_hdr->entry[mini->last_meta_pos[batch]];
        data_key_copy(mini->data_cfg, last_entry->end_key, key);
        cache_mark_dirty(mini->cc, last_meta_page);
        if (mini->last_meta_addr[batch] != mini->meta_tail) {
          cache_unlock(mini->cc, last_meta_page);
          cache_unclaim(mini->cc, last_meta_page);
          cache_unget(mini->cc, last_meta_page);
        }
      }
      mini->last_meta_pos[batch] = new_pos;
      mini->last_meta_addr[batch] = new_meta_addr;
    } else {
      memset(entry->start_key, 0, MAX_KEY_SIZE);
      memset(entry->end_key, 0, MAX_KEY_SIZE);
    }
    entry->extent_addr = next_extent_addr;
    entry->zapped = FALSE;
    // if (key != NULL) {
    //   char key_str[256];
    //   data_key_to_string(key, key_str, 24);
    //   platform_log("alloc %12lu %12lu %2lu %s\n",
    //         next_extent_addr, new_meta_addr, new_pos, key_str);
    //} else {
    //   platform_log("alloc %12lu %12lu %2lu NULL\n",
    //         next_extent_addr, new_meta_addr, new_pos);
    //}
    cache_mark_dirty(mini->cc, meta_page);
    cache_unlock(mini->cc, meta_page);
    cache_unclaim(mini->cc, meta_page);
    cache_unget(mini->cc, meta_page);

    return next_addr;
  }

  // we got a valid new addr
  if (next_extent) {
    *next_extent = mini->next_extent[batch];
  }
  mini->next_addr[batch] = next_addr + cache_page_size(mini->cc);
  return next_addr;
}

void mini_allocator_release(mini_allocator *mini, char *key) {
  for (uint64 batch = 0; batch < mini->num_batches; batch++) {
    // Dealloc the next extent
    cache_dealloc(mini->cc, mini->next_extent[batch], mini->type);

    // Set the end_key of the last extent from this batch
    if (key != NULL && mini->last_meta_addr[batch] != 0) {
      page_handle *last_meta_page =
          cache_get(mini->cc, mini->last_meta_addr[batch], TRUE, mini->type);
      uint64 wait = 1;
      while (!cache_claim(mini->cc, last_meta_page)) {
        // should never happen
        platform_sleep(wait);
        wait = wait > 1024 ? wait : 2 * wait;
      }
      wait = 1;
      cache_lock(mini->cc, last_meta_page);
      mini_allocator_meta_hdr *last_hdr =
          (mini_allocator_meta_hdr *)last_meta_page->data;
      mini_allocator_meta_entry *last_entry =
          &last_hdr->entry[mini->last_meta_pos[batch]];
      data_key_copy(mini->data_cfg, last_entry->end_key, key);
      cache_mark_dirty(mini->cc, last_meta_page);
      cache_unlock(mini->cc, last_meta_page);
      cache_unclaim(mini->cc, last_meta_page);
      cache_unget(mini->cc, last_meta_page);
    }
  }
}

typedef bool (*mini_allocator_for_each_fn)(cache *cc, page_type type,
                                           uint64 base_addr,
                                           uint64 *pages_outstanding);

void mini_allocator_print(cache *cc, data_config *data_cfg, page_type type,
                          uint64 meta_head) {
  page_handle *meta_page;
  uint64 i;
  mini_allocator_meta_hdr *hdr;
  uint64 next_meta_addr = meta_head;

  do {
    meta_page = cache_get(cc, next_meta_addr, TRUE, PAGE_TYPE_MISC);
    hdr = (mini_allocator_meta_hdr *)meta_page->data;

    platform_log("meta addr %12lu\n", next_meta_addr);
    for (i = 0; i < hdr->pos; i++) {
      mini_allocator_meta_entry *entry = &hdr->entry[i];
      char start_key_str[256];
      data_key_to_string(data_cfg, entry->start_key, start_key_str, 24);
      char end_key_str[256];
      data_key_to_string(data_cfg, entry->end_key, end_key_str, 24);
      allocator *al = cache_allocator(cc);
      uint8 ref_count = allocator_get_refcount(al, entry->extent_addr);
      platform_log("%2lu %12lu %s %s %d (%u)\n", i, entry->extent_addr,
                   start_key_str, end_key_str, entry->zapped, ref_count);
    }

    next_meta_addr = hdr->next_meta_addr;

    cache_unget(cc, meta_page);
  } while (next_meta_addr != 0);
}

static inline bool mini_allocator_addrs_share_extent(cache *cc,
                                                     uint64 left_addr,
                                                     uint64 right_addr) {
  uint64 extent_size = cache_extent_size(cc);
  return right_addr / extent_size == left_addr / extent_size;
}

bool mini_allocator_for_each(cache *cc, data_config *data_cfg, page_type type,
                             uint64 meta_head, mini_allocator_for_each_fn func,
                             const char *start_key, const char *end_key,
                             uint64 *pages_outstanding) {
  page_handle *meta_page;
  uint64 i;
  mini_allocator_meta_hdr *hdr;
  uint64 next_meta_addr = meta_head;
  uint64 last_meta_addr;
  uint64 wait = 1;

  debug_assert(IMPLIES(data_cfg == NULL, start_key == NULL));

  bool fully_zapped = TRUE;
  do {
    meta_page = cache_get(cc, next_meta_addr, TRUE, PAGE_TYPE_MISC);
    while (!cache_claim(cc, meta_page)) {
      cache_unget(cc, meta_page);
      meta_page = NULL;
      platform_sleep(wait);
      wait = wait > 1024 ? wait : 2 * wait;
      meta_page = cache_get(cc, next_meta_addr, TRUE, PAGE_TYPE_MISC);
    }
    wait = 1;
    cache_lock(cc, meta_page);

    hdr = (mini_allocator_meta_hdr *)meta_page->data;

    for (i = 0; i < hdr->pos; i++) {
      mini_allocator_meta_entry *entry = &hdr->entry[i];
      /*
       * extent is in range if
       * 1. full range (start_key == NULL and end_key == NULL)
       * 2. extent in range (start_key_[1,2] < end_key_[2,1])
       * 3. range is a point (end_key == NULL) and point is in extent
       */
      bool extent_in_range = FALSE;
      if (start_key == NULL && end_key == NULL) {
        // case 1
        extent_in_range = TRUE;
      } else if (end_key == NULL) {
        // case 3
        extent_in_range =
            1 && data_key_compare(data_cfg, start_key, entry->end_key) <= 0 &&
            data_key_compare(data_cfg, entry->start_key, start_key) <= 0;
      } else {
        // case 2
        extent_in_range =
            1 && data_key_compare(data_cfg, start_key, entry->end_key) <= 0 &&
            data_key_compare(data_cfg, entry->start_key, end_key) <= 0;
      }
      if (extent_in_range) {
        if (entry->zapped) {
          platform_log("ERROR: entry %lu already_zapped\n", entry->extent_addr);
        }
        platform_assert(!entry->zapped);
        entry->zapped = func(cc, type, entry->extent_addr, pages_outstanding);
      }
      fully_zapped = fully_zapped && entry->zapped;
    }

    last_meta_addr = next_meta_addr;
    next_meta_addr = hdr->next_meta_addr;

    cache_mark_dirty(cc, meta_page);
    cache_unlock(cc, meta_page);
    cache_unclaim(cc, meta_page);
    cache_unget(cc, meta_page);
  } while (next_meta_addr != 0);
  if (fully_zapped) {
    // platform_log("fully zapped %lu\n", meta_head - 4096);
    uint64 next_meta_addr = meta_head;
    do {
      meta_page = cache_get(cc, next_meta_addr, TRUE, PAGE_TYPE_MISC);
      hdr = (mini_allocator_meta_hdr *)meta_page->data;
      last_meta_addr = next_meta_addr;
      next_meta_addr = hdr->next_meta_addr;
      cache_unget(cc, meta_page);
      if (!mini_allocator_addrs_share_extent(cc, last_meta_addr,
                                             next_meta_addr)) {
        uint64 last_meta_base_addr =
            last_meta_addr / cache_extent_size(cc) * cache_extent_size(cc);
        func(cc, type, last_meta_base_addr, pages_outstanding);
      }
    } while (next_meta_addr != 0);
  }

  return fully_zapped;
}

bool mini_allocator_zap_extent(cache *cc, page_type type, uint64 base_addr,
                               uint64 *pages_outstanding) {
  return cache_dealloc(cc, base_addr, type);
}

bool mini_allocator_zap(cache *cc, data_config *data_cfg, uint64 meta_head,
                        const char *start_key, const char *end_key,
                        page_type type) {
  // if (start_key != NULL) {
  //   char start_key_str[256];
  //   if (start_key != NULL) {
  //      data_key_to_string(start_key, start_key_str, 24);
  //   }
  //   if (end_key == NULL) {
  //      platform_log("mini_allocator_zap %12lu %s\n",
  //            meta_head, start_key_str);
  //   } else {
  //      char end_key_str[256];
  //      data_key_to_string(end_key, end_key_str, 24);
  //      platform_log("mini_allocator_zap %12lu %s %s\n",
  //            meta_head, start_key_str, end_key_str);
  //   }
  //} else {
  //   platform_log("mini_allocator_zap %12lu full\n", meta_head);
  //}
  // mini_allocator_print(cc, data_cfg, type, meta_head);
  bool fully_zapped = mini_allocator_for_each(cc, data_cfg, type, meta_head,
                                              mini_allocator_zap_extent,
                                              start_key, end_key, NULL);
  // if (fully_zapped) {
  //   platform_log("fully zapped\n");
  //}
  //} else {
  //   platform_log("mini allocator after zap\n");
  //   mini_allocator_print(cc, data_cfg, type, meta_head);
  //}
  return fully_zapped;
}

bool mini_allocator_sync_extent(cache *cc, page_type type, uint64 base_addr,
                                uint64 *pages_outstanding) {
  cache_extent_sync(cc, base_addr, pages_outstanding);
  return FALSE;
}

void mini_allocator_sync(cache *cc, page_type type, uint64 meta_head,
                         uint64 *pages_outstanding) {
  mini_allocator_for_each(cc, NULL, type, meta_head, mini_allocator_sync_extent,
                          NULL, NULL, pages_outstanding);
}

bool mini_allocator_inc_extent(cache *cc, page_type type, uint64 base_addr,
                               uint64 *pages_outstanding) {
  allocator *al = cache_allocator(cc);
  allocator_inc_refcount(al, base_addr);
  return FALSE;
}

void mini_allocator_inc_range(cache *cc, data_config *data_cfg, page_type type,
                              uint64 meta_head, const char *start_key,
                              const char *end_key) {
  // if (start_key != NULL) {
  //   char start_key_str[256];
  //   if (start_key != NULL) {
  //      data_key_to_string(start_key, start_key_str, 24);
  //   }
  //   if (end_key == NULL) {
  //      platform_log("mini_allocator_inc_range %12lu %s\n",
  //            meta_head, start_key_str);
  //   } else {
  //      char end_key_str[256];
  //      data_key_to_string(end_key, end_key_str, 24);
  //      platform_log("mini_allocator_inc_range %12lu %s %s\n",
  //            meta_head, start_key_str, end_key_str);
  //   }
  //} else {
  //   platform_log("mini_allocator_inc_range %12lu full\n", meta_head);
  //}
  // mini_allocator_print(cc, data_cfg, type, meta_head);
  mini_allocator_for_each(cc, data_cfg, type, meta_head,
                          mini_allocator_inc_extent, start_key, end_key, NULL);
  // platform_log("mini allocator after inc\n");
  // mini_allocator_print(cc, data_cfg, type, meta_head);
}

uint64 mini_allocator_extent_count(cache *cc, page_type type,
                                   uint64 meta_head) {
  page_handle *meta_page;
  uint64 next_meta_addr = meta_head;
  uint64 num_extents = 0;

  do {
    meta_page = cache_get(cc, next_meta_addr, TRUE, PAGE_TYPE_MISC);
    num_extents++;

    mini_allocator_meta_hdr *hdr = (mini_allocator_meta_hdr *)meta_page->data;

    for (uint64 i = 0; i < hdr->pos; i++) {
      mini_allocator_meta_entry *entry = &hdr->entry[i];
      if (!entry->zapped) {
        num_extents++;
      }
    }
    next_meta_addr = hdr->next_meta_addr;
    cache_unget(cc, meta_page);
  } while (next_meta_addr != 0);

  return num_extents;
}

bool mini_allocator_count_extent(cache *cc, page_type type, uint64 base_addr,
                                 uint64 *count) {
  (*count)++;
  return FALSE;
}

uint64 mini_allocator_count_extents_in_range(cache *cc, data_config *data_cfg,
                                             page_type type, uint64 meta_head,
                                             const char *start_key,
                                             const char *end_key) {
  uint64 num_extents = 0;
  mini_allocator_for_each(cc, data_cfg, type, meta_head,
                          mini_allocator_count_extent, start_key, end_key,
                          &num_extents);
  return num_extents;
}

bool mini_allocator_prefetch_extent(cache *cc, page_type type, uint64 base_addr,
                                    uint64 *pages_outstanding) {
  cache_prefetch(cc, base_addr, TRUE);
  return FALSE;
}

void mini_allocator_prefetch(cache *cc, page_type type, uint64 meta_head) {
  mini_allocator_for_each(cc, NULL, type, meta_head,
                          mini_allocator_prefetch_extent, NULL, NULL, NULL);
}

page_handle *mini_allocator_blind_inc(cache *cc, uint64 meta_head) {
  return cache_get(cc, meta_head, TRUE, PAGE_TYPE_MISC);
  // uint64 next_meta_addr = meta_head;
  // do {
  //   page_handle *meta_page = cache_get(cc, next_meta_addr, TRUE,
  //   PAGE_TYPE_MISC); mini_allocator_meta_hdr *hdr = (mini_allocator_meta_hdr
  //   *)meta_page->data; allocator *al = cache_allocator(cc); for (uint64 i =
  //   0; i < hdr->pos; i++) {
  //      mini_allocator_meta_entry *entry = &hdr->entry[i];
  //      if (!entry->zapped) {
  //         allocator_inc_refcount(al, entry->extent_addr);
  //      }
  //   }
  //   next_meta_addr = hdr->next_meta_addr;
  //   cache_unget(cc, meta_page);
  //} while (next_meta_addr != 0);
}

void mini_allocator_blind_zap(cache *cc, page_type type,
                              page_handle *meta_page) {
  cache_unget(cc, meta_page);
  // uint64 next_meta_addr = meta_head;
  // bool fully_zapped = TRUE;
  // bool did_a_zap = FALSE;
  // do {
  //   bool locked = FALSE;
  //   page_handle *meta_page = cache_get(cc, next_meta_addr, TRUE,
  //   PAGE_TYPE_MISC);

  //   mini_allocator_meta_hdr *hdr = (mini_allocator_meta_hdr
  //   *)meta_page->data; for (uint64 i = 0; i < hdr->pos; i++) {
  //      mini_allocator_meta_entry *entry = &hdr->entry[i];
  //      if (!entry->zapped) {
  //         bool just_zapped = cache_dealloc(cc, entry->extent_addr, type);
  //         if (just_zapped) {
  //            if (!locked) {
  //               uint64 wait = 1;
  //               while (!cache_claim(cc, meta_page)) {
  //                  cache_unget(cc, meta_page);
  //                  meta_page = NULL;
  //                  platform_sleep(wait);
  //                  wait = wait > 1024 ? wait : 2 * wait;
  //                  meta_page = cache_get(cc, next_meta_addr, TRUE,
  //                  PAGE_TYPE_MISC);
  //               }
  //               cache_lock(cc, meta_page);
  //               locked = TRUE;
  //            }
  //            entry->zapped = TRUE;
  //            did_a_zap = TRUE;
  //         }
  //      }
  //      fully_zapped = fully_zapped && entry->zapped;
  //   }
  //   if (hdr->pos == 0 && next_meta_addr == meta_head) {
  //      fully_zapped = FALSE;
  //   }
  //   next_meta_addr = hdr->next_meta_addr;
  //   if (locked) {
  //      cache_mark_dirty(cc, meta_page);
  //      cache_unlock(cc, meta_page);
  //      cache_unclaim(cc, meta_page);
  //   }
  //   cache_unget(cc, meta_page);
  //} while (next_meta_addr != 0);
  // if (fully_zapped && did_a_zap) {
  //   //platform_log("fully zapped %lu\n", meta_head - 4096);
  //   uint64 next_meta_addr = meta_head;
  //   do {
  //      page_handle *meta_page = cache_get(cc, next_meta_addr, TRUE,
  //      PAGE_TYPE_MISC); mini_allocator_meta_hdr *hdr =
  //      (mini_allocator_meta_hdr *)meta_page->data; uint64 last_meta_addr =
  //      next_meta_addr; next_meta_addr = hdr->next_meta_addr; cache_unget(cc,
  //      meta_page); if (!mini_allocator_addrs_share_extent(cc, last_meta_addr,
  //      next_meta_addr)) {
  //         uint64 last_meta_base_addr =
  //            last_meta_addr / cache_extent_size(cc) * cache_extent_size(cc);
  //         cache_dealloc(cc, last_meta_base_addr, type);
  //      }
  //   } while (next_meta_addr != 0);
  //}
}
