// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * mini_allocator.c --
 *
 *     This file contains the implementation for an allocator which
 *     allocates individual pages from extents.
 */

#include "platform.h"

#include "allocator.h"
#include "cache.h"
#include "splinterdb/data.h"
#include "mini_allocator.h"
#include "util.h"

#include "poison.h"

// MINI_WAIT is a lock token used to lock a batch
#define MINI_WAIT 1
// MINI_NO_REFS is the ref count of an unkeyed mini allocator with no external
// refs
#define MINI_NO_REFS 2

/*
 *-----------------------------------------------------------------------------
 *
 * mini_meta_hdr --
 *
 *      The header of a meta_page in a mini_allocator. Keyed mini_allocators
 *      use entry_buffer and unkeyed ones use entry.
 *
 *-----------------------------------------------------------------------------
 */

typedef struct mini_meta_hdr {
   uint64 next_meta_addr;
   uint64 pos;
   char   entry_buffer[];
} mini_meta_hdr;

/*
 *-----------------------------------------------------------------------------
 *
 * mini_init_meta_page --
 *
 *      Initializes the header of the given meta_page.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

void
mini_init_meta_page(mini_allocator *mini, page_handle *meta_page)
{
   mini_meta_hdr *hdr  = (mini_meta_hdr *)meta_page->data;
   hdr->pos            = 0;
   hdr->next_meta_addr = 0;
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_full_[lock,unlock]_meta_[page,tail] --
 *
 *      Convenience functions to write lock/unlock the given meta_page or
 *      meta_tail.
 *
 * Results:
 *      lock: the page_handle of the locked page
 *      unlock: None.
 *
 * Side effects:
 *      Disk allocation, standard cache side effects.
 *
 *-----------------------------------------------------------------------------
 */

page_handle *
mini_full_lock_meta_page(mini_allocator *mini, uint64 meta_addr)
{
   page_handle *meta_page;
   uint64       wait = 1;
   while (1) {
      meta_page = cache_get(mini->cc, meta_addr, TRUE, mini->type);
      if (cache_claim(mini->cc, meta_page)) {
         break;
      }
      cache_unget(mini->cc, meta_page);
      platform_sleep(wait);
      wait = wait > 1024 ? wait : 2 * wait;
   }
   cache_lock(mini->cc, meta_page);
   return meta_page;
}

page_handle *
mini_full_lock_meta_tail(mini_allocator *mini)
{
   /*
    * This loop follows the standard idiom for obtaining a claim.  Note that
    * mini is shared, so the value of mini->meta_tail can change before we
    * obtain the lock, thus we must check after the get.
    */
   page_handle *meta_page;
   uint64       wait = 1;
   while (1) {
      uint64 meta_tail = mini->meta_tail;
      meta_page        = cache_get(mini->cc, meta_tail, TRUE, mini->type);
      if (meta_tail == mini->meta_tail && cache_claim(mini->cc, meta_page)) {
         break;
      }
      cache_unget(mini->cc, meta_page);
      platform_sleep(wait);
      wait = wait > 1024 ? wait : 2 * wait;
   }
   cache_lock(mini->cc, meta_page);

   return meta_page;
}

void
mini_full_unlock_meta_page(mini_allocator *mini, page_handle *meta_page)
{
   cache_mark_dirty(mini->cc, meta_page);
   cache_unlock(mini->cc, meta_page);
   cache_unclaim(mini->cc, meta_page);
   cache_unget(mini->cc, meta_page);
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_(un)get_(un)claim_meta_page --
 *
 *      Convenience functions to read lock and claim the given meta_page.
 *
 * Results:
 *      get_claim; the page_handle of the locked page
 *      unget_unclaim: None.
 *
 * Side effects:
 *      Disk allocation, standard cache side effects.
 *
 *-----------------------------------------------------------------------------
 */

page_handle *
mini_get_claim_meta_page(cache *cc, uint64 meta_addr, page_type type)
{
   page_handle *meta_page;
   uint64       wait = 1;
   while (1) {
      meta_page = cache_get(cc, meta_addr, TRUE, type);
      if (cache_claim(cc, meta_page)) {
         break;
      }
      cache_unget(cc, meta_page);
      platform_sleep(wait);
      wait = wait > 1024 ? wait : 2 * wait;
   }
   return meta_page;
}

void
mini_unget_unclaim_meta_page(cache *cc, page_handle *meta_page)
{
   cache_unclaim(cc, meta_page);
   cache_unget(cc, meta_page);
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_init --
 *
 *      Initialize a new mini allocator.
 *
 *      There are two types of mini allocator, keyed and unkeyed. A keyed
 *      allocator stores a key range for each extent and allows incrementing
 *      and decrementing key ranges. An unkeyed allocator has a single refcount
 *      for the whole allocator which is overloaded onto the meta_head
 *      disk-allocator ref count.
 *
 * Results:
 *      The 0th batch next address to be allocated.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

uint64
mini_init(mini_allocator *mini,
          cache *         cc,
          data_config *   cfg,
          uint64          meta_head,
          uint64          meta_tail,
          uint64          num_batches,
          page_type       type,
          bool            keyed)
{
   platform_assert(num_batches <= MINI_MAX_BATCHES);
   platform_assert(num_batches != 0);
   platform_assert(mini != NULL);
   platform_assert(cc != NULL);
   platform_assert(!keyed || cfg != NULL);

   ZERO_CONTENTS(mini);
   mini->cc          = cc;
   mini->al          = cache_allocator(cc);
   mini->data_cfg    = cfg;
   mini->keyed       = keyed;
   mini->meta_head   = meta_head;
   mini->num_batches = num_batches;
   mini->type        = type;

   page_handle *meta_page;
   if (meta_tail == 0) {
      // new mini allocator
      mini->meta_tail = meta_head;
      meta_page       = cache_alloc(cc, mini->meta_head, type);
      mini_init_meta_page(mini, meta_page);

      if (!keyed) {
         // meta_page gets an extra ref
         uint64 base_addr = cache_base_addr(cc, mini->meta_head);
         uint8  ref       = allocator_inc_refcount(mini->al, base_addr);
         platform_assert(ref == MINI_NO_REFS + 1);
      }

      mini_full_unlock_meta_page(mini, meta_page);
   } else {
      // load mini allocator
      mini->meta_tail = meta_tail;
   }

   for (uint64 batch = 0; batch < num_batches; batch++) {
      // because we recover ref counts from the mini allocators on recovery, we
      // don't need to store these in the mini allocator until we consume them.
      platform_status rc =
         allocator_alloc_extent(mini->al, &mini->next_extent[batch]);
      platform_assert_status_ok(rc);
   }

   return mini->next_extent[0];
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_meta_page_is_full --
 *
 * Results:
 *      TRUE is meta_page is full, FALSE otherwise
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

uint64
mini_num_entries(page_handle *meta_page)
{
   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;
   return hdr->pos;
}

uint64
mini_unkeyed_entries_per_page(cache *cc)
{
   return (cache_page_size(cc) - sizeof(mini_meta_hdr)) / sizeof(uint64);
}

uint64
mini_keyed_entry_size(data_config *cfg)
{
   return sizeof(uint64) + 2 * cfg->key_size;
}

uint64
mini_keyed_entries_per_page(cache *cc, data_config *cfg)
{
   return (cache_page_size(cc) - sizeof(mini_meta_hdr)) /
          mini_keyed_entry_size(cfg);
}

bool
mini_meta_page_is_full(mini_allocator *mini, page_handle *meta_page)
{
   if (mini->keyed) {
      debug_assert(mini_num_entries(meta_page) <=
                   mini_keyed_entries_per_page(mini->cc, mini->data_cfg));
      return mini_num_entries(meta_page) ==
             mini_keyed_entries_per_page(mini->cc, mini->data_cfg);
   }
   // unkeyed
   debug_assert(mini_num_entries(meta_page) <=
                mini_unkeyed_entries_per_page(mini->cc));
   return mini_num_entries(meta_page) ==
          mini_unkeyed_entries_per_page(mini->cc);
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_keyed_[get,set]_entry --
 * mini_keyed_set_last_end_key --
 * mini_unkeyed_[get,set]_entry --
 *
 *      Allocator functions for adding new extents to the meta_page or getting
 *      the metadata of the pos-th extent in the given meta_page.
 *
 *      For keyed allocators, when setting an entry, only the start key is
 *      known. When a new extent is allocated, its start key becomes the
 *      previous extent's end_key (within a batch). This is set by calling
 *      mini_keyed_set_last_end_key.
 *
 *      Unkeyed allocators simply add/fetch the extent_addr as an entry by
 *      itself.
 *
 * Results:
 *      get: the extent_addr, start_key and end_key of the entry
 *      set: None.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

void
mini_keyed_get_entry(cache *      cc,
                     data_config *cfg,
                     page_handle *meta_page,
                     uint64       pos,
                     uint64 *     extent_addr,
                     const char **start_key,
                     const char **end_key)
{
   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;

   uint64 entry_size   = mini_keyed_entry_size(cfg);
   char * entry_cursor = hdr->entry_buffer + pos * entry_size;

   *extent_addr = *(uint64 *)entry_cursor;
   *start_key   = entry_cursor + sizeof(uint64);
   *end_key     = *start_key + cfg->key_size;
}

void
mini_keyed_set_entry(mini_allocator *mini,
                     uint64          batch,
                     page_handle *   meta_page,
                     uint64          extent_addr,
                     const char *    key)
{
   debug_assert(mini->keyed);
   debug_assert(batch < mini->num_batches);
   debug_assert(key != NULL);
   debug_assert(extent_addr != 0);
   debug_assert(extent_addr % cache_page_size(mini->cc) == 0);

   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;

   uint64 pos = hdr->pos++;
   char * entry_cursor =
      hdr->entry_buffer + pos * mini_keyed_entry_size(mini->data_cfg);
   uint64 *entry_extent_addr = (uint64 *)entry_cursor;
   char *  entry_start_key   = entry_cursor + sizeof(uint64);

   *entry_extent_addr = extent_addr;
   data_key_copy(mini->data_cfg, entry_start_key, key);

   // set last_meta_[addr,pos]
   mini->last_meta_addr[batch] = meta_page->disk_addr;
   mini->last_meta_pos[batch]  = pos;
}

void
mini_keyed_set_last_end_key(mini_allocator *mini,
                            uint64          batch,
                            page_handle *   meta_page,
                            const char *    key)
{
   debug_assert(mini->keyed);
   debug_assert(batch < mini->num_batches);
   debug_assert(key != NULL);

   if (mini->last_meta_addr[batch] == 0) {
      return;
   }

   page_handle *last_meta_page = NULL;
   bool         need_unlock;
   if (meta_page != NULL &&
       mini->last_meta_addr[batch] == meta_page->disk_addr) {
      last_meta_page = meta_page;
      need_unlock    = FALSE;
   } else {
      last_meta_page =
         mini_full_lock_meta_page(mini, mini->last_meta_addr[batch]);
      need_unlock = TRUE;
   }
   mini_meta_hdr *last_hdr = (mini_meta_hdr *)last_meta_page->data;

   uint64 pos = mini->last_meta_pos[batch];
   debug_assert(pos < last_hdr->pos);
   char *entry_cursor =
      last_hdr->entry_buffer + pos * mini_keyed_entry_size(mini->data_cfg);
   char *entry_end_key =
      entry_cursor + sizeof(uint64) + mini->data_cfg->key_size;
   data_key_copy(mini->data_cfg, entry_end_key, key);

   if (need_unlock) {
      mini_full_unlock_meta_page(mini, last_meta_page);
   }
}

void
mini_unkeyed_set_entry(mini_allocator *mini,
                       page_handle *   meta_page,
                       uint64          extent_addr)
{
   debug_assert(!mini->keyed);

   mini_meta_hdr *hdr   = (mini_meta_hdr *)meta_page->data;
   uint64         pos   = hdr->pos++;
   uint64 *       entry = (uint64 *)hdr->entry_buffer;
   entry[pos]           = extent_addr;
}

uint64
mini_unkeyed_get_entry(cache *cc, page_handle *meta_page, uint64 pos)
{
   debug_assert(pos < mini_unkeyed_entries_per_page(cc));

   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;
   debug_assert(pos < hdr->pos);
   uint64 *entry = (uint64 *)hdr->entry_buffer;
   return entry[pos];
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_release --
 *
 *      Called to finalize the mini_allocator. After calling, no more
 *      allocations can be made, but the mini_allocator linked list containing
 *      the extents allocated and their metadata can be accessed by functions
 *      using its meta_head.
 *
 *      Keyed allocators use this to set the final end keys of the batches.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Disk deallocation, standard cache side effects.
 *
 *-----------------------------------------------------------------------------
 */

void
mini_release(mini_allocator *mini, const char *key)
{
   debug_assert(!mini->keyed || key != NULL);

   for (uint64 batch = 0; batch < mini->num_batches; batch++) {
      // Dealloc the next extent
      uint8 ref = allocator_dec_refcount(mini->al, mini->next_extent[batch]);
      platform_assert(ref == AL_NO_REFS);
      ref = allocator_dec_refcount(mini->al, mini->next_extent[batch]);
      platform_assert(ref == AL_FREE);

      if (mini->keyed) {
         // Set the end_key of the last extent from this batch
         mini_keyed_set_last_end_key(mini, batch, NULL, key);
      }
   }
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_[lock,unlock]_batch_[get,set]next_addr --
 *
 *      Lock locks allocation on the given batch by replacing its next_addr
 *      with a lock token.
 *
 *      Unlock unlocks allocation on the given batch by replacing the lock
 *      token with the next free disk address to allocate.
 *
 * Results:
 *      Lock: the next disk address to allocate
 *      Unlock: None.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

uint64
mini_lock_batch_get_next_addr(mini_allocator *mini, uint64 batch)
{
   uint64 next_addr = mini->next_addr[batch];
   uint64 wait      = 1;
   while (next_addr == MINI_WAIT ||
          !__sync_bool_compare_and_swap(
             &mini->next_addr[batch], next_addr, MINI_WAIT)) {
      platform_sleep(wait);
      wait      = wait > 1024 ? wait : 2 * wait;
      next_addr = mini->next_addr[batch];
   }
   return next_addr;
}

void
mini_unlock_batch_set_next_addr(mini_allocator *mini,
                                uint64          batch,
                                uint64          next_addr)
{
   debug_assert(batch < mini->num_batches);
   debug_assert(mini->next_addr[batch] == MINI_WAIT);

   mini->next_addr[batch] = next_addr;
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_[get,set]_next_meta_addr --
 *
 *      Sets the next_meta_addr on meta_page to next_meta_addr. This links
 *      next_meta_addr in the linked list where meta_page is the current
 *      meta_tail.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

uint64
mini_get_next_meta_addr(page_handle *meta_page)
{
   // works for keyed and unkeyed
   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;
   return hdr->next_meta_addr;
}

void
mini_set_next_meta_addr(mini_allocator *mini,
                        page_handle *   meta_page,
                        uint64          next_meta_addr)
{
   // works for keyed and unkeyed
   mini_meta_hdr *hdr  = (mini_meta_hdr *)meta_page->data;
   hdr->next_meta_addr = next_meta_addr;
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_alloc --
 *
 *      Allocate a next disk address from the mini_allocator.
 *
 *      If the allocator is keyed, then the extent from which the allocation is
 *      made will include the given key.
 *      NOTE: This requires keys provided be monotonically increasing.
 *
 *      If next_extent is not NULL, then the successor extent to the allocated
 *      addr will be copied to it.
 *
 * Results:
 *      A newly allocated disk address.
 *
 * Side effects:
 *      Disk allocation, standard cache side effects.
 *
 *-----------------------------------------------------------------------------
 */

uint64
mini_alloc(mini_allocator *mini,
           uint64          batch,
           const char *    key,
           uint64 *        next_extent)
{
   debug_assert(batch < mini->num_batches);
   debug_assert(!mini->keyed || key != NULL);

   uint64 next_addr = mini_lock_batch_get_next_addr(mini, batch);

   if (next_addr % cache_extent_size(mini->cc) == 0) {
      // need to allocate the next extent

      uint64          extent_addr = mini->next_extent[batch];
      platform_status rc =
         allocator_alloc_extent(mini->al, &mini->next_extent[batch]);
      platform_assert_status_ok(rc);
      next_addr = extent_addr;

      page_handle *meta_page = mini_full_lock_meta_tail(mini);
      if (mini_meta_page_is_full(mini, meta_page)) {
         // need to allocate a new meta page
         uint64 new_meta_tail = mini->meta_tail + cache_page_size(mini->cc);
         if (new_meta_tail % cache_extent_size(mini->cc) == 0) {
            // need to allocate the next meta extent
            rc = allocator_alloc_extent(mini->al, &new_meta_tail);
            platform_assert_status_ok(rc);
         }

         mini_set_next_meta_addr(mini, meta_page, new_meta_tail);

         page_handle *last_meta_page = meta_page;
         meta_page       = cache_alloc(mini->cc, new_meta_tail, mini->type);
         mini->meta_tail = new_meta_tail;
         mini_full_unlock_meta_page(mini, last_meta_page);
         mini_init_meta_page(mini, meta_page);
      }
      if (mini->keyed) {
         mini_keyed_set_last_end_key(mini, batch, meta_page, key);
         mini_keyed_set_entry(mini, batch, meta_page, next_addr, key);
      } else {
         // unkeyed
         mini_unkeyed_set_entry(mini, meta_page, next_addr);
      }

      mini_full_unlock_meta_page(mini, meta_page);
   }

   if (next_extent) {
      *next_extent = mini->next_extent[batch];
   }

   uint64 new_next_addr = next_addr + cache_page_size(mini->cc);
   mini_unlock_batch_set_next_addr(mini, batch, new_next_addr);
   return next_addr;
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_[keyed,unkeyed]_for_each(_self_exclusive) --
 *
 *      Calls func on each extent_addr in the mini_allocator.
 *
 *      If the allocator is keyed and a single key or key range is given, calls
 *      it only on the extent_addrs with intersecting key ranges.
 *
 *      The self-exclusive version does hand-over-hand locking with claims to
 *      prevent races among callers. This is used for mini_keyed_dec_ref so
 *      that an order is enforced and the last caller can deinit the
 *      meta_pages.
 *
 *      NOTE: Should not be called if there are no intersecting ranges.
 *
 * Results:
 *      unkeyed: None
 *      keyed: TRUE if every call to func returns true, FALSE otherwise.
 *
 * Side effects:
 *      func may store output in out.
 *
 *-----------------------------------------------------------------------------
 */


typedef bool (*mini_for_each_fn)(cache *   cc,
                                 page_type type,
                                 uint64    base_addr,
                                 void *    out);

void
mini_unkeyed_for_each(cache *          cc,
                      uint64           meta_head,
                      page_type        type,
                      mini_for_each_fn func,
                      void *           out)
{
   uint64 meta_addr = meta_head;
   do {
      page_handle *meta_page = cache_get(cc, meta_addr, TRUE, type);

      uint64 num_meta_entries = mini_num_entries(meta_page);
      for (uint64 i = 0; i < num_meta_entries; i++) {
         uint64 extent_addr = mini_unkeyed_get_entry(cc, meta_page, i);
         func(cc, type, extent_addr, out);
      }
      meta_addr = mini_get_next_meta_addr(meta_page);
      cache_unget(cc, meta_page);
   } while (meta_addr != 0);
}

bool
mini_keyed_extent_in_range(data_config *cfg,
                           const char * entry_start_key,
                           const char * entry_end_key,
                           const char * start_key,
                           const char * end_key)
{
   /*
    * extent is in range if
    * 1. full range (start_key == NULL and end_key == NULL)
    * 2. extent in range (start_key_[1,2] < end_key_[2,1])
    * 3. range is a point (end_key == NULL) and point is in extent
    */
   if (start_key == NULL && end_key == NULL) {
      // case 1
      return TRUE;
   }
   if (end_key == NULL) {
      // case 3
      return data_key_compare(cfg, start_key, entry_end_key) <= 0 &&
             data_key_compare(cfg, entry_start_key, start_key) <= 0;
   } else {
      // case 2
      return data_key_compare(cfg, start_key, entry_end_key) <= 0 &&
             data_key_compare(cfg, entry_start_key, end_key) <= 0;
   }
   platform_assert(0);
}

bool
mini_keyed_for_each(cache *          cc,
                    data_config *    cfg,
                    uint64           meta_head,
                    page_type        type,
                    const char *     start_key,
                    const char *     end_key,
                    mini_for_each_fn func,
                    void *           out)
{
   // We return true for cleanup if every call to func returns TRUE.
   bool should_cleanup = TRUE;
   // Should not be called if there are no intersecting ranges, we track with
   // did_work.
   debug_only bool did_work = FALSE;

   uint64 meta_addr = meta_head;

   do {
      page_handle *meta_page = cache_get(cc, meta_addr, TRUE, type);
      for (uint64 i = 0; i < mini_num_entries(meta_page); i++) {
         uint64      extent_addr;
         const char *entry_start_key, *entry_end_key;
         mini_keyed_get_entry(cc,
                              cfg,
                              meta_page,
                              i,
                              &extent_addr,
                              &entry_start_key,
                              &entry_end_key);
         if (mini_keyed_extent_in_range(
                cfg, entry_start_key, entry_end_key, start_key, end_key)) {
            debug_code(did_work = TRUE);
            bool entry_should_cleanup = func(cc, type, extent_addr, out);
            should_cleanup            = should_cleanup && entry_should_cleanup;
         }
      }

      meta_addr = mini_get_next_meta_addr(meta_page);
      cache_unget(cc, meta_page);
   } while (meta_addr != 0);


   debug_assert(did_work);
   return should_cleanup;
}

bool
mini_keyed_for_each_self_exclusive(cache *          cc,
                                   data_config *    cfg,
                                   uint64           meta_head,
                                   page_type        type,
                                   const char *     start_key,
                                   const char *     end_key,
                                   mini_for_each_fn func,
                                   void *           out)
{
   // We return true for cleanup if every call to func returns TRUE.
   bool should_cleanup = TRUE;
   // Should not be called if there are no intersecting ranges, we track with
   // did_work.
   debug_only bool did_work = FALSE;

   uint64       meta_addr = meta_head;
   page_handle *meta_page = mini_get_claim_meta_page(cc, meta_head, type);

   do {
      for (uint64 i = 0; i < mini_num_entries(meta_page); i++) {
         uint64      extent_addr;
         const char *entry_start_key, *entry_end_key;
         mini_keyed_get_entry(cc,
                              cfg,
                              meta_page,
                              i,
                              &extent_addr,
                              &entry_start_key,
                              &entry_end_key);
         if (mini_keyed_extent_in_range(
                cfg, entry_start_key, entry_end_key, start_key, end_key)) {
            debug_code(did_work = TRUE);
            bool entry_should_cleanup = func(cc, type, extent_addr, out);
            should_cleanup            = should_cleanup && entry_should_cleanup;
         }
      }

      meta_addr = mini_get_next_meta_addr(meta_page);
      if (meta_addr != 0) {
         page_handle *next_meta_page =
            mini_get_claim_meta_page(cc, meta_addr, type);
         mini_unget_unclaim_meta_page(cc, meta_page);
         meta_page = next_meta_page;
      }
   } while (meta_addr != 0);

   mini_unget_unclaim_meta_page(cc, meta_page);

   debug_assert(did_work);
   return should_cleanup;
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_unkeyed_[inc,dec]_ref --
 *
 *      Increments or decrements the ref count of the unkeyed allocator. When
 *      the external ref count reaches 0 (actual ref count reachs
 *      MINI_NO_REFS), the mini allocator is destroyed.
 *
 * Results:
 *      Prior external ref count (internal ref count - MINI_NO_REFS)
 *
 * Side effects:
 *      Deallocation/cache side effects when external ref count hits 0
 *
 *-----------------------------------------------------------------------------
 */


uint8
mini_unkeyed_inc_ref(cache *cc, uint64 meta_head)
{
   allocator *al        = cache_allocator(cc);
   uint64     base_addr = cache_base_addr(cc, meta_head);
   uint8      ref       = allocator_inc_refcount(al, base_addr);
   platform_assert(ref > MINI_NO_REFS);
   return ref - MINI_NO_REFS;
}

static inline bool
mini_addrs_share_extent(cache *cc, uint64 left_addr, uint64 right_addr)
{
   uint64 extent_size = cache_extent_size(cc);
   return right_addr / extent_size == left_addr / extent_size;
}

void
mini_deinit(cache *cc, uint64 meta_head, page_type type)
{
   allocator *al        = cache_allocator(cc);
   uint64     meta_addr = meta_head;
   do {
      page_handle *meta_page      = cache_get(cc, meta_addr, TRUE, type);
      uint64       last_meta_addr = meta_addr;
      meta_addr                   = mini_get_next_meta_addr(meta_page);
      cache_unget(cc, meta_page);

      if (!mini_addrs_share_extent(cc, last_meta_addr, meta_addr)) {
         uint64 last_meta_base_addr = cache_base_addr(cc, last_meta_addr);
         uint8  ref = allocator_dec_refcount(al, last_meta_base_addr);
         platform_assert(ref == AL_NO_REFS);
         cache_hard_evict_extent(cc, last_meta_base_addr, type);
         ref = allocator_dec_refcount(al, last_meta_base_addr);
         platform_assert(ref == AL_FREE);
      }
   } while (meta_addr != 0);
}

bool
mini_dealloc_extent(cache *cc, page_type type, uint64 base_addr, void *out)
{
   allocator *al  = cache_allocator(cc);
   uint8      ref = allocator_dec_refcount(al, base_addr);
   platform_assert(ref == AL_NO_REFS);
   cache_hard_evict_extent(cc, base_addr, type);
   ref = allocator_dec_refcount(al, base_addr);
   platform_assert(ref == AL_FREE);
   return TRUE;
}

uint8
mini_unkeyed_dec_ref(cache *cc, uint64 meta_head, page_type type)
{
   allocator *al        = cache_allocator(cc);
   uint64     base_addr = cache_base_addr(cc, meta_head);
   uint8      ref       = allocator_dec_refcount(al, base_addr);
   if (ref != MINI_NO_REFS) {
      debug_assert(ref != AL_NO_REFS);
      debug_assert(ref != AL_FREE);
      return ref - MINI_NO_REFS;
   }

   // need to deallocate and clean up the mini allocator
   mini_unkeyed_for_each(cc, meta_head, type, mini_dealloc_extent, NULL);
   mini_deinit(cc, meta_head, type);
   return 0;
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_keyed_[inc,dec]_ref --
 *
 *      In keyed mini allocators, ref counts are kept on a per-extent basis,
 *      and ref count increments and decrements are performed on key ranges.
 *
 *      See mini_keyed_for_each for key range intersection rules.
 *
 *      In SplinterDB, keyed mini allocators are used for branches, which have
 *      at least one extent (the extent containing the root) whose key range
 *      covers the key range of the branch itself (and therefore the mini
 *      allocator). Therefore, a dec_ref which deallocates every extent it
 *      intersects must have deallocated this extent as well, and therefore
 *      there are no refs in the allocator and it can be cleaned up.
 *
 * Results:
 *      None
 *
 * Side effects:
 *      Deallocation/cache side effects.
 *
 *-----------------------------------------------------------------------------
 */

bool
mini_keyed_inc_ref_extent(cache *   cc,
                          page_type type,
                          uint64    base_addr,
                          void *    out)
{
   allocator *al = cache_allocator(cc);
   allocator_inc_refcount(al, base_addr);
   return FALSE;
}

void
mini_keyed_inc_ref(cache *      cc,
                   data_config *data_cfg,
                   page_type    type,
                   uint64       meta_head,
                   const char * start_key,
                   const char * end_key)
{
   mini_keyed_for_each(cc,
                       data_cfg,
                       meta_head,
                       type,
                       start_key,
                       end_key,
                       mini_keyed_inc_ref_extent,
                       NULL);
}

bool
mini_keyed_dec_ref_extent(cache *   cc,
                          page_type type,
                          uint64    base_addr,
                          void *    out)
{
   allocator *al  = cache_allocator(cc);
   uint8      ref = allocator_dec_refcount(al, base_addr);
   if (ref == AL_NO_REFS) {
      cache_hard_evict_extent(cc, base_addr, type);
      ref = allocator_dec_refcount(al, base_addr);
      platform_assert(ref == AL_FREE);
      return TRUE;
   }
   return FALSE;
}

void
mini_keyed_dec_ref(cache *      cc,
                   data_config *data_cfg,
                   page_type    type,
                   uint64       meta_head,
                   const char * start_key,
                   const char * end_key)
{
   bool should_cleanup =
      mini_keyed_for_each_self_exclusive(cc,
                                         data_cfg,
                                         meta_head,
                                         type,
                                         start_key,
                                         end_key,
                                         mini_keyed_dec_ref_extent,
                                         NULL);
   if (should_cleanup) {
      mini_deinit(cc, meta_head, type);
   }
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_keyed_count_extents --
 *
 *      Returns the number of extents in the mini allocator intersecting the
 *      given key range (see mini_keyed_for_each for intersection rules).
 *
 * Results:
 *      The extent count.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

bool
mini_keyed_count_extents(cache *cc, page_type type, uint64 base_addr, void *out)
{
   uint64 *count = (uint64 *)out;
   (*count)++;
   return FALSE;
}

uint64
mini_keyed_extent_count(cache *      cc,
                        data_config *data_cfg,
                        page_type    type,
                        uint64       meta_head,
                        const char * start_key,
                        const char * end_key)
{
   uint64 count = 0;
   mini_keyed_for_each(cc,
                       data_cfg,
                       meta_head,
                       type,
                       start_key,
                       end_key,
                       mini_keyed_count_extents,
                       &count);
   return count;
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_unkeyed_prefetch --
 *
 *      Prefetches all extents in the (unkeyed) mini allocator.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Standard cache side effects.
 *
 *-----------------------------------------------------------------------------
 */

bool
mini_prefetch_extent(cache *cc, page_type type, uint64 base_addr, void *out)
{
   cache_prefetch(cc, base_addr, type);
   return FALSE;
}

void
mini_unkeyed_prefetch(cache *cc, page_type type, uint64 meta_head)
{
   mini_unkeyed_for_each(cc, meta_head, type, mini_prefetch_extent, NULL);
}

/*
 *-----------------------------------------------------------------------------
 *
 * mini_[keyed,unkeyed]_print --
 *
 *      Prints each meta_page together with all its entries to
 *      PLATFORM_DEFAULT_LOG.
 *
 *      Keyed allocators print each extent addr together with start and end
 *      keys, unkeyed allocators only print the extent addr.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

void
mini_unkeyed_print(cache *cc, uint64 meta_head, page_type type)
{
   uint64 next_meta_addr = meta_head;

   platform_log("---------------------------------------------\n");
   platform_log("| Mini Allocator -- meta_head: %12lu |\n", meta_head);
   platform_log("|-------------------------------------------|\n");
   platform_log("| idx | %35s |\n", "extent_addr");
   platform_log("|-------------------------------------------|\n");

   do {
      page_handle *meta_page = cache_get(cc, next_meta_addr, TRUE, type);

      platform_log("| meta addr %31lu |\n", next_meta_addr);
      platform_log("|-------------------------------------------|\n");

      uint64 num_entries = mini_num_entries(meta_page);
      for (uint64 i = 0; i < num_entries; i++) {
         uint64 extent_addr = mini_unkeyed_get_entry(cc, meta_page, i);
         platform_log("| %3lu | %35lu |\n", i, extent_addr);
      }
      platform_log("|-------------------------------------------|\n");

      next_meta_addr = mini_get_next_meta_addr(meta_page);
      cache_unget(cc, meta_page);
   } while (next_meta_addr != 0);
   platform_log("\n");
}

void
mini_keyed_print(cache *      cc,
                 data_config *data_cfg,
                 uint64       meta_head,
                 page_type    type)
{
   uint64 next_meta_addr = meta_head;

   platform_log(
      "----------------------------------------------------------------\n");
   platform_log("| Mini Keyed Allocator -- meta_head: %12lu              |\n",
                meta_head);
   platform_log(
      "|--------------------------------------------------------------|\n");
   platform_log(
      "| idx | %12s | %18s | %18s |\n", "extent_addr", "start_key", "end_key");
   platform_log(
      "|--------------------------------------------------------------|\n");

   do {
      page_handle *meta_page = cache_get(cc, next_meta_addr, TRUE, type);

      platform_log(
         "| meta addr: %12lu                                      |\n",
         next_meta_addr);
      platform_log(
         "|--------------------------------------------------------------|\n");

      uint64 num_entries = mini_num_entries(meta_page);
      for (uint64 i = 0; i < num_entries; i++) {
         const char *start_key, *end_key;
         uint64      extent_addr;
         mini_keyed_get_entry(
            cc, data_cfg, meta_page, i, &extent_addr, &start_key, &end_key);
         char start_key_str[MAX_KEY_STR_LEN];
         data_key_to_string(
            data_cfg, start_key, start_key_str, MAX_KEY_STR_LEN);
         char end_key_str[MAX_KEY_STR_LEN];
         data_key_to_string(data_cfg, end_key, end_key_str, MAX_KEY_STR_LEN);
         platform_log("| %3lu | %12lu | %18s | %18s |\n",
                      i,
                      extent_addr,
                      start_key_str,
                      end_key_str);
      }
      platform_log(
         "|--------------------------------------------------------------|\n");

      next_meta_addr = mini_get_next_meta_addr(meta_page);
      cache_unget(cc, meta_page);
   } while (next_meta_addr != 0);
   platform_log("\n");
}
