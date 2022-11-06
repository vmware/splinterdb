// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *-----------------------------------------------------------------------------
 * mini_allocator.c --
 *
 *     This file contains the implementation for an allocator which
 *     allocates individual pages from extents.
 *-----------------------------------------------------------------------------
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
 * mini_meta_hdr -- Disk-resident structure
 *
 *      The header of a meta_page in a mini_allocator. Keyed mini_allocators
 *      use entry_buffer and unkeyed ones use entry.
 *-----------------------------------------------------------------------------
 */
typedef struct ONDISK mini_meta_hdr {
   uint64    next_meta_addr;
   page_type type;
   uint64 pos;
   uint32 num_entries;
   char   entry_buffer[];
} mini_meta_hdr;

#define TERMINAL_EXTENT_ADDR ((uint64)-1)

/*
 *-----------------------------------------------------------------------------
 * keyed_meta_entry -- Disk-resident structure
 *
 *      Metadata for each extent stored in the extent list for a keyed
 *      mini_allocator. The key range for each extent goes from start_key to
 *      the start_key of its successor (the next keyed_meta_entry from the same
        batch).
 *-----------------------------------------------------------------------------
 */
typedef struct ONDISK keyed_meta_entry {
   uint64    extent_addr;
   uint16    start_key_length;
   uint8     batch;
   page_type type;
   char      start_key[];
} keyed_meta_entry;

/*
 *-----------------------------------------------------------------------------
 * unkeyed_meta_entry -- Disk-resident structure
 *
 *      Metadata for each extent stored in the extent list for an unkeyed
 *      mini_allocator. Currently, this is just the extent address itself.
 *-----------------------------------------------------------------------------
 */
typedef struct ONDISK unkeyed_meta_entry {
   uint64    extent_addr;
   page_type type;
} unkeyed_meta_entry;

static uint64
sizeof_keyed_meta_entry(const keyed_meta_entry *entry)
{
   return sizeof(keyed_meta_entry) + entry->start_key_length;
}

static uint64
keyed_meta_entry_size(slice key)
{
   return sizeof(keyed_meta_entry) + slice_length(key);
}

static slice
keyed_meta_entry_start_key(keyed_meta_entry *entry)
{
   return slice_create(entry->start_key_length, entry->start_key);
}

static keyed_meta_entry *
keyed_first_entry(page_handle *meta_page)
{
   return (keyed_meta_entry *)((mini_meta_hdr *)meta_page->data)->entry_buffer;
}

static keyed_meta_entry *
keyed_next_entry(keyed_meta_entry *entry)
{
   return (keyed_meta_entry *)((char *)entry + sizeof_keyed_meta_entry(entry));
}

static unkeyed_meta_entry *
unkeyed_first_entry(page_handle *meta_page)
{
   return (unkeyed_meta_entry *)((mini_meta_hdr *)meta_page->data)
      ->entry_buffer;
}

static unkeyed_meta_entry *
unkeyed_next_entry(unkeyed_meta_entry *entry)
{
   return entry + 1;
}

/*
 *-----------------------------------------------------------------------------
 * mini_init_meta_page --
 *
 *      Initializes the header of the given meta_page.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static void
mini_init_meta_page(mini_allocator *mini, page_handle *meta_page)
{
   mini_meta_hdr *hdr  = (mini_meta_hdr *)meta_page->data;
   hdr->next_meta_addr = 0;
   hdr->type           = mini->meta_type;
   hdr->pos            = offsetof(typeof(*hdr), entry_buffer);
   hdr->num_entries    = 0;
}

/*
 *-----------------------------------------------------------------------------
 * mini_full_[lock,unlock]_meta_tail --
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
 *-----------------------------------------------------------------------------
 */
static page_handle *
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
      meta_page        = cache_get(mini->cc, meta_tail, TRUE, mini->meta_type);
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

static void
mini_full_unlock_meta_page(mini_allocator *mini, page_handle *meta_page)
{
   cache_mark_dirty(mini->cc, meta_page);
   cache_unlock(mini->cc, meta_page);
   cache_unclaim(mini->cc, meta_page);
   cache_unget(mini->cc, meta_page);
}

/*
 *-----------------------------------------------------------------------------
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
 *-----------------------------------------------------------------------------
 */
static page_handle *
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

static void
mini_unget_unclaim_meta_page(cache *cc, page_handle *meta_page)
{
   cache_unclaim(cc, meta_page);
   cache_unget(cc, meta_page);
}

/*
 * Allocate a new extent from the underlying extent allocator and
 * update our bookkeeping.
 */
static platform_status
mini_allocator_get_new_extent(mini_allocator *mini,
                              page_type       type,
                              uint64         *addr)
{
   platform_status rc = allocator_alloc(mini->al, addr, type);
   if (SUCCESS(rc)) {
      __sync_fetch_and_add(&mini->num_extents, 1);
   }
   return rc;
}

/*
 *-----------------------------------------------------------------------------
 * mini_init --
 *
 *      Initialize a new mini allocator.
 *
 *      There are two types of mini allocator: keyed and unkeyed.
 *
 *      - A keyed allocator stores a key range for each extent and allows
 *        incrementing and decrementing key ranges.
 *
 *      - An unkeyed allocator has a single ref for the whole allocator which
 *        is overloaded onto the meta_head disk-allocator ref count.
 *
 * Results:
 *      platform_status
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
platform_status
mini_init(mini_allocator *mini,
          cache          *cc,
          data_config    *cfg,
          uint64          meta_head,
          uint64          meta_tail,
          uint64          num_batches,
          page_type       meta_type,
          const page_type types[],
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
   mini->num_batches = num_batches;
   mini->meta_type   = meta_type;
   memcpy(mini->types, types, num_batches * sizeof(*types));

   mini->num_extents = 1; // for the meta page
   mini->meta_head   = meta_head;

   page_handle *meta_page;
   if (meta_tail == 0) {
      // new mini allocator
      mini->meta_tail = meta_head;
      meta_page       = cache_alloc(cc, mini->meta_head, meta_type);
      mini_init_meta_page(mini, meta_page);

      if (!keyed) {
         // meta_page gets an extra ref
         uint64 base_addr = cache_extent_base_addr(cc, mini->meta_head);
         uint8  ref       = allocator_inc_ref(mini->al, base_addr);
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
      platform_status rc = mini_allocator_get_new_extent(
         mini, mini->types[batch], &mini->next_extent[batch]);
      platform_assert_status_ok(rc);
   }

   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 * mini_num_entries --
 *      Return the number of entries in the meta_page.
 *-----------------------------------------------------------------------------
 */
static uint64
mini_num_entries(page_handle *meta_page)
{
   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;
   return hdr->num_entries;
}

static page_type
mini_meta_type(page_handle *meta_page)
{
   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;
   return hdr->type;
}

/*
 *-----------------------------------------------------------------------------
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
 *-----------------------------------------------------------------------------
 */
static bool
entry_fits_in_page(uint64 page_size, uint64 start, uint64 entry_size)
{
   return start + entry_size <= page_size;
}

static bool
mini_keyed_append_entry(mini_allocator *mini,
                        uint64          batch,
                        page_handle    *meta_page,
                        uint64          extent_addr,
                        const slice     start_key,
                        page_type       type)
{
   debug_assert(mini->keyed);
   debug_assert(batch < mini->num_batches);
   debug_assert(!slice_is_null(start_key));
   debug_assert(extent_addr != 0);
   debug_assert(extent_addr == TERMINAL_EXTENT_ADDR
                || extent_addr % cache_page_size(mini->cc) == 0);

   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;

   if (!entry_fits_in_page(cache_page_size(mini->cc),
                           hdr->pos,
                           keyed_meta_entry_size(start_key)))
   {
      return FALSE;
   }

   keyed_meta_entry *new_entry = pointer_byte_offset(hdr, hdr->pos);

   new_entry->extent_addr = extent_addr;
   new_entry->batch       = batch;
   new_entry->type        = type;
   slice_copy_contents(new_entry->start_key, start_key);
   new_entry->start_key_length = slice_length(start_key);

   hdr->pos += keyed_meta_entry_size(start_key);
   hdr->num_entries++;
   return TRUE;
}

static bool
mini_unkeyed_append_entry(mini_allocator *mini,
                          page_handle    *meta_page,
                          uint64          extent_addr,
                          page_type       type)
{
   debug_assert(!mini->keyed);
   debug_assert(extent_addr != 0);
   debug_assert(extent_addr % cache_page_size(mini->cc) == 0);

   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;

   if (!entry_fits_in_page(
          cache_page_size(mini->cc), hdr->pos, sizeof(unkeyed_meta_entry)))
   {
      return FALSE;
   }

   unkeyed_meta_entry *new_entry = pointer_byte_offset(hdr, hdr->pos);
   new_entry->extent_addr        = extent_addr;
   new_entry->type               = type;

   hdr->pos += sizeof(unkeyed_meta_entry);
   hdr->num_entries++;
   return TRUE;
}

/*
 *-----------------------------------------------------------------------------
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
 *-----------------------------------------------------------------------------
 */
static uint64
mini_lock_batch_get_next_addr(mini_allocator *mini, uint64 batch)
{
   uint64 next_addr = mini->next_addr[batch];
   uint64 wait      = 1;
   while (next_addr == MINI_WAIT
          || !__sync_bool_compare_and_swap(
             &mini->next_addr[batch], next_addr, MINI_WAIT))
   {
      platform_sleep(wait);
      wait      = wait > 1024 ? wait : 2 * wait;
      next_addr = mini->next_addr[batch];
   }
   return next_addr;
}

static void
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
 *-----------------------------------------------------------------------------
 */
static uint64
mini_get_next_meta_addr(page_handle *meta_page)
{
   // works for keyed and unkeyed
   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;
   return hdr->next_meta_addr;
}

static void
mini_set_next_meta_addr(mini_allocator *mini,
                        page_handle    *meta_page,
                        uint64          next_meta_addr)
{
   // works for keyed and unkeyed
   mini_meta_hdr *hdr  = (mini_meta_hdr *)meta_page->data;
   hdr->next_meta_addr = next_meta_addr;
}

static bool
mini_append_entry(mini_allocator *mini,
                  uint64          batch,
                  const slice     key,
                  uint64          next_addr)
{
   page_handle *meta_page = mini_full_lock_meta_tail(mini);
   bool         success;
   if (mini->keyed) {
      success = mini_keyed_append_entry(
         mini, batch, meta_page, next_addr, key, mini->types[batch]);
   } else {
      // unkeyed
      success = mini_unkeyed_append_entry(
         mini, meta_page, next_addr, mini->types[batch]);
   }
   if (!success) {
      // need to allocate a new meta page
      uint64 new_meta_tail = mini->meta_tail + cache_page_size(mini->cc);
      if (new_meta_tail % cache_extent_size(mini->cc) == 0) {
         // need to allocate the next meta extent
         platform_status rc = mini_allocator_get_new_extent(
            mini, mini->meta_type, &new_meta_tail);
         platform_assert_status_ok(rc);
      }

      mini_set_next_meta_addr(mini, meta_page, new_meta_tail);

      page_handle *last_meta_page = meta_page;
      meta_page       = cache_alloc(mini->cc, new_meta_tail, mini->meta_type);
      mini->meta_tail = new_meta_tail;
      mini_full_unlock_meta_page(mini, last_meta_page);
      mini_init_meta_page(mini, meta_page);

      if (mini->keyed) {
         success = mini_keyed_append_entry(
            mini, batch, meta_page, next_addr, key, mini->types[batch]);
      } else {
         // unkeyed
         success = mini_unkeyed_append_entry(
            mini, meta_page, next_addr, mini->types[batch]);
      }

      debug_assert(success);
   }
   mini_full_unlock_meta_page(mini, meta_page);
   return TRUE;
}

/*
 *-----------------------------------------------------------------------------
 * mini_alloc_bytes --
 *
 *      Allocate num_bytes (<= extent_size) in at most two contiguous
 *      chunks at disk addresses of the given alignment.
 *      If boundary >= num_bytes, then the allocation
 *      will be in one chunk that does not cross an address that is a
 *      multiple of boundary.  Note that you must have
 *         - alignment divides extent_size
 *      and if num_bytes <= boundary, then you must have
 *         - alignment divides boundary
 *         - boundary divides extent_size
 *
 *      If the allocator is keyed, then the extent(s) from which the allocation
 *      is made will include the given key. NOTE: This requires keys provided be
 *      monotonically increasing.
 *
 *      The starting addresses of the chunks are returned in addrs[].
 *      If the bytes are allocated in 1 chunk, then addrs[1] == 0.
 *
 * Results:
 *      platform_status indicating success or error.
 *
 * Side effects:
 *      Disk allocation, standard cache side effects.
 *-----------------------------------------------------------------------------
 */
platform_status
mini_alloc_bytes(mini_allocator *mini,
                 uint64          batch,
                 uint64          num_bytes,
                 uint64          alignment,
                 uint64          boundary,
                 const slice     key,
                 uint64          addrs[2],
                 uint64         *txn_addr,
                 uint64         *next_extent)
{
   uint64 extent_size = cache_extent_size(mini->cc);
   if (alignment == 0) {
      alignment = 1;
   }
   debug_assert(batch < MINI_MAX_BATCHES);
   debug_assert(batch < mini->num_batches);
   debug_assert(num_bytes <= extent_size);
   debug_assert(boundary % alignment == 0);
   debug_assert(boundary == 0 || extent_size % boundary == 0);
   debug_assert(!mini->keyed || !slice_is_null(key));

   addrs[0] = 0;
   addrs[1] = 0;

   uint64 next_addr = mini_lock_batch_get_next_addr(mini, batch);

   /* Align next_addr */
   if (next_addr % alignment) {
      next_addr += alignment - (next_addr % alignment);
   }

   /* If next_addr would do a forbidden boundary crossing, advance
    * next_addr to the next boundary. Since alignment must divide boundary, this
    * will preserve any alignment constraints. */
   if (num_bytes <= boundary
       && next_addr / boundary != (next_addr + num_bytes - 1) / boundary)
   {
      next_addr += boundary - (next_addr % boundary);
   }

   int    num_allocs = 0;
   uint64 remainder  = num_bytes;
   while (remainder) {
      /* On the first iteration, we are guaranteed to satisfy
         alignment and boundary constraints (from above).
         Furthermore, if we now allocate a new extent, then next_addr
         will point to the beginning of an extent, which is also
         guaranteed to satisfy alignment and boundary constraints.  On
         a second iteration, we are guaranteed to allocate a new
         extent and hence the second iteration will satisfy alignment
         constraints.  And there cannot be any boundary constraint if
         we perform two iterations. */
      if (next_addr % extent_size == 0) {
         // need to allocate the next extent

         uint64          extent_addr = mini->next_extent[batch];
         platform_status rc          = mini_allocator_get_new_extent(
            mini, mini->types[batch], &mini->next_extent[batch]);
         platform_assert_status_ok(rc);
         next_addr = extent_addr;

         bool success = mini_append_entry(mini, batch, key, next_addr);
         platform_assert(success);
      }

      uint64 this_alloc_size =
         MIN(remainder, extent_size - (next_addr % extent_size));

      debug_assert(num_allocs < 2);
      addrs[num_allocs] = next_addr;
      remainder -= this_alloc_size;
      num_allocs++;
      next_addr += this_alloc_size;
   }

   if (next_extent) {
      *next_extent = mini->next_extent[batch];
   }

   *txn_addr = next_addr;
   return STATUS_OK;
}

void
mini_alloc_bytes_finish(mini_allocator *mini, uint64 batch, uint64 txn_addr)
{
   mini_unlock_batch_set_next_addr(mini, batch, txn_addr);
}

uint64
mini_alloc_page(mini_allocator *mini,
                uint64          batch,
                const slice     key,
                uint64         *next_extent)
{
   uint64          alloced_addrs[2] = {0, 0};
   uint64          txn_addr;
   uint64          page_size        = cache_page_size(mini->cc);
   platform_status rc               = mini_alloc_bytes(mini,
                                         batch,
                                         page_size,
                                         page_size,
                                         0,
                                         key,
                                         alloced_addrs,
                                         &txn_addr,
                                         next_extent);
   debug_assert(alloced_addrs[1] == 0);
   if (!SUCCESS(rc)) {
      return 0;
   }
   mini_alloc_bytes_finish(mini, batch, txn_addr);
   return alloced_addrs[0];
}

uint64
mini_alloc_extent(mini_allocator *mini,
                  uint64          batch,
                  const slice     key,
                  uint64         *next_extent)
{
   uint64          alloced_addrs[2] = {0, 0};
   uint64          txn_addr;
   uint64          extent_size      = cache_extent_size(mini->cc);
   platform_status rc               = mini_alloc_bytes(mini,
                                         batch,
                                         extent_size,
                                         extent_size,
                                         0,
                                         key,
                                         alloced_addrs,
                                         &txn_addr,
                                         next_extent);
   debug_assert(alloced_addrs[1] == 0);
   if (!SUCCESS(rc)) {
      return 0;
   }
   mini_alloc_bytes_finish(mini, batch, txn_addr);
   return alloced_addrs[0];
}

platform_status
mini_attach_extent(mini_allocator *mini, uint64 batch, slice key, uint64 addr)
{
   debug_assert(!mini->keyed || !slice_is_null(key));
   mini_lock_batch_get_next_addr(mini, batch);
   allocator_inc_ref(mini->al, addr);
   bool success = mini_append_entry(mini, batch, key, addr);
   platform_assert(success);
   mini_unlock_batch_set_next_addr(
      mini, batch, addr + cache_extent_size(mini->cc));
   return STATUS_OK;
}

uint64
mini_next_addr(mini_allocator *mini, uint64 batch)
{
   if (mini->next_addr[batch]) {
      return mini->next_addr[batch];
   } else {
      return mini->next_extent[batch];
   }
}

/*
 *-----------------------------------------------------------------------------
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
 *-----------------------------------------------------------------------------
 */
void
mini_release(mini_allocator *mini, const slice key)
{
   debug_assert(!mini->keyed || !slice_is_null(key));

   for (uint64 batch = 0; batch < mini->num_batches; batch++) {
      // Dealloc the next extent
      uint8 ref = allocator_dec_ref(
         mini->al, mini->next_extent[batch], mini->types[batch]);
      platform_assert(ref == AL_NO_REFS);
      ref = allocator_dec_ref(
         mini->al, mini->next_extent[batch], mini->types[batch]);
      platform_assert(ref == AL_FREE);

      if (mini->keyed) {
         // Set the end_key of the last extent from this batch
         mini_append_entry(mini, batch, key, TERMINAL_EXTENT_ADDR);
      }
   }
}


/*
 *-----------------------------------------------------------------------------
 * mini_deinit --
 *
 *      Cleanup function to deallocate the metadata extents of the mini
 *      allocator. Does not deallocate or otherwise access the data extents.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Disk deallocation, standard cache side effects.
 *-----------------------------------------------------------------------------
 */

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

      if (!cache_pages_share_extent(cc, last_meta_addr, meta_addr)) {
         uint64 last_meta_base_addr =
            cache_extent_base_addr(cc, last_meta_addr);
         uint8 ref = allocator_dec_ref(al, last_meta_base_addr, type);
         platform_assert(ref == AL_NO_REFS);
         cache_hard_evict_extent(cc, last_meta_base_addr, type);
         ref = allocator_dec_ref(al, last_meta_base_addr, type);
         platform_assert(ref == AL_FREE);
      }
   } while (meta_addr != 0);
}

/*
 *-----------------------------------------------------------------------------
 * mini_destroy_unused --
 *
 *      Called to destroy a mini_allocator that was created but never used to
 *      allocate an extent or to attach any extents. Can only be called on a
 *      keyed mini allocator.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Disk deallocation, standard cache side effects.
 *-----------------------------------------------------------------------------
 */

void
mini_destroy_unused(mini_allocator *mini)
{
   debug_assert(mini->keyed);
   debug_assert(mini->num_extents == mini->num_batches);

   for (uint64 batch = 0; batch < mini->num_batches; batch++) {
      // Dealloc the next extent
      uint8 ref = allocator_dec_ref(
         mini->al, mini->next_extent[batch], mini->types[batch]);
      platform_assert(ref == AL_NO_REFS);
      ref = allocator_dec_ref(
         mini->al, mini->next_extent[batch], mini->types[batch]);
      platform_assert(ref == AL_FREE);
   }

   mini_deinit(mini->cc, mini->meta_head, mini->meta_type);
}


/*
 *-----------------------------------------------------------------------------
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
 *-----------------------------------------------------------------------------
 */

typedef bool (*mini_for_each_fn)(cache    *cc,
                                 page_type type,
                                 uint64    base_addr,
                                 void     *out);

static void
mini_unkeyed_for_each(cache           *cc,
                      uint64           meta_head,
                      page_type        meta_type,
                      mini_for_each_fn func,
                      void            *out)
{
   uint64 meta_addr = meta_head;
   do {
      page_handle *meta_page = cache_get(cc, meta_addr, TRUE, meta_type);
      debug_assert(meta_type == mini_meta_type(meta_page));
      uint64              num_meta_entries = mini_num_entries(meta_page);
      unkeyed_meta_entry *entry            = unkeyed_first_entry(meta_page);
      for (uint64 i = 0; i < num_meta_entries; i++) {
         func(cc, entry->type, entry->extent_addr, out);
         entry = unkeyed_next_entry(entry);
      }
      meta_addr = mini_get_next_meta_addr(meta_page);
      cache_unget(cc, meta_page);
   } while (meta_addr != 0);
}

/*
 * NOTE: The exact values of these enums is *** important *** to
 * interval_intersects_range(). See its implementation and comments.
 */
typedef enum boundary_state {
   before_start = 1,
   in_range     = 0,
   after_end    = 2
} boundary_state;

static bool
interval_intersects_range(boundary_state left_state, boundary_state right_state)
{
   /*
    * The interval [left, right] intersects the interval [begin, end]
    * if left_state != right_state or if left_state == right_state ==
    * in_range = 0.
    *
    * The predicate below works as long as
    * - in_range == 0, and
    * - before_start & after_end == 0.
    */
   return (left_state & right_state) == 0;
}

static boundary_state
state(data_config *cfg,
      const slice  start_key,
      const slice  end_key,
      const slice  entry_start_key)
{
   debug_assert(slice_is_null(start_key) == slice_is_null(end_key));
   if (slice_is_null(start_key)) {
      return in_range;
   } else if (data_key_compare(cfg, entry_start_key, start_key) < 0) {
      return before_start;
   } else if (data_key_compare(cfg, entry_start_key, end_key) <= 0) {
      return in_range;
   } else {
      return after_end;
   }
}

/*
 *-----------------------------------------------------------------------------
 * Apply func to every extent whose key range intersects [start_key, _end_key].
 *
 * Note: if start_key is null, then so must be _end_key, and func is
 * applied to all extents.
 *
 * Note: if _end_key is null, then we apply func to all extents whose
 * key range contains start_key.
 *
 * Note: the first extent in each batch is treated as starting at
 * -infinity, regardless of what key was specified as its starting
 * point in the call to mini_alloc.
 *
 * Note: the last extent in each batch is treated as ending at
 * +infinity, regardless of the what key was specified as the ending
 * point passed to mini_release.
 *-----------------------------------------------------------------------------
 */
static bool
mini_keyed_for_each(cache           *cc,
                    data_config     *cfg,
                    uint64           meta_head,
                    page_type        meta_type,
                    const slice      start_key,
                    const slice      _end_key,
                    mini_for_each_fn func,
                    void            *out)
{
   slice end_key = _end_key;
   if (slice_is_null(end_key))
      end_key = start_key;

   // Should not be called if there are no intersecting ranges, we track with
   // did_work.
   debug_only bool did_work = FALSE;

   uint64 meta_addr = meta_head;

   boundary_state current_state[MINI_MAX_BATCHES];
   uint64         extent_addr[MINI_MAX_BATCHES];
   page_type      extent_type[MINI_MAX_BATCHES];
   bool           should_cleanup[MINI_MAX_BATCHES];
   for (uint64 i = 0; i < MINI_MAX_BATCHES; i++) {
      current_state[i] = before_start;
      extent_addr[i]    = TERMINAL_EXTENT_ADDR;
      extent_type[i]    = PAGE_TYPE_INVALID;
      should_cleanup[i] = 2;
      debug_assert(should_cleanup[i] != TRUE);
      debug_assert(should_cleanup[i] != FALSE);
   }

   do {
      page_handle      *meta_page = cache_get(cc, meta_addr, TRUE, meta_type);
      debug_assert(meta_type == mini_meta_type(meta_page));
      keyed_meta_entry *entry     = keyed_first_entry(meta_page);
      for (uint64 i = 0; i < mini_num_entries(meta_page); i++) {
         uint64         batch = entry->batch;
         boundary_state next_state;
         if (extent_addr[batch] == TERMINAL_EXTENT_ADDR) {
            // Treat the first extent in each batch as if it started at
            // -infinity
            next_state = before_start;
         } else if (entry->extent_addr == TERMINAL_EXTENT_ADDR) {
            // Treat the last extent as going to +infinity
            next_state = after_end;
         } else {
            const slice entry_start_key = keyed_meta_entry_start_key(entry);
            next_state = state(cfg, start_key, end_key, entry_start_key);
         }

         if (interval_intersects_range(current_state[batch], next_state)) {
            debug_code(did_work = TRUE);
            bool entry_should_cleanup =
               func(cc, extent_type[batch], extent_addr[batch], out);
            should_cleanup[batch] =
               should_cleanup[batch] && entry_should_cleanup;
         }

         extent_addr[batch]   = entry->extent_addr;
         extent_type[batch]   = entry->type;
         current_state[batch] = next_state;
         entry                = keyed_next_entry(entry);
      }

      meta_addr = mini_get_next_meta_addr(meta_page);
      cache_unget(cc, meta_page);
   } while (meta_addr != 0);


   debug_code(
      if (!did_work) { mini_keyed_print(cc, cfg, meta_head, meta_type); });
   debug_assert(did_work);

   for (int i = MINI_MAX_BATCHES - 1; i >= 0; i--) {
      if (should_cleanup[i] != 2) {
         return should_cleanup[i];
      }
   }
   platform_assert(FALSE);
}

/*
 * Apply func to every extent whose key range intersects [start_key, _end_key].
 *
 * Note: if start_key is null, then so must be _end_key, and func is
 * applied to all extents.
 *
 * Note: if _end_key is null, then we apply func to all extents whose
 * key range contains start_key.
 *
 * Note: the first extent in each batch is treated as starting at
 * -infinity, regardless of what key was specified as its starting
 * point in the call to mini_alloc.
 *
 * Note: the last extent in each batch is treated as ending at
 * +infinity, regardless of the what key was specified as the ending
 * point passed to mini_release.
 *-----------------------------------------------------------------------------
 */
static bool
mini_keyed_for_each_self_exclusive(cache           *cc,
                                   data_config     *cfg,
                                   uint64           meta_head,
                                   page_type        meta_type,
                                   const slice      start_key,
                                   const slice      _end_key,
                                   mini_for_each_fn func,
                                   void            *out)
{
   slice end_key = _end_key;
   if (slice_is_null(end_key)) {
      end_key = start_key;
   }

   // Should not be called if there are no intersecting ranges, we track with
   // did_work.
   debug_only bool did_work = FALSE;

   uint64       meta_addr = meta_head;
   page_handle *meta_page = mini_get_claim_meta_page(cc, meta_head, meta_type);
   debug_assert(meta_type == mini_meta_type(meta_page));

   boundary_state current_state[MINI_MAX_BATCHES];
   uint64         extent_addr[MINI_MAX_BATCHES];
   page_type      extent_type[MINI_MAX_BATCHES];
   bool           should_cleanup[MINI_MAX_BATCHES];
   for (uint64 i = 0; i < MINI_MAX_BATCHES; i++) {
      current_state[i] = before_start;
      extent_addr[i]    = TERMINAL_EXTENT_ADDR;
      extent_type[i]    = PAGE_TYPE_INVALID;
      should_cleanup[i] = 2;
      debug_assert(should_cleanup[i] != TRUE);
      debug_assert(should_cleanup[i] != FALSE);
   }

   do {
      keyed_meta_entry *entry = keyed_first_entry(meta_page);
      for (uint64 i = 0; i < mini_num_entries(meta_page); i++) {
         uint64         batch = entry->batch;
         boundary_state next_state;
         if (extent_addr[batch] == TERMINAL_EXTENT_ADDR) {
            // Treat the first extent in each batch as if it started at
            // -infinity
            next_state = before_start;
         } else if (entry->extent_addr == TERMINAL_EXTENT_ADDR) {
            // Treat the last extent as going to +infinity
            next_state = after_end;
         } else {
            const slice entry_start_key = keyed_meta_entry_start_key(entry);
            next_state = state(cfg, start_key, end_key, entry_start_key);
         }

         if (interval_intersects_range(current_state[batch], next_state)) {
            debug_code(did_work = TRUE);
            bool entry_should_cleanup =
               func(cc, extent_type[batch], extent_addr[batch], out);
            should_cleanup[batch] =
               should_cleanup[batch] && entry_should_cleanup;
         }

         extent_addr[batch]   = entry->extent_addr;
         extent_type[batch]   = entry->type;
         current_state[batch] = next_state;
         entry                = keyed_next_entry(entry);
      }

      meta_addr = mini_get_next_meta_addr(meta_page);
      if (meta_addr != 0) {
         page_handle *next_meta_page =
            mini_get_claim_meta_page(cc, meta_addr, meta_type);
         debug_assert(meta_type == mini_meta_type(next_meta_page));
         mini_unget_unclaim_meta_page(cc, meta_page);
         meta_page = next_meta_page;
      }
   } while (meta_addr != 0);

   mini_unget_unclaim_meta_page(cc, meta_page);

   debug_code(
      if (!did_work) { mini_keyed_print(cc, cfg, meta_head, meta_type); });
   debug_assert(did_work);

   for (int i = MINI_MAX_BATCHES - 1; i >= 0; i--) {
      if (should_cleanup[i] != 2) {
         return should_cleanup[i];
      }
   }
   platform_assert(FALSE);
}

/*
 *-----------------------------------------------------------------------------
 * mini_unkeyed_[inc,dec]_ref --
 *
 *      Increments or decrements the ref count of the unkeyed allocator. When
 *      the external ref count reaches 0 (actual ref count reaches
 *      MINI_NO_REFS), the mini allocator is destroyed.
 *
 * Results:
 *      Prior external ref count (internal ref count - MINI_NO_REFS)
 *
 * Side effects:
 *      Deallocation/cache side effects when external ref count hits 0
 *-----------------------------------------------------------------------------
 */
uint8
mini_unkeyed_inc_ref(cache *cc, uint64 meta_head)
{
   allocator *al        = cache_allocator(cc);
   uint64     base_addr = cache_extent_base_addr(cc, meta_head);
   uint8      ref       = allocator_inc_ref(al, base_addr);
   platform_assert(ref > MINI_NO_REFS);
   return ref - MINI_NO_REFS;
}

static bool
mini_dealloc_extent(cache *cc, page_type type, uint64 base_addr, void *out)
{
   allocator *al  = cache_allocator(cc);
   uint8      ref = allocator_dec_ref(al, base_addr, type);
   if (ref == AL_NO_REFS) {
      cache_hard_evict_extent(cc, base_addr, type);
      ref = allocator_dec_ref(al, base_addr, type);
      platform_assert(ref == AL_FREE);
   }
   return TRUE;
}

uint8
mini_unkeyed_dec_ref(cache *cc, uint64 meta_head, page_type meta_type)
{
   allocator *al        = cache_allocator(cc);
   uint64     base_addr = cache_extent_base_addr(cc, meta_head);
   uint8      ref       = allocator_dec_ref(al, base_addr, meta_type);
   if (ref != MINI_NO_REFS) {
      debug_assert(ref != AL_NO_REFS);
      debug_assert(ref != AL_FREE);
      return ref - MINI_NO_REFS;
   }

   // need to deallocate and clean up the mini allocator
   mini_unkeyed_for_each(cc, meta_head, meta_type, mini_dealloc_extent, NULL);
   mini_deinit(cc, meta_head, meta_type);
   return 0;
}

/*
 *-----------------------------------------------------------------------------
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
 *      Note: Range queries do not hold keyed references to branches in the
 *      mini_allocator (b/c it's too expensive), and instead hold references to
 *      the meta_head, called blocks here. To prevent calls from
 *      mini_keyed_dec_ref from deallocating while they are reading,
 *      mini_keyed_dec_ref must see no additional refs (blockers) on the
 *      meta_head before proceeding. After starting, they do not need to check
 *      again, since a range query cannot have gotten a reference to their range
 *      after the call to dec_ref is made.
 *
 * Results:
 *      None
 *
 * Side effects:
 *      Deallocation/cache side effects.
 *-----------------------------------------------------------------------------
 */
static bool
mini_keyed_inc_ref_extent(cache    *cc,
                          page_type type,
                          uint64    base_addr,
                          void     *out)
{
   allocator *al = cache_allocator(cc);
   allocator_inc_ref(al, base_addr);
   return FALSE;
}

void
mini_keyed_inc_ref(cache       *cc,
                   data_config *data_cfg,
                   page_type    type,
                   uint64       meta_head,
                   const slice  start_key,
                   const slice  end_key)
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

static bool
mini_keyed_dec_ref_extent(cache    *cc,
                          page_type type,
                          uint64    base_addr,
                          void     *out)
{
   allocator *al  = cache_allocator(cc);
   uint8      ref = allocator_dec_ref(al, base_addr, type);
   if (ref == AL_NO_REFS) {
      cache_hard_evict_extent(cc, base_addr, type);
      ref = allocator_dec_ref(al, base_addr, type);
      platform_assert(ref == AL_FREE);
      return TRUE;
   }
   return FALSE;
}

static void
mini_wait_for_blockers(cache *cc, uint64 meta_head)
{
   allocator *al        = cache_allocator(cc);
   uint64     base_addr = cache_extent_base_addr(cc, meta_head);
   uint64     wait      = 1;
   while (allocator_get_ref(al, base_addr) != AL_ONE_REF) {
      platform_sleep(wait);
      wait = wait > 1024 ? wait : 2 * wait;
   }
}

bool
mini_keyed_dec_ref(cache       *cc,
                   data_config *data_cfg,
                   page_type    meta_type,
                   uint64       meta_head,
                   const slice  start_key,
                   const slice  end_key)
{
   mini_wait_for_blockers(cc, meta_head);
   bool should_cleanup =
      mini_keyed_for_each_self_exclusive(cc,
                                         data_cfg,
                                         meta_head,
                                         meta_type,
                                         start_key,
                                         end_key,
                                         mini_keyed_dec_ref_extent,
                                         NULL);
   if (should_cleanup) {
      allocator *al        = cache_allocator(cc);
      uint64     base_addr = cache_extent_base_addr(cc, meta_head);
      uint8      ref       = allocator_get_ref(al, base_addr);
      platform_assert(ref == AL_ONE_REF);
      mini_deinit(cc, meta_head, meta_type);
   }
   return should_cleanup;
}

/*
 *-----------------------------------------------------------------------------
 * mini_keyed_(un)block_dec_ref --
 *
 *      Block/unblock dec_ref callers. See note in mini_keyed_dec_ref for
 *      details.
 *
 * Results:
 *      None
 *
 * Side effects:
 *      None
 *-----------------------------------------------------------------------------
 */
void
mini_block_dec_ref(cache *cc, uint64 meta_head)
{
   allocator *al        = cache_allocator(cc);
   uint64     base_addr = cache_extent_base_addr(cc, meta_head);
   uint8      ref       = allocator_inc_ref(al, base_addr);
   platform_assert(ref > AL_ONE_REF);
}

void
mini_unblock_dec_ref(cache *cc, uint64 meta_head)
{
   allocator *al        = cache_allocator(cc);
   uint64     base_addr = cache_extent_base_addr(cc, meta_head);
   uint8      ref       = allocator_dec_ref(al, base_addr, PAGE_TYPE_INVALID);
   platform_assert(ref >= AL_ONE_REF);
}

/*
 *-----------------------------------------------------------------------------
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
 *-----------------------------------------------------------------------------
 */
static bool
mini_keyed_count_extents(cache *cc, page_type type, uint64 base_addr, void *out)
{
   uint64 *count = (uint64 *)out;
   (*count)++;
   return FALSE;
}

uint64
mini_keyed_extent_count(cache       *cc,
                        data_config *data_cfg,
                        page_type    type,
                        uint64       meta_head,
                        const slice  start_key,
                        const slice  end_key)
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
 * mini_unkeyed_prefetch --
 *
 *      Prefetches all extents in the (unkeyed) mini allocator.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Standard cache side effects.
 *-----------------------------------------------------------------------------
 */
static bool
mini_prefetch_extent(cache *cc, page_type type, uint64 base_addr, void *out)
{
   cache_prefetch(cc, base_addr, type);
   return FALSE;
}

void
mini_unkeyed_prefetch(cache *cc, page_type meta_type, uint64 meta_head)
{
   mini_unkeyed_for_each(cc, meta_head, meta_type, mini_prefetch_extent, NULL);
}

/*
 *-----------------------------------------------------------------------------
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
 *-----------------------------------------------------------------------------
 */
void
mini_unkeyed_print(cache *cc, uint64 meta_head, page_type type)
{
   uint64 next_meta_addr = meta_head;

   platform_default_log("---------------------------------------------\n");
   platform_default_log("| Mini Allocator -- meta_head: %12lu |\n", meta_head);
   platform_default_log("|-------------------------------------------|\n");
   platform_default_log("| idx | %35s |\n", "extent_addr");
   platform_default_log("|-------------------------------------------|\n");

   do {
      page_handle *meta_page = cache_get(cc, next_meta_addr, TRUE, type);

      platform_default_log("| meta addr %31lu |\n", next_meta_addr);
      platform_default_log("|-------------------------------------------|\n");

      uint64              num_entries = mini_num_entries(meta_page);
      unkeyed_meta_entry *entry       = unkeyed_first_entry(meta_page);
      for (uint64 i = 0; i < num_entries; i++) {
         platform_default_log("| %3lu | %35lu |\n", i, entry->extent_addr);
         entry = unkeyed_next_entry(entry);
      }
      platform_default_log("|-------------------------------------------|\n");

      next_meta_addr = mini_get_next_meta_addr(meta_page);
      cache_unget(cc, meta_page);
   } while (next_meta_addr != 0);
   platform_default_log("\n");
}

void
mini_keyed_print(cache       *cc,
                 data_config *data_cfg,
                 uint64       meta_head,
                 page_type    type)
{
   allocator *al             = cache_allocator(cc);
   uint64     next_meta_addr = meta_head;

   platform_default_log("------------------------------------------------------"
                        "---------------\n");
   platform_default_log(
      "| Mini Keyed Allocator -- meta_head: %12lu                   |\n",
      meta_head);
   platform_default_log("|-----------------------------------------------------"
                        "--------------|\n");
   platform_default_log("| idx | %5s | %14s | %18s | %3s |\n",
                        "batch",
                        "extent_addr",
                        "start_key",
                        "rc");
   platform_default_log("|-----------------------------------------------------"
                        "--------------|\n");

   do {
      page_handle *meta_page = cache_get(cc, next_meta_addr, TRUE, type);

      uint64 base_meta_addr = cache_extent_base_addr(cc, next_meta_addr);
      platform_default_log(
         "| meta addr: %12lu (%u)                                       |\n",
         next_meta_addr,
         allocator_get_ref(al, base_meta_addr));
      platform_default_log("|--------------------------------------------------"
                           "-----------------|\n");

      uint64            num_entries = mini_num_entries(meta_page);
      keyed_meta_entry *entry       = keyed_first_entry(meta_page);
      for (uint64 i = 0; i < num_entries; i++) {
         slice start_key = keyed_meta_entry_start_key(entry);
         char  extent_str[32];
         if (entry->extent_addr == TERMINAL_EXTENT_ADDR) {
            snprintf(extent_str, sizeof(extent_str), "TERMINAL_ENTRY");
         } else {
            snprintf(
               extent_str, sizeof(extent_str), "%14lu", entry->extent_addr);
         }
         char ref_str[4];
         if (entry->extent_addr == TERMINAL_EXTENT_ADDR) {
            snprintf(ref_str, 4, "n/a");
         } else {
            uint8 ref = allocator_get_ref(al, entry->extent_addr);
            snprintf(ref_str, 4, "%3u", ref);
         }
         platform_default_log("| %3lu | %5u | %14s | %18.18s | %3s |\n",
                              i,
                              entry->batch,
                              extent_str,
                              key_string(data_cfg, start_key),
                              ref_str);
         entry = keyed_next_entry(entry);
      }
      platform_default_log("|--------------------------------------------------"
                           "-----------------|\n");

      next_meta_addr = mini_get_next_meta_addr(meta_page);
      cache_unget(cc, meta_page);
   } while (next_meta_addr != 0);
   platform_default_log("\n");
}
