// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *-----------------------------------------------------------------------------
 * mini_allocator.c --
 *
 *     This file contains the implementation for an allocator which
 *     allocates individual pages from extents.
 *-----------------------------------------------------------------------------
 */

#include "allocator.h"
#include "cache.h"
#include "mini_allocator.h"
#include "util.h"
#include "platform_sleep.h"

#include "poison.h"

// MINI_WAIT is a lock token used to lock a batch
#define MINI_WAIT 1
// MINI_NO_REFS is the ref count of a mini allocator with no external
// refs
#define MINI_NO_REFS 2

/*
 *-----------------------------------------------------------------------------
 * mini_meta_hdr -- Disk-resident structure
 *
 *      The header of a meta_page in a mini_allocator.
 *-----------------------------------------------------------------------------
 */
typedef struct ONDISK mini_meta_hdr {
   uint64 next_meta_addr;
   uint64 prev_meta_addr;
   uint64 pos;
   uint32 num_entries;
   char   entry_buffer[];
} mini_meta_hdr;

#define TERMINAL_EXTENT_ADDR ((uint64) - 1)

/*
 *-----------------------------------------------------------------------------
 * meta_entry -- Disk-resident structure
 *
 *      Metadata for each extent stored in the extent list for a
 *      mini_allocator. Currently, this is just the extent address itself.
 *-----------------------------------------------------------------------------
 */
/*
 * A meta_entry is packed into a single 8-byte word to keep the extent list
 * dense (it avoids ONDISK padding and wastes no space). Extents are at least
 * 128 KiB, so an extent address has at least 17 always-zero low bits; we store
 * the extent *number* (extent_addr / extent_size) instead of the address,
 * which frees more than enough bits to also record the page type and the
 * originating mini_allocator batch:
 *
 *   bits [ 0: 7]  batch          (one of MINI_MAX_BATCHES batches)
 *   bits [ 8:15]  type           (page_type)
 *   bits [16:63]  extent number  (extent_addr / extent_size)
 */
typedef struct ONDISK meta_entry {
   uint64 packed;
} meta_entry;

#define META_ENTRY_BATCH_BITS  (8)
#define META_ENTRY_TYPE_BITS   (8)
#define META_ENTRY_EXTENT_BITS (64 - META_ENTRY_BATCH_BITS - META_ENTRY_TYPE_BITS)

_Static_assert(MINI_MAX_BATCHES <= (1 << META_ENTRY_BATCH_BITS),
               "mini_allocator batch number does not fit in a meta_entry");
_Static_assert(NUM_PAGE_TYPES <= (1 << META_ENTRY_TYPE_BITS),
               "page_type does not fit in a meta_entry");

static inline uint64
meta_entry_batch(const meta_entry *entry)
{
   return entry->packed & ((1 << META_ENTRY_BATCH_BITS) - 1);
}

static inline page_type
meta_entry_type(const meta_entry *entry)
{
   return (page_type)((entry->packed >> META_ENTRY_BATCH_BITS)
                      & ((1 << META_ENTRY_TYPE_BITS) - 1));
}

static inline uint64
meta_entry_extent_addr(cache *cc, const meta_entry *entry)
{
   uint64 extent_number =
      entry->packed >> (META_ENTRY_BATCH_BITS + META_ENTRY_TYPE_BITS);
   return extent_number * cache_extent_size(cc);
}

static inline void
meta_entry_pack(cache      *cc,
                meta_entry *entry,
                uint64      extent_addr,
                page_type   type,
                uint64      batch)
{
   uint64 extent_size   = cache_extent_size(cc);
   uint64 extent_number = extent_addr / extent_size;
   debug_assert((extent_addr % extent_size) == 0);
   debug_assert(extent_number < (1ULL << META_ENTRY_EXTENT_BITS));
   debug_assert((uint64)type < (1 << META_ENTRY_TYPE_BITS));
   debug_assert(batch < (1 << META_ENTRY_BATCH_BITS));
   entry->packed =
      (extent_number << (META_ENTRY_BATCH_BITS + META_ENTRY_TYPE_BITS))
      | ((uint64)type << META_ENTRY_BATCH_BITS) | batch;
}

static meta_entry *
first_entry(page_handle *meta_page)
{
   return (meta_entry *)((mini_meta_hdr *)meta_page->data)->entry_buffer;
}

static meta_entry *
next_entry(meta_entry *entry)
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
   hdr->prev_meta_addr = 0;
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
      if (meta_tail == mini->meta_tail && cache_try_claim(mini->cc, meta_page))
      {
         break;
      }
      cache_unget(mini->cc, meta_page);
      platform_sleep_ns(wait);
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

static uint64
base_addr(cache *cc, uint64 addr)
{
   return allocator_config_extent_base_addr(
      allocator_get_config(cache_get_allocator(cc)), addr);
}

/*
 *-----------------------------------------------------------------------------
 * mini_init --
 *
 *      Initialize a new mini allocator.
 *
 * Results:
 *      The 0th batch next address to be allocated.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
uint64
mini_init(mini_allocator *mini,
          cache          *cc,
          uint64          meta_head,
          uint64          meta_tail,
          uint64          num_batches,
          page_type       type)
{
   page_type types[MINI_MAX_BATCHES];
   for (uint64 batch = 0; batch < num_batches; batch++) {
      types[batch] = type;
   }
   return mini_init_with_types(
      mini, cc, meta_head, meta_tail, num_batches, type, types);
}

uint64
mini_init_with_types(mini_allocator  *mini,
                     cache           *cc,
                     uint64           meta_head,
                     uint64           meta_tail,
                     uint64           num_batches,
                     page_type        meta_type,
                     const page_type *types)
{
   platform_assert(num_batches <= MINI_MAX_BATCHES);
   platform_assert(num_batches != 0);
   platform_assert(mini != NULL);
   platform_assert(cc != NULL);
   platform_assert(types != NULL);

   ZERO_CONTENTS(mini);
   mini->cc          = cc;
   mini->al          = cache_get_allocator(cc);
   mini->meta_head   = meta_head;
   mini->num_extents = 1; // for the meta page
   mini->num_batches = num_batches;
   mini->meta_type   = meta_type;
   mini->pinned      = (meta_type == PAGE_TYPE_MEMTABLE);
   memcpy(mini->types, types, num_batches * sizeof(*types));

   page_handle *meta_page;
   if (meta_tail == 0) {
      // new mini allocator
      mini->meta_tail = meta_head;
      meta_page       = cache_alloc(cc, mini->meta_head, meta_type);
      mini_init_meta_page(mini, meta_page);

      // meta_page gets an extra ref
      refcount ref =
         allocator_inc_ref(mini->al, base_addr(cc, mini->meta_head));
      platform_assert(ref == MINI_NO_REFS + 1);

      if (mini->pinned) {
         cache_pin(cc, meta_page);
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

   return mini_next_addr(mini, 0);
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

static bool32
entry_fits_in_page(uint64 page_size, uint64 start, uint64 entry_size)
{
   return start + entry_size <= page_size;
}


static bool32
mini_append_entry_to_page(mini_allocator *mini,
                          page_handle    *meta_page,
                          uint64          extent_addr,
                          page_type       type,
                          uint64          batch)
{
   uint64 page_size = cache_page_size(mini->cc);
   debug_assert(extent_addr != 0);
   debug_assert((extent_addr % page_size) == 0);

   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;

   if (!entry_fits_in_page(page_size, hdr->pos, sizeof(meta_entry))) {
      return FALSE;
   }

   meta_entry *new_entry = pointer_byte_offset(hdr, hdr->pos);
   meta_entry_pack(mini->cc, new_entry, extent_addr, type, batch);

   hdr->pos += sizeof(meta_entry);
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
      platform_sleep_ns(wait);
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
   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;
   return hdr->next_meta_addr;
}

static void
mini_set_next_meta_addr(mini_allocator *mini,
                        page_handle    *meta_page,
                        uint64          next_meta_addr)
{
   mini_meta_hdr *hdr  = (mini_meta_hdr *)meta_page->data;
   hdr->next_meta_addr = next_meta_addr;
}

static void
mini_set_prev_meta_addr(mini_allocator *mini,
                        page_handle    *meta_page,
                        uint64          prev_meta_addr)
{
   mini_meta_hdr *hdr  = (mini_meta_hdr *)meta_page->data;
   hdr->prev_meta_addr = prev_meta_addr;
}

static bool32
mini_append_entry(mini_allocator *mini, uint64 batch, uint64 next_addr)
{
   page_handle *meta_page = mini_full_lock_meta_tail(mini);
   bool32       success;
   success = mini_append_entry_to_page(
      mini, meta_page, next_addr, mini->types[batch], batch);
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
      uint64       last_meta_addr = mini->meta_tail;
      meta_page       = cache_alloc(mini->cc, new_meta_tail, mini->meta_type);
      mini->meta_tail = new_meta_tail;
      mini_full_unlock_meta_page(mini, last_meta_page);
      mini_init_meta_page(mini, meta_page);
      // Doubly-link the meta list so a prefetch cursor can scan it backward.
      mini_set_prev_meta_addr(mini, meta_page, last_meta_addr);

      success = mini_append_entry_to_page(
         mini, meta_page, next_addr, mini->types[batch], batch);

      if (mini->pinned) {
         cache_pin(mini->cc, meta_page);
      }
      debug_assert(success);
   }
   // Record the meta page that now holds this extent's entry, so btree nodes
   // allocated from this extent can point straight at it (see
   // mini_current_extent_meta_page). Safe: the caller holds the batch lock, so
   // mini->meta_tail cannot advance under us until we unlock the page below.
   mini->cur_extent_meta_page[batch] = mini->meta_tail;
   mini_full_unlock_meta_page(mini, meta_page);
   return TRUE;
}

/*
 *-----------------------------------------------------------------------------
 * mini_alloc --
 *
 *      Allocate a next disk address from the mini_allocator.
 *
 *      If next_extent is not NULL, then the successor extent to the allocated
 *      addr will be copied to it.
 *
 * Results:
 *      A newly allocated disk address.
 *
 * Side effects:
 *      Disk allocation, standard cache side effects.
 *-----------------------------------------------------------------------------
 */
uint64
mini_alloc(mini_allocator *mini, uint64 batch, uint64 *next_extent)
{
   debug_assert(batch < mini->num_batches);

   uint64 next_addr = mini_lock_batch_get_next_addr(mini, batch);

   if (next_addr % cache_extent_size(mini->cc) == 0) {
      // need to allocate the next extent

      uint64          extent_addr = mini->next_extent[batch];
      platform_status rc          = mini_allocator_get_new_extent(
         mini, mini->types[batch], &mini->next_extent[batch]);
      platform_assert_status_ok(rc);
      next_addr = extent_addr;

      bool32 success = mini_append_entry(mini, batch, next_addr);
      platform_assert(success);
   }

   if (next_extent) {
      *next_extent = mini->next_extent[batch];
   }

   uint64 new_next_addr = next_addr + cache_page_size(mini->cc);
   mini_unlock_batch_set_next_addr(mini, batch, new_next_addr);
   return next_addr;
}

platform_status
mini_alloc_bytes(mini_allocator *mini,
                 uint64          batch,
                 uint64          num_bytes,
                 uint64          alignment,
                 uint64          boundary,
                 uint64          addrs[2],
                 uint64         *next_extent)
{
   uint64 extent_size = cache_extent_size(mini->cc);
   if (alignment == 0) {
      alignment = 1;
   }
   debug_assert(batch < mini->num_batches);
   debug_assert(num_bytes <= extent_size);
   debug_assert(boundary % alignment == 0);
   debug_assert(boundary < num_bytes || extent_size % boundary == 0);

   addrs[0] = 0;
   addrs[1] = 0;

   uint64 next_addr = mini_lock_batch_get_next_addr(mini, batch);

   if (next_addr % alignment) {
      next_addr += alignment - (next_addr % alignment);
   }

   if (num_bytes <= boundary
       && next_addr / boundary != (next_addr + num_bytes - 1) / boundary)
   {
      next_addr += boundary - (next_addr % boundary);
   }

   uint64 num_allocs = 0;
   uint64 remainder  = num_bytes;
   while (remainder) {
      if (next_addr % extent_size == 0) {
         uint64          extent_addr = mini->next_extent[batch];
         platform_status rc          = mini_allocator_get_new_extent(
            mini, mini->types[batch], &mini->next_extent[batch]);
         if (!SUCCESS(rc)) {
            mini_unlock_batch_set_next_addr(mini, batch, next_addr);
            return rc;
         }
         next_addr = extent_addr;

         bool32 success = mini_append_entry(mini, batch, next_addr);
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

   debug_assert(mini->saved_next_addr[batch] == 0);
   mini->saved_next_addr[batch] = next_addr;
   return STATUS_OK;
}

void
mini_alloc_bytes_finish(mini_allocator *mini, uint64 batch)
{
   uint64 saved_next_addr       = mini->saved_next_addr[batch];
   mini->saved_next_addr[batch] = 0;
   mini_unlock_batch_set_next_addr(mini, batch, saved_next_addr);
}

uint64
mini_alloc_page(mini_allocator *mini, uint64 batch, uint64 *next_extent)
{
   uint64          addrs[2]  = {0, 0};
   uint64          page_size = cache_page_size(mini->cc);
   platform_status rc        = mini_alloc_bytes(
      mini, batch, page_size, page_size, 0, addrs, next_extent);
   if (!SUCCESS(rc)) {
      return 0;
   }
   debug_assert(addrs[1] == 0);
   mini_alloc_bytes_finish(mini, batch);
   return addrs[0];
}

uint64
mini_alloc_extent(mini_allocator *mini, uint64 batch, uint64 *next_extent)
{
   uint64          addrs[2]    = {0, 0};
   uint64          extent_size = cache_extent_size(mini->cc);
   platform_status rc          = mini_alloc_bytes(
      mini, batch, extent_size, extent_size, 0, addrs, next_extent);
   if (!SUCCESS(rc)) {
      return 0;
   }
   debug_assert(addrs[1] == 0);
   mini_alloc_bytes_finish(mini, batch);
   return addrs[0];
}

platform_status
mini_attach_extent(mini_allocator *mini, uint64 batch, uint64 addr)
{
   debug_assert(batch < mini->num_batches);
   debug_assert(addr % cache_extent_size(mini->cc) == 0);

   uint64 old_next_addr = mini_lock_batch_get_next_addr(mini, batch);
   allocator_inc_ref(mini->al, addr);
   bool32 success = mini_append_entry(mini, batch, addr);
   platform_assert(success);
   mini_unlock_batch_set_next_addr(mini, batch, old_next_addr);
   return STATUS_OK;
}

uint64
mini_next_addr(mini_allocator *mini, uint64 batch)
{
   debug_assert(batch < mini->num_batches);
   if ((mini->next_addr[batch] % cache_extent_size(mini->cc)) != 0) {
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
 * Results:
 *      None.
 *
 * Side effects:
 *      Disk deallocation, standard cache side effects.
 *-----------------------------------------------------------------------------
 */
void
mini_release(mini_allocator *mini)
{
   for (uint64 batch = 0; batch < mini->num_batches; batch++) {
      // Dealloc the next extent
      refcount ref = allocator_dec_ref(
         mini->al, mini->next_extent[batch], mini->types[batch]);
      platform_assert(ref == AL_NO_REFS);
      ref = allocator_dec_ref(
         mini->al, mini->next_extent[batch], mini->types[batch]);
      platform_assert(ref == AL_FREE);
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

static void
mini_deinit(cache *cc, uint64 meta_head, page_type type)
{
   allocator *al        = cache_get_allocator(cc);
   uint64     meta_addr = meta_head;
   do {
      page_handle *meta_page      = cache_get(cc, meta_addr, TRUE, type);
      uint64       last_meta_addr = meta_addr;
      meta_addr                   = mini_get_next_meta_addr(meta_page);
      cache_unget(cc, meta_page);

      allocator_config *allocator_cfg =
         allocator_get_config(cache_get_allocator(cc));
      if (!allocator_config_pages_share_extent(
             allocator_cfg, last_meta_addr, meta_addr))
      {
         uint64   last_meta_base_addr = base_addr(cc, last_meta_addr);
         refcount ref = allocator_dec_ref(al, last_meta_base_addr, type);
         platform_assert(ref == AL_NO_REFS);
         cache_extent_discard(cc, last_meta_base_addr, type);
         ref = allocator_dec_ref(al, last_meta_base_addr, type);
         platform_assert(ref == AL_FREE);
      }
   } while (meta_addr != 0);
}

/*
 *-----------------------------------------------------------------------------
 * mini_for_each_meta_page --
 *
 *      Calls func on each meta_page in the mini_allocator.
 *
 * Results:
 *      None
 *
 * Side effects:
 *      func may store output in arg.
 *-----------------------------------------------------------------------------
 */

typedef void (*mini_for_each_meta_page_fn)(cache       *cc,
                                           page_type    type,
                                           page_handle *meta_page,
                                           void        *arg);

static void
mini_for_each_meta_page(cache                     *cc,
                        uint64                     meta_head,
                        page_type                  type,
                        mini_for_each_meta_page_fn func,
                        void                      *arg)
{
   uint64 meta_addr = meta_head;
   while (meta_addr != 0) {
      page_handle *meta_page = cache_get(cc, meta_addr, TRUE, type);
      func(cc, type, meta_page, arg);
      meta_addr = mini_get_next_meta_addr(meta_page);
      cache_unget(cc, meta_page);
   }
}

/* mini_for_each(): call a function on each allocated extent in the
 * mini_allocator (not including the extents used by the mini_allocator itself).
 */
typedef void (*mini_for_each_fn)(cache    *cc,
                                 page_type type,
                                 uint64    extent_addr,
                                 void     *arg);

typedef struct for_each_func {
   mini_for_each_fn func;
   void            *arg;
} for_each_func;

static void
mini_for_each_meta_page_func(cache       *cc,
                             page_type    type,
                             page_handle *meta_page,
                             void        *arg)
{
   for_each_func *fef = (for_each_func *)arg;

   uint64      num_meta_entries = mini_num_entries(meta_page);
   meta_entry *entry            = first_entry(meta_page);
   for (uint64 i = 0; i < num_meta_entries; i++) {
      fef->func(cc,
                meta_entry_type(entry),
                meta_entry_extent_addr(cc, entry),
                fef->arg);
      entry = next_entry(entry);
   }
}

static void
mini_for_each(cache           *cc,
              uint64           meta_head,
              page_type        type,
              mini_for_each_fn func,
              void            *out)
{
   for_each_func fef = {func, out};
   mini_for_each_meta_page(
      cc, meta_head, type, mini_for_each_meta_page_func, &fef);
}


/*
 *-----------------------------------------------------------------------------
 * mini_[inc,dec]_ref --
 *
 *      Increments or decrements the ref count of the allocator. When
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
refcount
mini_inc_ref(cache *cc, uint64 meta_head)
{
   allocator *al  = cache_get_allocator(cc);
   refcount   ref = allocator_inc_ref(al, base_addr(cc, meta_head));
   platform_assert(ref > MINI_NO_REFS);
   return ref - MINI_NO_REFS;
}

static void
mini_dealloc_extent(cache *cc, page_type type, uint64 base_addr, void *out)
{
   allocator *al  = cache_get_allocator(cc);
   refcount   ref = allocator_dec_ref(al, base_addr, type);
   if (ref == AL_NO_REFS) {
      cache_extent_discard(cc, base_addr, type);
      ref = allocator_dec_ref(al, base_addr, type);
      platform_assert(ref == AL_FREE);
   } else {
      platform_assert(ref != AL_FREE);
   }
}

refcount
mini_dec_ref(cache *cc, uint64 meta_head, page_type type)
{
   allocator *al  = cache_get_allocator(cc);
   refcount   ref = allocator_dec_ref(al, base_addr(cc, meta_head), type);
   if (ref != MINI_NO_REFS) {
      debug_assert(ref != AL_NO_REFS);
      debug_assert(ref != AL_FREE);
      return ref - MINI_NO_REFS;
   }

   // need to deallocate and clean up the mini allocator
   mini_for_each(cc, meta_head, type, mini_dealloc_extent, NULL);
   mini_deinit(cc, meta_head, type);
   return 0;
}

/*
 *-----------------------------------------------------------------------------
 * mini_prefetch --
 *
 *      Prefetches all extents in the mini allocator.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Standard cache side effects.
 *-----------------------------------------------------------------------------
 */
static void
mini_prefetch_extent(cache *cc, page_type type, uint64 base_addr, void *out)
{
   cache_prefetch(cc, base_addr, type);
}

void
mini_prefetch(cache *cc, page_type type, uint64 meta_head)
{
   mini_for_each(cc, meta_head, type, mini_prefetch_extent, NULL);
}

/*
 *-----------------------------------------------------------------------------
 * mini_meta_cursor -- forward cursor over a mini_allocator's extent entries.
 *-----------------------------------------------------------------------------
 */
void
mini_meta_cursor_init(mini_meta_cursor *cursor,
                      cache            *cc,
                      page_type         meta_type,
                      uint64            meta_addr)
{
   cursor->cc          = cc;
   cursor->meta_type   = meta_type;
   cursor->meta_page   = NULL;
   cursor->meta_addr   = meta_addr;
   cursor->entry_idx   = 0;
   cursor->num_entries = 0;
}

void
mini_meta_cursor_deinit(mini_meta_cursor *cursor)
{
   if (cursor->meta_page != NULL) {
      cache_unget(cursor->cc, cursor->meta_page);
      cursor->meta_page = NULL;
   }
}

mini_meta_cursor_status
mini_meta_cursor_next(mini_meta_cursor *cursor,
                      uint64           *extent_addr,
                      uint64           *batch)
{
   while (TRUE) {
      if (cursor->meta_page == NULL) {
         if (cursor->meta_addr == 0) {
            return MINI_META_CURSOR_END;
         }
         // Non-blocking: if the meta page isn't resident, kick off a single-page
         // prefetch and let the caller retry later.
         cursor->meta_page = cache_get(
            cursor->cc, cursor->meta_addr, FALSE, cursor->meta_type);
         if (cursor->meta_page == NULL) {
            cache_prefetch_page(
               cursor->cc, cursor->meta_addr, cursor->meta_type);
            return MINI_META_CURSOR_WOULD_BLOCK;
         }
         cursor->num_entries = mini_num_entries(cursor->meta_page);
         cursor->entry_idx   = 0;
      }

      if (cursor->entry_idx < cursor->num_entries) {
         meta_entry *entry = first_entry(cursor->meta_page) + cursor->entry_idx;
         *extent_addr      = meta_entry_extent_addr(cursor->cc, entry);
         *batch            = meta_entry_batch(entry);
         cursor->entry_idx++;
         return MINI_META_CURSOR_ENTRY;
      }

      // Exhausted this page; advance to the next one (if any).
      uint64 next_meta_addr = mini_get_next_meta_addr(cursor->meta_page);
      cache_unget(cursor->cc, cursor->meta_page);
      cursor->meta_page = NULL;
      cursor->meta_addr = next_meta_addr;
   }
}

mini_meta_cursor_status
mini_meta_cursor_seek_extent(mini_meta_cursor *cursor,
                             uint64            target_extent_addr)
{
   uint64 extent_addr;
   uint64 batch;
   while (TRUE) {
      mini_meta_cursor_status status =
         mini_meta_cursor_next(cursor, &extent_addr, &batch);
      if (status != MINI_META_CURSOR_ENTRY) {
         return status; // END or WOULD_BLOCK
      }
      if (extent_addr == target_extent_addr) {
         return MINI_META_CURSOR_ENTRY;
      }
   }
}

mini_meta_cursor_status
mini_meta_cursor_prev(mini_meta_cursor *cursor,
                      uint64           *extent_addr,
                      uint64           *batch)
{
   while (TRUE) {
      if (cursor->meta_page == NULL) {
         return MINI_META_CURSOR_END;
      }

      if (cursor->entry_idx > 0) {
         cursor->entry_idx--;
         meta_entry *entry = first_entry(cursor->meta_page) + cursor->entry_idx;
         *extent_addr      = meta_entry_extent_addr(cursor->cc, entry);
         *batch            = meta_entry_batch(entry);
         return MINI_META_CURSOR_ENTRY;
      }

      // entry_idx == 0: exhausted this page going backward.
      mini_meta_hdr *hdr       = (mini_meta_hdr *)cursor->meta_page->data;
      uint64         prev_addr = hdr->prev_meta_addr;
      if (prev_addr == 0) {
         return MINI_META_CURSOR_END;
      }

      // Non-blocking: keep the current page alive so prev_meta_addr remains
      // accessible on a WOULD_BLOCK retry — do NOT release before the load.
      page_handle *prev_page =
         cache_get(cursor->cc, prev_addr, FALSE, cursor->meta_type);
      if (prev_page == NULL) {
         cache_prefetch_page(cursor->cc, prev_addr, cursor->meta_type);
         return MINI_META_CURSOR_WOULD_BLOCK;
      }

      cache_unget(cursor->cc, cursor->meta_page);
      cursor->meta_page   = prev_page;
      cursor->meta_addr   = prev_addr;
      cursor->num_entries = mini_num_entries(cursor->meta_page);
      cursor->entry_idx   = cursor->num_entries;
      // Loop: entry_idx == num_entries > 0, will decrement and read.
   }
}

static void
space_use_add_extent(cache *cc, page_type type, uint64 extent_addr, void *out)
{
   uint64 *sum = (uint64 *)out;
   *sum += cache_extent_size(cc);
}

static void
space_use_add_meta_page(cache       *cc,
                        page_type    type,
                        page_handle *meta_page,
                        void        *out)
{
   uint64 *sum = (uint64 *)out;
   *sum += cache_page_size(cc);
}

uint64
mini_space_use_bytes(cache *cc, uint64 meta_head, page_type type)
{
   uint64 total = 0;
   mini_for_each(cc, meta_head, type, space_use_add_extent, &total);
   mini_for_each_meta_page(
      cc, meta_head, type, space_use_add_meta_page, &total);
   return total;
}


/*
 *-----------------------------------------------------------------------------
 * mini_print --
 *
 *      Prints each meta_page together with all its entries to
 *      PLATFORM_DEFAULT_LOG.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
void
mini_print(cache *cc, uint64 meta_head, page_type type)
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

      uint64      num_entries = mini_num_entries(meta_page);
      meta_entry *entry       = first_entry(meta_page);
      for (uint64 i = 0; i < num_entries; i++) {
         platform_default_log("| %3lu | %35lu | %s\n",
                              i,
                              meta_entry_extent_addr(cc, entry),
                              page_type_str[meta_entry_type(entry)]);
         entry = next_entry(entry);
      }
      platform_default_log("|-------------------------------------------|\n");

      next_meta_addr = mini_get_next_meta_addr(meta_page);
      cache_unget(cc, meta_page);
   } while (next_meta_addr != 0);
   platform_default_log("\n");
}
