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
   uint64 meta_page_no;
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
 *      mini_allocator.
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

#define META_ENTRY_BATCH_BITS (8)
#define META_ENTRY_TYPE_BITS  (8)
#define META_ENTRY_EXTENT_BITS                                                 \
   (64 - META_ENTRY_BATCH_BITS - META_ENTRY_TYPE_BITS)

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
   platform_assert(cc != NULL);
   platform_assert(entry != NULL);

   uint64 extent_size   = cache_extent_size(cc);
   uint64 extent_number = extent_addr / extent_size;
   platform_assert(extent_addr != 0);
   platform_assert((extent_addr % extent_size) == 0);
   platform_assert(extent_number < (1ULL << META_ENTRY_EXTENT_BITS));
   platform_assert(PAGE_TYPE_FIRST <= type && type < NUM_PAGE_TYPES);
   platform_assert(batch < MINI_MAX_BATCHES);

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

static platform_status
mini_validate_metadata_stream(cache *cc, uint64 meta_head, page_type type);

static platform_status
mini_validate_meta_page_contents(cache       *cc,
                                 page_handle *meta_page,
                                 uint64       expected_prev,
                                 bool32       check_prev,
                                 uint64       expected_page_no,
                                 bool32       check_page_no);

static platform_status
mini_metadata_corruption(const char *reason, uint64 addr);

static bool32
mini_metadata_valid_geometry(const allocator_config *cfg);

static bool32
mini_metadata_valid_page_addr(const allocator_config *cfg, uint64 addr);

static bool32
mini_metadata_valid_extent_addr(const allocator_config *cfg, uint64 addr);

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
mini_init_meta_page(mini_allocator *mini,
                    page_handle    *meta_page,
                    uint64          meta_page_no)
{
   mini_meta_hdr *hdr  = (mini_meta_hdr *)meta_page->data;
   hdr->next_meta_addr = 0;
   hdr->prev_meta_addr = 0;
   hdr->meta_page_no   = meta_page_no;
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
static platform_status
mini_full_lock_meta_tail(mini_allocator *mini, page_handle *meta_page)
{
   platform_assert(mini != NULL);
   /*
    * This loop follows the standard idiom for obtaining a claim. Note that
    * mini is shared, so meta_tail can change before we obtain the claim; check
    * it again after the claim succeeds.
    */
   uint64 wait = 1;
   while (1) {
      uint64 meta_tail = __atomic_load_n(&mini->meta_tail, __ATOMIC_ACQUIRE);
      platform_status rc =
         cache_get(mini->cc, meta_tail, TRUE, mini->meta_type, meta_page);
      if (!SUCCESS(rc)) {
         return rc;
      }
      if (!cache_try_claim(mini->cc, meta_page)) {
         cache_unget(mini->cc, meta_page);
      } else if (meta_tail
                 == __atomic_load_n(&mini->meta_tail, __ATOMIC_ACQUIRE))
      {
         break;
      } else {
         cache_unclaim(mini->cc, meta_page);
         cache_unget(mini->cc, meta_page);
      }
      platform_sleep_ns(wait);
      wait = wait > 1024 ? wait : 2 * wait;
   }
   cache_lock(mini->cc, meta_page);

   return STATUS_OK;
}

static void
mini_full_unlock_meta_page(mini_allocator *mini, page_handle *meta_page)
{
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
 *      STATUS_OK on success. If next_addr is non-NULL, stores the 0th batch's
 *      next address there.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
platform_status
mini_init(mini_allocator *mini,
          cache          *cc,
          uint64          meta_head,
          uint64          meta_tail,
          uint64          num_batches,
          page_type       type,
          uint64         *next_addr)
{
   page_type types[MINI_MAX_BATCHES];
   for (uint64 batch = 0; batch < num_batches; batch++) {
      types[batch] = type;
   }
   return mini_init_with_types(
      mini, cc, meta_head, meta_tail, num_batches, type, types, next_addr);
}

platform_status
mini_init_with_types(mini_allocator  *mini,
                     cache           *cc,
                     uint64           meta_head,
                     uint64           meta_tail,
                     uint64           num_batches,
                     page_type        meta_type,
                     const page_type *types,
                     uint64          *next_addr)
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

   page_handle  meta_storage;
   page_handle *meta_page = NULL;
   if (meta_tail == 0) {
      // new mini allocator
      mini->meta_tail = meta_head;
      meta_page = cache_alloc(cc, mini->meta_head, meta_type, &meta_storage);
      mini_init_meta_page(mini, meta_page, 0);

      // meta_page gets an extra ref
      refcount ref =
         allocator_inc_ref(mini->al, base_addr(cc, mini->meta_head));
      platform_assert(ref == MINI_NO_REFS + 1);

   } else {
      // load mini allocator
      mini->meta_tail = meta_tail;
   }

   uint64 batch = 0;
   for (; batch < num_batches; batch++) {
      // because we recover ref counts from the mini allocators on recovery, we
      // don't need to store these in the mini allocator until we consume them.
      platform_status rc = mini_allocator_get_new_extent(
         mini, mini->types[batch], &mini->next_extent[batch]);
      if (!SUCCESS(rc)) {
         for (uint64 initialized_batch = 0; initialized_batch < batch;
              initialized_batch++)
         {
            refcount ref =
               allocator_dec_ref(mini->al,
                                 mini->next_extent[initialized_batch],
                                 mini->types[initialized_batch]);
            platform_assert(ref == AL_NO_REFS);
            ref = allocator_dec_ref(mini->al,
                                    mini->next_extent[initialized_batch],
                                    mini->types[initialized_batch]);
            platform_assert(ref == AL_FREE);
         }
         if (meta_page != NULL) {
            mini_full_unlock_meta_page(mini, meta_page);
            refcount ref = allocator_dec_ref(
               mini->al, base_addr(cc, mini->meta_head), mini->meta_type);
            platform_assert(ref == AL_ONE_REF);
         }
         return rc;
      }
   }

   if (meta_page != NULL) {
      if (mini->pinned) {
         cache_pin(cc, meta_page);
      }
      mini_full_unlock_meta_page(mini, meta_page);
   }
   if (next_addr != NULL) {
      *next_addr = mini_next_addr(mini, 0);
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
   platform_assert(mini != NULL);
   platform_assert(mini->cc != NULL);
   platform_assert(meta_page != NULL);
   platform_assert(PAGE_TYPE_FIRST <= type && type < NUM_PAGE_TYPES);
   platform_assert(batch < mini->num_batches);

   uint64 page_size = cache_page_size(mini->cc);
   platform_assert(extent_addr != 0);
   platform_assert((extent_addr % page_size) == 0);

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
   uint64 next_addr =
      __atomic_load_n(&mini->next_addr[batch], __ATOMIC_ACQUIRE);
   uint64 wait = 1;
   while (next_addr == MINI_WAIT
          || !__sync_bool_compare_and_swap(
             &mini->next_addr[batch], next_addr, MINI_WAIT))
   {
      platform_sleep_ns(wait);
      wait      = wait > 1024 ? wait : 2 * wait;
      next_addr = __atomic_load_n(&mini->next_addr[batch], __ATOMIC_ACQUIRE);
   }
   return next_addr;
}

static void
mini_unlock_batch_set_next_addr(mini_allocator *mini,
                                uint64          batch,
                                uint64          next_addr)
{
   debug_assert(batch < mini->num_batches);
   debug_assert(__atomic_load_n(&mini->next_addr[batch], __ATOMIC_RELAXED)
                == MINI_WAIT);

   __atomic_store_n(&mini->next_addr[batch], next_addr, __ATOMIC_RELEASE);
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

static platform_status
mini_append_entry(mini_allocator *mini, uint64 batch, uint64 next_addr)
{
   page_handle     meta_storage;
   page_handle    *meta_page = &meta_storage;
   platform_status rc        = mini_full_lock_meta_tail(mini, meta_page);
   if (!SUCCESS(rc)) {
      return rc;
   }
   bool32       success;
   success = mini_append_entry_to_page(
      mini, meta_page, next_addr, mini->types[batch], batch);
   if (!success) {
      // need to allocate a new meta page
      uint64 old_meta_tail =
         __atomic_load_n(&mini->meta_tail, __ATOMIC_RELAXED);
      uint64 new_meta_tail = old_meta_tail + cache_page_size(mini->cc);
      if (new_meta_tail % cache_extent_size(mini->cc) == 0) {
         // need to allocate the next meta extent
         platform_status rc = mini_allocator_get_new_extent(
            mini, mini->meta_type, &new_meta_tail);
         if (!SUCCESS(rc)) {
            mini_full_unlock_meta_page(mini, meta_page);
            return rc;
         }
      }

      mini_set_next_meta_addr(mini, meta_page, new_meta_tail);

      page_handle    last_meta_storage = *meta_page;
      page_handle   *last_meta_page    = &last_meta_storage;
      uint64         last_meta_addr = old_meta_tail;
      mini_meta_hdr *last_meta_hdr  = (mini_meta_hdr *)meta_page->data;
      platform_assert(last_meta_hdr->meta_page_no != UINT64_MAX,
                      "mini allocator metadata page number overflow");
      uint64 new_meta_page_no = last_meta_hdr->meta_page_no + 1;
      meta_page =
         cache_alloc(mini->cc, new_meta_tail, mini->meta_type, &meta_storage);
      __atomic_store_n(&mini->meta_tail, new_meta_tail, __ATOMIC_RELEASE);
      mini_full_unlock_meta_page(mini, last_meta_page);
      mini_init_meta_page(mini, meta_page, new_meta_page_no);
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
   mini->cur_extent_meta_page[batch] =
      __atomic_load_n(&mini->meta_tail, __ATOMIC_RELAXED);
   mini_full_unlock_meta_page(mini, meta_page);
   return STATUS_OK;
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
         uint64          new_next_extent;
         platform_status rc = mini_allocator_get_new_extent(
            mini, mini->types[batch], &new_next_extent);
         if (!SUCCESS(rc)) {
            mini_unlock_batch_set_next_addr(mini, batch, next_addr);
            return rc;
         }
         next_addr = extent_addr;

         rc = mini_append_entry(mini, batch, next_addr);
         if (!SUCCESS(rc)) {
            refcount ref =
               allocator_dec_ref(mini->al, new_next_extent, mini->types[batch]);
            platform_assert(ref == AL_NO_REFS);
            ref =
               allocator_dec_ref(mini->al, new_next_extent, mini->types[batch]);
            platform_assert(ref == AL_FREE);
            __sync_fetch_and_sub(&mini->num_extents, 1);
            mini_unlock_batch_set_next_addr(mini, batch, next_addr);
            return rc;
         }
         mini->next_extent[batch] = new_next_extent;
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

platform_status
mini_alloc_page(mini_allocator *mini,
                uint64          batch,
                uint64         *addr,
                uint64         *next_extent)
{
   uint64          addrs[2]  = {0, 0};
   uint64          page_size = cache_page_size(mini->cc);
   platform_status rc        = mini_alloc_bytes(
      mini, batch, page_size, page_size, 0, addrs, next_extent);
   if (!SUCCESS(rc)) {
      return rc;
   }
   debug_assert(addrs[1] == 0);
   mini_alloc_bytes_finish(mini, batch);
   *addr = addrs[0];
   return STATUS_OK;
}

platform_status
mini_alloc_extent(mini_allocator *mini,
                  uint64          batch,
                  uint64         *addr,
                  uint64         *next_extent)
{
   uint64          addrs[2]    = {0, 0};
   uint64          extent_size = cache_extent_size(mini->cc);
   platform_status rc          = mini_alloc_bytes(
      mini, batch, extent_size, extent_size, 0, addrs, next_extent);
   if (!SUCCESS(rc)) {
      return rc;
   }
   debug_assert(addrs[1] == 0);
   mini_alloc_bytes_finish(mini, batch);
   *addr = addrs[0];
   return STATUS_OK;
}

platform_status
mini_attach_extent(mini_allocator *mini, uint64 batch, uint64 addr)
{
   debug_assert(batch < mini->num_batches);
   debug_assert(addr % cache_extent_size(mini->cc) == 0);

   uint64 old_next_addr = mini_lock_batch_get_next_addr(mini, batch);
   allocator_inc_ref(mini->al, addr);
   platform_status rc = mini_append_entry(mini, batch, addr);
   if (!SUCCESS(rc)) {
      (void)allocator_dec_ref(mini->al, addr, mini->types[batch]);
      mini_unlock_batch_set_next_addr(mini, batch, old_next_addr);
      return rc;
   }
   mini_unlock_batch_set_next_addr(mini, batch, old_next_addr);
   return STATUS_OK;
}

uint64
mini_next_addr(mini_allocator *mini, uint64 batch)
{
   debug_assert(batch < mini->num_batches);
   uint64 next_addr =
      __atomic_load_n(&mini->next_addr[batch], __ATOMIC_RELAXED);
   if ((next_addr % cache_extent_size(mini->cc)) != 0) {
      return next_addr;
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

static platform_status
mini_deinit(cache *cc, uint64 meta_head, page_type type)
{
   /* Metadata is immutable once its mini allocator is finalized. Validate the
    * complete chain before using links to release metadata extents. */
   platform_status rc = mini_validate_metadata_stream(cc, meta_head, type);
   if (!SUCCESS(rc)) {
      return rc;
   }

   allocator *al        = cache_get_allocator(cc);
   uint64     meta_addr = meta_head;
   do {
      page_handle  meta_storage;
      page_handle *meta_page = &meta_storage;
      rc                     = cache_get(cc, meta_addr, TRUE, type, meta_page);
      if (!SUCCESS(rc)) {
         return rc;
      }
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
   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 * mini_for_each_meta_page --
 *
 *      Walks the meta_page chain of the mini_allocator, calling func on each
 *      meta_page.
 *
 *      If before is non-NULL, it is called once per distinct meta extent --
 *      before that extent's first page is cache_get()'d -- with just the
 *      extent's address. This lets a caller that does not yet have allocator
 *      ownership of these pages (e.g. crash recovery, rebuilding the refcount
 *      table from scratch) establish it first, before cache_get()'s debug
 *      allocation checks run. Ordinary callers, which already own everything
 *      they are walking, pass NULL.
 *
 *      func and before may fail; the walk stops and returns the first failure.
 *
 * Results:
 *      STATUS_OK, the first failure from before/func, or STATUS_IO_ERROR if a
 *      meta page could not be read.
 *
 * Side effects:
 *      func/before may store output in arg.
 *-----------------------------------------------------------------------------
 */

typedef platform_status (*mini_meta_extent_fn)(cache    *cc,
                                               page_type type,
                                               uint64    extent_addr,
                                               void     *arg);

typedef platform_status (*mini_for_each_meta_page_fn)(cache       *cc,
                                                      page_type    type,
                                                      page_handle *meta_page,
                                                      void        *arg);

static platform_status
mini_for_each_meta_page(cache                     *cc,
                        uint64                     meta_head,
                        page_type                  type,
                        mini_meta_extent_fn        before,
                        mini_for_each_meta_page_fn func,
                        void                      *arg)
{
   if (cc == NULL || type < PAGE_TYPE_FIRST || type >= NUM_PAGE_TYPES) {
      return STATUS_BAD_PARAM;
   }
   allocator_config *cfg = allocator_get_config(cache_get_allocator(cc));
   if (!mini_metadata_valid_geometry(cfg)
       || !mini_metadata_valid_page_addr(cfg, meta_head))
   {
      return mini_metadata_corruption("metadata head is not a page", meta_head);
   }

   uint64 meta_addr        = meta_head;
   uint64 extent_size      = cache_extent_size(cc);
   uint64 prior_extent     = (uint64)-1;
   uint64 expected_prev    = 0;
   uint64 expected_page_no = 0;

   while (meta_addr != 0) {
      if (!mini_metadata_valid_page_addr(cfg, meta_addr)) {
         return mini_metadata_corruption("metadata chain names an invalid page",
                                         meta_addr);
      }
      if (before != NULL) {
         uint64 extent_addr = meta_addr - meta_addr % extent_size;
         if (!mini_metadata_valid_extent_addr(cfg, extent_addr)) {
            return mini_metadata_corruption("metadata extent is out of range",
                                            extent_addr);
         }
         if (extent_addr != prior_extent) {
            platform_status rc = before(cc, type, extent_addr, arg);
            if (!SUCCESS(rc)) {
               return rc;
            }
            prior_extent = extent_addr;
         }
      }

      page_handle     meta_storage;
      page_handle    *meta_page = &meta_storage;
      platform_status rc = cache_get(cc, meta_addr, TRUE, type, meta_page);
      if (!SUCCESS(rc)) {
         return rc;
      }

      if (meta_page->disk_addr != meta_addr) {
         cache_unget(cc, meta_page);
         return mini_metadata_corruption("metadata page address mismatch",
                                         meta_addr);
      }

      rc = mini_validate_meta_page_contents(
         cc, meta_page, expected_prev, TRUE, expected_page_no, TRUE);
      if (SUCCESS(rc) && func != NULL) {
         rc = func(cc, type, meta_page, arg);
      }
      uint64 next_meta_addr =
         SUCCESS(rc) ? mini_get_next_meta_addr(meta_page) : 0;
      cache_unget(cc, meta_page);
      if (!SUCCESS(rc)) {
         return rc;
      }
      if (next_meta_addr != 0) {
         if (expected_page_no == UINT64_MAX) {
            return mini_metadata_corruption("metadata page number wraps",
                                            meta_addr);
         }
         expected_page_no++;
      }
      expected_prev = meta_addr;
      meta_addr     = next_meta_addr;
   }
   return STATUS_OK;
}

static platform_status
mini_validate_metadata_stream(cache *cc, uint64 meta_head, page_type type)
{
   return mini_for_each_meta_page(cc, meta_head, type, NULL, NULL, NULL);
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

static platform_status
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
   return STATUS_OK;
}

static platform_status
mini_for_each(cache           *cc,
              uint64           meta_head,
              page_type        type,
              mini_for_each_fn func,
              void            *out)
{
   for_each_func fef = {func, out};
   return mini_for_each_meta_page(
      cc, meta_head, type, NULL, mini_for_each_meta_page_func, &fef);
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

   /* The external reference was consumed even if reclamation fails. Never
    * retry this root: an earlier pass may already have decremented shared
    * extents or released metadata pages. Keep the remaining space allocated
    * until recovery rebuilds the map from reachable roots. */
   platform_status rc = mini_validate_metadata_stream(cc, meta_head, type);
   if (SUCCESS(rc)) {
      rc = mini_for_each(cc, meta_head, type, mini_dealloc_extent, NULL);
   }
   if (SUCCESS(rc)) {
      rc = mini_deinit(cc, meta_head, type);
   }
   if (!SUCCESS(rc)) {
      allocator_record_reclamation_failure(al, rc);
      platform_error_log(
         "WARNING: abandoned reclamation of mini allocator at %lu (type %s): "
         "%s. Remaining space is unavailable until reopen; clean allocation "
         "state will not be written. Recovery is required.\n",
         meta_head,
         page_type_str[type],
         platform_status_to_string(rc));
   }
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
   platform_status rc =
      mini_for_each(cc, meta_head, type, mini_prefetch_extent, NULL);
   if (!SUCCESS(rc)) {
      platform_error_log("mini_prefetch: invalid metadata stream: %s\n",
                         platform_status_to_string(rc));
   }
}

/*
 * -----------------------------------------------------------------------------
 * mini_recover_allocations -- crash-recovery allocator-reference rebuild.
 * -----------------------------------------------------------------------------
 */

static platform_status
mini_metadata_corruption(const char *reason, uint64 addr)
{
   platform_error_log(
      "Malformed mini allocator metadata: %s (addr=%lu).\n", reason, addr);
   return STATUS_INVALID_STATE;
}

static bool32
mini_metadata_valid_geometry(const allocator_config *cfg)
{
   if (cfg == NULL || cfg->io_cfg == NULL || cfg->capacity == 0
       || cfg->io_cfg->page_size == 0 || cfg->io_cfg->extent_size == 0
       || cfg->io_cfg->page_size < offsetof(mini_meta_hdr, entry_buffer)
       || cfg->io_cfg->extent_size < cfg->io_cfg->page_size
       || cfg->io_cfg->extent_size % cfg->io_cfg->page_size != 0
       || cfg->capacity % cfg->io_cfg->extent_size != 0)
   {
      return FALSE;
   }

   return TRUE;
}

static bool32
mini_metadata_valid_page_addr(const allocator_config *cfg, uint64 addr)
{
   uint64 page_size = cfg->io_cfg->page_size;

   return addr != 0 && addr % page_size == 0
          && addr <= cfg->capacity - page_size;
}

static bool32
mini_metadata_valid_extent_addr(const allocator_config *cfg, uint64 addr)
{
   uint64 extent_size = cfg->io_cfg->extent_size;

   return addr != 0 && addr % extent_size == 0
          && addr <= cfg->capacity - extent_size;
}

static platform_status
mini_validate_meta_header(const mini_meta_hdr *hdr,
                          uint64               page_size,
                          uint64               expected_prev,
                          bool32               check_prev,
                          uint64               expected_page_no,
                          bool32               check_page_no,
                          uint64               meta_addr)
{
   uint64 first_entry_offset = offsetof(mini_meta_hdr, entry_buffer);
   uint64 max_entries = (page_size - first_entry_offset) / sizeof(meta_entry);
   if (check_prev && hdr->prev_meta_addr != expected_prev) {
      return mini_metadata_corruption("metadata prev link is not reciprocal",
                                      meta_addr);
   }
   if (check_page_no && hdr->meta_page_no != expected_page_no) {
      return mini_metadata_corruption("metadata page number is not contiguous",
                                      meta_addr);
   }
   if ((hdr->meta_page_no == 0) != (hdr->prev_meta_addr == 0)) {
      return mini_metadata_corruption(
         "metadata page number disagrees with predecessor link", meta_addr);
   }

   if (hdr->num_entries > max_entries) {
      return mini_metadata_corruption("metadata entry count exceeds page",
                                      meta_addr);
   }

   uint64 expected_pos =
      first_entry_offset + (uint64)hdr->num_entries * sizeof(meta_entry);
   if (hdr->pos != expected_pos) {
      return mini_metadata_corruption("metadata entry position is invalid",
                                      meta_addr);
   }

   return STATUS_OK;
}

static platform_status
mini_validate_next_meta_addr(const allocator_config *cfg,
                             uint64                  meta_addr,
                             uint64                  next_meta_addr)
{
   if (next_meta_addr == 0) {
      return STATUS_OK;
   }

   if (!mini_metadata_valid_page_addr(cfg, next_meta_addr)) {
      return mini_metadata_corruption("metadata next link is not a page",
                                      next_meta_addr);
   }

   uint64 extent_size = cfg->io_cfg->extent_size;
   uint64 page_size   = cfg->io_cfg->page_size;
   uint64 offset      = meta_addr % extent_size;
   if (offset == extent_size - page_size) {
      if (next_meta_addr % extent_size != 0) {
         return mini_metadata_corruption(
            "metadata extent transition does not start at an extent base",
            next_meta_addr);
      }
   } else if (next_meta_addr != meta_addr + page_size) {
      return mini_metadata_corruption(
         "metadata pages do not advance contiguously within an extent",
         next_meta_addr);
   }

   return STATUS_OK;
}

static platform_status
mini_validate_prev_meta_addr(const allocator_config *cfg,
                             uint64                  meta_addr,
                             uint64                  prev_meta_addr)
{
   if (prev_meta_addr == 0) {
      return STATUS_OK;
   }
   if (!mini_metadata_valid_page_addr(cfg, prev_meta_addr)) {
      return mini_metadata_corruption("metadata prev link is not a page",
                                      prev_meta_addr);
   }

   uint64 extent_size = cfg->io_cfg->extent_size;
   uint64 page_size   = cfg->io_cfg->page_size;
   uint64 offset      = meta_addr % extent_size;
   if (offset == 0) {
      if (prev_meta_addr % extent_size != extent_size - page_size) {
         return mini_metadata_corruption(
            "metadata extent transition has an invalid predecessor",
            prev_meta_addr);
      }
   } else if (prev_meta_addr != meta_addr - page_size) {
      return mini_metadata_corruption(
         "metadata pages do not retreat contiguously within an extent",
         prev_meta_addr);
   }
   return STATUS_OK;
}

static platform_status
mini_validate_meta_entry(const allocator_config *cfg,
                         const meta_entry       *entry,
                         uint64                  meta_addr)
{
   uint64    batch       = meta_entry_batch(entry);
   page_type extent_type = meta_entry_type(entry);
   uint64    extent_number =
      entry->packed >> (META_ENTRY_BATCH_BITS + META_ENTRY_TYPE_BITS);

   if (batch >= MINI_MAX_BATCHES || extent_type < PAGE_TYPE_FIRST
       || extent_type >= NUM_PAGE_TYPES || extent_number == 0
       || extent_number > UINT64_MAX / cfg->io_cfg->extent_size)
   {
      return mini_metadata_corruption("metadata extent entry is invalid",
                                      meta_addr);
   }

   uint64 extent_addr = extent_number * cfg->io_cfg->extent_size;
   return mini_metadata_valid_extent_addr(cfg, extent_addr)
             ? STATUS_OK
             : mini_metadata_corruption("metadata extent is out of range",
                                        extent_addr);
}

static platform_status
mini_validate_meta_page_contents(cache       *cc,
                                 page_handle *meta_page,
                                 uint64       expected_prev,
                                 bool32       check_prev,
                                 uint64       expected_page_no,
                                 bool32       check_page_no)
{
   allocator_config *cfg = allocator_get_config(cache_get_allocator(cc));
   if (meta_page == NULL) {
      return mini_metadata_corruption("metadata page is invalid", 0);
   }

   uint64          meta_addr = meta_page->disk_addr;
   mini_meta_hdr  *hdr       = (mini_meta_hdr *)meta_page->data;
   platform_status rc        = mini_validate_meta_header(hdr,
                                                  cfg->io_cfg->page_size,
                                                  expected_prev,
                                                  check_prev,
                                                  expected_page_no,
                                                  check_page_no,
                                                  meta_addr);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = mini_validate_next_meta_addr(cfg, meta_addr, hdr->next_meta_addr);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = mini_validate_prev_meta_addr(cfg, meta_addr, hdr->prev_meta_addr);
   if (!SUCCESS(rc)) {
      return rc;
   }

   meta_entry *entry = first_entry(meta_page);
   for (uint64 i = 0; i < hdr->num_entries; i++) {
      rc = mini_validate_meta_entry(cfg, entry, meta_addr);
      if (!SUCCESS(rc)) {
         return rc;
      }
      entry = next_entry(entry);
   }
   return STATUS_OK;
}

/*
 * Validate only the fields a cursor needs before it starts accessing entries.
 * Entries and links are checked individually when the cursor uses them, which
 * avoids a full metadata-page scan every time a range iterator is initialized.
 */
static platform_status
mini_meta_cursor_validate_page(mini_meta_cursor *cursor,
                               page_handle      *meta_page,
                               uint64            expected_prev,
                               bool32            check_prev,
                               uint64            expected_page_no,
                               bool32            check_page_no)
{
   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;
   return mini_validate_meta_header(hdr,
                                    cache_page_size(cursor->cc),
                                    expected_prev,
                                    check_prev,
                                    expected_page_no,
                                    check_page_no,
                                    meta_page->disk_addr);
}

static platform_status
mini_meta_cursor_validate_entry(mini_meta_cursor *cursor, uint64 entry_idx)
{
   if (entry_idx >= cursor->num_entries) {
      return STATUS_INVALID_STATE;
   }
   allocator_config *cfg =
      allocator_get_config(cache_get_allocator(cursor->cc));
   meta_entry *entry = first_entry(&cursor->meta_page) + entry_idx;
   return mini_validate_meta_entry(cfg, entry, cursor->meta_addr);
}

typedef struct mini_recover_state {
   allocator        *al;
   allocator_config *cfg;
} mini_recover_state;

/*
 * The "before" hook mini_for_each_meta_page() calls once per distinct meta
 * extent, before that extent's first page is cache_get()'d. Recording the
 * reference here -- ahead of the read -- is what lets a rebuilding allocator
 * (which does not yet consider this extent allocated) satisfy cache_get()'s
 * debug allocation check.
 */
static platform_status
mini_recover_visit_meta_extent(cache    *cc,
                               page_type type,
                               uint64    extent_addr,
                               void     *arg)
{
   mini_recover_state *state = (mini_recover_state *)arg;

   if (!mini_metadata_valid_extent_addr(state->cfg, extent_addr)) {
      return mini_metadata_corruption("metadata extent is out of range",
                                      extent_addr);
   }
   return allocator_recovery_record_reference(state->al, extent_addr, type);
}

/*
 * The "func" hook mini_for_each_meta_page() calls once per meta page, with the
 * page held. Validates the page and every data-extent entry on it before
 * trusting any of it, and records a reference for each data extent found (its
 * type is self-describing on disk, unlike a meta page's).
 */
static platform_status
mini_recover_visit_meta_page(cache       *cc,
                             page_type    type,
                             page_handle *meta_page,
                             void        *arg)
{
   mini_recover_state *state = (mini_recover_state *)arg;
   (void)type;

   mini_meta_hdr *hdr = (mini_meta_hdr *)meta_page->data;

   meta_entry *entry = first_entry(meta_page);
   for (uint64 entry_no = 0; entry_no < hdr->num_entries; entry_no++) {
      page_type extent_type = meta_entry_type(entry);
      uint64    extent_addr = meta_entry_extent_addr(cc, entry);

      platform_status rc = allocator_recovery_record_reference(
         state->al, extent_addr, extent_type);
      if (!SUCCESS(rc)) {
         return rc;
      }

      entry = next_entry(entry);
   }

   return STATUS_OK;
}

/*
 * mini_recover_allocations --
 *
 *     Rebuild allocator references for a finalized mini allocator without
 *     trusting allocator refcounts -- the discovery primitive crash recovery
 *     uses to reconstruct them. Records one reference for every metadata
 *     extent and every data-extent entry found in the on-disk mini metadata
 *     stream (data entries are not deduplicated: repeated entries mean
 *     repeated references, and are recorded exactly as often as they occur).
 *
 *     meta_type is the page type of meta_head's own chain; each data extent's
 *     type is read from its own metadata entry.
 *
 *     Validates the metadata-page chain, page-header bounds, page types,
 *     batches, and extent addresses before using them. A malformed on-disk
 *     stream returns STATUS_INVALID_STATE. This function performs no writes
 *     and does not use allocator refcounts to decide what to traverse.
 *
 *     This is a physical enumeration only. Recovering logical reference
 *     multiplicity, and deduplicating references shared by distinct mini
 *     allocator roots, remains the responsibility of the higher-level
 *     trunk/log recovery walker.
 */
static platform_status
mini_recover_allocations(cache *cc, uint64 meta_head, page_type meta_type)
{
   if (cc == NULL || meta_type < PAGE_TYPE_FIRST || meta_type >= NUM_PAGE_TYPES)
   {
      return STATUS_BAD_PARAM;
   }

   allocator *al = cache_get_allocator(cc);
   if (al == NULL) {
      return STATUS_BAD_PARAM;
   }
   allocator_config *cfg = allocator_get_config(al);
   if (!mini_metadata_valid_geometry(cfg)) {
      return STATUS_BAD_PARAM;
   }
   if (!mini_metadata_valid_page_addr(cfg, meta_head)) {
      return mini_metadata_corruption("metadata head is not a page", meta_head);
   }

   mini_recover_state state = {.al = al, .cfg = cfg};

   return mini_for_each_meta_page(cc,
                                  meta_head,
                                  meta_type,
                                  mini_recover_visit_meta_extent,
                                  mini_recover_visit_meta_page,
                                  &state);
}

platform_status
mini_recover_references(cache *cc, uint64 meta_head, page_type type)
{
   allocator *al   = cache_get_allocator(cc);
   uint64     base = base_addr(cc, meta_head);

   if (allocator_get_refcount(al, base) == AL_FREE) {
      platform_status rc = mini_recover_allocations(cc, meta_head, type);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   /*
    * Every holder adds one, the first one included.  Enumerating the extents
    * accounts for the mini allocator existing -- it leaves the metadata extent
    * at MINI_NO_REFS, which is what mini_init_with_types() establishes with its
    * "meta_page gets an extra ref" -- and says nothing about who refers to it.
    * External references are the count above that, so each is recorded here.
    */
   return allocator_recovery_record_reference(al, base, type);
}

/*
 *-----------------------------------------------------------------------------
 * mini_meta_cursor -- cursor over a mini_allocator's extent entries.
 *-----------------------------------------------------------------------------
 */
mini_meta_cursor_status
mini_meta_cursor_init(mini_meta_cursor *cursor,
                      cache            *cc,
                      page_type         meta_type,
                      uint64            meta_addr,
                      uint64            target_extent_addr)
{
   if (cursor == NULL || cc == NULL || meta_type < PAGE_TYPE_FIRST
       || meta_type >= NUM_PAGE_TYPES)
   {
      return MINI_META_CURSOR_INVALID;
   }

   cursor->cc           = cc;
   cursor->meta_type    = meta_type;
   cursor->meta_page    = (page_handle){0};
   cursor->meta_addr    = meta_addr;
   cursor->meta_page_no = 0;
   cursor->entry_idx    = 0;
   cursor->num_entries  = 0;

   allocator_config *cfg = allocator_get_config(cache_get_allocator(cc));
   if (!mini_metadata_valid_geometry(cfg)
       || !mini_metadata_valid_page_addr(cfg, meta_addr)
       || !mini_metadata_valid_extent_addr(cfg, target_extent_addr))
   {
      return MINI_META_CURSOR_INVALID;
   }

   platform_status rc = cache_get(cursor->cc,
                                  cursor->meta_addr,
                                  FALSE,
                                  cursor->meta_type,
                                  &cursor->meta_page);
   if (STATUS_IS_EQ(rc, STATUS_BUSY)) {
      cache_prefetch_page(cursor->cc, cursor->meta_addr, cursor->meta_type);
      return MINI_META_CURSOR_WOULD_BLOCK;
   }

   if (!SUCCESS(rc)) {
      return MINI_META_CURSOR_INVALID;
   }
   rc = mini_meta_cursor_validate_page(
      cursor, &cursor->meta_page, 0, FALSE, 0, FALSE);
   if (!SUCCESS(rc)) {
      cache_unget(cursor->cc, &cursor->meta_page);
      cursor->meta_page = (page_handle){0};
      return MINI_META_CURSOR_INVALID;
   }

   mini_meta_hdr *hdr          = (mini_meta_hdr *)cursor->meta_page.data;
   cursor->meta_page_no        = hdr->meta_page_no;
   cursor->num_entries         = mini_num_entries(&cursor->meta_page);
   cursor->entry_idx           = 0;
   uint64 target_extent_number = target_extent_addr / cfg->io_cfg->extent_size;
   while (cursor->entry_idx < cursor->num_entries) {
      meta_entry *entry = first_entry(&cursor->meta_page) + cursor->entry_idx;
      uint64      extent_number =
         entry->packed >> (META_ENTRY_BATCH_BITS + META_ENTRY_TYPE_BITS);
      if (extent_number == target_extent_number) {
         /* The scan only compares fixed-width extent numbers. Validate the
          * matching entry before exposing its remaining fields through curr;
          * entries skipped by the search are validated if next/prev later
          * moves the cursor onto them. */
         rc = mini_meta_cursor_validate_entry(cursor, cursor->entry_idx);
         if (!SUCCESS(rc)) {
            cache_unget(cursor->cc, &cursor->meta_page);
            cursor->meta_page = (page_handle){0};
            return MINI_META_CURSOR_INVALID;
         }
         return MINI_META_CURSOR_ENTRY;
      }
      cursor->entry_idx++;
   }

   cache_unget(cursor->cc, &cursor->meta_page);
   cursor->meta_page = (page_handle){0};
   return MINI_META_CURSOR_INVALID;
}

void
mini_meta_cursor_deinit(mini_meta_cursor *cursor)
{
   platform_assert(cursor != NULL);

   if (cursor->meta_page.data != NULL) {
      cache_unget(cursor->cc, &cursor->meta_page);
      cursor->meta_page = (page_handle){0};
   }
   cursor->meta_addr    = 0;
   cursor->meta_page_no = 0;
   cursor->entry_idx    = 0;
   cursor->num_entries  = 0;
}

void
mini_meta_cursor_curr(mini_meta_cursor *cursor,
                      uint64           *extent_addr,
                      uint64           *batch)
{
   platform_assert(cursor != NULL);
   platform_assert(cursor->cc != NULL);
   platform_assert(extent_addr != NULL);
   platform_assert(batch != NULL);
   platform_assert(PAGE_TYPE_FIRST <= cursor->meta_type
                   && cursor->meta_type < NUM_PAGE_TYPES);
   platform_assert(cursor->meta_page.data != NULL);
   platform_assert(cursor->entry_idx < cursor->num_entries);

   meta_entry *entry = first_entry(&cursor->meta_page) + cursor->entry_idx;
   *extent_addr      = meta_entry_extent_addr(cursor->cc, entry);
   *batch            = meta_entry_batch(entry);
}

mini_meta_cursor_status
mini_meta_cursor_next(mini_meta_cursor *cursor)
{
   if (cursor == NULL || cursor->cc == NULL || cursor->meta_page.data == NULL
       || cursor->entry_idx >= cursor->num_entries)
   {
      return MINI_META_CURSOR_INVALID;
   }

   if (cursor->entry_idx + 1 < cursor->num_entries) {
      if (!SUCCESS(
             mini_meta_cursor_validate_entry(cursor, cursor->entry_idx + 1)))
      {
         return MINI_META_CURSOR_INVALID;
      }
      cursor->entry_idx++;
      return MINI_META_CURSOR_ENTRY;
   }

   uint64 next_meta_addr = mini_get_next_meta_addr(&cursor->meta_page);
   if (next_meta_addr == 0) {
      return MINI_META_CURSOR_END;
   }
   if (cursor->meta_page_no == UINT64_MAX) {
      return MINI_META_CURSOR_INVALID;
   }

   allocator_config *cfg =
      allocator_get_config(cache_get_allocator(cursor->cc));
   if (!SUCCESS(
          mini_validate_next_meta_addr(cfg, cursor->meta_addr, next_meta_addr)))
   {
      return MINI_META_CURSOR_INVALID;
   }

   page_handle     next_page_storage;
   page_handle    *next_page = &next_page_storage;
   platform_status rc        = cache_get(
      cursor->cc, next_meta_addr, FALSE, cursor->meta_type, next_page);
   if (STATUS_IS_EQ(rc, STATUS_BUSY)) {
      cache_prefetch_page(cursor->cc, next_meta_addr, cursor->meta_type);
      return MINI_META_CURSOR_WOULD_BLOCK;
   }

   if (!SUCCESS(rc)) {
      return MINI_META_CURSOR_INVALID;
   }
   rc                      = mini_meta_cursor_validate_page(cursor,
                                       next_page,
                                       cursor->meta_addr,
                                       TRUE,
                                       cursor->meta_page_no + 1,
                                       TRUE);
   uint64 next_num_entries = SUCCESS(rc) ? mini_num_entries(next_page) : 0;
   if (!SUCCESS(rc) || next_num_entries == 0) {
      cache_unget(cursor->cc, next_page);
      return MINI_META_CURSOR_INVALID;
   }

   cache_unget(cursor->cc, &cursor->meta_page);
   cursor->meta_page = *next_page;
   cursor->meta_addr = next_meta_addr;
   cursor->meta_page_no++;
   cursor->num_entries = next_num_entries;
   cursor->entry_idx   = 0;
   if (!SUCCESS(mini_meta_cursor_validate_entry(cursor, 0))) {
      return MINI_META_CURSOR_INVALID;
   }
   return MINI_META_CURSOR_ENTRY;
}

mini_meta_cursor_status
mini_meta_cursor_prev(mini_meta_cursor *cursor)
{
   if (cursor == NULL || cursor->cc == NULL || cursor->meta_page.data == NULL
       || cursor->entry_idx >= cursor->num_entries)
   {
      return MINI_META_CURSOR_INVALID;
   }

   if (cursor->entry_idx > 0) {
      if (!SUCCESS(
             mini_meta_cursor_validate_entry(cursor, cursor->entry_idx - 1)))
      {
         return MINI_META_CURSOR_INVALID;
      }
      cursor->entry_idx--;
      return MINI_META_CURSOR_ENTRY;
   }

   mini_meta_hdr *hdr       = (mini_meta_hdr *)cursor->meta_page.data;
   uint64         prev_addr = hdr->prev_meta_addr;
   if (prev_addr == 0) {
      return MINI_META_CURSOR_END;
   }
   if (cursor->meta_page_no == 0) {
      return MINI_META_CURSOR_INVALID;
   }

   allocator_config *cfg =
      allocator_get_config(cache_get_allocator(cursor->cc));
   if (!SUCCESS(
          mini_validate_prev_meta_addr(cfg, cursor->meta_addr, prev_addr)))
   {
      return MINI_META_CURSOR_INVALID;
   }

   page_handle     prev_page_storage;
   page_handle    *prev_page = &prev_page_storage;
   platform_status rc =
      cache_get(cursor->cc, prev_addr, FALSE, cursor->meta_type, prev_page);
   if (STATUS_IS_EQ(rc, STATUS_BUSY)) {
      cache_prefetch_page(cursor->cc, prev_addr, cursor->meta_type);
      return MINI_META_CURSOR_WOULD_BLOCK;
   }

   if (!SUCCESS(rc)) {
      return MINI_META_CURSOR_INVALID;
   }
   rc = mini_meta_cursor_validate_page(
      cursor, prev_page, 0, FALSE, cursor->meta_page_no - 1, TRUE);
   mini_meta_hdr *prev_hdr = (mini_meta_hdr *)prev_page->data;
   uint64 prev_num_entries = SUCCESS(rc) ? mini_num_entries(prev_page) : 0;
   if (SUCCESS(rc)) {
      rc = mini_validate_next_meta_addr(
         cfg, prev_page->disk_addr, prev_hdr->next_meta_addr);
   }
   if (!SUCCESS(rc) || prev_hdr->next_meta_addr != cursor->meta_addr
       || prev_num_entries == 0)
   {
      cache_unget(cursor->cc, prev_page);
      return MINI_META_CURSOR_INVALID;
   }

   cache_unget(cursor->cc, &cursor->meta_page);
   cursor->meta_page = *prev_page;
   cursor->meta_addr = prev_addr;
   cursor->meta_page_no--;
   cursor->num_entries = prev_num_entries;
   cursor->entry_idx   = cursor->num_entries - 1;
   if (!SUCCESS(mini_meta_cursor_validate_entry(cursor, cursor->entry_idx))) {
      return MINI_META_CURSOR_INVALID;
   }
   return MINI_META_CURSOR_ENTRY;
}

static void
space_use_add_extent(cache *cc, page_type type, uint64 extent_addr, void *out)
{
   uint64 *sum = (uint64 *)out;
   *sum += cache_extent_size(cc);
}

static platform_status
space_use_add_meta_page(cache       *cc,
                        page_type    type,
                        page_handle *meta_page,
                        void        *out)
{
   uint64 *sum = (uint64 *)out;
   *sum += cache_page_size(cc);
   return STATUS_OK;
}

platform_status
mini_space_use_bytes(cache *cc, uint64 meta_head, page_type type, uint64 *bytes)
{
   uint64          total = 0;
   platform_status rc =
      mini_for_each(cc, meta_head, type, space_use_add_extent, &total);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = mini_for_each_meta_page(
      cc, meta_head, type, NULL, space_use_add_meta_page, &total);
   if (!SUCCESS(rc)) {
      return rc;
   }
   *bytes = total;
   return STATUS_OK;
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
   platform_status rc = mini_validate_metadata_stream(cc, meta_head, type);
   if (!SUCCESS(rc)) {
      platform_default_log("invalid mini allocator metadata: %s\n",
                           platform_status_to_string(rc));
      return;
   }
   uint64 next_meta_addr = meta_head;

   platform_default_log("---------------------------------------------\n");
   platform_default_log("| Mini Allocator -- meta_head: %12lu |\n", meta_head);
   platform_default_log("|-------------------------------------------|\n");
   platform_default_log("| idx | %35s |\n", "extent_addr");
   platform_default_log("|-------------------------------------------|\n");

   do {
      page_handle  meta_storage;
      page_handle *meta_page = &meta_storage;
      rc = cache_get(cc, next_meta_addr, TRUE, type, meta_page);
      if (!SUCCESS(rc)) {
         platform_default_log("cannot read mini allocator page %lu: %s\n",
                              next_meta_addr,
                              platform_status_to_string(rc));
         return;
      }

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
