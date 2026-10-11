// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "blob.h"
#include "platform_sleep.h"
#include "poison.h"

#define MIN_LIVE_PERCENTAGE (90ULL)

static platform_status
blob_get_descriptor(slice sblob, const blob **blobby)
{
   if (slice_length(sblob) < sizeof(blob)) {
      return STATUS_INVALID_STATE;
   }

   const blob *candidate = slice_data(sblob);
   if (candidate->format != BLOB_FORMAT) {
      return STATUS_INVALID_STATE;
   }

   *blobby = candidate;
   return STATUS_OK;
}

/* If the data is large enough (or close enough to a whole number of
 * rounded_size pieces), then we just put it entirely into
 * rounded_size pieces, since this won't waste too much space.
 */
bool
can_round_up(uint64 rounded_size, uint64 length)
{
   if (rounded_size == 0) {
      return FALSE;
   }
   if (length == 0) {
      return TRUE;
   }
   uint64 covering_rounded_count =
      length / rounded_size + (length % rounded_size != 0);
   __uint128_t allocated = (__uint128_t)covering_rounded_count * rounded_size;
   return (__uint128_t)MIN_LIVE_PERCENTAGE * allocated
          <= (__uint128_t)100 * length;
}

static bool32
blob_range_is_valid(uint64 capacity,
                    uint64 extent_size,
                    uint64 addr,
                    uint64 len)
{
   return extent_size <= addr && addr < capacity && len <= capacity - addr;
}

static bool32
blob_extent_is_valid(cache *cc, uint64 addr)
{
   uint64 extent_size = cache_extent_size(cc);
   uint64 capacity    = allocator_get_config(cache_get_allocator(cc))->capacity;
   return addr % extent_size == 0
          && blob_range_is_valid(capacity, extent_size, addr, extent_size);
}

/* Cache reads assert on unallocated addresses and short reads. Recovery must
 * turn missing pointer pages into truncation before reaching that interface. */
static platform_status
blob_get_pointer_page(cache        *cc,
                      uint64        addr,
                      bool32        disk_only,
                      page_handle **page)
{
   if (!allocator_page_valid(cache_get_allocator(cc), addr)) {
      return STATUS_INVALID_STATE;
   }
   if (disk_only) {
      bool32          readable;
      platform_status rc =
         cache_range_is_readable(cc, addr, cache_page_size(cc), &readable);
      if (!SUCCESS(rc)) {
         return rc;
      }
      if (!readable) {
         return STATUS_IO_ERROR;
      }
   }
   *page = cache_get(cc, addr, TRUE, PAGE_TYPE_BLOB);
   return *page == NULL ? STATUS_IO_ERROR : STATUS_OK;
}

platform_status
blob_parse_descriptor(cache *cc, slice sblob, parsed_blob *pblobby)
{
   if (cc == NULL || pblobby == NULL) {
      return STATUS_BAD_PARAM;
   }

   const blob     *blobby;
   platform_status rc = blob_get_descriptor(sblob, &blobby);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64 address_bytes = slice_length(sblob) - sizeof(blob);
   if (address_bytes % sizeof(uint64) != 0) {
      return STATUS_INVALID_STATE;
   }
   uint64            available_addrs = address_bytes / sizeof(uint64);
   uint64            extent_size     = cache_extent_size(cc);
   uint64            page_size       = cache_page_size(cc);
   allocator_config *allocator_cfg =
      allocator_get_config(cache_get_allocator(cc));
   uint64 capacity = allocator_cfg->capacity;

   if (page_size == 0 || page_size % sizeof(uint64) != 0 || extent_size == 0
       || extent_size % page_size != 0 || capacity < extent_size
       || extent_size / sizeof(uint64) < 2 || blobby->length > capacity)
   {
      return STATUS_INVALID_STATE;
   }

   memset(pblobby, 0, sizeof(*pblobby));
   pblobby->base        = blobby;
   pblobby->height      = 1;
   pblobby->extent_span = 1;
   uint64 remainder     = blobby->length;
   uint64 entry         = 0;

   if (can_round_up(extent_size, remainder)) {
      pblobby->num_extents =
         remainder / extent_size + (remainder % extent_size != 0);
      uint64 fanout = extent_size / sizeof(uint64);
      while (pblobby->num_extents > available_addrs) {
         if (available_addrs == 0 || pblobby->num_extents == 1
             || pblobby->height == BLOB_MAX_HEIGHT
             || pblobby->extent_span > UINT64_MAX / fanout)
         {
            return STATUS_INVALID_STATE;
         }
         pblobby->num_extents = pblobby->num_extents / fanout
                                + (pblobby->num_extents % fanout != 0);
         pblobby->extent_span *= fanout;
         pblobby->height++;
      }
      entry = pblobby->num_extents;
   } else {
      pblobby->num_extents = remainder / extent_size;
      remainder -= pblobby->num_extents * extent_size;
      entry                  = pblobby->num_extents;
      uint64 leftovers_entry = 0;
      while (page_size <= remainder) {
         if (entry >= available_addrs
             || leftovers_entry >= ARRAY_SIZE(pblobby->leftovers))
         {
            return STATUS_INVALID_STATE;
         }
         uint64 addr       = blobby->addrs[entry];
         uint64 max_length = extent_size - (addr % extent_size);
         uint64 length;
         if (max_length <= remainder) {
            length = max_length;
         } else if (can_round_up(page_size, blobby->length)) {
            length = remainder;
         } else {
            length = remainder - (remainder % page_size);
         }
         if (length == 0
             || !blob_range_is_valid(capacity, extent_size, addr, length))
         {
            return STATUS_INVALID_STATE;
         }
         pblobby->leftovers[leftovers_entry].addr   = addr;
         pblobby->leftovers[leftovers_entry].length = length;
         remainder -= length;
         entry++;
         leftovers_entry++;
      }

      if (remainder) {
         if (entry >= available_addrs
             || leftovers_entry >= ARRAY_SIZE(pblobby->leftovers))
         {
            return STATUS_INVALID_STATE;
         }
         uint64 addr = blobby->addrs[entry];
         if (!blob_range_is_valid(capacity, extent_size, addr, remainder)) {
            return STATUS_INVALID_STATE;
         }
         pblobby->leftovers[leftovers_entry].addr   = addr;
         pblobby->leftovers[leftovers_entry].length = remainder;
         entry++;
      }
   }

   if (entry != available_addrs) {
      return STATUS_INVALID_STATE;
   }

   for (uint64 i = 0; i < pblobby->num_extents; i++) {
      uint64 addr = pblobby->base->addrs[i];
      if (addr % extent_size != 0
          || !blob_range_is_valid(capacity, extent_size, addr, extent_size))
      {
         return STATUS_INVALID_STATE;
      }
   }
   return STATUS_OK;
}

uint64
blob_length(slice sblobby)
{
   const blob *blobby = slice_data(sblobby);
   debug_assert(sizeof(*blobby) <= slice_length(sblobby));
   debug_assert(blobby->format == BLOB_FORMAT);
   return blobby->length;
}

static void
flat_fragment_for_offset(uint64             extent_size,
                         uint64             page_size,
                         const parsed_blob *pblobby,
                         uint64             offset,
                         page_fragment     *fragment)
{
   uint64 byte_addr       = 0;
   uint64 entry_remainder = 0;
   debug_assert(offset < pblobby->base->length);

   if (offset / extent_size < pblobby->num_extents) {
      byte_addr =
         pblobby->base->addrs[offset / extent_size] + (offset % extent_size);
      entry_remainder = MIN(pblobby->base->length - offset,
                            extent_size - (offset % extent_size));
   } else {
      offset -= pblobby->num_extents * extent_size;
      int i;
      for (i = 0; i < ARRAY_SIZE(pblobby->leftovers)
                  && 0 < pblobby->leftovers[i].length;
           i++)
      {
         if (offset < pblobby->leftovers[i].length) {
            byte_addr       = pblobby->leftovers[i].addr + offset;
            entry_remainder = pblobby->leftovers[i].length - offset;
            break;
         }
         offset -= pblobby->leftovers[i].length;
      }
      platform_assert(i < ARRAY_SIZE(pblobby->leftovers));
   }

   fragment->offset = byte_addr % page_size;
   fragment->addr   = byte_addr - fragment->offset;
   fragment->length = MIN(entry_remainder, page_size - fragment->offset);
}

static platform_status
blob_fragment_for_offset(blob_page_iterator *iter)
{
   if (iter->pblob.height == 1) {
      flat_fragment_for_offset(iter->extent_size,
                               iter->page_size,
                               &iter->pblob,
                               iter->offset,
                               &iter->fragment);
      return STATUS_OK;
   }

   uint64 extent_no = iter->offset / iter->extent_size;
   if (iter->data_extent_addr == 0 || iter->data_extent != extent_no) {
      uint64 span   = iter->pblob.extent_span;
      uint64 addr   = iter->pblob.base->addrs[extent_no / span];
      uint64 within = extent_no % span;
      uint64 fanout = iter->extent_size / sizeof(uint64);
      for (uint64 level = iter->pblob.height - 1; level > 0; level--) {
         span /= fanout;
         uint64 slot = within / span;
         within %= span;
         uint64 byte        = slot * sizeof(uint64);
         uint64 page_addr   = addr + byte / iter->page_size * iter->page_size;
         page_handle **page = &iter->index_pages[level];
         if (*page == NULL || (*page)->disk_addr != page_addr) {
            if (*page != NULL) {
               cache_unget(iter->cc, *page);
               *page = NULL;
            }
            platform_status rc = blob_get_pointer_page(
               iter->cc,
               page_addr,
               iter->mode == BLOB_PAGE_ITERATOR_MODE_VERIFY,
               page);
            if (!SUCCESS(rc)) {
               return rc;
            }
         }
         memcpy(&addr, (*page)->data + byte % iter->page_size, sizeof(addr));
         if (!blob_extent_is_valid(iter->cc, addr)) {
            return STATUS_INVALID_STATE;
         }
      }
      iter->data_extent      = extent_no;
      iter->data_extent_addr = addr;
      if (iter->mode == BLOB_PAGE_ITERATOR_MODE_PREFETCH) {
         if (!allocator_page_valid(cache_get_allocator(iter->cc), addr)) {
            return STATUS_INVALID_STATE;
         }
         cache_prefetch(iter->cc, addr, PAGE_TYPE_BLOB);
      }
   }
   uint64 within = iter->offset % iter->extent_size;
   iter->fragment.addr =
      iter->data_extent_addr + within / iter->page_size * iter->page_size;
   iter->fragment.offset = within % iter->page_size;
   iter->fragment.length = MIN(iter->pblob.base->length - iter->offset,
                               iter->page_size - iter->fragment.offset);
   return STATUS_OK;
}

static void
maybe_do_prefetch(blob_page_iterator *iter)
{
   uint64 curr_extent_num = iter->offset / iter->extent_size;
   if (iter->pblob.height == 1 && iter->mode == BLOB_PAGE_ITERATOR_MODE_PREFETCH
       && curr_extent_num + 1 < iter->pblob.num_extents)
   {
      cache_prefetch(iter->cc,
                     iter->pblob.base->addrs[curr_extent_num + 1],
                     PAGE_TYPE_BLOB);
   }
}

platform_status
blob_page_iterator_init(cache                  *cc,
                        blob_page_iterator     *iter,
                        slice                   sblobby,
                        uint64                  offset,
                        blob_page_iterator_mode mode)
{
   debug_assert(mode == BLOB_PAGE_ITERATOR_MODE_PREFETCH
                || mode == BLOB_PAGE_ITERATOR_MODE_NO_PREFETCH
                || mode == BLOB_PAGE_ITERATOR_MODE_ALLOC
                || mode == BLOB_PAGE_ITERATOR_MODE_VERIFY);

   iter->cc               = cc;
   iter->mode             = mode;
   iter->extent_size      = cache_extent_size(cc);
   iter->page_size        = cache_page_size(cc);
   iter->offset           = offset;
   iter->page             = NULL;
   iter->status           = STATUS_OK;
   iter->data_extent_addr = 0;

   platform_status rc = blob_parse_descriptor(cc, sblobby, &iter->pblob);
   if (!SUCCESS(rc)) {
      return rc;
   }
   if (iter->pblob.base->length < offset) {
      return STATUS_BAD_PARAM;
   }

   for (uint64 level = 1; level < iter->pblob.height; level++) {
      iter->index_pages[level] = NULL;
   }
   if (offset < iter->pblob.base->length) {
      rc = blob_fragment_for_offset(iter);
      if (!SUCCESS(rc)) {
         blob_page_iterator_deinit(iter);
         return rc;
      }
      maybe_do_prefetch(iter);
   }

   return STATUS_OK;
}

/*
 * This function must be kept in sync with the code that decides
 * whether to call cache_alloc in blob_build.c.
 *
 * The current policy is: The blob_build thread that gets the first
 * byte of any page is reponsible for calling cache_alloc on that
 * page.  If the thread gets only part of the page, then it calls
 * cache_alloc before calling mini_alloc_bytes_finish
 * (i.e. immediately after getting the page from the mini_allocator).
 *
 * Thus we need to call cache_alloc now only if we got the entire
 * page.  Any partial page that we got will have already been
 * cache_alloced by us (in blob_build) or by some other thread.
 */

static bool
should_alloc(blob_page_iterator *iter)
{
   return iter->mode == BLOB_PAGE_ITERATOR_MODE_ALLOC
          && iter->fragment.offset == 0
          && (iter->page_size <= iter->fragment.length
              || can_round_up(iter->page_size, iter->pblob.base->length));
}

static void
blob_page_iterator_release_page(blob_page_iterator *iter)
{
   if (iter->page) {
      if (iter->mode == BLOB_PAGE_ITERATOR_MODE_ALLOC) {
         cache_unlock(iter->cc, iter->page);
         cache_unclaim(iter->cc, iter->page);
      }
      cache_unget(iter->cc, iter->page);
      iter->page = NULL;
   }
}

void
blob_page_iterator_deinit(blob_page_iterator *iter)
{
   blob_page_iterator_release_page(iter);
   for (uint64 level = 1; level < iter->pblob.height; level++) {
      if (iter->index_pages[level] != NULL) {
         cache_unget(iter->cc, iter->index_pages[level]);
         iter->index_pages[level] = NULL;
      }
   }
}

platform_status
blob_page_iterator_get_curr(blob_page_iterator *iter,
                            uint64             *offset,
                            slice              *result)
{
   if (!SUCCESS(iter->status)) {
      return iter->status;
   }
   if (blob_page_iterator_at_end(iter)) {
      return STATUS_BAD_PARAM;
   }
   if (iter->mode == BLOB_PAGE_ITERATOR_MODE_VERIFY) {
      bool32          readable;
      platform_status rc = cache_range_is_readable(
         iter->cc, iter->fragment.addr, iter->page_size, &readable);
      if (!SUCCESS(rc)) {
         return rc;
      }
      if (!readable) {
         return STATUS_IO_ERROR;
      }
   }
   if (!allocator_page_valid(cache_get_allocator(iter->cc),
                             iter->fragment.addr))
   {
      return STATUS_INVALID_STATE;
   }
   if (iter->page == NULL) {
      if (should_alloc(iter)) {
         iter->page =
            cache_alloc(iter->cc, iter->fragment.addr, PAGE_TYPE_BLOB);
      } else {
         iter->page =
            cache_get(iter->cc, iter->fragment.addr, TRUE, PAGE_TYPE_BLOB);
         if (iter->mode == BLOB_PAGE_ITERATOR_MODE_ALLOC) {
            uint64 wait = 1;
            while (!cache_try_claim(iter->cc, iter->page)) {
               cache_unget(iter->cc, iter->page);
               platform_sleep_ns(wait);
               wait       = MIN(2 * wait, 2048);
               iter->page = cache_get(
                  iter->cc, iter->fragment.addr, TRUE, PAGE_TYPE_BLOB);
            }
            cache_lock(iter->cc, iter->page);
         }
      }
   }

   *offset = iter->offset;
   *result = slice_create(iter->fragment.length,
                          iter->page->data + iter->fragment.offset);
   return STATUS_OK;
}

bool
blob_page_iterator_at_end(blob_page_iterator *iter)
{
   return iter->pblob.base->length <= iter->offset;
}

void
blob_page_iterator_advance_bytes(blob_page_iterator *iter, uint64 num_bytes)
{
   blob_page_iterator_release_page(iter);

   iter->offset += num_bytes;
   if (iter->offset < iter->pblob.base->length) {
      iter->status = blob_fragment_for_offset(iter);
      if (SUCCESS(iter->status)) {
         maybe_do_prefetch(iter);
      }
   }
}

void
blob_page_iterator_advance_page(blob_page_iterator *iter)
{
   blob_page_iterator_advance_bytes(iter, iter->fragment.length);
}

platform_status
blob_validate(cache *cc, slice sblob)
{
   if (cc == NULL) {
      return STATUS_BAD_PARAM;
   }

   const blob     *blobby;
   platform_status rc = blob_get_descriptor(sblob, &blobby);
   if (!SUCCESS(rc)) {
      return rc;
   }
   checksum128 expected = blobby->checksum;

   XXH3_state_t *checksum_state = XXH3_createState();
   if (checksum_state == NULL) {
      return STATUS_NO_MEMORY;
   }
   if (XXH3_128bits_reset_withSeed(checksum_state, BLOB_CHECKSUM_SEED)
       != XXH_OK)
   {
      XXH3_freeState(checksum_state);
      return STATUS_INVALID_STATE;
   }

   blob_page_iterator iter;
   /*
    * Recovery can encounter a descriptor whose log page reached disk while
    * one of the blob pages did not.  Do not prefetch ahead of the readability
    * check below: cache_get() is deliberately strict and a short read is a
    * cache invariant violation, whereas an incomplete blob is ordinary crash
    * truncation that validation must report to the log iterator.
    */
   rc = blob_page_iterator_init(
      cc, &iter, sblob, 0, BLOB_PAGE_ITERATOR_MODE_VERIFY);
   if (!SUCCESS(rc)) {
      XXH3_freeState(checksum_state);
      return rc;
   }

   while (!blob_page_iterator_at_end(&iter)) {
      uint64 offset;
      slice  data;
      rc = blob_page_iterator_get_curr(&iter, &offset, &data);
      if (!SUCCESS(rc)) {
         goto out;
      }

      if (XXH3_128bits_update(
             checksum_state, slice_data(data), slice_length(data))
          != XXH_OK)
      {
         rc = STATUS_INVALID_STATE;
         goto out;
      }
      blob_page_iterator_advance_page(&iter);
   }

   checksum128 actual = XXH3_128bits_digest(checksum_state);
   if (!platform_checksum_is_equal(actual, expected)) {
      rc = STATUS_IO_ERROR;
   }

out:
   blob_page_iterator_deinit(&iter);
   XXH3_freeState(checksum_state);
   return rc;
}

platform_status
blob_materialize(cache           *cc,
                 slice            sblobby,
                 uint64           start,
                 uint64           end,
                 writable_buffer *result)
{
   const blob     *blobby;
   platform_status rc = blob_get_descriptor(sblobby, &blobby);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (end < start || blobby->length < end) {
      return STATUS_BAD_PARAM;
   }

   blob_page_iterator iter;
   rc = blob_page_iterator_init(
      cc, &iter, sblobby, start, BLOB_PAGE_ITERATOR_MODE_PREFETCH);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = writable_buffer_resize(result, end - start);
   if (!SUCCESS(rc)) {
      goto out;
   }

   while (!blob_page_iterator_at_end(&iter) && iter.offset < end) {
      uint64 offset;
      slice  data;
      rc = blob_page_iterator_get_curr(&iter, &offset, &data);
      if (!SUCCESS(rc)) {
         goto out;
      }

      uint64 length = MIN(end - offset, slice_length(data));
      memcpy((char *)writable_buffer_data(result) + (offset - start),
             slice_data(data),
             length);
      blob_page_iterator_advance_page(&iter);
   }

out:
   blob_page_iterator_deinit(&iter);
   return rc;
}

platform_status
blob_materialize_full(cache *cc, slice sblob, writable_buffer *result)
{
   const blob     *blobby;
   platform_status rc = blob_get_descriptor(sblob, &blobby);
   if (!SUCCESS(rc)) {
      return rc;
   }

   return blob_materialize(cc, sblob, 0, blobby->length, result);
}

static platform_status
blob_walk_extent(cache               *cc,
                 uint64               addr,
                 uint64               height,
                 uint64               span,
                 uint64               data_extents,
                 bool32               disk_only,
                 blob_extent_callback visit,
                 void                *arg)
{
   if (!blob_extent_is_valid(cc, addr)) {
      return STATUS_INVALID_STATE;
   }
   platform_status rc = visit(cc, addr, arg);
   if (!SUCCESS(rc) || height == 0) {
      return rc;
   }
   uint64 fanout     = cache_extent_size(cc) / sizeof(uint64);
   uint64 child_span = span / fanout;
   uint64 children =
      data_extents / child_span + (data_extents % child_span != 0);
   uint64       per_page = cache_page_size(cc) / sizeof(uint64);
   page_handle *page     = NULL;
   for (uint64 i = 0; i < children; i++) {
      if (i % per_page == 0) {
         if (page != NULL) {
            cache_unget(cc, page);
            page = NULL;
         }
         rc = blob_get_pointer_page(
            cc, addr + i * sizeof(uint64), disk_only, &page);
         if (!SUCCESS(rc)) {
            break;
         }
      }
      uint64 child;
      memcpy(
         &child, page->data + (i % per_page) * sizeof(uint64), sizeof(child));
      uint64 count = MIN(data_extents, child_span);
      rc           = blob_walk_extent(
         cc, child, height - 1, child_span, count, disk_only, visit, arg);
      if (!SUCCESS(rc)) {
         break;
      }
      data_extents -= count;
   }
   if (page != NULL) {
      cache_unget(cc, page);
   }
   return rc;
}

platform_status
blob_walk_extents(cache               *cc,
                  slice                sblob,
                  bool32               disk_only,
                  blob_extent_callback visit,
                  void                *arg)
{
   parsed_blob     pblob;
   platform_status rc = blob_parse_descriptor(cc, sblob, &pblob);
   if (!SUCCESS(rc)) {
      return rc;
   }
   uint64 extent_size = cache_extent_size(cc);
   uint64 remaining   = pblob.base->length / extent_size
                      + (pblob.base->length % extent_size != 0);
   for (uint64 i = 0; i < pblob.num_extents; i++) {
      uint64 count = MIN(remaining, pblob.extent_span);
      rc           = blob_walk_extent(cc,
                            pblob.base->addrs[i],
                            pblob.height - 1,
                            pblob.extent_span,
                            count,
                            disk_only,
                            visit,
                            arg);
      if (!SUCCESS(rc)) {
         return rc;
      }
      remaining -= count;
   }
   return STATUS_OK;
}

/*
 * Record one reference for the extent containing addr, unless something already
 * has.  See blob_recover_allocations().
 */
static platform_status
blob_recover_extent(cache *cc, uint64 addr, void *unused)
{
   (void)unused;
   allocator *al = cache_get_allocator(cc);
   uint64     base =
      allocator_config_extent_base_addr(allocator_get_config(al), addr);

   if (allocator_get_refcount(al, base) != AL_FREE) {
      return STATUS_OK;
   }
   return allocator_recovery_record_reference(al, base, PAGE_TYPE_BLOB);
}

platform_status
blob_recover_allocations(cache *cc, slice sblob)
{
   platform_status rc;
   parsed_blob     pblob;
   rc = blob_parse_descriptor(cc, sblob, &pblob);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = blob_walk_extents(cc, sblob, TRUE, blob_recover_extent, NULL);
   if (!SUCCESS(rc)) {
      return rc;
   }
   /*
    * The tail lives in up to three page-aligned fragments, which may sit in an
    * extent this blob does not otherwise occupy -- and typically one it shares.
    */
   for (uint64 i = 0; i < ARRAY_SIZE(pblob.leftovers); i++) {
      if (pblob.leftovers[i].length == 0) {
         break;
      }
      rc = blob_recover_extent(cc, pblob.leftovers[i].addr, NULL);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   return STATUS_OK;
}

static platform_status
blob_writeback_extent(cache *cc, uint64 addr, void *arg)
{
   writeback_set *set = arg;
   return set != NULL ? writeback_set_add_extent(set, addr, PAGE_TYPE_BLOB)
                      : cache_writeback_extent(cc, addr, PAGE_TYPE_BLOB, NULL);
}

platform_status
blob_writeback(cache *cc, slice sblob, writeback_set *set)
{
   platform_status rc;
   uint64          page_size = cache_page_size(cc);
   parsed_blob     pblob;
   rc = blob_parse_descriptor(cc, sblob, &pblob);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = blob_walk_extents(cc, sblob, FALSE, blob_writeback_extent, set);
   if (!SUCCESS(rc)) {
      return rc;
   }

   for (uint64 i = 0; i < ARRAY_SIZE(pblob.leftovers); i++) {
      const parsed_blob_entry *tail = &pblob.leftovers[i];
      if (tail->length == 0) {
         break;
      }

      uint64 byte_addr = tail->addr;
      uint64 remaining = tail->length;
      while (remaining > 0) {
         uint64 page_offset   = byte_addr % page_size;
         uint64 page_addr     = byte_addr - page_offset;
         uint64 bytes_on_page = MIN(remaining, page_size - page_offset);

         page_handle *page = cache_get(cc, page_addr, TRUE, PAGE_TYPE_BLOB);
         if (page == NULL) {
            return STATUS_IO_ERROR;
         }

         if (set != NULL) {
            rc = writeback_set_add_page(set, page, PAGE_TYPE_BLOB);
         } else {
            rc = cache_writeback_page(cc, page, PAGE_TYPE_BLOB, NULL);
         }
         cache_unget(cc, page);
         if (!SUCCESS(rc)) {
            return rc;
         }

         byte_addr += bytes_on_page;
         remaining -= bytes_on_page;
      }
   }

   return STATUS_OK;
}
