// Copyright 2022 VMware, Inc. All rights reserved. -- VMware Confidential
// SPDX-License-Identifier: Apache-2.0

#include "indirect.h"
#include "poison.h"

#define MIN_LIVE_PERCENTAGE         (90ULL)
#define EXTENT_BATCH                (0)
#define PAGE_BATCH                  (1)
#define SUBPAGE_BATCH               (2)


/* We break the value into parts as follows:
 * - extent-sized segments
 * - sub-extent-sized multi-page segments (at most 2)
 * - less-than-page-sized segments (at most 1 -- page fragments are not split
 *   across extents)
 */
typedef struct ONDISK indirection {
   uint64 length; // length of the byte sequence represented by this indirection
   uint64 addrs[];
} indirection;

/* This is used internally to avoid recomputing the number of extent entries,
 * etc. */
typedef struct parsed_indirection_entry {
   uint64 addr;
   uint64 length;
} parsed_indirection_entry;

typedef struct parsed_indirection {
   uint64                   length;
   uint64                   num_extents; // == number of extent entries
   const uint64            *extents; // extent entries in original indirection
   parsed_indirection_entry leftovers[3]; // multi-page and sub-page entries
} parsed_indirection;

typedef struct indirection_page_iterator {
   cache             *cc;
   page_type          type;
   bool               do_prefetch;
   uint64             extent_size;
   uint64             page_size;
   parsed_indirection pindy;
   uint64             offset;    // logical byte offset into entire sequence
   uint64             page_addr;
   uint64             page_offset;
   uint64             length;
   page_handle       *page;   // the page with the data in it.
} indirection_page_iterator;

/* If the data is large enough (or close enough to a whole number of
 * rounded_size pieces), then we just put it entirely into
 * rounded_size pieces, since this won't waste too much space.
 */
static inline bool
can_round_up(uint64 rounded_size, uint64 length)
{
   uint64 covering_rounded_count = (length + rounded_size - 1) / rounded_size;
   uint64 covering_space_efficiency =
      100ULL * length / (covering_rounded_count * rounded_size);
   return MIN_LIVE_PERCENTAGE <= covering_space_efficiency;
}

static inline void
parse_indirection(uint64              extent_size,
                  uint64              page_size,
                  const indirection  *indy,
                  parsed_indirection *pindy)
{
   pindy->length    = indy->length;
   pindy->extents   = indy->addrs;
   uint64 remainder = pindy->length;

   if (can_round_up(extent_size, remainder)) {
      pindy->num_extents         = (remainder + extent_size - 1) / extent_size;
      pindy->leftovers[0].length = 0;
      return;
   }

   pindy->num_extents = remainder / extent_size;
   remainder -= pindy->num_extents * extent_size;

   memset(pindy->leftovers, 0, sizeof(pindy->leftovers));
   uint64 entry           = pindy->num_extents;
   uint64 leftovers_entry = 0;
   while (page_size <= remainder) {
      pindy->leftovers[leftovers_entry].addr = indy->addrs[entry];
      uint64 max_length = extent_size - (indy->addrs[entry] % extent_size);
      if (max_length <= remainder) {
         pindy->leftovers[leftovers_entry].length = max_length;
      } else if (can_round_up(page_size, pindy->length)) {
         pindy->leftovers[leftovers_entry].length = remainder;
      } else {
         pindy->leftovers[leftovers_entry].length =
            remainder - (remainder % page_size);
      }

      remainder -= pindy->leftovers[leftovers_entry].length;
      entry++;
      leftovers_entry++;
   }

   if (remainder) {
      pindy->leftovers[leftovers_entry].addr   = indy->addrs[entry];
      pindy->leftovers[leftovers_entry].length = remainder;
   }
}

static inline uint64
num_leftovers(parsed_indirection *pindy)
{
   int i = 0;
   while (pindy->leftovers[i].length) {
      i++;
   }
   return i;
}

uint64
indirection_data_length(slice sindy)
{
   const indirection *indy = slice_data(sindy);
   debug_assert(sizeof(*indy) <= slice_length(sindy));
   return indy->length;
}

static void
addr_for_offset(uint64                    extent_size,
                uint64                    page_size,
                const parsed_indirection *pindy,
                uint64                    offset,
                uint64                   *page_addr,
                uint64                   *page_offset,
                uint64                   *length)
{
   uint64 byte_addr;
   uint64 entry_remainder;
   debug_assert(offset < indy->length);

   if (offset / extent_size < pindy->num_extents) {
      byte_addr = pindy->extents[offset / extent_size] + (offset % extent_size);
      entry_remainder = extent_size - (offset % extent_size);

   } else {

      offset -= pindy->num_extents * extent_size;
      int i;
      for (i = 0;
           i < ARRAY_SIZE(pindy->leftovers) && 0 < pindy->leftovers[i].length;
           i++)
      {
         if (offset < pindy->leftovers[i].length) {
            byte_addr             = pindy->leftovers[i].addr + offset;
            entry_remainder       = pindy->leftovers[i].length - offset;
            break;
         } else {
            offset -= pindy->leftovers[i].length;
         }
      }
      platform_assert(i < ARRAY_SIZE(pindy->leftovers));
   }

   *page_offset = byte_addr % page_size;
   *page_addr   = byte_addr - *page_offset;
   *length      = MIN(entry_remainder, page_size - *page_offset);
}

platform_status
indirection_page_iterator_init(cache                     *cc,
                               indirection_page_iterator *iter,
                               slice                      sindy,
                               uint64                     offset,
                               page_type                  type,
                               bool                       do_prefetch)
{
   iter->cc                = cc;
   iter->type              = type;
   iter->do_prefetch       = do_prefetch;
   iter->extent_size       = cache_extent_size(cc);
   iter->page_size         = cache_page_size(cc);
   parse_indirection(
      iter->extent_size, iter->page_size, slice_data(sindy), &iter->pindy);
   iter->offset = offset;
   addr_for_offset(iter->extent_size,
                   iter->page_size,
                   &iter->pindy,
                   iter->offset,
                   &iter->page_addr,
                   &iter->page_offset,
                   &iter->length);
   return STATUS_OK;
}

void
indirection_page_iterator_deinit(indirection_page_iterator *iter)
{
   if (iter->page) {
      cache_unget(iter->cc, iter->page);
      iter->page = NULL;
   }
}

platform_status
indirection_page_iterator_get_curr(indirection_page_iterator *iter,
                                   uint64                    *offset,
                                   slice                     *result)
{
   if (iter->page == NULL) {
      iter->page = cache_get(iter->cc, iter->page_addr, FALSE, iter->type);
   }

   *offset = iter->offset;
   *result = slice_create(iter->length, iter->page->data + iter->page_offset);
   return STATUS_OK;
}

bool
indirection_page_iterator_at_end(indirection_page_iterator *iter)
{
   return iter->pindy.length <= iter->offset;
}

void
indirection_page_iterator_advance(indirection_page_iterator *iter)
{
   if (iter->page) {
      cache_unget(iter->cc, iter->page);
      iter->page = NULL;
   }

   iter->offset += iter->length;
   addr_for_offset(iter->extent_size,
                   iter->page_size,
                   &iter->pindy,
                   iter->offset,
                   &iter->page_addr,
                   &iter->page_offset,
                   &iter->length);
}

platform_status
indirection_materialize(cache           *cc,
                        slice            sindy,
                        uint64           start,
                        uint64           end,
                        page_type        type,
                        writable_buffer *result)
{
   const indirection *indy = slice_data(sindy);

   if (end < start || indy->length < end) {
      return STATUS_BAD_PARAM;
   }

   platform_status rc = writable_buffer_resize(result, end - start);
   if (!SUCCESS(rc)) {
      return rc;
   }

   indirection_page_iterator iter;
   rc = indirection_page_iterator_init(cc, &iter, sindy, start, type, TRUE);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64 offset;
   slice  data;
   rc = indirection_page_iterator_get_curr(&iter, &offset, &data);
   if (!SUCCESS(rc)) {
      goto out;
   }

   void *dst = writable_buffer_data(result);
   while (offset < end) {
      uint64 slen   = slice_length(data);
      uint64 length = end - offset < slen ? end - offset : slen;
      memcpy(dst + (offset - start), slice_data(data), length);

      indirection_page_iterator_advance(&iter);
      if (indirection_page_iterator_at_end(&iter)) {
         goto out;
      }

      rc = indirection_page_iterator_get_curr(&iter, &offset, &data);
      if (!SUCCESS(rc)) {
         goto out;
      }
   }

out:
   indirection_page_iterator_deinit(&iter);
   return rc;
}

static platform_status
allocate_leftover_entries(cache           *cc,
                          mini_allocator  *mini,
                          slice            key,
                          uint64           data_len,
                          uint64           remainder,
                          writable_buffer *result)
{
   uint64 page_size = cache_page_size(cc);

   /* Allocate the page entries */
   while (page_size <= remainder) {
      uint64 num_pages;
      if (can_round_up(page_size, data_len)) {
         num_pages = (remainder + page_size - 1) / page_size;
      } else {
         num_pages = remainder / page_size;
      }
      uint64 addr;
      uint64 alloced_pages;
      addr = mini_alloc_pages(
         mini, PAGE_BATCH, num_pages, key, NULL, &alloced_pages);
      if (addr == 0) {
         return STATUS_NO_SPACE;
      }
      writable_buffer_append(result, sizeof(addr), &addr);
      if (remainder < alloced_pages * page_size) {
         remainder = 0;
      } else {
         remainder -= alloced_pages * page_size;
      }
   }

   /* Allocate the sub-page entry */
   if (remainder) {
      uint64 addr = mini_alloc_bytes(mini, remainder, key, NULL);
      if (addr == 0) {
         return STATUS_NO_SPACE;
      }
      writable_buffer_append(result, sizeof(addr), &addr);
   }

   return STATUS_OK;
}

static platform_status
build_indirection_table(cache           *cc,
                        mini_allocator  *mini,
                        slice            key,
                        uint64           data_len,
                        writable_buffer *result)
{
   uint64 extent_size = cache_extent_size(cc);
   uint64 page_size   = cache_page_size(cc);

   /* Allocate the extent entries */
   uint64 num_extents;
   uint64 remainder;
   if (can_round_up(extent_size, data_len)) {
      num_extents = (data_len + extent_size - 1) / extent_size;
      remainder   = 0;
   } else {
      num_extents = data_len / extent_size;
      remainder   = data_len - num_extents * extent_size;
   }

   writable_buffer_resize(result,
                          sizeof(indirection) + num_extents * sizeof(uint64));
   indirection *indy = writable_buffer_data(result);
   indy->length      = data_len;

   for (uint64 i = 0; i < num_extents; i++) {
      uint64 alloced_pages;
      indy->addrs[i] = mini_alloc_pages(mini,
                                        EXTENT_BATCH,
                                        extent_size / page_size,
                                        key,
                                        NULL,
                                        &alloced_pages);
      platform_assert(indy->addrs[i]);
      platform_assert(alloced_pages == extent_size / page_size);
   }

   return allocate_leftover_entries(cc, mini, key, data_len, remainder, result);
}

platform_status
indirection_build(cache           *cc,
                  mini_allocator  *mini,
                  slice            key,
                  slice            data,
                  page_type        type,
                  writable_buffer *result)
{
   platform_status rc =
      build_indirection_table(cc, mini, key, slice_length(data), result);

   if (!SUCCESS(rc)) {
      return rc;
   }

   indirection_page_iterator iter;
   rc = indirection_page_iterator_init(
      cc, &iter, writable_buffer_to_slice(result), 0, type, TRUE);
   if (!SUCCESS(rc)) {
      return rc;
   }

   const char *raw_data = slice_data(data);
   while (!indirection_page_iterator_at_end(&iter)) {
      uint64 offset;
      slice  result;
      rc = indirection_page_iterator_get_curr(&iter, &offset, &result);
      if (!SUCCESS(rc)) {
         return rc;
      }

      if (!cache_claim(cc, iter.page)) {
         goto out;
      }
      cache_lock(cc, iter.page);

      memcpy(iter.page->data + iter.page_offset,
             raw_data + offset,
             slice_length(result));

      cache_unlock(cc, iter.page);
      cache_unclaim(cc, iter.page);

      indirection_page_iterator_advance(&iter);
   }

out:
   indirection_page_iterator_deinit(&iter);
   return rc;
}

static platform_status
clone_indirection_table(cache              *cc,
                        mini_allocator     *mini,
                        slice               key,
                        parsed_indirection *pindy,
                        writable_buffer    *result)
{
   uint64          naddrs = pindy->num_extents + num_leftovers(pindy);
   platform_status rc     = writable_buffer_resize(
      result, sizeof(indirection) + naddrs * sizeof(uint64));
   if (!SUCCESS(rc)) {
      return rc;
   }

   indirection *indy = writable_buffer_data(result);

   indy->length = pindy->length;

   for (int i = 0; i < pindy->num_extents; i++) {
      indy->addrs[i] = pindy->extents[i];
      rc = mini_attach_extent(mini, EXTENT_BATCH, key, indy->addrs[i]);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   uint64 extent_size = cache_extent_size(cc);
   if (pindy->num_extents * extent_size < pindy->length) {
      uint64 remainder = pindy->length - pindy->num_extents * extent_size;
      rc               = allocate_leftover_entries(
         cc, mini, key, pindy->length, remainder, result);
   }

   return rc;
}

platform_status
indirection_clone(cache           *cc,
                  mini_allocator  *mini,
                  slice            key,
                  slice            sindy,
                  page_type        src_type,
                  page_type        dst_type,
                  writable_buffer *result)
{
   uint64             extent_size = cache_extent_size(cc);
   uint64             page_size   = cache_page_size(cc);
   const indirection *indy        = slice_data(sindy);
   parsed_indirection pindy;

   parse_indirection(extent_size, page_size, indy, &pindy);

   platform_status rc = clone_indirection_table(cc, mini, key, &pindy, result);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (pindy.num_extents * extent_size < pindy.length) {
      uint64 start = pindy.num_extents * extent_size;

      indirection_page_iterator src_iter;
      indirection_page_iterator dst_iter;
      rc = indirection_page_iterator_init(
         cc, &src_iter, sindy, start, src_type, TRUE);
      if (!SUCCESS(rc)) {
         return rc;
      }
      rc = indirection_page_iterator_init(cc,
                                          &dst_iter,
                                          writable_buffer_to_slice(result),
                                          start,
                                          dst_type,
                                          TRUE);
      if (!SUCCESS(rc)) {
         return rc;
      }

      while (!indirection_page_iterator_at_end(&src_iter)
             && !indirection_page_iterator_at_end(&dst_iter))
      {
         uint64 src_offset;
         slice  src_result;
         uint64 dst_offset;
         slice  dst_result;
         rc = indirection_page_iterator_get_curr(
            &src_iter, &src_offset, &src_result);
         if (!SUCCESS(rc)) {
            return rc;
         }
         rc = indirection_page_iterator_get_curr(
            &dst_iter, &dst_offset, &dst_result);
         if (!SUCCESS(rc)) {
            return rc;
         }

         platform_assert(src_offset == dst_offset);
         platform_assert(slice_length(src_result) == slice_length(dst_result));

         if (!cache_claim(cc, dst_iter.page)) {
            goto out;
         }
         cache_lock(cc, dst_iter.page);

         memcpy(dst_iter.page->data + dst_iter.page_offset,
                slice_data(src_result),
                slice_length(src_result));

         cache_unlock(cc, dst_iter.page);
         cache_unclaim(cc, dst_iter.page);

         indirection_page_iterator_advance(&src_iter);
         indirection_page_iterator_advance(&dst_iter);
      }

   out:
      indirection_page_iterator_deinit(&src_iter);
      indirection_page_iterator_deinit(&dst_iter);
   }

   return rc;
}
