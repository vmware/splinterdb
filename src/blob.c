// Copyright 2022 VMware, Inc. All rights reserved. -- VMware Confidential
// SPDX-License-Identifier: Apache-2.0

#include "blob.h"
#include "poison.h"

#define MIN_LIVE_PERCENTAGE         (90ULL)

/* If the data is large enough (or close enough to a whole number of
 * rounded_size pieces), then we just put it entirely into
 * rounded_size pieces, since this won't waste too much space.
 */
bool
can_round_up(uint64 rounded_size, uint64 length)
{
   uint64 covering_rounded_count = (length + rounded_size - 1) / rounded_size;
   uint64 covering_space_efficiency =
      100ULL * length / (covering_rounded_count * rounded_size);
   return MIN_LIVE_PERCENTAGE <= covering_space_efficiency;
}

void
parse_blob(uint64       extent_size,
           uint64       page_size,
           const blob  *blobby,
           parsed_blob *pblobby)
{
   pblobby->base      = blobby;
   uint64 remainder   = blobby->length;

   if (can_round_up(extent_size, remainder)) {
      pblobby->num_extents = (remainder + extent_size - 1) / extent_size;
      pblobby->leftovers[0].length = 0;
      return;
   }

   pblobby->num_extents = remainder / extent_size;
   remainder -= pblobby->num_extents * extent_size;

   memset(pblobby->leftovers, 0, sizeof(pblobby->leftovers));
   uint64 entry           = pblobby->num_extents;
   uint64 leftovers_entry = 0;
   while (page_size <= remainder) {
      pblobby->leftovers[leftovers_entry].addr = blobby->addrs[entry];
      uint64 max_length = extent_size - (blobby->addrs[entry] % extent_size);
      if (max_length <= remainder) {
         pblobby->leftovers[leftovers_entry].length = max_length;
      } else if (can_round_up(page_size, blobby->length)) {
         pblobby->leftovers[leftovers_entry].length = remainder;
      } else {
         pblobby->leftovers[leftovers_entry].length =
            remainder - (remainder % page_size);
      }

      remainder -= pblobby->leftovers[leftovers_entry].length;
      entry++;
      leftovers_entry++;
   }

   if (remainder) {
      pblobby->leftovers[leftovers_entry].addr   = blobby->addrs[entry];
      pblobby->leftovers[leftovers_entry].length = remainder;
   }
}

uint64
blob_length(slice sblobby)
{
   const blob *blobby = slice_data(sblobby);
   debug_assert(sizeof(*blobby) <= slice_length(sblobby));
   return blobby->length;
}

static void
addr_for_offset(uint64             extent_size,
                uint64             page_size,
                const parsed_blob *pblobby,
                uint64             offset,
                uint64            *page_addr,
                uint64            *page_offset,
                uint64            *length)
{
   uint64 byte_addr;
   uint64 entry_remainder;
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
            byte_addr             = pblobby->leftovers[i].addr + offset;
            entry_remainder       = pblobby->leftovers[i].length - offset;
            break;
         } else {
            offset -= pblobby->leftovers[i].length;
         }
      }
      platform_assert(i < ARRAY_SIZE(pblobby->leftovers));
   }

   *page_offset = byte_addr % page_size;
   *page_addr   = byte_addr - *page_offset;
   *length      = MIN(entry_remainder, page_size - *page_offset);
}

static void
maybe_do_prefetch(blob_page_iterator *iter)
{
   if (!iter->alloc && iter->offset + iter->length < iter->pblob.base->length) {
      uint64 next_page_addr;
      uint64 next_page_offset;
      uint64 next_length;
      addr_for_offset(iter->extent_size,
                      iter->page_size,
                      &iter->pblob,
                      iter->offset + iter->length,
                      &next_page_addr,
                      &next_page_offset,
                      &next_length);
      uint64 next_extent_addr =
         cache_extent_base_addr(iter->cc, next_page_addr);
      if (next_extent_addr == next_page_addr
          && next_length == iter->extent_size) {
         cache_prefetch(iter->cc, next_extent_addr, PAGE_TYPE_BLOB);
      }
   }
}

platform_status
blob_page_iterator_init(cache              *cc,
                        blob_page_iterator *iter,
                        slice               sblobby,
                        uint64              offset,
                        bool                alloc,
                        bool                do_prefetch)
{
   iter->cc                = cc;
   iter->alloc             = alloc;
   iter->do_prefetch       = do_prefetch;
   iter->extent_size       = cache_extent_size(cc);
   iter->page_size         = cache_page_size(cc);
   iter->offset            = offset;
   iter->page              = NULL;

   parse_blob(
      iter->extent_size, iter->page_size, slice_data(sblobby), &iter->pblob);

   if (offset < iter->pblob.base->length) {
      addr_for_offset(iter->extent_size,
                      iter->page_size,
                      &iter->pblob,
                      iter->offset,
                      &iter->page_addr,
                      &iter->page_offset,
                      &iter->length);
      maybe_do_prefetch(iter);
   }

   return STATUS_OK;
}

static bool
should_alloc(blob_page_iterator *iter)
{
   return iter->alloc && iter->page_offset == 0
          && (iter->page_size <= iter->length
              || can_round_up(iter->page_size, iter->pblob.base->length));
}

void
blob_page_iterator_deinit(blob_page_iterator *iter)
{
   if (iter->page) {
      if (iter->alloc) {
         cache_unlock(iter->cc, iter->page);
         cache_unclaim(iter->cc, iter->page);
      }
      cache_unget(iter->cc, iter->page);
      iter->page = NULL;
   }
}

platform_status
blob_page_iterator_get_curr(blob_page_iterator *iter,
                            uint64             *offset,
                            slice              *result)
{
   if (iter->page == NULL) {
      if (should_alloc(iter)) {
         iter->page = cache_alloc(iter->cc, iter->page_addr, PAGE_TYPE_BLOB);
      } else {
         iter->page =
            cache_get(iter->cc, iter->page_addr, TRUE, PAGE_TYPE_BLOB);
         if (iter->alloc) {
            int wait = 1;
            while (!cache_claim(iter->cc, iter->page)) {
               cache_unget(iter->cc, iter->page);
               platform_sleep(wait);
               wait = MIN(2 * wait, 2048);
               iter->page =
                  cache_get(iter->cc, iter->page_addr, TRUE, PAGE_TYPE_BLOB);
            }
            cache_lock(iter->cc, iter->page);
            cache_mark_dirty(iter->cc, iter->page);
         }
      }
   }

   *offset = iter->offset;
   *result = slice_create(iter->length, iter->page->data + iter->page_offset);
   return STATUS_OK;
}

bool
blob_page_iterator_at_end(blob_page_iterator *iter)
{
   return iter->pblob.base->length <= iter->offset;
}

void
blob_page_iterator_advance_partial(blob_page_iterator *iter, uint64 amount)
{
   if (iter->page) {
      if (iter->alloc) {
         cache_unlock(iter->cc, iter->page);
         cache_unclaim(iter->cc, iter->page);
      }
      cache_unget(iter->cc, iter->page);
      iter->page = NULL;
   }

   iter->offset += amount;
   if (iter->offset < iter->pblob.base->length) {
      addr_for_offset(iter->extent_size,
                      iter->page_size,
                      &iter->pblob,
                      iter->offset,
                      &iter->page_addr,
                      &iter->page_offset,
                      &iter->length);
      maybe_do_prefetch(iter);
   }
}

void
blob_page_iterator_advance(blob_page_iterator *iter)
{
   blob_page_iterator_advance_partial(iter, iter->length);
}

platform_status
blob_materialize(cache           *cc,
                 slice            sblobby,
                 uint64           start,
                 uint64           end,
                 writable_buffer *result)
{
   const blob *blobby = slice_data(sblobby);

   if (end < start || blobby->length < end) {
      return STATUS_BAD_PARAM;
   }

   platform_status rc = writable_buffer_resize(result, end - start);
   if (!SUCCESS(rc)) {
      return rc;
   }

   blob_page_iterator iter;
   rc = blob_page_iterator_init(cc, &iter, sblobby, start, FALSE, TRUE);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64 offset;
   slice  data;
   rc = blob_page_iterator_get_curr(&iter, &offset, &data);
   if (!SUCCESS(rc)) {
      goto out;
   }

   void *dst = writable_buffer_data(result);
   while (offset < end) {
      uint64 slen   = slice_length(data);
      uint64 length = end - offset < slen ? end - offset : slen;
      memcpy(dst + (offset - start), slice_data(data), length);

      blob_page_iterator_advance(&iter);
      if (blob_page_iterator_at_end(&iter)) {
         break;
      }

      rc = blob_page_iterator_get_curr(&iter, &offset, &data);
      if (!SUCCESS(rc)) {
         goto out;
      }
   }

out:
   blob_page_iterator_deinit(&iter);
   return rc;
}

platform_status
blob_sync(cache *cc, slice sblob)
{
   blob_page_iterator itor;
   platform_status    rc;

   rc = blob_page_iterator_init(cc, &itor, sblob, 0, FALSE, FALSE);
   if (!SUCCESS(rc)) {
      return rc;
   }

   while (!blob_page_iterator_at_end(&itor)) {
      uint64 offset;
      slice  result;
      rc = blob_page_iterator_get_curr(&itor, &offset, &result);
      if (!SUCCESS(rc)) {
         blob_page_iterator_deinit(&itor);
         return rc;
      }
      cache_page_sync(cc, itor.page, FALSE, PAGE_TYPE_BLOB);
      blob_page_iterator_advance(&itor);
   }

   blob_page_iterator_deinit(&itor);
   return STATUS_OK;
}
