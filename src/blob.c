// Copyright 2022 VMware, Inc. All rights reserved. -- VMware Confidential
// SPDX-License-Identifier: Apache-2.0

#include "blob.h"
#include "poison.h"

#define MIN_LIVE_PERCENTAGE         (90ULL)
#define EXTENT_BATCH                (2)
#define PAGE_BATCH                  (1)
#define SUBPAGE_BATCH               (0)


/* We break the value into parts as follows:
 * - extent-sized segments
 * - sub-extent-sized multi-page segments (at most 2)
 * - less-than-page-sized segments (at most 1 -- page fragments are not split
 *   across extents)
 */
typedef struct ONDISK blob {
   uint64 length; // length of the byte sequence represented by this blob
   uint64 addrs[];
} blob;

/* This is used internally to avoid recomputing the number of extent entries,
 * etc. */
typedef struct parsed_blob_entry {
   uint64 addr;
   uint64 length;
} parsed_blob_entry;

typedef struct parsed_blob {
   uint64                   length;
   uint64                   num_extents; // == number of extent entries
   const uint64            *extents;     // extent entries in original blob
   parsed_blob_entry        leftovers[3]; // multi-page and sub-page entries
} parsed_blob;

typedef struct blob_page_iterator {
   cache             *cc;
   page_type          type;
   bool               alloc;
   bool               do_prefetch;
   uint64             extent_size;
   uint64             page_size;
   parsed_blob        pblob;
   uint64             offset;    // logical byte offset into entire sequence
   uint64             page_addr;
   uint64             page_offset;
   uint64             length;
   page_handle       *page;   // the page with the data in it.
} blob_page_iterator;

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
parse_blob(uint64       extent_size,
           uint64       page_size,
           const blob  *blobby,
           parsed_blob *pblobby)
{
   pblobby->length    = blobby->length;
   pblobby->extents   = blobby->addrs;
   uint64 remainder   = pblobby->length;

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
      } else if (can_round_up(page_size, pblobby->length)) {
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
   debug_assert(offset < pblobby->length);

   if (offset / extent_size < pblobby->num_extents) {
      byte_addr =
         pblobby->extents[offset / extent_size] + (offset % extent_size);
      entry_remainder = extent_size - (offset % extent_size);

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
   if (!iter->alloc && iter->offset + iter->length < iter->pblob.length) {
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
      cache_prefetch(iter->cc, next_extent_addr, iter->type);
   }
}

platform_status
blob_page_iterator_init(cache              *cc,
                        blob_page_iterator *iter,
                        slice               sblobby,
                        uint64              offset,
                        page_type           type,
                        bool                alloc,
                        bool                do_prefetch)
{
   iter->cc                = cc;
   iter->type              = type;
   iter->alloc             = alloc;
   iter->do_prefetch       = do_prefetch;
   iter->extent_size       = cache_extent_size(cc);
   iter->page_size         = cache_page_size(cc);
   parse_blob(
      iter->extent_size, iter->page_size, slice_data(sblobby), &iter->pblob);
   iter->offset = offset;
   if (offset < iter->pblob.length) {
      addr_for_offset(iter->extent_size,
                      iter->page_size,
                      &iter->pblob,
                      iter->offset,
                      &iter->page_addr,
                      &iter->page_offset,
                      &iter->length);
      maybe_do_prefetch(iter);
   }
   iter->page = NULL;
   return STATUS_OK;
}

void
blob_page_iterator_deinit(blob_page_iterator *iter)
{
   if (iter->page) {
      if (iter->alloc && iter->page_addr % iter->page_size == 0
          && iter->page_offset == 0 && iter->page_size <= iter->length)
      {
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
      if (iter->alloc && iter->page_addr % iter->page_size == 0
          && iter->page_offset == 0 && iter->page_size <= iter->length)
      {
         iter->page = cache_alloc(iter->cc, iter->page_addr, iter->type);
      } else {
         iter->page = cache_get(iter->cc, iter->page_addr, TRUE, iter->type);
      }
   }

   *offset = iter->offset;
   *result = slice_create(iter->length, iter->page->data + iter->page_offset);
   return STATUS_OK;
}

bool
blob_page_iterator_at_end(blob_page_iterator *iter)
{
   return iter->pblob.length <= iter->offset;
}

void
blob_page_iterator_advance(blob_page_iterator *iter)
{
   if (iter->page) {
      if (iter->alloc && iter->page_addr % iter->page_size == 0
          && iter->page_offset == 0 && iter->page_size <= iter->length)
      {
         cache_unlock(iter->cc, iter->page);
         cache_unclaim(iter->cc, iter->page);
      }
      cache_unget(iter->cc, iter->page);
      iter->page = NULL;
   }

   iter->offset += iter->length;
   if (iter->offset < iter->pblob.length) {
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

platform_status
blob_materialize(cache           *cc,
                 slice            sblobby,
                 uint64           start,
                 uint64           end,
                 page_type        type,
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
   rc = blob_page_iterator_init(cc, &iter, sblobby, start, type, FALSE, TRUE);
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

static platform_status
allocate_leftover_entries(cache           *cc,
                          mini_allocator  *mini,
                          slice            key,
                          uint64           data_len,
                          uint64           remainder,
                          writable_buffer *result)
{
   uint64 page_size = cache_page_size(cc);
   uint64 extent_size = cache_extent_size(cc);

   /* Allocate the page entries */
   uint64 num_pages;
   if (can_round_up(page_size, data_len)) {
      num_pages = (remainder + page_size - 1) / page_size;
   } else {
      num_pages = remainder / page_size;
   }
   uint64          alloced_pages[2];
   platform_status rc;
   if (num_pages) {
      rc = mini_alloc_bytes(mini,
                            PAGE_BATCH,
                            num_pages * page_size,
                            page_size,
                            0,
                            key,
                            alloced_pages,
                            NULL);
      if (!SUCCESS(rc)) {
         return rc;
      }
      if (alloced_pages[1]) {
         writable_buffer_append(result, sizeof(alloced_pages), &alloced_pages);
      } else {
         writable_buffer_append(
            result, sizeof(alloced_pages[0]), &alloced_pages[0]);
      }

      if (remainder < num_pages * page_size) {
         remainder = 0;
      } else {
         remainder -= num_pages * page_size;
      }
   }

   /* Allocate the sub-page entry */
   if (remainder) {
      rc = mini_alloc_bytes(mini,
                            SUBPAGE_BATCH,
                            remainder,
                            1,
                            extent_size,
                            key,
                            alloced_pages,
                            NULL);
      if (!SUCCESS(rc)) {
         return rc;
      }
      platform_assert(alloced_pages[1] == 0);
      writable_buffer_append(
         result, sizeof(alloced_pages[0]), &alloced_pages[0]);
   }

   return STATUS_OK;
}

static platform_status
build_blob_table(cache           *cc,
                 mini_allocator  *mini,
                 slice            key,
                 uint64           data_len,
                 writable_buffer *result)
{
   uint64 extent_size = cache_extent_size(cc);

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

   writable_buffer_resize(result, sizeof(blob) + num_extents * sizeof(uint64));
   blob *blobby        = writable_buffer_data(result);
   blobby->length      = data_len;

   for (uint64 i = 0; i < num_extents; i++) {
      uint64          alloced_pages[2];
      platform_status rc;
      rc = mini_alloc_bytes(mini,
                            EXTENT_BATCH,
                            extent_size,
                            extent_size,
                            extent_size,
                            key,
                            alloced_pages,
                            NULL);
      if (!SUCCESS(rc)) {
         return rc;
      }
      debug_assert(alloced_pages[1] == 0);
      blobby->addrs[i] = alloced_pages[0];
      platform_assert(blobby->addrs[i]);
   }

   return allocate_leftover_entries(cc, mini, key, data_len, remainder, result);
}

platform_status
blob_build(cache           *cc,
           mini_allocator  *mini,
           slice            key,
           slice            data,
           page_type        type,
           writable_buffer *result)
{
   platform_status rc =
      build_blob_table(cc, mini, key, slice_length(data), result);

   if (!SUCCESS(rc)) {
      return rc;
   }

   blob_page_iterator iter;
   rc = blob_page_iterator_init(
      cc, &iter, writable_buffer_to_slice(result), 0, type, TRUE, TRUE);
   if (!SUCCESS(rc)) {
      return rc;
   }

   const char *raw_data = slice_data(data);
   while (!blob_page_iterator_at_end(&iter)) {
      uint64 offset;
      slice  result;
      rc = blob_page_iterator_get_curr(&iter, &offset, &result);
      if (!SUCCESS(rc)) {
         goto out;
      }

      memcpy(iter.page->data + iter.page_offset,
             raw_data + offset,
             slice_length(result));

      blob_page_iterator_advance(&iter);
   }

out:
   blob_page_iterator_deinit(&iter);
   return rc;
}

static platform_status
clone_blob_table(cache           *cc,
                 mini_allocator  *mini,
                 slice            key,
                 parsed_blob     *pblobby,
                 writable_buffer *result)
{
   platform_status rc = writable_buffer_resize(
      result, sizeof(blob) + pblobby->num_extents * sizeof(uint64));
   if (!SUCCESS(rc)) {
      return rc;
   }

   blob *blobby = writable_buffer_data(result);

   blobby->length = pblobby->length;

   for (int i = 0; i < pblobby->num_extents; i++) {
      blobby->addrs[i] = pblobby->extents[i];
      rc = mini_attach_extent(mini, EXTENT_BATCH, key, blobby->addrs[i]);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   uint64 extent_size = cache_extent_size(cc);
   if (pblobby->num_extents * extent_size < pblobby->length) {
      uint64 remainder = pblobby->length - pblobby->num_extents * extent_size;
      rc               = allocate_leftover_entries(
         cc, mini, key, pblobby->length, remainder, result);
   }

   return rc;
}

platform_status
blob_clone(cache           *cc,
           mini_allocator  *mini,
           slice            key,
           slice            sblobby,
           page_type        src_type,
           page_type        dst_type,
           writable_buffer *result)
{
   uint64             extent_size = cache_extent_size(cc);
   uint64             page_size   = cache_page_size(cc);
   const blob        *blobby      = slice_data(sblobby);
   parsed_blob        pblobby;

   parse_blob(extent_size, page_size, blobby, &pblobby);

   platform_status rc = clone_blob_table(cc, mini, key, &pblobby, result);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (pblobby.num_extents * extent_size < pblobby.length) {
      uint64 start = pblobby.num_extents * extent_size;

      blob_page_iterator src_iter;
      blob_page_iterator dst_iter;
      rc = blob_page_iterator_init(
         cc, &src_iter, sblobby, start, src_type, FALSE, TRUE);
      if (!SUCCESS(rc)) {
         return rc;
      }
      rc = blob_page_iterator_init(cc,
                                   &dst_iter,
                                   writable_buffer_to_slice(result),
                                   start,
                                   dst_type,
                                   TRUE,
                                   TRUE);
      if (!SUCCESS(rc)) {
         return rc;
      }

      while (!blob_page_iterator_at_end(&src_iter)
             && !blob_page_iterator_at_end(&dst_iter))
      {
         uint64 src_offset;
         slice  src_result;
         uint64 dst_offset;
         slice  dst_result;
         rc = blob_page_iterator_get_curr(&src_iter, &src_offset, &src_result);
         if (!SUCCESS(rc)) {
            goto out;
         }
         rc = blob_page_iterator_get_curr(&dst_iter, &dst_offset, &dst_result);
         if (!SUCCESS(rc)) {
            goto out;
         }

         platform_assert(src_offset == dst_offset);
         platform_assert(slice_length(src_result) == slice_length(dst_result));

         memcpy(dst_iter.page->data + dst_iter.page_offset,
                slice_data(src_result),
                slice_length(src_result));

         blob_page_iterator_advance(&src_iter);
         blob_page_iterator_advance(&dst_iter);
      }

   out:
      blob_page_iterator_deinit(&src_iter);
      blob_page_iterator_deinit(&dst_iter);
   }

   return rc;
}
