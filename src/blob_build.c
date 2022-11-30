// Copyright 2022 VMware, Inc. All rights reserved. -- VMware Confidential
// SPDX-License-Identifier: Apache-2.0

#include "blob_build.h"
#include "poison.h"

#define EXTENT_BATCH                (2)
#define PAGE_BATCH                  (1)
#define SUBPAGE_BATCH               (0)

static platform_status
allocate_leftover_entries(const blob_build_config *cfg,
                          cache                   *cc,
                          mini_allocator          *mini,
                          key                      alloc_key,
                          uint64                   data_len,
                          uint64                   remainder,
                          writable_buffer         *result)
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
                            cfg->page_batch,
                            num_pages * page_size,
                            MAX(page_size, cfg->alignment),
                            0,
                            alloc_key,
                            alloced_pages,
                            NULL);
      if (!SUCCESS(rc)) {
         return rc;
      }
      mini_alloc_bytes_finish(mini, cfg->page_batch);
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
                            cfg->subpage_batch,
                            remainder,
                            cfg->alignment,
                            extent_size,
                            alloc_key,
                            alloced_pages,
                            NULL);
      if (!SUCCESS(rc)) {
         return rc;
      }
      platform_assert(alloced_pages[1] == 0);
      if (alloced_pages[0] % page_size == 0) {
         page_handle *page = cache_alloc(cc, alloced_pages[0], PAGE_TYPE_BLOB);
         cache_unlock(cc, page);
         cache_unclaim(cc, page);
         cache_unget(cc, page);
      } else if (alloced_pages[0] / page_size
                 != (alloced_pages[0] + remainder - 1) / page_size)
      {
         uint64 next_page_addr =
            (alloced_pages[0] + remainder - 1) / page_size * page_size;
         page_handle *page = cache_alloc(cc, next_page_addr, PAGE_TYPE_BLOB);
         cache_unlock(cc, page);
         cache_unclaim(cc, page);
         cache_unget(cc, page);
      }
      mini_alloc_bytes_finish(mini, cfg->subpage_batch);
      writable_buffer_append(
         result, sizeof(alloced_pages[0]), &alloced_pages[0]);
   }

   return STATUS_OK;
}

static platform_status
build_blob_table(const blob_build_config *cfg,
                 cache                   *cc,
                 mini_allocator          *mini,
                 key                      alloc_key,
                 uint64                   data_len,
                 writable_buffer         *result)
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
      uint64 alloced_page =
         mini_alloc_extent(mini, cfg->extent_batch, alloc_key, NULL);
      debug_assert(alloced_page != 0);
      debug_assert(alloced_page != 1);
      blobby->addrs[i] = alloced_page;
   }

   return allocate_leftover_entries(
      cfg, cc, mini, alloc_key, data_len, remainder, result);
}

platform_status
blob_build(const blob_build_config *cfg,
           cache                   *cc,
           mini_allocator          *mini,
           key                      alloc_key,
           slice                    data,
           writable_buffer         *result)
{
   platform_status rc =
      build_blob_table(cfg, cc, mini, alloc_key, slice_length(data), result);

   if (!SUCCESS(rc)) {
      return rc;
   }

   blob_page_iterator iter;
   rc = blob_page_iterator_init(
      cc, &iter, writable_buffer_to_slice(result), 0, TRUE, TRUE);
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

      debug_assert(offset + slice_length(result) <= slice_length(data));
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
clone_blob_table(const blob_build_config *cfg,
                 cache                   *cc,
                 mini_allocator          *mini,
                 key                      alloc_key,
                 parsed_blob             *pblobby,
                 writable_buffer         *result)
{
   platform_status rc = writable_buffer_resize(
      result, sizeof(blob) + pblobby->num_extents * sizeof(uint64));
   if (!SUCCESS(rc)) {
      return rc;
   }

   blob *blobby = writable_buffer_data(result);

   blobby->length = pblobby->base->length;

   for (int i = 0; i < pblobby->num_extents; i++) {
      blobby->addrs[i] = pblobby->base->addrs[i];
      rc               = mini_attach_extent(
         mini, cfg->extent_batch, alloc_key, blobby->addrs[i]);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   uint64 extent_size = cache_extent_size(cc);
   if (pblobby->num_extents * extent_size < pblobby->base->length) {
      uint64 remainder =
         pblobby->base->length - pblobby->num_extents * extent_size;
      rc = allocate_leftover_entries(
         cfg, cc, mini, alloc_key, pblobby->base->length, remainder, result);
   }

   return rc;
}

platform_status
blob_clone(const blob_build_config *cfg,
           cache                   *cc,
           mini_allocator          *mini,
           key                      alloc_key,
           slice                    sblobby,
           writable_buffer         *result)
{
   uint64             extent_size = cache_extent_size(cc);
   uint64             page_size   = cache_page_size(cc);
   const blob        *blobby      = slice_data(sblobby);
   parsed_blob        pblobby;

   parse_blob(extent_size, page_size, blobby, &pblobby);

   platform_status rc =
      clone_blob_table(cfg, cc, mini, alloc_key, &pblobby, result);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (pblobby.num_extents * extent_size < pblobby.base->length) {
      uint64 start = pblobby.num_extents * extent_size;

      blob_page_iterator src_iter;
      blob_page_iterator dst_iter;
      rc = blob_page_iterator_init(cc, &src_iter, sblobby, start, FALSE, TRUE);
      if (!SUCCESS(rc)) {
         return rc;
      }
      rc = blob_page_iterator_init(
         cc, &dst_iter, writable_buffer_to_slice(result), start, TRUE, TRUE);
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

         uint64 amount_to_copy =
            MIN(slice_length(src_result), slice_length(dst_result));

         memcpy(dst_iter.page->data + dst_iter.page_offset,
                slice_data(src_result),
                amount_to_copy);

         blob_page_iterator_advance_partial(&src_iter, amount_to_copy);
         blob_page_iterator_advance_partial(&dst_iter, amount_to_copy);
      }

      debug_assert(blob_page_iterator_at_end(&src_iter));
      debug_assert(blob_page_iterator_at_end(&dst_iter));

   out:
      blob_page_iterator_deinit(&src_iter);
      blob_page_iterator_deinit(&dst_iter);
   }

   return rc;
}
