// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "blob_build.h"
#include "poison.h"

static checksum128
checksum_blob_data(slice data)
{
   const void *bytes = slice_length(data) == 0 ? "" : slice_data(data);
   return platform_checksum128(bytes, slice_length(data), BLOB_CHECKSUM_SEED);
}

static platform_status
allocate_leftover_entries(const blob_build_config *cfg,
                          cache                   *cc,
                          mini_allocator          *mini,
                          uint64                   data_len,
                          uint64                   remainder,
                          writable_buffer         *result)
{
   uint64 page_size   = cache_page_size(cc);
   uint64 extent_size = cache_extent_size(cc);

   uint64 num_pages;
   if (can_round_up(page_size, data_len)) {
      num_pages = (remainder + page_size - 1) / page_size;
   } else {
      num_pages = remainder / page_size;
   }

   uint64          alloced_pages[2];
   platform_status rc;
   if (num_pages != 0) {
      rc = mini_alloc_bytes(mini,
                            cfg->page_batch,
                            num_pages * page_size,
                            MAX(page_size, cfg->alignment),
                            0,
                            alloced_pages,
                            NULL);
      if (!SUCCESS(rc)) {
         return rc;
      }
      mini_alloc_bytes_finish(mini, cfg->page_batch);

      if (alloced_pages[1] != 0) {
         rc = writable_buffer_append(
            result, sizeof(alloced_pages), &alloced_pages);
      } else {
         rc = writable_buffer_append(
            result, sizeof(alloced_pages[0]), &alloced_pages[0]);
      }
      if (!SUCCESS(rc)) {
         return rc;
      }

      if (remainder < num_pages * page_size) {
         remainder = 0;
      } else {
         remainder -= num_pages * page_size;
      }
   }

   if (remainder != 0) {
      rc = mini_alloc_bytes(mini,
                            cfg->subpage_batch,
                            remainder,
                            cfg->alignment,
                            extent_size,
                            alloced_pages,
                            NULL);
      if (!SUCCESS(rc)) {
         return rc;
      }
      platform_assert(alloced_pages[1] == 0);

      if (alloced_pages[0] % page_size == 0) {
         page_handle  storage;
         page_handle *page =
            cache_alloc(cc, alloced_pages[0], PAGE_TYPE_BLOB, &storage);
         cache_unlock(cc, page);
         cache_unclaim(cc, page);
         cache_unget(cc, page);
      } else if (alloced_pages[0] / page_size
                 != (alloced_pages[0] + remainder - 1) / page_size)
      {
         uint64 next_page_addr =
            (alloced_pages[0] + remainder - 1) / page_size * page_size;
         page_handle  storage;
         page_handle *page =
            cache_alloc(cc, next_page_addr, PAGE_TYPE_BLOB, &storage);
         cache_unlock(cc, page);
         cache_unclaim(cc, page);
         cache_unget(cc, page);
      }

      mini_alloc_bytes_finish(mini, cfg->subpage_batch);
      rc = writable_buffer_append(
         result, sizeof(alloced_pages[0]), &alloced_pages[0]);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   return STATUS_OK;
}

/* Plan with counts of data extents, avoiding overflowing byte capacities. */
static platform_status
blob_plan(const blob_build_config *cfg,
          uint64                   extent_size,
          uint64                   length,
          uint64                  *height,
          uint64                  *span,
          uint64                  *entries)
{
   uint64 budget = cfg->blob_max_descriptor_size;
   if (budget == 0) {
      budget = BLOB_DEFAULT_MAX_DESCRIPTOR_SIZE;
   }
   if (budget < BLOB_MIN_MAX_DESCRIPTOR_SIZE
       || extent_size % sizeof(uint64) != 0 || extent_size / sizeof(uint64) < 2)
   {
      return STATUS_BAD_PARAM;
   }
   *height = 1;
   *span   = 1;
   if (!can_round_up(extent_size, length)) {
      *entries = length / extent_size + 3;
      return STATUS_OK;
   }
   *entries           = length / extent_size + (length % extent_size != 0);
   uint64 max_entries = (budget - sizeof(blob)) / sizeof(uint64);
   uint64 fanout      = extent_size / sizeof(uint64);
   while (*entries > max_entries) {
      if (*height == BLOB_MAX_HEIGHT || *span > UINT64_MAX / fanout) {
         return STATUS_LIMIT_EXCEEDED;
      }
      *entries = *entries / fanout + (*entries % fanout != 0);
      *span *= fanout;
      (*height)++;
   }
   return STATUS_OK;
}

platform_status
blob_descriptor_size_bound(const blob_build_config *cfg,
                           uint64                   extent_size,
                           uint64                   length,
                           uint64                  *size)
{
   if (cfg == NULL || size == NULL) {
      return STATUS_BAD_PARAM;
   }
   uint64          height, span, entries;
   platform_status rc =
      blob_plan(cfg, extent_size, length, &height, &span, &entries);
   if (!SUCCESS(rc)) {
      return rc;
   }
   if (entries > (UINT64_MAX - sizeof(blob)) / sizeof(uint64)) {
      return STATUS_LIMIT_EXCEEDED;
   }
   *size = sizeof(blob) + entries * sizeof(uint64);
   return STATUS_OK;
}

/* Construct private, immutable pointer extents. Only pages containing used
 * slots are initialized. source is non-NULL when re-encoding a large clone;
 * in that case share data extents while constructing a smaller descriptor. */
static platform_status
build_blob_extent(const blob_build_config *cfg,
                  cache                   *cc,
                  mini_allocator          *mini,
                  uint64                   height,
                  uint64                   span,
                  uint64                   count,
                  blob_page_iterator      *source,
                  uint64                  *addr)
{
   if (height == 0 && source != NULL) {
      if (!SUCCESS(source->status)) {
         return source->status;
      }
      *addr              = source->fragment.addr;
      platform_status rc = mini_attach_extent(mini, cfg->extent_batch, *addr);
      if (SUCCESS(rc)) {
         uint64 bytes = MIN(cache_extent_size(cc),
                            source->pblob.base->length - source->offset);
         blob_page_iterator_advance_bytes(source, bytes);
      }
      return rc;
   }
   platform_status rc = mini_alloc_extent(mini, cfg->extent_batch, addr, NULL);
   if (!SUCCESS(rc) || height == 0) {
      return rc;
   }
   uint64       child_span   = span / (cache_extent_size(cc) / sizeof(uint64));
   uint64       children     = count / child_span + (count % child_span != 0);
   uint64       per_page     = cache_page_size(cc) / sizeof(uint64);
   page_handle  page_storage = {0};
   page_handle *page         = &page_storage;
   for (uint64 i = 0; i < children; i++) {
      if (i % per_page == 0) {
         if (!page_handle_is_null(page)) {
            cache_unlock(cc, page);
            cache_unclaim(cc, page);
            cache_unget(cc, page);
         }
         cache_alloc(cc, *addr + i * sizeof(uint64), PAGE_TYPE_BLOB, page);
         memset(page_handle_data(page), 0, cache_page_size(cc));
      }
      uint64 child;
      uint64 child_count = MIN(count, child_span);
      rc                 = build_blob_extent(
         cfg, cc, mini, height - 1, child_span, child_count, source, &child);
      if (!SUCCESS(rc)) {
         break;
      }
      memcpy(page_handle_data(page) + (i % per_page) * sizeof(uint64),
             &child,
             sizeof(child));
      count -= child_count;
   }
   if (!page_handle_is_null(page)) {
      cache_unlock(cc, page);
      cache_unclaim(cc, page);
      cache_unget(cc, page);
   }
   return rc;
}

static platform_status
build_blob_table(const blob_build_config *cfg,
                 cache                   *cc,
                 mini_allocator          *mini,
                 uint64                   data_len,
                 blob_page_iterator      *source,
                 writable_buffer         *result)
{
   uint64 extent_size = cache_extent_size(cc);
   if (data_len > allocator_get_config(cache_get_allocator(cc))->capacity) {
      return STATUS_LIMIT_EXCEEDED;
   }
   uint64          height, span, num_extents;
   platform_status rc =
      blob_plan(cfg, extent_size, data_len, &height, &span, &num_extents);
   if (!SUCCESS(rc)) {
      return rc;
   }
   uint64 remainder = 0;
   if (!can_round_up(extent_size, data_len)) {
      num_extents = data_len / extent_size;
      remainder   = data_len % extent_size;
   }

   rc = writable_buffer_resize(result,
                               sizeof(blob) + num_extents * sizeof(uint64));
   if (!SUCCESS(rc)) {
      return rc;
   }

   blob *blobby     = writable_buffer_data(result);
   blobby->length   = data_len;
   blobby->checksum = (checksum128){0};
   blobby->format   = BLOB_FORMAT;

   uint64 remaining = data_len / extent_size + (data_len % extent_size != 0);
   for (uint64 i = 0; i < num_extents; i++) {
      uint64 alloced_page;
      uint64 count = MIN(remaining, span);
      rc           = build_blob_extent(
         cfg, cc, mini, height - 1, span, count, source, &alloced_page);
      if (!SUCCESS(rc)) {
         return rc;
      }
      blobby->addrs[i] = alloced_page;
      remaining -= count;
   }

   return allocate_leftover_entries(cfg, cc, mini, data_len, remainder, result);
}

platform_status
blob_build(const blob_build_config *cfg,
           cache                   *cc,
           mini_allocator          *mini,
           slice                    data,
           writable_buffer         *result)
{
   platform_status rc =
      build_blob_table(cfg, cc, mini, slice_length(data), NULL, result);
   if (!SUCCESS(rc)) {
      return rc;
   }

   blob_page_iterator iter;
   rc = blob_page_iterator_init(cc,
                                &iter,
                                writable_buffer_to_slice(result),
                                0,
                                BLOB_PAGE_ITERATOR_MODE_ALLOC);
   if (!SUCCESS(rc)) {
      return rc;
   }

   const char *raw_data = slice_data(data);
   while (!blob_page_iterator_at_end(&iter)) {
      uint64 offset;
      slice  dst;
      rc = blob_page_iterator_get_curr(&iter, &offset, &dst);
      if (!SUCCESS(rc)) {
         goto out;
      }

      debug_assert(offset + slice_length(dst) <= slice_length(data));
      memcpy(page_handle_data(&iter.page) + iter.fragment.offset,
             raw_data + offset,
             slice_length(dst));

      blob_page_iterator_advance_page(&iter);
   }

out:
   blob_page_iterator_deinit(&iter);
   if (SUCCESS(rc)) {
      blob *blobby     = writable_buffer_data(result);
      blobby->checksum = checksum_blob_data(data);
   }
   return rc;
}

typedef struct blob_clone_extents {
   mini_allocator *mini;
   uint64          batch;
} blob_clone_extents;

static platform_status
blob_attach_extent(cache *cc, uint64 addr, void *arg)
{
   (void)cc;
   blob_clone_extents *state = arg;
   return mini_attach_extent(state->mini, state->batch, addr);
}

static platform_status
clone_blob_table(const blob_build_config *cfg,
                 cache                   *cc,
                 mini_allocator          *mini,
                 parsed_blob             *pblobby,
                 slice                    sblob,
                 writable_buffer         *result)
{
   platform_status rc = writable_buffer_resize(
      result, sizeof(blob) + pblobby->num_extents * sizeof(uint64));
   if (!SUCCESS(rc)) {
      return rc;
   }

   blob *blobby = writable_buffer_data(result);
   memcpy(blobby, pblobby->base, sizeof(*blobby));

   memcpy(blobby->addrs,
          pblobby->base->addrs,
          pblobby->num_extents * sizeof(uint64));
   blob_clone_extents state = {.mini = mini, .batch = cfg->extent_batch};
   rc = blob_walk_extents(cc, sblob, FALSE, blob_attach_extent, &state);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64 extent_size = cache_extent_size(cc);
   if (pblobby->height == 1
       && pblobby->num_extents * extent_size < pblobby->base->length)
   {
      uint64 remainder =
         pblobby->base->length - pblobby->num_extents * extent_size;
      rc = allocate_leftover_entries(
         cfg, cc, mini, pblobby->base->length, remainder, result);
   }

   return rc;
}

platform_status
blob_clone(const blob_build_config *cfg,
           cache                   *cc,
           mini_allocator          *mini,
           slice                    sblob,
           writable_buffer         *result)
{
   if (slice_length(sblob) < sizeof(blob)) {
      return STATUS_INVALID_STATE;
   }

   uint64          extent_size = cache_extent_size(cc);
   parsed_blob     pblobby;
   platform_status rc = blob_parse_descriptor(cc, sblob, &pblobby);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64 budget = cfg->blob_max_descriptor_size != 0
                      ? cfg->blob_max_descriptor_size
                      : BLOB_DEFAULT_MAX_DESCRIPTOR_SIZE;
   if (budget < BLOB_MIN_MAX_DESCRIPTOR_SIZE) {
      return STATUS_BAD_PARAM;
   }
   if (slice_length(sblob) > budget) {
      /* Packed-tail descriptors always fit the minimum budget. */
      blob_page_iterator source;
      rc = blob_page_iterator_init(
         cc, &source, sblob, 0, BLOB_PAGE_ITERATOR_MODE_NO_PREFETCH);
      if (!SUCCESS(rc)) {
         return rc;
      }
      rc =
         build_blob_table(cfg, cc, mini, pblobby.base->length, &source, result);
      blob_page_iterator_deinit(&source);
      if (SUCCESS(rc)) {
         ((blob *)writable_buffer_data(result))->checksum =
            pblobby.base->checksum;
      }
      return rc;
   }

   rc = clone_blob_table(cfg, cc, mini, &pblobby, sblob, result);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (pblobby.height == 1
       && pblobby.num_extents * extent_size < pblobby.base->length)
   {
      uint64 start = pblobby.num_extents * extent_size;

      blob_page_iterator src_iter;
      blob_page_iterator dst_iter;
      rc = blob_page_iterator_init(
         cc, &src_iter, sblob, start, BLOB_PAGE_ITERATOR_MODE_PREFETCH);
      if (!SUCCESS(rc)) {
         return rc;
      }
      rc = blob_page_iterator_init(cc,
                                   &dst_iter,
                                   writable_buffer_to_slice(result),
                                   start,
                                   BLOB_PAGE_ITERATOR_MODE_ALLOC);
      if (!SUCCESS(rc)) {
         blob_page_iterator_deinit(&src_iter);
         return rc;
      }

      while (!blob_page_iterator_at_end(&src_iter)
             && !blob_page_iterator_at_end(&dst_iter))
      {
         uint64 src_offset;
         slice  src_data;
         uint64 dst_offset;
         slice  dst_data;
         rc = blob_page_iterator_get_curr(&src_iter, &src_offset, &src_data);
         if (!SUCCESS(rc)) {
            goto out;
         }
         rc = blob_page_iterator_get_curr(&dst_iter, &dst_offset, &dst_data);
         if (!SUCCESS(rc)) {
            goto out;
         }

         platform_assert(src_offset == dst_offset);
         uint64 amount_to_copy =
            MIN(slice_length(src_data), slice_length(dst_data));
         memcpy(page_handle_data(&dst_iter.page) + dst_iter.fragment.offset,
                slice_data(src_data),
                amount_to_copy);

         blob_page_iterator_advance_bytes(&src_iter, amount_to_copy);
         blob_page_iterator_advance_bytes(&dst_iter, amount_to_copy);
      }

      debug_assert(blob_page_iterator_at_end(&src_iter));
      debug_assert(blob_page_iterator_at_end(&dst_iter));

   out:
      blob_page_iterator_deinit(&src_iter);
      blob_page_iterator_deinit(&dst_iter);
   }

   return rc;
}
