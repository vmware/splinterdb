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
allocate_leftover_entries(const blob_build_config   *cfg,
                          cache                     *cc,
                          const blob_tail_allocator *tails,
                          uint64                     data_len,
                          uint64                     remainder,
                          writable_buffer           *result)
{
   if (remainder != 0 && (tails == NULL || tails->alloc == NULL)) {
      return STATUS_BAD_PARAM;
   }
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
      rc = tails->alloc(tails->arg,
                        num_pages * page_size,
                        MAX(page_size, cfg->alignment),
                        0,
                        alloced_pages);
      if (!SUCCESS(rc)) {
         return rc;
      }

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
      rc = tails->alloc(
         tails->arg, remainder, cfg->alignment, extent_size, alloced_pages);
      if (!SUCCESS(rc)) {
         return rc;
      }
      platform_assert(alloced_pages[1] == 0);
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

/* Construct private, immutable pointer extents. Pages containing used slots
 * and the zero terminator (unless full) are initialized. source is non-NULL
 * when re-encoding a large clone; in that case share data extents while
 * constructing a smaller descriptor. */
static platform_status
build_blob_extent(const blob_build_config   *cfg,
                  cache                     *cc,
                  const blob_tail_allocator *tails,
                  uint64                     height,
                  uint64                     span,
                  uint64                     count,
                  blob_page_iterator        *source,
                  uint64                    *addr)
{
   *addr = 0;
   if (height == 0 && source != NULL) {
      if (!SUCCESS(source->status)) {
         return source->status;
      }
      *addr = source->fragment.addr;
      platform_status rc =
         allocator_inc_ref(cache_get_allocator(cc), *addr, NULL);
      if (SUCCESS(rc)) {
         uint64 bytes = MIN(cache_extent_size(cc),
                            source->pblob.base->length - source->offset);
         blob_page_iterator_advance_bytes(source, bytes);
      }
      return rc;
   }
   platform_status rc =
      allocator_alloc(cache_get_allocator(cc), addr, PAGE_TYPE_BLOB);
   if (!SUCCESS(rc) || height == 0) {
      return rc;
   }
   uint64       child_span   = span / (cache_extent_size(cc) / sizeof(uint64));
   uint64       children     = count / child_span + (count % child_span != 0);
   uint64       per_page     = cache_page_size(cc) / sizeof(uint64);
   page_handle  page_storage = {0};
   page_handle *page         = &page_storage;
   uint64       fanout       = cache_extent_size(cc) / sizeof(uint64);
   for (uint64 i = 0; i <= children && i < fanout; i++) {
      if (i % per_page == 0) {
         if (!page_handle_is_null(page)) {
            cache_unlock(cc, page);
            cache_unclaim(cc, page);
            cache_unget(cc, page);
         }
         cache_alloc(cc, *addr + i * sizeof(uint64), PAGE_TYPE_BLOB, page);
         memset(page_handle_data(page), 0, cache_page_size(cc));
      }
      /* Initialize the terminator even when it is on the next page. */
      if (i == children) {
         break;
      }
      uint64 child;
      uint64 child_count = MIN(count, child_span);
      rc                 = build_blob_extent(
         cfg, cc, tails, height - 1, child_span, child_count, source, &child);
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
   if (!SUCCESS(rc)) {
      /* The first unused slot is zero even after a failed child build. */
      (void)blob_ref_release(cc, blob_ref_create(*addr, height));
      *addr = 0;
   }
   return rc;
}

/* Undo only acquired roots, including partially constructed descriptors. */
static void
release_roots(cache *cc, writable_buffer *result, uint64 roots, uint64 height)
{
   blob *descriptor = writable_buffer_data(result);
   for (uint64 i = 0; i < roots; i++) {
      (void)blob_ref_release(cc, blob_ref_create(descriptor->addrs[i], height));
   }
}

static platform_status
build_blob_table(const blob_build_config   *cfg,
                 cache                     *cc,
                 const blob_tail_allocator *tails,
                 uint64                     data_len,
                 blob_page_iterator        *source,
                 writable_buffer           *result)
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
         cfg, cc, tails, height - 1, span, count, source, &alloced_page);
      if (!SUCCESS(rc)) {
         release_roots(cc, result, i, height - 1);
         return rc;
      }
      blobby->addrs[i] = alloced_page;
      remaining -= count;
   }

   rc = allocate_leftover_entries(cfg, cc, tails, data_len, remainder, result);
   if (!SUCCESS(rc)) {
      release_roots(cc, result, num_extents, height - 1);
   }
   return rc;
}

platform_status
blob_build(const blob_build_config   *cfg,
           cache                     *cc,
           const blob_tail_allocator *tails,
           slice                      data,
           writable_buffer           *result)
{
   platform_status rc =
      build_blob_table(cfg, cc, tails, slice_length(data), NULL, result);
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
      (void)blob_release_refs(cc, writable_buffer_to_slice(result));
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
   } else {
      (void)blob_release_refs(cc, writable_buffer_to_slice(result));
   }
   return rc;
}

static platform_status
clone_blob_table(const blob_build_config   *cfg,
                 cache                     *cc,
                 const blob_tail_allocator *tails,
                 parsed_blob               *pblobby,
                 slice                      sblob,
                 writable_buffer           *result)
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
   uint64 acquired = 0;
   for (; acquired < pblobby->num_extents; acquired++) {
      rc = allocator_inc_ref(
         cache_get_allocator(cc), blobby->addrs[acquired], NULL);
      if (!SUCCESS(rc)) {
         break;
      }
   }
   if (!SUCCESS(rc)) {
      for (uint64 i = 0; i < acquired; i++) {
         (void)blob_ref_release(cc, blob_get_ref(cc, pblobby, i));
      }
      return rc;
   }

   uint64 extent_size = cache_extent_size(cc);
   if (pblobby->height == 1
       && pblobby->num_extents * extent_size < pblobby->base->length)
   {
      uint64 remainder =
         pblobby->base->length - pblobby->num_extents * extent_size;
      rc = allocate_leftover_entries(
         cfg, cc, tails, pblobby->base->length, remainder, result);
   }

   if (!SUCCESS(rc)) {
      for (uint64 i = 0; i < pblobby->num_extents; i++) {
         (void)blob_ref_release(cc, blob_get_ref(cc, pblobby, i));
      }
   }
   return rc;
}

platform_status
blob_clone(const blob_build_config   *cfg,
           cache                     *cc,
           const blob_tail_allocator *tails,
           slice                      sblob,
           writable_buffer           *result)
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
      rc = build_blob_table(
         cfg, cc, tails, pblobby.base->length, &source, result);
      blob_page_iterator_deinit(&source);
      if (SUCCESS(rc)) {
         ((blob *)writable_buffer_data(result))->checksum =
            pblobby.base->checksum;
      }
      return rc;
   }

   rc = clone_blob_table(cfg, cc, tails, &pblobby, sblob, result);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (pblobby.height == 1
       && pblobby.num_extents * extent_size < pblobby.base->length)
   {
      uint64 start = pblobby.num_extents * extent_size;

      auto_writable_buffer staging;
      writable_buffer_init(&staging, result->heap_id);
      uint64             length = pblobby.base->length;
      blob_page_iterator dst_iter;
      rc = blob_page_iterator_init(cc,
                                   &dst_iter,
                                   writable_buffer_to_slice(result),
                                   start,
                                   BLOB_PAGE_ITERATOR_MODE_ALLOC);
      if (!SUCCESS(rc)) {
         (void)blob_release_refs(cc, writable_buffer_to_slice(result));
         return rc;
      }
      while (start < length) {
         uint64 amount = MIN(dst_iter.fragment.length, length - start);
         rc = blob_materialize(cc, sblob, start, start + amount, &staging);
         if (!SUCCESS(rc)) {
            break;
         }
         uint64 offset;
         slice  dst;
         rc = blob_page_iterator_get_curr(&dst_iter, &offset, &dst);
         if (!SUCCESS(rc)) {
            break;
         }
         memcpy(page_handle_data(&dst_iter.page) + dst_iter.fragment.offset,
                writable_buffer_data(&staging),
                amount);
         blob_page_iterator_advance_bytes(&dst_iter, amount);
         /* Release the destination write handle before the next source read,
          * including when a partial page still has space remaining. */
         blob_page_iterator_deinit(&dst_iter);
         start += amount;
         if (start < length) {
            rc = blob_page_iterator_init(cc,
                                         &dst_iter,
                                         writable_buffer_to_slice(result),
                                         start,
                                         BLOB_PAGE_ITERATOR_MODE_ALLOC);
            if (!SUCCESS(rc)) {
               break;
            }
         }
      }
      blob_page_iterator_deinit(&dst_iter);
      if (!SUCCESS(rc)) {
         (void)blob_release_refs(cc, writable_buffer_to_slice(result));
      }
   }
   return rc;
}
