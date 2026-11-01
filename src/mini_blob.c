// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0
#include "mini_blob.h"
#include "poison.h"

typedef struct mini_blob_tails {
   const mini_blob_config *cfg;
   mini_allocator         *mini;
} mini_blob_tails;

static blob_build_config
mini_blob_build_config(const mini_blob_config *cfg)
{
   return (blob_build_config){.alignment = cfg->alignment,
                              .blob_max_descriptor_size =
                                 cfg->blob_max_descriptor_size};
}

platform_status
mini_blob_descriptor_size_bound(const mini_blob_config *cfg,
                                uint64                  extent_size,
                                uint64                  length,
                                uint64                 *size)
{
   if (cfg == NULL) {
      return STATUS_BAD_PARAM;
   }
   blob_build_config build = mini_blob_build_config(cfg);
   return blob_descriptor_size_bound(&build, extent_size, length, size);
}

static platform_status
mini_blob_allocate_tail(void  *arg,
                        uint64 bytes,
                        uint64 alignment,
                        uint64 boundary,
                        uint64 addrs[2])
{
   mini_blob_tails *tails = arg;
   mini_allocator  *mini  = tails->mini;
   cache           *cc    = mini->cc;
   uint64           batch =
      boundary == 0 ? tails->cfg->page_batch : tails->cfg->subpage_batch;
   platform_status rc =
      mini_alloc_bytes(mini, batch, bytes, alignment, boundary, addrs, NULL);
   if (!SUCCESS(rc)) {
      return rc;
   }
   uint64 page_size   = cache_page_size(cc);
   uint64 extent_size = cache_extent_size(cc);
   uint64 remaining   = bytes;
   /* Initialize every new page while the batch is locked. Subsequent tail
    * allocations may immediately reuse the last partial page. */
   for (uint64 i = 0; i < 2 && remaining; i++) {
      uint64 count     = MIN(remaining, extent_size - addrs[i] % extent_size);
      uint64 end       = addrs[i] + count;
      uint64 page_addr = (addrs[i] + page_size - 1) / page_size * page_size;
      for (; page_addr < end; page_addr += page_size) {
         page_handle  storage;
         page_handle *page =
            cache_alloc(cc, page_addr, PAGE_TYPE_BLOB, &storage);
         platform_assert(page != NULL);
         cache_unlock(cc, page);
         cache_unclaim(cc, page);
         cache_unget(cc, page);
      }
      remaining -= count;
   }
   mini_alloc_bytes_finish(mini, batch);
   return STATUS_OK;
}

static platform_status
mini_blob_adopt(const mini_blob_config *cfg,
                cache                  *cc,
                mini_allocator         *mini,
                writable_buffer        *result)
{
   parsed_blob     parsed;
   platform_status rc =
      blob_parse_descriptor(cc, writable_buffer_to_slice(result), &parsed);
   platform_assert_status_ok(rc);
   uint64 i = 0;
   for (; i < parsed.num_extents; i++) {
      rc = mini_adopt_blob_ref(mini,
                               cfg->page_batch,
                               blob_get_ref(cc, &parsed, i),
                               blob_root_num_extents(cc, &parsed, i));
      if (!SUCCESS(rc)) {
         break;
      }
   }
   for (; i < parsed.num_extents; i++) {
      (void)blob_ref_release(cc, blob_get_ref(cc, &parsed, i));
   }
   return rc;
}

static platform_status
mini_blob_construct(const mini_blob_config *cfg,
                    cache                  *cc,
                    mini_allocator         *mini,
                    slice                   data,
                    writable_buffer        *result,
                    bool32                  clone)
{
   blob_build_config   build = mini_blob_build_config(cfg);
   mini_blob_tails     state = {cfg, mini};
   blob_tail_allocator tails = {mini_blob_allocate_tail, &state};
   platform_status     rc = clone ? blob_clone(&build, cc, &tails, data, result)
                                  : blob_build(&build, cc, &tails, data, result);
   if (SUCCESS(rc)) {
      rc = mini_blob_adopt(cfg, cc, mini, result);
   }
   if (!SUCCESS(rc)) {
      (void)writable_buffer_resize(result, 0);
   }
   return rc;
}

platform_status
mini_blob_build(const mini_blob_config *cfg,
                cache                  *cc,
                mini_allocator         *mini,
                slice                   data,
                writable_buffer        *result)
{
   return mini_blob_construct(cfg, cc, mini, data, result, FALSE);
}

platform_status
mini_blob_clone(const mini_blob_config *cfg,
                cache                  *cc,
                mini_allocator         *mini,
                slice                   source,
                writable_buffer        *result)
{
   return mini_blob_construct(cfg, cc, mini, source, result, TRUE);
}
