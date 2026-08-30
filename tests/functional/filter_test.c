// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * filter_test.c --
 *
 *     This file contains the test interfaces for Alex's filter
 */
#include "splinterdb/data.h"
#include "test.h"
#include "routing_filter.h"
#include "allocator.h"
#include "rc_allocator.h"
#include "cache.h"
#include "clockcache.h"
#include "util.h"
#include "platform_time.h"
#include "platform_typed_alloc.h"
#include "platform_assert.h"
#include "poison.h"

typedef struct ONDISK test_routing_hdr {
   uint16 num_remainders;
   char   encoding[];
} test_routing_hdr;

typedef struct ONDISK test_mini_meta_hdr {
   uint64 next_meta_addr;
   uint64 prev_meta_addr;
   uint64 meta_page_no;
   uint64 pos;
   uint32 num_entries;
   char   entry_buffer[];
} test_mini_meta_hdr;

typedef struct ONDISK test_mini_meta_entry {
   uint64 packed;
} test_mini_meta_entry;

static page_handle *
test_filter_claim_page(cache *cc, uint64 addr, page_type type)
{
   page_handle *page = cache_get(cc, addr, TRUE, type);
   platform_assert(page != NULL);
   while (!cache_try_claim(cc, page)) {
      cache_unget(cc, page);
      page = cache_get(cc, addr, TRUE, type);
      platform_assert(page != NULL);
   }
   cache_lock(cc, page);
   return page;
}

static void
test_filter_unclaim_page(cache *cc, page_handle *page)
{
   cache_unlock(cc, page);
   cache_unclaim(cc, page);
   cache_unget(cc, page);
}

static platform_status
test_filter_async_lookup_status(cache                *cc,
                                const routing_config *cfg,
                                routing_filter       *filter,
                                key                   target)
{
   uint64 found_values;
   return async_call_sync_callback(cache_cleanup(cc),
                                   routing_filter_lookup_async,
                                   cc,
                                   cfg,
                                   *filter,
                                   target,
                                   &found_values);
}

static bool32
test_filter_lookup_rejects(cache                *cc,
                           const routing_config *cfg,
                           routing_filter       *filter,
                           key                   target)
{
   uint64 found_values;
   platform_status sync_rc =
      routing_filter_lookup(cc, cfg, filter, target, &found_values);
   platform_status async_rc =
      test_filter_async_lookup_status(cc, cfg, filter, target);
   return !SUCCESS(sync_rc) && !SUCCESS(async_rc);
}

static platform_status
test_filter_malicious_metadata(cache           *cc,
                               routing_config  *cfg,
                               platform_heap_id hid)
{
   platform_default_log(
      "filter_test: malicious routing/mini metadata test started\n");

   auto_writable_buffer keywb = NULL_WRITABLE_BUFFER(hid);
   platform_status rc = writable_buffer_resize(&keywb, sizeof(uint64));
   if (!SUCCESS(rc)) {
      return rc;
   }
   uint64 *keybuf = writable_buffer_data(&keywb);
   *keybuf        = 42;
   key target     = key_create(TRUE, sizeof(*keybuf), keybuf);
   uint32 fp      = data_key_hash(cfg->data_cfg, target, cfg->seed);

   routing_filter null_filter = NULL_ROUTING_FILTER;
   routing_filter filter      = NULL_ROUTING_FILTER;
   rc = routing_filter_add(cc, cfg, &null_filter, &filter, &fp, 1, 0);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64 found_values;
   rc = routing_filter_lookup(cc, cfg, &filter, target, &found_values);
   if (!SUCCESS(rc)
       || !SUCCESS(test_filter_async_lookup_status(cc, cfg, &filter, target)))
   {
      rc = STATUS_TEST_FAILED;
      goto out;
   }

   routing_filter invalid_filter = filter;
   invalid_filter.value_size     = 6;
   if (!test_filter_lookup_rejects(cc, cfg, &invalid_filter, target)) {
      rc = STATUS_TEST_FAILED;
      goto out;
   }

   uint64 page_size = cache_page_size(cc);
   page_handle *index_page =
      test_filter_claim_page(cc, filter.addr, PAGE_TYPE_FILTER);
   uint64 saved_hdr_raw_addr = ((uint64 *)index_page->data)[0];
   ((uint64 *)index_page->data)[0] =
      saved_hdr_raw_addr - saved_hdr_raw_addr % page_size + page_size - 1;
   test_filter_unclaim_page(cc, index_page);
   bool32 rejected = test_filter_lookup_rejects(cc, cfg, &filter, target);
   index_page = test_filter_claim_page(cc, filter.addr, PAGE_TYPE_FILTER);
   ((uint64 *)index_page->data)[0] = saved_hdr_raw_addr;
   test_filter_unclaim_page(cc, index_page);
   if (!rejected) {
      rc = STATUS_TEST_FAILED;
      goto out;
   }

   uint64 header_addr =
      saved_hdr_raw_addr - saved_hdr_raw_addr % page_size;
   uint64 header_off = saved_hdr_raw_addr - header_addr;
   page_handle *header_page =
      test_filter_claim_page(cc, header_addr, PAGE_TYPE_FILTER);
   test_routing_hdr *hdr =
      (test_routing_hdr *)(header_page->data + header_off);
   uint16 saved_num_remainders = hdr->num_remainders;
   hdr->num_remainders         = UINT16_MAX;
   test_filter_unclaim_page(cc, header_page);
   rejected = test_filter_lookup_rejects(cc, cfg, &filter, target);
   header_page = test_filter_claim_page(cc, header_addr, PAGE_TYPE_FILTER);
   hdr = (test_routing_hdr *)(header_page->data + header_off);
   hdr->num_remainders = saved_num_remainders;
   test_filter_unclaim_page(cc, header_page);
   if (!rejected) {
      rc = STATUS_TEST_FAILED;
      goto out;
   }

   uint64 encoding_size =
      (saved_num_remainders + cfg->index_size - 1) / 8 + 4;
   char *saved_encoding = TYPED_ARRAY_MALLOC(
      hid, saved_encoding, encoding_size);
   if (saved_encoding == NULL) {
      rc = STATUS_NO_MEMORY;
      goto out;
   }
   header_page = test_filter_claim_page(cc, header_addr, PAGE_TYPE_FILTER);
   hdr = (test_routing_hdr *)(header_page->data + header_off);
   memcpy(saved_encoding, hdr->encoding, encoding_size);
   memset(hdr->encoding, 0, encoding_size);
   test_filter_unclaim_page(cc, header_page);
   rejected = test_filter_lookup_rejects(cc, cfg, &filter, target);
   header_page = test_filter_claim_page(cc, header_addr, PAGE_TYPE_FILTER);
   hdr = (test_routing_hdr *)(header_page->data + header_off);
   memcpy(hdr->encoding, saved_encoding, encoding_size);
   test_filter_unclaim_page(cc, header_page);
   platform_free(hid, saved_encoding);
   if (!rejected) {
      rc = STATUS_TEST_FAILED;
      goto out;
   }

   routing_filter empty_filters[MAX_FILTERS] = {{0}};
   uint32 estimated_unique;
   rc = routing_filter_estimate_unique_fp(
      cc, cfg, hid, empty_filters, MAX_FILTERS, &estimated_unique);
   if (!SUCCESS(rc) || estimated_unique != 0
       || SUCCESS(routing_filter_estimate_unique_fp(
          cc, cfg, hid, empty_filters, MAX_FILTERS + 1, &estimated_unique)))
   {
      rc = STATUS_TEST_FAILED;
      goto out;
   }

   page_handle *meta_page =
      test_filter_claim_page(cc, filter.meta_head, PAGE_TYPE_FILTER);
   test_mini_meta_hdr *meta_hdr = (test_mini_meta_hdr *)meta_page->data;
   if (meta_hdr->num_entries == 0) {
      test_filter_unclaim_page(cc, meta_page);
      rc = STATUS_TEST_FAILED;
      goto out;
   }
   test_mini_meta_entry *entry =
      (test_mini_meta_entry *)meta_hdr->entry_buffer;
   uint64 target_extent = (entry->packed >> 16) * cache_extent_size(cc);
   if (meta_hdr->meta_page_no != 0) {
      test_filter_unclaim_page(cc, meta_page);
      rc = STATUS_TEST_FAILED;
      goto out;
   }
   test_filter_unclaim_page(cc, meta_page);

   mini_meta_cursor cursor = {0};
   mini_meta_cursor_status cursor_rc = mini_meta_cursor_init(
      &cursor, cc, PAGE_TYPE_FILTER, filter.meta_head, target_extent);
   if (cursor_rc != MINI_META_CURSOR_ENTRY) {
      mini_meta_cursor_deinit(&cursor);
      rc = STATUS_TEST_FAILED;
      goto out;
   }
   mini_meta_cursor_deinit(&cursor);

   /* The head is page zero. A non-zero ordinal must be rejected even by a
    * cursor initialized in the middle of a stream. */
   meta_page =
      test_filter_claim_page(cc, filter.meta_head, PAGE_TYPE_FILTER);
   meta_hdr               = (test_mini_meta_hdr *)meta_page->data;
   meta_hdr->meta_page_no = 1;
   test_filter_unclaim_page(cc, meta_page);
   cursor_rc = mini_meta_cursor_init(
      &cursor, cc, PAGE_TYPE_FILTER, filter.meta_head, target_extent);
   mini_meta_cursor_deinit(&cursor);
   meta_page =
      test_filter_claim_page(cc, filter.meta_head, PAGE_TYPE_FILTER);
   meta_hdr               = (test_mini_meta_hdr *)meta_page->data;
   meta_hdr->meta_page_no = 0;
   test_filter_unclaim_page(cc, meta_page);
   if (cursor_rc != MINI_META_CURSOR_INVALID) {
      rc = STATUS_TEST_FAILED;
      goto out;
   }

   /* Build a temporary second metadata page inside the already-owned metadata
    * extent. This exercises exact +1/-1 cursor validation without allocating
    * hundreds of data extents merely to fill the real head page. */
   meta_page =
      test_filter_claim_page(cc, filter.meta_head, PAGE_TYPE_FILTER);
   meta_hdr = (test_mini_meta_hdr *)meta_page->data;
   uint64 head_next_meta_addr = meta_hdr->next_meta_addr;
   uint32 head_num_entries    = meta_hdr->num_entries;
   test_mini_meta_entry head_entry =
      *(test_mini_meta_entry *)meta_hdr->entry_buffer;
   test_filter_unclaim_page(cc, meta_page);
   uint64 second_meta_addr = filter.meta_head + page_size;
   if (head_next_meta_addr != 0 || head_num_entries == 0
       || second_meta_addr / cache_extent_size(cc)
             != filter.meta_head / cache_extent_size(cc))
   {
      rc = STATUS_TEST_FAILED;
      goto out;
   }

   page_handle *second_meta_page =
      cache_alloc(cc, second_meta_addr, PAGE_TYPE_FILTER);
   memset(second_meta_page->data, 0, page_size);
   test_mini_meta_hdr *second_meta_hdr =
      (test_mini_meta_hdr *)second_meta_page->data;
   second_meta_hdr->prev_meta_addr = filter.meta_head;
   second_meta_hdr->meta_page_no   = 1;
   second_meta_hdr->num_entries    = 1;
   second_meta_hdr->pos = offsetof(test_mini_meta_hdr, entry_buffer)
                           + sizeof(test_mini_meta_entry);
   *(test_mini_meta_entry *)second_meta_hdr->entry_buffer = head_entry;
   test_filter_unclaim_page(cc, second_meta_page);

   meta_page =
      test_filter_claim_page(cc, filter.meta_head, PAGE_TYPE_FILTER);
   meta_hdr                 = (test_mini_meta_hdr *)meta_page->data;
   meta_hdr->next_meta_addr = second_meta_addr;
   test_filter_unclaim_page(cc, meta_page);

   bool32 sequence_ok = TRUE;
   cursor_rc = mini_meta_cursor_init(
      &cursor, cc, PAGE_TYPE_FILTER, filter.meta_head, target_extent);
   for (uint32 i = 0;
        cursor_rc == MINI_META_CURSOR_ENTRY && i < head_num_entries;
        i++)
   {
      cursor_rc = mini_meta_cursor_next(&cursor);
   }
   sequence_ok &= cursor_rc == MINI_META_CURSOR_ENTRY
                  && cursor.meta_addr == second_meta_addr;
   mini_meta_cursor_deinit(&cursor);

   second_meta_page =
      test_filter_claim_page(cc, second_meta_addr, PAGE_TYPE_FILTER);
   second_meta_hdr               = (test_mini_meta_hdr *)second_meta_page->data;
   second_meta_hdr->meta_page_no = 2;
   test_filter_unclaim_page(cc, second_meta_page);
   cursor_rc = mini_meta_cursor_init(
      &cursor, cc, PAGE_TYPE_FILTER, filter.meta_head, target_extent);
   for (uint32 i = 0;
        cursor_rc == MINI_META_CURSOR_ENTRY && i < head_num_entries;
        i++)
   {
      cursor_rc = mini_meta_cursor_next(&cursor);
   }
   sequence_ok &= cursor_rc == MINI_META_CURSOR_INVALID;
   mini_meta_cursor_deinit(&cursor);

   /* Starting in the middle must validate the backward -1 transition too. */
   cursor_rc = mini_meta_cursor_init(
      &cursor, cc, PAGE_TYPE_FILTER, second_meta_addr, target_extent);
   sequence_ok &= cursor_rc == MINI_META_CURSOR_ENTRY;
   if (cursor_rc == MINI_META_CURSOR_ENTRY) {
      cursor_rc = mini_meta_cursor_prev(&cursor);
      sequence_ok &= cursor_rc == MINI_META_CURSOR_INVALID;
   }
   mini_meta_cursor_deinit(&cursor);

   second_meta_page =
      test_filter_claim_page(cc, second_meta_addr, PAGE_TYPE_FILTER);
   second_meta_hdr               = (test_mini_meta_hdr *)second_meta_page->data;
   second_meta_hdr->meta_page_no = 1;
   test_filter_unclaim_page(cc, second_meta_page);
   cursor_rc = mini_meta_cursor_init(
      &cursor, cc, PAGE_TYPE_FILTER, second_meta_addr, target_extent);
   sequence_ok &= cursor_rc == MINI_META_CURSOR_ENTRY;
   if (cursor_rc == MINI_META_CURSOR_ENTRY) {
      cursor_rc = mini_meta_cursor_prev(&cursor);
      sequence_ok &= cursor_rc == MINI_META_CURSOR_ENTRY
                     && cursor.meta_addr == filter.meta_head;
   }
   mini_meta_cursor_deinit(&cursor);

   meta_page =
      test_filter_claim_page(cc, filter.meta_head, PAGE_TYPE_FILTER);
   meta_hdr                 = (test_mini_meta_hdr *)meta_page->data;
   meta_hdr->next_meta_addr = head_next_meta_addr;
   test_filter_unclaim_page(cc, meta_page);
   if (!sequence_ok) {
      rc = STATUS_TEST_FAILED;
      goto out;
   }

   meta_page =
      test_filter_claim_page(cc, filter.meta_head, PAGE_TYPE_FILTER);
   meta_hdr = (test_mini_meta_hdr *)meta_page->data;
   uint64 saved_pos        = meta_hdr->pos;
   meta_hdr->pos           = saved_pos + 1;
   test_filter_unclaim_page(cc, meta_page);
   cursor_rc = mini_meta_cursor_init(
      &cursor, cc, PAGE_TYPE_FILTER, filter.meta_head, target_extent);
   mini_meta_cursor_deinit(&cursor);
   meta_page =
      test_filter_claim_page(cc, filter.meta_head, PAGE_TYPE_FILTER);
   meta_hdr      = (test_mini_meta_hdr *)meta_page->data;
   meta_hdr->pos = saved_pos;
   test_filter_unclaim_page(cc, meta_page);
   if (cursor_rc != MINI_META_CURSOR_INVALID) {
      rc = STATUS_TEST_FAILED;
      goto out;
   }

   /* The recovery walker uses the same page validator, but runs before the
    * allocator considers metadata pages allocated. Verify both rejection and
    * a clean retry from a freshly reset recovery map. */
   meta_page =
      test_filter_claim_page(cc, filter.meta_head, PAGE_TYPE_FILTER);
   meta_hdr      = (test_mini_meta_hdr *)meta_page->data;
   meta_hdr->pos = saved_pos + 1;
   test_filter_unclaim_page(cc, meta_page);
   allocator *al = cache_get_allocator(cc);
   rc = allocator_recovery_begin(al);
   if (!SUCCESS(rc)) {
      goto out;
   }
   rc = mini_recover_references(cc, filter.meta_head, PAGE_TYPE_FILTER);
   if (SUCCESS(rc)) {
      rc = STATUS_TEST_FAILED;
      goto out;
   }
   meta_page =
      test_filter_claim_page(cc, filter.meta_head, PAGE_TYPE_FILTER);
   meta_hdr      = (test_mini_meta_hdr *)meta_page->data;
   meta_hdr->pos = saved_pos;
   test_filter_unclaim_page(cc, meta_page);
   rc = allocator_recovery_begin(al);
   if (!SUCCESS(rc)) {
      goto out;
   }
   rc = mini_recover_references(cc, filter.meta_head, PAGE_TYPE_FILTER);
   if (!SUCCESS(rc)) {
      goto out;
   }
   allocator_recovery_finish(al);

   meta_page =
      test_filter_claim_page(cc, filter.meta_head, PAGE_TYPE_FILTER);
   meta_hdr = (test_mini_meta_hdr *)meta_page->data;
   entry    = (test_mini_meta_entry *)meta_hdr->entry_buffer;
   uint64 saved_entry = entry->packed;
   entry->packed      = (saved_entry & ~UINT64_C(0xff)) | UINT64_C(0xff);
   test_filter_unclaim_page(cc, meta_page);
   cursor_rc = mini_meta_cursor_init(
      &cursor, cc, PAGE_TYPE_FILTER, filter.meta_head, target_extent);
   mini_meta_cursor_deinit(&cursor);
   meta_page =
      test_filter_claim_page(cc, filter.meta_head, PAGE_TYPE_FILTER);
   meta_hdr      = (test_mini_meta_hdr *)meta_page->data;
   entry         = (test_mini_meta_entry *)meta_hdr->entry_buffer;
   entry->packed = saved_entry;
   test_filter_unclaim_page(cc, meta_page);
   if (cursor_rc != MINI_META_CURSOR_INVALID) {
      rc = STATUS_TEST_FAILED;
      goto out;
   }

   rc = STATUS_OK;

out:
   routing_filter_dec_ref(cc, &filter);
   return rc;
}

static platform_status
test_filter_index_expansion_unique_count(cache           *cc,
                                         routing_config  *cfg,
                                         platform_heap_id hid)
{
   platform_default_log(
      "filter_test: routing filter index expansion test started\n");

   platform_assert(cfg->log_index_size + 2 <= cfg->fingerprint_size);
   platform_assert(cfg->log_index_size < 30);

   /* The old filter has two indices and the combined filter has four.  Offset
    * the new fingerprints by half a stride so both sets remain unique while
    * every old index crosses a new subpartition boundary. */
   uint32  num_fingerprints         = 1U << (cfg->log_index_size + 1);
   uint32  fingerprint_stride_shift = 32 - (cfg->log_index_size + 1);
   uint32  midpoint_bit             = 1U << (fingerprint_stride_shift - 1);
   uint32 *old_fp_arr = TYPED_ARRAY_MALLOC(hid, old_fp_arr, num_fingerprints);
   uint32 *new_fp_arr = TYPED_ARRAY_MALLOC(hid, new_fp_arr, num_fingerprints);
   if (old_fp_arr == NULL || new_fp_arr == NULL) {
      platform_free(hid, old_fp_arr);
      platform_free(hid, new_fp_arr);
      return STATUS_NO_MEMORY;
   }

   for (uint32 i = 0; i < num_fingerprints; i++) {
      old_fp_arr[i] = i << fingerprint_stride_shift;
      new_fp_arr[i] = old_fp_arr[i] | midpoint_bit;
   }

   routing_filter  null_filter     = NULL_ROUTING_FILTER;
   routing_filter  old_filter      = NULL_ROUTING_FILTER;
   routing_filter  expanded_filter = NULL_ROUTING_FILTER;
   platform_status rc              = routing_filter_add(
      cc, cfg, &null_filter, &old_filter, old_fp_arr, num_fingerprints, 0);
   if (!SUCCESS(rc)) {
      goto out;
   }
   platform_assert(old_filter.num_fingerprints == num_fingerprints);
   platform_assert(old_filter.num_unique == num_fingerprints);

   rc = routing_filter_add(
      cc, cfg, &old_filter, &expanded_filter, new_fp_arr, num_fingerprints, 1);
   if (!SUCCESS(rc)) {
      goto out;
   }
   platform_assert(expanded_filter.num_fingerprints == 2 * num_fingerprints);
   platform_assert(expanded_filter.num_unique == 2 * num_fingerprints,
                   "num_unique=%u, expected=%u\n",
                   expanded_filter.num_unique,
                   2 * num_fingerprints);

out:
   routing_filter_dec_ref(cc, &expanded_filter);
   routing_filter_dec_ref(cc, &old_filter);
   platform_free(hid, new_fp_arr);
   platform_free(hid, old_fp_arr);
   return rc;
}

static platform_status
test_filter_basic(cache           *cc,
                  routing_config  *cfg,
                  platform_heap_id hid,
                  uint64           key_size,
                  uint64           num_fingerprints,
                  uint64           num_values)
{
   platform_default_log("filter_test: routing filter basic test started\n");
   platform_status rc = STATUS_OK;

   if (key_size < sizeof(uint64)) {
      platform_default_log("key_size %lu too small\n", key_size);
      return STATUS_BAD_PARAM;
   }

   uint32 **fp_arr = TYPED_ARRAY_MALLOC(hid, fp_arr, num_values);
   for (uint64 i = 0; i < num_values; i++) {
      fp_arr[i] = TYPED_ARRAY_MALLOC(hid, fp_arr[i], num_fingerprints);
   }

   bool32 *used_keys =
      TYPED_ARRAY_ZALLOC(hid, used_keys, (num_values + 1) * num_fingerprints);

   uint32 *num_input_keys = TYPED_ARRAY_ZALLOC(hid, num_input_keys, num_values);

   auto_writable_buffer keywb = NULL_WRITABLE_BUFFER(hid);
   writable_buffer_resize(&keywb, key_size);
   writable_buffer_memset(&keywb, 0);
   uint64 *keybuf = writable_buffer_data(&keywb);
   key     target = key_create(TRUE, key_size, keybuf);
   for (uint64 i = 0; i < num_values; i++) {
      if (i != 0) {
         num_input_keys[i] = num_input_keys[i - 1];
      }
      for (uint64 j = 0; j < num_fingerprints; j++) {
         if (!used_keys[(i + 1) * j]) {
            used_keys[(i + 1) * j] = TRUE;
            num_input_keys[i]++;
         }
         *keybuf      = (i + 1) * j;
         fp_arr[i][j] = data_key_hash(cfg->data_cfg, target, cfg->seed);
      }
   }

   platform_free(hid, used_keys);

   routing_filter filter[MAX_FILTERS] = {{0}};
   for (uint64 i = 0; i < num_values; i++) {
      rc = routing_filter_add(
         cc, cfg, &filter[i], &filter[i + 1], fp_arr[i], num_fingerprints, i);
      // platform_default_log("FILTER %lu\n", i);
      // routing_filter_print(cc, cfg, &filter);
      uint32 estimated_input_keys =
         routing_filter_estimate_unique_keys(&filter[i + 1], cfg);

      platform_default_log("num input keys %8u estimate %8u num_unique %u\n",
                           num_input_keys[i],
                           estimated_input_keys,
                           filter[i + 1].num_unique);
   }

   uint32 num_unique;
   rc = routing_filter_estimate_unique_fp(
      cc, cfg, hid, filter + 1, num_values, &num_unique);
   if (!SUCCESS(rc)) {
      for (uint64 i = 0; i < num_values; i++) {
         routing_filter_dec_ref(cc, &filter[i + 1]);
      }
      goto out;
   }
   num_unique = routing_filter_estimate_unique_keys_from_count(cfg, num_unique);
   platform_default_log("across filters: num input keys %8u estimate %8u\n",
                        num_input_keys[num_values - 1],
                        num_unique);

   platform_free(hid, num_input_keys);

   for (uint64 i = 0; i < num_values; i++) {
      for (uint64 j = 0; j < num_fingerprints; j++) {
         *keybuf = (i + 1) * j;
         uint64 found_values;
         rc = routing_filter_lookup(
            cc, cfg, &filter[i + 1], target, &found_values);
         platform_assert_status_ok(rc);
         if (!routing_filter_is_value_found(found_values, i)) {
            platform_default_log(
               "key-value pair (%lu, %lu) not found in filter\n",
               (i + 1) * j,
               i);
            rc = STATUS_NOT_FOUND;
            goto out;
         }
      }
   }

   uint64 unused_key      = (num_values + 1) * num_fingerprints;
   uint64 false_positives = 0;
   for (uint64 i = unused_key; i < unused_key + num_fingerprints; i++) {
      *keybuf = i;
      uint64 found_values;
      rc = routing_filter_lookup(
         cc, cfg, &filter[num_values], target, &found_values);
      if (found_values) {
         false_positives++;
      }
   }

   fraction false_positive_rate =
      init_fraction(false_positives, num_fingerprints);
   platform_default_log(
      "routing filter basic test: false positive rate " FRACTION_FMT(1, 4) "\n",
      FRACTION_ARGS(false_positive_rate));

   for (uint64 i = 0; i < num_values; i++) {
      routing_filter_dec_ref(cc, &filter[i + 1]);
   }

out:
   if (fp_arr) {
      for (uint64 i = 0; i < num_values; i++) {
         platform_free(hid, fp_arr[i]);
      }
   }
   platform_free(hid, fp_arr);
   return rc;
}

static platform_status
test_filter_perf(cache           *cc,
                 routing_config  *cfg,
                 platform_heap_id hid,
                 uint64           key_size,
                 uint64           num_fingerprints,
                 uint64           num_values,
                 uint64           num_trees)
{
   platform_default_log("filter_test: routing filter perf test started\n");
   platform_status rc = STATUS_OK;

   if (key_size < sizeof(uint64)) {
      platform_default_log("key_size %lu too small\n", key_size);
      return STATUS_BAD_PARAM;
   }

   uint32 *fp_arr = TYPED_ARRAY_MALLOC(
      hid, fp_arr, num_trees * num_values * num_fingerprints);
   if (fp_arr == NULL) {
      return STATUS_NO_MEMORY;
   }
   auto_writable_buffer keywb = NULL_WRITABLE_BUFFER(hid);
   writable_buffer_resize(&keywb, key_size);
   writable_buffer_memset(&keywb, 0);
   uint64 *keybuf = writable_buffer_data(&keywb);
   key     target = key_create(TRUE, key_size, keybuf);
   for (uint64 k = 0; k < num_trees; k++) {
      for (uint64 i = 0; i < num_values * num_fingerprints; i++) {
         uint64 idx  = k * num_values * num_fingerprints + i;
         *keybuf     = idx;
         fp_arr[idx] = data_key_hash(cfg->data_cfg, target, cfg->seed);
      }
   }

   uint64          start_time = platform_get_timestamp();
   routing_filter *filter     = TYPED_ARRAY_ZALLOC(hid, filter, num_trees);
   for (uint64 k = 0; k < num_trees; k++) {
      for (uint64 i = 0; i < num_values; i++) {
         routing_filter new_filter = {0};
         uint64         fp_start =
            k * num_fingerprints * num_values + i * num_fingerprints;
         platform_status rc = routing_filter_add(cc,
                                                 cfg,
                                                 &filter[k],
                                                 &new_filter,
                                                 &fp_arr[fp_start],
                                                 num_fingerprints,
                                                 i);
         if (!SUCCESS(rc)) {
            goto out;
         }
         routing_filter_dec_ref(cc, &filter[k]);
         filter[k] = new_filter;
      }
   }
   platform_default_log("filter insert time per key %lu\n",
                        platform_timestamp_elapsed(start_time)
                           / (num_fingerprints * num_values * num_trees));

   start_time = platform_get_timestamp();
   for (uint64 k = 0; k < num_trees; k++) {
      for (uint64 i = 0; i < num_values * num_fingerprints; i++) {
         *keybuf = k * num_values * num_fingerprints + i;
         uint64 found_values;
         rc = routing_filter_lookup(cc, cfg, &filter[k], target, &found_values);
         platform_assert_status_ok(rc);
         if (!routing_filter_is_value_found(found_values, i / num_fingerprints))
         {
            platform_default_log(
               "key-value pair (%lu, %lu) not found in filter %lu (%lu)\n",
               k * num_values * num_fingerprints + i,
               i / num_fingerprints,
               k,
               found_values);

            routing_filter_lookup(cc, cfg, &filter[k], target, &found_values);
            platform_assert(0);
            rc = STATUS_NOT_FOUND;
            goto out;
         }
      }
   }
   platform_default_log("filter positive lookup time per key %lu\n",
                        platform_timestamp_elapsed(start_time)
                           / (num_fingerprints * num_trees * num_values));

   start_time             = platform_get_timestamp();
   uint64 unused_key      = num_values * num_fingerprints * num_trees;
   uint64 false_positives = 0;
   for (uint64 k = 0; k < num_trees; k++) {
      for (uint64 i = 0; i < num_values * num_fingerprints; i++) {
         *keybuf = k * num_values * num_fingerprints + i + unused_key;
         uint64 found_values;
         rc = routing_filter_lookup(cc, cfg, &filter[k], target, &found_values);
         platform_assert_status_ok(rc);
         if (found_values) {
            false_positives++;
         }
      }
   }

   platform_default_log("filter negative lookup time per key %lu\n",
                        platform_timestamp_elapsed(start_time)
                           / (num_fingerprints * num_trees * num_values));
   fraction false_positive_rate =
      init_fraction(false_positives, num_fingerprints * num_trees * num_values);
   platform_default_log("filter_basic_test: false positive rate " FRACTION_FMT(
                           1, 4) " for %lu trees\n",
                        FRACTION_ARGS(false_positive_rate),
                        num_trees);

   cache_print_stats(Platform_default_log_handle, cc);

out:
   for (uint64 i = 0; i < num_trees; i++) {
      routing_filter_dec_ref(cc, &filter[i]);
   }
   if (fp_arr) {
      platform_free(hid, fp_arr);
   }
   platform_free(hid, filter);
   return rc;
}

static void
usage(const char *argv0)
{
   platform_error_log("Usage:\n"
                      "\t%s\n"
                      "\t%s --perf\n",
                      argv0,
                      argv0);
   config_usage();
}

int
filter_test(int argc, char *argv[])
{
   int                    r;
   system_config          system_cfg;
   rc_allocator           al;
   clockcache            *cc;
   int                    config_argc;
   char                 **config_argv;
   bool32                 run_perf_test;
   platform_status        rc;
   uint64                 seed;
   test_message_generator gen;

   platform_register_thread();

   if (argc > 1 && strncmp(argv[1], "--perf", sizeof("--perf")) == 0) {
      run_perf_test = TRUE;
      config_argc   = argc - 2;
      config_argv   = argv + 2;
   } else {
      run_perf_test = FALSE;
      config_argc   = argc - 1;
      config_argv   = argv + 1;
   }

   bool use_shmem = config_parse_use_shmem(config_argc, config_argv);

   // Create a heap for io, allocator, cache and splinter
   platform_heap_id hid = NULL;
   rc                   = platform_heap_create(
      platform_get_module_id(), 512 * MiB, use_shmem, &hid);
   platform_assert_status_ok(rc);

   uint64               num_memtable_bg_threads_unused = 0;
   uint64               num_normal_bg_threads_unused   = 0;
   test_workload_config workload_cfg;

   rc = test_parse_args(&system_cfg,
                        &workload_cfg,
                        &seed,
                        &gen,
                        &num_memtable_bg_threads_unused,
                        &num_normal_bg_threads_unused,
                        config_argc,
                        config_argv);
   if (!SUCCESS(rc)) {
      platform_error_log("filter_test: failed to parse config: %s\n",
                         platform_status_to_string(rc));
      //
      // Provided arguments but set things up incorrectly.
      // Print usage so client can fix commandline.
      usage(argv[0]);
      r = -1;
      goto cleanup;
   }

   io_handle *io = io_handle_create(&system_cfg.io_cfg, hid);
   if (io == NULL) {
      platform_error_log("Failed to create IO handle\n");
      rc = STATUS_NO_MEMORY;
      r  = -1;
      goto cleanup;
   }

   task_system ts;
   rc = task_system_init(&ts, hid, &system_cfg.task_cfg);
   platform_assert_status_ok(rc);

   rc = rc_allocator_init(
      &al, &system_cfg.allocator_cfg, io, hid, platform_get_module_id());
   platform_assert_status_ok(rc);

   cc = TYPED_MALLOC(hid, cc);
   platform_assert(cc);
   rc = clockcache_init(cc,
                        &system_cfg.cache_cfg,
                        io,
                        (allocator *)&al,
                        "test",
                        hid,
                        platform_get_module_id());
   platform_assert_status_ok(rc);

   uint64 rflimit = routing_filter_max_fingerprints(
      (cache_config *)&system_cfg.cache_cfg, &system_cfg.filter_cfg);

   if (run_perf_test) {
      rc = test_filter_perf((cache *)cc,
                            &system_cfg.filter_cfg,
                            hid,
                            workload_cfg.key_size,
                            rflimit / system_cfg.trunk_node_cfg.target_fanout,
                            system_cfg.trunk_node_cfg.target_fanout,
                            100);
      platform_assert(SUCCESS(rc));
   } else {
      rc = test_filter_index_expansion_unique_count(
         (cache *)cc, &system_cfg.filter_cfg, hid);
      platform_assert(SUCCESS(rc));
      rc = test_filter_malicious_metadata(
         (cache *)cc, &system_cfg.filter_cfg, hid);
      platform_assert(SUCCESS(rc));
      rc = test_filter_basic((cache *)cc,
                             &system_cfg.filter_cfg,
                             hid,
                             workload_cfg.key_size,
                             rflimit / system_cfg.trunk_node_cfg.target_fanout,
                             system_cfg.trunk_node_cfg.target_fanout);
      platform_assert(SUCCESS(rc));
      rc = test_filter_basic((cache *)cc,
                             &system_cfg.filter_cfg,
                             hid,
                             workload_cfg.key_size,
                             100,
                             system_cfg.trunk_node_cfg.target_fanout);
      platform_assert(SUCCESS(rc));
      rc = test_filter_basic((cache *)cc,
                             &system_cfg.filter_cfg,
                             hid,
                             workload_cfg.key_size,
                             1,
                             system_cfg.trunk_node_cfg.target_fanout);
      platform_assert(SUCCESS(rc));
      rc = test_filter_basic((cache *)cc,
                             &system_cfg.filter_cfg,
                             hid,
                             workload_cfg.key_size,
                             1,
                             2 * system_cfg.trunk_node_cfg.target_fanout);
      platform_assert(SUCCESS(rc));
   }

   io_wait_all(io);
   clockcache_deinit(cc);
   platform_free(hid, cc);
   rc_allocator_deinit(&al);
   task_system_deinit(&ts);
   io_handle_destroy(io);
   r = 0;
cleanup:
   platform_heap_destroy(&hid);
   platform_deregister_thread();
   return r;
}
