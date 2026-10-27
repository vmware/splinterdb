// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * cache_test.c --
 *
 *     This file contains the tests for clockcache.
 */

#include "test.h"
#include "allocator.h"
#include "rc_allocator.h"
#include "cache.h"
#include "clockcache.h"
#include "mini_allocator.h"
#include "task.h"
#include "random.h"
#include "test_common.h"
#include "platform_condvar.h"
#include "poison.h"

/*
 * One allocation claims one starting position from the shared hand and scans
 * the ring locally. It publishes how far that bounded scan progressed so the
 * next allocator does not rescan the same occupied prefix.
 */
static void
test_rc_allocator_bounded_scan(rc_allocator *al)
{
   allocator *allocator = (struct allocator *)al;
   __atomic_store_n(&al->hand, 0, __ATOMIC_RELAXED);

   uint64 first_addr;
   platform_assert_status_ok(
      allocator_alloc(allocator, &first_addr, PAGE_TYPE_MISC));
   uint64 first_extent = first_addr / al->cfg->io_cfg->extent_size;
   platform_assert(__atomic_load_n(&al->hand, __ATOMIC_RELAXED)
                   == first_extent + 1);

   uint64 second_addr;
   platform_assert_status_ok(
      allocator_alloc(allocator, &second_addr, PAGE_TYPE_MISC));
   platform_assert(second_addr == first_addr + al->cfg->io_cfg->extent_size);
   platform_assert(__atomic_load_n(&al->hand, __ATOMIC_RELAXED)
                   == first_extent + 2);

   platform_assert(allocator_dec_ref(allocator, first_addr, PAGE_TYPE_MISC)
                   == AL_NO_REFS);
   platform_assert(allocator_dec_ref(allocator, first_addr, PAGE_TYPE_MISC)
                   == AL_FREE);
   platform_assert(allocator_dec_ref(allocator, second_addr, PAGE_TYPE_MISC)
                   == AL_NO_REFS);
   platform_assert(allocator_dec_ref(allocator, second_addr, PAGE_TYPE_MISC)
                   == AL_FREE);
}

static uint32
test_cache_released_read_refs(cache *cc, uint64 addr)
{
   page_handle page = test_cache_get_page(cc, addr, TRUE, PAGE_TYPE_MISC);
   uint32      refs = cache_get_read_ref(cc, &page) - 1;
   cache_unget(cc, &page);
   return refs;
}

platform_status
test_cache_page_pin(cache *cc, uint64 *addrs, uint64 page_capacity)
{
   platform_status rc = STATUS_OK;
   platform_assert(cache_count_dirty(cc) == 0);
   for (uint64 i = 0; i < page_capacity; i++) {
      page_handle page =
         test_cache_get_page(cc, addrs[i], TRUE, PAGE_TYPE_MISC);
      cache_try_claim(cc, &page);
      cache_lock(cc, &page);
      cache_pin(cc, &page);
      cache_unlock(cc, &page);
      cache_unclaim(cc, &page);
      cache_unget(cc, &page);
   }
   cache_evict(cc, TRUE /* ignore pinned pages */);
   for (uint64 i = 0; i < page_capacity; i++) {
      page_handle     page;
      platform_status get_rc =
         cache_get(cc, addrs[i], FALSE, PAGE_TYPE_MISC, &page);
      if (!SUCCESS(get_rc)) {
         platform_error_log("Pinned Entry Evicted\n");
         rc = STATUS_TEST_FAILED;
         continue;
      }
      cache_unpin(cc, &page);
      cache_unget(cc, &page);
   }
   return rc;
}

static platform_status
cache_test_alloc_extents(cache             *cc,
                         clockcache_config *cfg,
                         uint64             addr_arr[],
                         uint32             extents_to_allocate)
{
   allocator *al               = cache_get_allocator(cc);
   uint64     page_size        = cache_config_page_size(&cfg->super);
   uint64     pages_per_extent = cache_config_pages_per_extent(&cfg->super);
   platform_status rc;

   for (uint32 j = 0; j < extents_to_allocate; j++) {
      uint64 base_addr;
      rc = allocator_alloc(al, &base_addr, PAGE_TYPE_MISC);
      if (!SUCCESS(rc)) {
         platform_error_log("Expected to be able to allocate %u entries,"
                            "but only allocated: %u",
                            extents_to_allocate,
                            j);
         break;
      }

      for (uint32 i = 0; i < pages_per_extent; i++) {
         uint64       addr = base_addr + i * page_size;
         page_handle  storage;
         page_handle *page = cache_alloc(cc, addr, PAGE_TYPE_MISC, &storage);
         addr_arr[j * pages_per_extent + i] = addr;
         cache_unlock(cc, page);
         cache_unclaim(cc, page);
         cache_unget(cc, page);
      }
   }

   return rc;
}

static platform_status
cache_test_fill_page(cache *cc, uint64 addr, uint64 page_size, uint8 value)
{
   page_handle acquired_page =
      test_cache_get_page(cc, addr, TRUE, PAGE_TYPE_MISC);
   page_handle *page = &acquired_page;
   if (!cache_try_claim(cc, page)) {
      cache_unget(cc, page);
      return STATUS_BUSY;
   }

   cache_lock(cc, page);
   memset(page_handle_data(page), value, page_size);
   cache_unlock(cc, page);
   cache_unclaim(cc, page);
   cache_unget(cc, page);
   return STATUS_OK;
}

static platform_status
cache_test_verify_disk_page(cache *cc,
                            uint64 addr,
                            uint64 page_size,
                            uint8  expected)
{
   clockcache     *clock = (clockcache *)cc;
   buffer_handle   buffer;
   platform_status rc = platform_buffer_init(&buffer, page_size);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = io_read(clock->io, platform_buffer_getaddr(&buffer), page_size, addr);
   if (SUCCESS(rc)) {
      const uint8 *bytes = platform_buffer_getaddr(&buffer);
      for (uint64 i = 0; i < page_size; i++) {
         if (bytes[i] != expected) {
            platform_error_log("cache_test: unexpected byte at addr=%lu "
                               "offset=%lu: got=%u expected=%u\n",
                               addr,
                               i,
                               bytes[i],
                               expected);
            rc = STATUS_TEST_FAILED;
            break;
         }
      }
   }

   platform_status deinit_rc = platform_buffer_deinit(&buffer);
   if (SUCCESS(rc) && !SUCCESS(deinit_rc)) {
      rc = deinit_rc;
   }
   return rc;
}

/*
 * The refcount allocator is a pure store: mount() attaches without reading the
 * persisted map, and the caller then chooses how to populate it via
 * allocator_load_refcounts().  rebuild == FALSE loads the trusted persisted
 * map; rebuild == TRUE ignores it and reserves only the allocator's own fixed
 * extents, leaving all other ownership for a caller-driven rebuild.
 * persist() writes the map durably; if a rebuild fails there is nothing to
 * undo -- deinit() alone discards it, since nothing has been persisted.  This
 * exercises that contract end to end (clean-vs-rebuild is the caller's
 * decision -- the superblock's -- not the allocator's).
 */
static platform_status
test_rc_allocator_rejects_corrupt_reserved_extent(allocator_config *cfg,
                                                  io_handle        *io,
                                                  platform_heap_id  hid)
{
   platform_status          result = STATUS_OK;
   buffer_handle            buffer, tag_buffer;
   bool32                   buffer_live = FALSE, tag_buffer_live = FALSE;
   bool32                   map_changed = FALSE;
   rc_allocator             al;
   bool32                   al_live = FALSE;
   uint8                   *header = NULL, *tags = NULL;
   uint8                    saved_header[RC_ALLOCATOR_DISK_HEADER_BYTES];
   uint8                    saved_tag = 0;
   disk_geometry            geometry  = {.disk_size = cfg->capacity,
                                         .page_size = cfg->io_cfg->page_size,
                                         .extent_size = cfg->io_cfg->extent_size};
   rc_allocator_disk_layout layout;
   platform_status rc = rc_allocator_disk_compute_layout(&geometry, &layout);
   if (!SUCCESS(rc)) {
      return rc;
   }

   ZERO_CONTENTS(&al);
   rc = platform_buffer_init(&buffer, cfg->io_cfg->page_size);
   if (!SUCCESS(rc)) {
      return rc;
   }
   buffer_live = TRUE;

   uint64 map_addr = cfg->io_cfg->extent_size;
   rc              = io_read(
      io, platform_buffer_getaddr(&buffer), cfg->io_cfg->page_size, map_addr);
   if (!SUCCESS(rc)) {
      result = rc;
      goto cleanup;
   }

   header = platform_buffer_getaddr(&buffer);
   memcpy(saved_header, header, sizeof(saved_header));
   rc = platform_buffer_init(&tag_buffer, layout.tag_disk_bytes);
   if (!SUCCESS(rc)) {
      result = rc;
      goto cleanup;
   }
   tag_buffer_live = TRUE;
   tags            = platform_buffer_getaddr(&tag_buffer);
   uint64 tag_addr = map_addr + cfg->io_cfg->page_size;
   rc              = io_read(io, tags, layout.tag_disk_bytes, tag_addr);
   if (!SUCCESS(rc)) {
      result = rc;
      goto cleanup;
   }
   saved_tag = tags[0];
   if ((saved_tag >> RC_ALLOCATOR_DISK_TAG_BITS) != AL_ONE_REF) {
      result = STATUS_TEST_FAILED;
      goto cleanup;
   }

   /* Extent 1 is the high nibble of the first tag byte. Recompute both
    * checksums so this reaches reserved-extent validation, rather than merely
    * rejecting damaged bytes. */
   tags[0] &= RC_ALLOCATOR_DISK_TAG_ESCAPE;
   checksum128 tag_hash = XXH3_128bits(tags, layout.tag_bytes);
   rc_allocator_disk_put_u64(header + 88, tag_hash.low64);
   rc_allocator_disk_put_u64(header + 96, tag_hash.high64);
   rc_allocator_disk_seal_block(header, cfg->io_cfg->page_size);
   map_changed = TRUE;
   rc          = io_write(io, tags, cfg->io_cfg->page_size, tag_addr);
   if (SUCCESS(rc)) {
      rc = io_write(io, header, cfg->io_cfg->page_size, map_addr);
   }
   if (SUCCESS(rc)) {
      rc = io_durable_barrier(io);
   }
   if (!SUCCESS(rc)) {
      result = rc;
      goto cleanup;
   }

   rc = rc_allocator_mount(&al, cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      result = rc;
      goto cleanup;
   }
   al_live = TRUE;
   rc      = allocator_load_refcounts((allocator *)&al);
   if (!STATUS_IS_EQ(rc, STATUS_INVALID_STATE)) {
      platform_error_log("cache_test: allocator accepted a persisted map "
                         "with a free reserved extent: %s\n",
                         platform_status_to_string(rc));
      result = STATUS_TEST_FAILED;
   }

cleanup:
   if (al_live) {
      rc_allocator_deinit(&al);
   }
   if (map_changed) {
      tags[0] = saved_tag;
      memcpy(header, saved_header, sizeof(saved_header));
      rc = io_write(
         io, tags, cfg->io_cfg->page_size, map_addr + cfg->io_cfg->page_size);
      if (SUCCESS(rc)) {
         rc = io_write(io, header, cfg->io_cfg->page_size, map_addr);
      }
      if (SUCCESS(rc)) {
         rc = io_durable_barrier(io);
      }
      if (SUCCESS(result) && !SUCCESS(rc)) {
         result = rc;
      }
   }
   if (tag_buffer_live) {
      rc = platform_buffer_deinit(&tag_buffer);
      if (SUCCESS(result) && !SUCCESS(rc)) {
         result = rc;
      }
   }
   if (buffer_live) {
      rc = platform_buffer_deinit(&buffer);
      if (SUCCESS(result) && !SUCCESS(rc)) {
         result = rc;
      }
   }
   return result;
}

static platform_status
test_rc_allocator_recovery_bootstrap(allocator_config *cfg,
                                     io_handle        *io,
                                     platform_heap_id  hid)
{
   disk_geometry            geometry = {.disk_size = cfg->capacity,
                                        .page_size = cfg->io_cfg->page_size,
                                        .extent_size = cfg->io_cfg->extent_size};
   rc_allocator_disk_layout layout;
   platform_status          layout_rc =
      rc_allocator_disk_compute_layout(&geometry, &layout);
   if (!SUCCESS(layout_rc)) {
      return layout_rc;
   }
   uint64          reserved_extent_count = layout.reserved_extents;
   uint64          stale_extent_addr     = 0;
   platform_status rc                    = STATUS_OK;
   rc_allocator    al;
   bool32          al_live = FALSE;

   ZERO_CONTENTS(&al);
   platform_default_log(
      "cache_test: allocator recovery bootstrap test started\n");

   // 1) Format, allocate a data extent, persist the map, tear down.
   rc = rc_allocator_init(&al, cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   al_live = TRUE;
   rc = allocator_alloc((allocator *)&al, &stale_extent_addr, PAGE_TYPE_MISC);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc = allocator_persist((allocator *)&al, NULL);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc_allocator_deinit(&al);
   al_live = FALSE;

   // 2) Clean open: attach + load_refcounts(rebuild=FALSE) loads the map.
   rc = rc_allocator_mount(&al, cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   al_live = TRUE;
   rc      = allocator_load_refcounts((allocator *)&al);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   if (allocator_get_refcount((allocator *)&al, stale_extent_addr)
       != AL_ONE_REF)
   {
      platform_error_log(
         "cache_test: clean open did not load the persisted refcount map\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   rc_allocator_deinit(&al);
   al_live = FALSE;

   rc = test_rc_allocator_rejects_corrupt_reserved_extent(cfg, io, hid);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   // 3) Rebuild open: attach + load_refcounts(rebuild=TRUE) ignores the
   //    persisted map, reserving only the fixed extents.
   rc = rc_allocator_mount(&al, cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   al_live = TRUE;
   rc      = allocator_recovery_begin((allocator *)&al);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   if (allocator_in_use((allocator *)&al) != reserved_extent_count) {
      platform_error_log("cache_test: rebuild expected %lu reserved extents, "
                         "found %lu\n",
                         reserved_extent_count,
                         allocator_in_use((allocator *)&al));
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   for (uint64 extent_no = 0; extent_no < reserved_extent_count; extent_no++) {
      uint64 extent_addr = extent_no * cfg->io_cfg->extent_size;
      if (allocator_get_refcount((allocator *)&al, extent_addr) != AL_ONE_REF) {
         platform_error_log(
            "cache_test: rebuild did not reserve fixed extent %lu\n",
            extent_no);
         rc = STATUS_TEST_FAILED;
         goto cleanup;
      }
   }
   if (allocator_get_refcount((allocator *)&al, stale_extent_addr) != AL_FREE) {
      platform_error_log(
         "cache_test: rebuild trusted a persisted data refcount\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }

   // rebuild_acquire establishes the first reference.
   rc = allocator_recovery_record_reference(
      (allocator *)&al, stale_extent_addr, PAGE_TYPE_MISC);
   if (!SUCCESS(rc)
       || allocator_get_refcount((allocator *)&al, stale_extent_addr)
             != AL_ONE_REF)
   {
      platform_error_log(
         "cache_test: first rebuilt extent reference was incorrect\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }

   // 4) A failed/abandoned rebuild persists nothing: deinit alone discards it,
   //    so a later clean open still sees the pre-rebuild map.
   rc_allocator_deinit(&al);
   al_live = FALSE;

   rc = rc_allocator_mount(&al, cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   al_live = TRUE;
   rc      = allocator_load_refcounts((allocator *)&al);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   if (allocator_get_refcount((allocator *)&al, stale_extent_addr)
       != AL_ONE_REF)
   {
      platform_error_log(
         "cache_test: aborted rebuild leaked into the persisted map\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   rc_allocator_deinit(&al);
   al_live = FALSE;

   // 5) Rebuild + finish + persist round-trips the rebuilt refcount.
   rc = rc_allocator_mount(&al, cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   al_live = TRUE;
   rc      = allocator_recovery_begin((allocator *)&al);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc = allocator_recovery_record_reference(
      (allocator *)&al, stale_extent_addr, PAGE_TYPE_MISC);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc = allocator_recovery_record_reference(
      (allocator *)&al, stale_extent_addr, PAGE_TYPE_MISC);
   if (!SUCCESS(rc)
       || allocator_get_refcount((allocator *)&al, stale_extent_addr)
             != AL_ONE_REF + 1)
   {
      platform_error_log(
         "cache_test: rebuilt extent reference did not increment\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   allocator_recovery_finish((allocator *)&al);
   rc = allocator_persist((allocator *)&al, NULL);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc_allocator_deinit(&al);
   al_live = FALSE;

   rc = rc_allocator_mount(&al, cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   al_live = TRUE;
   rc      = allocator_load_refcounts((allocator *)&al);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   if (allocator_get_refcount((allocator *)&al, stale_extent_addr)
       != AL_ONE_REF + 1)
   {
      platform_error_log("cache_test: finished recovery was not persisted\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }

cleanup:
   if (al_live) {
      rc_allocator_deinit(&al);
   }

   if (SUCCESS(rc)) {
      platform_default_log("cache_test: allocator recovery bootstrap test "
                           "passed\n");
   }
   return rc;
}

#define TEST_MINI_RECOVER_NUM_DATA_EXTENTS 3

/*
 * Build a real mini_allocator with one metadata extent and several data
 * extents, durably persist it, then simulate a crash: mount a fresh allocator
 * in recovery mode (which deliberately ignores the persisted refcount table)
 * and confirm mini_recover_allocations() rediscovers every extent from the
 * on-disk metadata alone -- including the ordering property this whole
 * mechanism exists for, since a rebuilding allocator considers all of these
 * extents unallocated until mini_recover_allocations() itself establishes
 * their references, ahead of the test_cache_get_page() calls it issues to read
 * them.
 */
static platform_status
test_mini_recover_allocations(allocator_config  *allocator_cfg,
                              clockcache_config *cache_cfg,
                              io_handle         *io,
                              platform_heap_id   hid)
{
   platform_status rc = STATUS_OK;
   rc_allocator    original, recovery;
   clockcache      original_cc, recovery_cc;
   bool32          original_al_live = FALSE, original_cc_live = FALSE;
   bool32          recovery_al_live = FALSE, recovery_cc_live = FALSE;
   mini_allocator  mini;
   bool32          mini_live                                        = FALSE;
   uint64          meta_head                                        = 0;
   uint64          data_extents[TEST_MINI_RECOVER_NUM_DATA_EXTENTS] = {0};

   platform_default_log("cache_test: mini_recover_allocations test started\n");

   rc = rc_allocator_init(
      &original, allocator_cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   original_al_live = TRUE;

   rc = clockcache_init(&original_cc,
                        cache_cfg,
                        io,
                        (allocator *)&original,
                        "test",
                        hid,
                        platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   original_cc_live    = TRUE;
   cache *original_ccp = (cache *)&original_cc;

   rc = allocator_alloc((allocator *)&original, &meta_head, PAGE_TYPE_MISC);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc = mini_init(&mini, original_ccp, meta_head, 0, 1, PAGE_TYPE_MISC, NULL);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   mini_live = TRUE;

   for (uint64 i = 0; i < TEST_MINI_RECOVER_NUM_DATA_EXTENTS; i++) {
      rc = mini_alloc_extent(&mini, 0, &data_extents[i], NULL);
      if (!SUCCESS(rc)) {
         goto cleanup;
      }
   }
   mini_release(&mini);
   mini_live = FALSE;

   /* Durably persist before "crashing": recovery must see real disk state. */
   cache_flush(original_ccp);
   clockcache_deinit(&original_cc);
   original_cc_live = FALSE;
   rc               = allocator_persist((allocator *)&original, NULL);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc_allocator_deinit(&original);
   original_al_live = FALSE;

   rc = rc_allocator_mount(
      &recovery, allocator_cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   recovery_al_live = TRUE;
   rc               = allocator_recovery_begin((allocator *)&recovery);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   /* load_refcounts(rebuild) deliberately ignores the persisted map. */
   if (allocator_get_refcount((allocator *)&recovery, meta_head) != AL_FREE) {
      platform_error_log(
         "cache_test: recovery mount trusted a persisted refcount\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }

   rc = clockcache_init(&recovery_cc,
                        cache_cfg,
                        io,
                        (allocator *)&recovery,
                        "test",
                        hid,
                        platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   recovery_cc_live = TRUE;

   /*
    * Every extent below is still AL_FREE from recovery's point of view: this
    * call must establish each one's reference itself, before its own
    * test_cache_get_page() calls, or those test_cache_get_page()s would trip
    * the cache's debug allocation check.
    */
   rc =
      mini_recover_references((cache *)&recovery_cc, meta_head, PAGE_TYPE_MISC);
   if (!SUCCESS(rc)) {
      platform_error_log("cache_test: mini_recover_allocations failed: %s\n",
                         platform_status_to_string(rc));
      goto cleanup;
   }

   /* Recovery restores one owner for the metadata head and each data extent.
    * The head has no additional self-reference. */
   if (allocator_get_refcount((allocator *)&recovery, meta_head) != AL_ONE_REF)
   {
      platform_error_log(
         "cache_test: mini_recover_allocations did not recover the metadata "
         "extent reference\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   for (uint64 i = 0; i < TEST_MINI_RECOVER_NUM_DATA_EXTENTS; i++) {
      if (allocator_get_refcount((allocator *)&recovery, data_extents[i])
          != AL_ONE_REF)
      {
         platform_error_log("cache_test: mini_recover_allocations did not "
                            "recover data extent %lu\n",
                            i);
         rc = STATUS_TEST_FAILED;
         goto cleanup;
      }
   }
   allocator_recovery_finish((allocator *)&recovery);

cleanup:
   if (mini_live) {
      mini_release(&mini);
   }
   if (recovery_cc_live) {
      clockcache_deinit(&recovery_cc);
   }
   if (recovery_al_live) {
      rc_allocator_deinit(&recovery);
   }
   if (original_cc_live) {
      clockcache_deinit(&original_cc);
   }
   if (original_al_live) {
      rc_allocator_deinit(&original);
   }

   if (SUCCESS(rc)) {
      platform_default_log(
         "cache_test: mini_recover_allocations test passed\n");
   }
   return rc;
}

typedef struct {
   cache          *cc;
   volatile bool32 finished;
   platform_status status;
} cache_fence_test_context;

static void
cache_test_writeback_fence_thread(void *arg)
{
   cache_fence_test_context *ctxt = (cache_fence_test_context *)arg;
   ctxt->status                   = cache_writeback_dirty(ctxt->cc);
   __atomic_store_n(&ctxt->finished, TRUE, __ATOMIC_RELEASE);
}

/*
 * Verify the simplified writeback-fence contract: a single pass flushes every
 * dirty, unlocked page and completes even while another page is write-locked,
 * skipping the locked page rather than waiting for it. old_page is left dirty
 * and unlocked so the fence must flush it; new_page is held write-locked so the
 * fence must skip it and still finish promptly.
 */
static platform_status
test_cache_writeback_fence(cache             *cc,
                           clockcache_config *cfg,
                           platform_heap_id   hid)
{
   platform_status rc            = STATUS_OK;
   uint64       pages_per_extent = cache_config_pages_per_extent(&cfg->super);
   uint64      *addr_arr         = NULL;
   page_handle  old_page_storage;
   page_handle *old_page = NULL;
   page_handle  new_page_storage;
   page_handle *new_page    = NULL;
   bool32       new_claimed = FALSE, new_locked = FALSE;
   bool32       fence_thread_started = FALSE;
   const uint8  old_baseline = 0x11, old_value = 0x22;
   const uint8  new_baseline = 0x33, new_value = 0x44;
   cache_fence_test_context fence_ctxt = {
      .cc = cc, .finished = FALSE, .status = STATUS_OK};
   platform_thread fence_thread;

   platform_default_log("cache_test: writeback fence test started\n");
   platform_assert(cfg->page_capacity >= 2 * pages_per_extent);

   addr_arr = TYPED_ARRAY_MALLOC(hid, addr_arr, 2 * pages_per_extent);
   if (addr_arr == NULL) {
      rc = STATUS_NO_MEMORY;
      goto cleanup;
   }

   rc = cache_test_alloc_extents(cc, cfg, addr_arr, 2);
   if (!SUCCESS(rc)) {
      platform_free(hid, addr_arr);
      addr_arr = NULL;
      goto cleanup;
   }

   rc = cache_test_fill_page(
      cc, addr_arr[0], cache_config_page_size(&cfg->super), old_baseline);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc = cache_test_fill_page(cc,
                             addr_arr[pages_per_extent],
                             cache_config_page_size(&cfg->super),
                             new_baseline);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   cache_flush(cc);

   /* Dirty old_page and leave it unlocked: the fence must flush it. */
   old_page  = &old_page_storage;
   *old_page = test_cache_get_page(cc, addr_arr[0], TRUE, PAGE_TYPE_MISC);
   if (!cache_try_claim(cc, old_page)) {
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   cache_lock(cc, old_page);
   memset(page_handle_data(old_page),
          old_value,
          cache_config_page_size(&cfg->super));
   cache_unlock(cc, old_page);
   cache_unclaim(cc, old_page);
   cache_unget(cc, old_page);
   old_page = NULL;

   /* Hold new_page write-locked: the fence must skip it and still complete. */
   new_page = &new_page_storage;
   *new_page =
      test_cache_get_page(cc, addr_arr[pages_per_extent], TRUE, PAGE_TYPE_MISC);
   if (!cache_try_claim(cc, new_page)) {
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   new_claimed = TRUE;
   cache_lock(cc, new_page);
   new_locked = TRUE;
   memset(page_handle_data(new_page),
          new_value,
          cache_config_page_size(&cfg->super));

   rc = platform_thread_create(&fence_thread,
                               FALSE,
                               cache_test_writeback_fence_thread,
                               &fence_ctxt,
                               hid);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   fence_thread_started = TRUE;

   /* The fence must finish without waiting for the write-locked new_page. */
   timestamp wait_start = platform_get_timestamp();
   while (!__atomic_load_n(&fence_ctxt.finished, __ATOMIC_ACQUIRE)) {
      if (platform_timestamp_elapsed(wait_start) > SEC_TO_NSEC(5)) {
         platform_error_log(
            "cache_test: fence did not complete while a page was locked\n");
         rc = STATUS_TIMEDOUT;
         goto cleanup;
      }
      platform_sleep_ns(1000);
   }

cleanup:
   if (new_locked) {
      cache_unlock(cc, new_page);
   }
   if (new_claimed) {
      cache_unclaim(cc, new_page);
   }
   if (new_page != NULL) {
      cache_unget(cc, new_page);
   }
   if (old_page != NULL) {
      cache_unget(cc, old_page);
   }
   if (fence_thread_started) {
      platform_status join_rc = platform_thread_join(&fence_thread);
      if (!SUCCESS(join_rc) && SUCCESS(rc)) {
         rc = join_rc;
      }
      if (!SUCCESS(fence_ctxt.status) && SUCCESS(rc)) {
         rc = fence_ctxt.status;
      }
   }

   /* Only new_page, write-locked throughout the fence, remains dirty. */
   if (SUCCESS(rc)) {
      uint32 dirty_count = cache_count_dirty(cc);
      if (dirty_count != 1) {
         platform_error_log("cache_test: expected one dirty page after fence, "
                            "found %u\n",
                            dirty_count);
         rc = STATUS_TEST_FAILED;
      }
   }
   if (SUCCESS(rc)) {
      rc = cache_durable_barrier(cc);
   }
   /* The dirty, unlocked page was flushed by the fence. */
   if (SUCCESS(rc)) {
      rc = cache_test_verify_disk_page(
         cc, addr_arr[0], cache_config_page_size(&cfg->super), old_value);
   }
   /* The write-locked page was skipped, so its baseline is still on disk. */
   if (SUCCESS(rc)) {
      rc = cache_test_verify_disk_page(cc,
                                       addr_arr[pages_per_extent],
                                       cache_config_page_size(&cfg->super),
                                       new_baseline);
   }

   if (addr_arr != NULL) {
      cache_flush(cc);
      for (uint32 i = 0; i < 2; i++) {
         uint64     addr = addr_arr[i * pages_per_extent];
         allocator *al   = cache_get_allocator(cc);
         refcount   ref  = allocator_dec_ref(al, addr, PAGE_TYPE_MISC);
         platform_assert(ref == AL_NO_REFS);
         cache_extent_discard(cc, addr, PAGE_TYPE_MISC);
         ref = allocator_dec_ref(al, addr, PAGE_TYPE_MISC);
         platform_assert(ref == AL_FREE);
      }
      platform_free(hid, addr_arr);
   }

   if (SUCCESS(rc)) {
      platform_default_log("cache_test: writeback fence test passed\n");
   } else {
      platform_default_log("cache_test: writeback fence test failed\n");
   }
   return rc;
}

/*
 * A page that is only claimed (not write-locked) is not cleanable, so a fence
 * skips it and completes; the page's baseline is therefore still what is on
 * disk. Once the claim is dropped, a second fence flushes the page.
 */
static platform_status
test_cache_writeback_fence_claimed_page(cache             *cc,
                                        clockcache_config *cfg,
                                        platform_heap_id   hid)
{
   platform_status rc            = STATUS_OK;
   uint64       pages_per_extent = cache_config_pages_per_extent(&cfg->super);
   uint64      *addr_arr         = NULL;
   page_handle  page_storage;
   page_handle *page    = NULL;
   bool32       claimed = FALSE, locked = FALSE;
   bool32       fence_thread_started = FALSE;
   const uint8  baseline = 0x55, final_value = 0x66;
   cache_fence_test_context fence_ctxt = {
      .cc = cc, .finished = FALSE, .status = STATUS_OK};
   platform_thread fence_thread;

   platform_default_log("cache_test: claimed-page fence test started\n");

   addr_arr = TYPED_ARRAY_MALLOC(hid, addr_arr, pages_per_extent);
   if (addr_arr == NULL) {
      rc = STATUS_NO_MEMORY;
      goto cleanup;
   }

   rc = cache_test_alloc_extents(cc, cfg, addr_arr, 1);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc = cache_test_fill_page(
      cc, addr_arr[0], cache_config_page_size(&cfg->super), baseline);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   cache_flush(cc);

   /* Dirty the page, then keep only a claim (release the write lock). */
   page  = &page_storage;
   *page = test_cache_get_page(cc, addr_arr[0], TRUE, PAGE_TYPE_MISC);
   if (!cache_try_claim(cc, page)) {
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   claimed = TRUE;
   cache_lock(cc, page);
   locked = TRUE;
   memset(
      page_handle_data(page), final_value, cache_config_page_size(&cfg->super));
   cache_unlock(cc, page);
   locked = FALSE;

   rc = platform_thread_create(&fence_thread,
                               FALSE,
                               cache_test_writeback_fence_thread,
                               &fence_ctxt,
                               hid);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   fence_thread_started = TRUE;

   /* A claimed page is not cleanable, so the fence skips it and completes. */
   timestamp wait_start = platform_get_timestamp();
   while (!__atomic_load_n(&fence_ctxt.finished, __ATOMIC_ACQUIRE)) {
      if (platform_timestamp_elapsed(wait_start) > SEC_TO_NSEC(5)) {
         platform_error_log(
            "cache_test: fence did not complete while page was claimed\n");
         rc = STATUS_TIMEDOUT;
         goto cleanup;
      }
      platform_sleep_ns(1000);
   }

   platform_status join_rc = platform_thread_join(&fence_thread);
   fence_thread_started    = FALSE;
   if (!SUCCESS(join_rc)) {
      rc = join_rc;
      goto cleanup;
   }
   if (!SUCCESS(fence_ctxt.status)) {
      rc = fence_ctxt.status;
      goto cleanup;
   }

   /* The skipped page was not written, so its baseline is still on disk. */
   rc = cache_test_verify_disk_page(
      cc, addr_arr[0], cache_config_page_size(&cfg->super), baseline);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   /* Drop the claim; the page is now cleanable and a second fence flushes it.
    */
   cache_unclaim(cc, page);
   claimed = FALSE;
   cache_unget(cc, page);
   page = NULL;

   rc = cache_writeback_dirty(cc);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   if (cache_count_dirty(cc) != 0) {
      platform_error_log("cache_test: second fence left dirty pages\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   rc = cache_durable_barrier(cc);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc = cache_test_verify_disk_page(
      cc, addr_arr[0], cache_config_page_size(&cfg->super), final_value);

cleanup:
   if (locked) {
      cache_unlock(cc, page);
   }
   if (claimed) {
      cache_unclaim(cc, page);
   }
   if (page != NULL) {
      cache_unget(cc, page);
   }
   if (fence_thread_started) {
      platform_status jrc = platform_thread_join(&fence_thread);
      if (!SUCCESS(jrc) && SUCCESS(rc)) {
         rc = jrc;
      }
      if (!SUCCESS(fence_ctxt.status) && SUCCESS(rc)) {
         rc = fence_ctxt.status;
      }
   }

   if (addr_arr != NULL) {
      cache_flush(cc);
      allocator *al  = cache_get_allocator(cc);
      refcount   ref = allocator_dec_ref(al, addr_arr[0], PAGE_TYPE_MISC);
      platform_assert(ref == AL_NO_REFS);
      cache_extent_discard(cc, addr_arr[0], PAGE_TYPE_MISC);
      ref = allocator_dec_ref(al, addr_arr[0], PAGE_TYPE_MISC);
      platform_assert(ref == AL_FREE);
      platform_free(hid, addr_arr);
   }

   if (SUCCESS(rc)) {
      platform_default_log("cache_test: claimed-page fence test passed\n");
   } else {
      platform_default_log("cache_test: claimed-page fence test failed\n");
   }
   return rc;
}

typedef struct {
   cache          *cc;
   uint64         *addrs;
   uint64          num_addrs;
   uint64          page_size;
   volatile bool32 stop;
} cache_hammer_context;

/*
 * Continuously dirty and asynchronously write back a rotating set of pages, so
 * the I/O subsystem never reaches global quiescence.
 */
static void
cache_test_hammer_thread(void *arg)
{
   cache_hammer_context *ctxt = (cache_hammer_context *)arg;
   uint64                i    = 0;
   while (!__atomic_load_n(&ctxt->stop, __ATOMIC_ACQUIRE)) {
      uint64      addr = ctxt->addrs[i % ctxt->num_addrs];
      page_handle acquired_page =
         test_cache_get_page(ctxt->cc, addr, TRUE, PAGE_TYPE_MISC);
      page_handle *page = &acquired_page;
      if (cache_try_claim(ctxt->cc, page)) {
         cache_lock(ctxt->cc, page);
         memset(page_handle_data(page), (uint8)i, ctxt->page_size);
         cache_unlock(ctxt->cc, page);
         cache_unclaim(ctxt->cc, page);
         // Other threads race us for these pages, so STATUS_BUSY is expected.
         cache_writeback_page(ctxt->cc, page, PAGE_TYPE_MISC, NULL);
      }
      cache_unget(ctxt->cc, page);
      i++;
   }
}

/*
 * A writeback fence must drain only the pages dirty at its cut and then return,
 * even while another thread keeps dirtying and writing back other pages. The
 * previous io_wait_all implementation waited for *global* I/O quiescence, which
 * this workload never reaches, so it would hang here.
 */
static platform_status
test_cache_writeback_fence_liveness(cache             *cc,
                                    clockcache_config *cfg,
                                    platform_heap_id   hid)
{
   platform_status rc           = STATUS_OK;
   uint64          page_size    = cache_config_page_size(&cfg->super);
   uint64      pages_per_extent = cache_config_pages_per_extent(&cfg->super);
   uint64     *addr_arr         = NULL;
   const uint8 baseline = 0x11, target_value = 0x77;
   bool32      hammer_started = FALSE, fence_started = FALSE;
   cache_hammer_context     hammer_ctxt = {.cc = cc, .stop = FALSE};
   cache_fence_test_context fence_ctxt  = {
       .cc = cc, .finished = FALSE, .status = STATUS_OK};
   platform_thread hammer_thread, fence_thread;

   platform_default_log("cache_test: writeback fence liveness test started\n");
   platform_assert(cfg->page_capacity >= 2 * pages_per_extent);

   addr_arr = TYPED_ARRAY_MALLOC(hid, addr_arr, 2 * pages_per_extent);
   if (addr_arr == NULL) {
      rc = STATUS_NO_MEMORY;
      goto cleanup;
   }
   rc = cache_test_alloc_extents(cc, cfg, addr_arr, 2);
   if (!SUCCESS(rc)) {
      platform_free(hid, addr_arr);
      addr_arr = NULL;
      goto cleanup;
   }
   rc = cache_test_fill_page(cc, addr_arr[0], page_size, baseline);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   cache_flush(cc);

   /* Dirty the target page (unlocked); the fence must flush it. */
   rc = cache_test_fill_page(cc, addr_arr[0], page_size, target_value);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   /* Hammer the second extent's pages to keep I/O perpetually in flight. */
   hammer_ctxt.addrs     = &addr_arr[pages_per_extent];
   hammer_ctxt.num_addrs = pages_per_extent;
   hammer_ctxt.page_size = page_size;
   rc                    = platform_thread_create(
      &hammer_thread, FALSE, cache_test_hammer_thread, &hammer_ctxt, hid);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   hammer_started = TRUE;

   rc = platform_thread_create(&fence_thread,
                               FALSE,
                               cache_test_writeback_fence_thread,
                               &fence_ctxt,
                               hid);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   fence_started = TRUE;

   /* The fence must return despite the ongoing hammer activity. */
   timestamp wait_start = platform_get_timestamp();
   while (!__atomic_load_n(&fence_ctxt.finished, __ATOMIC_ACQUIRE)) {
      if (platform_timestamp_elapsed(wait_start) > SEC_TO_NSEC(10)) {
         platform_error_log(
            "cache_test: fence did not terminate under concurrent writes\n");
         rc = STATUS_TIMEDOUT;
         break;
      }
      platform_sleep_ns(1000);
   }

cleanup:
   /* Stop the hammer first so I/O can quiesce even if the fence is stuck. */
   if (hammer_started) {
      __atomic_store_n(&hammer_ctxt.stop, TRUE, __ATOMIC_RELEASE);
      platform_status jrc = platform_thread_join(&hammer_thread);
      if (!SUCCESS(jrc) && SUCCESS(rc)) {
         rc = jrc;
      }
   }
   if (fence_started) {
      platform_status jrc = platform_thread_join(&fence_thread);
      if (!SUCCESS(jrc) && SUCCESS(rc)) {
         rc = jrc;
      }
      if (!SUCCESS(fence_ctxt.status) && SUCCESS(rc)) {
         rc = fence_ctxt.status;
      }
   }

   /* The target page, dirty at the cut, must have been made durable. */
   if (SUCCESS(rc)) {
      rc = cache_durable_barrier(cc);
   }
   if (SUCCESS(rc)) {
      rc =
         cache_test_verify_disk_page(cc, addr_arr[0], page_size, target_value);
   }

   if (addr_arr != NULL) {
      cache_flush(cc);
      for (uint32 i = 0; i < 2; i++) {
         uint64     addr = addr_arr[i * pages_per_extent];
         allocator *al   = cache_get_allocator(cc);
         refcount   ref  = allocator_dec_ref(al, addr, PAGE_TYPE_MISC);
         platform_assert(ref == AL_NO_REFS);
         cache_extent_discard(cc, addr, PAGE_TYPE_MISC);
         ref = allocator_dec_ref(al, addr, PAGE_TYPE_MISC);
         platform_assert(ref == AL_FREE);
      }
      platform_free(hid, addr_arr);
   }

   if (SUCCESS(rc)) {
      platform_default_log(
         "cache_test: writeback fence liveness test passed\n");
   } else {
      platform_default_log(
         "cache_test: writeback fence liveness test failed\n");
   }
   return rc;
}

platform_status
test_cache_basic(cache *cc, clockcache_config *cfg, platform_heap_id hid)
{
   platform_default_log("cache_test: basic test started\n");
   platform_status rc       = STATUS_OK;
   page_handle    *page_arr = NULL;
   uint64         *addr_arr = NULL;

   rc = test_cache_writeback_fence(cc, cfg, hid);
   if (!SUCCESS(rc)) {
      goto exit;
   }

   rc = test_cache_writeback_fence_claimed_page(cc, cfg, hid);
   if (!SUCCESS(rc)) {
      goto exit;
   }

   rc = test_cache_writeback_fence_liveness(cc, cfg, hid);
   if (!SUCCESS(rc)) {
      goto exit;
   }

   /* allocate twice as many pages as the cache capacity */
   uint64 pages_per_extent    = cache_config_pages_per_extent(&cfg->super);
   uint32 extent_capacity     = cfg->page_capacity / pages_per_extent;
   uint32 extents_to_allocate = 2 * extent_capacity;
   uint64 pages_to_allocate   = extents_to_allocate * pages_per_extent;
   addr_arr = TYPED_ARRAY_MALLOC(hid, addr_arr, pages_to_allocate);
   rc       = cache_test_alloc_extents(cc, cfg, addr_arr, extents_to_allocate);
   if (!SUCCESS(rc)) {
      /* no need to set status because we got here from an error status */
      goto exit;
   }
   cache_flush(cc);
   uint32 dirty_count = cache_count_dirty(cc);
   if (dirty_count != 0) {
      platform_error_log("Expected no dirty entries but found: %u",
                         dirty_count);
      rc = STATUS_TEST_FAILED;
      goto exit;
   }

   /*
    * Get all entries for read, verify ref counts, and release. Verify
    * that there are no dirty entries afterwards.
    */
   uint32 pages_allocated = extents_to_allocate * pages_per_extent;
   page_arr = TYPED_ARRAY_MALLOC(hid, page_arr, cfg->page_capacity);
   if (page_arr == NULL) {
      rc = STATUS_NO_MEMORY;
      goto exit;
   }

   for (uint32 j = 0; j < pages_allocated && SUCCESS(rc);) {
      uint32 i;
      for (i = 0; i < cfg->page_capacity; i++) {
         uint64 page_idx = j + i;
         page_arr[i] =
            test_cache_get_page(cc, addr_arr[page_idx], TRUE, PAGE_TYPE_MISC);
         uint32 refcount = cache_get_read_ref(cc, &page_arr[i]);
         if (refcount != 1) {
            platform_error_log("Expected one reference, but found %u\n",
                               refcount);
            rc = STATUS_TEST_FAILED;
            goto exit;
         }
      }
      for (i = 0; i < cfg->page_capacity; i++) {
         cache_unget(cc, &page_arr[i]);
         uint32 refcount = test_cache_released_read_refs(cc, addr_arr[j + i]);
         if (refcount != 0) {
            platform_error_log("Expected zero references, but found %u\n",
                               refcount);
            rc = STATUS_TEST_FAILED;
            goto exit;
         }
      }
      j += i;
   }
   if (!SUCCESS(rc)) {
      goto exit;
   }
   dirty_count = cache_count_dirty(cc);
   if (dirty_count != 0) {
      platform_error_log("Expected no dirty entries but found: %u",
                         dirty_count);
      rc = STATUS_TEST_FAILED;
      goto exit;
   }

   /*
    * Get all entries for read, upgrade to write, verify ref counts,
    * and release. Verify that there are no dirty entries afterwards.
    */
   for (uint32 j = 0; j < pages_allocated && SUCCESS(rc);) {
      uint32 i;
      for (i = 0; i < cfg->page_capacity; i++) {
         page_arr[i] =
            test_cache_get_page(cc, addr_arr[j + i], TRUE, PAGE_TYPE_MISC);
         bool32 claim_obtained = cache_try_claim(cc, &page_arr[i]);
         if (!claim_obtained) {
            platform_error_log("Expected uncontested claim, but failed\n");
            rc = STATUS_TEST_FAILED;
         }
         cache_lock(cc, &page_arr[i]);
         uint32 refcount = cache_get_read_ref(cc, &page_arr[i]);
         if (refcount != 1) {
            platform_error_log("Expected one reference, but found %u\n",
                               refcount);
            rc = STATUS_TEST_FAILED;
         }
      }
      for (i = 0; i < cfg->page_capacity; i++) {
         cache_unlock(cc, &page_arr[i]);
         cache_unclaim(cc, &page_arr[i]);
         cache_unget(cc, &page_arr[i]);
         uint32 refcount = test_cache_released_read_refs(cc, addr_arr[j + i]);
         if (refcount != 0) {
            platform_error_log("Expected zero references, but found %u\n",
                               refcount);
            rc = STATUS_TEST_FAILED;
         }
      }
      j += i;
   }
   if (!SUCCESS(rc)) {
      goto exit;
   }
   if (dirty_count != 0) {
      platform_error_log("Expected no dirty entries but found: %u",
                         dirty_count);
      rc = STATUS_TEST_FAILED;
      goto exit;
   }

   /*
    * Get all entries for write, mark dirty, release, and
    * flush. Verify that there are no dirty entries afterwards.
    */

   for (uint32 j = 0; j < pages_allocated && SUCCESS(rc);) {
      uint32 i;
      for (i = 0; i < cfg->page_capacity; i++) {
         page_arr[i] =
            test_cache_get_page(cc, addr_arr[j + i], TRUE, PAGE_TYPE_MISC);
         bool32 claim_obtained = cache_try_claim(cc, &page_arr[i]);
         if (!claim_obtained) {
            platform_error_log("Expected uncontested claim, but failed\n");
            rc = STATUS_TEST_FAILED;
         }
         cache_lock(cc, &page_arr[i]);
         uint32 refcount = cache_get_read_ref(cc, &page_arr[i]);
         if (refcount != 1) {
            platform_error_log("Expected one reference, but found %u\n",
                               refcount);
            rc = STATUS_TEST_FAILED;
         }
      }
      for (i = 0; i < cfg->page_capacity; i++) {
         cache_unlock(cc, &page_arr[i]);
         cache_unclaim(cc, &page_arr[i]);
         cache_unget(cc, &page_arr[i]);
         uint32 refcount = test_cache_released_read_refs(cc, addr_arr[j + i]);
         if (refcount != 0) {
            platform_error_log("Expected zero references, but found %u\n",
                               refcount);
            rc = STATUS_TEST_FAILED;
         }
      }
      j += i;
   }
   if (!SUCCESS(rc)) {
      goto exit;
   }
   cache_flush(cc);
   dirty_count = cache_count_dirty(cc);
   if (dirty_count != 0) {
      platform_error_log("Expected no dirty entries but found: %u",
                         dirty_count);
      rc = STATUS_TEST_FAILED;
      goto exit;
   }

   rc = test_cache_page_pin(
      cc, &addr_arr[pages_allocated - cfg->page_capacity], cfg->page_capacity);

   /*
    * Deallocate all the entries.
    */
   for (uint32 i = 0; i < extents_to_allocate; i++) {
      uint64     addr = addr_arr[i * pages_per_extent];
      allocator *al   = cache_get_allocator(cc);
      refcount   ref  = allocator_dec_ref(al, addr, PAGE_TYPE_MISC);
      platform_assert(ref == AL_NO_REFS);
      cache_extent_discard(cc, addr, PAGE_TYPE_MISC);
      ref = allocator_dec_ref(al, addr, PAGE_TYPE_MISC);
      platform_assert(ref == AL_FREE);
   }

exit:
   if (addr_arr) {
      platform_free(hid, addr_arr);
   }

   if (page_arr) {
      platform_free(hid, page_arr);
   }

   if (SUCCESS(rc)) {
      platform_default_log("cache_test: basic test passed\n");
   } else {
      platform_default_log("cache_test: basic test failed\n");
   }

   return rc;
}

typedef struct {
   enum { MONO, RAND, HOP } type;
   union {
      struct {
         uint32 cur;
         int32  incr;
      } mono;
      struct {
         random_state rs;
         uint32       min;
         uint32       max;
      } rand;
      struct {
         uint32 start;
         uint32 end;
         int32  incr;
         bool32 arity;
      } hop;
   };
} cache_test_index_itor;

static void
cache_test_index_itor_mono_init(cache_test_index_itor *itor,
                                uint32                 cur,
                                int32                  incr)
{
   itor->type      = MONO;
   itor->mono.cur  = cur;
   itor->mono.incr = incr;
}

/*
 * cache_test_index_itor_rand_init() --
 *
 * Index iterator random initializer. Caller provides us a [min, max] capacity
 * values to use as the range between which to randomly choose a page address.
 */
static void
cache_test_index_itor_rand_init(cache_test_index_itor *itor,
                                uint32                 seed,
                                uint32                 min,
                                uint32                 max)
{
   itor->type = RAND;
   random_init(&itor->rand.rs, seed, 0);

   platform_assert(min > 0);
   platform_assert(max > 0);
   platform_assert((min < max), "min=%u, max=%u", min, max);

   itor->rand.min = min;
   itor->rand.max = max;
}

static void
cache_test_index_itor_hop_init(cache_test_index_itor *itor,
                               uint32                 start,
                               uint32                 end,
                               int32                  incr)
{
   itor->type      = HOP;
   itor->hop.start = start;
   itor->hop.end   = end;
   itor->hop.incr  = incr;
   itor->hop.arity = 0;
}

static uint32
cache_test_index_itor_get(cache_test_index_itor *itor)
{
   uint32 idx;

   switch (itor->type) {
      case MONO:
         idx = itor->mono.cur;
         itor->mono.cur += itor->mono.incr;
         break;
      case HOP:
         if (itor->hop.arity == 0) {
            idx = itor->hop.start;
            itor->hop.start += itor->hop.incr;
         } else {
            itor->hop.end -= itor->hop.incr;
            idx = itor->hop.end;
         }
         itor->hop.arity = !itor->hop.arity;
         break;
      case RAND:
      {
         uint32 range = itor->rand.max - itor->rand.min;
         idx = itor->rand.min + random_next_uint32(&itor->rand.rs) % range;
         break;
      }
   }

   return idx;
}

static platform_status
cache_test_dirty_flush(cache                 *cc,
                       clockcache_config     *cfg,
                       const char            *testname,
                       const uint64          *addr_arr,
                       cache_test_index_itor *itor)
{
   platform_status rc = STATUS_OK;
   timestamp       t_start;

   platform_error_log("Running Flush %s test case ... ", testname);
   /*
    * Get all entries for write, mark dirty, release, and
    * flush. Verify that there are no dirty entries afterwards.
    */
   for (uint32 i = 0; i < cfg->page_capacity; i++) {
      const uint32 idx = cache_test_index_itor_get(itor);
      page_handle  ph_handle =
         test_cache_get_page(cc, addr_arr[idx], TRUE, PAGE_TYPE_MISC);
      page_handle *ph             = &ph_handle;
      bool32       claim_obtained = cache_try_claim(cc, ph);
      if (!claim_obtained) {
         platform_error_log("Expected uncontested claim, but failed\n");
         rc = STATUS_TEST_FAILED;
      }
      cache_lock(cc, ph);
      uint32 refcount = cache_get_read_ref(cc, ph);
      if (refcount != 1) {
         platform_error_log("Expected one reference, but found %u\n", refcount);
         rc = STATUS_TEST_FAILED;
         break;
      }
      cache_unlock(cc, ph);
      cache_unclaim(cc, ph);
      cache_unget(cc, ph);
      refcount = test_cache_released_read_refs(cc, addr_arr[idx]);
      if (refcount != 0) {
         platform_error_log("Expected zero references, but found %u\n",
                            refcount);
         rc = STATUS_TEST_FAILED;
         break;
      }
   }
   if (!SUCCESS(rc)) {
      goto done;
   }
   t_start = platform_get_timestamp();
   cache_flush(cc);
   t_start = NSEC_TO_MSEC(platform_timestamp_elapsed(t_start));
   platform_default_log("Flush %s took %lu msec (%lu MiB/sec)\n",
                        testname,
                        t_start,
                        (cfg->page_capacity << cfg->log_page_size) / MiB
                           * SEC_TO_MSEC(1) / t_start);
   uint32 dirty_count = cache_count_dirty(cc);
   if (dirty_count != 0) {
      platform_error_log("Expected no dirty entries but found: %u",
                         dirty_count);
      rc = STATUS_TEST_FAILED;
   }

done:
   return rc;
}

platform_status
test_cache_flush(cache             *cc,
                 clockcache_config *cfg,
                 platform_heap_id   hid,
                 uint64             al_extent_capacity)
{
   platform_default_log("cache_test: flush test started\n");
   platform_status rc       = STATUS_OK;
   uint64         *addr_arr = NULL;
   timestamp       t_start;

   uint64 pages_per_extent = cache_config_pages_per_extent(&cfg->super);
   uint32 extent_capacity  = cfg->page_capacity / pages_per_extent;
   /*
    * Allocator capacity as factor of cache size, accounting for 1 extent
    * as allocator metadata.
    */
   uint32 factor = al_extent_capacity / extent_capacity - 1;
   // Don't allocate >100 times cache size; that's enough span for random IO
   if (factor > 100) {
      factor = 100;
   }
   uint32 extents_to_allocate = factor * extent_capacity;
   uint64 pages_to_allocate   = extents_to_allocate * pages_per_extent;
   platform_default_log("Allocate %d extents ... ", extents_to_allocate);

   addr_arr = TYPED_ARRAY_MALLOC(hid, addr_arr, pages_to_allocate);
   t_start  = platform_get_timestamp();
   rc       = cache_test_alloc_extents(cc, cfg, addr_arr, extents_to_allocate);
   if (!SUCCESS(rc)) {
      platform_error_log("failed.\n");
      /* no need to set status because we got here from an error status */
      goto exit;
   }
   platform_default_log("Allocation took %lu secs\n",
                        NSEC_TO_SEC(platform_timestamp_elapsed(t_start)));

   cache_test_index_itor itor;

   // First: monotonically increasing seq addresses
   cache_test_index_itor_mono_init(&itor, 0, 1);
   rc = cache_test_dirty_flush(cc, cfg, "Seq", addr_arr, &itor);
   if (!SUCCESS(rc)) {
      platform_error_log("failed test seq inc");
      goto exit;
   }

   // Second: monotonically decreasing seq addresses
   cache_test_index_itor_mono_init(&itor, cfg->page_capacity * 2, -1);
   rc = cache_test_dirty_flush(cc, cfg, "Reverse Seq", addr_arr, &itor);
   if (!SUCCESS(rc)) {
      platform_error_log("failed test seq dec");
      goto exit;
   }

   // Third: addresses hopping between min and max
   cache_test_index_itor_hop_init(
      &itor, cfg->page_capacity * 3, cfg->page_capacity * 4, 1);
   rc = cache_test_dirty_flush(cc, cfg, "Hop", addr_arr, &itor);
   if (!SUCCESS(rc)) {
      platform_error_log("failed test seq dec");
      goto exit;
   }

   // Because for this test we require (disk-capacity > 5*cache-capacity),
   // we can assert the following. (That's checked elsewhere.)
   platform_assert((factor >= 4), "factor=%d\n", factor);

   // Based on input db-/cache-sizes specified when running the test, we may
   // end up with factor==4. Account for this lapsed case, to specify some
   // reasonable range of [min < max] for the random iterator to pick from.
   uint32 min_factor = ((factor == 4) ? 2 : 4);

   // Fourth: random addresses
   cache_test_index_itor_rand_init(&itor,
                                   42,
                                   (cfg->page_capacity * min_factor),
                                   cfg->page_capacity * factor);
   rc = cache_test_dirty_flush(cc, cfg, "Random", addr_arr, &itor);
   if (!SUCCESS(rc)) {
      platform_error_log("failed test seq dec");
      goto exit;
   }
   t_start = platform_get_timestamp();
   /*
    * Deallocate all the entries.
    */
   for (uint32 i = 0; i < extents_to_allocate; i++) {
      uint64     addr = addr_arr[i * pages_per_extent];
      allocator *al   = cache_get_allocator(cc);
      refcount   ref  = allocator_dec_ref(al, addr, PAGE_TYPE_MISC);
      platform_assert(ref == AL_NO_REFS);
      cache_extent_discard(cc, addr, PAGE_TYPE_MISC);
      ref = allocator_dec_ref(al, addr, PAGE_TYPE_MISC);
      platform_assert(ref == AL_FREE);
   }
   platform_default_log("Dealloc took %lu secs\n",
                        NSEC_TO_SEC(platform_timestamp_elapsed(t_start)));

exit:
   if (addr_arr) {
      platform_free(hid, addr_arr);
   }

   if (SUCCESS(rc)) {
      platform_default_log("cache_test: flush test passed\n");
   } else {
      platform_default_log("cache_test: flush test failed\n");
   }

   return rc;
}

#define READER_BATCH_SIZE 32

typedef struct {
   page_get_async_state_buffer buffer;
   enum { waiting_on_io, ready_to_continue, done } status;
} cache_test_async_ctxt;

typedef struct {
   cache                *cc;                      // IN
   clockcache_config    *cfg;                     // IN
   task_system          *ts;                      // IN
   platform_thread       thread;                  // IN
   platform_heap_id      hid;                     // IN
   bool32                mt_reader;               // IN readers are MT
   bool32                logger;                  // IN logger thread
   const uint64         *addr_arr;                // IN array of page addrs
   uint64                num_pages;               // IN #of pages to get
   uint64                num_pages_ws;            // IN #of pages in working set
   uint32                sync_probability;        // IN probability of sync get
   page_handle          *handle_arr;              // page handles
   cache_test_async_ctxt ctxt[READER_BATCH_SIZE]; // async_get() contexts
} test_params;

void
test_async_callback(void *ctxt)
{
   cache_test_async_ctxt *test_ctxt = (cache_test_async_ctxt *)ctxt;
   test_ctxt->status                = ready_to_continue;
}

// Wait for in flight async lookups
static void
test_wait_inflight(test_params *params,
                   uint64       batch_start) // Exclusive
{
   uint64 j;

   for (j = 0; j < READER_BATCH_SIZE; j++) {
      cache_test_async_ctxt *ctxt = &params->ctxt[j];

      if (ctxt->status != done) {
         while (ctxt->status != done) {
            if (ctxt->status == waiting_on_io) {
               cache_cleanup(params->cc);
            } else if (ctxt->status == ready_to_continue) {
               ctxt->status     = waiting_on_io;
               async_status res = cache_get_async(&ctxt->buffer);
               if (res == ASYNC_STATUS_DONE) {
                  ctxt->status = done;
               }
            }
         }

         platform_assert(
            page_handle_is_null(&params->handle_arr[batch_start + j]));
         params->handle_arr[batch_start + j] =
            test_cache_get_async_page(&ctxt->buffer);
      }

      platform_assert(page_handle_addr(&params->handle_arr[batch_start + j])
                      == params->addr_arr[batch_start + j]);
   }
}

// Do async reads for a batch of addresses, and wait for them to complete
static bool32
test_do_read_batch(threadid tid, test_params *params, uint64 batch_start)
{
   page_handle  *handle_arr = &params->handle_arr[batch_start];
   const uint64 *addr_arr   = &params->addr_arr[batch_start];
   const bool32  mt_reader  = params->mt_reader;
   cache        *cc         = params->cc;
   uint64        j;

   for (j = 0; j < READER_BATCH_SIZE; j++) {
      async_status           res;
      cache_test_async_ctxt *ctxt = &params->ctxt[j];

      // MT test probabilistically mixes sync and async api to test races
      if (mt_reader && params->sync_probability != 0
          && (tid + batch_start + j) % params->sync_probability == 0)
      {
         handle_arr[j] =
            test_cache_get_page(cc, addr_arr[j], TRUE, PAGE_TYPE_MISC);
         platform_assert(!page_handle_is_null(&handle_arr[j]));
         ctxt->status = done;
      } else {
         cache_get_async_state_init(&ctxt->buffer,
                                    cc,
                                    addr_arr[j],
                                    PAGE_TYPE_MISC,
                                    test_async_callback,
                                    &params->ctxt[j]);
         ctxt->status = waiting_on_io;
         res          = cache_get_async(&ctxt->buffer);
         switch (res) {
            case ASYNC_STATUS_DONE:
               platform_assert(page_handle_is_null(&handle_arr[j]));
               handle_arr[j] = test_cache_get_async_page(&ctxt->buffer);
               platform_assert(!page_handle_is_null(&handle_arr[j]));
               ctxt->status = done;
               break;
            case ASYNC_STATUS_RUNNING:
               break;
            default:
               platform_assert(0);
         }
      }
   }

   // Wait for the batch of async gets to complete
   test_wait_inflight(params, batch_start);

   return FALSE;
}

void
test_reader_thread(void *arg)
{
   test_params   *params     = (test_params *)arg;
   page_handle   *handle_arr = params->handle_arr;
   cache         *cc         = params->cc;
   uint64         i, j, k;
   const uint64   num_pages = ROUNDDOWN(params->num_pages, READER_BATCH_SIZE);
   const threadid tid       = platform_get_tid();
   uint64         progress  = 0;

   for (i = k = 0; i < num_pages; i += READER_BATCH_SIZE) {
      if (params->logger) {
         test_print_progress(&progress,
                             i * 100 / num_pages,
                             PLATFORM_CR "test %3lu%% complete",
                             i * 100 / num_pages);
      }
      // Maintain working set by doing ungets on old pages
      if (i >= k + params->num_pages_ws) {
         for (j = 0; j < READER_BATCH_SIZE; j++) {
            cache_unget(cc, &handle_arr[k + j]);
            handle_arr[k + j] = (page_handle){0};
         }
         k += READER_BATCH_SIZE;
      }
      bool32 need_retry;
      do {
         need_retry = test_do_read_batch(tid, params, i);
         if (need_retry) {
            cache_cleanup(cc);
         }
      } while (need_retry);
   }

   for (; k < num_pages; k += j) {
      for (j = 0; j < READER_BATCH_SIZE; j++) {
         platform_assert(!page_handle_is_null(&handle_arr[k + j]));
         cache_unget(cc, &handle_arr[k + j]);
         handle_arr[k + j] = (page_handle){0};
      }
   }
}

void
test_writer_thread(void *arg)
{
   test_params  *params     = (test_params *)arg;
   const uint64 *addr_arr   = params->addr_arr;
   page_handle  *handle_arr = params->handle_arr;
   cache        *cc         = params->cc;
   uint64        i, k;
   const uint64  num_pages = ROUNDDOWN(params->num_pages, READER_BATCH_SIZE);

   for (i = k = 0; i < num_pages; i++) {
      // Maintain working set by doing ungets on old pages
      if (i >= k + params->num_pages_ws) {
         for (; k < i - params->num_pages_ws; k++) {
            platform_assert(!page_handle_is_null(&handle_arr[k]));
            cache_unlock(cc, &handle_arr[k]);
            cache_unclaim(cc, &handle_arr[k]);
            cache_unget(cc, &handle_arr[k]);
            handle_arr[k] = (page_handle){0};
         }
      }
      do {
         handle_arr[i] =
            test_cache_get_page(cc, addr_arr[i], TRUE, PAGE_TYPE_MISC);
         if (cache_try_claim(cc, &handle_arr[i])) {
            break;
         }
         cache_unget(cc, &handle_arr[i]);
         handle_arr[i] = (page_handle){0};
      } while (1);
      cache_lock(cc, &handle_arr[i]);
   }
   for (; k < num_pages; k++) {
      platform_assert(!page_handle_is_null(&handle_arr[k]));
      cache_unlock(cc, &handle_arr[k]);
      cache_unclaim(cc, &handle_arr[k]);
      cache_unget(cc, &handle_arr[k]);
      handle_arr[k] = (page_handle){0};
   }
}

/*
 * Test the cache's async get() api. This creates a bunch of reader
 * threads which do a mix of sync and async get(). Each thread has a
 * working set which is the number of pages (as a % of cache size)
 * that it keeps a reference to. It also creates a bunch of writer
 * threads to test races with the readers and to test races with
 * eviction.
 */
platform_status
test_cache_async(cache             *cc,
                 clockcache_config *cfg,
                 platform_heap_id   hid,
                 task_system       *ts,
                 uint32             num_reader_threads,
                 uint32             num_writer_threads,
                 uint32             working_set_percent)
{
   platform_status rc;
   uint32          total_threads = num_reader_threads + num_writer_threads;
   test_params    *params =
      TYPED_ARRAY_ZALLOC(hid, params, num_reader_threads + num_writer_threads);
   uint32  i;
   uint64 *addr_arr = NULL;

   /* allocate twice as many pages as the cache capacity */
   uint64       pages_per_extent = cache_config_pages_per_extent(&cfg->super);
   uint32       extent_capacity  = cfg->page_capacity / pages_per_extent;
   uint32       extents_to_allocate = 2 * extent_capacity;
   uint64       pages_to_allocate   = extents_to_allocate * pages_per_extent;
   const uint64 working_set_pages =
      cfg->page_capacity * working_set_percent / 100;

   platform_assert(working_set_percent < 100);
   if (working_set_percent * num_reader_threads > 100) {
      /*
       * If the sum of all threads working set is > 100%, a sync get()
       * will just lock up the cache, so don't do those.
       */
      platform_assert(num_writer_threads == 0);
   }
   platform_default_log(
      "cache_test: async test started with %u+%u threads (ws=%u%%)\n",
      num_reader_threads,
      num_writer_threads,
      working_set_percent);
   addr_arr = TYPED_ARRAY_MALLOC(hid, addr_arr, pages_to_allocate);
   rc       = cache_test_alloc_extents(cc, cfg, addr_arr, extents_to_allocate);
   if (!SUCCESS(rc)) {
      return rc;
   }
   platform_default_log("cache_test: %lu pages allocated\n", pages_to_allocate);
   // Start cache with a clean slate
   cache_flush(cc);
   cache_evict(cc, TRUE);
   cache_reset_stats(cc);
   for (i = 0; i < total_threads; i++) {
      const bool32 is_reader = i < num_reader_threads ? TRUE : FALSE;

      params[i].cc           = cc;
      params[i].cfg          = cfg;
      params[i].addr_arr     = addr_arr;
      params[i].num_pages    = pages_to_allocate;
      params[i].num_pages_ws = is_reader ? working_set_pages : 16;
      /*
       * Probability of test doing sync gets(). If sum of all threads'
       * working set is > 100%, a sync get() will just lock up the
       * cache, so don't do those.
       */
      if (working_set_percent * num_reader_threads > 100) {
         params[i].sync_probability = 0;
      } else {
         params[i].sync_probability = 10;
      }
      params[i].handle_arr =
         TYPED_ARRAY_ZALLOC(hid, params[i].handle_arr, params[i].num_pages);
      params[i].ts     = ts;
      params[i].hid    = hid;
      params[i].logger = (i == 0) ? TRUE : FALSE;
      /*
       * With multiple threads doing async_get() to the same page, it's
       * possible that async_get() returns retry. Not so with single
       * thread.
       */
      params[i].mt_reader = total_threads > 1 ? TRUE : FALSE;
      if (is_reader) {
         rc = platform_thread_create(
            &params[i].thread, FALSE, test_reader_thread, &params[i], hid);
      } else {
         rc = platform_thread_create(
            &params[i].thread, FALSE, test_writer_thread, &params[i], hid);
      }
      if (!SUCCESS(rc)) {
         total_threads = i;
         break;
      }
   }
   // Wait for test threads
   for (i = 0; i < total_threads; i++) {
      platform_thread_join(&params[i].thread);
   }

   for (i = 0; i < pages_to_allocate; i++) {
      cache_assert_ungot(cc, addr_arr[i]);
   }

   for (i = 0; i < total_threads; i++) {
      platform_free(hid, params[i].handle_arr);
   }
   // Deallocate all the entries.
   for (uint32 i = 0; i < extents_to_allocate; i++) {
      uint64     addr = addr_arr[i * pages_per_extent];
      allocator *al   = cache_get_allocator(cc);
      refcount   ref  = allocator_dec_ref(al, addr, PAGE_TYPE_MISC);
      platform_assert(ref == AL_NO_REFS);
      cache_extent_discard(cc, addr, PAGE_TYPE_MISC);
      ref = allocator_dec_ref(al, addr, PAGE_TYPE_MISC);
      platform_assert(ref == AL_FREE);
   }
   platform_free(hid, addr_arr);
   platform_free(hid, params);
   cache_print_stats(Platform_default_log_handle, cc);
   platform_default_log("\n");

   return rc;
}

// Hold one prefetch in the I/O backend so we can notify its waiters before
// the read completes, without relying on thread timing.
static struct {
   const io_ops      *saved_ops;
   io_async_state_ops state_ops;
   io_async_state    *pending;
   async_callback_fn  callback;
   void              *callback_arg;
   bool32             completed;
} delayed_prefetch;

static async_status
delayed_prefetch_run(io_async_state *state)
{
   platform_assert(state == delayed_prefetch.pending);
   return delayed_prefetch.completed ? ASYNC_STATUS_DONE : ASYNC_STATUS_RUNNING;
}

static platform_status
delayed_prefetch_result(io_async_state *state)
{
   platform_assert(delayed_prefetch.completed);
   return STATUS_OK;
}

static platform_status
delayed_prefetch_init(io_async_state   *state,
                      io_handle        *io,
                      io_async_cmd      cmd,
                      uint64            addr,
                      async_callback_fn callback,
                      void             *callback_arg)
{
   platform_assert(cmd == io_async_preadv);
   platform_assert(delayed_prefetch.pending == NULL);
   platform_status rc = delayed_prefetch.saved_ops->async_state_init(
      state, io, cmd, addr, callback, callback_arg);
   if (SUCCESS(rc)) {
      delayed_prefetch.state_ops            = *state->ops;
      delayed_prefetch.state_ops.run        = delayed_prefetch_run;
      delayed_prefetch.state_ops.get_result = delayed_prefetch_result;
      state->ops                            = &delayed_prefetch.state_ops;
      delayed_prefetch.pending              = state;
      delayed_prefetch.callback             = callback;
      delayed_prefetch.callback_arg         = callback_arg;
   }
   return rc;
}

typedef struct {
   page_get_async_state_buffer buffer;
   async_status                status;
   uint32                      callbacks;
} prefetch_waiter;

static void
prefetch_waiter_callback(void *arg)
{
   prefetch_waiter *waiter = arg;
   waiter->callbacks++;
   waiter->status = cache_get_async(&waiter->buffer);
}

static void
test_cache_prefetch_early_wakeup(clockcache *clock)
{
   cache     *cc = (cache *)clock;
   allocator *al = cache_get_allocator(cc);
   uint64     addr;
   uint64     page_size = cache_page_size(cc);
   platform_assert_status_ok(allocator_alloc(al, &addr, PAGE_TYPE_MISC));
   buffer_handle buffer;
   platform_assert_status_ok(platform_buffer_init(&buffer, page_size));
   memset(platform_buffer_getaddr(&buffer), 'B', page_size);
   platform_assert_status_ok(
      io_write(clock->io, platform_buffer_getaddr(&buffer), page_size, addr));

   io_wait_all(clock->io);
   ZERO_CONTENTS(&delayed_prefetch);
   delayed_prefetch.saved_ops = clock->io->ops;
   io_ops held_ops            = *clock->io->ops;
   held_ops.async_state_init  = delayed_prefetch_init;
   clock->io->ops             = &held_ops;
   cache_prefetch_page(cc, addr, PAGE_TYPE_MISC);
   platform_assert(delayed_prefetch.pending != NULL);
   uint32 entry_number = clock->lookup[addr / page_size];
   platform_assert(entry_number < clock->cfg->page_capacity);
   clockcache_entry *entry = &clock->entry[entry_number];
   // Simulate stale bytes left by the prior occupant of this cache entry.
   page_handle observed_page = {.page = &entry->page};
   memset(entry->page.data, 'A', page_size);

   prefetch_waiter waiters[2] = {0};
   for (uint32 i = 0; i < ARRAY_SIZE(waiters); i++) {
      cache_get_async_state_init(&waiters[i].buffer,
                                 cc,
                                 addr,
                                 PAGE_TYPE_MISC,
                                 prefetch_waiter_callback,
                                 &waiters[i]);
      waiters[i].status = cache_get_async(&waiters[i].buffer);
      platform_assert(waiters[i].status == ASYNC_STATUS_RUNNING);
   }
   platform_assert(cache_get_read_ref(cc, &observed_page)
                   == ARRAY_SIZE(waiters));

   // A prior prefetch can clear LOADING, get evicted, and only then release
   // this entry's wait queue. Model that late release while B is still loading.
   // Resume inside each callback to exercise requeueing during release_all.
   for (uint32 wakeup = 1; wakeup <= 2; wakeup++) {
      async_wait_queue_release_all(&entry->waiters);
      for (uint32 i = 0; i < ARRAY_SIZE(waiters); i++) {
         platform_assert(waiters[i].callbacks == wakeup);
         platform_assert(waiters[i].status == ASYNC_STATUS_RUNNING);
      }
      platform_assert(cache_get_read_ref(cc, &observed_page)
                      == ARRAY_SIZE(waiters));
   }

   uint64              iov_count;
   const struct iovec *iov = delayed_prefetch.pending->ops->get_iovec(
      delayed_prefetch.pending, &iov_count);
   platform_assert(iov_count == 1 && iov[0].iov_len == page_size);
   platform_assert_status_ok(
      io_read(clock->io, iov[0].iov_base, page_size, addr));
   delayed_prefetch.completed = TRUE;
   delayed_prefetch.callback(delayed_prefetch.callback_arg);
   delayed_prefetch.pending = NULL;
   clock->io->ops           = delayed_prefetch.saved_ops;

   for (uint32 i = 0; i < ARRAY_SIZE(waiters); i++) {
      platform_assert(waiters[i].callbacks == 3);
      platform_assert(waiters[i].status == ASYNC_STATUS_DONE);
      page_handle acquired_page = test_cache_get_async_page(&waiters[i].buffer);
      page_handle *page         = &acquired_page;
      platform_assert(page_handle_addr(page) == addr);
      platform_assert(memcmp(page_handle_data(page),
                             platform_buffer_getaddr(&buffer),
                             page_size)
                      == 0);
      cache_unget(cc, page);
   }
   platform_assert(cache_get_read_ref(cc, &observed_page) == 0);

   // A ready cache hit should still return immediately, without a callback.
   waiters[0].callbacks = 0;
   cache_get_async_state_init(&waiters[0].buffer,
                              cc,
                              addr,
                              PAGE_TYPE_MISC,
                              prefetch_waiter_callback,
                              &waiters[0]);
   platform_assert(cache_get_async(&waiters[0].buffer) == ASYNC_STATUS_DONE);
   platform_assert(waiters[0].callbacks == 0);
   page_handle hit_page = test_cache_get_async_page(&waiters[0].buffer);
   cache_unget(cc, &hit_page);
   platform_assert(cache_get_read_ref(cc, &observed_page) == 0);

   platform_assert(allocator_dec_ref(al, addr, PAGE_TYPE_MISC) == AL_NO_REFS);
   cache_extent_discard(cc, addr, PAGE_TYPE_MISC);
   platform_assert(allocator_dec_ref(al, addr, PAGE_TYPE_MISC) == AL_FREE);
   platform_assert_status_ok(platform_buffer_deinit(&buffer));
   platform_default_log("cache_test: prefetch early wakeup test passed\n");
}

typedef struct {
   cache           *cc;
   uint64           idle_addr;
   uint64          *addrs;
   page_handle     *pages;
   uint32           page_capacity;
   platform_condvar cv;
   bool32           park;
   bool32           ready;
   bool32           start;
   bool32           filled;
   bool32           resume;
} cache_batch_test;

static void
cache_batch_test_discard(cache *cc, uint64 addr)
{
   allocator *al = cache_get_allocator(cc);
   platform_assert(allocator_dec_ref(al, addr, PAGE_TYPE_MISC) == AL_NO_REFS);
   cache_extent_discard(cc, addr, PAGE_TYPE_MISC);
   // Keep the extent reserved for the next iteration of this test.
   platform_assert_status_ok(allocator_inc_ref(al, addr, NULL));
}

static void
cache_batch_test_alloc_discard(cache_batch_test *test)
{
   page_handle  storage;
   page_handle *page =
      cache_alloc(test->cc, test->idle_addr, PAGE_TYPE_MISC, &storage);
   memset(page_handle_data(page), 0xa5, cache_page_size(test->cc));
   cache_unlock(test->cc, page);
   cache_unclaim(test->cc, page);
   cache_unget(test->cc, page);
   cache_batch_test_discard(test->cc, test->idle_addr);
}

static void
cache_batch_idle_thread(void *arg)
{
   cache_batch_test *test = arg;
   cache_batch_test_alloc_discard(test);
   platform_assert_status_ok(platform_condvar_lock(&test->cv));
   test->ready = TRUE;
   platform_assert_status_ok(platform_condvar_signal(&test->cv));
   while (test->park && !test->resume) {
      platform_assert_status_ok(platform_condvar_wait(&test->cv));
   }
   platform_assert_status_ok(platform_condvar_unlock(&test->cv));
   if (test->park) {
      // Another allocator has used our former batch. Reusing the hint must
      // reacquire ownership and inspect its current contents.
      cache_batch_test_alloc_discard(test);
   }
}

static void
cache_batch_fill_thread(void *arg)
{
   cache_batch_test *test      = arg;
   uint64            page_size = cache_page_size(test->cc);
   uint64            pages_per_extent =
      cache_config_pages_per_extent(cache_get_config(test->cc));
   platform_assert_status_ok(platform_condvar_lock(&test->cv));
   while (!test->start) {
      platform_assert_status_ok(platform_condvar_wait(&test->cv));
   }
   platform_assert_status_ok(platform_condvar_unlock(&test->cv));
   for (uint32 i = 0; i < test->page_capacity; i++) {
      cache_alloc(test->cc, test->addrs[i], PAGE_TYPE_MISC, &test->pages[i]);
      memset(page_handle_data(&test->pages[i]), (uint8)i, page_size);
   }
   // Recycle the first extent twice while keeping every other slot locked.
   // Both refills must recover the freed slots. No eviction can supply
   // replacement slots elsewhere.
   for (uint32 round = 0; round < 2; round++) {
      for (uint32 i = 0; i < pages_per_extent; i++) {
         cache_unlock(test->cc, &test->pages[i]);
         cache_unclaim(test->cc, &test->pages[i]);
         cache_unget(test->cc, &test->pages[i]);
      }
      cache_batch_test_discard(test->cc, test->addrs[0]);
      for (uint32 i = 0; i < pages_per_extent; i++) {
         cache_alloc(test->cc, test->addrs[i], PAGE_TYPE_MISC, &test->pages[i]);
         memset(page_handle_data(&test->pages[i]), (uint8)i, page_size);
      }
   }
   // All slots are held simultaneously. Even one abandoned batch would
   // prevent this thread from reaching here.
   for (uint32 i = 0; i < test->page_capacity; i++) {
      platform_assert(page_handle_addr(&test->pages[i]) == test->addrs[i]);
      for (uint64 j = 0; j < page_size; j++) {
         platform_assert((uint8)page_handle_data(&test->pages[i])[j]
                         == (uint8)i);
      }
      cache_unlock(test->cc, &test->pages[i]);
      cache_unclaim(test->cc, &test->pages[i]);
      cache_unget(test->cc, &test->pages[i]);
   }
   for (uint32 i = 0; i < test->page_capacity; i += pages_per_extent) {
      cache_batch_test_discard(test->cc, test->addrs[i]);
   }
   platform_assert_status_ok(platform_condvar_lock(&test->cv));
   test->filled = TRUE;
   platform_assert_status_ok(platform_condvar_signal(&test->cv));
   platform_assert_status_ok(platform_condvar_unlock(&test->cv));
}

static void
cache_batch_test_wait(cache_batch_test *test, bool32 *condition)
{
   timestamp start = platform_get_timestamp();
   while (TRUE) {
      platform_assert_status_ok(platform_condvar_lock(&test->cv));
      bool32 complete = *condition;
      platform_assert_status_ok(platform_condvar_unlock(&test->cv));
      if (complete) {
         return;
      }
      platform_assert(platform_timestamp_elapsed(start) < SEC_TO_NSEC(10),
                      "cache allocation did not make progress");
      platform_sleep_ns(SEC_TO_NSEC(1) / 1000);
   }
}

static void
test_cache_idle_batches(clockcache *main_cc, platform_heap_id hid)
{
   // Use a separate, small cache so this test does not depend on the stress
   // test's size, clock position, or worker scheduling.
   clockcache_config cfg = *main_cc->cfg;
   cfg.page_capacity     = MAX(256, main_cc->cfg->pages_per_extent);
   cfg.capacity = cfg.page_capacity * cache_page_size((cache *)main_cc);
   clockcache cc;
   platform_assert_status_ok(clockcache_init(&cc,
                                             &cfg,
                                             main_cc->io,
                                             main_cc->al,
                                             "idle batches",
                                             hid,
                                             platform_get_module_id()));
   uint64      *addrs = TYPED_ARRAY_MALLOC(hid, addrs, cfg.page_capacity);
   page_handle *pages = TYPED_ARRAY_MALLOC(hid, pages, cfg.page_capacity);
   platform_assert(addrs != NULL && pages != NULL);
   cache_batch_test test = {.cc            = (cache *)&cc,
                            .addrs         = addrs,
                            .pages         = pages,
                            .page_capacity = cfg.page_capacity};
   platform_assert_status_ok(platform_condvar_init(&test.cv, FALSE));
   platform_assert_status_ok(
      allocator_alloc(cc.al, &test.idle_addr, PAGE_TYPE_MISC));
   uint64 page_size        = cache_page_size(test.cc);
   uint64 pages_per_extent = cache_config_pages_per_extent(&cfg.super);
   for (uint32 i = 0; i < test.page_capacity; i += pages_per_extent) {
      uint64 base;
      platform_assert_status_ok(allocator_alloc(cc.al, &base, PAGE_TYPE_MISC));
      for (uint32 j = 0; j < pages_per_extent; j++) {
         addrs[i + j] = base + j * page_size;
      }
   }
   for (uint32 parked = 0; parked < 2; parked++) {
      test.park  = parked;
      test.ready = test.start = test.filled = test.resume = FALSE;
      platform_thread idle, filler;
      // Reserve a distinct thread ID before the idle thread can exit, so
      // ID reuse cannot accidentally recover an abandoned reservation.
      platform_assert_status_ok(platform_thread_create(
         &filler, FALSE, cache_batch_fill_thread, &test, hid));
      platform_assert_status_ok(platform_thread_create(
         &idle, FALSE, cache_batch_idle_thread, &test, hid));
      cache_batch_test_wait(&test, &test.ready);
      if (!test.park) {
         platform_assert_status_ok(platform_thread_join(&idle));
      }
      platform_assert_status_ok(platform_condvar_lock(&test.cv));
      test.start = TRUE;
      platform_assert_status_ok(platform_condvar_broadcast(&test.cv));
      platform_assert_status_ok(platform_condvar_unlock(&test.cv));
      cache_batch_test_wait(&test, &test.filled);
      platform_assert_status_ok(platform_thread_join(&filler));
      if (test.park) {
         platform_assert_status_ok(platform_condvar_lock(&test.cv));
         test.resume = TRUE;
         platform_assert_status_ok(platform_condvar_signal(&test.cv));
         platform_assert_status_ok(platform_condvar_unlock(&test.cv));
         platform_assert_status_ok(platform_thread_join(&idle));
      }
   }
   for (uint32 i = 0; i < test.page_capacity; i += pages_per_extent) {
      platform_assert(allocator_dec_ref(cc.al, addrs[i], PAGE_TYPE_MISC)
                      == AL_NO_REFS);
      platform_assert(allocator_dec_ref(cc.al, addrs[i], PAGE_TYPE_MISC)
                      == AL_FREE);
   }
   platform_assert(allocator_dec_ref(cc.al, test.idle_addr, PAGE_TYPE_MISC)
                   == AL_NO_REFS);
   platform_assert(allocator_dec_ref(cc.al, test.idle_addr, PAGE_TYPE_MISC)
                   == AL_FREE);
   platform_condvar_destroy(&test.cv);
   platform_free(hid, pages);
   platform_free(hid, addrs);
   clockcache_deinit(&cc);
   platform_default_log(
      "cache_test: idle and exited batch owners test passed\n");
}

#if SPLINTER_DEBUG
typedef struct {
   clockcache      *clock;
   platform_heap_id hid;
   uint64           addr;
   uint32           slot;
   page_handle     *guard_to_release;
   page_handle      winner;
   platform_thread  loader;
   platform_condvar cv;
   bool32           unpublished;
   bool32           resume;
   bool32           finished;
   bool32           attempted;
} cache_admission_race;

static void
cache_admission_wait(cache_admission_race *test, const bool32 *condition)
{
   timestamp start = platform_get_timestamp();
   while (TRUE) {
      platform_assert_status_ok(platform_condvar_lock(&test->cv));
      bool32 complete = *condition;
      platform_assert_status_ok(platform_condvar_unlock(&test->cv));
      if (complete) {
         return;
      }
      platform_assert(platform_timestamp_elapsed(start) < SEC_TO_NSEC(10),
                      "unpublished cache admission did not make progress");
      platform_sleep_ns(SEC_TO_NSEC(1) / 1000);
   }
}

static void
cache_admission_before_publish(clockcache *clock,
                               uint64      addr,
                               uint32      slot,
                               void       *arg)
{
   cache_admission_race *test = arg;
   if (slot != test->slot) {
      return;
   }
   platform_assert(addr == test->addr);
   platform_assert_status_ok(platform_condvar_lock(&test->cv));
   platform_assert(!test->unpublished);
   test->unpublished = TRUE;
   platform_assert_status_ok(platform_condvar_signal(&test->cv));
   while (!test->resume) {
      platform_assert_status_ok(platform_condvar_wait(&test->cv));
   }
   platform_assert_status_ok(platform_condvar_unlock(&test->cv));
}

static void
cache_admission_loader(void *arg)
{
   cache_admission_race *test = arg;
   cache                *cc   = (cache *)test->clock;
   cache_prefetch_page(cc, test->addr, PAGE_TYPE_MISC);
   platform_assert_status_ok(platform_condvar_lock(&test->cv));
   test->finished = TRUE;
   platform_assert_status_ok(platform_condvar_signal(&test->cv));
   platform_assert_status_ok(platform_condvar_unlock(&test->cv));
}

static void
cache_admission_save_candidate(clockcache *clock,
                               uint64      addr,
                               uint32      slot,
                               void       *arg)
{
   cache_admission_race *test = arg;
   cache                *cc   = (cache *)clock;
   platform_assert(addr == test->addr && slot == test->slot);
   clock->before_candidate_ref = NULL;
   cache_batch_test_discard(cc, addr);
   platform_assert_status_ok(platform_thread_create(
      &test->loader, FALSE, cache_admission_loader, test, test->hid));
   cache_admission_wait(test, &test->unpublished);

   // X now names A but has not published its mapping. Free one guard so a
   // second loader must publish A in Y while X remains parked.
   cache_unget(cc, test->guard_to_release);
   test->winner = test_cache_get_page(cc, addr, TRUE, PAGE_TYPE_MISC);
   platform_assert(test->winner.page != &clock->entry[slot].page);
}

static void
cache_admission_after_ref_attempt(clockcache *clock,
                                  uint64      addr,
                                  uint32      slot,
                                  bool32      acquired,
                                  void       *arg)
{
   cache_admission_race *test = arg;
   platform_assert(addr == test->addr && slot == test->slot);
   platform_assert(!test->attempted);
   test->attempted = TRUE;
   platform_assert_status_ok(platform_condvar_lock(&test->cv));
   test->resume = TRUE;
   platform_assert_status_ok(platform_condvar_signal(&test->cv));
   platform_assert_status_ok(platform_condvar_unlock(&test->cv));
   cache_admission_wait(test, &test->finished);

   // The parked admission lost publication to Y and released X. An old
   // implementation had already accepted a foreign reference to this now
   // free slot, or could have enqueued an async waiter on it.
   platform_assert(
      __atomic_load_n(&clock->entry[slot].page.disk_addr, __ATOMIC_RELAXED)
      == UINT64_MAX);
   platform_assert(!acquired,
                   "unpublished admission accepted a stale candidate read "
                   "reference before losing duplicate publication");
}

static void
test_cache_prefetch_unpublished_admission(clockcache      *main_cc,
                                          platform_heap_id hid)
{
   clockcache_config cfg = *main_cc->cfg;
   cfg.page_capacity     = 64;
   cfg.capacity = cfg.page_capacity * cache_page_size((cache *)main_cc);
   clockcache clock;
   platform_assert_status_ok(clockcache_init(&clock,
                                             &cfg,
                                             main_cc->io,
                                             main_cc->al,
                                             "unpublished admission",
                                             hid,
                                             platform_get_module_id()));
   cache       *cc               = (cache *)&clock;
   const uint64 page_size        = cache_page_size(cc);
   const uint64 pages_per_extent = cache_extent_size(cc) / page_size;
   uint64       extents[66];
   uint64       num_extents = 2 + (cfg.page_capacity - 2) / pages_per_extent;
   platform_assert(num_extents <= ARRAY_SIZE(extents));
   for (uint64 i = 0; i < num_extents; i++) {
      platform_assert_status_ok(
         allocator_alloc(clock.al, &extents[i], PAGE_TYPE_MISC));
   }
   buffer_handle buffer;
   platform_assert_status_ok(platform_buffer_init(&buffer, page_size));
   void *bytes = platform_buffer_getaddr(&buffer);
   {
      memset(bytes, 'A', page_size);
      platform_assert_status_ok(
         io_write(clock.io, bytes, page_size, extents[0]));
      page_handle victim =
         test_cache_get_page(cc, extents[0], TRUE, PAGE_TYPE_MISC);
      cache_admission_race test = {
         .clock = &clock,
         .hid   = hid,
         .addr  = extents[0],
         .slot  = (clockcache_entry *)victim.page - clock.entry,
      };
      platform_assert_status_ok(platform_condvar_init(&test.cv, FALSE));
      cache_unget(cc, &victim);
      page_handle guards[63];
      memset(bytes, 'G', page_size);
      for (uint64 i = 0; i < ARRAY_SIZE(guards); i++) {
         uint64 addr = extents[1 + i / pages_per_extent]
                       + (i % pages_per_extent) * page_size;
         platform_assert_status_ok(io_write(clock.io, bytes, page_size, addr));
         guards[i] = test_cache_get_page(cc, addr, TRUE, PAGE_TYPE_MISC);
      }
      test.guard_to_release                 = &guards[0];
      clock.before_candidate_ref            = cache_admission_save_candidate;
      clock.before_candidate_ref_arg        = &test;
      clock.before_prefetch_publish         = cache_admission_before_publish;
      clock.before_prefetch_publish_arg     = &test;
      clock.after_candidate_ref_attempt     = cache_admission_after_ref_attempt;
      clock.after_candidate_ref_attempt_arg = &test;
      page_handle     result;
      platform_status rc =
         cache_get(cc, test.addr, FALSE, PAGE_TYPE_MISC, &result);
      platform_assert(STATUS_IS_EQ(rc, STATUS_BUSY)
                      && page_handle_is_null(&result) && test.attempted);
      platform_assert_status_ok(platform_thread_join(&test.loader));
      clock.before_prefetch_publish     = NULL;
      clock.after_candidate_ref_attempt = NULL;
      platform_assert(
         cache_get_read_ref(
            cc, &(page_handle){.page = &clock.entry[test.slot].page})
         == 0);
      platform_assert(page_handle_addr(&test.winner) == test.addr);
      for (uint64 i = 0; i < page_size; i++) {
         platform_assert(page_handle_data(&test.winner)[i] == 'A');
      }
      cache_unget(cc, &test.winner);
      for (uint64 i = 1; i < ARRAY_SIZE(guards); i++) {
         cache_unget(cc, &guards[i]);
      }
      for (uint64 i = 0; i < num_extents; i++) {
         cache_batch_test_discard(cc, extents[i]);
      }
      platform_condvar_destroy(&test.cv);
   }
   for (uint64 i = 0; i < num_extents; i++) {
      platform_assert(allocator_dec_ref(clock.al, extents[i], PAGE_TYPE_MISC)
                      == AL_NO_REFS);
      platform_assert(allocator_dec_ref(clock.al, extents[i], PAGE_TYPE_MISC)
                      == AL_FREE);
   }
   platform_buffer_deinit(&buffer);
   clockcache_deinit(&clock);
   platform_default_log(
      "cache_test: unpublished prefetch admission race passed\n");
}
#endif

static void
usage(const char *argv0)
{
   platform_error_log(
      "Usage:\n"
      "\t%s [--async | --batch | --prefetch-admission | --perf]\n",
      argv0);
   config_usage();
}

int
cache_test(int argc, char *argv[])
{
   system_config          system_cfg;
   int                    config_argc = argc - 1;
   char                 **config_argv = argv + 1;
   platform_status        rc;
   task_system            ts;
   bool32                 benchmark = FALSE, async = FALSE, batch = FALSE;
   bool32                 prefetch_admission = FALSE;
   uint64                 seed;
   test_message_generator gen;

   platform_register_thread();

   if (argc > 1) {
      if (strncmp(argv[1], "--perf", sizeof("--perf")) == 0) {
         benchmark = TRUE;
         config_argc--;
         config_argv++;
      } else if (strncmp(argv[1], "--async", sizeof("--async")) == 0) {
         async = TRUE;
         config_argc--;
         config_argv++;
      } else if (strcmp(argv[1], "--prefetch-admission") == 0) {
         prefetch_admission = TRUE;
         config_argc--;
         config_argv++;
      } else if (strcmp(argv[1], "--batch") == 0) {
         batch = TRUE;
         config_argc--;
         config_argv++;
      }
   }

   bool use_shmem = config_parse_use_shmem(config_argc, config_argv);
   platform_default_log("\nStarted cache_test %s%s\n",
                        (batch       ? "batch ownership"
                         : benchmark ? "performance benchmarking."
                         : async     ? "async performance."
                                     : "basic"),
                        (use_shmem ? " using shared memory" : ""));

   // Create a heap for io, allocator, cache and splinter
   platform_heap_id hid = NULL;
   rc                   = platform_heap_create(
      platform_get_module_id(), 512 * MiB, use_shmem, &hid);
   platform_assert_status_ok(rc);

   uint64       num_bg_threads[NUM_TASK_TYPES] = {0}; // no bg threads
   core_config *splinter_cfg = TYPED_MALLOC(hid, splinter_cfg);

   rc = test_parse_args(&system_cfg,
                        NULL,
                        &seed,
                        &gen,
                        &num_bg_threads[TASK_TYPE_MEMTABLE],
                        &num_bg_threads[TASK_TYPE_NORMAL],
                        config_argc,
                        config_argv);
   if (!SUCCESS(rc)) {
      platform_error_log("cache_test: failed to parse config: %s\n",
                         platform_status_to_string(rc));
      /*
       * Provided arguments but set things up incorrectly.
       * Print usage so client can fix commandline.
       */
      usage(argv[0]);
      goto cleanup;
   }

   if (system_cfg.allocator_cfg.page_capacity
       < 5 * system_cfg.cache_cfg.page_capacity)
   {
      platform_error_log("cache_test: disk capacity, # of pages=%lu, must be"
                         " at least 5 times cache capacity # of pages=%u\n",
                         system_cfg.allocator_cfg.page_capacity,
                         system_cfg.cache_cfg.page_capacity);
      rc = STATUS_BAD_PARAM;
      goto cleanup;
   }

   io_handle *io = io_handle_create(&system_cfg.io_cfg, hid);
   if (io == NULL) {
      platform_error_log("Failed to create IO handle\n");
      rc = STATUS_NO_MEMORY;
      goto cleanup;
   }

   rc =
      test_rc_allocator_recovery_bootstrap(&system_cfg.allocator_cfg, io, hid);
   if (!SUCCESS(rc)) {
      goto destroy_iohandle;
   }

   rc = test_mini_recover_allocations(
      &system_cfg.allocator_cfg, &system_cfg.cache_cfg, io, hid);
   if (!SUCCESS(rc)) {
      goto destroy_iohandle;
   }

   rc = test_init_task_system(&ts, hid, &system_cfg.task_cfg);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to init splinter state: %s\n",
                         platform_status_to_string(rc));
      goto destroy_iohandle;
   }

   rc_allocator al;
   rc_allocator_init(
      &al, &system_cfg.allocator_cfg, io, hid, platform_get_module_id());
   test_rc_allocator_bounded_scan(&al);

   clockcache *cc = TYPED_MALLOC(hid, cc);
   rc             = clockcache_init(cc,
                        &system_cfg.cache_cfg,
                        io,
                        (allocator *)&al,
                        "test",
                        hid,
                        platform_get_module_id());
   platform_assert_status_ok(rc);

   cache *ccp = (cache *)cc;

   if (prefetch_admission) {
#if SPLINTER_DEBUG
      test_cache_prefetch_unpublished_admission(cc, hid);
#else
      platform_error_log("--prefetch-admission requires SPLINTER_DEBUG\n");
      rc = STATUS_BAD_PARAM;
#endif
   } else if (batch) {
      test_cache_idle_batches(cc, hid);
   } else if (benchmark) {
      rc = test_cache_flush(ccp,
                            &system_cfg.cache_cfg,
                            hid,
                            system_cfg.allocator_cfg.extent_capacity);
   } else if (async) {
      // Single thread, no cache pressure
      rc = test_cache_async(ccp,
                            &system_cfg.cache_cfg,
                            hid,
                            &ts,
                            1,   // num readers
                            0,   // num writers
                            10); // per-thread working set
      // Multi thread, no cache pressure
      platform_assert(SUCCESS(rc));
      rc = test_cache_async(ccp,
                            &system_cfg.cache_cfg,
                            hid,
                            &ts,
                            8,   // num reader
                            0,   // num writers
                            10); // per-thread working set
      // Multi thread, no cache pressure, with writers
      platform_assert(SUCCESS(rc));
      rc = test_cache_async(ccp,
                            &system_cfg.cache_cfg,
                            hid,
                            &ts,
                            8,   // num reader
                            2,   // num writers
                            10); // per-thread working set
      platform_assert(SUCCESS(rc));
      // Single thread, cache pressure
      rc = test_cache_async(ccp,
                            &system_cfg.cache_cfg,
                            hid,
                            &ts,
                            1,   // num readers
                            0,   // num writers
                            80); // per-thread working set
      platform_assert(SUCCESS(rc));
      // Multi  thread, cache pressure
      rc = test_cache_async(ccp,
                            &system_cfg.cache_cfg,
                            hid,
                            &ts,
                            8,   // num readers
                            0,   // num writers
                            80); // per-thread working set
      // Multi  thread, high cache pressure
      rc = test_cache_async(ccp,
                            &system_cfg.cache_cfg,
                            hid,
                            &ts,
                            8,   // num readers
                            0,   // num writers
                            96); // per-thread working set
      platform_assert(SUCCESS(rc));
      test_cache_prefetch_early_wakeup(cc);
      test_cache_idle_batches(cc, hid);
   } else {
      rc = test_cache_basic(ccp, &system_cfg.cache_cfg, hid);
   }
   platform_assert_status_ok(rc);

   clockcache_deinit(cc);
   platform_free(hid, cc);
   rc_allocator_deinit(&al);
   test_deinit_task_system(&ts);
   rc = STATUS_OK;
destroy_iohandle:
   io_handle_destroy(io);
cleanup:
   platform_free(hid, splinter_cfg);
   platform_heap_destroy(&hid);
   platform_deregister_thread();
   return SUCCESS(rc) ? 0 : -1;
}
