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
#include "task.h"
#include "random.h"
#include "test_common.h"
#include "poison.h"

platform_status
test_cache_page_pin(cache *cc, page_handle **page_arr, uint64 page_capacity)
{
   platform_status rc = STATUS_OK;

   platform_assert(cache_count_dirty(cc) == 0);

   for (uint64 curr_page = 0; curr_page < page_capacity; curr_page++) {
      page_handle *page =
         cache_get(cc, page_arr[curr_page]->disk_addr, TRUE, PAGE_TYPE_MISC);
      cache_try_claim(cc, page);
      cache_lock(cc, page);
      cache_pin(cc, page);
      cache_unlock(cc, page);
      cache_unclaim(cc, page);
      cache_unget(cc, page);
   }

   cache_evict(cc, TRUE /* ignore pinned pages */);

   for (uint64 curr_page = 0; curr_page < page_capacity; curr_page++) {
      page_handle *page = page_arr[curr_page];
      if (!cache_present(cc, page)) {
         platform_error_log("Pinned Entry Evicted\n");
         rc = STATUS_TEST_FAILED;
         goto out;
      }
   }

out:
   for (uint64 curr_page = 0; curr_page < page_capacity; curr_page++) {
      page_handle *page = page_arr[curr_page];
      cache_unpin(cc, page);
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
         page_handle *page = cache_alloc(cc, addr, PAGE_TYPE_MISC);
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
   page_handle *page = cache_get(cc, addr, TRUE, PAGE_TYPE_MISC);
   if (!cache_try_claim(cc, page)) {
      cache_unget(cc, page);
      return STATUS_BUSY;
   }

   cache_lock(cc, page);
   memset(page->data, value, page_size);
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
   clockcache   *clock = (clockcache *)cc;
   buffer_handle buffer;
   platform_status rc  = platform_buffer_init(&buffer, page_size);
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
 * Corrupt one full raw-I/O page and make that corruption durable.  The
 * allocator clean-state records are intentionally outside the cache, so this
 * lets the bootstrap test exercise A/B fallback without test-only allocator
 * hooks.
 */
static platform_status
cache_test_zero_disk_page(io_handle *io, uint64 addr, uint64 page_size)
{
   buffer_handle   buffer;
   platform_status rc = platform_buffer_init(&buffer, page_size);
   if (!SUCCESS(rc)) {
      return rc;
   }

   void *page = platform_buffer_getaddr(&buffer);
   memset(page, 0, page_size);
   rc = io_write(io, page, page_size, addr);
   if (SUCCESS(rc)) {
      rc = io_durable_barrier(io);
   }

   platform_status deinit_rc = platform_buffer_deinit(&buffer);
   if (SUCCESS(rc) && !SUCCESS(deinit_rc)) {
      rc = deinit_rc;
   }
   return rc;
}

/*
 * The recovery allocator must bootstrap only the extents it owns itself, then
 * let recovery walkers rebuild all other ownership.  In particular, it must
 * not trust a refcount table persisted by an earlier clean shutdown.
 */
static platform_status
test_rc_allocator_recovery_bootstrap(allocator_config *cfg,
                                     io_handle         *io,
                                     platform_heap_id   hid)
{
   const allocator_root_id root_id = 1;
   uint64                  refcount_buffer_size =
      ROUNDUP(cfg->extent_capacity * sizeof(refcount), cfg->io_cfg->page_size);
   uint64 refcount_extent_count =
      (refcount_buffer_size + cfg->io_cfg->extent_size - 1)
      / cfg->io_cfg->extent_size;
   uint64 reserved_extent_count = 1 + refcount_extent_count + 2;
   uint64 old_clean_state_addr =
      (1 + refcount_extent_count + 1) * cfg->io_cfg->extent_size;
   uint64 super_addr             = 0;
   uint64 stale_extent_addr      = 0;
   platform_status rc            = STATUS_OK;
   rc_allocator   original, recovery, remounted;
   bool32         original_live = FALSE;
   bool32         recovery_live = FALSE;
   bool32         remounted_live = FALSE;

   ZERO_CONTENTS(&original);
   ZERO_CONTENTS(&recovery);
   ZERO_CONTENTS(&remounted);
   platform_default_log("cache_test: allocator recovery bootstrap test started\n");

   rc = rc_allocator_init(
      &original, cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   original_live = TRUE;

   rc = allocator_alloc_super_addr(
      (allocator *)&original, root_id, &super_addr);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc = allocator_alloc(
      (allocator *)&original, &stale_extent_addr, PAGE_TYPE_MISC);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   /* Persist a non-reserved extent that the recovery mount must ignore. */
   rc_allocator_unmount(&original);
   original_live = FALSE;

   /*
    * New allocator initialization writes false records with sequences 1 and
    * 2.  The first clean close writes the true record into slot 0 (sequence
    * 3), making slot 1 the older false record.  Destroying that older slot
    * must not prevent a normal mount from trusting the surviving true slot.
    */
   rc = cache_test_zero_disk_page(
      io, old_clean_state_addr, cfg->io_cfg->page_size);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   rc = rc_allocator_mount(
      &remounted, cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      platform_error_log("cache_test: normal mount did not fall back to its "
                         "surviving clean-state record\n");
      goto cleanup;
   }
   remounted_live = TRUE;
   if (allocator_get_refcount((allocator *)&remounted, stale_extent_addr)
       != AL_ONE_REF)
   {
      platform_error_log("cache_test: normal mount did not load the clean "
                         "refcount map\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }

   /* Simulate a crash after normal mount durably marked the allocator dirty. */
   rc_allocator_deinit(&remounted);
   remounted_live = FALSE;

   rc = rc_allocator_mount(
      &remounted, cfg, io, hid, platform_get_module_id());
   if (rc.r != STATUS_INVALID_STATE.r) {
      platform_error_log("cache_test: normal mount trusted an allocator after "
                         "a simulated mounted crash\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   rc = STATUS_OK;

   rc = rc_allocator_mount_recovery(
      &recovery, cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   recovery_live = TRUE;

   uint64 mounted_super_addr = 0;
   rc = allocator_get_super_addr(
      (allocator *)&recovery, root_id, &mounted_super_addr);
   if (!SUCCESS(rc) || mounted_super_addr != super_addr) {
      platform_error_log("cache_test: recovery lost a persisted superblock "
                         "mapping\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   if (allocator_in_use((allocator *)&recovery) != reserved_extent_count) {
      platform_error_log("cache_test: recovery expected %lu reserved extents, "
                         "found %lu\n",
                         reserved_extent_count,
                         allocator_in_use((allocator *)&recovery));
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   for (uint64 extent_no = 0; extent_no < reserved_extent_count; extent_no++)
   {
      uint64 extent_addr = extent_no * cfg->io_cfg->extent_size;
      if (allocator_get_refcount((allocator *)&recovery, extent_addr)
          != AL_ONE_REF)
      {
         platform_error_log("cache_test: recovery did not reserve extent %lu\n",
                            extent_no);
         rc = STATUS_TEST_FAILED;
         goto cleanup;
      }
   }
   if (allocator_get_refcount((allocator *)&recovery, stale_extent_addr)
       != AL_FREE)
   {
      platform_error_log("cache_test: recovery reused a persisted data "
                         "refcount\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }

   rc = rc_allocator_rebuild_acquire_extent(&recovery, stale_extent_addr);
   if (!SUCCESS(rc)
       || allocator_get_refcount((allocator *)&recovery, stale_extent_addr)
             != AL_ONE_REF)
   {
      platform_error_log("cache_test: first rebuilt extent reference was "
                         "incorrect\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }

   /*
    * Abort never persists a partial rebuild and leaves the allocator marked
    * unclean, so a normal mount must refuse to trust the old refcount map.
    */
   rc_allocator_abort_recovery(&recovery);
   recovery_live = FALSE;

   rc = rc_allocator_mount(
      &remounted, cfg, io, hid, platform_get_module_id());
   if (rc.r != STATUS_INVALID_STATE.r) {
      platform_error_log("cache_test: normal mount trusted an unclean "
                         "allocator map\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   rc = STATUS_OK;

   rc = rc_allocator_mount_recovery(
      &recovery, cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   recovery_live = TRUE;

   rc = rc_allocator_rebuild_acquire_extent(&recovery, stale_extent_addr);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc = rc_allocator_rebuild_acquire_extent(&recovery, stale_extent_addr);
   if (!SUCCESS(rc)
       || allocator_get_refcount((allocator *)&recovery, stale_extent_addr)
             != AL_ONE_REF + 1)
   {
      platform_error_log("cache_test: rebuilt extent reference did not "
                         "increment\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   rc_allocator_rebuild_finish(&recovery);
   rc_allocator_unmount(&recovery);
   recovery_live = FALSE;

   rc = rc_allocator_mount(
      &remounted, cfg, io, hid, platform_get_module_id());
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   remounted_live = TRUE;
   if (allocator_get_refcount((allocator *)&remounted, stale_extent_addr)
       != AL_ONE_REF + 1)
   {
      platform_error_log("cache_test: finished recovery was not persisted by "
                         "clean unmount\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }

cleanup:
   if (remounted_live) {
      rc_allocator_deinit(&remounted);
   }
   if (recovery_live) {
      if (recovery.recovery_in_progress) {
         rc_allocator_abort_recovery(&recovery);
      } else {
         rc_allocator_deinit(&recovery);
      }
   }
   if (original_live) {
      rc_allocator_deinit(&original);
   }

   if (SUCCESS(rc)) {
      platform_default_log("cache_test: allocator recovery bootstrap test "
                           "passed\n");
   }
   return rc;
}

typedef struct {
   cache          *cc;
   volatile bool32 finished;
   platform_status status;
} cache_fence_test_context;

/* Mirrors clockcache's private CC_WRITEBACK_REQUESTED status bit. */
#define CACHE_TEST_WRITEBACK_REQUESTED (1u << 7)

static void
cache_test_writeback_fence_thread(void *arg)
{
   cache_fence_test_context *ctxt = (cache_fence_test_context *)arg;
   ctxt->status = cache_writeback_fence(ctxt->cc);
   __atomic_store_n(&ctxt->finished, TRUE, __ATOMIC_RELEASE);
}

/*
 * Verify that a fence drains an already-dirty page without waiting for a page
 * dirtied after its cut. Keeping old_page write-locked forces the fence to
 * remain in progress long enough to deterministically dirty new_page after
 * the dirty-generation rotation.
 */
static platform_status
test_cache_writeback_fence(cache             *cc,
                           clockcache_config *cfg,
                           platform_heap_id   hid)
{
   platform_status          rc = STATUS_OK;
   uint64                   pages_per_extent =
      cache_config_pages_per_extent(&cfg->super);
   uint64                  *addr_arr = NULL;
   page_handle             *old_page = NULL;
   page_handle             *new_page = NULL;
   bool32                   old_claimed = FALSE, old_locked = FALSE;
   bool32                   new_claimed = FALSE, new_locked = FALSE;
   bool32                   fence_thread_started = FALSE;
   const uint8              old_baseline = 0x11, old_value = 0x22;
   const uint8              new_baseline = 0x33, new_value = 0x44;
   cache_fence_test_context fence_ctxt = {
      .cc = cc, .finished = FALSE, .status = STATUS_OK};
   platform_thread          fence_thread;

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

   old_page = cache_get(cc, addr_arr[0], TRUE, PAGE_TYPE_MISC);
   if (!cache_try_claim(cc, old_page)) {
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   old_claimed = TRUE;
   cache_lock(cc, old_page);
   old_locked = TRUE;
   memset(old_page->data,
          old_value,
          cache_config_page_size(&cfg->super));

   clockcache *clock = (clockcache *)cc;
   uint64 initial_generation =
      __atomic_load_n(&clock->dirty_generation, __ATOMIC_ACQUIRE);
   rc = platform_thread_create(&fence_thread,
                               FALSE,
                               cache_test_writeback_fence_thread,
                               &fence_ctxt,
                               hid);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   fence_thread_started = TRUE;

   timestamp wait_start = platform_get_timestamp();
   while (__atomic_load_n(&clock->dirty_generation, __ATOMIC_ACQUIRE)
          == initial_generation)
   {
      if (platform_timestamp_elapsed(wait_start) > SEC_TO_NSEC(5)) {
         platform_error_log("cache_test: writeback fence did not take a cut\n");
         rc = STATUS_TIMEDOUT;
         goto cleanup;
      }
      platform_sleep_ns(1000);
   }

   new_page = cache_get(
      cc, addr_arr[pages_per_extent], TRUE, PAGE_TYPE_MISC);
   if (!cache_try_claim(cc, new_page)) {
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   new_claimed = TRUE;
   cache_lock(cc, new_page);
   new_locked = TRUE;
   memset(new_page->data,
          new_value,
          cache_config_page_size(&cfg->super));
   cache_unlock(cc, new_page);
   new_locked = FALSE;
   cache_unclaim(cc, new_page);
   new_claimed = FALSE;
   cache_unget(cc, new_page);
   new_page = NULL;

   if (__atomic_load_n(&fence_ctxt.finished, __ATOMIC_ACQUIRE)) {
      platform_error_log("cache_test: fence completed while old page was locked\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
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
   if (old_locked) {
      cache_unlock(cc, old_page);
   }
   if (old_claimed) {
      cache_unclaim(cc, old_page);
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

   if (SUCCESS(rc)) {
      uint32 dirty_count = cache_count_dirty(cc);
      if (dirty_count != 1) {
         platform_error_log("cache_test: expected one post-cut dirty page, "
                            "found %u\n",
                            dirty_count);
         rc = STATUS_TEST_FAILED;
      }
   }
   if (SUCCESS(rc)) {
      rc = cache_durable_barrier(cc);
   }
   if (SUCCESS(rc)) {
      rc = cache_test_verify_disk_page(cc,
                                       addr_arr[0],
                                       cache_config_page_size(&cfg->super),
                                       old_value);
   }
   if (SUCCESS(rc)) {
      /* The post-cut page remains dirty, so its baseline must still be disk. */
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
 * A fence must allow an already-claimed writer to finish its selected dirty
 * interval, but it must not complete before that writer releases the claim.
 */
static platform_status
test_cache_writeback_fence_claimed_page(cache             *cc,
                                        clockcache_config *cfg,
                                        platform_heap_id   hid)
{
   platform_status          rc = STATUS_OK;
   uint64                   pages_per_extent =
      cache_config_pages_per_extent(&cfg->super);
   uint64                  *addr_arr = NULL;
   page_handle             *page = NULL;
   bool32                   claimed = FALSE, locked = FALSE;
   bool32                   fence_thread_started = FALSE;
   const uint8              baseline = 0x55, final_value = 0x66;
   cache_fence_test_context fence_ctxt = {
      .cc = cc, .finished = FALSE, .status = STATUS_OK};
   platform_thread          fence_thread;

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

   page = cache_get(cc, addr_arr[0], TRUE, PAGE_TYPE_MISC);
   if (!cache_try_claim(cc, page)) {
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   claimed = TRUE;
   cache_lock(cc, page);
   locked = TRUE;
   memset(page->data, final_value, cache_config_page_size(&cfg->super));
   cache_unlock(cc, page);
   locked = FALSE;

   clockcache *clock = (clockcache *)cc;
   uint64 initial_generation =
      __atomic_load_n(&clock->dirty_generation, __ATOMIC_ACQUIRE);
   rc = platform_thread_create(&fence_thread,
                               FALSE,
                               cache_test_writeback_fence_thread,
                               &fence_ctxt,
                               hid);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   fence_thread_started = TRUE;

   uint64 entry_no =
      clock->lookup[addr_arr[0] >> clock->cfg->log_page_size];
   platform_assert(entry_no < clock->cfg->page_capacity);
   timestamp wait_start = platform_get_timestamp();
   while ((__atomic_load_n(&clock->dirty_generation, __ATOMIC_ACQUIRE)
           == initial_generation)
          || !(__atomic_load_n(&clock->entry[entry_no].status,
                                __ATOMIC_ACQUIRE)
               & CACHE_TEST_WRITEBACK_REQUESTED))
   {
      if (platform_timestamp_elapsed(wait_start) > SEC_TO_NSEC(5)) {
         platform_error_log("cache_test: fence did not request claimed page\n");
         rc = STATUS_TIMEDOUT;
         goto cleanup;
      }
      platform_sleep_ns(1000);
   }

   if (__atomic_load_n(&fence_ctxt.finished, __ATOMIC_ACQUIRE)) {
      platform_error_log("cache_test: fence completed while page was claimed\n");
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }

   /* This claim predates the request, so it is permitted to finish. */
   cache_lock(cc, page);
   locked = TRUE;
   memset(page->data, final_value, cache_config_page_size(&cfg->super));
   cache_unlock(cc, page);
   locked = FALSE;
   cache_unclaim(cc, page);
   claimed = FALSE;
   cache_unget(cc, page);
   page = NULL;

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
      platform_status join_rc = platform_thread_join(&fence_thread);
      if (!SUCCESS(join_rc) && SUCCESS(rc)) {
         rc = join_rc;
      }
      if (!SUCCESS(fence_ctxt.status) && SUCCESS(rc)) {
         rc = fence_ctxt.status;
      }
   }

   if (SUCCESS(rc) && cache_count_dirty(cc) != 0) {
      platform_error_log("cache_test: claimed-page fence left dirty pages\n");
      rc = STATUS_TEST_FAILED;
   }
   if (SUCCESS(rc)) {
      rc = cache_durable_barrier(cc);
   }
   if (SUCCESS(rc)) {
      rc = cache_test_verify_disk_page(cc,
                                       addr_arr[0],
                                       cache_config_page_size(&cfg->super),
                                       final_value);
   }

   if (addr_arr != NULL) {
      cache_flush(cc);
      allocator *al = cache_get_allocator(cc);
      refcount ref = allocator_dec_ref(al, addr_arr[0], PAGE_TYPE_MISC);
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

platform_status
test_cache_basic(cache *cc, clockcache_config *cfg, platform_heap_id hid)
{
   platform_default_log("cache_test: basic test started\n");
   platform_status rc       = STATUS_OK;
   page_handle   **page_arr = NULL;
   uint64         *addr_arr = NULL;

   rc = test_cache_writeback_fence(cc, cfg, hid);
   if (!SUCCESS(rc)) {
      goto exit;
   }

   rc = test_cache_writeback_fence_claimed_page(cc, cfg, hid);
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
         page_arr[i] = cache_get(cc, addr_arr[page_idx], TRUE, PAGE_TYPE_MISC);
         uint32 refcount = cache_get_read_ref(cc, page_arr[i]);
         if (refcount != 1) {
            platform_error_log("Expected one reference, but found %u\n",
                               refcount);
            rc = STATUS_TEST_FAILED;
            goto exit;
         }
      }
      for (i = 0; i < cfg->page_capacity; i++) {
         cache_unget(cc, page_arr[i]);
         uint32 refcount = cache_get_read_ref(cc, page_arr[i]);
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
         page_arr[i] = cache_get(cc, addr_arr[j + i], TRUE, PAGE_TYPE_MISC);
         bool32 claim_obtained = cache_try_claim(cc, page_arr[i]);
         if (!claim_obtained) {
            platform_error_log("Expected uncontested claim, but failed\n");
            rc = STATUS_TEST_FAILED;
         }
         cache_lock(cc, page_arr[i]);
         uint32 refcount = cache_get_read_ref(cc, page_arr[i]);
         if (refcount != 1) {
            platform_error_log("Expected one reference, but found %u\n",
                               refcount);
            rc = STATUS_TEST_FAILED;
         }
      }
      for (i = 0; i < cfg->page_capacity; i++) {
         cache_unlock(cc, page_arr[i]);
         cache_unclaim(cc, page_arr[i]);
         cache_unget(cc, page_arr[i]);
         uint32 refcount = cache_get_read_ref(cc, page_arr[i]);
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
         page_arr[i] = cache_get(cc, addr_arr[j + i], TRUE, PAGE_TYPE_MISC);
         bool32 claim_obtained = cache_try_claim(cc, page_arr[i]);
         if (!claim_obtained) {
            platform_error_log("Expected uncontested claim, but failed\n");
            rc = STATUS_TEST_FAILED;
         }
         cache_lock(cc, page_arr[i]);
         uint32 refcount = cache_get_read_ref(cc, page_arr[i]);
         if (refcount != 1) {
            platform_error_log("Expected one reference, but found %u\n",
                               refcount);
            rc = STATUS_TEST_FAILED;
         }
      }
      for (i = 0; i < cfg->page_capacity; i++) {
         cache_unlock(cc, page_arr[i]);
         cache_unclaim(cc, page_arr[i]);
         cache_unget(cc, page_arr[i]);
         uint32 refcount = cache_get_read_ref(cc, page_arr[i]);
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

   rc = test_cache_page_pin(cc, page_arr, cfg->page_capacity);

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
      page_handle *ph  = cache_get(cc, addr_arr[idx], TRUE, PAGE_TYPE_MISC);
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
      refcount = cache_get_read_ref(cc, ph);
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
   page_handle         **handle_arr;              // page handles
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

         platform_assert(params->handle_arr[batch_start + j] == NULL);
         params->handle_arr[batch_start + j] =
            cache_get_async_state_result(&ctxt->buffer);
      }

      platform_assert(params->handle_arr[batch_start + j]->disk_addr
                      == params->addr_arr[batch_start + j]);
   }
}

// Do async reads for a batch of addresses, and wait for them to complete
static bool32
test_do_read_batch(threadid tid, test_params *params, uint64 batch_start)
{
   page_handle **handle_arr = &params->handle_arr[batch_start];
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
         handle_arr[j] = cache_get(cc, addr_arr[j], TRUE, PAGE_TYPE_MISC);
         platform_assert(handle_arr[j] != NULL);
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
               platform_assert(handle_arr[j] == NULL);
               handle_arr[j] = cache_get_async_state_result(&ctxt->buffer);
               platform_assert(handle_arr[j] != NULL);
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
   page_handle  **handle_arr = params->handle_arr;
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
            cache_unget(cc, handle_arr[k + j]);
            handle_arr[k + j] = NULL;
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
         platform_assert(handle_arr[k + j] != NULL);
         cache_unget(cc, handle_arr[k + j]);
         handle_arr[k + j] = NULL;
      }
   }
}

void
test_writer_thread(void *arg)
{
   test_params  *params     = (test_params *)arg;
   const uint64 *addr_arr   = params->addr_arr;
   page_handle **handle_arr = params->handle_arr;
   cache        *cc         = params->cc;
   uint64        i, k;
   const uint64  num_pages = ROUNDDOWN(params->num_pages, READER_BATCH_SIZE);

   for (i = k = 0; i < num_pages; i++) {
      // Maintain working set by doing ungets on old pages
      if (i >= k + params->num_pages_ws) {
         for (; k < i - params->num_pages_ws; k++) {
            platform_assert(handle_arr[k] != NULL);
            cache_unlock(cc, handle_arr[k]);
            cache_unclaim(cc, handle_arr[k]);
            cache_unget(cc, handle_arr[k]);
            handle_arr[k] = NULL;
         }
      }
      do {
         handle_arr[i] = cache_get(cc, addr_arr[i], TRUE, PAGE_TYPE_MISC);
         if (cache_try_claim(cc, handle_arr[i])) {
            break;
         }
         cache_unget(cc, handle_arr[i]);
         handle_arr[i] = NULL;
      } while (1);
      cache_lock(cc, handle_arr[i]);
   }
   for (; k < num_pages; k++) {
      platform_assert(handle_arr[k] != NULL);
      cache_unlock(cc, handle_arr[k]);
      cache_unclaim(cc, handle_arr[k]);
      cache_unget(cc, handle_arr[k]);
      handle_arr[k] = NULL;
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

static void
usage(const char *argv0)
{
   platform_error_log("Usage:\n"
                      "\t%s\n",
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
   bool32                 benchmark = FALSE, async = FALSE;
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
      }
   }

   bool use_shmem = config_parse_use_shmem(config_argc, config_argv);
   platform_default_log("\nStarted cache_test %s%s\n",
                        ((argc == 1) ? "basic"
                         : benchmark ? "performance benchmarking."
                                     : "async performance."),
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

   rc = test_rc_allocator_recovery_bootstrap(
      &system_cfg.allocator_cfg, io, hid);
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

   if (benchmark) {
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
