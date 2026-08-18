// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * writeback_set_test.c --
 *
 *     Unit tests for the writeback set (src/writeback_set.c).
 *
 *     These drive a bare clockcache: allocate pages, dirty them, and check that
 *     a set issues their writes, waits for completion, and makes them durable.
 *     The observable that matters is cache_count_dirty() -- if
 *     writeback_set_wait() returned before the writes landed, pages would still
 *     be dirty afterwards.
 */

#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

#include "functional/test.h"
#include "splinterdb/data.h"
#include "../config.h"
#include "platform_io.h"
#include "platform_units.h"
#include "rc_allocator.h"
#include "clockcache.h"
// For the init_*_config_from_master_config() helpers.
#include "btree_test_common.h"
#include "writeback_set.h"
#include "poison.h"

CTEST_DATA(writeback_set)
{
   master_config     master_cfg;
   data_config      *data_cfg;
   io_config         io_cfg;
   allocator_config  allocator_cfg;
   clockcache_config cache_cfg;

   platform_heap_id hid;
   io_handle       *io;
   rc_allocator     al;
   clockcache       cc;
};

CTEST_SETUP(writeback_set)
{
   platform_register_thread();
   config_set_defaults(&data->master_cfg);
   data->master_cfg.cache_capacity = MiB_TO_B(64);
   data->data_cfg                  = test_data_config;

   if (!SUCCESS(
          config_parse(&data->master_cfg, 1, Ctest_argc, (char **)Ctest_argv))
       || !init_data_config_from_master_config(data->data_cfg,
                                               &data->master_cfg)
       || !init_io_config_from_master_config(&data->io_cfg, &data->master_cfg)
       || !init_rc_allocator_config_from_master_config(
          &data->allocator_cfg, &data->master_cfg, &data->io_cfg)
       || !init_clockcache_config_from_master_config(
          &data->cache_cfg, &data->master_cfg, &data->io_cfg))
   {
      ASSERT_TRUE(FALSE, "Failed to parse args\n");
   }

   if (!SUCCESS(platform_heap_create(platform_get_module_id(),
                                     512 * MiB,
                                     data->master_cfg.use_shmem,
                                     &data->hid)))
   {
      ASSERT_TRUE(FALSE, "Failed to init heap\n");
   }

   data->io = io_handle_create(&data->io_cfg, data->hid);
   ASSERT_NOT_NULL(data->io);

   ASSERT_TRUE(SUCCESS(rc_allocator_init(&data->al,
                                         &data->allocator_cfg,
                                         data->io,
                                         data->hid,
                                         platform_get_module_id())));
   ASSERT_TRUE(SUCCESS(clockcache_init(&data->cc,
                                       &data->cache_cfg,
                                       data->io,
                                       (allocator *)&data->al,
                                       "test",
                                       data->hid,
                                       platform_get_module_id())));
}

CTEST_TEARDOWN(writeback_set)
{
   clockcache_deinit(&data->cc);
   rc_allocator_deinit(&data->al);
   io_handle_destroy(data->io);
   platform_heap_destroy(&data->hid);
   platform_deregister_thread();
}

/*
 * One extent, deliberately. Issuing many writes before waiting is *less*
 * sensitive, not more: each submission to the io layer tends to reap earlier
 * completions, so a large set cleans itself during the issue phase and a
 * writeback_set_wait() that never waited would still look correct. Keeping the
 * issue phase short leaves the completions genuinely outstanding when wait() is
 * called.
 */

/*
 * Allocate one extent's worth of pages and dirty every one of them. Returns the
 * extent's base address; page i is at base + i * page_size.
 */
static uint64
alloc_and_dirty_extent(clockcache *cc, rc_allocator *al)
{
   uint64          base_addr;
   platform_status rc =
      allocator_alloc((allocator *)al, &base_addr, PAGE_TYPE_MISC);
   ASSERT_TRUE(SUCCESS(rc));

   cache *ccp       = (cache *)cc;
   uint64 page_size = cache_config_page_size(cache_get_config(ccp));
   uint64 pages_per_extent =
      cache_config_pages_per_extent(cache_get_config(ccp));

   for (uint64 i = 0; i < pages_per_extent; i++) {
      uint64       addr = base_addr + i * page_size;
      page_handle *page = cache_alloc(ccp, addr, PAGE_TYPE_MISC);
      ASSERT_NOT_NULL(page);
      // cache_alloc hands back a dirty, write-locked page.
      memset(page->data, (int)(i & 0xff), page_size);
      cache_unlock(ccp, page);
      cache_unclaim(ccp, page);
      cache_unget(ccp, page);
   }
   return base_addr;
}

/*
 * A set of individually added pages is issued, waited for, and made durable,
 * and the pages really are clean afterwards.
 */
CTEST2(writeback_set, test_wait_makes_pages_clean)
{
   cache *ccp       = (cache *)&data->cc;
   uint64 page_size = cache_config_page_size(cache_get_config(ccp));
   uint64 num_pages = cache_config_pages_per_extent(cache_get_config(ccp));

   uint64 base_addr = alloc_and_dirty_extent(&data->cc, &data->al);
   ASSERT_EQUAL(num_pages, cache_count_dirty(ccp));

   writeback_set set;
   writeback_set_init(&set, ccp, data->hid);

   for (uint64 i = 0; i < num_pages; i++) {
      uint64       addr = base_addr + i * page_size;
      page_handle *page = cache_get(ccp, addr, TRUE, PAGE_TYPE_MISC);
      ASSERT_NOT_NULL(page);
      ASSERT_TRUE(SUCCESS(writeback_set_add_page(&set, page, PAGE_TYPE_MISC)));
      cache_unget(ccp, page);
   }
   ASSERT_EQUAL(num_pages, writeback_set_num_requests(&set));

   ASSERT_TRUE(SUCCESS(writeback_set_wait(&set)));
   /*
    * Checked before make_durable(), deliberately: wait() is what establishes
    * completion, and letting the barrier run first would give the I/O extra
    * time to land and mask a wait() that did not actually wait.
    */
   ASSERT_EQUAL(0, cache_count_dirty(ccp));

   ASSERT_TRUE(SUCCESS(writeback_set_make_durable(&set)));
   writeback_set_deinit(&set);
}

/* The same, but adding the extent in one call rather than page by page. */
CTEST2(writeback_set, test_add_extent)
{
   cache *ccp = (cache *)&data->cc;

   uint64 base_addr = alloc_and_dirty_extent(&data->cc, &data->al);
   ASSERT_NOT_EQUAL(0, cache_count_dirty(ccp));

   writeback_set set;
   writeback_set_init(&set, ccp, data->hid);

   ASSERT_TRUE(
      SUCCESS(writeback_set_add_extent(&set, base_addr, PAGE_TYPE_MISC)));
   // One request covers the whole extent, however many pages it holds.
   ASSERT_EQUAL(1, writeback_set_num_requests(&set));

   ASSERT_TRUE(SUCCESS(writeback_set_wait(&set)));
   ASSERT_EQUAL(0, cache_count_dirty(ccp)); // before the barrier; see above

   ASSERT_TRUE(SUCCESS(writeback_set_make_durable(&set)));
   writeback_set_deinit(&set);
}

/* Waiting on an empty set is legal and does nothing. */
CTEST2(writeback_set, test_empty_set)
{
   cache        *ccp = (cache *)&data->cc;
   writeback_set set;
   writeback_set_init(&set, ccp, data->hid);

   ASSERT_EQUAL(0, writeback_set_num_requests(&set));
   ASSERT_TRUE(SUCCESS(writeback_set_wait(&set)));
   ASSERT_TRUE(SUCCESS(writeback_set_make_durable(&set)));

   writeback_set_deinit(&set);
}

/*
 * Adding an already-clean page is a no-op the set still accounts for: the
 * request carries gen == 0, and waiting on it completes immediately.
 */
CTEST2(writeback_set, test_add_clean_page)
{
   cache *ccp       = (cache *)&data->cc;
   uint64 page_size = cache_config_page_size(cache_get_config(ccp));
   uint64 num_pages = cache_config_pages_per_extent(cache_get_config(ccp));

   uint64 base_addr = alloc_and_dirty_extent(&data->cc, &data->al);

   // Clean everything first, so the pages below are already written back.
   writeback_set flush;
   writeback_set_init(&flush, ccp, data->hid);
   ASSERT_TRUE(
      SUCCESS(writeback_set_add_extent(&flush, base_addr, PAGE_TYPE_MISC)));
   ASSERT_TRUE(SUCCESS(writeback_set_wait(&flush)));
   writeback_set_deinit(&flush);
   ASSERT_EQUAL(0, cache_count_dirty(ccp));

   writeback_set set;
   writeback_set_init(&set, ccp, data->hid);
   for (uint64 i = 0; i < num_pages; i++) {
      uint64       addr = base_addr + i * page_size;
      page_handle *page = cache_get(ccp, addr, TRUE, PAGE_TYPE_MISC);
      ASSERT_NOT_NULL(page);
      ASSERT_TRUE(SUCCESS(writeback_set_add_page(&set, page, PAGE_TYPE_MISC)));
      cache_unget(ccp, page);
   }

   ASSERT_TRUE(SUCCESS(writeback_set_wait(&set)));
   ASSERT_EQUAL(0, cache_count_dirty(ccp)); // before the barrier; see above

   ASSERT_TRUE(SUCCESS(writeback_set_make_durable(&set)));
   writeback_set_deinit(&set);
}

/*
 * A set may be reused across rounds: dirty, flush, dirty again, flush again.
 * Catches a wait() that trusted stale receipts from an earlier round.
 */
CTEST2(writeback_set, test_repeated_rounds)
{
   cache *ccp       = (cache *)&data->cc;
   uint64 page_size = cache_config_page_size(cache_get_config(ccp));
   uint64 num_pages = cache_config_pages_per_extent(cache_get_config(ccp));

   uint64 base_addr = alloc_and_dirty_extent(&data->cc, &data->al);

   for (uint64 round = 0; round < 4; round++) {
      if (round > 0) {
         // Re-dirty every page.
         for (uint64 i = 0; i < num_pages; i++) {
            uint64       addr = base_addr + i * page_size;
            page_handle *page = cache_get(ccp, addr, TRUE, PAGE_TYPE_MISC);
            ASSERT_NOT_NULL(page);
            while (!cache_try_claim(ccp, page)) {
               cache_unget(ccp, page);
               page = cache_get(ccp, addr, TRUE, PAGE_TYPE_MISC);
            }
            cache_lock(ccp, page);
            memset(page->data, (int)(round & 0xff), page_size);
            cache_unlock(ccp, page);
            cache_unclaim(ccp, page);
            cache_unget(ccp, page);
         }
         ASSERT_NOT_EQUAL(0, cache_count_dirty(ccp));
      }

      writeback_set set;
      writeback_set_init(&set, ccp, data->hid);
      ASSERT_TRUE(
         SUCCESS(writeback_set_add_extent(&set, base_addr, PAGE_TYPE_MISC)));
      ASSERT_TRUE(SUCCESS(writeback_set_wait(&set)));
      ASSERT_EQUAL(0, cache_count_dirty(ccp)); // before the barrier; see above
      ASSERT_TRUE(SUCCESS(writeback_set_make_durable(&set)));
      writeback_set_deinit(&set);
   }
}
