// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * mini_allocator_test.c --
 *
 *  Exercises some of the interfaces in mini_allocator.c
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.
#include "rc_allocator.h"
#include "config.h"
#include "splinterdb/default_data_config.h"
#include "btree.h"
#include "functional/random.h"

#define TEST_MAX_KEY_SIZE 44

/*
 * Global data declaration macro:
 */
CTEST_DATA(mini_allocator)
{
   // Declare head handles for io, allocator, cache memory allocation.
   platform_heap_handle hh;
   platform_heap_id     hid;
   platform_module_id   mid;

   // Config structs for sub-systems that clockcache depends on
   io_config          io_cfg;
   task_system_config task_cfg;
   allocator_config   al_cfg;
   clockcache_config  cache_cfg;
   data_config        default_data_cfg;

   platform_io_handle *io;
   task_system        *tasks;
   allocator          *al;
   rc_allocator       *rc_al;
   clockcache         *clock_cache;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(mini_allocator)
{
   if (Ctest_verbose) {
      platform_set_log_streams(stdout, stderr);
   }
   uint64 heap_capacity = 1024 * MiB;

   default_data_config_init(TEST_MAX_KEY_SIZE, &data->default_data_cfg);

   // Create a heap for io, allocator, cache and splinter
   platform_status rc = platform_heap_create(
      platform_get_module_id(), heap_capacity, &data->hh, &data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   // Allocate memory for and set default for a master config struct
   master_config *master_cfg = TYPED_ZALLOC(data->hid, master_cfg);
   config_set_defaults(master_cfg);

   io_config_init(&data->io_cfg,
                  master_cfg->page_size,
                  master_cfg->extent_size,
                  master_cfg->io_flags,
                  master_cfg->io_perms,
                  master_cfg->io_async_queue_depth,
                  master_cfg->io_filename);

   // no background threads by default.
   uint64 num_bg_threads[NUM_TASK_TYPES] = {0};
   rc = task_system_config_init(&data->task_cfg,
                                TRUE, // use stats
                                num_bg_threads,
                                trunk_get_scratch_size());

   ASSERT_TRUE(SUCCESS(rc));
   allocator_config_init(
      &data->al_cfg, &data->io_cfg, master_cfg->allocator_capacity);

   clockcache_config_init(&data->cache_cfg,
                          &data->io_cfg,
                          master_cfg->cache_capacity,
                          master_cfg->cache_logfile,
                          master_cfg->use_stats);

   // Allocate and initialize the task, IO, rc-allocator sub-systems.
   data->io = TYPED_ZALLOC(data->hid, data->io);
   ASSERT_TRUE((data->io != NULL));
   rc = io_handle_init(data->io, &data->io_cfg, data->hh, data->hid);

   rc = task_system_create(data->hid, data->io, &data->tasks, &data->task_cfg);
   ASSERT_TRUE(SUCCESS(rc));

   data->rc_al = TYPED_ZALLOC(data->hid, data->rc_al);
   ASSERT_TRUE((data->rc_al != NULL));
   rc_allocator_init(data->rc_al,
                     &data->al_cfg,
                     (io_handle *)data->io,
                     data->hid,
                     platform_get_module_id());
   data->al = (allocator *)data->rc_al;

   data->clock_cache = TYPED_ZALLOC(data->hid, data->clock_cache);
   ASSERT_TRUE((data->clock_cache != NULL));

   rc = clockcache_init(data->clock_cache,
                        &data->cache_cfg,
                        (io_handle *)data->io,
                        data->al,
                        "test_mini_allocator",
                        data->hid,
                        data->mid);
   ASSERT_TRUE(SUCCESS(rc));

   platform_free(data->hid, master_cfg);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(mini_allocator)
{
   clockcache_deinit(data->clock_cache);
   platform_free(data->hid, data->clock_cache);

   rc_allocator_deinit(data->rc_al);
   platform_free(data->hid, data->rc_al);
   data->al = NULL;

   task_system_destroy(data->hid, &data->tasks);

   io_handle_deinit(data->io);
   platform_free(data->hid, data->io);

   platform_heap_destroy(&data->hh);
}

/*
 * Exercise the core APIs of the mini-allocator. Before we can do
 * page-allocation, need to allocate a new extent, which will be used by the
 * mini-allocator.
 */
CTEST2(mini_allocator, test_mini_allocator_basic)
{
   platform_status rc          = STATUS_TEST_FAILED;
   uint64          extent_addr = 0;

   rc = allocator_alloc(data->al, &extent_addr, PAGE_TYPE_BRANCH);
   ASSERT_TRUE(SUCCESS(rc));

   mini_allocator  mini_alloc_ctxt;
   mini_allocator *mini = &mini_alloc_ctxt;
   ZERO_CONTENTS(mini);

   page_type type             = PAGE_TYPE_BRANCH;
   uint64    first_ext_addr   = extent_addr;
   bool      keyed_mini_alloc = TRUE;

   uint64 meta_head_addr = allocator_next_page_addr(data->al, first_ext_addr);

   first_ext_addr = mini_init(mini,
                              (cache *)data->clock_cache,
                              &data->default_data_cfg,
                              meta_head_addr,
                              0,
                              MINI_MAX_BATCHES,
                              type,
                              keyed_mini_alloc);
   ASSERT_TRUE(first_ext_addr != extent_addr);

   mini_destroy_unused(mini);
}

/*
 * Exercise the mini-allocator's interfaces for unkeyed page allocations.
 */
CTEST2(mini_allocator, test_mini_unkeyed_many_allocs_one_batch)
{
   platform_status rc          = STATUS_TEST_FAILED;
   uint64          extent_addr = 0;
   page_type       pgtype      = PAGE_TYPE_BRANCH;

   uint64 extents_in_use_prev = allocator_in_use(data->al);
   ASSERT_TRUE(extents_in_use_prev != 0);

   rc = allocator_alloc(data->al, &extent_addr, pgtype);
   ASSERT_TRUE(SUCCESS(rc));

   mini_allocator  mini_alloc_ctxt;
   mini_allocator *mini = &mini_alloc_ctxt;
   ZERO_CONTENTS(mini);

   uint64 first_ext_addr   = extent_addr;
   uint64 num_batches      = 1;
   bool   keyed_mini_alloc = FALSE;

   uint64 meta_head_addr = allocator_next_page_addr(data->al, first_ext_addr);

   first_ext_addr = mini_init(mini,
                              (cache *)data->clock_cache,
                              &data->default_data_cfg,
                              meta_head_addr,
                              0,
                              num_batches,
                              pgtype,
                              keyed_mini_alloc);
   ASSERT_TRUE(first_ext_addr != extent_addr);

   // 1 for the extent holding the metadata page and 1 for the newly allocated
   // extent.
   uint64 exp_num_extents = 2;
   ASSERT_EQUAL(exp_num_extents, mini_num_extents(mini));

   allocator_config *allocator_cfg = allocator_get_config(data->al);

   uint64 extent_size = allocator_cfg->io_cfg->extent_size;
   uint64 page_size   = allocator_cfg->io_cfg->page_size;

   // Test that mini-allocator's state of extent-to-use changes when all pages
   // in currently pre-allocated extent are allocated.
   uint64 next_ext_addr    = 0;
   uint64 exp_next_page    = first_ext_addr;
   uint64 next_page_addr   = 0;
   uint64 npages_in_extent = (extent_size / page_size);
   uint64 batch_num        = 0;
   uint64 pgctr;

   for (pgctr = 0; pgctr < npages_in_extent; pgctr++) {
      next_page_addr = mini_alloc(mini, batch_num, NULL_KEY, &next_ext_addr);

      // All pages allocated should be from previously allocated extent
      ASSERT_EQUAL(first_ext_addr,
                   allocator_extent_base_addr(data->al, next_page_addr));

      // And we should get a diff page for each allocation request
      ASSERT_EQUAL(exp_next_page,
                   next_page_addr,
                   "pgctr=%lu, exp_next_page=%lu, next_page_addr=%lu\n",
                   pgctr,
                   exp_next_page,
                   next_page_addr);

      // We should have pre-allocated a new extent when we just started to
      // allocate pages from currently allocated extent.
      if (pgctr == 0) {
         exp_num_extents++;
      }
      ASSERT_EQUAL(exp_num_extents, mini_num_extents(mini));

      exp_next_page = allocator_next_page_addr(data->al, next_page_addr);
   }

   uint64 new_next_ext_addr;

   // Allocating the next page in a new extent pre-allocates a new extent.
   next_page_addr = mini_alloc(mini, batch_num, NULL_KEY, &new_next_ext_addr);

   ASSERT_NOT_EQUAL(first_ext_addr, next_ext_addr);

   // This most-recently allocated page should be from the recently
   // pre-allocated extent, tracked by next_ext_addr. In fact it should be
   // exactly that 1st page on that pre-allocated extent.
   ASSERT_EQUAL(next_ext_addr,
                next_page_addr,
                "pgctr=%lu, next_ext_addr=%lu, next_page_addr=%lu\n",
                pgctr,
                next_ext_addr,
                next_page_addr);

   // The alloc of this 1st page should have pre-allocated a new extent.
   exp_num_extents++;
   ASSERT_EQUAL(exp_num_extents, mini_num_extents(mini));

   // Release extents reserved by mini-allocator, to verify extents in-use.
   mini_deinit(mini, NULL_KEY, NULL_KEY);

   exp_num_extents = 0;
   ASSERT_EQUAL(exp_num_extents, mini_num_extents(mini));

   uint64 extents_in_use_now = allocator_in_use(data->al);
   ASSERT_EQUAL(extents_in_use_prev, extents_in_use_now);
}

/*
 * Exercise the mini-allocator's interfaces for unkeyed page allocations,
 * pretending that we are allocating pages for all levels of the trunk's nodes.
 * Then, exercise the method to print contents of chain of unkeyed allocator's
 * meta-data pages to ensure that the print function works reasonably.
 */
CTEST2(mini_allocator, test_trunk_mini_unkeyed_allocs_print_diags)
{
   platform_status rc          = STATUS_TEST_FAILED;
   uint64          extent_addr = 0;
   page_type       pgtype      = PAGE_TYPE_TRUNK;

   rc = allocator_alloc(data->al, &extent_addr, pgtype);
   ASSERT_TRUE(SUCCESS(rc));

   mini_allocator  mini_alloc_ctxt;
   mini_allocator *mini = &mini_alloc_ctxt;
   ZERO_CONTENTS(mini);

   uint64 first_ext_addr   = extent_addr;
   uint64 num_batches      = TRUNK_MAX_HEIGHT;
   bool   keyed_mini_alloc = FALSE;

   uint64 meta_head_addr = allocator_next_page_addr(data->al, first_ext_addr);

   first_ext_addr = mini_init(mini,
                              (cache *)data->clock_cache,
                              &data->default_data_cfg,
                              meta_head_addr,
                              0,
                              num_batches,
                              pgtype,
                              keyed_mini_alloc);
   ASSERT_TRUE(first_ext_addr != extent_addr);

   uint64 npages_in_extent = data->clock_cache->cfg->pages_per_extent;
   uint64 npages_allocated = 0;

   uint64 exp_num_extents = mini_num_extents(mini);

   // Allocate n-pages for each level (bctr) of the trunk tree. Pick some
   // large'ish # of pages to allocate so we fill-up multiple metadata pages
   // worth of unkeyed_meta_entry{} entries.
   for (uint64 bctr = 0; bctr < num_batches; bctr++) {
      uint64 num_extents_per_level = (64 * bctr);
      for (uint64 pgctr = 0; pgctr < (num_extents_per_level * npages_in_extent);
           pgctr++)
      {
         uint64 next_page_addr = mini_alloc(mini, bctr, NULL_KEY, NULL);
         ASSERT_FALSE(next_page_addr == 0);

         npages_allocated++;
      }
      exp_num_extents += num_extents_per_level;
   }
   // Validate book-keeping of # of extents allocated by mini-allocator
   ASSERT_EQUAL(exp_num_extents, mini_num_extents(mini));

   // Exercise print method of mini-allocator's keyed meta-page
   CTEST_LOG_INFO("\n** Unkeyed Mini-Allocator dump **\n");
   mini_unkeyed_print((cache *)data->clock_cache, meta_head_addr, pgtype);

   mini_deinit(mini, NULL_KEY, NULL_KEY);
}

/*
 * Exercise the mini-allocator's interfaces for keyed page allocations,
 * pretending that we are allocating pages for all levels of a BTree.
 * Then, exercise the method to print contents of chain of keyed allocator's
 * meta-data pages to ensure that the print function works reasonably.
 */
CTEST2(mini_allocator, test_trunk_mini_keyed_allocs_print_diags)
{
   platform_status rc          = STATUS_TEST_FAILED;
   uint64          extent_addr = 0;
   page_type       pgtype      = PAGE_TYPE_BRANCH;

   rc = allocator_alloc(data->al, &extent_addr, pgtype);
   ASSERT_TRUE(SUCCESS(rc));

   mini_allocator  mini_alloc_ctxt;
   mini_allocator *mini = &mini_alloc_ctxt;
   ZERO_CONTENTS(mini);

   uint64 first_ext_addr   = extent_addr;
   uint64 num_batches      = BTREE_MAX_HEIGHT;
   bool   keyed_mini_alloc = TRUE;

   uint64 meta_head_addr = allocator_next_page_addr(data->al, first_ext_addr);

   first_ext_addr = mini_init(mini,
                              (cache *)data->clock_cache,
                              &data->default_data_cfg,
                              meta_head_addr,
                              0,
                              num_batches,
                              pgtype,
                              keyed_mini_alloc);
   ASSERT_TRUE(first_ext_addr != extent_addr);

   uint64 npages_in_extent = data->clock_cache->cfg->pages_per_extent;
   uint64 npages_allocated = 0;

   uint64 exp_num_extents = mini_num_extents(mini);

   // Allocate n-pages for each level (bctr) of the Btree. Pick some
   // large'ish # of pages to allocate so we fill-up multiple metadata pages
   // worth of unkeyed_meta_entry{} entries.
   for (uint64 bctr = 0; bctr < num_batches; bctr++) {
      uint64 num_extents_per_level = (64 * bctr);
      for (uint64 pgctr = 0; pgctr < (num_extents_per_level * npages_in_extent);
           pgctr++)
      {
         random_state rand_state;
         random_init(&rand_state, pgctr + 42, 0);
         char key_buffer[TEST_MAX_KEY_SIZE] = {0};
         random_bytes(&rand_state, key_buffer, TEST_MAX_KEY_SIZE);

         uint64 next_page_addr = mini_alloc(
            mini, bctr, key_create(sizeof(key_buffer), key_buffer), NULL);
         ASSERT_FALSE(next_page_addr == 0);

         npages_allocated++;
      }
      exp_num_extents += num_extents_per_level;
   }
   // Validate book-keeping of # of extents allocated by mini-allocator
   ASSERT_EQUAL(exp_num_extents, mini_num_extents(mini));

   // Exercise print method of mini-allocator's keyed meta-page
   CTEST_LOG_INFO("\n** Keyed Mini-Allocator dump **\n");
   mini_keyed_print(
      (cache *)data->clock_cache, mini->data_cfg, meta_head_addr, pgtype);

   mini_deinit(mini, NEGATIVE_INFINITY_KEY, POSITIVE_INFINITY_KEY);
}
