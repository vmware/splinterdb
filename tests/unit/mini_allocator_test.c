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
   uint64    first_ext_addr   = 0;
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
 */
CTEST2(mini_allocator, test_mini_alloc_many)
{
   platform_status rc          = STATUS_TEST_FAILED;
   uint64          extent_addr = 0;

   rc = allocator_alloc(data->al, &extent_addr, PAGE_TYPE_BRANCH);
   ASSERT_TRUE(SUCCESS(rc));

   mini_allocator  mini_alloc_ctxt;
   mini_allocator *mini = &mini_alloc_ctxt;
   ZERO_CONTENTS(mini);

   page_type type             = PAGE_TYPE_BRANCH;
   uint64    first_ext_addr   = 0;
   uint64    num_batches      = 1;
   bool      keyed_mini_alloc = TRUE;

   uint64 meta_head_addr = allocator_next_page_addr(data->al, first_ext_addr);

   first_ext_addr = mini_init(mini,
                              (cache *)data->clock_cache,
                              &data->default_data_cfg,
                              meta_head_addr,
                              0,
                              num_batches,
                              type,
                              keyed_mini_alloc);
   ASSERT_TRUE(first_ext_addr != extent_addr);

   uint64 extent_size = allocator_cfg->io_cfg->extent_size;
   uint64 page_size   = allocator_cfg->io_cfg->page_size;

   // Test that mini-allocator's state of extent-to-use changes when all pages
   // in currently pre-allocated extent are allocated.
   uint64 next_ext_addr    = 0;
   uint64 prev_page_addr   = first_ext_addr;
   uint64 npages_in_extent = (extent_size / page_size);
   uint64 batch_num        = 0;
   for (uint64 pgctr = 0; pgctr < (npages_in_extent - 1);
        pgctr++, page_addr += page_size)
   {
      uint64 next_page_addr =
         mini_alloc(mini, batch_num, alloc_key, &next_ext_addr);
   }

   mini_destroy_unused(mini);
}
