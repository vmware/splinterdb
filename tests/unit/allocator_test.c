// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * allocator_test.c --
 *
 *  Exercises some of the interfaces in allocator.c
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.
#include "rc_allocator.h"
#include "config.h"

/*
 * Global data declaration macro:
 */
CTEST_DATA(allocator)
{
   // Declare head handles for io, allocator, cache and splinter allocation.
   platform_heap_handle hh;
   platform_heap_id     hid;
   platform_module_id   mid;

   // Config structs for sub-systems that clockcache depends on
   io_config        io_cfg;
   allocator_config al_cfg;

   platform_io_handle *io;
   allocator          *al;
   rc_allocator       *rc_al;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(allocator)
{
   uint64 heap_capacity = 1024 * MiB;

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

   allocator_config_init(
      &data->al_cfg, &data->io_cfg, master_cfg->allocator_capacity);

   // Allocate and initialize the IO, rc-allocator sub-systems.
   data->io = TYPED_ZALLOC(data->hid, data->io);
   ASSERT_TRUE((data->io != NULL));
   rc = io_handle_init(data->io, &data->io_cfg, data->hh, data->hid);

   data->rc_al = TYPED_ZALLOC(data->hid, data->rc_al);
   ASSERT_TRUE((data->rc_al != NULL));
   rc_allocator_init(data->rc_al,
                     &data->al_cfg,
                     (io_handle *)data->io,
                     data->hid,
                     platform_get_module_id());
   data->al = (allocator *)data->rc_al;

   platform_free(data->hid, master_cfg);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(allocator)
{
   rc_allocator_deinit(data->rc_al);
   platform_free(data->hid, data->rc_al);
   data->al = NULL;

   io_handle_deinit(data->io);
   platform_free(data->hid, data->io);

   platform_heap_destroy(&data->hh);
}

/*
 * Validate that conversion methods work correctly on some fabricated
 * page and extent addresses.
 */
CTEST2(allocator, test_allocator_valid_page_addr)
{
   ASSERT_TRUE(allocator_valid_page_addr(data->al, 0));

   allocator_config *allocator_cfg = allocator_get_config(data->al);

   uint64 page_size   = allocator_cfg->io_cfg->page_size;
   uint64 extent_size = allocator_cfg->io_cfg->extent_size;

   ASSERT_TRUE(allocator_valid_page_addr(data->al, page_size));
   ASSERT_TRUE(allocator_valid_page_addr(data->al, (2 * page_size)));
   ASSERT_TRUE(allocator_valid_page_addr(data->al, (3 * page_size)));

   ASSERT_TRUE(allocator_valid_page_addr(data->al, extent_size));
   ASSERT_TRUE(allocator_valid_page_addr(data->al, (2 * extent_size)));
   ASSERT_TRUE(allocator_valid_page_addr(data->al, (extent_size + page_size)));

   /* Following are invalid page-addresses; so the check should fail */
   ASSERT_FALSE(allocator_valid_page_addr(data->al, (page_size - 1)));
   ASSERT_FALSE(allocator_valid_page_addr(data->al, (page_size + 1)));
   ASSERT_FALSE(allocator_valid_page_addr(data->al, (extent_size - 1)));
   ASSERT_FALSE(allocator_valid_page_addr(data->al, (extent_size + 1)));
   ASSERT_FALSE(allocator_valid_page_addr(
      data->al, ((2 * extent_size) + (page_size / 2))));
}

/* Validate correctness of allocator_valid_extent_addr() boolean */
CTEST2(allocator, test_allocator_valid_extent_addr)
{
   allocator_config *allocator_cfg = allocator_get_config(data->al);

   uint64 extent_size = allocator_cfg->io_cfg->extent_size;
   ASSERT_TRUE(allocator_valid_extent_addr(data->al, extent_size));

   ASSERT_FALSE(allocator_valid_extent_addr(data->al, (extent_size - 1)));
   ASSERT_FALSE(allocator_valid_extent_addr(data->al, (extent_size + 1)));

   uint64 page_size = allocator_cfg->io_cfg->page_size;
   ASSERT_FALSE(
      allocator_valid_extent_addr(data->al, (extent_size + page_size)));

   // Run through all page-addresses in an extent to verify boolean macro
   uint64 npages_in_extent = (extent_size / page_size);
   uint64 extent_addr      = (42 * extent_size);

   uint64 page_addr = extent_addr;
   ASSERT_TRUE(allocator_valid_extent_addr(data->al, page_addr));

   // Position to 2nd page in the extent.
   page_addr += page_size;
   for (uint64 pgctr = 1; pgctr < npages_in_extent;
        pgctr++, page_addr += page_size)
   {
      ASSERT_FALSE(allocator_valid_extent_addr(data->al, page_addr),
                   "pgctr=%lu, page_addr=%lu\n",
                   pgctr,
                   page_addr);
   }

   // Now we should be positioned at the start of next extent.
   ASSERT_TRUE(allocator_valid_extent_addr(data->al, page_addr));
}

/* Validate correctness of allocator_extent_number() conversion function */
CTEST2(allocator, test_allocator_extent_number)
{
   allocator_config *allocator_cfg = allocator_get_config(data->al);

   uint64 extent_size = allocator_cfg->io_cfg->extent_size;
   uint64 page_size   = allocator_cfg->io_cfg->page_size;

   uint64 extent_num  = 42;
   uint64 extent_addr = (extent_num * extent_size);

   // Run through all page-addresses in an extent to verify conversion macro
   uint64 npages_in_extent = (extent_size / page_size);
   uint64 page_addr        = extent_addr;
   ASSERT_EQUAL(extent_num, allocator_extent_number(data->al, page_addr));

   // Position to 2nd page in the extent.
   page_addr += page_size;
   for (uint64 pgctr = 1; pgctr < npages_in_extent;
        pgctr++, page_addr += page_size)
   {
      ASSERT_EQUAL(extent_num, allocator_extent_number(data->al, page_addr));
   }
   // Now page_addr should be positioned on the next extent.
   ASSERT_TRUE(allocator_valid_extent_addr(data->al, page_addr));
   ASSERT_EQUAL((extent_num + 1), allocator_extent_number(data->al, page_addr));
}

/* Validate correctness of allocator_page_offset() conversion function */
CTEST2(allocator, test_allocator_page_offset)
{
   allocator_config *allocator_cfg = allocator_get_config(data->al);

   uint64 extent_size = allocator_cfg->io_cfg->extent_size;
   uint64 page_size   = allocator_cfg->io_cfg->page_size;

   uint64 extent_num  = 4200;
   uint64 extent_addr = (extent_num * extent_size);

   // Run through all page-addresses in an extent to verify conversion macro
   uint64 npages_in_extent = (extent_size / page_size);

   uint64 page_addr = extent_addr;
   for (uint64 pgctr = 0; pgctr < npages_in_extent;
        pgctr++, page_addr += page_size)
   {
      ASSERT_EQUAL(pgctr, allocator_page_offset(data->al, page_addr));
   }
   // Now page_addr should be positioned at the start of the next extent.
   ASSERT_EQUAL(0, allocator_page_offset(data->al, page_addr));
}
