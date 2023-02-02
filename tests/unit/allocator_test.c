// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * allocator_test.c --
 *
 * Exercises some of the interfaces in allocator.c . Also exercises some
 * extern interfaces from rc_allocator.c, which is based on allocator.c
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

// Setup function for suite, called before every test in suite
CTEST_SETUP(allocator)
{
   uint64 heap_capacity = 1024 * MiB;

   // Create a heap for io and allocator sub-systems
   platform_status rc = platform_heap_create(
      platform_get_module_id(), heap_capacity, &data->hh, &data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   // Allocate memory for and set defaults for a master config struct
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

// Teardown function for suite, called after every test in suite
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
 * Validate that page-/extent-address methods work correctly on some
 * fabricated page and extent addresses.
 */
CTEST2(allocator, test_allocator_valid_page_addr)
{
   ASSERT_TRUE(allocator_valid_page_addr(data->al, 0));

   // Directly access innards of allocator to get page/extent sizes.
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
   // Use accessor method from rc_allocator to get page/extent size
   uint64 page_size   = rc_allocator_page_size(data->rc_al);
   uint64 extent_size = rc_allocator_extent_size(data->rc_al);
   ASSERT_TRUE(allocator_valid_extent_addr(data->al, extent_size));

   ASSERT_FALSE(allocator_valid_extent_addr(data->al, (extent_size - 1)));
   ASSERT_FALSE(allocator_valid_extent_addr(data->al, (extent_size + 1)));

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

/* Validate correctness of allocator_extent_base_addr() conversion function */
CTEST2(allocator, test_allocator_extent_base_addr)
{
   uint64 extent_size = rc_allocator_extent_size(data->rc_al);
   uint64 page_size   = rc_allocator_page_size(data->rc_al);

   uint64 extent_num  = 4321;
   uint64 extent_addr = (extent_num * extent_size);

   // Run through all page-addresses in an extent to verify conversion macro
   uint64 npages_in_extent = (extent_size / page_size);
   uint64 page_addr        = extent_addr;
   ASSERT_EQUAL(extent_addr, allocator_extent_base_addr(data->al, page_addr));

   // Position to 2nd page in the extent.
   page_addr += page_size;
   for (uint64 pgctr = 1; pgctr < npages_in_extent;
        pgctr++, page_addr += page_size)
   {
      ASSERT_EQUAL(extent_addr,
                   allocator_extent_base_addr(data->al, page_addr));
   }

   // Now page_addr should be positioned on the next extent.
   ASSERT_EQUAL((extent_addr + extent_size),
                allocator_extent_base_addr(data->al, page_addr));
}

/* Validate correctness of allocator_extent_number() conversion function */
CTEST2(allocator, test_allocator_extent_number)
{
   uint64 page_size   = rc_allocator_page_size(data->rc_al);
   uint64 extent_size = rc_allocator_extent_size(data->rc_al);

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

/* Validate correctness of allocator_page_number() conversion function */
CTEST2(allocator, test_allocator_page_number)
{
   uint64 page_size   = rc_allocator_page_size(data->rc_al);
   uint64 extent_size = rc_allocator_extent_size(data->rc_al);

   uint64 extent_num  = 4200;
   uint64 extent_addr = (extent_num * extent_size);

   // Run through all page-addresses in an extent to verify conversion macro
   uint64 npages_in_extent = (extent_size / page_size);
   uint64 start_pageno     = (extent_num * npages_in_extent);

   uint64 page_addr = extent_addr;
   for (uint64 pgctr = 0; pgctr < npages_in_extent;
        pgctr++, page_addr += page_size)
   {
      uint64 exp_pageno = (start_pageno + pgctr);
      ASSERT_EQUAL(exp_pageno, allocator_page_number(data->al, page_addr));
   }
}

/* Validate correctness of allocator_page_offset() conversion function */
CTEST2(allocator, test_allocator_page_offset)
{
   uint64 page_size   = rc_allocator_page_size(data->rc_al);
   uint64 extent_size = rc_allocator_extent_size(data->rc_al);

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

/*
 * Validate correctness of allocator_page_valid() boolean checker function.
 * In this test case, we have not done any actual allocations, yet. So use
 * use page-addresses to verify different checks done in the boolean
 * checker-function, some of which may raise error messages. (We don't quite
 * check the actual error, just that all code paths are exercised.)
 */
CTEST2(allocator, test_error_checks_in_allocator_page_valid)
{
   uint64 extent_size = rc_allocator_extent_size(data->rc_al);
   uint64 page_size   = rc_allocator_page_size(data->rc_al);

   uint64 extent_num  = 4200;
   uint64 extent_addr = (extent_num * extent_size);

   uint64 page_addr = extent_addr;

   // Invalid page-address should trip an error message.
   ASSERT_FALSE(
      allocator_page_valid(data->al, (page_addr + 1), PAGE_TYPE_MISC));

   // Should raise an error that extent is unreferenced.
   ASSERT_FALSE(allocator_page_valid(data->al, page_addr, PAGE_TYPE_MISC));

   // Check pages outside capacity of configured db
   page_addr = allocator_get_capacity(data->al) + page_size;
   ASSERT_FALSE(allocator_page_valid(data->al, page_addr, PAGE_TYPE_MISC));
}

/*
 * Validate correctness of allocator_alloc() function. Successive calls
 * to this function should 'allocate' different extents.
 */
CTEST2(allocator, test_allocator_alloc)
{
   uint64          extent_addr1 = 0;
   platform_status rc           = STATUS_TEST_FAILED;

   rc = allocator_alloc(data->al, &extent_addr1, PAGE_TYPE_BRANCH);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_TRUE(extent_addr1 != 0);

   uint64 extent_addr2 = 0;
   rc = allocator_alloc(data->al, &extent_addr2, PAGE_TYPE_BRANCH);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_TRUE(extent_addr2 != 0);

   ASSERT_TRUE(extent_addr1 != extent_addr2);
}

/*
 * Validate correctness of allocator_alloc() and subsequent refcount handling.
 */
CTEST2(allocator, test_allocator_refcounts)
{
   page_type       ptype       = PAGE_TYPE_BRANCH;
   uint64          extent_addr = 0;
   platform_status rc          = STATUS_TEST_FAILED;

   rc = allocator_alloc(data->al, &extent_addr, ptype);
   ASSERT_TRUE(SUCCESS(rc));

   uint8 refcount = allocator_get_refcount(data->al, extent_addr);
   ASSERT_EQUAL(2, refcount);

   refcount = allocator_inc_ref(data->al, extent_addr);
   ASSERT_EQUAL(3, refcount);

   refcount = allocator_dec_ref(data->al, extent_addr, ptype);
   ASSERT_EQUAL(2, refcount);

   refcount = allocator_dec_ref(data->al, extent_addr, ptype);
   ASSERT_EQUAL(1, refcount);

   refcount = allocator_dec_ref(data->al, extent_addr, ptype);
   ASSERT_EQUAL(0, refcount);

   refcount = allocator_get_refcount(data->al, extent_addr);
   ASSERT_EQUAL(0, refcount);
}

/*
 * Validate correctness of allocator_page_valid() boolean checker function
 * after having allocated an extent. As long as it's a valid page address
 * and the holding extent has a non-zero reference count, the boolean function
 * will return TRUE.
 */
CTEST2(allocator, test_allocator_page_valid)
{
   uint64 extent_size = rc_allocator_extent_size(data->rc_al);
   uint64 page_size   = rc_allocator_page_size(data->rc_al);

   uint64          extent_addr = 0;
   platform_status rc =
      allocator_alloc(data->al, &extent_addr, PAGE_TYPE_BRANCH);
   ASSERT_TRUE(SUCCESS(rc));

   // All pages in extent should appear as validly 'allocated'
   uint64 page_addr        = extent_addr;
   uint64 npages_in_extent = (extent_size / page_size);

   for (uint64 pgctr = 0; pgctr < npages_in_extent;
        pgctr++, page_addr += page_size)
   {
      ASSERT_TRUE(allocator_page_valid(data->al, page_addr, PAGE_TYPE_BRANCH));
   }
}
