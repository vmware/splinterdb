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

/*
 * Global data declaration macro:
 */
CTEST_DATA(mini_allocator)
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
CTEST_SETUP(mini_allocator)
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
CTEST_TEARDOWN(mini_allocator)
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
CTEST2(mini_allocator, test_mini_allocator_basic) {}
