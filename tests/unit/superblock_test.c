// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * superblock_test.c --
 *
 *  Unit tests for the unified superblock (src/superblock.c): fresh format
 *  state, durable publish + re-mount round-trip, the A/B generation selection,
 *  and torn-write fallback (a corrupted newest slot must leave the previous
 *  generation intact and mountable).
 * -----------------------------------------------------------------------------
 */
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.
#include "platform.h"
#include "config.h" // Reqd for definition of master_config{}
#include "allocator.h"
#include "superblock.h"

CTEST_DATA(superblock)
{
   platform_heap_id hid;
   io_config        io_cfg;
   io_handle       *ioh;
   allocator_config allocator_cfg;
};

CTEST_SETUP(superblock)
{
   platform_register_thread();

   bool use_shmem = config_parse_use_shmem(Ctest_argc, (char **)Ctest_argv);

   platform_status rc = platform_heap_create(
      platform_get_module_id(), (256 * MiB), use_shmem, &data->hid);
   platform_assert_status_ok(rc);

   master_config master_cfg;
   config_set_defaults(&master_cfg);

   io_config_init(&data->io_cfg,
                  master_cfg.page_size,
                  master_cfg.extent_size,
                  master_cfg.io_flags,
                  master_cfg.io_perms,
                  master_cfg.io_async_queue_depth,
                  master_cfg.io_filename);

   data->ioh = io_handle_create(&data->io_cfg, data->hid);
   ASSERT_TRUE(data->ioh != NULL, "Failed to create IO handle\n");

   allocator_config_init(
      &data->allocator_cfg, &data->io_cfg, master_cfg.allocator_capacity);
}

CTEST_TEARDOWN(superblock)
{
   io_handle_destroy(data->ioh);
   platform_heap_destroy(&data->hid);
   platform_deregister_thread();
}

/* Zero one raw superblock slot (page) and make the corruption durable. */
static void
superblock_test_corrupt_slot(io_handle *io, uint64 page_size, uint64 slot)
{
   buffer_handle   buffer;
   platform_status rc = platform_buffer_init(&buffer, page_size);
   platform_assert_status_ok(rc);

   void *page = platform_buffer_getaddr(&buffer);
   memset(page, 0, page_size);
   rc = io_write(io, page, page_size, slot * page_size);
   platform_assert_status_ok(rc);
   rc = io_durable_barrier(io);
   platform_assert_status_ok(rc);

   platform_buffer_deinit(&buffer);
}

/*
 * A freshly formatted superblock has an empty tree table and invalid (rebuild)
 * allocation state, and a subsequent mount reads that back.
 */
CTEST2(superblock, test_format_sets_fresh_state)
{
   superblock_context ctx;
   platform_status    rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   rc = superblock_format(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_FALSE(superblock_allocation_state_valid(&ctx));
   ASSERT_EQUAL(0, superblock_num_trees(&ctx));
   superblock_context_deinit(&ctx);

   rc = superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_mount(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_EQUAL(0, superblock_num_trees(&ctx));
   ASSERT_FALSE(superblock_allocation_state_valid(&ctx));
   superblock_context_deinit(&ctx);
}

/*
 * A published tree record and allocation state survive a deinit + re-mount:
 * the newest generation wins.
 */
CTEST2(superblock, test_publish_persists_tree_record)
{
   superblock_context ctx;
   platform_status    rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_format(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));

   superblock_tree_record rec = {
      .table_id                = 1,
      .root_addr               = 0x4000,
      .log_meta_head           = 0,
      .incorporated_generation = SUPERBLOCK_NO_INCORPORATED_GENERATION,
      .unmounted               = TRUE,
   };
   rc = superblock_set_tree_record(&ctx, &rec);
   ASSERT_TRUE(SUCCESS(rc));
   superblock_set_allocation_state_addr(&ctx, 0x8000);
   rc = superblock_publish(&ctx);
   ASSERT_TRUE(SUCCESS(rc));
   superblock_context_deinit(&ctx);

   rc = superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_mount(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_EQUAL(1, superblock_num_trees(&ctx));
   ASSERT_TRUE(superblock_allocation_state_valid(&ctx));
   ASSERT_EQUAL(0x8000, superblock_allocation_state_addr(&ctx));

   superblock_tree_record got;
   rc = superblock_get_tree_record(&ctx, 1, &got);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_EQUAL(0x4000, got.root_addr);
   ASSERT_TRUE(got.unmounted);
   superblock_context_deinit(&ctx);
}

/*
 * The core torn-write guarantee: format writes gen 1 to slot 0 and gen 2 to
 * slot 1, so the next publish targets slot 0 (gen 3) and carries the new tree
 * record.  Corrupting that newest slot must leave slot 1 (gen 2, which predates
 * the record) intact, and mount must fall back to it.
 */
CTEST2(superblock, test_torn_write_falls_back_to_older_generation)
{
   superblock_context ctx;
   platform_status    rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_format(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));

   // After format the image is gen 2 in slot 1, so this publish targets slot 0.
   superblock_tree_record rec = {
      .table_id                = 1,
      .root_addr               = 0x4000,
      .incorporated_generation = SUPERBLOCK_NO_INCORPORATED_GENERATION,
      .unmounted               = TRUE,
   };
   rc = superblock_set_tree_record(&ctx, &rec);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_publish(&ctx);
   ASSERT_TRUE(SUCCESS(rc));
   superblock_context_deinit(&ctx);

   // Simulate a torn write of the newest slot (slot 0, gen 3).
   superblock_test_corrupt_slot(data->ioh, data->io_cfg.page_size, 0);

   // Mount must still succeed, falling back to slot 1 (gen 2) -- which has no
   // tree record, proving the older generation was left intact.
   rc = superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_mount(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_EQUAL(0, superblock_num_trees(&ctx));

   superblock_tree_record got;
   rc = superblock_get_tree_record(&ctx, 1, &got);
   ASSERT_FALSE(SUCCESS(rc));
   superblock_context_deinit(&ctx);
}

/* Both slots corrupt => no valid superblock. */
CTEST2(superblock, test_both_slots_corrupt_is_not_found)
{
   superblock_context ctx;
   platform_status    rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_format(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));
   superblock_context_deinit(&ctx);

   superblock_test_corrupt_slot(data->ioh, data->io_cfg.page_size, 0);
   superblock_test_corrupt_slot(data->ioh, data->io_cfg.page_size, 1);

   rc = superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_mount(&ctx, &data->allocator_cfg);
   ASSERT_FALSE(SUCCESS(rc));
   superblock_context_deinit(&ctx);
}

/*
 * The raw bootstrap geometry read (used before any subsystem is configured)
 * returns the formatted geometry.
 */
CTEST2(superblock, test_read_geometry)
{
   superblock_context ctx;
   platform_status    rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_format(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));
   superblock_context_deinit(&ctx);

   disk_geometry geom;
   rc = superblock_read_geometry(data->io_cfg.filename, &geom);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_EQUAL(data->allocator_cfg.capacity, geom.disk_size);
   ASSERT_EQUAL(data->io_cfg.page_size, geom.page_size);
   ASSERT_EQUAL(data->io_cfg.extent_size, geom.extent_size);
}
