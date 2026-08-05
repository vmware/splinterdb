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
   superblock_tree_record rec;
   superblock_get_tree_record(&ctx, &rec);
   ASSERT_EQUAL(0, rec.root_addr); // empty tree
   ASSERT_EQUAL(0,
                rec.first_unincorporated_generation); // replay from the start
   superblock_context_deinit(&ctx);

   rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_mount(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_FALSE(superblock_allocation_state_valid(&ctx));
   superblock_get_tree_record(&ctx, &rec);
   ASSERT_EQUAL(0, rec.root_addr);
   superblock_context_deinit(&ctx);
}

/*
 * A durable tree snapshot and a subsequent durable allocator snapshot (the
 * clean-unmount Part A / Part B sequence) survive a deinit + re-mount: the
 * newest generation wins.
 */
CTEST2(superblock, test_snapshot_persists_state)
{
   superblock_context ctx;
   platform_status    rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_format(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));

   superblock_log_head live = {
      .head = {.addr = 0x6000, .meta_addr = 0x8000, .nonce = {.low = 0x11}}};
   superblock_log_cut(&ctx, live);
   superblock_snapshot_tree(&ctx, 0x4000, 0);
   rc = superblock_make_durable(&ctx);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_FALSE(
      superblock_allocation_state_valid(&ctx)); // snapshot invalidated

   superblock_snapshot_allocator(&ctx, 0x8000);
   rc = superblock_make_durable(&ctx);
   ASSERT_TRUE(SUCCESS(rc));
   superblock_context_deinit(&ctx);

   rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_mount(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_TRUE(superblock_allocation_state_valid(&ctx));
   ASSERT_EQUAL(0x8000, superblock_allocation_state_addr(&ctx));

   superblock_tree_record got;
   superblock_get_tree_record(&ctx, &got);
   ASSERT_EQUAL(0x4000, got.root_addr);
   ASSERT_EQUAL(0x6000, got.live_log.head.addr);
   ASSERT_EQUAL(0x8000, got.live_log.head.meta_addr);
   ASSERT_EQUAL(0x11, got.live_log.head.nonce.low);
   ASSERT_TRUE(SUPERBLOCK_NO_LOG(got.sealed_log)); // no checkpoint in progress
   superblock_context_deinit(&ctx);
}

/*
 * Steady, begin-checkpoint, and complete-checkpoint tree-record states.  L1
 * covers generations 0..5 and is cut at 5, so L2 takes over at 6.
 */
static const superblock_log_head TEST_LOG_L1 = {
   .head = {.addr = 0x6000, .meta_addr = 0x8000, .nonce = {.low = 0x11}},
   .start_generation = 0};
static const superblock_log_head TEST_LOG_L2 = {
   .head = {.addr = 0x10000, .meta_addr = 0x12000, .nonce = {.low = 0x22}},
   .start_generation = 6};

/*
 * Walk the two-log checkpoint state machine through the superblock and confirm
 * each published state reads back: steady {root R0, live L1, no sealed} ->
 * begin {root R0, sealed L1, live L2} -> complete {root R1, live L2, no
 * sealed}.
 */
CTEST2(superblock, test_two_log_checkpoint_transitions)
{
   superblock_context ctx;
   platform_status    rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_format(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));

   // Steady: root R0, live L1, no sealed log.
   superblock_log_cut(&ctx, TEST_LOG_L1);
   superblock_snapshot_tree(&ctx, 0x4000, 5);
   rc = superblock_make_durable(&ctx);
   ASSERT_TRUE(SUCCESS(rc));

   // Begin: cut the log -- L1 becomes sealed, L2 becomes live.
   superblock_log_cut(&ctx, TEST_LOG_L2);
   rc = superblock_make_durable(&ctx);
   ASSERT_TRUE(SUCCESS(rc));

   superblock_tree_record got;
   superblock_get_tree_record(&ctx, &got);
   ASSERT_EQUAL(TEST_LOG_L1.head.meta_addr, got.sealed_log.head.meta_addr);
   ASSERT_EQUAL(TEST_LOG_L2.head.meta_addr, got.live_log.head.meta_addr);

   // Complete: advance the root past L1's coverage (first unincorporated 9 >=
   // L2's start 6), so the sealed log is dropped and L2 carries forward.
   superblock_snapshot_tree(&ctx, 0x4400, 9);
   rc = superblock_make_durable(&ctx);
   ASSERT_TRUE(SUCCESS(rc));
   superblock_context_deinit(&ctx);

   // A fresh mount reads the completed state.
   rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_mount(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));
   superblock_get_tree_record(&ctx, &got);
   ASSERT_EQUAL(0x4400, got.root_addr);
   ASSERT_EQUAL(9, got.first_unincorporated_generation);
   ASSERT_EQUAL(TEST_LOG_L2.head.meta_addr, got.live_log.head.meta_addr);
   ASSERT_TRUE(SUPERBLOCK_NO_LOG(got.sealed_log));
   superblock_context_deinit(&ctx);
}

/*
 * A tree snapshot published while a sealed log is only partly incorporated must
 * keep that sealed log: dropping it would strand the generations it still
 * holds.
 *
 * This is the case a durability checkpoint hits when it commits a root that is
 * newer than the sealed log's start but older than its end.  L1 is sealed
 * covering generations 0..5 (L2 starts at 6); a root whose first unincorporated
 * generation is 5 has not yet folded in generation 5, so L1 must survive.  Once
 * a later root reaches 6, L1 is dropped.
 */
CTEST2(superblock, test_snapshot_preserves_unincorporated_sealed_log)
{
   superblock_context ctx;
   platform_status    rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_format(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));

   // Get to a checkpoint mid-flight: install L1, then cut to L2, which moves L1
   // into the sealed slot.
   superblock_log_cut(&ctx, TEST_LOG_L1);
   superblock_log_cut(&ctx, TEST_LOG_L2);
   rc = superblock_make_durable(&ctx);
   ASSERT_TRUE(SUCCESS(rc));
   superblock_tree_record mid;
   superblock_get_tree_record(&ctx, &mid);
   ASSERT_EQUAL(TEST_LOG_L1.head.meta_addr, mid.sealed_log.head.meta_addr);
   ASSERT_EQUAL(TEST_LOG_L2.head.meta_addr, mid.live_log.head.meta_addr);

   // Commit a root that stops short of L1's last generation: L1 must be kept.
   superblock_snapshot_tree(&ctx, 0x4000, 5);
   rc = superblock_make_durable(&ctx);
   ASSERT_TRUE(SUCCESS(rc));

   superblock_tree_record got;
   superblock_get_tree_record(&ctx, &got);
   ASSERT_EQUAL(0x4000, got.root_addr);
   ASSERT_EQUAL(TEST_LOG_L2.head.meta_addr, got.live_log.head.meta_addr);
   ASSERT_EQUAL(TEST_LOG_L1.head.meta_addr, got.sealed_log.head.meta_addr);
   ASSERT_EQUAL(TEST_LOG_L1.start_generation, got.sealed_log.start_generation);

   // Now a root that covers all of L1's generations: it is dropped.
   superblock_snapshot_tree(&ctx, 0x4400, 6);
   rc = superblock_make_durable(&ctx);
   ASSERT_TRUE(SUCCESS(rc));
   superblock_get_tree_record(&ctx, &got);
   ASSERT_TRUE(SUPERBLOCK_NO_LOG(got.sealed_log));

   superblock_context_deinit(&ctx);
}

/*
 * A torn begin-checkpoint publish must leave the previous (steady) generation
 * intact: mount falls back to {root R0, live L1, no sealed}, which recovery can
 * replay -- never to a half-written checkpoint.
 */
CTEST2(superblock, test_two_log_checkpoint_torn_begin)
{
   superblock_context ctx;
   platform_status    rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_format(&ctx, &data->allocator_cfg); // slot0=gen1, slot1=gen2
   ASSERT_TRUE(SUCCESS(rc));

   // Make the steady state durable into BOTH slots (gen3->slot0, gen4->slot1),
   // so the fallback below is unambiguously the steady state, not the empty
   // format one.
   superblock_log_cut(&ctx, TEST_LOG_L1);
   superblock_snapshot_tree(&ctx, 0x4000, 5);
   rc = superblock_make_durable(&ctx); // gen3 -> slot0
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_make_durable(&ctx); // gen4 -> slot1
   ASSERT_TRUE(SUCCESS(rc));

   // Begin checkpoint: cut the log; this make_durable (gen5) targets slot0.
   superblock_log_cut(&ctx, TEST_LOG_L2);
   rc = superblock_make_durable(&ctx); // gen5 -> slot0
   ASSERT_TRUE(SUCCESS(rc));
   superblock_context_deinit(&ctx);

   // Tear the begin publish (newest slot, slot0/gen5).
   superblock_test_corrupt_slot(data->ioh, data->io_cfg.page_size, 0);

   // Mount falls back to slot1 (gen4) = steady: live L1, no sealed.
   rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_mount(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));
   superblock_tree_record got;
   superblock_get_tree_record(&ctx, &got);
   ASSERT_EQUAL(0x4000, got.root_addr);
   ASSERT_EQUAL(TEST_LOG_L1.head.meta_addr, got.live_log.head.meta_addr);
   ASSERT_TRUE(SUPERBLOCK_NO_LOG(got.sealed_log));
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

   // After format the image is gen 2 in slot 1, so this make_durable targets
   // slot 0.  Snapshot a nonempty root with no live log.
   superblock_snapshot_tree(&ctx, 0x4000, 0);
   rc = superblock_make_durable(&ctx);
   ASSERT_TRUE(SUCCESS(rc));
   superblock_context_deinit(&ctx);

   // Simulate a torn write of the newest slot (slot 0, gen 3).
   superblock_test_corrupt_slot(data->ioh, data->io_cfg.page_size, 0);

   // Mount must still succeed, falling back to slot 1 (gen 2), which carries
   // the format's empty tree (root_addr 0), not the published root -- proving
   // the older generation was left intact by the torn write.
   rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_mount(&ctx, &data->allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));

   superblock_tree_record got;
   superblock_get_tree_record(&ctx, &got);
   ASSERT_EQUAL(0, got.root_addr);
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

   rc =
      superblock_context_init(&ctx, data->ioh, &data->allocator_cfg, data->hid);
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
