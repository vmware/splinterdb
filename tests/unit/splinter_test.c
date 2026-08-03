// Copyright 2022-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinter_test.c --
 *
 *  Exercises internal interfaces, using private APIs.
 *
 *  If you're writing new unit tests of the public API, please do not use this
 *  file as a template or example.
 *
 * NOTE: There is some duplication of the splinter_do_inserts() in the test
 * cases which adds considerable execution times. The test_inserts() test case
 * will run with the default test configuration, which is sufficiently large
 * enough to trigger a compaction. The expectation is that this unit test case
 * will be invoked on its own, with a reduced memtable capacity to invoke the
 * lookups test case(s):
 *
 * $ bin/unit/splinter_test test_inserts
 * $ bin/unit/splinter_test --memtable-capacity-mib 4 test_lookups
 * -----------------------------------------------------------------------------
 */
#include "core.h"
#include "clockcache.h"
#include "allocator.h"
#include "rc_allocator.h"
#include "task.h"
#include "platform_threads.h"
#include "functional/test.h"
#include "functional/test_async.h"
#include "test_common.h"
#include "config.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

typedef struct shadow_entry {
   uint64 key_offset;
   uint64 key_length;
   uint64 value_length;
} shadow_entry;

typedef struct trunk_shadow {
   data_config    *data_cfg;
   bool32          sorted;
   writable_buffer entries;
   writable_buffer data;
} trunk_shadow;

/* Function prototypes */
static uint64
splinter_do_inserts(void         *datap,
                    core_handle  *spl,
                    bool32        verify,
                    trunk_shadow *shadow); // Out

static platform_status
test_lookup_by_range(void         *datap,
                     core_handle  *spl,
                     uint64        num_inserts,
                     trunk_shadow *shadow,
                     uint64        num_ranges);

// Verify consistency of data after so-many inserts
#define TEST_VERIFY_GRANULARITY 100000

/* Macro to show progress message as workload is running */
#define SHOW_PCT_PROGRESS(op_num, num_ops, msg)                                \
   do {                                                                        \
      if ((num_ops) < 100 || ((op_num) % ((num_ops) / 100)) == 0) {            \
         platform_default_log(PLATFORM_CR msg, 100 * (op_num) / (num_ops));    \
      }                                                                        \
   } while (0)

/*
 * Global data declaration macro:
 */
CTEST_DATA(splinter)
{
   // Declare head handles for io, allocator, cache and splinter allocation.
   platform_heap_id hid;

   // Thread-related config parameters. These don't change for unit tests
   uint32 num_insert_threads;
   uint32 num_lookup_threads;
   uint32 max_async_inflight;

   rc_allocator al;

   // Following get setup pointing to allocated memory
   system_config         *system_cfg;
   test_workload_config  *workload_cfg;
   io_handle             *io;
   clockcache            *clock_cache;
   task_system            tasks;
   test_message_generator gen;

   // Test execution related configuration
   test_exec_config test_exec_cfg;
};

/*
 * -------------------------------------------------------------------------
 * Setup Splinter configuration:
 * -------------------------------------------------------------------------
 */
// clang-format off
CTEST_SETUP(splinter)
{
   platform_register_thread();
   bool use_shmem = config_parse_use_shmem(Ctest_argc, (char **)Ctest_argv);

   // Defaults: For basic unit-tests, use single threads
   data->num_insert_threads = 1;
   data->num_lookup_threads = 1;
   data->max_async_inflight = 64;

   // The config layer still parses per-config arrays; this test uses one.
   int num_tables       = 1;
   uint64 heap_capacity = 1024 * MiB;

   // Create a heap for io, allocator, cache and splinter
   platform_status rc = platform_heap_create(platform_get_module_id(),
                                             heap_capacity,
                                             use_shmem,
                                             &data->hid);
   platform_assert_status_ok(rc);

   // Allocate memory for global config structures
   data->system_cfg = TYPED_ARRAY_MALLOC(data->hid, data->system_cfg,
                                          num_tables);
   data->workload_cfg =
      TYPED_ARRAY_MALLOC(data->hid, data->workload_cfg, num_tables);

   ZERO_STRUCT(data->test_exec_cfg);

   rc = test_parse_args_n(data->system_cfg,
                          &data->test_exec_cfg,
                          data->workload_cfg,
                          &data->gen,
                          num_tables,
                          Ctest_argc,   // argc/argv globals setup by CTests
                          (char **)Ctest_argv);
   platform_assert_status_ok(rc);

   // Establish Max active threads
   uint32 total_threads = data->num_lookup_threads;
   if (total_threads < data->num_insert_threads) {
      total_threads = data->num_insert_threads;
   }

   // Check if IO subsystem has enough reqs for max async IOs inflight
   io_config * io_cfgp = &data->system_cfg->io_cfg;
   if (io_cfgp->kernel_queue_size < total_threads * data->max_async_inflight) {
      io_cfgp->kernel_queue_size =
         ROUNDUP(total_threads * data->max_async_inflight, 32);
      CTEST_LOG_INFO("Bumped up IO queue size to %lu\n",
                   io_cfgp->kernel_queue_size);
   }

   // Allocate and initialize the IO sub-system.
   data->io = io_handle_create(&data->system_cfg->io_cfg, data->hid);
   ASSERT_TRUE((data->io != NULL), "Failed to create IO handle\n");

   rc = test_init_task_system(&data->tasks, data->hid, &data->system_cfg->task_cfg);
   ASSERT_TRUE(SUCCESS(rc),
              "Failed to init splinter state: %s\n",
              platform_status_to_string(rc));

   rc_allocator_init(&data->al, &data->system_cfg->allocator_cfg, data->io, data->hid,
                     platform_get_module_id());

   data->clock_cache = TYPED_MALLOC(data->hid, data->clock_cache);
   ASSERT_TRUE((data->clock_cache != NULL));

   rc = clockcache_init(data->clock_cache,
                        &data->system_cfg->cache_cfg,
                        data->io,
                        (allocator *)&data->al,
                        "test",
                        data->hid,
                        platform_get_module_id());

   ASSERT_TRUE(SUCCESS(rc), "clockcache_init() failed. ");
}

// clang-format on

/*
 * Tear down memory allocated for various sub-systems. Shutdown Splinter.
 */
CTEST_TEARDOWN(splinter)
{
   clockcache_deinit(data->clock_cache);
   platform_free(data->hid, data->clock_cache);

   allocator *alp = (allocator *)&data->al;
   allocator_assert_noleaks(alp);

   rc_allocator_deinit(&data->al);
   test_deinit_task_system(&data->tasks);

   io_handle_destroy(data->io);

   if (data->system_cfg) {
      platform_free(data->hid, data->system_cfg);
   }
   if (data->workload_cfg) {
      platform_free(data->hid, data->workload_cfg);
   }

   platform_heap_destroy(&data->hid);
   platform_deregister_thread();
}

/*
 * **************************************************************************
 * Basic test case to verify trunk_insert() API and validate a very large #
 * of inserts. This test case is designed to insert enough rows to trigger
 * compaction. (We don't, quite, actually verify that compaction has occurred
 * but based on the default test configs, we expect that it would trigger.)
 * **************************************************************************
 */
CTEST2(splinter, test_inserts)
{
   allocator *alp = (allocator *)&data->al;

   core_handle     spl;
   platform_status rc = core_mkfs(&spl,
                                  &data->system_cfg->splinter_cfg,
                                  alp,
                                  (cache *)data->clock_cache,
                                  data->io,
                                  &data->tasks,
                                  test_generate_allocator_root_id(),
                                  data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   // TRUE : Also do verification-after-inserts
   uint64 num_inserts = splinter_do_inserts(data, &spl, TRUE, NULL);
   ASSERT_NOT_EQUAL(0,
                    num_inserts,
                    "Expected to have inserted non-zero rows, num_inserts=%lu.",
                    num_inserts);

   core_destroy(&spl);
}

/*
 * With logging enabled, core_checkpoint() rotates the log via the two-log
 * protocol and advances the durable root.  Data inserted before the checkpoint
 * must survive it, and teardown's allocator_assert_noleaks() must pass (the
 * sealed log's extents are freed, the root is not leaked or double-freed).
 *
 * A zero rotation timeout forces the rotation immediately rather than waiting
 * for insert traffic to trigger one.  The second checkpoint takes no new
 * inserts, so it also covers the empty-memtable forced rotation: the resulting
 * generation retires with no branch at all.
 */
CTEST2(splinter, test_two_log_checkpoint)
{
   allocator *alp = (allocator *)&data->al;
   data->system_cfg->splinter_cfg.use_log =
      TRUE; // exercise the two-log lifecycle

   core_handle     spl;
   platform_status rc = core_mkfs(&spl,
                                  &data->system_cfg->splinter_cfg,
                                  alp,
                                  (cache *)data->clock_cache,
                                  data->io,
                                  &data->tasks,
                                  test_generate_allocator_root_id(),
                                  data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   uint64 num_inserts = splinter_do_inserts(data, &spl, FALSE, NULL);
   ASSERT_NOT_EQUAL(0, num_inserts);

   // Checkpoint: seal the live log into the sealed slot, start a fresh live
   // log, incorporate, advance the durable root, then clear + free the sealed
   // log.
   rc = core_checkpoint(&spl, 0);
   ASSERT_TRUE(SUCCESS(rc));

   // A sample of keys must still be found after the checkpoint.
   lookup_result qdata;
   lookup_result_init(
      &qdata, spl.cfg.data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   DECLARE_AUTO_KEY_BUFFER(keybuf, data->hid);
   const size_t key_size     = data->workload_cfg->key_size;
   uint64       verify_count = (num_inserts < 1000) ? num_inserts : 1000;
   for (uint64 i = 0; i < verify_count; i++) {
      test_key(&keybuf, TEST_RANDOM, i, 0, 0, key_size, 0);
      rc = core_lookup(&spl, key_buffer_key(&keybuf), &qdata);
      ASSERT_TRUE(SUCCESS(rc));
      verify_tuple(
         &spl,
         &data->gen,
         i,
         key_buffer_key(&keybuf),
         merge_accumulator_to_message(lookup_result_accumulator(&qdata)),
         TRUE);
   }
   lookup_result_deinit(&qdata);

   // Second checkpoint with no new inserts is a clean rotate.
   rc = core_checkpoint(&spl, 0);
   ASSERT_TRUE(SUCCESS(rc));

   core_destroy(&spl);
}

/*
 * An application that manages checkpoints itself: the interval policy is off,
 * so nothing arms a checkpoint automatically and core_checkpoint() is the only
 * thing that can rotate the log.  It must therefore cut the log and reclaim
 * what it retires, or the live log would grow without bound.
 *
 * Each checkpoint must (a) install a different live log -- proving a cut
 * happened -- and (b) leave the retired log's metadata extent free.  Total
 * space in use must also stay flat across many checkpoints of the same data.
 */
CTEST2(splinter, test_self_managed_checkpoints_reclaim_log_space)
{
   allocator *alp                         = (allocator *)&data->al;
   data->system_cfg->splinter_cfg.use_log = TRUE;
   // No automatic checkpoints: this test drives them all itself.
   data->system_cfg->splinter_cfg.checkpoint_log_size_bytes = 0;

   core_handle     spl;
   platform_status rc = core_mkfs(&spl,
                                  &data->system_cfg->splinter_cfg,
                                  alp,
                                  (cache *)data->clock_cache,
                                  data->io,
                                  &data->tasks,
                                  test_generate_allocator_root_id(),
                                  data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   uint64 num_inserts = splinter_do_inserts(data, &spl, FALSE, NULL);
   ASSERT_NOT_EQUAL(0, num_inserts);

   superblock_tree_record rec;
   const uint64           num_checkpoints = 10;
   uint64                 baseline_in_use = 0;

   for (uint64 i = 0; i < num_checkpoints; i++) {
      superblock_get_tree_record(&spl.superblock, &rec);
      uint64 retired_meta_addr = rec.live_log.meta_addr;
      ASSERT_NOT_EQUAL(0, retired_meta_addr);

      rc = core_checkpoint(&spl, 0);
      ASSERT_TRUE(SUCCESS(rc));

      // (a) A cut happened: a different log is now live, and it covers only
      // generations from the cut onward.
      superblock_get_tree_record(&spl.superblock, &rec);
      ASSERT_NOT_EQUAL(retired_meta_addr, rec.live_log.meta_addr);
      ASSERT_TRUE(SUPERBLOCK_NO_LOG(rec.sealed_log));

      // (b) The retired log's space came back.
      ASSERT_EQUAL(0,
                   allocator_get_refcount(alp, retired_meta_addr),
                   "checkpoint %lu did not reclaim retired log at %lu\n",
                   i,
                   retired_meta_addr);

      // Space in use must not creep upward checkpoint over checkpoint.
      if (i == 0) {
         baseline_in_use = allocator_in_use(alp);
      } else {
         ASSERT_TRUE(allocator_in_use(alp) <= baseline_in_use,
                     "space in use grew from %lu to %lu by checkpoint %lu\n",
                     baseline_in_use,
                     allocator_in_use(alp),
                     i);
      }
   }

   core_destroy(&spl);
}

/*
 * Shared workload for the automatic-checkpoint tests: create with auto
 * checkpoints enabled, insert enough to drive several rotations, verify a
 * sample of keys survives the mid-run log rotations, then destroy.  The
 * teardown's allocator_assert_noleaks() must pass -- each sealed log's extents
 * are freed as its checkpoint completes, and the root is neither leaked nor
 * double-freed.  The caller sets up the task system (foreground or background).
 */
static void
run_auto_checkpoint_workload(void *datap, uint64 log_size_threshold)
{
   struct CTEST_IMPL_DATA_SNAME(splinter) *data =
      (struct CTEST_IMPL_DATA_SNAME(splinter) *)datap;

   allocator *alp                         = (allocator *)&data->al;
   data->system_cfg->splinter_cfg.use_log = TRUE;
   // Rotate the log / advance the durable root once the log reaches this size.
   data->system_cfg->splinter_cfg.checkpoint_log_size_bytes =
      log_size_threshold;

   core_handle     spl;
   platform_status rc = core_mkfs(&spl,
                                  &data->system_cfg->splinter_cfg,
                                  alp,
                                  (cache *)data->clock_cache,
                                  data->io,
                                  &data->tasks,
                                  test_generate_allocator_root_id(),
                                  data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   uint64 num_inserts = splinter_do_inserts(data, &spl, FALSE, NULL);
   ASSERT_NOT_EQUAL(0, num_inserts);

   // Drain so any in-flight checkpoint completes and the state settles.
   rc = task_perform_until_quiescent(spl.ts);
   ASSERT_TRUE(SUCCESS(rc));

   if (log_size_threshold != 0) {
      // The inserts must have driven at least one automatic checkpoint through
      // to completion.
      ASSERT_NOT_EQUAL(0, spl.checkpoint.completions);

      // At rest a checkpoint has either completed (IDLE) or been armed for a
      // rotation that idle never triggered (PENDING); both leave no sealed log.
      ASSERT_TRUE(spl.checkpoint.phase == CORE_CHECKPOINT_IDLE
                  || spl.checkpoint.phase == CORE_CHECKPOINT_PENDING);
      superblock_tree_record rec;
      superblock_get_tree_record(&spl.superblock, &rec);
      ASSERT_TRUE(SUPERBLOCK_NO_LOG(rec.sealed_log));
      // A checkpoint published an advanced, incorporated durable root mid-run:
      // at least one generation was folded in, so the first unincorporated
      // generation has advanced past 0.
      ASSERT_NOT_EQUAL(0, rec.first_unincorporated_generation);
   }

   // A sample of keys must still be found after the rotations.
   lookup_result qdata;
   lookup_result_init(
      &qdata, spl.cfg.data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   DECLARE_AUTO_KEY_BUFFER(keybuf, data->hid);
   const size_t key_size     = data->workload_cfg->key_size;
   uint64       verify_count = (num_inserts < 1000) ? num_inserts : 1000;
   for (uint64 i = 0; i < verify_count; i++) {
      test_key(&keybuf, TEST_RANDOM, i, 0, 0, key_size, 0);
      rc = core_lookup(&spl, key_buffer_key(&keybuf), &qdata);
      ASSERT_TRUE(SUCCESS(rc));
      verify_tuple(
         &spl,
         &data->gen,
         i,
         key_buffer_key(&keybuf),
         merge_accumulator_to_message(lookup_result_accumulator(&qdata)),
         TRUE);
   }
   lookup_result_deinit(&qdata);

   core_destroy(&spl);
}

/*
 * Automatic, incorporation-driven checkpoints (foreground): the log is rotated
 * and the durable root advanced during normal inserts, with no stop-the-world.
 */
CTEST2(splinter, test_auto_checkpoint)
{
   // One extent: small enough that the test workload drives several rotations.
   run_auto_checkpoint_workload(data, 2 * data->system_cfg->io_cfg.extent_size);
}

/*
 * The reason the policy is sized in log bytes rather than memtable generations.
 *
 * Repeatedly overwriting one key updates the memtable btree in place, so it
 * never accumulates extents, never becomes "full", and never rotates -- the
 * generation stays put for the whole workload.  Every write still appends to
 * the log, so the log grows without bound.  A generation-based trigger could
 * never fire here; the size-based one must.
 */
CTEST2(splinter, test_auto_checkpoint_on_overwrites)
{
   allocator *alp                         = (allocator *)&data->al;
   data->system_cfg->splinter_cfg.use_log = TRUE;
   data->system_cfg->splinter_cfg.checkpoint_log_size_bytes =
      2 * data->system_cfg->io_cfg.extent_size;
   // Also verify the reported checkpoint count against the internal one.
   data->system_cfg->splinter_cfg.use_stats = TRUE;

   core_handle     spl;
   platform_status rc = core_mkfs(&spl,
                                  &data->system_cfg->splinter_cfg,
                                  alp,
                                  (cache *)data->clock_cache,
                                  data->io,
                                  &data->tasks,
                                  test_generate_allocator_root_id(),
                                  data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   uint64 start_generation = memtable_generation(&spl.mt_ctxt);

   // Hammer a single key.  Enough writes to push the log well past one extent.
   DECLARE_AUTO_KEY_BUFFER(keybuf, data->hid);
   merge_accumulator msg;
   merge_accumulator_init(&msg, data->hid);
   const uint64 num_overwrites = 20000;
   for (uint64 i = 0; i < num_overwrites; i++) {
      test_key(&keybuf, TEST_RANDOM, 0, 0, 0, data->workload_cfg->key_size, 0);
      generate_test_message(&data->gen, i, &msg);
      rc = core_insert(&spl,
                       key_buffer_key(&keybuf),
                       merge_accumulator_to_message(&msg),
                       NULL);
      ASSERT_TRUE(SUCCESS(rc));
   }

   rc = task_perform_until_quiescent(spl.ts);
   ASSERT_TRUE(SUCCESS(rc));

   /*
    * The workload never filled a memtable on its own, so a generation-based
    * policy would have had nothing to trigger on: any generation advance here
    * came from a checkpoint forcing a rotation, not from the memtable filling.
    */
   ASSERT_NOT_EQUAL(0,
                    spl.checkpoint.completions,
                    "overwrite-only workload did not trigger a checkpoint; "
                    "generation went %lu -> %lu\n",
                    start_generation,
                    memtable_generation(&spl.mt_ctxt));

   /*
    * The reported statistic must agree with the machinery's own count: it is
    * summed across threads, so this catches both a missed increment and a
    * double count.
    */
   uint64 reported = 0;
   for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      reported += spl.stats[thr_i].checkpoints_completed;
   }
   ASSERT_EQUAL(spl.checkpoint.completions,
                reported,
                "checkpoints_completed stat (%lu) disagrees with the "
                "checkpoint state's count (%lu)\n",
                reported,
                spl.checkpoint.completions);

   // The surviving value must be the last one written.
   lookup_result qdata;
   lookup_result_init(
      &qdata, spl.cfg.data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   test_key(&keybuf, TEST_RANDOM, 0, 0, 0, data->workload_cfg->key_size, 0);
   rc = core_lookup(&spl, key_buffer_key(&keybuf), &qdata);
   ASSERT_TRUE(SUCCESS(rc));
   generate_test_message(&data->gen, num_overwrites - 1, &msg);
   ASSERT_EQUAL(0,
                message_lex_cmp(merge_accumulator_to_message(&msg),
                                merge_accumulator_to_message(
                                   lookup_result_accumulator(&qdata))));
   lookup_result_deinit(&qdata);
   merge_accumulator_deinit(&msg);

   core_destroy(&spl);
}

/*
 * The crash-recovery refcount rebuild has to reconstruct, from the tree alone,
 * exactly the map that normal operation maintained -- so this checks it against
 * the one authority on the subject: the map a clean unmount persisted.
 *
 * Comparing whole maps rather than spot-checking a few extents is the point. An
 * extent the walk misses leaks, and one it counts twice is freed while still in
 * use; both show up here as a mismatched refcount, and nothing else in the
 * suite would notice either.
 *
 * Coverage note: the default configuration builds a tree of one node, which
 * exercises the per-branch and per-filter accounting but never the descent. The
 * height a tree reaches is driven by how much data it holds and not by the
 * memtable size, so reaching a second level needs a large run -- at
 * --num-inserts 20000000 this walks 36 nodes, and so also covers descending,
 * the branches shared between nodes, and the guard against descending twice.
 * That is too slow to make the default, hence this note.
 */
CTEST2(splinter, test_recover_allocations_reproduces_persisted_map)
{
   allocator        *alp     = (allocator *)&data->al;
   allocator_config *acfg    = allocator_get_config(alp);
   allocator_root_id root_id = test_generate_allocator_root_id();
   core_handle       spl;
   platform_status   rc;

   rc = core_mkfs(&spl,
                  &data->system_cfg->splinter_cfg,
                  alp,
                  (cache *)data->clock_cache,
                  data->io,
                  &data->tasks,
                  root_id,
                  data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   /*
    * A real tree, not the empty root: the walk is only interesting once there
    * are interior nodes, several bundles per node, and branches that more than
    * one node references.
    */
   splinter_do_inserts(data, &spl, FALSE, NULL);

   rc = core_unmount(&spl, FALSE);
   ASSERT_TRUE(SUCCESS(rc));

   /* Read the durable record the way a mount would. */
   superblock_context sb;
   rc = superblock_context_init(&sb, data->io, acfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_TRUE(SUCCESS(superblock_mount(&sb, acfg)));
   superblock_tree_record rec;
   superblock_get_tree_record(&sb, &rec);
   // A clean unmount: the persisted map is trustworthy and the logs are gone.
   ASSERT_TRUE(superblock_allocation_state_valid(&sb));
   ASSERT_TRUE(SUPERBLOCK_NO_LOG(rec.live_log));
   ASSERT_TRUE(SUPERBLOCK_NO_LOG(rec.sealed_log));
   ASSERT_NOT_EQUAL(0, rec.root_addr);
   superblock_context_deinit(&sb);

   /*
    * Ground truth.  core_unmount() persisted this very map, so the in-memory
    * copy still standing here is byte-for-byte what a clean mount would load.
    */
   uint64 extent_size = acfg->io_cfg->extent_size;
   uint64 num_extents = allocator_get_capacity(alp) / extent_size;
   refcount *expected = TYPED_ARRAY_MALLOC(data->hid, expected, num_extents);
   ASSERT_NOT_NULL(expected);
   uint64 num_referenced = 0;
   for (uint64 i = 0; i < num_extents; i++) {
      expected[i] = allocator_get_refcount(alp, i * extent_size);
      if (expected[i] != AL_FREE) {
         num_referenced++;
      }
   }

   /*
    * Rebuild it -- twice.
    *
    * Once for the obvious reason, and a second time because recovery itself
    * rebuilds twice: once counting the logs so replay is not handed their
    * space, and again from the root alone afterwards, which is what releases
    * it.  A second rebuild that drifted from the first would mean recovery
    * silently leaking or double-freeing on every crash, so the round it runs in
    * has to make no difference at all.
    *
    * The first rebuild starts from a freshly attached allocator, matching a
    * real mount; the second runs against the map the first one left behind,
    * which is the case recovery actually depends on.  The cache keeps pointing
    * at the same allocator struct throughout, which is how the branch and
    * filter walks reach it.
    */
   rc_allocator_deinit(&data->al);
   rc = rc_allocator_mount(&data->al,
                           acfg,
                           data->io,
                           data->hid,
                           platform_get_module_id());
   ASSERT_TRUE(SUCCESS(rc));

   uint64 mismatches = 0;
   for (uint64 round = 0; round < 2; round++) {
      ASSERT_TRUE(SUCCESS(allocator_recovery_begin(alp)));
      rc = trunk_recover_allocations(
         data->system_cfg->splinter_cfg.trunk_node_cfg,
         (cache *)data->clock_cache,
         data->hid,
         rec.root_addr);
      ASSERT_TRUE(SUCCESS(rc));
      allocator_recovery_finish(alp);

      for (uint64 i = 0; i < num_extents; i++) {
         refcount actual = allocator_get_refcount(alp, i * extent_size);
         if (actual != expected[i]) {
            if (mismatches < 16) {
               platform_error_log("round %lu: extent %lu (addr %lu): persisted "
                                  "refcount %u, rebuilt %u\n",
                                  round,
                                  i,
                                  i * extent_size,
                                  expected[i],
                                  actual);
            }
            mismatches++;
         }
      }
      // The count the allocator reports has to be rebuilt too, not accumulated.
      ASSERT_EQUAL(num_referenced,
                   allocator_in_use(alp),
                   "round %lu: the allocator reports %lu extents in use, but "
                   "%lu are referenced\n",
                   round,
                   allocator_in_use(alp),
                   num_referenced);
   }
   platform_free(data->hid, expected);

   ASSERT_EQUAL(0,
                mismatches,
                "the rebuilt map differs from the persisted one in %lu of %lu "
                "extents\n",
                mismatches,
                num_extents);
   // Guard against the comparison passing because there was nothing to compare.
   ASSERT_TRUE(num_referenced > 1,
               "only %lu extents were referenced; the tree is too small for "
               "this test to mean anything\n",
               num_referenced);

   /*
    * Leave nothing behind for the fixture's leak check: the map still holds
    * every reference the tree needs, so erase the tree.  Mounting reloads the
    * persisted map over the rebuilt one, which the comparison above has just
    * shown to be the same map.
    */
   core_handle cleanup;
   rc = core_mount(&cleanup,
                   &data->system_cfg->splinter_cfg,
                   alp,
                   (cache *)data->clock_cache,
                   data->io,
                   &data->tasks,
                   root_id,
                   data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   core_destroy(&cleanup);
}

/*
 * The second checkpoint slot is a torn-write fallback, not permission for a
 * normal mount to silently roll back past a newer, valid active record.  A
 * successful mount publishes such an active record, so this walks a record pair
 * through the states that produces -- clean, then active, then clean again --
 * and requires it to stay mountable throughout.
 *
 * It used to also require that a second mount over the active record be
 * *rejected*, which was true only while crash recovery was unimplemented: an
 * invalid allocation state now sends a mount into recovery rather than into an
 * error, which is the whole point of it.  Two consequences worth knowing:
 *
 *   - Recovering the newer active record, rather than refusing it, is what
 *     actually honours the no-rollback rule.  Exercising that needs a genuine
 *     crash, because recovery rebuilds the refcount map from scratch and so
 *     invalidates the accounting of any handle still holding the instance --
 *     which no test can arrange from inside this fixture.  It belongs with the
 *     process-level crash tests.
 *   - Nothing now stops a second mount of a *live* instance.  The clean-only
 *     rule used to prevent that as a side effect; distinguishing "crashed" from
 *     "mounted by someone else" needs a marker of its own, since the allocation
 *     state means only the former.
 */
CTEST2(splinter, test_mount_active_checkpoint_stays_mountable)
{
   allocator        *alp     = (allocator *)&data->al;
   allocator_root_id root_id = test_generate_allocator_root_id();
   core_handle       created, mounted, cleanup;
   platform_status   rc;

   rc = core_mkfs(&created,
                  &data->system_cfg->splinter_cfg,
                  alp,
                  (cache *)data->clock_cache,
                  data->io,
                  &data->tasks,
                  root_id,
                  data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   /* Give the clean record a real COW root, not just the empty-tree root. */
   DECLARE_AUTO_KEY_BUFFER(keybuf, data->hid);
   merge_accumulator msg;
   merge_accumulator_init(&msg, data->hid);
   test_key(&keybuf, TEST_RANDOM, 1, 0, 0, data->workload_cfg->key_size, 0);
   generate_test_message(&data->gen, 1, &msg);
   rc = core_insert(&created,
                    key_buffer_key(&keybuf),
                    merge_accumulator_to_message(&msg),
                    NULL);
   merge_accumulator_deinit(&msg);
   ASSERT_TRUE(SUCCESS(rc));

   rc = core_unmount(&created, FALSE);
   ASSERT_TRUE(SUCCESS(rc));

   /* This mount advances the A/B sequence with an unmounted=FALSE record. */
   rc = core_mount(&mounted,
                   &data->system_cfg->splinter_cfg,
                   alp,
                   (cache *)data->clock_cache,
                   data->io,
                   &data->tasks,
                   root_id,
                   data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   /* Finish cleanly, then prove the same record pair is mountable again. */
   rc = core_unmount(&mounted, FALSE);
   ASSERT_TRUE(SUCCESS(rc));

   rc = core_mount(&cleanup,
                   &data->system_cfg->splinter_cfg,
                   alp,
                   (cache *)data->clock_cache,
                   data->io,
                   &data->tasks,
                   root_id,
                   data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   core_destroy(&cleanup);
}

static void
trunk_shadow_init(trunk_shadow    *shadow,
                  data_config     *data_cfg,
                  platform_heap_id hid)
{
   shadow->data_cfg = data_cfg;
   shadow->sorted   = TRUE;
   writable_buffer_init(&shadow->entries, hid);
   writable_buffer_init(&shadow->data, hid);
}

static void
trunk_shadow_deinit(trunk_shadow *shadow)
{
   writable_buffer_deinit(&shadow->entries);
   writable_buffer_deinit(&shadow->data);
}

static void
trunk_shadow_reinit(trunk_shadow *shadow)
{
   shadow->sorted = TRUE;
   writable_buffer_set_to_null(&shadow->entries);
   writable_buffer_set_to_null(&shadow->data);
}

/*
 * Copy the newly inserted key/value row to a shadow buffer. This set of
 * rows will be used later during lookup-validation using range searches.
 */
static void
trunk_shadow_append(trunk_shadow *shadow, key tuple_key, message value)
{
   platform_assert(message_class(value) == MESSAGE_TYPE_INSERT);
   uint64          key_offset = writable_buffer_length(&shadow->data);
   platform_status rc         = writable_buffer_append(
      &shadow->data, key_length(tuple_key), key_data(tuple_key));
   platform_assert_status_ok(rc);
   rc = writable_buffer_append(
      &shadow->data, message_length(value), message_data(value));
   platform_assert_status_ok(rc);

   shadow_entry new_entry = {.key_offset   = key_offset,
                             .key_length   = key_length(tuple_key),
                             .value_length = message_length(value)};
   rc = writable_buffer_append(&shadow->entries, sizeof(new_entry), &new_entry);
   platform_assert_status_ok(rc);
   shadow->sorted = FALSE;
}

static key
shadow_entry_key(const shadow_entry *entry, char *data)
{
   return key_create(FALSE, entry->key_length, data + entry->key_offset);
}

static message
shadow_entry_value(const shadow_entry *entry, char *data)
{
   return message_create(
      MESSAGE_TYPE_INSERT,
      NULL,
      slice_create(entry->value_length,
                   data + entry->key_offset + entry->key_length));
}

static int
compare_shadow_entries(const void *a, const void *b, void *arg)
{
   trunk_shadow *shadow = (trunk_shadow *)arg;
   char         *data   = writable_buffer_data(&shadow->data);
   key           akey   = shadow_entry_key(a, data);
   key           bkey   = shadow_entry_key(b, data);
   return data_key_compare(shadow->data_cfg, akey, bkey);
}

static uint64
trunk_shadow_length(trunk_shadow *shadow)
{
   return writable_buffer_length(&shadow->entries) / sizeof(shadow_entry);
}

static void
trunk_shadow_sort(trunk_shadow *shadow)
{
   shadow_entry *entries  = writable_buffer_data(&shadow->entries);
   uint64        nentries = trunk_shadow_length(shadow);
   shadow_entry  temp;

   platform_sort_slow(entries,
                      nentries,
                      sizeof(*entries),
                      compare_shadow_entries,
                      shadow,
                      &temp);
   shadow->sorted = TRUE;
}

static void
trunk_shadow_get(trunk_shadow *shadow, uint64 i, key *tuple_key, message *value)
{

   if (!shadow->sorted) {
      trunk_shadow_sort(shadow);
   }

   shadow_entry     *entries  = writable_buffer_data(&shadow->entries);
   debug_only uint64 nentries = trunk_shadow_length(shadow);
   debug_assert(i < nentries);
   shadow_entry *entry = &entries[i];

   char *data = writable_buffer_data(&shadow->data);
   *tuple_key = shadow_entry_key(entry, data);
   *value     = shadow_entry_value(entry, data);
}

static uint64
test_splinter_bsearch(trunk_shadow *shadow, key needle)
{
   uint64 lo = 0;
   uint64 hi = trunk_shadow_length(shadow);
   while (lo < hi) {
      // invariant: forall i | i < lo  :: s[i] < key
      // invariant: forall i | hi <= i :: key <= s[i]
      key     ckey;
      message cvalue;
      uint64  mid = (lo + hi) / 2;
      trunk_shadow_get(shadow, mid, &ckey, &cvalue);
      int cmp = data_key_compare(shadow->data_cfg, needle, ckey);
      if (cmp <= 0) {
         // key <= s[mid]
         hi = mid;
      } else {
         // s[mid] < key
         lo = mid + 1;
      }
   }

   return lo;
}

/*
 * **************************************************************************
 * Test case to run a bunch of inserts into Splinter, and then perform
 * different types of lookup-verification. As all lookups need an inserts
 * step, this test case is really a set of multiple sub-test-cases for
 * inserts, synchronous and async lookups, and lookups-by-range rolled into
 * one.
 * **************************************************************************
 */
CTEST2(splinter, test_lookups)
{
   allocator *alp = (allocator *)&data->al;

   core_handle     spl;
   platform_status rc_init = core_mkfs(&spl,
                                       &data->system_cfg->splinter_cfg,
                                       alp,
                                       (cache *)data->clock_cache,
                                       data->io,
                                       &data->tasks,
                                       test_generate_allocator_root_id(),
                                       data->hid);
   ASSERT_TRUE(SUCCESS(rc_init));

   trunk_shadow shadow;
   trunk_shadow_init(&shadow, data->system_cfg->data_cfg, data->hid);

   // FALSE : No need to do verification-after-inserts, as that functionality
   // has been tested earlier in test_inserts() case.
   uint64 num_inserts = splinter_do_inserts(data, &spl, FALSE, &shadow);
   ASSERT_NOT_EQUAL(0,
                    num_inserts,
                    "Expected to have inserted non-zero rows, num_inserts=%lu.",
                    num_inserts);

   lookup_result qdata;
   lookup_result_init(
      &qdata, spl.cfg.data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   DECLARE_AUTO_KEY_BUFFER(keybuf, data->hid);
   const size_t key_size = data->workload_cfg->key_size;

   platform_status rc;

   // **************************************************************************
   // Test sub-case 1: Validate using synchronous trunk_lookup().
   //   Verify that all the keys inserted are found via lookup.
   // **************************************************************************
   uint64 start_time = platform_get_timestamp();

   CTEST_LOG_INFO("\n");
   for (uint64 insert_num = 0; insert_num < num_inserts; insert_num++) {

      // Show progress message in %age-completed to stdout
      SHOW_PCT_PROGRESS(
         insert_num, num_inserts, "Verify positive lookups %3lu%% complete");

      test_key(&keybuf, TEST_RANDOM, insert_num, 0, 0, key_size, 0);
      rc = core_lookup(&spl, key_buffer_key(&keybuf), &qdata);
      ASSERT_TRUE(SUCCESS(rc),
                  "trunk_lookup() FAILURE, insert_num=%lu: %s\n",
                  insert_num,
                  platform_status_to_string(rc));

      verify_tuple(
         &spl,
         &data->gen,
         insert_num,
         key_buffer_key(&keybuf),
         merge_accumulator_to_message(lookup_result_accumulator(&qdata)),
         TRUE);
   }

   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
   CTEST_LOG_INFO(
      " ... splinter positive lookup time %lu s, per tuple %lu ns\n",
      NSEC_TO_SEC(elapsed_ns),
      (elapsed_ns / num_inserts));

   // **************************************************************************
   // Test sub-case 2: Validate using synchronous trunk_lookup() that we
   //   do not find any keys outside the range of keys inserted.
   // **************************************************************************

   start_time = platform_get_timestamp();

   for (uint64 insert_num = num_inserts; insert_num < 2 * num_inserts;
        insert_num++)
   {
      // Show progress message in %age-completed to stdout
      SHOW_PCT_PROGRESS((insert_num - num_inserts),
                        num_inserts,
                        "Verify negative lookups %3lu%% complete");

      test_key(&keybuf, TEST_RANDOM, insert_num, 0, 0, key_size, 0);

      rc = core_lookup(&spl, key_buffer_key(&keybuf), &qdata);
      ASSERT_TRUE(SUCCESS(rc),
                  "trunk_lookup() FAILURE, insert_num=%lu: %s\n",
                  insert_num,
                  platform_status_to_string(rc));

      verify_tuple(
         &spl,
         &data->gen,
         insert_num,
         key_buffer_key(&keybuf),
         merge_accumulator_to_message(lookup_result_accumulator(&qdata)),
         FALSE);
   }

   elapsed_ns = platform_timestamp_elapsed(start_time);
   CTEST_LOG_INFO(
      " ... splinter negative lookup time %lu s, per tuple %lu ns\n",
      NSEC_TO_SEC(elapsed_ns),
      (elapsed_ns / num_inserts));

   lookup_result_deinit(&qdata);

   // **************************************************************************
   // Test sub-case 3: Validate using binary searches across ranges for the
   //   keys inside the range of keys inserted.
   // **************************************************************************

   int niters = 3;
   CTEST_LOG_INFO("Perform test_lookup_by_range() for %d iterations ...\n",
                  niters);
   // Iterate thru small set of num_ranges for additional coverage.
   trunk_shadow_sort(&shadow);
   for (int ictr = 1; ictr <= 3; ictr++) {

      uint64 num_ranges = (num_inserts / 128) * ictr;

      // Range search uses the shadow-copy of the rows previously inserted while
      // doing a binary-search.
      rc = test_lookup_by_range(
         (void *)data, &spl, num_inserts, &shadow, num_ranges);
      ASSERT_TRUE(SUCCESS(rc),
                  "test_lookup_by_range() FAILURE, num_ranges=%d: %s\n",
                  num_ranges,
                  platform_status_to_string(rc));
   }

   /*
   ** **********************************************
   ** **** Start of Async lookup sub-test-cases ****
   ** **********************************************
   */
   // Setup Async-context sub-system for async lookups.
   test_async_lookup *async_lookup;
   async_ctxt_init(data->hid, data->max_async_inflight, &async_lookup);

   test_async_ctxt *ctxt = NULL;

   // **************************************************************************
   // Test sub-case 4: Validate using asynchronous trunk_lookup().
   //   Verify that all the keys inserted are found via lookup.
   // **************************************************************************

   // Declare an expected data tuple that will be found.
   verify_tuple_arg vtarg_true = {.expected_found = TRUE};

   start_time = platform_get_timestamp();
   for (uint64 insert_num = 0; insert_num < num_inserts; insert_num++) {

      // Show progress message in %age-completed to stdout
      SHOW_PCT_PROGRESS(insert_num,
                        num_inserts,
                        "Verify async positive lookups %3lu%% complete");

      ctxt = test_async_ctxt_get(&spl, async_lookup, &vtarg_true);

      test_key(&ctxt->key, TEST_RANDOM, insert_num, 0, 0, key_size, 0);
      ctxt->lookup_num = insert_num;
      async_ctxt_submit(
         &spl, async_lookup, ctxt, NULL, verify_tuple_callback, &vtarg_true);
   }
   test_wait_for_inflight(&spl, async_lookup, &vtarg_true);

   elapsed_ns = platform_timestamp_elapsed(start_time);
   CTEST_LOG_INFO(
      " ... splinter positive async lookup time %lu s, per tuple %lu ns\n",
      NSEC_TO_SEC(elapsed_ns),
      (elapsed_ns / num_inserts));

   // **************************************************************************
   // Test sub-case 5: Validate using asynchronous trunk_lookup() that we
   //   do not find any keys outside the range of keys inserted.
   // **************************************************************************

   // Declare a tuple that data will not be found.
   verify_tuple_arg vtarg_false = {.expected_found = FALSE};

   start_time = platform_get_timestamp();
   for (uint64 insert_num = num_inserts; insert_num < 2 * num_inserts;
        insert_num++)
   {
      // Show progress message in %age-completed to stdout
      SHOW_PCT_PROGRESS((insert_num - num_inserts),
                        num_inserts,
                        "Verify async negative lookups %3lu%% complete");

      ctxt = test_async_ctxt_get(&spl, async_lookup, &vtarg_false);
      test_key(&ctxt->key, TEST_RANDOM, insert_num, 0, 0, key_size, 0);
      ctxt->lookup_num = insert_num;
      async_ctxt_submit(
         &spl, async_lookup, ctxt, NULL, verify_tuple_callback, &vtarg_false);
   }
   test_wait_for_inflight(&spl, async_lookup, &vtarg_false);

   elapsed_ns = platform_timestamp_elapsed(start_time);
   CTEST_LOG_INFO(
      " ... splinter negative async lookup time %lu s, per tuple %lu ns\n",
      NSEC_TO_SEC(elapsed_ns),
      (elapsed_ns / num_inserts));

   // Cleanup memory allocated in this test case
   if (async_lookup) {
      async_ctxt_deinit(data->hid, async_lookup);
   }

   core_destroy(&spl);
   trunk_shadow_deinit(&shadow);
}

/*
 * -----------------------------------------------------------------------------
 * Simple test cases to exercise print / diagnostic functions provided
 * by various sub-systems.  Test is now readily useful as an educational tool.
 *
 * NOTE: This test case is mentioned in external docs. Be careful what
 *  changes you bring in here.
 * -----------------------------------------------------------------------------
 */
CTEST2(splinter, test_splinter_print_diags)
{
   set_log_streams_for_tests(MSG_LEVEL_DEBUG);

   allocator *alp = (allocator *)&data->al;

   core_handle     spl;
   platform_status rc = core_mkfs(&spl,
                                  &data->system_cfg->splinter_cfg,
                                  alp,
                                  (cache *)data->clock_cache,
                                  data->io,
                                  &data->tasks,
                                  test_generate_allocator_root_id(),
                                  data->hid);
   ASSERT_TRUE(SUCCESS(rc));

   uint64 num_inserts = splinter_do_inserts(data, &spl, FALSE, NULL);
   ASSERT_NOT_EQUAL(0,
                    num_inserts,
                    "Expected to have inserted non-zero rows, num_inserts=%lu",
                    num_inserts);

   CTEST_LOG_INFO("**** Splinter Diagnostics ****\n"
                  "Generated by %s:%d:%s ****\n",
                  __FILE__,
                  __LINE__,
                  __func__);

   core_print_super_block(Platform_default_log_handle, &spl);

   core_print_space_use(Platform_default_log_handle, &spl);

   CTEST_LOG_INFO("\n** Allocator stats **\n");
   allocator_print_stats(alp);
   allocator_print_allocated(alp);

   set_log_streams_for_tests(MSG_LEVEL_INFO);
   core_destroy(&spl);
}

/*
 * ----------------------------------
 * Helper and minions live here.
 * ----------------------------------
 */
/*
 * Work-horse function to drive inserts into Splinter. # of inserts is
 * determined by config parameters, and computed below.
 *
 * Parmeters:
 *  datap       - Ptr to global data struct { }
 *  spl         - Ptr to splinter handle, established by caller.
 *  verify      - Boolean; periodically verify splinter tree consistency
 *  shadow      - Ptr to shadow buffer, which will be re-initialized
 *                and filled-out in this function, if supplied.
 *
 * Returns the # of rows inserted.
 */
static uint64
splinter_do_inserts(void         *datap,
                    core_handle  *spl,
                    bool32        verify,
                    trunk_shadow *shadow) // Out
{
   // Cast void * datap to ptr-to-CTEST_DATA() struct in use.
   struct CTEST_IMPL_DATA_SNAME(splinter) *data =
      (struct CTEST_IMPL_DATA_SNAME(splinter) *)datap;

   // First see if test was invoked with --num-inserts execution parameter.
   // (Override the default, which is some big value, like 12988800, with this
   // hook for faster test execution.)
   int num_inserts = data->test_exec_cfg.num_inserts;

   // If not, derive total # of rows to be inserted
   if (!num_inserts) {
      core_config *system_cfg = &data->system_cfg->splinter_cfg;
      num_inserts = system_cfg[0].trunk_node_cfg->incorporation_size_kv_bytes
                    * system_cfg[0].trunk_node_cfg->target_fanout / 2
                    / generator_average_message_size(&data->gen);
   }

   CTEST_LOG_INFO(
      "system_cfg max_kv_bytes_per_node=%lu"
      ", fanout=%lu"
      ", max_extents_per_memtable=%lu, num_inserts=%d. ",
      data->system_cfg[0].trunk_node_cfg.incorporation_size_kv_bytes,
      data->system_cfg[0].trunk_node_cfg.target_fanout,
      data->system_cfg[0].splinter_cfg.mt_cfg.max_extents_per_memtable,
      num_inserts);

   uint64 start_time = platform_get_timestamp();
   uint64 insert_num;
   DECLARE_AUTO_KEY_BUFFER(keybuf, spl->heap_id);
   const size_t key_size = data->workload_cfg->key_size;

   // Allocate a large array for copying over shadow copies of rows
   // inserted, if user has asked to return such an array.
   if (shadow) {
      trunk_shadow_reinit(shadow);
   }

   platform_status rc;

   CTEST_LOG_INFO("trunk_insert() test with %d inserts %s ...\n",
                  num_inserts,
                  (verify ? "and verify" : ""));
   merge_accumulator msg;
   merge_accumulator_init(&msg, spl->heap_id);
   for (insert_num = 0; insert_num < num_inserts; insert_num++) {

      // Show progress message in %age-completed to stdout
      SHOW_PCT_PROGRESS(insert_num, num_inserts, "inserting %3lu%% complete");

      test_key(&keybuf, TEST_RANDOM, insert_num, 0, 0, key_size, 0);
      generate_test_message(&data->gen, insert_num, &msg);
      rc = core_insert(spl,
                       key_buffer_key(&keybuf),
                       merge_accumulator_to_message(&msg),
                       NULL);
      ASSERT_TRUE(SUCCESS(rc),
                  "trunk_insert() FAILURE: %s\n",
                  platform_status_to_string(rc));

      // Caller is interested in using a copy of the rows inserted for
      // verification; e.g. by range-search lookups.
      if (shadow) {
         trunk_shadow_append(shadow,
                             key_buffer_key(&keybuf),
                             merge_accumulator_to_message(&msg));
      }
   }

   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
   uint64 elapsed_s  = NSEC_TO_SEC(elapsed_ns);

   // For small # of inserts, elapsed sec will be 0. Deal with it.
   CTEST_LOG_INFO(
      "... average tuple_size=%lu, splinter insert time %lu s, per "
      "tuple %lu ns, %s%lu rows/sec. ",
      key_size + generator_average_message_size(&data->gen),
      elapsed_s,
      (elapsed_ns / num_inserts),
      (elapsed_s ? "" : "(n/a)"),
      (elapsed_s ? (num_inserts / NSEC_TO_SEC(elapsed_ns)) : num_inserts));

   cache_assert_free((cache *)data->clock_cache);

   // Cleanup memory allocated in this test case
   merge_accumulator_deinit(&msg);
   return num_inserts;
}

typedef struct shadow_check_tuple_arg {
   core_handle  *spl;
   trunk_shadow *shadow;
   uint64        pos;
   uint64        errors;
} shadow_check_tuple_arg;

static void
shadow_check_tuple_func(key returned_key, message value, void *varg)
{
   shadow_check_tuple_arg *arg = varg;

   key     shadow_key;
   message shadow_value;
   trunk_shadow_get(arg->shadow, arg->pos, &shadow_key, &shadow_value);
   if (data_key_compare(arg->spl->cfg.data_cfg, returned_key, shadow_key)
       || message_lex_cmp(value, shadow_value))
   {
      char expected_key[128];
      char actual_key[128];
      char expected_value[128];
      char actual_value[128];

      core_key_to_string(arg->spl, shadow_key, expected_key);
      core_key_to_string(arg->spl, returned_key, actual_key);

      core_message_to_string(arg->spl, shadow_value, expected_value);
      core_message_to_string(arg->spl, value, actual_value);

      CTEST_LOG_INFO("\nexpected: '%s' | '%s'\n", expected_key, expected_value);
      CTEST_LOG_INFO("actual  : '%s' | '%s'\n", actual_key, actual_value);
      arg->errors++;
   }

   arg->pos++;
}

/*
 * -----------------------------------------------------------------------------
 * Driver routine to verify Splinter lookup by range searches.
 *
 * Parameters:
 *  datap       - Ptr to global data struct
 *  spl         - Ptr to trunk_handle
 *  num_inserts - # of inserts that we want ranges to span
 *  shadow      - Shadow buffer allocated by caller, while inserting rows.
 *  num_ranges  - # of range searches to do in this run
 * -----------------------------------------------------------------------------
 */
static platform_status
test_lookup_by_range(void         *datap,
                     core_handle  *spl,
                     uint64        num_inserts,
                     trunk_shadow *shadow,
                     uint64        num_ranges)
{
   struct CTEST_IMPL_DATA_SNAME(splinter) *data =
      (struct CTEST_IMPL_DATA_SNAME(splinter) *)datap;
   const size_t key_size = data->workload_cfg->key_size;

   uint64 start_time = platform_get_timestamp();

   platform_status rc;

   DECLARE_AUTO_KEY_BUFFER(start_key_buf, spl->heap_id);

   for (uint64 range_num = 0; range_num != num_ranges; range_num++) {

      // Show progress message in %age-completed to stdout
      SHOW_PCT_PROGRESS(
         range_num, num_ranges, "Verify range    lookups %3lu%% complete");

      test_key(&start_key_buf,
               TEST_RANDOM,
               num_inserts + range_num,
               0,
               0,
               key_size,
               0);
      key    start_key    = key_buffer_key(&start_key_buf);
      uint64 range_tuples = test_range(range_num, 1, 100);

      uint64 start_idx = test_splinter_bsearch(shadow, start_key);
      uint64 expected_returned_tuples = num_inserts - start_idx > range_tuples
                                           ? range_tuples
                                           : num_inserts - start_idx;

      shadow_check_tuple_arg arg = {
         .spl = spl, .shadow = shadow, .pos = start_idx, .errors = 0};

      rc = core_apply_to_range(
         spl, start_key, range_tuples, shadow_check_tuple_func, &arg);

      ASSERT_TRUE(SUCCESS(rc));
      ASSERT_TRUE(
         arg.errors == 0, "trunk_range() found %lu mismatches", arg.errors);
      ASSERT_TRUE(arg.pos == start_idx + expected_returned_tuples,
                  "trunk_range() saw wrong number of tuples: "
                  " expected_returned_tuples=%lu"
                  ", returned_tuples=%lu"
                  ", start_key='%.*s'"
                  ", errors=%lu",
                  expected_returned_tuples,
                  arg.pos - start_idx,
                  key_size,
                  start_key,
                  arg.errors);
   }

   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
   CTEST_LOG_INFO(" ... splinter range time %lu s, per operation %lu ns"
                  ", %lu ranges\n",
                  NSEC_TO_SEC(elapsed_ns),
                  (elapsed_ns / num_ranges),
                  num_ranges);

   return rc;
}
