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
#include "shard_log.h"
#include "blob_build.h"
#include "clockcache.h"
#include "allocator.h"
#include "mini_allocator.h"
#include "rc_allocator.h"
#include "task.h"
#include "platform_threads.h"
#include "platform_sleep.h"
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

/*
 * Test-only durable-barrier fault injection for checkpoint retries.
 *
 * The whole fixture shares one io_handle, so temporarily replacing its ops
 * table reaches log, cache, and superblock barriers without changing any of
 * their production interfaces.  Tests install only one injector at a time and
 * keep it installed until the task system is quiescent.
 */
typedef struct checkpoint_barrier_fault {
   io_handle    *io;
   const io_ops *saved_ops;
   io_ops        fault_ops;
   uint64        skip_barriers;
   uint64        fail_barriers;
   uint64        barriers;
   uint64        block_barrier;
   bool32        enabled;
   bool32        block_enabled;
   bool32        block_entered;
   bool32        block_released;
   bool32        installed;
} checkpoint_barrier_fault;

/*
 * Long enough to cover both transition-triggered and explicit advance attempts,
 * finite so an implementation which mistakenly swallows the errors eventually
 * escapes and fails the test instead of hanging the test suite forever.
 */
#define CHECKPOINT_BARRIER_FAULT_COUNT 32

static checkpoint_barrier_fault *active_checkpoint_barrier_fault;

static platform_status
checkpoint_fault_durable_barrier(io_handle *io)
{
   checkpoint_barrier_fault *fault =
      __atomic_load_n(&active_checkpoint_barrier_fault, __ATOMIC_ACQUIRE);
   platform_assert(fault != NULL && fault->installed && fault->io == io);

   if (__atomic_load_n(&fault->enabled, __ATOMIC_ACQUIRE)) {
      uint64 barrier =
         __atomic_fetch_add(&fault->barriers, 1, __ATOMIC_RELAXED);
      if (__atomic_load_n(&fault->block_enabled, __ATOMIC_ACQUIRE)
          && barrier == fault->block_barrier)
      {
         __atomic_store_n(&fault->block_entered, TRUE, __ATOMIC_RELEASE);
         while (!__atomic_load_n(&fault->block_released, __ATOMIC_ACQUIRE)) {
            platform_sleep_ns(USEC_TO_NSEC(50));
         }
      }
      if (barrier >= fault->skip_barriers
          && barrier - fault->skip_barriers < fault->fail_barriers)
      {
         return STATUS_IO_ERROR;
      }
   }

   return fault->saved_ops->durable_barrier(io);
}

static void
checkpoint_barrier_fault_install(checkpoint_barrier_fault *fault, io_handle *io)
{
   platform_assert(active_checkpoint_barrier_fault == NULL);
   platform_assert(!fault->installed);

   fault->io                        = io;
   fault->saved_ops                 = io->ops;
   fault->fault_ops                 = *io->ops;
   fault->fault_ops.durable_barrier = checkpoint_fault_durable_barrier;
   fault->installed                 = TRUE;

   __atomic_store_n(&active_checkpoint_barrier_fault, fault, __ATOMIC_RELEASE);
   io->ops = &fault->fault_ops;
}

static void
checkpoint_barrier_fault_arm(checkpoint_barrier_fault *fault,
                             uint64                    skip_barriers)
{
   platform_assert(fault->installed);
   __atomic_store_n(&fault->enabled, FALSE, __ATOMIC_RELEASE);
   __atomic_store_n(&fault->block_released, TRUE, __ATOMIC_RELEASE);
   __atomic_store_n(&fault->block_enabled, FALSE, __ATOMIC_RELEASE);
   fault->skip_barriers = skip_barriers;
   fault->fail_barriers = CHECKPOINT_BARRIER_FAULT_COUNT;
   __atomic_store_n(&fault->barriers, 0, __ATOMIC_RELAXED);
   __atomic_store_n(&fault->enabled, TRUE, __ATOMIC_RELEASE);
}

/* Block one selected successful device barrier until the test releases it. */
static void
checkpoint_barrier_fault_block(checkpoint_barrier_fault *fault,
                               uint64                    block_barrier)
{
   platform_assert(fault->installed);
   __atomic_store_n(&fault->enabled, FALSE, __ATOMIC_RELEASE);
   __atomic_store_n(&fault->block_released, TRUE, __ATOMIC_RELEASE);
   __atomic_store_n(&fault->block_enabled, FALSE, __ATOMIC_RELEASE);

   fault->skip_barriers = 0;
   fault->fail_barriers = 0;
   fault->block_barrier = block_barrier;
   __atomic_store_n(&fault->barriers, 0, __ATOMIC_RELAXED);
   __atomic_store_n(&fault->block_entered, FALSE, __ATOMIC_RELAXED);
   __atomic_store_n(&fault->block_released, FALSE, __ATOMIC_RELAXED);
   __atomic_store_n(&fault->block_enabled, TRUE, __ATOMIC_RELEASE);
   __atomic_store_n(&fault->enabled, TRUE, __ATOMIC_RELEASE);
}

static void
checkpoint_barrier_fault_release(checkpoint_barrier_fault *fault)
{
   __atomic_store_n(&fault->block_released, TRUE, __ATOMIC_RELEASE);
}

static void
checkpoint_barrier_fault_disable(checkpoint_barrier_fault *fault)
{
   checkpoint_barrier_fault_release(fault);
   __atomic_store_n(&fault->block_enabled, FALSE, __ATOMIC_RELEASE);
   __atomic_store_n(&fault->enabled, FALSE, __ATOMIC_RELEASE);
}

static void
checkpoint_barrier_fault_uninstall(checkpoint_barrier_fault *fault)
{
   if (!fault->installed) {
      return;
   }

   checkpoint_barrier_fault_disable(fault);
   io_wait_all(fault->io);
   fault->io->ops = fault->saved_ops;
   __atomic_store_n(&active_checkpoint_barrier_fault, NULL, __ATOMIC_RELEASE);
   fault->installed = FALSE;
   fault->io        = NULL;
   fault->saved_ops = NULL;
}

/*
 * Pause the first core insert after its leaf-lock callback has reserved a log
 * group and made the memtable mutation visible, but before the reserved append
 * consumes that reservation.  Later writes pass through so a test can prove
 * that the blocked reservation does not exclude unrelated writers.
 */
typedef struct core_log_write_block {
   log_handle    *log;
   const log_ops *saved_ops;
   log_ops        blocked_ops;
   uint64         calls;
   bool32         entered;
   bool32         released;
   bool32         installed;
} core_log_write_block;

static core_log_write_block *active_core_log_write_block;

static int
core_log_write_reserved_blocked(log_write_token *token,
                                key              tuple_key,
                                message          data,
                                uint64           memtable_generation,
                                uint64           leaf_generation)
{
   core_log_write_block *block =
      __atomic_load_n(&active_core_log_write_block, __ATOMIC_ACQUIRE);
   platform_assert(block != NULL && block->installed
                   && block->log == token->log);

   uint64 call = __atomic_fetch_add(&block->calls, 1, __ATOMIC_RELAXED);
   if (call == 0) {
      __atomic_store_n(&block->entered, TRUE, __ATOMIC_RELEASE);
      while (!__atomic_load_n(&block->released, __ATOMIC_ACQUIRE)) {
         platform_sleep_ns(USEC_TO_NSEC(50));
      }
   }
   return block->saved_ops->write_reserved(
      token, tuple_key, data, memtable_generation, leaf_generation);
}

static void
core_log_write_block_install(core_log_write_block *block, log_handle *log)
{
   platform_assert(active_core_log_write_block == NULL);
   platform_assert(!block->installed);

   block->log                        = log;
   block->saved_ops                  = log->ops;
   block->blocked_ops                = *log->ops;
   block->blocked_ops.write_reserved = core_log_write_reserved_blocked;
   block->calls                      = 0;
   block->entered                    = FALSE;
   block->released                   = FALSE;
   block->installed                  = TRUE;
   __atomic_store_n(&active_core_log_write_block, block, __ATOMIC_RELEASE);
   log->ops = &block->blocked_ops;
}

static void
core_log_write_block_release(core_log_write_block *block)
{
   __atomic_store_n(&block->released, TRUE, __ATOMIC_RELEASE);
}

static void
core_log_write_block_uninstall(core_log_write_block *block)
{
   if (!block->installed) {
      return;
   }
   core_log_write_block_release(block);
   block->log->ops = block->saved_ops;
   __atomic_store_n(&active_core_log_write_block, NULL, __ATOMIC_RELEASE);
   block->installed = FALSE;
   block->log       = NULL;
   block->saved_ops = NULL;
}

#define CORE_DURABLE_BARRIER_TEST_TIMEOUT_NS SEC_TO_NSEC(10)

static bool32
core_durable_barrier_test_wait(const bool32 *flag)
{
   timestamp start = platform_get_timestamp();
   while (!__atomic_load_n(flag, __ATOMIC_ACQUIRE)
          && platform_timestamp_elapsed(start)
                < CORE_DURABLE_BARRIER_TEST_TIMEOUT_NS)
   {
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
   return __atomic_load_n(flag, __ATOMIC_ACQUIRE);
}

static bool32
core_durable_barrier_test_wait_for_ticket_refs(shard_log *log, uint64 target)
{
   timestamp start = platform_get_timestamp();
   while (platform_timestamp_elapsed(start)
          < CORE_DURABLE_BARRIER_TEST_TIMEOUT_NS)
   {
      uint64 refs = __atomic_load_n(&log->ticket_refs, __ATOMIC_RELAXED);
      if (refs >= target) {
         return TRUE;
      }
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
   return FALSE;
}

static bool32
core_durable_barrier_test_wait_for_io_barriers(checkpoint_barrier_fault *fault,
                                               uint64                    target)
{
   timestamp start = platform_get_timestamp();
   while (__atomic_load_n(&fault->barriers, __ATOMIC_ACQUIRE) < target
          && platform_timestamp_elapsed(start)
                < CORE_DURABLE_BARRIER_TEST_TIMEOUT_NS)
   {
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
   return __atomic_load_n(&fault->barriers, __ATOMIC_ACQUIRE) >= target;
}

/* log->group_lock is held. */
static log_durable_ticket
core_durable_barrier_test_cut_frontier_locked(shard_log *log)
{
   shard_log_group *current =
      __atomic_load_n(&log->accepting.group, __ATOMIC_SEQ_CST);
   if (current == NULL) {
      uint64 install_state =
         __atomic_load_n(&log->install.state, __ATOMIC_ACQUIRE);
      platform_assert(install_state & SHARD_LOG_INSTALL_TERMINAL_BIT);
      return install_state & SHARD_LOG_INSTALL_ID_MASK;
   }

   uint64 current_ticket =
      __atomic_load_n(&log->accepting.id, __ATOMIC_SEQ_CST);
   platform_assert(current_ticket == current->id);
   platform_assert(current_ticket >= SHARD_LOG_FIRST_GROUP_ID);
   return current_ticket - 1;
}

static bool32
core_durable_barrier_test_wait_for_live_log_cut(shard_log *log)
{
   timestamp start = platform_get_timestamp();
   while (platform_timestamp_elapsed(start)
          < CORE_DURABLE_BARRIER_TEST_TIMEOUT_NS)
   {
      platform_status rc = platform_mutex_lock(&log->group_lock);
      if (!SUCCESS(rc)) {
         return FALSE;
      }
      bool32 cut = core_durable_barrier_test_cut_frontier_locked(log) != 0;
      rc         = platform_mutex_unlock(&log->group_lock);
      if (!SUCCESS(rc)) {
         return FALSE;
      }
      if (cut) {
         return TRUE;
      }
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
   return FALSE;
}

static bool32
core_durable_barrier_test_wait_for_live_log_durable(shard_log *log)
{
   timestamp start = platform_get_timestamp();
   while (platform_timestamp_elapsed(start)
          < CORE_DURABLE_BARRIER_TEST_TIMEOUT_NS)
   {
      platform_status rc = platform_mutex_lock(&log->group_lock);
      if (!SUCCESS(rc)) {
         return FALSE;
      }
      log_durable_ticket cut_frontier =
         core_durable_barrier_test_cut_frontier_locked(log);
      bool32 durable =
         cut_frontier != 0
         && __atomic_load_n(&log->durable_ticket, __ATOMIC_ACQUIRE)
               >= cut_frontier;
      rc = platform_mutex_unlock(&log->group_lock);
      if (!SUCCESS(rc)) {
         return FALSE;
      }
      if (durable) {
         return TRUE;
      }
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
   return FALSE;
}

typedef struct core_durable_barrier_thread_args {
   core_handle    *spl;
   platform_status rc;
   bool32          started;
   bool32          done;
} core_durable_barrier_thread_args;

static void
core_durable_barrier_test_thread(void *arg)
{
   core_durable_barrier_thread_args *args = arg;
   __atomic_store_n(&args->started, TRUE, __ATOMIC_RELEASE);
   args->rc = core_durable_barrier(args->spl);
   __atomic_store_n(&args->done, TRUE, __ATOMIC_RELEASE);
}

typedef struct core_checkpoint_thread_args {
   core_handle    *spl;
   platform_status rc;
   bool32          done;
} core_checkpoint_thread_args;

static void
core_checkpoint_test_thread(void *arg)
{
   core_checkpoint_thread_args *args = arg;
   args->rc                          = core_checkpoint(args->spl, 0);
   __atomic_store_n(&args->done, TRUE, __ATOMIC_RELEASE);
}

typedef struct core_durable_barrier_insert_args {
   core_handle    *spl;
   key             tuple_key;
   message         msg;
   bool32         *start;
   bool32         *release;
   threadid        tid;
   platform_status rc;
   bool32          ready;
   bool32          done;
} core_durable_barrier_insert_args;

static void
core_durable_barrier_insert_thread(void *arg)
{
   core_durable_barrier_insert_args *args = arg;
   args->tid                              = platform_get_tid();
   __atomic_store_n(&args->ready, TRUE, __ATOMIC_RELEASE);
   while (args->start != NULL
          && !__atomic_load_n(args->start, __ATOMIC_ACQUIRE))
   {
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
   args->rc = core_insert(args->spl, args->tuple_key, args->msg, NULL);
   __atomic_store_n(&args->done, TRUE, __ATOMIC_RELEASE);
   while (args->release != NULL
          && !__atomic_load_n(args->release, __ATOMIC_ACQUIRE))
   {
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
}

static bool32
core_durable_barrier_test_wait_for_insert_threads(
   core_durable_barrier_insert_args *args,
   uint64                            num_args,
   bool32                            wait_for_done)
{
   timestamp start = platform_get_timestamp();
   while (platform_timestamp_elapsed(start)
          < CORE_DURABLE_BARRIER_TEST_TIMEOUT_NS)
   {
      bool32 all_reached = TRUE;
      for (uint64 i = 0; i < num_args; i++) {
         const bool32 *flag = wait_for_done ? &args[i].done : &args[i].ready;
         all_reached &= __atomic_load_n(flag, __ATOMIC_ACQUIRE);
      }
      if (all_reached) {
         return TRUE;
      }
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
   return FALSE;
}

static bool32
checkpoint_record_names_log(superblock_log_head recorded, log_head log)
{
   return !SUPERBLOCK_NO_LOG(recorded) && log_head_is_equal(recorded.head, log);
}

static bool32
checkpoint_records_name_same_log(superblock_log_head left,
                                 superblock_log_head right)
{
   return !SUPERBLOCK_NO_LOG(left) && !SUPERBLOCK_NO_LOG(right)
          && log_head_is_equal(left.head, right.head);
}

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
   system_config           *system_cfg;
   test_workload_config    *workload_cfg;
   io_handle               *io;
   clockcache              *clock_cache;
   task_system              tasks;
   test_message_generator   gen;
   checkpoint_barrier_fault checkpoint_fault;

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
   ZERO_STRUCT(data->checkpoint_fault);

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
   checkpoint_barrier_fault_uninstall(&data->checkpoint_fault);

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

static void
blob_checksum_test_fill(writable_buffer *data, uint64 length)
{
   uint8 *bytes = writable_buffer_data(data);
   for (uint64 i = 0; i < length; i++) {
      bytes[i] = (uint8)(131 * i + i / 7 + length);
   }
}

static platform_status
blob_checksum_test_roundtrip(cache           *cc,
                             slice            descriptor,
                             slice            expected,
                             writable_buffer *materialized)
{
   platform_status rc = blob_materialize_full(cc, descriptor, materialized);
   if (!SUCCESS(rc)) {
      return rc;
   }
   if (writable_buffer_length(materialized) != slice_length(expected)) {
      return STATUS_TEST_FAILED;
   }
   if (slice_length(expected) != 0
       && memcmp(writable_buffer_data(materialized),
                 slice_data(expected),
                 slice_length(expected))
             != 0)
   {
      return STATUS_TEST_FAILED;
   }
   return STATUS_OK;
}

static platform_status
blob_checksum_test_writeback(cache *cc, slice descriptor)
{
   writeback_set set;
   writeback_set_init(&set, cc, platform_get_heap_id());

   platform_status rc      = blob_writeback(cc, descriptor, &set);
   platform_status wait_rc = writeback_set_wait(&set);
   if (SUCCESS(rc)) {
      rc = wait_rc;
   }
   writeback_set_deinit(&set);
   return rc;
}

static platform_status
blob_checksum_test_build_and_check(const blob_build_config *cfg,
                                   cache                   *cc,
                                   mini_allocator          *mini,
                                   uint64                   length,
                                   writable_buffer         *data,
                                   writable_buffer         *descriptor,
                                   writable_buffer         *materialized)
{
   platform_status rc = writable_buffer_resize(data, length);
   if (!SUCCESS(rc)) {
      return rc;
   }
   blob_checksum_test_fill(data, length);

   rc = blob_build(cfg, cc, mini, writable_buffer_to_slice(data), descriptor);
   if (!SUCCESS(rc)) {
      return rc;
   }

   slice sblob = writable_buffer_to_slice(descriptor);
   rc          = blob_checksum_test_writeback(cc, sblob);
   if (!SUCCESS(rc)) {
      return rc;
   }

   const blob *blobby = slice_data(sblob);
   if (slice_length(sblob) < sizeof(*blobby) || blobby->format != BLOB_FORMAT) {
      return STATUS_TEST_FAILED;
   }

   parsed_blob pblob;
   parse_blob(
      cache_extent_size(cc), cache_page_size(cc), slice_data(sblob), &pblob);
   uint64 num_addrs = pblob.num_extents;
   for (uint64 i = 0; i < ARRAY_SIZE(pblob.leftovers); i++) {
      if (pblob.leftovers[i].length == 0) {
         break;
      }
      num_addrs++;
   }
   if (slice_length(sblob) != sizeof(blob) + num_addrs * sizeof(uint64)) {
      return STATUS_TEST_FAILED;
   }

   rc = blob_validate(cc, sblob);
   if (!SUCCESS(rc)) {
      return rc;
   }
   return blob_checksum_test_roundtrip(
      cc, sblob, writable_buffer_to_slice(data), materialized);
}

static platform_status
blob_checksum_test_mini_init(cache *cc, mini_allocator *mini, uint64 *meta_head)
{
   allocator *al                      = cache_get_allocator(cc);
   page_type  types[NUM_BLOB_BATCHES] = {
      PAGE_TYPE_BLOB,
      PAGE_TYPE_BLOB,
      PAGE_TYPE_BLOB,
   };
   platform_status rc = allocator_alloc(al, meta_head, PAGE_TYPE_MISC);
   if (!SUCCESS(rc)) {
      return rc;
   }
   mini_init_with_types(
      mini, cc, *meta_head, 0, NUM_BLOB_BATCHES, PAGE_TYPE_MISC, types);
   return STATUS_OK;
}

static platform_status
blob_checksum_test_mini_deinit(cache          *cc,
                               mini_allocator *mini,
                               uint64          meta_head)
{
   mini_release(mini);
   return mini_dec_ref(cc, meta_head, PAGE_TYPE_MISC) == 0 ? STATUS_OK
                                                           : STATUS_TEST_FAILED;
}

CTEST2(splinter, test_blob_checksums)
{
   cache            *cc  = (cache *)data->clock_cache;
   blob_build_config cfg = {
      .extent_batch  = 0,
      .page_batch    = 1,
      .subpage_batch = 2,
      .alignment     = 0,
   };

   writable_buffer source_data;
   writable_buffer descriptor;
   writable_buffer materialized;
   writable_buffer checked_clone;
   writable_buffer invalid_clone;
   writable_buffer_init(&source_data, data->hid);
   writable_buffer_init(&descriptor, data->hid);
   writable_buffer_init(&materialized, data->hid);
   writable_buffer_init(&checked_clone, data->hid);
   writable_buffer_init(&invalid_clone, data->hid);

   mini_allocator source_mini;
   mini_allocator checked_clone_mini;
   bool32         source_mini_live        = FALSE;
   bool32         checked_clone_mini_live = FALSE;
   uint64         source_meta_head        = 0;
   uint64         checked_clone_meta_head = 0;

   platform_status rc =
      blob_checksum_test_mini_init(cc, &source_mini, &source_meta_head);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   source_mini_live = TRUE;

   uint64 page_size   = cache_page_size(cc);
   uint64 extent_size = cache_extent_size(cc);
   uint64 lengths[]   = {
      0,
      1,
      page_size / 2 + 1,
      page_size - 1,
      page_size + page_size / 2,
      extent_size - 1,
      extent_size,
      extent_size + page_size / 2,
      2 * extent_size + page_size + 17,
   };
   for (uint64 i = 0; i < ARRAY_SIZE(lengths); i++) {
      rc = blob_checksum_test_build_and_check(&cfg,
                                              cc,
                                              &source_mini,
                                              lengths[i],
                                              &source_data,
                                              &descriptor,
                                              &materialized);
      if (!SUCCESS(rc)) {
         goto cleanup;
      }
   }

   /* Leave one full extent plus a separately allocated tail for cloning. */
   uint64 clone_length = extent_size + page_size / 2 + 17;
   rc                  = blob_checksum_test_build_and_check(&cfg,
                                           cc,
                                           &source_mini,
                                           clone_length,
                                           &source_data,
                                           &descriptor,
                                           &materialized);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   rc = blob_checksum_test_mini_init(
      cc, &checked_clone_mini, &checked_clone_meta_head);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   checked_clone_mini_live = TRUE;
   rc                      = blob_clone(&cfg,
                   cc,
                   &checked_clone_mini,
                   writable_buffer_to_slice(&descriptor),
                   &checked_clone);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   const blob *source_blob = writable_buffer_data(&descriptor);
   const blob *clone_blob  = writable_buffer_data(&checked_clone);
   if (source_blob->format != BLOB_FORMAT || clone_blob->format != BLOB_FORMAT)
   {
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   checksum128 source_checksum = source_blob->checksum;
   checksum128 clone_checksum  = clone_blob->checksum;
   if (!platform_checksum_is_equal(source_checksum, clone_checksum)) {
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   rc = blob_checksum_test_writeback(cc,
                                     writable_buffer_to_slice(&checked_clone));
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc = blob_validate(cc, writable_buffer_to_slice(&checked_clone));
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   rc = blob_checksum_test_roundtrip(cc,
                                     writable_buffer_to_slice(&checked_clone),
                                     writable_buffer_to_slice(&source_data),
                                     &materialized);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }

   message inline_msg = message_create(
      MESSAGE_TYPE_INSERT, NULL, writable_buffer_to_slice(&source_data));
   message blob_msg = message_create(
      MESSAGE_TYPE_INSERT, cc, writable_buffer_to_slice(&descriptor));
   if (!SUCCESS(message_validate(inline_msg))
       || !SUCCESS(message_validate(blob_msg)))
   {
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }

   /* Unknown descriptor formats must be rejected, not treated as legacy. */
   blob  *mutable_blob         = writable_buffer_data(&descriptor);
   uint16 expected_format      = mutable_blob->format;
   mutable_blob->format        = BLOB_FORMAT + 1;
   slice           invalid     = writable_buffer_to_slice(&descriptor);
   platform_status validate_rc = blob_validate(cc, invalid);
   platform_status materialize_rc =
      blob_materialize_full(cc, invalid, &materialized);
   platform_status clone_rc =
      blob_clone(&cfg, cc, &checked_clone_mini, invalid, &invalid_clone);
   mutable_blob->format = expected_format;
   if (!STATUS_IS_EQ(validate_rc, STATUS_INVALID_STATE)
       || !STATUS_IS_EQ(materialize_rc, STATUS_INVALID_STATE)
       || !STATUS_IS_EQ(clone_rc, STATUS_INVALID_STATE))
   {
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }

   /* Corrupt the clone's private tail, leaving its shared full extent alone. */
   blob_page_iterator iter;
   rc = blob_page_iterator_init(cc,
                                &iter,
                                writable_buffer_to_slice(&checked_clone),
                                clone_length - 1,
                                BLOB_PAGE_ITERATOR_MODE_NO_PREFETCH);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   uint64 corrupt_offset;
   slice  corrupt_data;
   rc = blob_page_iterator_get_curr(&iter, &corrupt_offset, &corrupt_data);
   if (!SUCCESS(rc)) {
      blob_page_iterator_deinit(&iter);
      goto cleanup;
   }
   if (corrupt_offset != clone_length - 1 || slice_length(corrupt_data) == 0) {
      blob_page_iterator_deinit(&iter);
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   uint64 corrupt_page_addr   = iter.fragment.addr;
   uint64 corrupt_page_offset = iter.fragment.offset;
   blob_page_iterator_deinit(&iter);

   page_handle *page = cache_get(cc, corrupt_page_addr, TRUE, PAGE_TYPE_BLOB);
   if (page == NULL) {
      rc = STATUS_IO_ERROR;
      goto cleanup;
   }
   while (!cache_try_claim(cc, page)) {
      cache_unget(cc, page);
      page = cache_get(cc, corrupt_page_addr, TRUE, PAGE_TYPE_BLOB);
   }
   cache_lock(cc, page);
   page->data[corrupt_page_offset] ^= 0x80;
   cache_unlock(cc, page);
   cache_unclaim(cc, page);
   cache_unget(cc, page);

   rc = blob_validate(cc, writable_buffer_to_slice(&checked_clone));
   if (!STATUS_IS_EQ(rc, STATUS_IO_ERROR)) {
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   blob_msg = message_create(
      MESSAGE_TYPE_INSERT, cc, writable_buffer_to_slice(&checked_clone));
   rc = message_validate(blob_msg);
   if (!STATUS_IS_EQ(rc, STATUS_IO_ERROR)) {
      rc = STATUS_TEST_FAILED;
      goto cleanup;
   }
   rc = STATUS_OK;

cleanup:
   if (checked_clone_mini_live) {
      platform_status cleanup_rc = blob_checksum_test_mini_deinit(
         cc, &checked_clone_mini, checked_clone_meta_head);
      if (SUCCESS(rc) && !SUCCESS(cleanup_rc)) {
         rc = cleanup_rc;
      }
   }
   if (source_mini_live) {
      platform_status cleanup_rc =
         blob_checksum_test_mini_deinit(cc, &source_mini, source_meta_head);
      if (SUCCESS(rc) && !SUCCESS(cleanup_rc)) {
         rc = cleanup_rc;
      }
   }
   writable_buffer_deinit(&invalid_clone);
   writable_buffer_deinit(&checked_clone);
   writable_buffer_deinit(&materialized);
   writable_buffer_deinit(&descriptor);
   writable_buffer_deinit(&source_data);

   ASSERT_TRUE(SUCCESS(rc),
               "blob checksum test failed: %s",
               platform_status_to_string(rc));
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
 * Pause an insert after it has reserved a log group and made its memtable
 * mutation visible, but before it consumes the reservation.  A durability
 * barrier must cut that group and wait for the reserved write.  It must not
 * take insert exclusion: a second insert should reserve the new group and
 * finish while both the first insert and the barrier remain blocked.
 */
CTEST2(splinter, test_durable_barrier_waits_for_visible_insert_log_write)
{
   allocator *alp                         = (allocator *)&data->al;
   data->system_cfg->splinter_cfg.use_log = TRUE;
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

   DECLARE_AUTO_KEY_BUFFER(first_keybuf, data->hid);
   DECLARE_AUTO_KEY_BUFFER(second_keybuf, data->hid);
   merge_accumulator first_msg;
   merge_accumulator second_msg;
   merge_accumulator_init(&first_msg, data->hid);
   merge_accumulator_init(&second_msg, data->hid);
   test_key(
      &first_keybuf, TEST_RANDOM, 1, 0, 0, data->workload_cfg->key_size, 0);
   generate_test_message(&data->gen, 1, &first_msg);
   test_key(
      &second_keybuf, TEST_RANDOM, 2, 0, 0, data->workload_cfg->key_size, 0);
   generate_test_message(&data->gen, 2, &second_msg);

   core_durable_barrier_insert_args first_insert_args = {
      .spl       = &spl,
      .tuple_key = key_buffer_key(&first_keybuf),
      .msg       = merge_accumulator_to_message(&first_msg),
      .rc        = STATUS_INVALID_STATE,
   };
   core_durable_barrier_insert_args second_insert_args = {
      .spl       = &spl,
      .tuple_key = key_buffer_key(&second_keybuf),
      .msg       = merge_accumulator_to_message(&second_msg),
      .rc        = STATUS_INVALID_STATE,
   };
   core_durable_barrier_thread_args barrier_args = {
      .spl = &spl,
      .rc  = STATUS_INVALID_STATE,
   };
   platform_thread      first_insert_thread  = {0};
   platform_thread      second_insert_thread = {0};
   platform_thread      barrier_thread       = {0};
   core_log_write_block log_block            = {0};

   lookup_result qdata;
   lookup_result_init(
      &qdata, spl.cfg.data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   bool32          first_insert_created             = FALSE;
   bool32          second_insert_created            = FALSE;
   bool32          barrier_created                  = FALSE;
   bool32          write_blocked                    = FALSE;
   bool32          visible_before_log_write         = FALSE;
   bool32          log_cut_while_writer_blocked     = FALSE;
   bool32          second_insert_completed          = FALSE;
   bool32          barrier_completed_before_release = FALSE;
   platform_status lookup_rc                        = STATUS_INVALID_STATE;
   platform_status first_insert_create_rc           = STATUS_INVALID_STATE;
   platform_status second_insert_create_rc          = STATUS_INVALID_STATE;
   platform_status barrier_create_rc                = STATUS_INVALID_STATE;
   platform_status first_insert_join_rc             = STATUS_INVALID_STATE;
   platform_status second_insert_join_rc            = STATUS_INVALID_STATE;
   platform_status barrier_join_rc                  = STATUS_INVALID_STATE;

   core_log_write_block_install(&log_block, spl.log);
   first_insert_create_rc =
      platform_thread_create(&first_insert_thread,
                             FALSE,
                             core_durable_barrier_insert_thread,
                             &first_insert_args,
                             data->hid);
   first_insert_created = SUCCESS(first_insert_create_rc);
   if (first_insert_created) {
      write_blocked = core_durable_barrier_test_wait(&log_block.entered);
   }

   if (write_blocked) {
      lookup_rc = core_lookup(&spl, first_insert_args.tuple_key, &qdata);
      if (SUCCESS(lookup_rc)) {
         visible_before_log_write =
            message_lex_cmp(
               first_insert_args.msg,
               merge_accumulator_to_message(lookup_result_accumulator(&qdata)))
            == 0;
      }

      barrier_create_rc =
         platform_thread_create(&barrier_thread,
                                FALSE,
                                core_durable_barrier_test_thread,
                                &barrier_args,
                                data->hid);
      barrier_created = SUCCESS(barrier_create_rc);
      if (barrier_created) {
         log_cut_while_writer_blocked =
            core_durable_barrier_test_wait_for_live_log_cut(
               (shard_log *)spl.log);
         if (log_cut_while_writer_blocked) {
            second_insert_create_rc =
               platform_thread_create(&second_insert_thread,
                                      FALSE,
                                      core_durable_barrier_insert_thread,
                                      &second_insert_args,
                                      data->hid);
            second_insert_created = SUCCESS(second_insert_create_rc);
            if (second_insert_created) {
               second_insert_completed =
                  core_durable_barrier_test_wait(&second_insert_args.done);
            }
         }
         barrier_completed_before_release =
            __atomic_load_n(&barrier_args.done, __ATOMIC_ACQUIRE);
      }
   }

   core_log_write_block_release(&log_block);
   if (first_insert_created) {
      first_insert_join_rc = platform_thread_join(&first_insert_thread);
   }
   if (second_insert_created) {
      second_insert_join_rc = platform_thread_join(&second_insert_thread);
   }
   if (barrier_created) {
      barrier_join_rc = platform_thread_join(&barrier_thread);
   }
   core_log_write_block_uninstall(&log_block);

   lookup_result_deinit(&qdata);
   merge_accumulator_deinit(&second_msg);
   merge_accumulator_deinit(&first_msg);
   core_destroy(&spl);

   ASSERT_TRUE(SUCCESS(first_insert_create_rc));
   ASSERT_TRUE(write_blocked,
               "insert did not reach the reserved-write blocking hook\n");
   ASSERT_TRUE(SUCCESS(lookup_rc),
               "lookup of the paused insert failed: %s\n",
               platform_status_to_string(lookup_rc));
   ASSERT_TRUE(visible_before_log_write,
               "paused insert was not visible before its reserved write\n");
   ASSERT_TRUE(SUCCESS(barrier_create_rc));
   ASSERT_TRUE(
      log_cut_while_writer_blocked,
      "core_durable_barrier did not cut the reserved writer's group\n");
   ASSERT_TRUE(SUCCESS(second_insert_create_rc));
   ASSERT_TRUE(
      second_insert_completed,
      "core_durable_barrier excluded a writer after cutting the log\n");
   ASSERT_FALSE(barrier_completed_before_release,
                "core_durable_barrier passed a visible reserved insert\n");
   ASSERT_TRUE(SUCCESS(first_insert_join_rc));
   ASSERT_TRUE(SUCCESS(second_insert_join_rc));
   ASSERT_TRUE(SUCCESS(barrier_join_rc));
   ASSERT_TRUE(SUCCESS(first_insert_args.rc),
               "paused insert failed: %s\n",
               platform_status_to_string(first_insert_args.rc));
   ASSERT_TRUE(SUCCESS(second_insert_args.rc),
               "concurrent insert failed: %s\n",
               platform_status_to_string(second_insert_args.rc));
   ASSERT_TRUE(SUCCESS(barrier_args.rc),
               "core_durable_barrier failed: %s\n",
               platform_status_to_string(barrier_args.rc));
}

/*
 * Closing a group coalesces the small private tails left by concurrent writers
 * into one page.  That page also carries the group terminator, so the group's
 * durable page count is one rather than one page per writer plus a dedicated
 * terminator.  Keep every worker alive until after inspection to prevent the
 * platform from recycling thread IDs and accidentally sharing a tail buffer.
 */
#define CORE_DURABLE_BARRIER_TAIL_WRITERS 4
CTEST2(splinter, test_durable_barrier_packs_concurrent_small_tails)
{
   allocator *alp                         = (allocator *)&data->al;
   data->system_cfg->splinter_cfg.use_log = TRUE;
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

   key_buffer        keybuf[CORE_DURABLE_BARRIER_TAIL_WRITERS];
   merge_accumulator msg[CORE_DURABLE_BARRIER_TAIL_WRITERS];
   core_durable_barrier_insert_args
                   insert_args[CORE_DURABLE_BARRIER_TAIL_WRITERS];
   platform_thread insert_thread[CORE_DURABLE_BARRIER_TAIL_WRITERS] = {0};
   platform_status join_rc[CORE_DURABLE_BARRIER_TAIL_WRITERS];
   bool32          start   = FALSE;
   bool32          release = FALSE;

   uint64 num_initialized = 0;
   uint64 num_created     = 0;
   for (uint64 i = 0; i < CORE_DURABLE_BARRIER_TAIL_WRITERS; i++) {
      key_buffer_init(&keybuf[i], data->hid);
      merge_accumulator_init(&msg[i], data->hid);
      num_initialized++;
      test_key(
         &keybuf[i], TEST_RANDOM, i + 1, 0, 0, data->workload_cfg->key_size, 0);
      generate_test_message(&data->gen, i + 1, &msg[i]);
      insert_args[i] = (core_durable_barrier_insert_args){
         .spl       = &spl,
         .tuple_key = key_buffer_key(&keybuf[i]),
         .msg       = merge_accumulator_to_message(&msg[i]),
         .start     = &start,
         .release   = &release,
         .tid       = INVALID_TID,
         .rc        = STATUS_INVALID_STATE,
      };
      join_rc[i] = STATUS_INVALID_STATE;

      rc = platform_thread_create(&insert_thread[i],
                                  FALSE,
                                  core_durable_barrier_insert_thread,
                                  &insert_args[i],
                                  data->hid);
      if (!SUCCESS(rc)) {
         break;
      }
      num_created++;
   }

   bool32 all_created = num_created == CORE_DURABLE_BARRIER_TAIL_WRITERS;
   bool32 all_ready   = all_created
                      && core_durable_barrier_test_wait_for_insert_threads(
                         insert_args, num_created, FALSE);
   bool32 distinct_tids = all_ready;
   if (all_ready) {
      for (uint64 i = 0; i < num_created; i++) {
         distinct_tids &= insert_args[i].tid < MAX_THREADS;
         for (uint64 j = 0; j < i; j++) {
            distinct_tids &= insert_args[i].tid != insert_args[j].tid;
         }
      }
   }

   __atomic_store_n(&start, TRUE, __ATOMIC_RELEASE);
   bool32 all_inserted = all_ready
                         && core_durable_barrier_test_wait_for_insert_threads(
                            insert_args, num_created, TRUE);
   bool32 inserts_succeeded = all_inserted;
   if (all_inserted) {
      for (uint64 i = 0; i < num_created; i++) {
         inserts_succeeded &= SUCCESS(insert_args[i].rc);
      }
   }

   core_durable_barrier_thread_args barrier_args = {
      .spl = &spl,
      .rc  = STATUS_INVALID_STATE,
   };
   platform_thread barrier_thread    = {0};
   platform_status barrier_create_rc = STATUS_INVALID_STATE;
   platform_status barrier_join_rc   = STATUS_INVALID_STATE;
   bool32          barrier_created   = FALSE;
   bool32          barrier_blocked   = FALSE;
   bool32          inspected_group   = FALSE;
   uint64          page_count        = 0;

   if (inserts_succeeded && distinct_tids) {
      checkpoint_barrier_fault_install(&data->checkpoint_fault, data->io);
      checkpoint_barrier_fault_block(&data->checkpoint_fault, 0);
      barrier_create_rc =
         platform_thread_create(&barrier_thread,
                                FALSE,
                                core_durable_barrier_test_thread,
                                &barrier_args,
                                data->hid);
      barrier_created = SUCCESS(barrier_create_rc);
      if (barrier_created) {
         barrier_blocked = core_durable_barrier_test_wait(
            &data->checkpoint_fault.block_entered);
      }

      if (barrier_blocked) {
         shard_log *log = (shard_log *)spl.log;
         platform_mutex_lock(&log->group_lock);
         shard_log_group *closed = log->groups_head;
         shard_log_group *accepting =
            __atomic_load_n(&log->accepting.group, __ATOMIC_SEQ_CST);
         inspected_group = closed != NULL && closed != accepting;
         if (inspected_group) {
            page_count = closed->page_count;
         }
         platform_mutex_unlock(&log->group_lock);
      }

      checkpoint_barrier_fault_release(&data->checkpoint_fault);
      if (barrier_created) {
         barrier_join_rc = platform_thread_join(&barrier_thread);
      }
      checkpoint_barrier_fault_uninstall(&data->checkpoint_fault);
   }

   __atomic_store_n(&release, TRUE, __ATOMIC_RELEASE);
   for (uint64 i = 0; i < num_created; i++) {
      join_rc[i] = platform_thread_join(&insert_thread[i]);
   }
   for (uint64 i = 0; i < num_initialized; i++) {
      merge_accumulator_deinit(&msg[i]);
      key_buffer_deinit(&keybuf[i]);
   }
   core_destroy(&spl);

   ASSERT_TRUE(all_created, "failed to create all concurrent log writers\n");
   ASSERT_TRUE(all_ready, "concurrent log writers did not reach their gate\n");
   ASSERT_TRUE(distinct_tids,
               "concurrent log writers did not retain distinct thread IDs\n");
   ASSERT_TRUE(all_inserted, "concurrent log writers did not finish inserts\n");
   ASSERT_TRUE(inserts_succeeded, "a concurrent log writer failed\n");
   ASSERT_TRUE(SUCCESS(barrier_create_rc));
   ASSERT_TRUE(barrier_blocked,
               "core_durable_barrier did not reach the device barrier\n");
   ASSERT_TRUE(inspected_group,
               "could not inspect the closed durability group\n");
   ASSERT_EQUAL(1,
                page_count,
                "small concurrent tails used %lu log pages instead of one\n",
                page_count);
   ASSERT_TRUE(SUCCESS(barrier_join_rc));
   ASSERT_TRUE(SUCCESS(barrier_args.rc),
               "core_durable_barrier failed: %s\n",
               platform_status_to_string(barrier_args.rc));
   for (uint64 i = 0; i < num_created; i++) {
      ASSERT_TRUE(SUCCESS(join_rc[i]));
   }
}
#undef CORE_DURABLE_BARRIER_TAIL_WRITERS

/*
 * This order is a counterexample for both the old in-thread-order First Fit
 * packer and a largest-first packer which fills bins from the smallest item
 * backward.  With C as the page payload capacity, those algorithms produce
 * three pages:
 *
 *    old FF:  (.08 + .18 + .18), .68, .78
 *    reverse: (.78 + .08), (.68 + .18), .18
 *
 * First Fit Decreasing instead produces exactly two:
 *
 *    (.78 + .18), (.68 + .18 + .08)
 */
#define SHARD_LOG_FFD_TEST_WRITERS 5
static const uint64 shard_log_ffd_test_percent[SHARD_LOG_FFD_TEST_WRITERS] = {
   8,
   18,
   18,
   68,
   78,
};

typedef struct shard_log_ffd_test_writer {
   log_handle     *log;
   platform_thread thread;
   bool32         *start;
   bool32         *release;
   message         msg;
   uint64          entry_num;
   uint64          key_size;
   threadid        tid;
   int             log_rc;
   bool32          ready;
   bool32          done;
} shard_log_ffd_test_writer;

static void
shard_log_ffd_test_write(void *arg)
{
   shard_log_ffd_test_writer *writer = arg;
   platform_heap_id           hid    = platform_get_heap_id();
   DECLARE_AUTO_KEY_BUFFER(keybuf, hid);

   writer->tid = platform_get_tid();
   __atomic_store_n(&writer->ready, TRUE, __ATOMIC_RELEASE);
   while (!__atomic_load_n(writer->start, __ATOMIC_ACQUIRE)) {
      platform_sleep_ns(USEC_TO_NSEC(50));
   }

   key tuple_key = test_key(
      &keybuf, TEST_RANDOM, writer->entry_num, 0, 0, writer->key_size, 0);
   writer->log_rc =
      log_write(writer->log, tuple_key, writer->msg, writer->entry_num, 0);
   __atomic_store_n(&writer->done, TRUE, __ATOMIC_RELEASE);

   /* Keep the thread ID, and therefore its staging-buffer assignment, live. */
   while (!__atomic_load_n(writer->release, __ATOMIC_ACQUIRE)) {
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
}

static bool32
shard_log_ffd_test_wait_for_writers(shard_log_ffd_test_writer *writers,
                                    bool32                     wait_for_done)
{
   timestamp start = platform_get_timestamp();
   while (platform_timestamp_elapsed(start)
          < CORE_DURABLE_BARRIER_TEST_TIMEOUT_NS)
   {
      bool32 all_reached = TRUE;
      for (uint64 i = 0; i < SHARD_LOG_FFD_TEST_WRITERS; i++) {
         const bool32 *flag =
            wait_for_done ? &writers[i].done : &writers[i].ready;
         all_reached &= __atomic_load_n(flag, __ATOMIC_ACQUIRE);
      }
      if (all_reached) {
         return TRUE;
      }
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
   return FALSE;
}

typedef struct shard_log_ffd_test_sealer {
   log_handle     *log;
   platform_thread thread;
   platform_status rc;
} shard_log_ffd_test_sealer;

static void
shard_log_ffd_test_seal(void *arg)
{
   shard_log_ffd_test_sealer *sealer = arg;
   sealer->rc                        = log_seal(sealer->log);
}

CTEST2(splinter, test_shard_log_first_fit_decreasing_packing)
{
   const uint64 key_size = 8;
   uint64       page_capacity =
      cache_page_size((cache *)data->clock_cache) - sizeof(shard_log_hdr);
   /* sizeof(log_entry), which is private to shard_log.c. */
   uint64 entry_overhead = 2 * sizeof(uint64) + sizeof(ondisk_tuple) + key_size;

   char   *payload[SHARD_LOG_FFD_TEST_WRITERS] = {0};
   message expected_msg[SHARD_LOG_FFD_TEST_WRITERS];
   uint64  expected_size[SHARD_LOG_FFD_TEST_WRITERS];
   for (uint64 rank = 0; rank < SHARD_LOG_FFD_TEST_WRITERS; rank++) {
      expected_size[rank] =
         page_capacity * shard_log_ffd_test_percent[rank] / 100;
      platform_assert(expected_size[rank] > entry_overhead);
      uint64 message_size = expected_size[rank] - entry_overhead;
      platform_assert(message_size <= UINT16_MAX);
      payload[rank] =
         TYPED_ARRAY_MALLOC(data->hid, payload[rank], message_size);
      platform_assert(payload[rank] != NULL);
      memset(payload[rank], (int)(rank + 1), message_size);
      expected_msg[rank] = message_create(
         MESSAGE_TYPE_INSERT, NULL, slice_create(message_size, payload[rank]));
   }

   log_handle *log = NULL;
   platform_assert_status_ok(shard_log_create(
      (cache *)data->clock_cache, &data->system_cfg->log_cfg, data->hid, &log));
   log_head segment = log_get_head(log);

   shard_log_ffd_test_writer writers[SHARD_LOG_FFD_TEST_WRITERS] = {0};
   uint64                    order[SHARD_LOG_FFD_TEST_WRITERS];
   bool32                    selected[SHARD_LOG_FFD_TEST_WRITERS] = {0};
   bool32                    start                                = FALSE;
   bool32                    release                              = FALSE;
   for (uint64 i = 0; i < SHARD_LOG_FFD_TEST_WRITERS; i++) {
      writers[i] = (shard_log_ffd_test_writer){
         .log      = log,
         .start    = &start,
         .release  = &release,
         .tid      = INVALID_TID,
         .log_rc   = -1,
         .key_size = key_size,
      };
      platform_assert_status_ok(platform_thread_create(&writers[i].thread,
                                                       FALSE,
                                                       shard_log_ffd_test_write,
                                                       &writers[i],
                                                       data->hid));
   }

   bool32 all_ready = shard_log_ffd_test_wait_for_writers(writers, FALSE);
   platform_assert(all_ready, "FFD test writers did not become ready");

   /* Assign payload sizes by actual tid, not scheduler-dependent spawn order.
    */
   for (uint64 rank = 0; rank < SHARD_LOG_FFD_TEST_WRITERS; rank++) {
      threadid lowest_tid = INVALID_TID;
      uint64   lowest_i   = SHARD_LOG_FFD_TEST_WRITERS;
      for (uint64 i = 0; i < SHARD_LOG_FFD_TEST_WRITERS; i++) {
         if (!selected[i] && writers[i].tid < lowest_tid) {
            lowest_tid = writers[i].tid;
            lowest_i   = i;
         }
      }
      platform_assert(lowest_i != SHARD_LOG_FFD_TEST_WRITERS);
      platform_assert(lowest_tid != 0,
                      "thread 0 must remain the empty final-page buffer");
      selected[lowest_i]          = TRUE;
      order[rank]                 = lowest_i;
      writers[lowest_i].msg       = expected_msg[rank];
      writers[lowest_i].entry_num = rank;
   }

   __atomic_store_n(&start, TRUE, __ATOMIC_RELEASE);
   bool32 all_done = shard_log_ffd_test_wait_for_writers(writers, TRUE);
   platform_assert(all_done, "FFD test writers did not finish appending");
   for (uint64 i = 0; i < SHARD_LOG_FFD_TEST_WRITERS; i++) {
      platform_assert(writers[i].log_rc == 0);
   }

   shard_log       *slog = (shard_log *)log;
   shard_log_group *group =
      __atomic_load_n(&slog->accepting.group, __ATOMIC_SEQ_CST);
   platform_assert(group != NULL);
   platform_assert(group->thread_data[0].offset == sizeof(shard_log_hdr));
   for (uint64 rank = 0; rank < SHARD_LOG_FFD_TEST_WRITERS; rank++) {
      shard_log_ffd_test_writer *writer      = &writers[order[rank]];
      shard_log_thread_data     *thread_data = &group->thread_data[writer->tid];
      platform_assert(thread_data->state == SHARD_LOG_BUFFER_OPEN);
      ASSERT_EQUAL(expected_size[rank],
                   thread_data->offset - sizeof(shard_log_hdr),
                   "writer rank %lu staged an unexpected payload size\n",
                   rank);
   }

   checkpoint_barrier_fault_install(&data->checkpoint_fault, data->io);
   checkpoint_barrier_fault_block(&data->checkpoint_fault, 0);
   shard_log_ffd_test_sealer sealer = {
      .log = log,
      .rc  = STATUS_INVALID_STATE,
   };
   platform_assert_status_ok(platform_thread_create(
      &sealer.thread, FALSE, shard_log_ffd_test_seal, &sealer, data->hid));
   bool32 barrier_blocked =
      core_durable_barrier_test_wait(&data->checkpoint_fault.block_entered);

   uint64 page_count      = 0;
   bool32 inspected_group = FALSE;
   if (barrier_blocked) {
      platform_mutex_lock(&slog->group_lock);
      shard_log_group *sealed_group = slog->groups_head;
      shard_log_group *accepting =
         __atomic_load_n(&slog->accepting.group, __ATOMIC_SEQ_CST);
      inspected_group = sealed_group != NULL && accepting == NULL;
      if (inspected_group) {
         page_count = sealed_group->page_count;
      }
      platform_mutex_unlock(&slog->group_lock);
   }

   checkpoint_barrier_fault_release(&data->checkpoint_fault);
   platform_status seal_join_rc = platform_thread_join(&sealer.thread);
   checkpoint_barrier_fault_uninstall(&data->checkpoint_fault);

   __atomic_store_n(&release, TRUE, __ATOMIC_RELEASE);
   for (uint64 i = 0; i < SHARD_LOG_FFD_TEST_WRITERS; i++) {
      platform_assert_status_ok(platform_thread_join(&writers[i].thread));
   }

   ASSERT_TRUE(barrier_blocked,
               "sealed log did not reach the device barrier\n");
   ASSERT_TRUE(inspected_group,
               "could not inspect the sealed FFD test group\n");
   ASSERT_EQUAL(2,
                page_count,
                "FFD packed the counterexample into %lu pages, not two\n",
                page_count);
   ASSERT_TRUE(SUCCESS(seal_join_rc));
   ASSERT_TRUE(SUCCESS(sealer.rc),
               "sealing the FFD test log failed: %s\n",
               platform_status_to_string(sealer.rc));

   log_deinit(log);

   log_iterator   *itor = NULL;
   platform_status rc   = shard_log_iterator_create((cache *)data->clock_cache,
                                                  &data->system_cfg->log_cfg,
                                                  data->hid,
                                                  segment,
                                                  0,
                                                  &itor);
   platform_assert_status_ok(rc);
   ASSERT_TRUE(log_iterator_stream_complete(itor));
   DECLARE_AUTO_KEY_BUFFER(expected_keybuf, data->hid);
   for (uint64 rank = 0; rank < SHARD_LOG_FFD_TEST_WRITERS; rank++) {
      ASSERT_TRUE(log_iterator_can_next(itor));
      key expected_key =
         test_key(&expected_keybuf, TEST_RANDOM, rank, 0, 0, key_size, 0);
      key     actual_key;
      message actual_msg;
      log_iterator_curr(itor, &actual_key, &actual_msg);
      uint64 memtable_generation;
      uint64 leaf_generation;
      log_iterator_curr_generations(
         itor, &memtable_generation, &leaf_generation);
      ASSERT_EQUAL(rank, memtable_generation);
      ASSERT_EQUAL(0, leaf_generation);
      ASSERT_EQUAL(0,
                   data_key_compare(
                      data->system_cfg->data_cfg, expected_key, actual_key));
      ASSERT_EQUAL(0, message_lex_cmp(expected_msg[rank], actual_msg));
      platform_assert_status_ok(log_iterator_next(itor));
   }
   ASSERT_FALSE(log_iterator_can_next(itor));
   log_iterator_deinit(itor);
   shard_log_dec_ref((cache *)data->clock_cache, &segment);

   for (uint64 rank = 0; rank < SHARD_LOG_FFD_TEST_WRITERS; rank++) {
      platform_free(data->hid, payload[rank]);
   }
}
#undef SHARD_LOG_FFD_TEST_WRITERS

typedef struct shard_log_page_alloc_fault {
   cache           *cc;
   const cache_ops *saved_ops;
   cache_ops        fault_ops;
   bool32           fail_next_log_page;
   uint64           failures;
} shard_log_page_alloc_fault;

static shard_log_page_alloc_fault *active_shard_log_page_alloc_fault;

static page_handle *
shard_log_test_page_alloc(cache *cc, uint64 addr, page_type type)
{
   shard_log_page_alloc_fault *fault =
      __atomic_load_n(&active_shard_log_page_alloc_fault, __ATOMIC_ACQUIRE);
   platform_assert(fault != NULL && fault->cc == cc);

   if (type == PAGE_TYPE_LOG
       && __atomic_exchange_n(
          &fault->fail_next_log_page, FALSE, __ATOMIC_ACQ_REL))
   {
      __atomic_fetch_add(&fault->failures, 1, __ATOMIC_RELAXED);
      return NULL;
   }
   return fault->saved_ops->page_alloc(cc, addr, type);
}

static void
shard_log_page_alloc_fault_install(shard_log_page_alloc_fault *fault, cache *cc)
{
   platform_assert(
      __atomic_load_n(&active_shard_log_page_alloc_fault, __ATOMIC_ACQUIRE)
      == NULL);
   ZERO_CONTENTS(fault);
   fault->cc                   = cc;
   fault->saved_ops            = cc->ops;
   fault->fault_ops            = *cc->ops;
   fault->fault_ops.page_alloc = shard_log_test_page_alloc;
   fault->fail_next_log_page   = TRUE;
   __atomic_store_n(
      &active_shard_log_page_alloc_fault, fault, __ATOMIC_RELEASE);
   cc->ops = &fault->fault_ops;
}

static void
shard_log_page_alloc_fault_uninstall(shard_log_page_alloc_fault *fault)
{
   platform_assert(
      __atomic_load_n(&active_shard_log_page_alloc_fault, __ATOMIC_ACQUIRE)
      == fault);
   fault->cc->ops = fault->saved_ops;
   __atomic_store_n(&active_shard_log_page_alloc_fault, NULL, __ATOMIC_RELEASE);
   fault->cc        = NULL;
   fault->saved_ops = NULL;
}

/*
 * Page-slot allocation happens before an OPEN staging image is frozen. A
 * transient failure must therefore leave the image mutable and intact; a
 * later seal can allocate a different page and finish the exact same record.
 */
CTEST2(splinter, test_shard_log_page_alloc_failure_retries_open_buffer)
{
   cache      *cc = (cache *)data->clock_cache;
   log_handle *log;
   platform_assert_status_ok(
      shard_log_create(cc, &data->system_cfg->log_cfg, data->hid, &log));
   log_head segment = log_get_head(log);

   const uint64      entry_num = 8675309;
   merge_accumulator msg;
   merge_accumulator_init(&msg, data->hid);
   generate_test_message(&data->gen, entry_num, &msg);
   DECLARE_AUTO_KEY_BUFFER(keybuf, data->hid);
   key tuple_key = test_key(
      &keybuf, TEST_RANDOM, entry_num, 0, 0, data->workload_cfg->key_size, 0);
   int log_rc = log_write(
      log, tuple_key, merge_accumulator_to_message(&msg), entry_num, 0);
   platform_assert(log_rc == 0);

   shard_log_page_alloc_fault fault;
   shard_log_page_alloc_fault_install(&fault, cc);
   platform_status first_seal_rc = log_seal(log);
   shard_log_page_alloc_fault_uninstall(&fault);

   ASSERT_TRUE(STATUS_IS_EQ(first_seal_rc, STATUS_NO_SPACE),
               "faulted seal returned %s, not out-of-space\n",
               platform_status_to_string(first_seal_rc));
   ASSERT_EQUAL(1, __atomic_load_n(&fault.failures, __ATOMIC_RELAXED));

   shard_log *slog = (shard_log *)log;
   platform_mutex_lock(&slog->group_lock);
   shard_log_group *group              = slog->groups_head;
   bool32           group_is_retryable = FALSE;
   if (group != NULL) {
      shard_log_thread_data *final              = &group->thread_data[0];
      uint64                 writeback_requests = 0;
      for (threadid tid = 0; tid < MAX_THREADS; tid++) {
         writeback_requests +=
            writeback_set_num_requests(&group->thread_data[tid].wbset);
      }
      group_is_retryable = group->state == SHARD_LOG_GROUP_TERMINATING
                           && final->state == SHARD_LOG_BUFFER_OPEN
                           && final->offset > sizeof(shard_log_hdr)
                           && final->incache_page == NULL
                           && group->page_count == 0 && writeback_requests == 0;
   }
   platform_mutex_unlock(&slog->group_lock);
   ASSERT_TRUE(group_is_retryable,
               "page allocation failure did not preserve the OPEN image\n");

   platform_status retry_rc = log_seal(log);
   ASSERT_TRUE(SUCCESS(retry_rc),
               "seal retry failed: %s\n",
               platform_status_to_string(retry_rc));
   log_deinit(log);

   log_iterator   *itor = NULL;
   platform_status rc   = shard_log_iterator_create(
      cc, &data->system_cfg->log_cfg, data->hid, segment, 0, &itor);
   platform_assert_status_ok(rc);
   ASSERT_TRUE(log_iterator_stream_complete(itor));
   ASSERT_TRUE(log_iterator_can_next(itor));

   key     replayed_key;
   message replayed_msg;
   log_iterator_curr(itor, &replayed_key, &replayed_msg);
   uint64 memtable_generation;
   uint64 leaf_generation;
   log_iterator_curr_generations(itor, &memtable_generation, &leaf_generation);
   ASSERT_EQUAL(entry_num, memtable_generation);
   ASSERT_EQUAL(0, leaf_generation);
   ASSERT_EQUAL(
      0, data_key_compare(data->system_cfg->data_cfg, tuple_key, replayed_key));
   ASSERT_EQUAL(
      0, message_lex_cmp(merge_accumulator_to_message(&msg), replayed_msg));
   platform_assert_status_ok(log_iterator_next(itor));
   ASSERT_FALSE(log_iterator_can_next(itor));

   log_iterator_deinit(itor);
   shard_log_dec_ref(cc, &segment);
   merge_accumulator_deinit(&msg);
}

/* Without a WAL, the barrier must fold its frontier into a durable COW root. */
CTEST2(splinter, test_durable_barrier_without_log_publishes_root)
{
   allocator *alp                         = (allocator *)&data->al;
   data->system_cfg->splinter_cfg.use_log = FALSE;
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
   ASSERT_NULL(spl.log);

   uint64 insert_generation = memtable_generation(&spl.mt_ctxt);
   DECLARE_AUTO_KEY_BUFFER(keybuf, data->hid);
   merge_accumulator msg;
   merge_accumulator_init(&msg, data->hid);
   test_key(&keybuf, TEST_RANDOM, 1, 0, 0, data->workload_cfg->key_size, 0);
   generate_test_message(&data->gen, 1, &msg);
   rc = core_insert(
      &spl, key_buffer_key(&keybuf), merge_accumulator_to_message(&msg), NULL);
   ASSERT_TRUE(SUCCESS(rc));

   rc = core_durable_barrier(&spl);
   ASSERT_TRUE(SUCCESS(rc),
               "no-log core_durable_barrier failed: %s\n",
               platform_status_to_string(rc));

   /* Read a fresh image from disk rather than trusting the live context. */
   superblock_context disk_superblock;
   allocator_config  *allocator_cfg = allocator_get_config(alp);
   rc                               = superblock_context_init(
      &disk_superblock, data->io, allocator_cfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   rc = superblock_mount(&disk_superblock, allocator_cfg);
   ASSERT_TRUE(SUCCESS(rc));

   superblock_tree_record record;
   superblock_get_tree_record(&disk_superblock, &record);
   ASSERT_NOT_EQUAL(0, record.root_addr);
   ASSERT_TRUE(record.first_unincorporated_generation > insert_generation,
               "durable root stops at generation %lu, insert was in %lu\n",
               record.first_unincorporated_generation,
               insert_generation);
   ASSERT_TRUE(SUPERBLOCK_NO_LOG(record.sealed_log));
   ASSERT_TRUE(SUPERBLOCK_NO_LOG(record.live_log));

   superblock_context_deinit(&disk_superblock);
   merge_accumulator_deinit(&msg);
   core_destroy(&spl);
}

/*
 * The durability cut may briefly exclude inserts, but the slow device barrier
 * must not.  Hold the first device barrier after a logged update reaches it,
 * then require a new insert to finish while the barrier thread is still held
 * inside the I/O hook.  Releasing the hook must let the original durability
 * call finish successfully.
 *
 * No CTest assertion is made while the hook is installed or either worker may
 * still be live.  That keeps a failed assertion from stranding a registered
 * thread in the blocking hook and makes fixture cleanup deterministic.
 */
CTEST2(splinter, test_durable_barrier_reopens_writers_before_device_barrier)
{
   allocator *alp                         = (allocator *)&data->al;
   data->system_cfg->splinter_cfg.use_log = TRUE;
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

   DECLARE_AUTO_KEY_BUFFER(keybuf, data->hid);
   merge_accumulator msg;
   merge_accumulator_init(&msg, data->hid);

   test_key(&keybuf, TEST_RANDOM, 1, 0, 0, data->workload_cfg->key_size, 0);
   generate_test_message(&data->gen, 1, &msg);
   rc = core_insert(
      &spl, key_buffer_key(&keybuf), merge_accumulator_to_message(&msg), NULL);
   ASSERT_TRUE(SUCCESS(rc));
   rc = task_perform_until_quiescent(spl.ts);
   ASSERT_TRUE(SUCCESS(rc));

   /* Keep the second tuple's storage alive until its worker has joined. */
   test_key(&keybuf, TEST_RANDOM, 2, 0, 0, data->workload_cfg->key_size, 0);
   generate_test_message(&data->gen, 2, &msg);
   core_durable_barrier_insert_args insert_args = {
      .spl       = &spl,
      .tuple_key = key_buffer_key(&keybuf),
      .msg       = merge_accumulator_to_message(&msg),
      .rc        = STATUS_INVALID_STATE,
   };
   core_durable_barrier_thread_args barrier_args = {
      .spl = &spl,
      .rc  = STATUS_INVALID_STATE,
   };

   platform_thread barrier_thread                   = {0};
   platform_thread insert_thread                    = {0};
   bool32          barrier_created                  = FALSE;
   bool32          insert_created                   = FALSE;
   bool32          barrier_blocked                  = FALSE;
   bool32          insert_completed_before_release  = FALSE;
   bool32          barrier_completed_before_release = FALSE;
   platform_status barrier_create_rc                = STATUS_INVALID_STATE;
   platform_status insert_create_rc                 = STATUS_INVALID_STATE;
   platform_status barrier_join_rc                  = STATUS_INVALID_STATE;
   platform_status insert_join_rc                   = STATUS_INVALID_STATE;

   checkpoint_barrier_fault_install(&data->checkpoint_fault, data->io);
   checkpoint_barrier_fault_block(&data->checkpoint_fault, 0);

   barrier_create_rc = platform_thread_create(&barrier_thread,
                                              FALSE,
                                              core_durable_barrier_test_thread,
                                              &barrier_args,
                                              data->hid);
   barrier_created   = SUCCESS(barrier_create_rc);
   if (barrier_created) {
      barrier_blocked =
         core_durable_barrier_test_wait(&data->checkpoint_fault.block_entered);
   }

   if (barrier_blocked) {
      insert_create_rc =
         platform_thread_create(&insert_thread,
                                FALSE,
                                core_durable_barrier_insert_thread,
                                &insert_args,
                                data->hid);
      insert_created = SUCCESS(insert_create_rc);
      if (insert_created) {
         insert_completed_before_release =
            core_durable_barrier_test_wait(&insert_args.done);
      }
      barrier_completed_before_release =
         __atomic_load_n(&barrier_args.done, __ATOMIC_ACQUIRE);
   }

   checkpoint_barrier_fault_release(&data->checkpoint_fault);
   if (insert_created) {
      insert_join_rc = platform_thread_join(&insert_thread);
   }
   if (barrier_created) {
      barrier_join_rc = platform_thread_join(&barrier_thread);
   }
   checkpoint_barrier_fault_uninstall(&data->checkpoint_fault);

   merge_accumulator_deinit(&msg);
   core_destroy(&spl);

   ASSERT_TRUE(SUCCESS(barrier_create_rc));
   ASSERT_TRUE(barrier_blocked,
               "core_durable_barrier did not reach the device barrier\n");
   ASSERT_TRUE(SUCCESS(insert_create_rc));
   ASSERT_TRUE(insert_completed_before_release,
               "insert did not finish while the device barrier was blocked\n");
   ASSERT_FALSE(barrier_completed_before_release,
                "core_durable_barrier returned before its device barrier\n");
   ASSERT_TRUE(SUCCESS(insert_join_rc));
   ASSERT_TRUE(SUCCESS(barrier_join_rc));
   ASSERT_TRUE(SUCCESS(insert_args.rc),
               "concurrent insert failed: %s\n",
               platform_status_to_string(insert_args.rc));
   ASSERT_TRUE(SUCCESS(barrier_args.rc),
               "core_durable_barrier failed: %s\n",
               platform_status_to_string(barrier_args.rc));
}

/*
 * Two barriers over the same write frontier share one group ticket.  Hold the
 * first caller at the device, wait until both begin calls have pinned their
 * tickets, then release them together.  The second caller must neither return
 * early nor issue a redundant device barrier.
 */
CTEST2(splinter, test_concurrent_durable_barriers_coalesce)
{
   allocator *alp                         = (allocator *)&data->al;
   data->system_cfg->splinter_cfg.use_log = TRUE;
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

   DECLARE_AUTO_KEY_BUFFER(keybuf, data->hid);
   merge_accumulator msg;
   merge_accumulator_init(&msg, data->hid);
   test_key(&keybuf, TEST_RANDOM, 1, 0, 0, data->workload_cfg->key_size, 0);
   generate_test_message(&data->gen, 1, &msg);
   rc = core_insert(
      &spl, key_buffer_key(&keybuf), merge_accumulator_to_message(&msg), NULL);
   ASSERT_TRUE(SUCCESS(rc));
   rc = task_perform_until_quiescent(spl.ts);
   ASSERT_TRUE(SUCCESS(rc));

   core_durable_barrier_thread_args first = {
      .spl = &spl,
      .rc  = STATUS_INVALID_STATE,
   };
   core_durable_barrier_thread_args second = {
      .spl = &spl,
      .rc  = STATUS_INVALID_STATE,
   };
   platform_thread first_thread                    = {0};
   platform_thread second_thread                   = {0};
   bool32          first_created                   = FALSE;
   bool32          second_created                  = FALSE;
   bool32          first_blocked                   = FALSE;
   bool32          both_tickets_pinned             = FALSE;
   bool32          second_completed_before_release = FALSE;
   platform_status first_create_rc                 = STATUS_INVALID_STATE;
   platform_status second_create_rc                = STATUS_INVALID_STATE;
   platform_status first_join_rc                   = STATUS_INVALID_STATE;
   platform_status second_join_rc                  = STATUS_INVALID_STATE;

   checkpoint_barrier_fault_install(&data->checkpoint_fault, data->io);
   checkpoint_barrier_fault_block(&data->checkpoint_fault, 0);

   first_create_rc = platform_thread_create(&first_thread,
                                            FALSE,
                                            core_durable_barrier_test_thread,
                                            &first,
                                            data->hid);
   first_created   = SUCCESS(first_create_rc);
   if (first_created) {
      first_blocked =
         core_durable_barrier_test_wait(&data->checkpoint_fault.block_entered);
   }

   if (first_blocked) {
      second_create_rc =
         platform_thread_create(&second_thread,
                                FALSE,
                                core_durable_barrier_test_thread,
                                &second,
                                data->hid);
      second_created = SUCCESS(second_create_rc);
      if (second_created) {
         both_tickets_pinned = core_durable_barrier_test_wait_for_ticket_refs(
            (shard_log *)spl.log, 2);
         second_completed_before_release =
            __atomic_load_n(&second.done, __ATOMIC_ACQUIRE);
      }
   }

   checkpoint_barrier_fault_release(&data->checkpoint_fault);
   if (second_created) {
      second_join_rc = platform_thread_join(&second_thread);
   }
   if (first_created) {
      first_join_rc = platform_thread_join(&first_thread);
   }
   uint64 device_barriers =
      __atomic_load_n(&data->checkpoint_fault.barriers, __ATOMIC_ACQUIRE);
   checkpoint_barrier_fault_uninstall(&data->checkpoint_fault);

   merge_accumulator_deinit(&msg);
   core_destroy(&spl);

   ASSERT_TRUE(SUCCESS(first_create_rc));
   ASSERT_TRUE(first_blocked,
               "first core_durable_barrier did not reach the device\n");
   ASSERT_TRUE(SUCCESS(second_create_rc));
   ASSERT_TRUE(both_tickets_pinned,
               "concurrent barriers did not both pin the in-flight ticket\n");
   ASSERT_FALSE(second_completed_before_release,
                "second barrier passed an undurable shared ticket\n");
   ASSERT_TRUE(SUCCESS(first_join_rc));
   ASSERT_TRUE(SUCCESS(second_join_rc));
   ASSERT_TRUE(SUCCESS(first.rc),
               "first core_durable_barrier failed: %s\n",
               platform_status_to_string(first.rc));
   ASSERT_TRUE(SUCCESS(second.rc),
               "second core_durable_barrier failed: %s\n",
               platform_status_to_string(second.rc));
   ASSERT_EQUAL(1,
                device_barriers,
                "coalesced barriers issued %lu device barriers\n",
                device_barriers);
}

/*
 * A checkpoint cut is not recoverable through its new live log until the
 * superblock durably names both the retired and live streams.  Block exactly
 * that publication barrier (the retired-log seal is the preceding barrier),
 * append to the already-installed live log, and start a durability barrier.
 * The live log may become durable independently, but the core barrier must not
 * return until the blocked cut publication completes.
 *
 * As in the other blocking-hook tests, collect predicates while workers are
 * live and make CTest assertions only after releasing the hook and joining
 * both workers.
 */
CTEST2(splinter, test_durable_barrier_waits_for_checkpoint_publication)
{
   allocator *alp                         = (allocator *)&data->al;
   data->system_cfg->splinter_cfg.use_log = TRUE;
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

   DECLARE_AUTO_KEY_BUFFER(keybuf, data->hid);
   merge_accumulator msg;
   merge_accumulator_init(&msg, data->hid);
   test_key(&keybuf, TEST_RANDOM, 1, 0, 0, data->workload_cfg->key_size, 0);
   generate_test_message(&data->gen, 1, &msg);
   rc = core_insert(
      &spl, key_buffer_key(&keybuf), merge_accumulator_to_message(&msg), NULL);
   ASSERT_TRUE(SUCCESS(rc));

   platform_mutex_lock(&spl.checkpoint_state_lock);
   uint64 publication_target = spl.checkpoint.publications + 1;
   platform_mutex_unlock(&spl.checkpoint_state_lock);

   core_checkpoint_thread_args checkpoint_args = {
      .spl = &spl,
      .rc  = STATUS_INVALID_STATE,
   };
   core_durable_barrier_thread_args barrier_args = {
      .spl = &spl,
      .rc  = STATUS_INVALID_STATE,
   };
   platform_thread checkpoint_thread = {0};
   platform_thread barrier_thread    = {0};

   bool32          checkpoint_created                  = FALSE;
   bool32          barrier_created                     = FALSE;
   bool32          publication_blocked                 = FALSE;
   bool32          observed_publishing                 = FALSE;
   bool32          retired_log_already_sealed          = FALSE;
   bool32          live_insert_succeeded               = FALSE;
   bool32          live_barrier_reached_device         = FALSE;
   bool32          live_log_durable_before_publication = FALSE;
   bool32          barrier_completed_before_release    = FALSE;
   bool32          checkpoint_completed_before_release = FALSE;
   bool32          publication_completed               = FALSE;
   platform_status checkpoint_create_rc                = STATUS_INVALID_STATE;
   platform_status barrier_create_rc                   = STATUS_INVALID_STATE;
   platform_status live_insert_rc                      = STATUS_INVALID_STATE;
   platform_status checkpoint_join_rc                  = STATUS_INVALID_STATE;
   platform_status barrier_join_rc                     = STATUS_INVALID_STATE;

   checkpoint_barrier_fault_install(&data->checkpoint_fault, data->io);
   /* Seal is barrier 0; cut publication is barrier 1. */
   checkpoint_barrier_fault_block(&data->checkpoint_fault, 1);

   checkpoint_create_rc = platform_thread_create(&checkpoint_thread,
                                                 FALSE,
                                                 core_checkpoint_test_thread,
                                                 &checkpoint_args,
                                                 data->hid);
   checkpoint_created   = SUCCESS(checkpoint_create_rc);
   if (checkpoint_created) {
      publication_blocked =
         core_durable_barrier_test_wait(&data->checkpoint_fault.block_entered);
   }

   if (publication_blocked) {
      platform_mutex_lock(&spl.checkpoint_state_lock);
      observed_publishing = spl.checkpoint.phase == CORE_CHECKPOINT_PUBLISHING;
      retired_log_already_sealed = spl.checkpoint.log_to_seal == NULL;
      platform_mutex_unlock(&spl.checkpoint_state_lock);
      checkpoint_completed_before_release =
         __atomic_load_n(&checkpoint_args.done, __ATOMIC_ACQUIRE);

      /* This update belongs to the new live log named by the blocked cut. */
      test_key(&keybuf, TEST_RANDOM, 2, 0, 0, data->workload_cfg->key_size, 0);
      generate_test_message(&data->gen, 2, &msg);
      live_insert_rc        = core_insert(&spl,
                                   key_buffer_key(&keybuf),
                                   merge_accumulator_to_message(&msg),
                                   NULL);
      live_insert_succeeded = SUCCESS(live_insert_rc);
   }

   if (live_insert_succeeded) {
      barrier_create_rc =
         platform_thread_create(&barrier_thread,
                                FALSE,
                                core_durable_barrier_test_thread,
                                &barrier_args,
                                data->hid);
      barrier_created = SUCCESS(barrier_create_rc);
      if (barrier_created) {
         /* Barriers 0 and 1 belong to the checkpoint; 2 is the live log. */
         live_barrier_reached_device =
            core_durable_barrier_test_wait_for_io_barriers(
               &data->checkpoint_fault, 3);
         if (live_barrier_reached_device) {
            live_log_durable_before_publication =
               core_durable_barrier_test_wait_for_live_log_durable(
                  (shard_log *)spl.log);
         }
         barrier_completed_before_release =
            __atomic_load_n(&barrier_args.done, __ATOMIC_ACQUIRE);
      }
   }

   checkpoint_barrier_fault_release(&data->checkpoint_fault);
   if (barrier_created) {
      barrier_join_rc = platform_thread_join(&barrier_thread);
   }
   if (checkpoint_created) {
      checkpoint_join_rc = platform_thread_join(&checkpoint_thread);
   }
   platform_mutex_lock(&spl.checkpoint_state_lock);
   publication_completed = spl.checkpoint.publications >= publication_target;
   platform_mutex_unlock(&spl.checkpoint_state_lock);
   checkpoint_barrier_fault_uninstall(&data->checkpoint_fault);

   merge_accumulator_deinit(&msg);
   core_destroy(&spl);

   ASSERT_TRUE(SUCCESS(checkpoint_create_rc));
   ASSERT_TRUE(publication_blocked,
               "checkpoint did not reach its cut-publication barrier\n");
   ASSERT_TRUE(observed_publishing,
               "checkpoint was not PUBLISHING at the blocked barrier\n");
   ASSERT_TRUE(retired_log_already_sealed,
               "blocked the retired-log seal rather than cut publication\n");
   ASSERT_FALSE(checkpoint_completed_before_release,
                "checkpoint returned before its publication barrier\n");
   ASSERT_TRUE(live_insert_succeeded,
               "insert into the new live log failed: %s\n",
               platform_status_to_string(live_insert_rc));
   ASSERT_TRUE(SUCCESS(barrier_create_rc));
   ASSERT_TRUE(live_barrier_reached_device,
               "core_durable_barrier did not reach the live-log barrier\n");
   ASSERT_TRUE(live_log_durable_before_publication,
               "the new live log did not become durable while publication "
               "was blocked\n");
   ASSERT_FALSE(barrier_completed_before_release,
                "core_durable_barrier returned before cut publication\n");
   ASSERT_TRUE(SUCCESS(barrier_join_rc));
   ASSERT_TRUE(SUCCESS(checkpoint_join_rc));
   ASSERT_TRUE(SUCCESS(barrier_args.rc),
               "core_durable_barrier failed: %s\n",
               platform_status_to_string(barrier_args.rc));
   ASSERT_TRUE(SUCCESS(checkpoint_args.rc),
               "core_checkpoint failed: %s\n",
               platform_status_to_string(checkpoint_args.rc));
   ASSERT_TRUE(publication_completed,
               "checkpoint publication counter did not advance\n");
}

typedef struct checkpoint_advance_fault_result {
   platform_status mkfs_rc;
   platform_status initial_quiesce_rc;
   platform_status failure_rc;
   platform_status failure_quiesce_rc;
   platform_status fill_rc;
   platform_status advance_rc;
   platform_status retry_rc;
   platform_status retry_quiesce_rc;

   log_head retired_log;
   uint64   barriers_after_failure;
   uint64   retired_ref_after_failure;
   uint64   retired_ref_after_retry;
   bool32   threshold_reached;

   superblock_tree_record failure_record;
   superblock_tree_record pre_advance_record;
   superblock_tree_record advance_record;
   superblock_tree_record retry_record;
} checkpoint_advance_fault_result;

/*
 * Run one bounded, persistent-looking barrier-failure/retry cycle without
 * making assertions while the io ops table is overridden.  This guarantees
 * that the override is kept alive until all checkpoint tasks are quiescent and
 * restored even when an observed result is not the expected one.
 */
static checkpoint_advance_fault_result
checkpoint_test_advance_retry(struct CTEST_IMPL_DATA_SNAME(splinter) * data,
                              uint64 skip_barriers,
                              bool32 exercise_chained_advance)
{
   checkpoint_advance_fault_result result;
   ZERO_STRUCT(result);

   allocator  *alp = (allocator *)&data->al;
   core_handle spl;

   result.mkfs_rc = core_mkfs(&spl,
                              &data->system_cfg->splinter_cfg,
                              alp,
                              (cache *)data->clock_cache,
                              data->io,
                              &data->tasks,
                              test_generate_allocator_root_id(),
                              data->hid);
   if (!SUCCESS(result.mkfs_rc)) {
      return result;
   }

   result.initial_quiesce_rc = task_perform_until_quiescent(spl.ts);
   result.retired_log        = log_get_head(spl.log);

   checkpoint_barrier_fault_install(&data->checkpoint_fault, data->io);
   checkpoint_barrier_fault_arm(&data->checkpoint_fault, skip_barriers);

   result.failure_rc = core_checkpoint(&spl, 0);
   /*
    * Leave the fault run armed while draining: a background
    * incorporation may be the caller which first reaches COMPLETING.
    */
   result.failure_quiesce_rc = task_perform_until_quiescent(spl.ts);
   superblock_get_tree_record(&spl.superblock, &result.failure_record);
   result.barriers_after_failure =
      __atomic_load_n(&data->checkpoint_fault.barriers, __ATOMIC_RELAXED);
   result.retired_ref_after_failure =
      allocator_get_refcount(alp, result.retired_log.meta_addr);

   if (exercise_chained_advance) {
      /* Give the threshold-crossing retry a fresh bounded failure budget. */
      checkpoint_barrier_fault_arm(&data->checkpoint_fault, 0);
      /*
       * The failed cut's generation was incorporated by the quiesce above.
       * While publication is still faulted, fill the current log just far
       * enough to cross its extent-based size threshold.  The crossing attempt
       * fails and leaves the threshold hint set.  After disabling the fault,
       * exactly one more insert supplies the retry opportunity.  Its
       * core_checkpoint_advance() call for this failed cut must both republish
       * it and notice that completion is already eligible; no later
       * incorporation edge is guaranteed to arrive.
       */
      platform_assert(spl.cfg.checkpoint_log_size_bytes != 0);
      DECLARE_AUTO_KEY_BUFFER(keybuf, data->hid);
      merge_accumulator msg;
      merge_accumulator_init(&msg, data->hid);
      for (uint64 i = 0;
           i < 30000
           && log_get_size(spl.log) < spl.cfg.checkpoint_log_size_bytes;
           i++)
      {
         test_key(
            &keybuf, TEST_RANDOM, 0, 0, 0, data->workload_cfg->key_size, 0);
         generate_test_message(&data->gen, i, &msg);
         result.fill_rc = core_insert(&spl,
                                      key_buffer_key(&keybuf),
                                      merge_accumulator_to_message(&msg),
                                      NULL);
         if (!SUCCESS(result.fill_rc)) {
            break;
         }
      }
      result.threshold_reached =
         log_get_size(spl.log) >= spl.cfg.checkpoint_log_size_bytes;
      superblock_get_tree_record(&spl.superblock, &result.pre_advance_record);

      checkpoint_barrier_fault_disable(&data->checkpoint_fault);
      test_key(&keybuf, TEST_RANDOM, 0, 0, 0, data->workload_cfg->key_size, 0);
      generate_test_message(&data->gen, 30000, &msg);
      result.advance_rc = core_insert(&spl,
                                      key_buffer_key(&keybuf),
                                      merge_accumulator_to_message(&msg),
                                      NULL);
      merge_accumulator_deinit(&msg);
      superblock_get_tree_record(&spl.superblock, &result.advance_record);
   } else {
      checkpoint_barrier_fault_disable(&data->checkpoint_fault);
   }

   result.retry_rc         = core_checkpoint(&spl, 0);
   result.retry_quiesce_rc = task_perform_until_quiescent(spl.ts);
   superblock_get_tree_record(&spl.superblock, &result.retry_record);
   result.retired_ref_after_retry =
      allocator_get_refcount(alp, result.retired_log.meta_addr);

   /* Restore the real ops before any CTest assertion can leave this scope. */
   checkpoint_barrier_fault_uninstall(&data->checkpoint_fault);
   core_destroy(&spl);
   return result;
}

/*
 * Let the retiring log's durability barrier succeed, then start a long run of
 * failures for barriers used to publish the log cut.  The durable record must
 * continue to name the retiring log as live.  Once that generation has already
 * been incorporated, one later core_checkpoint_advance() call must publish the
 * cut and immediately complete it rather than waiting for a vanished
 * incorporation edge.
 */
CTEST2(splinter, test_checkpoint_advance_retries_cut_publish_failure)
{
   data->system_cfg->splinter_cfg.use_log                   = TRUE;
   data->system_cfg->splinter_cfg.checkpoint_log_size_bytes = 1;

   checkpoint_advance_fault_result result =
      checkpoint_test_advance_retry(data, 1, TRUE);

   ASSERT_TRUE(SUCCESS(result.mkfs_rc));
   ASSERT_TRUE(SUCCESS(result.initial_quiesce_rc));
   ASSERT_TRUE(STATUS_IS_EQ(result.failure_rc, STATUS_IO_ERROR),
               "checkpoint returned %s instead of the injected IO error\n",
               platform_status_to_string(result.failure_rc));
   ASSERT_TRUE(SUCCESS(result.failure_quiesce_rc));
   ASSERT_TRUE(result.barriers_after_failure >= 2);
   ASSERT_TRUE(checkpoint_record_names_log(result.failure_record.live_log,
                                           result.retired_log));
   ASSERT_TRUE(SUPERBLOCK_NO_LOG(result.failure_record.sealed_log));
   ASSERT_NOT_EQUAL(0, result.retired_ref_after_failure);

   ASSERT_TRUE(SUCCESS(result.fill_rc));
   ASSERT_TRUE(result.threshold_reached);
   ASSERT_TRUE(checkpoint_record_names_log(result.pre_advance_record.live_log,
                                           result.retired_log));
   ASSERT_TRUE(SUPERBLOCK_NO_LOG(result.pre_advance_record.sealed_log));
   ASSERT_TRUE(SUCCESS(result.advance_rc));
   ASSERT_FALSE(SUPERBLOCK_NO_LOG(result.advance_record.live_log));
   ASSERT_FALSE(checkpoint_record_names_log(result.advance_record.live_log,
                                            result.retired_log));
   ASSERT_FALSE(checkpoint_record_names_log(result.advance_record.sealed_log,
                                            result.retired_log));

   ASSERT_TRUE(SUCCESS(result.retry_rc),
               "checkpoint retry failed: %s\n",
               platform_status_to_string(result.retry_rc));
   ASSERT_TRUE(SUCCESS(result.retry_quiesce_rc));
   ASSERT_FALSE(SUPERBLOCK_NO_LOG(result.retry_record.live_log));
   ASSERT_TRUE(SUPERBLOCK_NO_LOG(result.retry_record.sealed_log));
   ASSERT_FALSE(checkpoint_record_names_log(result.retry_record.live_log,
                                            result.retired_log));
}

/*
 * Once the cut is durable and its generation is incorporated, start a long run
 * of root durability-barrier failures in COMPLETING.  The sealed log must
 * remain reachable and referenced until a later advance call commits the root
 * successfully.
 */
CTEST2(splinter, test_checkpoint_advance_retries_completion_failure)
{
   data->system_cfg->splinter_cfg.use_log                   = TRUE;
   data->system_cfg->splinter_cfg.checkpoint_log_size_bytes = 0;

   checkpoint_advance_fault_result result =
      checkpoint_test_advance_retry(data, 2, FALSE);

   ASSERT_TRUE(SUCCESS(result.mkfs_rc));
   ASSERT_TRUE(SUCCESS(result.initial_quiesce_rc));
   ASSERT_TRUE(STATUS_IS_EQ(result.failure_rc, STATUS_IO_ERROR),
               "checkpoint returned %s instead of the injected IO error\n",
               platform_status_to_string(result.failure_rc));
   ASSERT_TRUE(SUCCESS(result.failure_quiesce_rc));
   ASSERT_TRUE(result.barriers_after_failure >= 3);
   ASSERT_TRUE(checkpoint_record_names_log(result.failure_record.sealed_log,
                                           result.retired_log));
   ASSERT_FALSE(checkpoint_record_names_log(result.failure_record.live_log,
                                            result.retired_log));
   ASSERT_NOT_EQUAL(0, result.retired_ref_after_failure);

   ASSERT_TRUE(SUCCESS(result.retry_rc),
               "checkpoint retry failed: %s\n",
               platform_status_to_string(result.retry_rc));
   ASSERT_TRUE(SUCCESS(result.retry_quiesce_rc));
   ASSERT_FALSE(SUPERBLOCK_NO_LOG(result.retry_record.live_log));
   ASSERT_TRUE(SUPERBLOCK_NO_LOG(result.retry_record.sealed_log));
   ASSERT_FALSE(checkpoint_record_names_log(result.retry_record.live_log,
                                            result.retired_log));
   ASSERT_EQUAL(0, result.retired_ref_after_retry);
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
      uint64 retired_meta_addr = rec.live_log.head.meta_addr;
      ASSERT_NOT_EQUAL(0, retired_meta_addr);

      rc = core_checkpoint(&spl, 0);
      ASSERT_TRUE(SUCCESS(rc));

      // (a) A cut happened: a different log is now live, and it covers only
      // generations from the cut onward.
      superblock_get_tree_record(&spl.superblock, &rec);
      ASSERT_NOT_EQUAL(retired_meta_addr, rec.live_log.head.meta_addr);
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

   superblock_tree_record initial_rec;
   superblock_get_tree_record(&spl.superblock, &initial_rec);

   uint64 num_inserts = splinter_do_inserts(data, &spl, FALSE, NULL);
   ASSERT_NOT_EQUAL(0, num_inserts);

   // Drain so any in-flight checkpoint completes and the state settles.
   rc = task_perform_until_quiescent(spl.ts);
   ASSERT_TRUE(SUCCESS(rc));

   if (log_size_threshold != 0) {
      superblock_tree_record rec;
      superblock_get_tree_record(&spl.superblock, &rec);

      // The inserts drove at least one complete automatic log rotation.
      ASSERT_FALSE(SUPERBLOCK_NO_LOG(rec.live_log));
      ASSERT_FALSE(
         checkpoint_records_name_same_log(initial_rec.live_log, rec.live_log));
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
   // Also verify that the public checkpoint statistic is updated.
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

   superblock_tree_record initial_rec;
   superblock_get_tree_record(&spl.superblock, &initial_rec);

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
   superblock_tree_record rec;
   superblock_get_tree_record(&spl.superblock, &rec);
   ASSERT_FALSE(SUPERBLOCK_NO_LOG(rec.live_log));
   ASSERT_FALSE(
      checkpoint_records_name_same_log(initial_rec.live_log, rec.live_log),
      "overwrite-only workload did not rotate the log; generation went %lu "
      "-> %lu\n",
      start_generation,
      memtable_generation(&spl.mt_ctxt));
   ASSERT_TRUE(SUPERBLOCK_NO_LOG(rec.sealed_log));

   /* The statistic is per-thread, so sum it across every registered thread. */
   uint64 reported = 0;
   for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      reported += spl.stats[thr_i].checkpoints_completed;
   }
   ASSERT_NOT_EQUAL(0,
                    reported,
                    "overwrite-only workload did not complete a checkpoint; "
                    "generation went %lu -> %lu\n",
                    start_generation,
                    memtable_generation(&spl.mt_ctxt));

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
   uint64    extent_size = acfg->io_cfg->extent_size;
   uint64    num_extents = allocator_get_capacity(alp) / extent_size;
   refcount *expected    = TYPED_ARRAY_MALLOC(data->hid, expected, num_extents);
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
   rc = rc_allocator_mount(
      &data->al, acfg, data->io, data->hid, platform_get_module_id());
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
 * A conservative reference-release failure can leave the live allocator map
 * usable but inexact.  Unmount must still succeed once the root is durable,
 * while refusing to bless that map as clean allocation state.  The next mount
 * then rebuilds it, after which an ordinary clean unmount may persist it again.
 *
 * trunk_snapshot_release() has no deterministic failure injection today, so
 * set the core's sticky result bit directly to exercise the shutdown/recovery
 * contract that such a failure triggers.
 */
CTEST2(splinter, test_unmount_skips_allocator_map_that_needs_rebuild)
{
   allocator        *alp     = (allocator *)&data->al;
   allocator_config *acfg    = allocator_get_config(alp);
   allocator_root_id root_id = test_generate_allocator_root_id();
   core_handle       created, recovered, cleanup;
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

   created.allocator_map_needs_rebuild = TRUE;
   rc                                  = core_unmount(&created, FALSE);
   ASSERT_TRUE(SUCCESS(rc));

   superblock_context sb;
   rc = superblock_context_init(&sb, data->io, acfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_TRUE(SUCCESS(superblock_mount(&sb, acfg)));
   ASSERT_FALSE(superblock_allocation_state_valid(&sb));
   superblock_context_deinit(&sb);

   rc = core_mount(&recovered,
                   &data->system_cfg->splinter_cfg,
                   alp,
                   (cache *)data->clock_cache,
                   data->io,
                   &data->tasks,
                   root_id,
                   data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_FALSE(recovered.allocator_map_needs_rebuild);

   lookup_result qdata;
   lookup_result_init(
      &qdata, recovered.cfg.data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   rc = core_lookup(&recovered, key_buffer_key(&keybuf), &qdata);
   ASSERT_TRUE(SUCCESS(rc));
   verify_tuple(&recovered,
                &data->gen,
                1,
                key_buffer_key(&keybuf),
                merge_accumulator_to_message(lookup_result_accumulator(&qdata)),
                TRUE);
   lookup_result_deinit(&qdata);

   rc = core_unmount(&recovered, FALSE);
   ASSERT_TRUE(SUCCESS(rc));

   rc = superblock_context_init(&sb, data->io, acfg, data->hid);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_TRUE(SUCCESS(superblock_mount(&sb, acfg)));
   ASSERT_TRUE(superblock_allocation_state_valid(&sb));
   superblock_context_deinit(&sb);

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
