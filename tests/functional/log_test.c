// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * log_test.c --
 *
 *     This file contains tests for Alex's log
 */
#include "platform_time.h"
#include "log.h"
#include "shard_log.h"
#include "platform_io.h"
#include "allocator.h"
#include "rc_allocator.h"
#include "cache.h"
#include "clockcache.h"
#include "core.h"
#include "test.h"

#include "poison.h"

#define LOG_TEST_LEAVES_PER_MEMTABLE 97

int
test_log_crash(clockcache             *cc,
               clockcache_config      *cache_cfg,
               io_handle              *io,
               allocator              *al,
               shard_log_config       *cfg,
               task_system            *ts,
               platform_heap_id        hid,
               test_message_generator *gen,
               uint64                  key_size,
               uint64                  num_entries,
               bool32                  crash)

{
   platform_status   rc;
   log_handle       *logh;
   uint64            i;
   key               returned_key;
   message           returned_message;
   log_head          segment;
   log_iterator     *itor;
   char              key_str[128];
   char              data_str[128];
   merge_accumulator msg;
   DECLARE_AUTO_KEY_BUFFER(keybuffer, hid);

   platform_assert(cc != NULL);
   logh = shard_log_create((cache *)cc, cfg, hid);
   platform_assert(logh != NULL);

   // The identity is fixed at creation; capture it before writing/sealing.
   segment = log_get_head(logh);

   merge_accumulator_init(&msg, hid);

   for (i = 0; i < num_entries; i++) {
      uint64 entry_num = i;
      if (2 * LOG_TEST_LEAVES_PER_MEMTABLE <= num_entries
          && i < 2 * LOG_TEST_LEAVES_PER_MEMTABLE)
      {
         /*
          * Write the first two generations in the opposite order, and each
          * generation in reverse leaf order.  The iterator must restore the
          * (memtable_generation, leaf_generation) order below.
          */
         uint64 memtable_generation = 1 - i / LOG_TEST_LEAVES_PER_MEMTABLE;
         uint64 leaf_generation =
            LOG_TEST_LEAVES_PER_MEMTABLE - 1 - i % LOG_TEST_LEAVES_PER_MEMTABLE;
         entry_num = memtable_generation * LOG_TEST_LEAVES_PER_MEMTABLE
                     + leaf_generation;
      }
      key skey = test_key(&keybuffer,
                          TEST_RANDOM,
                          entry_num,
                          0,
                          0,
                          1 + (entry_num % key_size),
                          0);
      generate_test_message(gen, entry_num, &msg);
      int log_rc = log_write(logh,
                             skey,
                             merge_accumulator_to_message(&msg),
                             entry_num / LOG_TEST_LEAVES_PER_MEMTABLE,
                             entry_num % LOG_TEST_LEAVES_PER_MEMTABLE);
      platform_assert(log_rc == 0);
   }

   rc = log_seal(logh); // identity captured above
   platform_assert_status_ok(rc);
   log_deinit(logh);
   rc = cache_writeback_dirty((cache *)cc);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier((cache *)cc);
   platform_assert_status_ok(rc);

   if (crash) {
      clockcache_deinit(cc);
      rc = clockcache_init(
         cc, cache_cfg, io, al, "crashed", hid, platform_get_module_id());
      platform_assert_status_ok(rc);
   }

   itor = shard_log_iterator_create((cache *)cc, cfg, hid, segment);
   platform_assert(itor != NULL);
   // The stream was sealed, so replay must be able to see that it is whole.
   platform_assert(log_iterator_stream_complete(itor));

   for (i = 0; i < num_entries && log_iterator_can_next(itor); i++) {
      key skey =
         test_key(&keybuffer, TEST_RANDOM, i, 0, 0, 1 + (i % key_size), 0);
      generate_test_message(gen, i, &msg);
      message mmessage = merge_accumulator_to_message(&msg);
      log_iterator_curr(itor, &returned_key, &returned_message);
      uint64 memtable_generation;
      uint64 leaf_generation;
      log_iterator_curr_generations(
         itor, &memtable_generation, &leaf_generation);
      platform_assert(memtable_generation == i / LOG_TEST_LEAVES_PER_MEMTABLE);
      platform_assert(leaf_generation == i % LOG_TEST_LEAVES_PER_MEMTABLE);
      if (data_key_compare(cfg->data_cfg, skey, returned_key)
          || message_lex_cmp(mmessage, returned_message))
      {
         platform_default_log("log_test_basic: key or data mismatch\n");
         data_key_to_string(cfg->data_cfg, skey, key_str, 128);
         data_message_to_string(cfg->data_cfg, mmessage, data_str, 128);
         platform_default_log("expected: %s -- %s\n", key_str, data_str);
         data_key_to_string(cfg->data_cfg, returned_key, key_str, 128);
         data_message_to_string(cfg->data_cfg, returned_message, data_str, 128);
         platform_default_log("actual: %s -- %s\n", key_str, data_str);
         platform_assert(0);
      }
      rc = log_iterator_next(itor);
      platform_assert_status_ok(rc);
   }

   platform_default_log("log returned %lu of %lu entries\n", i, num_entries);
   platform_assert(i == num_entries);
   platform_assert(!log_iterator_can_next(itor));

   merge_accumulator_deinit(&msg);

   log_iterator_deinit(itor);
   log_dec_ref((cache *)cc, &segment);

   return 0;
}

static void
test_log_write_range(log_handle             *logh,
                     test_message_generator *gen,
                     platform_heap_id        hid,
                     uint64                  key_size,
                     uint64                  first_entry,
                     uint64                  num_entries)
{
   merge_accumulator msg;
   DECLARE_AUTO_KEY_BUFFER(keybuffer, hid);
   merge_accumulator_init(&msg, hid);

   for (uint64 i = 0; i < num_entries; i++) {
      uint64 entry_num = first_entry + i;
      key    skey      = test_key(&keybuffer,
                          TEST_RANDOM,
                          entry_num,
                          0,
                          0,
                          1 + (entry_num % key_size),
                          0);
      generate_test_message(gen, entry_num, &msg);
      int log_rc = log_write(
         logh, skey, merge_accumulator_to_message(&msg), entry_num, 0);
      platform_assert(log_rc == 0);
   }

   merge_accumulator_deinit(&msg);
}

static void
test_log_verify_segment(cache                  *cc,
                        shard_log_config       *cfg,
                        const log_head         *segment,
                        test_message_generator *gen,
                        platform_heap_id        hid,
                        uint64                  key_size,
                        uint64                  first_entry,
                        uint64                  num_entries)
{
   log_iterator     *itor;
   merge_accumulator msg;
   DECLARE_AUTO_KEY_BUFFER(keybuffer, hid);
   key     returned_key;
   message returned_message;

   platform_assert(segment->addr != 0);
   platform_assert(segment->meta_addr != 0);
   itor = shard_log_iterator_create(cc, cfg, hid, *segment);
   platform_assert(itor != NULL);
   platform_assert(log_iterator_stream_complete(itor));

   merge_accumulator_init(&msg, hid);
   for (uint64 i = 0; i < num_entries; i++) {
      uint64 entry_num = first_entry + i;
      platform_assert(log_iterator_can_next(itor));
      key skey = test_key(&keybuffer,
                          TEST_RANDOM,
                          entry_num,
                          0,
                          0,
                          1 + (entry_num % key_size),
                          0);
      generate_test_message(gen, entry_num, &msg);
      log_iterator_curr(itor, &returned_key, &returned_message);
      uint64 memtable_generation;
      uint64 leaf_generation;
      log_iterator_curr_generations(
         itor, &memtable_generation, &leaf_generation);
      platform_assert(memtable_generation == entry_num);
      platform_assert(leaf_generation == 0);
      platform_assert(data_key_compare(cfg->data_cfg, skey, returned_key) == 0);
      platform_assert(
         message_lex_cmp(merge_accumulator_to_message(&msg), returned_message)
         == 0);
      platform_status rc = log_iterator_next(itor);
      platform_assert_status_ok(rc);
   }
   platform_assert(!log_iterator_can_next(itor));

   merge_accumulator_deinit(&msg);
   log_iterator_deinit(itor);
}

/*
 * Sealing a stream and creating a fresh one must yield two distinct,
 * independently replayable segments. Reinitializing the cache after each forced
 * physical persistence cut makes this test exercise only persisted pages for
 * both identities; it does not model logical durable-tail publication.
 */
static int
test_log_two_segments(clockcache             *cc,
                      clockcache_config      *cache_cfg,
                      io_handle              *io,
                      allocator              *al,
                      shard_log_config       *cfg,
                      platform_heap_id        hid,
                      test_message_generator *gen,
                      uint64                  key_size)
{
   const uint64 old_first = 1000, old_count = 16;
   const uint64 new_first = 2000, new_count = 16;
   log_head     sealed, fresh;

   log_handle *log = shard_log_create((cache *)cc, cfg, hid);
   platform_assert(log != NULL);
   sealed = log_get_head(log); // identity is fixed at creation
   test_log_write_range(log, gen, hid, key_size, old_first, old_count);

   platform_status rc = log_seal(log);
   platform_assert_status_ok(rc);
   log_deinit(log);
   platform_assert(sealed.addr != 0);
   platform_assert(sealed.meta_addr != 0);

   rc = cache_writeback_dirty((cache *)cc);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier((cache *)cc);
   platform_assert_status_ok(rc);

   clockcache_deinit(cc);
   rc = clockcache_init(
      cc, cache_cfg, io, al, "sealed-old", hid, platform_get_module_id());
   platform_assert_status_ok(rc);
   test_log_verify_segment(
      (cache *)cc, cfg, &sealed, gen, hid, key_size, old_first, old_count);

   // A fresh stream is a distinct segment: new mini allocator and new magic.
   log = shard_log_create((cache *)cc, cfg, hid);
   platform_assert(log != NULL);
   fresh = log_get_head(log);
   test_log_write_range(log, gen, hid, key_size, new_first, new_count);
   rc = log_seal(log);
   platform_assert_status_ok(rc);
   log_deinit(log);
   platform_assert(fresh.addr != 0);
   platform_assert(fresh.meta_addr != 0);
   platform_assert(sealed.meta_addr != fresh.meta_addr);
   platform_assert(sealed.magic != fresh.magic);

   rc = cache_writeback_dirty((cache *)cc);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier((cache *)cc);
   platform_assert_status_ok(rc);

   clockcache_deinit(cc);
   rc = clockcache_init(
      cc, cache_cfg, io, al, "sealed-new", hid, platform_get_module_id());
   platform_assert_status_ok(rc);
   test_log_verify_segment(
      (cache *)cc, cfg, &sealed, gen, hid, key_size, old_first, old_count);
   test_log_verify_segment(
      (cache *)cc, cfg, &fresh, gen, hid, key_size, new_first, new_count);

   log_dec_ref((cache *)cc, &sealed);
   log_dec_ref((cache *)cc, &fresh);
   return 0;
}

static int
test_log_large_message(cache *cc, shard_log_config *cfg, platform_heap_id hid)
{
   platform_status   rc;
   log_head          sealed;
   log_iterator     *itor;
   merge_accumulator msg;
   key               returned_key;
   message           returned_message;
   char              key_data[] = "large-log-key";
   key               skey = key_create(FALSE, sizeof(key_data) - 1, key_data);
   uint64            value_len = 3 * cache_page_size(cc) + 123;

   log_handle *logh = shard_log_create(cc, cfg, hid);
   platform_assert(logh != NULL);
   sealed = log_get_head(logh); // identity is fixed at creation

   merge_accumulator_init(&msg, hid);
   bool32 success = merge_accumulator_resize(&msg, value_len);
   platform_assert(success);
   merge_accumulator_set_class(&msg, MESSAGE_TYPE_INSERT);
   memset(merge_accumulator_data(&msg), 'L', value_len);

   int log_rc = log_write(logh, skey, merge_accumulator_to_message(&msg), 0, 0);
   platform_assert(log_rc == 0);

   merge_accumulator filler;
   merge_accumulator_init(&filler, hid);
   success = merge_accumulator_resize(&filler, cache_page_size(cc) / 4);
   platform_assert(success);
   merge_accumulator_set_class(&filler, MESSAGE_TYPE_INSERT);
   memset(
      merge_accumulator_data(&filler), 'f', merge_accumulator_length(&filler));
   for (uint64 i = 1; i < 16; i++) {
      log_rc = log_write(
         logh, skey, merge_accumulator_to_message(&filler), i / 4, i % 4);
      platform_assert(log_rc == 0);
   }
   merge_accumulator_deinit(&filler);

   rc = log_seal(logh); // identity captured above
   platform_assert_status_ok(rc);
   log_deinit(logh);
   rc = cache_writeback_dirty(cc);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier(cc);
   platform_assert_status_ok(rc);

   itor = shard_log_iterator_create(cc, cfg, hid, sealed);
   platform_assert(itor != NULL);
   platform_assert(log_iterator_stream_complete(itor));
   platform_assert(log_iterator_can_next(itor));

   log_iterator_curr(itor, &returned_key, &returned_message);
   platform_assert(data_key_compare(cfg->data_cfg, skey, returned_key) == 0);
   platform_assert(
      message_lex_cmp(merge_accumulator_to_message(&msg), returned_message)
      == 0);

   log_iterator_deinit(itor);
   merge_accumulator_deinit(&msg);
   log_dec_ref(cc, &sealed);
   return 0;
}

typedef struct test_log_thread_params {
   log_handle             *logh;
   platform_thread         thread;
   int                     thread_id;
   test_message_generator *gen;
   uint64                  key_size;
   uint64                  num_entries;
} test_log_thread_params;

void
test_log_thread(void *arg)
{
   platform_heap_id        hid         = platform_get_heap_id();
   test_log_thread_params *params      = (test_log_thread_params *)arg;
   log_handle             *logh        = params->logh;
   int                     thread_id   = params->thread_id;
   uint64                  num_entries = params->num_entries;
   test_message_generator *gen         = params->gen;
   uint64                  key_size    = params->key_size;
   uint64                  i;
   merge_accumulator       msg;
   DECLARE_AUTO_KEY_BUFFER(keybuf, hid);

   merge_accumulator_init(&msg, hid);

   for (i = thread_id * num_entries; i < (thread_id + 1) * num_entries; i++) {
      key skey = test_key(&keybuf, TEST_RANDOM, i, 0, 0, key_size, 0);
      generate_test_message(gen, i, &msg);
      int log_rc = log_write(
         logh, skey, merge_accumulator_to_message(&msg), i / 1024, i % 1024);
      platform_assert(log_rc == 0);
   }

   merge_accumulator_deinit(&msg);
}

platform_status
test_log_perf(cache                  *cc,
              shard_log_config       *cfg,
              uint64                  num_entries,
              test_message_generator *gen,
              uint64                  key_size,
              uint64                  num_threads,
              task_system            *ts,
              platform_heap_id        hid)

{
   test_log_thread_params *params =
      TYPED_ARRAY_MALLOC(hid, params, num_threads);
   platform_assert(params);
   uint64          start_time;
   platform_status ret;

   log_handle *logh = shard_log_create((cache *)cc, cfg, hid);
   platform_assert(logh != NULL);
   log_head sealed = log_get_head(logh);

   for (uint64 i = 0; i < num_threads; i++) {
      params[i].logh        = logh;
      params[i].thread_id   = i;
      params[i].gen         = gen;
      params[i].key_size    = key_size;
      params[i].num_entries = num_entries / num_threads;
   }

   start_time = platform_get_timestamp();
   for (uint64 i = 0; i < num_threads; i++) {
      ret = platform_thread_create(
         &params[i].thread, FALSE, test_log_thread, &params[i], hid);
      if (!SUCCESS(ret)) {
         // Wait for existing threads to quit
         for (uint64 j = 0; j < i; j++) {
            platform_thread_join(&params[j].thread);
         }
         goto cleanup;
      }
   }
   for (uint64 i = 0; i < num_threads; i++) {
      platform_thread_join(&params[i].thread);
   }

   platform_default_log("log insertion rate: %luM insertions/second\n",
                        SEC_TO_MSEC(num_entries)
                           / platform_timestamp_elapsed(start_time));

cleanup:
   // Finish the stream, free the handle, and release the segment's extents.
   platform_assert_status_ok(log_seal(logh));
   log_deinit(logh);
   log_dec_ref((cache *)cc, &sealed);
   platform_free(hid, params);

   return ret;
}


static void
usage(const char *argv0)
{
   platform_error_log("Usage:\n"
                      "\t%s\n"
                      "\t%s --perf\n"
                      "\t%s --crash\n",
                      argv0,
                      argv0,
                      argv0);
   config_usage();
}

int
log_test(int argc, char *argv[])
{
   platform_status        status;
   system_config          system_cfg;
   rc_allocator           al;
   platform_status        ret;
   int                    config_argc;
   char                 **config_argv;
   bool32                 run_perf_test;
   bool32                 run_crash_test;
   int                    rc;
   uint64                 seed;
   task_system            ts;
   test_message_generator gen;

   platform_register_thread();

   if (argc > 1 && strncmp(argv[1], "--perf", sizeof("--perf")) == 0) {
      run_perf_test  = TRUE;
      run_crash_test = FALSE;
      config_argc    = argc - 2;
      config_argv    = argv + 2;
   } else if (argc > 1 && strncmp(argv[1], "--crash", sizeof("--crash")) == 0) {
      run_perf_test  = FALSE;
      run_crash_test = TRUE;
      config_argc    = argc - 2;
      config_argv    = argv + 2;
   } else {
      run_perf_test  = FALSE;
      run_crash_test = FALSE;
      config_argc    = argc - 1;
      config_argv    = argv + 1;
   }

   bool use_shmem = config_parse_use_shmem(config_argc, config_argv);
   platform_default_log("\nStarted log_test%s!!\n",
                        (use_shmem ? " using shared memory" : ""));

   // Create a heap for io, allocator, cache and splinter
   platform_heap_id hid = NULL;
   status               = platform_heap_create(
      platform_get_module_id(), 512 * MiB, use_shmem, &hid);
   platform_assert_status_ok(status);

   core_config         *cfg                            = TYPED_MALLOC(hid, cfg);
   uint64               num_bg_threads[NUM_TASK_TYPES] = {0}; // no bg threads
   test_workload_config workload_cfg;

   status = test_parse_args(&system_cfg,
                            &workload_cfg,
                            &seed,
                            &gen,
                            &num_bg_threads[TASK_TYPE_MEMTABLE],
                            &num_bg_threads[TASK_TYPE_NORMAL],
                            config_argc,
                            config_argv);
   if (!SUCCESS(status)) {
      platform_error_log("log_test: failed to parse config: %s\n",
                         platform_status_to_string(status));
      /*
       * Provided arguments but set things up incorrectly.
       * Print usage so client can fix commandline.
       */
      usage(argv[0]);
      rc = -1;
      goto cleanup;
   }

   io_handle *io = io_handle_create(&system_cfg.io_cfg, hid);
   if (io == NULL) {
      platform_error_log("Failed to create IO handle\n");
      rc = -1;
      goto cleanup;
   }

   status = test_init_task_system(&ts, hid, &system_cfg.task_cfg);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init splinter state: %s\n",
                         platform_status_to_string(status));
      rc = -1;
      goto destroy_iohandle;
   }

   status = rc_allocator_init(
      &al, &system_cfg.allocator_cfg, io, hid, platform_get_module_id());
   platform_assert_status_ok(status);

   clockcache *cc = TYPED_MALLOC(hid, cc);
   platform_assert(cc != NULL);
   status = clockcache_init(cc,
                            &system_cfg.cache_cfg,
                            io,
                            (allocator *)&al,
                            "test",
                            hid,
                            platform_get_module_id());
   platform_assert_status_ok(status);

   rc = test_log_large_message((cache *)cc, &system_cfg.log_cfg, hid);
   platform_assert(rc == 0);

   rc = test_log_two_segments(cc,
                              &system_cfg.cache_cfg,
                              io,
                              (allocator *)&al,
                              &system_cfg.log_cfg,
                              hid,
                              &gen,
                              workload_cfg.key_size);
   platform_assert(rc == 0);

   if (run_perf_test) {
      ret = test_log_perf((cache *)cc,
                          &system_cfg.log_cfg,
                          200000000,
                          &gen,
                          workload_cfg.key_size,
                          16,
                          &ts,
                          hid);
      platform_assert_status_ok(ret);
      rc = 0;
   } else if (run_crash_test) {
      rc = test_log_crash(cc,
                          &system_cfg.cache_cfg,
                          io,
                          (allocator *)&al,
                          &system_cfg.log_cfg,
                          &ts,
                          hid,
                          &gen,
                          workload_cfg.key_size,
                          500000,
                          TRUE /* crash */);
      platform_assert(rc == 0);
   } else {
      rc = test_log_crash(cc,
                          &system_cfg.cache_cfg,
                          io,
                          (allocator *)&al,
                          &system_cfg.log_cfg,
                          &ts,
                          hid,
                          &gen,
                          workload_cfg.key_size,
                          500000,
                          FALSE /* don't crash */);
      platform_assert(rc == 0);
   }

   io_wait_all(io);
   clockcache_deinit(cc);
   platform_free(hid, cc);
   rc_allocator_deinit(&al);
   test_deinit_task_system(&ts);
destroy_iohandle:
   io_handle_destroy(io);
cleanup:
   platform_free(hid, cfg);
   platform_heap_destroy(&hid);
   platform_deregister_thread();
   return rc == 0 ? 0 : -1;
}
