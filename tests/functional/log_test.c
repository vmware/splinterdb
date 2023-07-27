// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * log_test.c --
 *
 *     This file contains tests for Alex's log
 */
#include "platform.h"

#include "log.h"
#include "shard_log.h"
#include "io.h"
#include "allocator.h"
#include "rc_allocator.h"
#include "cache.h"
#include "clockcache.h"
#include "trunk.h"
#include "test.h"

#include "poison.h"

int
test_log_crash(clockcache             *cc,
               clockcache_config      *cache_cfg,
               io_handle              *io,
               allocator              *al,
               shard_log_config       *cfg,
               shard_log              *log,
               task_system            *ts,
               platform_heap_id        hid,
               test_message_generator *gen,
               uint64                  num_entries,
               bool32                  crash)

{
   platform_status    rc;
   log_handle        *logh;
   uint64             i;
   key                returned_key;
   message            returned_message;
   uint64             addr;
   uint64             magic;
   shard_log_iterator itor;
   iterator          *itorh = (iterator *)&itor;
   char               key_str[128];
   char               data_str[128];
   merge_accumulator  msg;
   DECLARE_AUTO_KEY_BUFFER(keybuffer, hid);

   platform_assert(cc != NULL);
   rc = shard_log_init(log, (cache *)cc, cfg);
   platform_assert_status_ok(rc);
   logh = (log_handle *)log;

   addr  = log_addr(logh);
   magic = log_magic(logh);

   merge_accumulator_init(&msg, hid);

   for (i = 0; i < num_entries; i++) {
      key skey = test_key(&keybuffer,
                          TEST_RANDOM,
                          i,
                          0,
                          0,
                          1 + (i % cfg->data_cfg->max_key_size),
                          0);
      generate_test_message(gen, i, &msg);
      log_write(logh, skey, merge_accumulator_to_message(&msg), i);
   }

   if (crash) {
      clockcache_deinit(cc);
      rc = clockcache_init(
         cc, cache_cfg, io, al, "crashed", hid, platform_get_module_id());
      platform_assert_status_ok(rc);
   }

   rc = shard_log_iterator_init((cache *)cc, cfg, hid, addr, magic, &itor);
   platform_assert_status_ok(rc);
   itorh = (iterator *)&itor;

   for (i = 0; i < num_entries && iterator_can_curr(itorh); i++) {
      key skey = test_key(&keybuffer,
                          TEST_RANDOM,
                          i,
                          0,
                          0,
                          1 + (i % cfg->data_cfg->max_key_size),
                          0);
      generate_test_message(gen, i, &msg);
      message mmessage = merge_accumulator_to_message(&msg);
      iterator_curr(itorh, &returned_key, &returned_message);
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
      rc = iterator_next(itorh);
      platform_assert_status_ok(rc);
   }

   platform_default_log("log returned %lu of %lu entries\n", i, num_entries);

   merge_accumulator_deinit(&msg);

   shard_log_iterator_deinit(hid, &itor);
   shard_log_zap(log);

   return 0;
}

typedef struct test_log_thread_params {
   shard_log              *log;
   platform_thread         thread;
   int                     thread_id;
   test_message_generator *gen;
   uint64                  num_entries;
} test_log_thread_params;

void
test_log_thread(void *arg)
{
   platform_heap_id        hid         = platform_get_heap_id();
   test_log_thread_params *params      = (test_log_thread_params *)arg;
   shard_log              *log         = params->log;
   log_handle             *logh        = (log_handle *)log;
   int                     thread_id   = params->thread_id;
   uint64                  num_entries = params->num_entries;
   test_message_generator *gen         = params->gen;
   uint64                  i;
   merge_accumulator       msg;
   DECLARE_AUTO_KEY_BUFFER(keybuf, hid);

   merge_accumulator_init(&msg, hid);

   for (i = thread_id * num_entries; i < (thread_id + 1) * num_entries; i++) {
      key skey = test_key(
         &keybuf, TEST_RANDOM, i, 0, 0, log->cfg->data_cfg->max_key_size, 0);
      generate_test_message(gen, i, &msg);
      log_write(logh, skey, merge_accumulator_to_message(&msg), i);
   }

   merge_accumulator_deinit(&msg);
}

platform_status
test_log_perf(cache                  *cc,
              shard_log_config       *cfg,
              shard_log              *log,
              uint64                  num_entries,
              test_message_generator *gen,
              uint64                  num_threads,
              task_system            *ts,
              platform_heap_id        hid)

{
   test_log_thread_params *params =
      TYPED_ARRAY_MALLOC(hid, params, num_threads);
   platform_assert(params);
   uint64          start_time;
   platform_status ret;

   ret = shard_log_init(log, (cache *)cc, cfg);
   platform_assert_status_ok(ret);

   for (uint64 i = 0; i < num_threads; i++) {
      params[i].log         = log;
      params[i].thread_id   = i;
      params[i].gen         = gen;
      params[i].num_entries = num_entries / num_threads;
   }

   start_time = platform_get_timestamp();
   for (uint64 i = 0; i < num_threads; i++) {
      ret = task_thread_create("log_thread",
                               test_log_thread,
                               &params[i],
                               0,
                               ts,
                               hid,
                               &params[i].thread);
      if (!SUCCESS(ret)) {
         // Wait for existing threads to quit
         for (uint64 j = 0; j < i; j++) {
            platform_thread_join(params[i].thread);
         }
         goto cleanup;
      }
   }
   for (uint64 i = 0; i < num_threads; i++) {
      platform_thread_join(params[i].thread);
   }

   platform_default_log("log insertion rate: %luM insertions/second\n",
                        SEC_TO_MSEC(num_entries)
                           / platform_timestamp_elapsed(start_time));

cleanup:
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
   data_config           *data_cfg;
   io_config              io_cfg;
   allocator_config       al_cfg;
   clockcache_config      cache_cfg;
   shard_log_config       log_cfg;
   task_system_config     task_cfg;
   rc_allocator           al;
   platform_status        ret;
   int                    config_argc;
   char                 **config_argv;
   bool32                 run_perf_test;
   bool32                 run_crash_test;
   int                    rc;
   uint64                 seed;
   task_system           *ts = NULL;
   test_message_generator gen;

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

   platform_default_log("\nStarted log_test!!\n");

   // Create a heap for io, allocator, cache and splinter
   platform_heap_handle hh;
   platform_heap_id     hid;
   status = platform_heap_create(platform_get_module_id(), 1 * GiB, &hh, &hid);
   platform_assert_status_ok(status);

   trunk_config *cfg                            = TYPED_MALLOC(hid, cfg);
   uint64        num_bg_threads[NUM_TASK_TYPES] = {0}; // no bg threads

   status = test_parse_args(cfg,
                            &data_cfg,
                            &io_cfg,
                            &al_cfg,
                            &cache_cfg,
                            &log_cfg,
                            &task_cfg,
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

   platform_io_handle *io = TYPED_MALLOC(hid, io);
   platform_assert(io != NULL);
   status = io_handle_init(io, &io_cfg, hh, hid);
   if (!SUCCESS(status)) {
      rc = -1;
      goto free_iohandle;
   }

   status = test_init_task_system(hid, io, &ts, &task_cfg);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init splinter state: %s\n",
                         platform_status_to_string(status));
      rc = -1;
      goto deinit_iohandle;
   }

   status = rc_allocator_init(
      &al, &al_cfg, (io_handle *)io, hid, platform_get_module_id());
   platform_assert_status_ok(status);

   clockcache *cc = TYPED_MALLOC(hid, cc);
   platform_assert(cc != NULL);
   status = clockcache_init(cc,
                            &cache_cfg,
                            (io_handle *)io,
                            (allocator *)&al,
                            "test",
                            hid,
                            platform_get_module_id());
   platform_assert_status_ok(status);

   shard_log *log = TYPED_MALLOC(hid, log);
   platform_assert(log != NULL);
   if (run_perf_test) {
      ret = test_log_perf(
         (cache *)cc, &log_cfg, log, 200000000, &gen, 16, ts, hid);
      rc = -1;
      platform_assert_status_ok(ret);
   } else if (run_crash_test) {
      rc = test_log_crash(cc,
                          &cache_cfg,
                          (io_handle *)io,
                          (allocator *)&al,
                          &log_cfg,
                          log,
                          ts,
                          hid,
                          &gen,
                          500000,
                          TRUE /* crash */);
      platform_assert(rc == 0);
   } else {
      rc = test_log_crash(cc,
                          &cache_cfg,
                          (io_handle *)io,
                          (allocator *)&al,
                          &log_cfg,
                          log,
                          ts,
                          hid,
                          &gen,
                          500000,
                          FALSE /* don't crash */);
      platform_assert(rc == 0);
   }

   clockcache_deinit(cc);
   platform_free(hid, log);
   platform_free(hid, cc);
   rc_allocator_deinit(&al);
   test_deinit_task_system(hid, &ts);
deinit_iohandle:
   io_handle_deinit(io);
free_iohandle:
   platform_free(hid, io);
cleanup:
   platform_free(hid, cfg);
   platform_heap_destroy(&hh);

   return rc == 0 ? 0 : -1;
}
