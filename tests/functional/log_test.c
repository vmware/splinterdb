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

// Function prototypes
void
test_log_thread(void *arg);

int
test_log_crash(clockcache             *cc,
               clockcache_config      *cache_cfg,
               io_handle              *io,
               allocator              *al,
               shard_log_config       *cfg,
               shard_log              *log,
               task_system            *ts,
               platform_heap_handle    hh,
               platform_heap_id        hid,
               test_message_generator *gen,
               uint64                  num_entries,
               bool                    crash)

{
   platform_status    rc;
   log_handle        *logh;
   uint64             i;
   char               keybuffer[MAX_KEY_SIZE];
   slice              returned_key;
   message            returned_message;
   uint64             addr;
   uint64             magic;
   shard_log_iterator itor;
   iterator          *itorh = (iterator *)&itor;
   char               key_str[128];
   char               data_str[128];
   bool               at_end;
   merge_accumulator  msg;

   platform_assert(cc != NULL);
   rc = shard_log_init(log, (cache *)cc, cfg);
   platform_assert_status_ok(rc);
   logh = (log_handle *)log;

   addr  = log_addr(logh);
   magic = log_magic(logh);

   merge_accumulator_init(&msg, hid);

   for (i = 0; i < num_entries; i++) {
      test_key(keybuffer, TEST_RANDOM, i, 0, 0, cfg->data_cfg->key_size, 0);
      generate_test_message(gen, i, &msg);
      slice skey = slice_create(1 + (i % cfg->data_cfg->key_size), keybuffer);
      log_write(logh, skey, merge_accumulator_to_message(&msg), i);
   }

   if (crash) {
      clockcache_deinit(cc);
      rc = clockcache_init(
         cc, cache_cfg, io, al, "crashed", hh, hid, platform_get_module_id());
      platform_assert_status_ok(rc);
   }

   rc = shard_log_iterator_init((cache *)cc, cfg, hid, addr, magic, &itor);
   platform_assert_status_ok(rc);
   itorh = (iterator *)&itor;

   iterator_at_end(itorh, &at_end);
   for (i = 0; i < num_entries && !at_end; i++) {
      test_key(keybuffer, TEST_RANDOM, i, 0, 0, cfg->data_cfg->key_size, 0);
      generate_test_message(gen, i, &msg);
      slice   skey = slice_create(1 + (i % cfg->data_cfg->key_size), keybuffer);
      message mmessage = merge_accumulator_to_message(&msg);
      iterator_get_curr(itorh, &returned_key, &returned_message);
      if (slice_lex_cmp(skey, returned_key)
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
      iterator_advance(itorh);
      iterator_at_end(itorh, &at_end);
   }

   platform_default_log("log returned %lu of %lu entries\n", i, num_entries);

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
   uint64 commit_every_n;   // sync-write log page every n-entries.
   uint64 num_commits;      // # of times we COMMIT'ed the xact
   uint64 num_bytes_logged; // Sum of (key, message)-lengths logged
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
   char                    key[MAX_KEY_SIZE];
   merge_accumulator       msg;

   slice skey = slice_create(log->cfg->data_cfg->key_size, key);
   merge_accumulator_init(&msg, hid);

   uint64 num_commits      = 0;
   uint64 num_bytes_logged = 0;

   for (i = thread_id * num_entries; i < (thread_id + 1) * num_entries; i++) {
      test_key(key, TEST_RANDOM, i, 0, 0, log->cfg->data_cfg->key_size, 0);
      generate_test_message(gen, i, &msg);
      log_write(logh, skey, merge_accumulator_to_message(&msg), i);
      num_bytes_logged += slice_length(skey)
                          + message_length(merge_accumulator_to_message(&msg));
      if (params->commit_every_n && ((i + 1) % params->commit_every_n) == 0) {
         log_commit(logh);
         num_commits++;
      }
   }

   merge_accumulator_deinit(&msg);
   // Return per-thread metrics on amount of data logged
   params->num_commits      = num_commits;
   params->num_bytes_logged = num_bytes_logged;
}

/* Generate log performance summary metrics line */
void
gen_log_perf_summary(test_log_thread_params *params,
                     uint64                  num_entries,
                     uint64                  num_threads,
                     uint64                  elapsed_ns,
                     uint64                  commit_freq,
                     uint64                  xlog_page_size)
{
   uint64 elapsed_s = NSEC_TO_SEC(elapsed_ns);
   if (elapsed_s == 0) {
      elapsed_s = 1;
   }

   uint64 rate_per_sec = (num_entries * BILLION) / elapsed_ns;

   platform_default_log("%lu threads inserted %lu (%lu M) log entries in "
                        "%lu ns (%lu.%lu s)"
                        ", committing every %lu log entries. "
                        "(rate_per_sec=%lu)."
                        " Log insertion rate: %lu.%lu M insertions/second\n",
                        num_threads,
                        num_entries,
                        N_TO_MILLION(num_entries),
                        elapsed_ns,
                        NSEC_TO_SEC(elapsed_ns),
                        N_TO_B_FRACT(elapsed_ns),
                        commit_freq,
                        rate_per_sec,
                        N_TO_MILLION(rate_per_sec),
                        N_TO_M_FRACT(rate_per_sec));
   platform_default_log("\nPer-Thread logging activity summary:\n");
   // clang-format off
   const char *dashes = "---------------------------------------------------------------------------";

   platform_default_log("%s\n", dashes);
   platform_default_log("Thread      Num              Num Bytes Logged      Ave Transaction Size\n");
   platform_default_log("  ID       Commits                                    Bytes       Pages\n");
   platform_default_log("%s\n", dashes);
   // clang-format on

   uint64                  sum_ncommits      = 0;
   uint64                  sum_nbytes_logged = 0;
   test_log_thread_params *threadp           = params;

   char outbuf[SIZE_TO_STR_LEN];
   for (uint64 tctr = 0; tctr < num_threads; tctr++, threadp++) {
      uint64   ave_bytes_per_xact = 0;
      fraction ave_nlog_pages_per_xact;
      if (threadp->num_commits) {
         ave_bytes_per_xact =
            (threadp->num_bytes_logged / threadp->num_commits);
         ave_nlog_pages_per_xact =
            init_fraction(ave_bytes_per_xact, xlog_page_size);
      }

      // clang-format off
      size_to_str(outbuf, sizeof(outbuf), threadp->num_bytes_logged);
      platform_default_log("  %-4lu        %-8lu      %-10lu %-14s  %-8lu  " FRACTION_FMT(6,2) "\n",
                           tctr,
                           threadp->num_commits,
                           threadp->num_bytes_logged,
                           outbuf,
                           ave_bytes_per_xact,
                           FRACTION_ARGS(ave_nlog_pages_per_xact));
      // clang-format on

      sum_ncommits += threadp->num_commits;
      sum_nbytes_logged += threadp->num_bytes_logged;
   }
   platform_default_log("%s\n", dashes);
   // clang-format off

   size_to_str(outbuf, sizeof(outbuf), sum_nbytes_logged);
   platform_default_log(" Totals       %-8lu    %-12lu %-14s  Need ~%lu log pages\n",
                        sum_ncommits,
                        sum_nbytes_logged,
                        outbuf,
                        (((sum_nbytes_logged / xlog_page_size) * 110) / 100));
   // clang-format on

   platform_default_log("%s\n", dashes);
   platform_default_log("\n");
}

/*
 * Exercise log performance test using main thread as a single client.
 * (This is mainly to facilitate debugging of this program.)
 */
platform_status
test_log_perf_single(cache                  *cc,
                     shard_log_config       *cfg,
                     shard_log              *log,
                     uint64                  num_entries,
                     test_message_generator *gen,
                     test_exec_config       *test_exec_cfg)
{
   test_log_thread_params params;
   ZERO_STRUCT(params);
   platform_status ret = STATUS_TEST_FAILED;

   ret = shard_log_init(log, (cache *)cc, cfg);
   platform_assert_status_ok(ret);

   // Fill-out the test execution parameters
   params.log            = log;
   params.thread_id      = 0;
   params.gen            = gen;
   params.num_entries    = num_entries;
   params.commit_every_n = test_exec_cfg->commit_every_n;

   uint64 start_time = platform_get_timestamp();

   test_log_thread((void *)&params);
   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);

   gen_log_perf_summary(&params,
                        num_entries,
                        1,
                        elapsed_ns,
                        test_exec_cfg->commit_every_n,
                        log_page_size((log_handle *)log));

   cache_print_stats(Platform_default_log_handle, cc);
   return ret;
}

/*
 * Exercise log performance test using n-threads and different configs.
 */
platform_status
test_log_perf(cache                  *cc,
              shard_log_config       *cfg,
              shard_log              *log,
              uint64                  num_entries,
              test_message_generator *gen,
              uint64                  num_threads,
              task_system            *ts,
              test_exec_config       *test_exec_cfg,
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
      params[i].log            = log;
      params[i].thread_id      = i;
      params[i].gen            = gen;
      params[i].num_entries    = num_entries / num_threads;
      params[i].commit_every_n = test_exec_cfg->commit_every_n;
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

   uint64 elapsed_ns = platform_timestamp_elapsed(start_time);
   gen_log_perf_summary(params,
                        num_entries,
                        num_threads,
                        elapsed_ns,
                        test_exec_cfg->commit_every_n,
                        log_page_size((log_handle *)log));

   cache_print_stats(Platform_default_log_handle, cc);

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
   rc_allocator_config    al_cfg;
   clockcache_config      cache_cfg;
   shard_log_config       log_cfg;
   rc_allocator           al;
   platform_status        ret;
   int                    config_argc;
   char                 **config_argv;
   bool                   run_perf_test_single;
   bool                   run_perf_test;
   bool                   run_crash_test;
   int                    rc;
   task_system           *ts = NULL;
   test_message_generator gen;

   run_perf_test_single = run_perf_test = run_crash_test = FALSE;
   if (argc > 1) {
      if (strncmp(argv[1], "--help", sizeof("--help")) == 0) {
         // Provide usage info and exit cleanly.
         usage(argv[0]);
         rc = 0;
         goto cleanup;
      }

      if (strncmp(argv[1], "--perf-single", sizeof("--perf-single")) == 0) {
         run_perf_test_single = TRUE;
      } else if (strncmp(argv[1], "--perf", sizeof("--perf")) == 0) {
         run_perf_test = TRUE;
      } else if (strncmp(argv[1], "--crash", sizeof("--crash")) == 0) {
         run_crash_test = TRUE;
      }
      config_argc = argc - 2;
      config_argv = argv + 2;
   } else {
      config_argc = argc - 1;
      config_argv = argv + 1;
   }

   platform_default_log("\nStarted log_test!!\n");

   // Create a heap for io, allocator, cache and splinter
   platform_heap_handle hh;
   platform_heap_id     hid;
   status =
      platform_heap_create(platform_get_module_id(), 1 * GiB, FALSE, &hh, &hid);
   platform_assert_status_ok(status);

   trunk_config *cfg = TYPED_MALLOC(hid, cfg);

   test_exec_config test_exec_cfg;
   ZERO_STRUCT(test_exec_cfg);

   status = test_parse_args_n(cfg,
                              &data_cfg,
                              &io_cfg,
                              &al_cfg,
                              &cache_cfg,
                              &log_cfg,
                              &test_exec_cfg,
                              &gen,
                              1,
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

   uint8 num_bg_threads[NUM_TASK_TYPES] = {0}; // no bg threads

   status = test_init_task_system(
      hid, io, &ts, cfg->use_stats, FALSE, num_bg_threads);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init splinter state: %s\n",
                         platform_status_to_string(status));
      rc = -1;
      goto deinit_iohandle;
   }

   status = rc_allocator_init(
      &al, &al_cfg, (io_handle *)io, hh, hid, platform_get_module_id());
   platform_assert_status_ok(status);

   clockcache *cc = TYPED_MALLOC(hid, cc);
   platform_assert(cc != NULL);
   status = clockcache_init(cc,
                            &cache_cfg,
                            (io_handle *)io,
                            (allocator *)&al,
                            "test",
                            hh,
                            hid,
                            platform_get_module_id());
   platform_assert_status_ok(status);

   shard_log *log = TYPED_MALLOC(hid, log);
   platform_assert(log != NULL);

   // User may specify # of log-entries to insert via --num-inserts arg
   uint64 num_log_entries = test_exec_cfg.num_inserts;
   if (run_perf_test_single) {

      if (!num_log_entries)
         num_log_entries = (1 * MILLION);
      ret = test_log_perf_single(
         (cache *)cc, &log_cfg, log, num_log_entries, &gen, &test_exec_cfg);
      rc = -1;
      platform_assert_status_ok(ret);
   } else if (run_perf_test) {
      if (!num_log_entries)
         num_log_entries = (200 * MILLION);

      ret = test_log_perf((cache *)cc,
                          &log_cfg,
                          log,
                          num_log_entries,
                          &gen,
                          16,
                          ts,
                          &test_exec_cfg,
                          hid);
      rc  = -1;
      platform_assert_status_ok(ret);
   } else if (run_crash_test) {
      if (!num_log_entries)
         num_log_entries = 500000;
      rc = test_log_crash(cc,
                          &cache_cfg,
                          (io_handle *)io,
                          (allocator *)&al,
                          &log_cfg,
                          log,
                          ts,
                          hh,
                          hid,
                          &gen,
                          num_log_entries,
                          TRUE /* crash */);
      platform_assert(rc == 0);
   } else {
      if (!num_log_entries)
         num_log_entries = 500000;
      rc = test_log_crash(cc,
                          &cache_cfg,
                          (io_handle *)io,
                          (allocator *)&al,
                          &log_cfg,
                          log,
                          ts,
                          hh,
                          hid,
                          &gen,
                          num_log_entries,
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
