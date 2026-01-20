// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * filter_test.c --
 *
 *     This file contains the test interfaces for Alex's filter
 */
#include "splinterdb/data.h"
#include "test.h"
#include "routing_filter.h"
#include "allocator.h"
#include "rc_allocator.h"
#include "cache.h"
#include "clockcache.h"
#include "util.h"
#include "platform_time.h"
#include "platform_typed_alloc.h"
#include "platform_assert.h"
#include "poison.h"

static platform_status
test_filter_basic(cache           *cc,
                  routing_config  *cfg,
                  platform_heap_id hid,
                  uint64           num_fingerprints,
                  uint64           num_values)
{
   platform_default_log("filter_test: routing filter basic test started\n");
   platform_status rc = STATUS_OK;

   const uint64 key_size = cfg->data_cfg->max_key_size;
   if (key_size < sizeof(uint64)) {
      platform_default_log("key_size %lu too small\n", key_size);
      return STATUS_BAD_PARAM;
   }

   uint32 **fp_arr = TYPED_ARRAY_MALLOC(hid, fp_arr, num_values);
   for (uint64 i = 0; i < num_values; i++) {
      fp_arr[i] = TYPED_ARRAY_MALLOC(hid, fp_arr[i], num_fingerprints);
   }

   bool32 *used_keys =
      TYPED_ARRAY_ZALLOC(hid, used_keys, (num_values + 1) * num_fingerprints);

   uint32 *num_input_keys = TYPED_ARRAY_ZALLOC(hid, num_input_keys, num_values);

   auto_writable_buffer keywb = NULL_WRITABLE_BUFFER(hid);
   writable_buffer_resize(&keywb, key_size);
   writable_buffer_memset(&keywb, 0);
   uint64 *keybuf = writable_buffer_data(&keywb);
   key     target = key_create(key_size, keybuf);
   for (uint64 i = 0; i < num_values; i++) {
      if (i != 0) {
         num_input_keys[i] = num_input_keys[i - 1];
      }
      for (uint64 j = 0; j < num_fingerprints; j++) {
         if (!used_keys[(i + 1) * j]) {
            used_keys[(i + 1) * j] = TRUE;
            num_input_keys[i]++;
         }
         *keybuf      = (i + 1) * j;
         fp_arr[i][j] = cfg->hash(keybuf, key_size, cfg->seed);
      }
   }

   platform_free(hid, used_keys);

   routing_filter filter[MAX_FILTERS] = {{0}};
   for (uint64 i = 0; i < num_values; i++) {
      rc = routing_filter_add(
         cc, cfg, &filter[i], &filter[i + 1], fp_arr[i], num_fingerprints, i);
      // platform_default_log("FILTER %lu\n", i);
      // routing_filter_print(cc, cfg, &filter);
      uint32 estimated_input_keys =
         routing_filter_estimate_unique_keys(&filter[i + 1], cfg);

      platform_default_log("num input keys %8u estimate %8u num_unique %u\n",
                           num_input_keys[i],
                           estimated_input_keys,
                           filter[i + 1].num_unique);
   }

   uint32 num_unique =
      routing_filter_estimate_unique_fp(cc, cfg, hid, filter + 1, num_values);
   num_unique = routing_filter_estimate_unique_keys_from_count(cfg, num_unique);
   platform_default_log("across filters: num input keys %8u estimate %8u\n",
                        num_input_keys[num_values - 1],
                        num_unique);

   platform_free(hid, num_input_keys);

   for (uint64 i = 0; i < num_values; i++) {
      for (uint64 j = 0; j < num_fingerprints; j++) {
         *keybuf = (i + 1) * j;
         uint64 found_values;
         rc = routing_filter_lookup(
            cc, cfg, &filter[i + 1], target, &found_values);
         platform_assert_status_ok(rc);
         if (!routing_filter_is_value_found(found_values, i)) {
            platform_default_log(
               "key-value pair (%lu, %lu) not found in filter\n",
               (i + 1) * j,
               i);
            rc = STATUS_NOT_FOUND;
            goto out;
         }
      }
   }

   uint64 unused_key      = (num_values + 1) * num_fingerprints;
   uint64 false_positives = 0;
   for (uint64 i = unused_key; i < unused_key + num_fingerprints; i++) {
      *keybuf = i;
      uint64 found_values;
      rc = routing_filter_lookup(
         cc, cfg, &filter[num_values], target, &found_values);
      if (found_values) {
         false_positives++;
      }
   }

   fraction false_positive_rate =
      init_fraction(false_positives, num_fingerprints);
   platform_default_log(
      "routing filter basic test: false positive rate " FRACTION_FMT(1, 4) "\n",
      FRACTION_ARGS(false_positive_rate));

   for (uint64 i = 0; i < num_values; i++) {
      routing_filter_dec_ref(cc, &filter[i + 1]);
   }

out:
   if (fp_arr) {
      for (uint64 i = 0; i < num_values; i++) {
         platform_free(hid, fp_arr[i]);
      }
   }
   platform_free(hid, fp_arr);
   return rc;
}

static platform_status
test_filter_perf(cache           *cc,
                 routing_config  *cfg,
                 platform_heap_id hid,
                 uint64           num_fingerprints,
                 uint64           num_values,
                 uint64           num_trees)
{
   platform_default_log("filter_test: routing filter perf test started\n");
   platform_status rc = STATUS_OK;

   const uint64 key_size = cfg->data_cfg->max_key_size;
   if (key_size < sizeof(uint64)) {
      platform_default_log("key_size %lu too small\n", key_size);
      return STATUS_BAD_PARAM;
   }

   uint32 *fp_arr = TYPED_ARRAY_MALLOC(
      hid, fp_arr, num_trees * num_values * num_fingerprints);
   if (fp_arr == NULL) {
      return STATUS_NO_MEMORY;
   }
   auto_writable_buffer keywb = NULL_WRITABLE_BUFFER(hid);
   writable_buffer_resize(&keywb, key_size);
   writable_buffer_memset(&keywb, 0);
   uint64 *keybuf = writable_buffer_data(&keywb);
   key     target = key_create(key_size, keybuf);
   for (uint64 k = 0; k < num_trees; k++) {
      for (uint64 i = 0; i < num_values * num_fingerprints; i++) {
         uint64 idx  = k * num_values * num_fingerprints + i;
         *keybuf     = idx;
         fp_arr[idx] = cfg->hash(keybuf, key_size, cfg->seed);
      }
   }

   uint64          start_time = platform_get_timestamp();
   routing_filter *filter     = TYPED_ARRAY_ZALLOC(hid, filter, num_trees);
   for (uint64 k = 0; k < num_trees; k++) {
      for (uint64 i = 0; i < num_values; i++) {
         routing_filter new_filter = {0};
         uint64         fp_start =
            k * num_fingerprints * num_values + i * num_fingerprints;
         platform_status rc = routing_filter_add(cc,
                                                 cfg,
                                                 &filter[k],
                                                 &new_filter,
                                                 &fp_arr[fp_start],
                                                 num_fingerprints,
                                                 i);
         if (!SUCCESS(rc)) {
            goto out;
         }
         routing_filter_dec_ref(cc, &filter[k]);
         filter[k] = new_filter;
      }
   }
   platform_default_log("filter insert time per key %lu\n",
                        platform_timestamp_elapsed(start_time)
                           / (num_fingerprints * num_values * num_trees));

   start_time = platform_get_timestamp();
   for (uint64 k = 0; k < num_trees; k++) {
      for (uint64 i = 0; i < num_values * num_fingerprints; i++) {
         *keybuf = k * num_values * num_fingerprints + i;
         uint64 found_values;
         rc = routing_filter_lookup(cc, cfg, &filter[k], target, &found_values);
         platform_assert_status_ok(rc);
         if (!routing_filter_is_value_found(found_values, i / num_fingerprints))
         {
            platform_default_log(
               "key-value pair (%lu, %lu) not found in filter %lu (%lu)\n",
               k * num_values * num_fingerprints + i,
               i / num_fingerprints,
               k,
               found_values);

            routing_filter_lookup(cc, cfg, &filter[k], target, &found_values);
            platform_assert(0);
            rc = STATUS_NOT_FOUND;
            goto out;
         }
      }
   }
   platform_default_log("filter positive lookup time per key %lu\n",
                        platform_timestamp_elapsed(start_time)
                           / (num_fingerprints * num_trees * num_values));

   start_time             = platform_get_timestamp();
   uint64 unused_key      = num_values * num_fingerprints * num_trees;
   uint64 false_positives = 0;
   for (uint64 k = 0; k < num_trees; k++) {
      for (uint64 i = 0; i < num_values * num_fingerprints; i++) {
         *keybuf = k * num_values * num_fingerprints + i + unused_key;
         uint64 found_values;
         rc = routing_filter_lookup(cc, cfg, &filter[k], target, &found_values);
         platform_assert_status_ok(rc);
         if (found_values) {
            false_positives++;
         }
      }
   }

   platform_default_log("filter negative lookup time per key %lu\n",
                        platform_timestamp_elapsed(start_time)
                           / (num_fingerprints * num_trees * num_values));
   fraction false_positive_rate =
      init_fraction(false_positives, num_fingerprints * num_trees * num_values);
   platform_default_log("filter_basic_test: false positive rate " FRACTION_FMT(
                           1, 4) " for %lu trees\n",
                        FRACTION_ARGS(false_positive_rate),
                        num_trees);

   cache_print_stats(Platform_default_log_handle, cc);

out:
   for (uint64 i = 0; i < num_trees; i++) {
      routing_filter_dec_ref(cc, &filter[i]);
   }
   if (fp_arr) {
      platform_free(hid, fp_arr);
   }
   platform_free(hid, filter);
   return rc;
}

static void
usage(const char *argv0)
{
   platform_error_log("Usage:\n"
                      "\t%s\n"
                      "\t%s --perf\n",
                      argv0,
                      argv0);
   config_usage();
}

int
filter_test(int argc, char *argv[])
{
   int                    r;
   system_config          system_cfg;
   rc_allocator           al;
   clockcache            *cc;
   int                    config_argc;
   char                 **config_argv;
   bool32                 run_perf_test;
   platform_status        rc;
   uint64                 seed;
   test_message_generator gen;

   platform_register_thread();

   if (argc > 1 && strncmp(argv[1], "--perf", sizeof("--perf")) == 0) {
      run_perf_test = TRUE;
      config_argc   = argc - 2;
      config_argv   = argv + 2;
   } else {
      run_perf_test = FALSE;
      config_argc   = argc - 1;
      config_argv   = argv + 1;
   }

   bool use_shmem = config_parse_use_shmem(config_argc, config_argv);

   // Create a heap for io, allocator, cache and splinter
   platform_heap_id hid = NULL;
   rc                   = platform_heap_create(
      platform_get_module_id(), 512 * MiB, use_shmem, &hid);
   platform_assert_status_ok(rc);

   uint64 num_memtable_bg_threads_unused = 0;
   uint64 num_normal_bg_threads_unused   = 0;

   rc = test_parse_args(&system_cfg,
                        &seed,
                        &gen,
                        &num_memtable_bg_threads_unused,
                        &num_normal_bg_threads_unused,
                        config_argc,
                        config_argv);
   if (!SUCCESS(rc)) {
      platform_error_log("filter_test: failed to parse config: %s\n",
                         platform_status_to_string(rc));
      //
      // Provided arguments but set things up incorrectly.
      // Print usage so client can fix commandline.
      usage(argv[0]);
      r = -1;
      goto cleanup;
   }

   io_handle *io = io_handle_create(&system_cfg.io_cfg, hid);
   if (io == NULL) {
      platform_error_log("Failed to create IO handle\n");
      rc = STATUS_NO_MEMORY;
      r  = -1;
      goto cleanup;
   }

   task_system ts;
   rc = task_system_init(&ts, hid, &system_cfg.task_cfg);
   platform_assert_status_ok(rc);

   rc = rc_allocator_init(
      &al, &system_cfg.allocator_cfg, io, hid, platform_get_module_id());
   platform_assert_status_ok(rc);

   cc = TYPED_MALLOC(hid, cc);
   platform_assert(cc);
   rc = clockcache_init(cc,
                        &system_cfg.cache_cfg,
                        io,
                        (allocator *)&al,
                        "test",
                        hid,
                        platform_get_module_id());
   platform_assert_status_ok(rc);

   uint64 rflimit = routing_filter_max_fingerprints(
      (cache_config *)&system_cfg.cache_cfg, &system_cfg.filter_cfg);

   if (run_perf_test) {
      rc = test_filter_perf((cache *)cc,
                            &system_cfg.filter_cfg,
                            hid,
                            rflimit / system_cfg.trunk_node_cfg.target_fanout,
                            system_cfg.trunk_node_cfg.target_fanout,
                            100);
      platform_assert(SUCCESS(rc));
   } else {
      rc = test_filter_basic((cache *)cc,
                             &system_cfg.filter_cfg,
                             hid,
                             rflimit / system_cfg.trunk_node_cfg.target_fanout,
                             system_cfg.trunk_node_cfg.target_fanout);
      platform_assert(SUCCESS(rc));
      rc = test_filter_basic((cache *)cc,
                             &system_cfg.filter_cfg,
                             hid,
                             100,
                             system_cfg.trunk_node_cfg.target_fanout);
      platform_assert(SUCCESS(rc));
      rc = test_filter_basic((cache *)cc,
                             &system_cfg.filter_cfg,
                             hid,
                             1,
                             system_cfg.trunk_node_cfg.target_fanout);
      platform_assert(SUCCESS(rc));
      rc = test_filter_basic((cache *)cc,
                             &system_cfg.filter_cfg,
                             hid,
                             1,
                             2 * system_cfg.trunk_node_cfg.target_fanout);
      platform_assert(SUCCESS(rc));
   }

   clockcache_deinit(cc);
   platform_free(hid, cc);
   rc_allocator_deinit(&al);
   task_system_deinit(&ts);
   io_handle_destroy(io);
   r = 0;
cleanup:
   platform_heap_destroy(&hid);
   platform_deregister_thread();
   return r;
}
