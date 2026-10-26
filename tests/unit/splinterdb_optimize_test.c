// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * Tests for the public splinterdb_optimize() API and notification modes.
 */

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "platform_threads.h"
#include "splinterdb_tests_private.h"
#include "unit_tests.h"
#include "util.h"
#include "config.h"
#include "ctest.h"

#define OPTIMIZE_TEST_KEY_LENGTH   22
#define OPTIMIZE_TEST_VALUE_LENGTH 31
#define OPTIMIZE_TEST_KEY_SIZE     (OPTIMIZE_TEST_KEY_LENGTH + 1)
#define OPTIMIZE_TEST_VALUE_SIZE   (OPTIMIZE_TEST_VALUE_LENGTH + 1)

static const char optimize_key_fmt[]   = "key-%018u";
static const char optimize_value_fmt[] = "value-%018u-marker";

typedef struct optimize_thread_args {
   splinterdb              *kvsb;
   splinterdb_notification *notification;
   pthread_barrier_t       *barrier;
   char                     min_key[OPTIMIZE_TEST_KEY_SIZE];
   char                     max_key[OPTIMIZE_TEST_KEY_SIZE];
   int                      rc;
} optimize_thread_args;

static void
format_key(char key[OPTIMIZE_TEST_KEY_SIZE], uint32 key_no);

static void
format_value(char value[OPTIMIZE_TEST_VALUE_SIZE], uint32 key_no);

static void
create_optimize_cfg(splinterdb_config *out_cfg, data_config *default_data_cfg);

static void
force_flush_current_memtable(splinterdb *kvsb);

static void
insert_key(splinterdb *kvsb, uint32 key_no);

static void
load_key_batches(splinterdb *kvsb, uint32 num_keys, uint32 batch_size);

static void
verify_point_lookups(splinterdb *kvsb, uint32 num_keys);

static void
verify_full_scan(splinterdb *kvsb, uint32 num_keys);

static void *
optimize_thread(void *arg);

CTEST_DATA(splinterdb_optimize)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;
   data_config       data_cfg;
};

CTEST_SETUP(splinterdb_optimize)
{
   default_data_config_init(&data->data_cfg);
   create_optimize_cfg(&data->cfg, &data->data_cfg);
   data->cfg.use_shmem =
      config_parse_use_shmem(Ctest_argc, (char **)Ctest_argv);

   int rc = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);
}

CTEST_TEARDOWN(splinterdb_optimize)
{
   if (data->kvsb != NULL) {
      splinterdb_close(&data->kvsb, FALSE);
   }
   platform_deregister_thread();
}

CTEST2(splinterdb_optimize, test_blocking_full_range)
{
   const uint32 num_keys = 320;

   load_key_batches(data->kvsb, num_keys, 40);

   int rc = splinterdb_optimize(data->kvsb, .full_leaf_compactions = TRUE);
   ASSERT_EQUAL(0, rc);

   verify_point_lookups(data->kvsb, num_keys);
   verify_full_scan(data->kvsb, num_keys);
}

CTEST2(splinterdb_optimize, test_blocking_without_full_leaf_compactions)
{
   const uint32 num_keys = 320;

   load_key_batches(data->kvsb, num_keys, 40);

   int rc = splinterdb_optimize(data->kvsb);
   ASSERT_EQUAL(0, rc);

   verify_point_lookups(data->kvsb, num_keys);
   verify_full_scan(data->kvsb, num_keys);
}

CTEST2(splinterdb_optimize, test_blocking_with_no_background_threads)
{
   const uint32 num_keys = 320;

   splinterdb_close(&data->kvsb, FALSE);
   data->cfg.num_memtable_bg_threads = 0;
   data->cfg.num_normal_bg_threads   = 0;

   int rc = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   load_key_batches(data->kvsb, num_keys, 40);

   rc = splinterdb_optimize(data->kvsb, .full_leaf_compactions = TRUE);
   ASSERT_EQUAL(0, rc);

   verify_point_lookups(data->kvsb, num_keys);
   verify_full_scan(data->kvsb, num_keys);
}

// Each update replaces the value with the requested number of 'x' bytes.
// A one-byte update can therefore grow or shrink the materialized value.
static int
resize_merge_final(const data_config *cfg,
                   slice              key,
                   merge_accumulator *message)
{
   uint8 length = *(const uint8 *)merge_accumulator_data(message);
   if (!merge_accumulator_resize(message, length)) {
      return ENOMEM;
   }
   memset(merge_accumulator_data(message), 'x', length);
   merge_accumulator_set_class(message, MESSAGE_TYPE_INSERT);
   return 0;
}

static int
resize_merge(const data_config *cfg,
             slice              key,
             message            old_message,
             merge_accumulator *new_message)
{
   return resize_merge_final(cfg, key, new_message);
}

static void
verify_resized_value(splinterdb *kvsb, slice key, uint8 length)
{
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(kvsb, &result);
   ASSERT_EQUAL(0, splinterdb_lookup(kvsb, key, &result));
   ASSERT_TRUE(splinterdb_lookup_found(&result));
   slice value;
   ASSERT_EQUAL(0, splinterdb_lookup_result_value(&result, &value));
   ASSERT_EQUAL(length, slice_length(value));
   char expected[UINT8_MAX];
   memset(expected, 'x', length);
   ASSERT_EQUAL(0, memcmp(expected, slice_data(value), length));
   splinterdb_lookup_result_deinit(&result);
}

CTEST2(splinterdb_optimize, test_merge_can_grow_and_shrink_values)
{
   splinterdb_close(&data->kvsb, FALSE);
   data->data_cfg.merge_tuples       = resize_merge;
   data->data_cfg.merge_tuples_final = resize_merge_final;
   data->cfg.num_memtable_bg_threads = 0;
   data->cfg.num_normal_bg_threads   = 0;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));

   slice       keys[]       = {slice_create(1, "a"), slice_create(1, "b")};
   const uint8 lengths[][2] = {{100, 100}, {200, 20}, {1, 2}};
   for (uint64 round = 0; round < ARRAY_SIZE(lengths); round++) {
      for (uint64 i = 0; i < ARRAY_SIZE(keys); i++) {
         ASSERT_EQUAL(0,
                      splinterdb_update(data->kvsb,
                                        keys[i],
                                        message_create(1, &lengths[round][i]),
                                        NULL));
      }
      force_flush_current_memtable(data->kvsb);
      ASSERT_EQUAL(
         0, splinterdb_optimize(data->kvsb, .full_leaf_compactions = TRUE));
      for (uint64 i = 0; i < ARRAY_SIZE(keys); i++) {
         verify_resized_value(data->kvsb, keys[i], lengths[round][i]);
      }
      splinterdb_close(&data->kvsb, FALSE);
      ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->kvsb));
      for (uint64 i = 0; i < ARRAY_SIZE(keys); i++) {
         verify_resized_value(data->kvsb, keys[i], lengths[round][i]);
      }
   }
}

CTEST2(splinterdb_optimize, test_open_reads_disk_geometry)
{
   const uint32 num_keys = 160;

   load_key_batches(data->kvsb, num_keys, 40);
   splinterdb_close(&data->kvsb, FALSE);

   data->cfg.disk_size   = 0;
   data->cfg.page_size   = 0;
   data->cfg.extent_size = 0;

   int rc = splinterdb_open(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   verify_point_lookups(data->kvsb, num_keys);
   verify_full_scan(data->kvsb, num_keys);
}

CTEST2(splinterdb_optimize, test_polling_subrange)
{
   const uint32 num_keys = 320;
   char         min_key[OPTIMIZE_TEST_KEY_SIZE];
   char         max_key[OPTIMIZE_TEST_KEY_SIZE];

   load_key_batches(data->kvsb, num_keys, 40);
   format_key(min_key, 80);
   format_key(max_key, 240);

   uint64                  user_data = 42;
   splinterdb_notification notification;
   splinterdb_notification_init_polling(&notification, &user_data);

   int rc =
      splinterdb_optimize(data->kvsb,
                          .min_key = slice_create(strlen(min_key), min_key),
                          .max_key = slice_create(strlen(max_key), max_key),
                          .full_leaf_compactions = TRUE,
                          .notification          = &notification);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(&user_data == splinterdb_notification_user_data(&notification));

   int status = EINVAL;
   rc         = splinterdb_notification_wait(data->kvsb, &notification);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_notification_poll(&notification, &status));
   ASSERT_EQUAL(0, status);

   splinterdb_notification_deinit(&notification);

   verify_point_lookups(data->kvsb, num_keys);
   verify_full_scan(data->kvsb, num_keys);
}

CTEST2(splinterdb_optimize, test_empty_and_rejected_ranges_all_modes)
{
   slice low  = slice_create(1, "a");
   slice high = slice_create(1, "z");

   splinterdb_notification  polling;
   splinterdb_notification *notifications[] = {
      SPLINTERDB_NOTIFICATION_BLOCKING,
      SPLINTERDB_NOTIFICATION_FIRE_AND_FORGET,
      &polling,
   };
   for (uint64 i = 0; i < ARRAY_SIZE(notifications); i++) {
      splinterdb_notification_init_polling(&polling, NULL);
      /* Empty ranges complete inline, before the blocking caller waits. */
      ASSERT_EQUAL(0,
                   splinterdb_optimize(data->kvsb,
                                       .min_key               = low,
                                       .max_key               = low,
                                       .full_leaf_compactions = TRUE,
                                       .notification = notifications[i]));
      if (notifications[i] == &polling) {
         int status = -1;
         ASSERT_TRUE(splinterdb_notification_poll(&polling, &status));
         ASSERT_EQUAL(0, status);
      }
      splinterdb_notification_deinit(&polling);

      splinterdb_notification_init_polling(&polling, NULL);
      /* Rejected submission schedules no completion. The private blocking
       * notification must release both references on this path. */
      ASSERT_EQUAL(EINVAL,
                   splinterdb_optimize(data->kvsb,
                                       .min_key               = high,
                                       .max_key               = low,
                                       .full_leaf_compactions = TRUE,
                                       .notification = notifications[i]));
      ASSERT_FALSE(splinterdb_notification_poll(&polling, NULL));
      splinterdb_notification_deinit(&polling);
   }
}

CTEST2(splinterdb_optimize, test_fire_and_forget_without_background_threads)
{
   const uint32 num_keys = 320;

   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   data->cfg.num_memtable_bg_threads = 0;
   data->cfg.num_normal_bg_threads   = 0;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->kvsb));
   load_key_batches(data->kvsb, num_keys, 40);

   ASSERT_EQUAL(0,
                splinterdb_optimize(
                   data->kvsb,
                   .full_leaf_compactions = TRUE,
                   .notification = SPLINTERDB_NOTIFICATION_FIRE_AND_FORGET));
   /* There is no notification recipient. Close must drain accepted work. */
   ASSERT_EQUAL(0, splinterdb_close(&data->kvsb, FALSE));
   ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->kvsb));
   verify_point_lookups(data->kvsb, num_keys);
   verify_full_scan(data->kvsb, num_keys);
}

CTEST2(splinterdb_optimize, test_concurrent_overlapping_ranges)
{
   const uint32            num_keys    = 320;
   const uint32            num_threads = 4;
   const uint32            range_min[] = {0, 40, 80, 120};
   const uint32            range_max[] = {200, 240, 280, 320};
   pthread_t               threads[4];
   optimize_thread_args    args[4];
   splinterdb_notification notifications[4];
   pthread_barrier_t       barrier;

   load_key_batches(data->kvsb, num_keys, 40);

   int rc = pthread_barrier_init(&barrier, NULL, num_threads);
   ASSERT_EQUAL(0, rc);

   for (uint32 i = 0; i < num_threads; i++) {
      ZERO_STRUCT(args[i]);
      args[i].kvsb         = data->kvsb;
      args[i].notification = &notifications[i];
      args[i].barrier      = &barrier;
      format_key(args[i].min_key, range_min[i]);
      format_key(args[i].max_key, range_max[i]);
      splinterdb_notification_init_polling(&notifications[i], &args[i]);

      rc = pthread_create(&threads[i], NULL, optimize_thread, &args[i]);
      ASSERT_EQUAL(0, rc);
   }

   for (uint32 i = 0; i < num_threads; i++) {
      rc = pthread_join(threads[i], NULL);
      ASSERT_EQUAL(0, rc);
      ASSERT_EQUAL(0, args[i].rc);
   }

   for (uint32 i = 0; i < num_threads; i++) {
      rc = splinterdb_notification_wait(data->kvsb, &notifications[i]);
      ASSERT_EQUAL(0, rc);
      ASSERT_TRUE(&args[i]
                  == splinterdb_notification_user_data(&notifications[i]));
      splinterdb_notification_deinit(&notifications[i]);
   }

   rc = pthread_barrier_destroy(&barrier);
   ASSERT_EQUAL(0, rc);

   verify_point_lookups(data->kvsb, num_keys);
   verify_full_scan(data->kvsb, num_keys);
}

static void
format_key(char key[OPTIMIZE_TEST_KEY_SIZE], uint32 key_no)
{
   ASSERT_EQUAL(
      OPTIMIZE_TEST_KEY_LENGTH,
      snprintf(key, OPTIMIZE_TEST_KEY_SIZE, optimize_key_fmt, key_no));
}

static void
format_value(char value[OPTIMIZE_TEST_VALUE_SIZE], uint32 key_no)
{
   ASSERT_EQUAL(
      OPTIMIZE_TEST_VALUE_LENGTH,
      snprintf(value, OPTIMIZE_TEST_VALUE_SIZE, optimize_value_fmt, key_no));
}

static void
create_optimize_cfg(splinterdb_config *out_cfg, data_config *default_data_cfg)
{
   *out_cfg = (splinterdb_config){
      .filename                = TEST_CONFIG_DEFAULT_IO_FILENAME,
      .cache_size              = 128 * Mega,
      .disk_size               = 256 * Mega,
      .data_cfg                = default_data_cfg,
      .fanout                  = 4,
      .memtable_capacity       = 1 * Mega,
      .num_memtable_bg_threads = 1,
      .num_normal_bg_threads   = 4,
      .queue_scale_percent     = UINT64_MAX,
   };
}

static void
force_flush_current_memtable(splinterdb *kvsb)
{
   core_handle    *core = (core_handle *)splinterdb_get_trunk_handle(kvsb);
   platform_status rc   = task_perform_until_quiescent(core->ts);
   ASSERT_TRUE(SUCCESS(rc));

   rc = memtable_force_rotation(&core->mt_ctxt, NULL);
   ASSERT_TRUE(SUCCESS(rc));

   rc = task_perform_until_quiescent(core->ts);
   ASSERT_TRUE(SUCCESS(rc));
}

static void
insert_key(splinterdb *kvsb, uint32 key_no)
{
   char key[OPTIMIZE_TEST_KEY_SIZE];
   char value[OPTIMIZE_TEST_VALUE_SIZE];

   format_key(key, key_no);
   format_value(value, key_no);

   int rc = splinterdb_insert(kvsb,
                              slice_create(strlen(key), key),
                              message_create(strlen(value), value),
                              NULL);
   ASSERT_EQUAL(0, rc);
}

static void
load_key_batches(splinterdb *kvsb, uint32 num_keys, uint32 batch_size)
{
   for (uint32 key_no = 0; key_no < num_keys; key_no++) {
      insert_key(kvsb, key_no);
      if ((key_no + 1) % batch_size == 0) {
         force_flush_current_memtable(kvsb);
      }
   }

   if (num_keys % batch_size != 0) {
      force_flush_current_memtable(kvsb);
   }
}

static void
verify_point_lookups(splinterdb *kvsb, uint32 num_keys)
{
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(kvsb, &result);

   for (uint32 key_no = 0; key_no < num_keys; key_no++) {
      char key[OPTIMIZE_TEST_KEY_SIZE];
      char value[OPTIMIZE_TEST_VALUE_SIZE];

      format_key(key, key_no);
      format_value(value, key_no);

      int rc = splinterdb_lookup(kvsb, slice_create(strlen(key), key), &result);
      ASSERT_EQUAL(0, rc);
      ASSERT_TRUE(splinterdb_lookup_found(&result));

      slice found;
      rc = splinterdb_lookup_result_value(&result, &found);
      ASSERT_EQUAL(0, rc);
      ASSERT_EQUAL(strlen(value), slice_length(found));
      ASSERT_EQUAL(0, memcmp(value, slice_data(found), slice_length(found)));
   }

   splinterdb_lookup_result_deinit(&result);
}

static void
verify_full_scan(splinterdb *kvsb, uint32 num_keys)
{
   splinterdb_iterator *itor = NULL;
   int                  rc   = splinterdb_iterator_init(kvsb, &itor);
   ASSERT_EQUAL(0, rc);

   for (uint32 key_no = 0; key_no < num_keys; key_no++) {
      char key[OPTIMIZE_TEST_KEY_SIZE];
      char value[OPTIMIZE_TEST_VALUE_SIZE];

      ASSERT_TRUE(splinterdb_iterator_valid(itor));

      format_key(key, key_no);
      format_value(value, key_no);

      slice found_key;
      slice found_value;
      splinterdb_iterator_get_current(itor, &found_key, &found_value);

      ASSERT_EQUAL(strlen(key), slice_length(found_key));
      ASSERT_EQUAL(0,
                   memcmp(key, slice_data(found_key), slice_length(found_key)));
      ASSERT_EQUAL(strlen(value), slice_length(found_value));
      ASSERT_EQUAL(
         0, memcmp(value, slice_data(found_value), slice_length(found_value)));

      splinterdb_iterator_next(itor);
   }

   ASSERT_FALSE(splinterdb_iterator_valid(itor));
   rc = splinterdb_iterator_status(itor);
   ASSERT_EQUAL(0, rc);
   splinterdb_iterator_deinit(itor);
}

static void *
optimize_thread(void *arg)
{
   optimize_thread_args *args = (optimize_thread_args *)arg;
   int                   rc   = pthread_barrier_wait(args->barrier);

   if (rc != 0 && rc != PTHREAD_BARRIER_SERIAL_THREAD) {
      args->rc = rc;
      return NULL;
   }

   args->rc = splinterdb_optimize(
      args->kvsb,
      .min_key = slice_create(strlen(args->min_key), args->min_key),
      .max_key = slice_create(strlen(args->max_key), args->max_key),
      .full_leaf_compactions = TRUE,
      .notification          = args->notification);

   return NULL;
}
