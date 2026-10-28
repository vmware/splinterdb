// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/* Public reusable-blob ownership, sharing, and durability regressions. */
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "blob.h"
#include "config.h"
#include "lookup_result.h"
#include "platform_threads.h"
#include "splinterdb_tests_private.h"
#include "unit_tests.h"
#include "util.h"
#include "ctest.h"

static uint64
blob_test_in_use_bytes(allocator *al)
{
   return allocator_in_use(al) * allocator_get_config(al)->io_cfg->extent_size;
}

// Append is deliberately non-idempotent: treating UPDATE as INSERT would
// discard the prefix and fail the expected-byte comparisons below.
static int
blob_test_append(const data_config *cfg,
                 slice              key,
                 message            older,
                 merge_accumulator *newer)
{
   uint64 old_length = message_length(older);
   uint64 new_length = merge_accumulator_length(newer);
   if (!merge_accumulator_resize(newer, old_length + new_length)) {
      return ENOMEM;
   }
   char *bytes = merge_accumulator_data(newer);
   memmove(bytes + old_length, bytes, new_length);
   memcpy(bytes, message_data(older), old_length);
   merge_accumulator_set_class(newer, message_class(older));
   return 0;
}

static int
blob_test_append_final(const data_config *cfg,
                       slice              key,
                       merge_accumulator *oldest)
{
   merge_accumulator_set_class(oldest, MESSAGE_TYPE_INSERT);
   return 0;
}

CTEST_DATA(splinterdb_blob)
{
   splinterdb       *db;
   splinterdb_config cfg;
   data_config       data_cfg;
};

CTEST_SETUP(splinterdb_blob)
{
   default_data_config_init(&data->data_cfg);
   data->db  = NULL;
   data->cfg = (splinterdb_config){
      .filename   = TEST_CONFIG_DEFAULT_IO_FILENAME,
      .cache_size = 64 * Mega,
      .disk_size  = 256 * Mega,
      .data_cfg   = &data->data_cfg,
      .use_shmem  = config_parse_use_shmem(Ctest_argc, (char **)Ctest_argv),
      .blob_max_descriptor_size   = 128,
      .log_group_size_pages       = UINT64_MAX,
      .checkpoint_log_size_bytes  = UINT64_MAX,
      .checkpoint_log_grace_bytes = UINT64_MAX,
   };
}

CTEST_TEARDOWN(splinterdb_blob)
{
   if (data->db != NULL) {
      ASSERT_EQUAL(0, splinterdb_close(&data->db, FALSE));
   }
   platform_deregister_thread();
}

static void
blob_test_check_result(splinterdb_lookup_result *result, slice expected)
{
   ASSERT_TRUE(splinterdb_lookup_found(result));
   slice actual;
   ASSERT_EQUAL(0, splinterdb_lookup_result_value(result, &actual));
   ASSERT_EQUAL(slice_length(expected), slice_length(actual));
   if (slice_length(expected) != 0) {
      ASSERT_EQUAL(0,
                   memcmp(slice_data(expected),
                          slice_data(actual),
                          slice_length(expected)));
   }
}

/* Inspect descriptors before the public value accessor materializes them.
 * Whole extents (including pointer trees) must be physically shared. Packed
 * tails are intentionally allowed to move when each owner clones its value. */
static void
blob_test_check_shared_values(splinterdb *db, uint64 count, slice expected)
{
   uint64                   root_addrs[64] = {0};
   uint64                   root_count     = 0;
   uint64                   height         = 0;
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(db, &result);
   for (uint64 i = 0; i < count; i++) {
      ASSERT_EQUAL(0,
                   splinterdb_lookup(db, slice_create(sizeof(i), &i), &result));
      ASSERT_TRUE(splinterdb_lookup_found(&result));
      lookup_result *internal = lookup_result_from_splinterdb(&result);
      ASSERT_TRUE(merge_accumulator_is_blob(&internal->value));
      message     msg = merge_accumulator_to_message(&internal->value);
      parsed_blob parsed;
      ASSERT_TRUE(
         SUCCESS(blob_parse_descriptor((cache *)splinterdb_get_cache_handle(db),
                                       message_slice(msg),
                                       &parsed)));
      ASSERT_TRUE(parsed.num_extents <= ARRAY_SIZE(root_addrs));
      if (i == 0) {
         root_count = parsed.num_extents;
         height     = parsed.height;
         memcpy(root_addrs, parsed.base->addrs, root_count * sizeof(uint64));
      } else {
         ASSERT_EQUAL(root_count, parsed.num_extents);
         ASSERT_EQUAL(height, parsed.height);
         ASSERT_EQUAL(0,
                      memcmp(root_addrs,
                             parsed.base->addrs,
                             root_count * sizeof(uint64)));
      }
      blob_test_check_result(&result, expected);
   }
   splinterdb_lookup_result_deinit(&result);
}

CTEST2(splinterdb_blob, test_reuse_release_compact_and_reopen)
{
   const uint64 sizes[] = {0, 17, 4097, 128 * KiB + 17, 2 * Mega + 17};
   const uint64 count   = 12;
   for (unsigned use_log = 0; use_log < 2; use_log++) {
      data->cfg.use_log = use_log;
      for (unsigned variant = 0; variant < ARRAY_SIZE(sizes); variant++) {
         ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->db));
         const uint64 size  = sizes[variant];
         char        *value = malloc(size ? size : 1);
         ASSERT_TRUE(value != NULL);
         memset(value, 0x39, size);
         message handle = {0};
         ASSERT_EQUAL(0,
                      splinterdb_blob_create(
                         data->db, slice_create(size, value), &handle));
         ASSERT_TRUE(message_is_blob(handle));

         /* Creation owns the bytes before the user's input changes. */
         memset(value, 0x7c, size);
         for (uint64 i = 0; i < count; i++) {
            ASSERT_EQUAL(
               0,
               splinterdb_insert(
                  data->db, slice_create(sizeof(i), &i), handle, NULL));
         }
         memset(value, 0x39, size);
         slice expected = slice_create(size, value);
         blob_test_check_shared_values(data->db, count, expected);

         splinterdb_blob_release(&handle);
         ASSERT_FALSE(message_is_blob(handle));
         ASSERT_EQUAL(
            0, splinterdb_optimize(data->db, .full_leaf_compactions = TRUE));
         blob_test_check_shared_values(data->db, count, expected);
         ASSERT_EQUAL(0, splinterdb_durable_barrier(data->db, NULL));
         ASSERT_EQUAL(0, splinterdb_close(&data->db, FALSE));
         ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->db));
         blob_test_check_shared_values(data->db, count, expected);
         ASSERT_EQUAL(0, splinterdb_close(&data->db, FALSE));
         free(value);
      }
   }
}

CTEST2(splinterdb_blob, test_overwrite_delete_and_old_result)
{
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->db));
   const uint64 size  = 2 * 128 * KiB + 19;
   char        *value = malloc(size);
   ASSERT_TRUE(value != NULL);
   memset(value, 0x65, size);
   slice   expected = slice_create(size, value);
   slice   key      = slice_create(3, "key");
   slice   ordinary = slice_create(8, "ordinary");
   message handle   = {0};
   ASSERT_EQUAL(0, splinterdb_blob_create(data->db, expected, &handle));
   ASSERT_EQUAL(
      0, splinterdb_insert(data->db, key, message_from_slice(ordinary), NULL));

   splinterdb_lookup_result old;
   splinterdb_lookup_result_init(data->db, &old);
   ASSERT_EQUAL(0, splinterdb_insert(data->db, key, handle, &old));
   blob_test_check_result(&old, ordinary);

   ASSERT_EQUAL(0, splinterdb_insert(data->db, key, handle, &old));
   blob_test_check_result(&old, expected);
   ASSERT_EQUAL(0, splinterdb_delete(data->db, key, &old));
   splinterdb_blob_release(&handle);
   blob_test_check_result(&old, expected);

   splinterdb_lookup_result absent;
   splinterdb_lookup_result_init(data->db, &absent);
   ASSERT_EQUAL(0, splinterdb_lookup(data->db, key, &absent));
   ASSERT_FALSE(splinterdb_lookup_found(&absent));
   splinterdb_lookup_result_deinit(&absent);
   splinterdb_lookup_result_deinit(&old);
   free(value);
}

CTEST2(splinterdb_blob, test_wrong_database_and_unused_release)
{
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->db));
   allocator   *al     = (allocator *)splinterdb_get_allocator_handle(data->db);
   const uint64 before = blob_test_in_use_bytes(al);
   char         value[8193];
   memset(value, 0x29, sizeof(value));
   for (unsigned i = 0; i < 32; i++) {
      message handle = {0};
      ASSERT_EQUAL(0,
                   splinterdb_blob_create(
                      data->db, slice_create(sizeof(value), value), &handle));
      splinterdb_blob_release(&handle);
      ASSERT_FALSE(message_is_blob(handle));
      splinterdb_blob_release(&handle);
      ASSERT_EQUAL(before, blob_test_in_use_bytes(al));
   }
   splinterdb_blob_release(NULL);
   message direct = message_create(sizeof(value), value);
   splinterdb_blob_release(&direct);
   ASSERT_TRUE(message_data(direct) == value);
   ASSERT_EQUAL(sizeof(value), message_length(direct));

   char other_filename[1024];
   int  length = snprintf(other_filename,
                         sizeof(other_filename),
                         "%s.blob-other",
                         data->cfg.filename);
   ASSERT_TRUE(length > 0 && length < sizeof(other_filename));
   data->data_cfg.merge_tuples       = blob_test_append;
   data->data_cfg.merge_tuples_final = blob_test_append_final;
   splinterdb_config other_cfg       = data->cfg;
   other_cfg.filename                = other_filename;
   splinterdb *other                 = NULL;
   ASSERT_EQUAL(0, splinterdb_create(&other_cfg, &other));
   message handle = {0};
   ASSERT_EQUAL(0,
                splinterdb_blob_create(
                   data->db, slice_create(sizeof(value), value), &handle));
   ASSERT_EQUAL(EINVAL,
                splinterdb_insert(other, slice_create(3, "key"), handle, NULL));
   ASSERT_EQUAL(EINVAL,
                splinterdb_update(other, slice_create(3, "key"), handle, NULL));
   splinterdb_blob_release(&handle);
   ASSERT_EQUAL(0, splinterdb_close(&other, FALSE));
   ASSERT_EQUAL(0, unlink(other_filename));
}

typedef struct blob_test_worker {
   splinterdb        *db;
   message            handle;
   pthread_barrier_t *barrier;
   uint64             begin;
   uint64             count;
   int                rc;
} blob_test_worker;

static void *
blob_test_insert_worker(void *arg)
{
   blob_test_worker *worker = arg;
   pthread_barrier_wait(worker->barrier);
   for (uint64 i = worker->begin; i < worker->begin + worker->count; i++) {
      worker->rc = splinterdb_insert(
         worker->db, slice_create(sizeof(i), &i), worker->handle, NULL);
      if (worker->rc != 0) {
         break;
      }
   }
   platform_deregister_thread();
   return NULL;
}

CTEST2(splinterdb_blob, test_concurrent_inserts_share_one_handle)
{
   data->cfg.use_log = TRUE;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->db));
   const uint64 size  = 2 * 128 * KiB + 17;
   char        *value = malloc(size);
   ASSERT_TRUE(value != NULL);
   memset(value, 0x52, size);
   message handle = {0};
   ASSERT_EQUAL(
      0, splinterdb_blob_create(data->db, slice_create(size, value), &handle));
   pthread_t         threads[4];
   blob_test_worker  workers[4];
   pthread_barrier_t barrier;
   ASSERT_EQUAL(0, pthread_barrier_init(&barrier, NULL, ARRAY_SIZE(threads)));
   for (unsigned i = 0; i < ARRAY_SIZE(threads); i++) {
      workers[i] = (blob_test_worker){
         .db      = data->db,
         .handle  = handle,
         .barrier = &barrier,
         .begin   = i * 16,
         .count   = 16,
      };
      ASSERT_EQUAL(0,
                   pthread_create(
                      &threads[i], NULL, blob_test_insert_worker, &workers[i]));
   }
   for (unsigned i = 0; i < ARRAY_SIZE(threads); i++) {
      ASSERT_EQUAL(0, pthread_join(threads[i], NULL));
      ASSERT_EQUAL(0, workers[i].rc);
   }
   ASSERT_EQUAL(0, pthread_barrier_destroy(&barrier));
   splinterdb_blob_release(&handle);
   ASSERT_EQUAL(0, splinterdb_durable_barrier(data->db, NULL));
   blob_test_check_shared_values(data->db, 64, slice_create(size, value));
   free(value);
}


CTEST2(splinterdb_blob, test_durable_reuse_and_orphans_after_process_exit)
{
   const uint64 update_count         = 2;
   const uint64 size                 = 2 * Mega + 17;
   const uint64 orphan_size          = 32 * Mega;
   const uint64 count                = 12;
   data->data_cfg.merge_tuples       = blob_test_append;
   data->data_cfg.merge_tuples_final = blob_test_append_final;
   char *value                       = malloc(size);
   ASSERT_TRUE(value != NULL);
   memset(value, 0x26, size);

   const char *child_mode = getenv("SPLINTER_TEST_PUBLIC_BLOB_CRASH_CHILD");
   if (child_mode != NULL) {
      data->cfg.use_log     = child_mode[0] == '1';
      const char *report_fd = getenv("SPLINTER_TEST_PUBLIC_BLOB_CRASH_FD");
      if (report_fd == NULL) {
         _exit(9);
      }
      splinterdb *db = NULL;
      if (splinterdb_create(&data->cfg, &db) != 0) {
         _exit(1);
      }
      message shared = {0};
      if (splinterdb_blob_create(db, slice_create(size, value), &shared) != 0) {
         _exit(2);
      }
      for (uint64 i = 0; i < count; i++) {
         if (splinterdb_insert(db, slice_create(sizeof(i), &i), shared, NULL)
             != 0)
         {
            _exit(3);
         }
      }
      for (uint64 i = 100; i < 100 + update_count; i++) {
         if (splinterdb_update(db, slice_create(sizeof(i), &i), shared, NULL)
             != 0)
         {
            _exit(11);
         }
      }
      if (splinterdb_durable_barrier(db, NULL) != 0) {
         _exit(4);
      }
      allocator *al = (allocator *)splinterdb_get_allocator_handle(db);
      uint64     before_orphan = blob_test_in_use_bytes(al);
      char      *orphan_value  = malloc(orphan_size);
      if (orphan_value == NULL) {
         _exit(5);
      }
      memset(orphan_value, 0x79, orphan_size);
      message orphan = {0};
      if (splinterdb_blob_create(
             db, slice_create(orphan_size, orphan_value), &orphan)
          != 0)
      {
         _exit(6);
      }
      if (blob_test_in_use_bytes(al) < before_orphan + orphan_size) {
         _exit(7);
      }
      if (write(atoi(report_fd), &before_orphan, sizeof(before_orphan))
          != sizeof(before_orphan))
      {
         _exit(8);
      }
      /* Deliberately retain both handles and skip clean close. The durable
       * keys own shared's data; the unreferenced blob has no recovery root. */
      _exit(0);
   }

   for (unsigned use_log = 0; use_log < 2; use_log++) {
      data->cfg.use_log = use_log;
      int report[2];
      ASSERT_EQUAL(0, pipe(report));
      pid_t child = fork();
      ASSERT_TRUE(child >= 0);
      if (child == 0) {
         close(report[0]);
         char fd_string[32];
         snprintf(fd_string, sizeof(fd_string), "%d", report[1]);
         setenv(
            "SPLINTER_TEST_PUBLIC_BLOB_CRASH_CHILD", use_log ? "1" : "0", 1);
         setenv("SPLINTER_TEST_PUBLIC_BLOB_CRASH_FD", fd_string, 1);
         /* A fresh process must own thread-registration state; reusing the
          * parent's initialized state across fork can affect later tests. */
         if (data->cfg.use_shmem) {
            execl("/proc/self/exe",
                  "splinterdb_blob_test",
                  "--use-shmem",
                  "test_durable_reuse_and_orphans_after_process_exit",
                  (char *)NULL);
         } else {
            execl("/proc/self/exe",
                  "splinterdb_blob_test",
                  "test_durable_reuse_and_orphans_after_process_exit",
                  (char *)NULL);
         }
         _exit(10);
      }
      close(report[1]);
      int status;
      ASSERT_EQUAL(child, waitpid(child, &status, 0));
      ASSERT_TRUE(WIFEXITED(status));
      ASSERT_EQUAL(0, WEXITSTATUS(status));
      uint64 before_orphan;
      ASSERT_EQUAL(sizeof(before_orphan),
                   read(report[0], &before_orphan, sizeof(before_orphan)));
      ASSERT_EQUAL(0, close(report[0]));
      ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->db));
      blob_test_check_shared_values(data->db, count, slice_create(size, value));
      splinterdb_lookup_result updated;
      splinterdb_lookup_result_init(data->db, &updated);
      for (uint64 i = 100; i < 100 + update_count; i++) {
         ASSERT_EQUAL(
            0,
            splinterdb_lookup(data->db, slice_create(sizeof(i), &i), &updated));
         blob_test_check_result(&updated, slice_create(size, value));
      }
      splinterdb_lookup_result_deinit(&updated);
      allocator *al = (allocator *)splinterdb_get_allocator_handle(data->db);
      /* Recovery creates fresh working trees/logs and may materialize the
       * two UPDATE messages. Allow that overhead; retaining the 32 MiB
       * unreachable object would exceed this bound. */
      ASSERT_TRUE(blob_test_in_use_bytes(al) < before_orphan + orphan_size / 2);
      ASSERT_EQUAL(0, splinterdb_close(&data->db, FALSE));
   }
   free(value);
}

CTEST2(splinterdb_blob, test_close_refuses_live_handles_and_can_retry)
{
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->db));
   message handle = {0};
   slice   value  = slice_create(5, "value");
   ASSERT_EQUAL(0, splinterdb_blob_create(data->db, value, &handle));
   splinterdb *original = data->db;
   ASSERT_EQUAL(EBUSY, splinterdb_close(&data->db, FALSE));
   ASSERT_TRUE(data->db == original);
   ASSERT_EQUAL(
      0, splinterdb_insert(data->db, slice_create(3, "key"), handle, NULL));
   splinterdb_blob_release(&handle);
   ASSERT_EQUAL(0, splinterdb_close(&data->db, FALSE));
   ASSERT_NULL(data->db);
   ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->db));
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(data->db, &result);
   ASSERT_EQUAL(0,
                splinterdb_lookup(data->db, slice_create(3, "key"), &result));
   blob_test_check_result(&result, value);
   splinterdb_lookup_result_deinit(&result);
}

CTEST2(splinterdb_blob, test_force_close_releases_live_handles)
{
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->db));
   const uint64 size        = 2 * 128 * KiB + 17;
   const uint64 orphan_size = 32 * Mega;
   char        *value       = malloc(orphan_size);
   ASSERT_TRUE(value != NULL);
   memset(value, 0x41, orphan_size);
   message shared = {0};
   ASSERT_EQUAL(
      0, splinterdb_blob_create(data->db, slice_create(size, value), &shared));
   for (uint64 i = 0; i < 12; i++) {
      ASSERT_EQUAL(0,
                   splinterdb_insert(
                      data->db, slice_create(sizeof(i), &i), shared, NULL));
   }
   ASSERT_EQUAL(0, splinterdb_durable_barrier(data->db, NULL));
   allocator *al = (allocator *)splinterdb_get_allocator_handle(data->db);
   uint64     before_orphan = blob_test_in_use_bytes(al);
   message    orphan        = {0};
   ASSERT_EQUAL(0,
                splinterdb_blob_create(
                   data->db, slice_create(orphan_size, value), &orphan));
   ASSERT_TRUE(blob_test_in_use_bytes(al) >= before_orphan + orphan_size);
   ASSERT_EQUAL(0, splinterdb_close(&data->db, TRUE));
   /* Forced close invalidated both caller pointers. */
   shared = message_create(0, NULL);
   orphan = message_create(0, NULL);
   ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->db));
   blob_test_check_shared_values(data->db, 12, slice_create(size, value));
   al = (allocator *)splinterdb_get_allocator_handle(data->db);
   ASSERT_TRUE(blob_test_in_use_bytes(al) < before_orphan + orphan_size / 2);
   free(value);
}

CTEST2(splinterdb_blob, test_reject_invalid_arguments)
{
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->db));
   message handle = {0};
   ASSERT_EQUAL(
      EINVAL, splinterdb_blob_create(data->db, slice_create(1, NULL), &handle));
   ASSERT_FALSE(message_is_blob(handle));
   ASSERT_EQUAL(EINVAL,
                splinterdb_blob_create(NULL, slice_create(0, NULL), &handle));
   ASSERT_FALSE(message_is_blob(handle));
   ASSERT_EQUAL(EINVAL,
                splinterdb_blob_create(data->db, slice_create(0, NULL), NULL));
   ASSERT_EQUAL(
      EINVAL,
      splinterdb_insert(
         data->db, slice_create(3, "key"), message_create(1, NULL), NULL));
}

CTEST2(splinterdb_blob, test_release_middle_head_and_tail_handles)
{
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->db));
   message handles[5] = {0};
   for (uint64 i = 0; i < ARRAY_SIZE(handles); i++) {
      char value = 'a' + i;
      ASSERT_EQUAL(0,
                   splinterdb_blob_create(data->db,
                                          slice_create(sizeof(value), &value),
                                          &handles[i]));
   }
   const unsigned order[] = {2, 4, 0, 1, 3};
   for (unsigned i = 0; i < ARRAY_SIZE(order); i++) {
      uint64 key = order[i];
      ASSERT_EQUAL(
         0,
         splinterdb_insert(
            data->db, slice_create(sizeof(key), &key), handles[key], NULL));
      splinterdb_blob_release(&handles[key]);
      ASSERT_FALSE(message_is_blob(handles[key]));
   }
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(data->db, &result);
   for (uint64 i = 0; i < ARRAY_SIZE(handles); i++) {
      char value = 'a' + i;
      ASSERT_EQUAL(
         0, splinterdb_lookup(data->db, slice_create(sizeof(i), &i), &result));
      blob_test_check_result(&result, slice_create(sizeof(value), &value));
   }
   splinterdb_lookup_result_deinit(&result);
   ASSERT_EQUAL(0, splinterdb_close(&data->db, FALSE));
}

CTEST2(splinterdb_blob, test_failed_creation_releases_allocations)
{
   data->cfg.disk_size = 64 * Mega;
   ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->db));
   allocator *al     = (allocator *)splinterdb_get_allocator_handle(data->db);
   uint64     before = blob_test_in_use_bytes(al);
   char       byte   = 'x';
   message    failed = {0};
   /* The bound must be checked before reading this deliberately short input. */
   ASSERT_EQUAL(
      ENOSPC,
      splinterdb_blob_create(
         data->db, slice_create(data->cfg.disk_size + 1, &byte), &failed));
   ASSERT_FALSE(message_is_blob(failed));
   ASSERT_EQUAL(before, blob_test_in_use_bytes(al));

   const uint64 size  = 4 * Mega;
   char        *value = malloc(size);
   ASSERT_TRUE(value != NULL);
   memset(value, 0x23, size);
   message  handles[32] = {0};
   unsigned count       = 0;
   int      rc          = 0;
   for (; count < ARRAY_SIZE(handles); count++) {
      rc = splinterdb_blob_create(
         data->db, slice_create(size, value), &handles[count]);
      if (rc != 0) {
         ASSERT_FALSE(message_is_blob(handles[count]));
         break;
      }
   }
   ASSERT_EQUAL(ENOSPC, rc);
   ASSERT_TRUE(count > 0 && count < ARRAY_SIZE(handles));
   for (unsigned i = 0; i < count; i++) {
      splinterdb_blob_release(&handles[i]);
   }
   ASSERT_EQUAL(before, blob_test_in_use_bytes(al));
   /* The failed creation did not poison subsequent allocator or list use. */
   ASSERT_EQUAL(
      0, splinterdb_blob_create(data->db, slice_create(size, value), &failed));
   splinterdb_blob_release(&failed);
   ASSERT_EQUAL(before, blob_test_in_use_bytes(al));
   free(value);
}

CTEST2(splinterdb_blob, test_same_message_for_insert_and_update)
{
   data->data_cfg.merge_tuples       = blob_test_append;
   data->data_cfg.merge_tuples_final = blob_test_append_final;
   const uint64 sizes[]              = {17, 2 * Mega + 17};
   for (unsigned use_log = 0; use_log < 2; use_log++) {
      data->cfg.use_log = use_log;
      for (unsigned variant = 0; variant < ARRAY_SIZE(sizes); variant++) {
         ASSERT_EQUAL(0, splinterdb_create(&data->cfg, &data->db));
         const uint64 length = sizes[variant];
         char        *bytes  = malloc(length + 4);
         ASSERT_TRUE(bytes != NULL);
         memcpy(bytes, "seed", 4);
         memset(bytes + 4, 'x', length);
         message payload = {0};
         ASSERT_EQUAL(0,
                      splinterdb_blob_create(
                         data->db, slice_create(length, bytes + 4), &payload));
         // Even a different message class must not change the called operation.
         payload.type  = MESSAGE_TYPE_DELETE;
         message alias = payload;

         slice direct_key = slice_create(6, "direct");
         slice absent_key = slice_create(6, "absent");
         slice older_key  = slice_create(5, "older");
         slice active_key = slice_create(6, "active");
         ASSERT_EQUAL(0,
                      splinterdb_insert(
                         data->db, older_key, message_create(4, bytes), NULL));
         ASSERT_EQUAL(0, splinterdb_optimize(data->db));
         ASSERT_EQUAL(
            0,
            splinterdb_insert(data->db,
                              active_key,
                              message_from_slice(slice_create(4, bytes)),
                              NULL));

         splinterdb_lookup_result old;
         splinterdb_lookup_result_init(data->db, &old);
         ASSERT_EQUAL(0, splinterdb_insert(data->db, direct_key, alias, &old));
         ASSERT_FALSE(splinterdb_lookup_found(&old));
         ASSERT_EQUAL(0, splinterdb_update(data->db, absent_key, alias, &old));
         ASSERT_FALSE(splinterdb_lookup_found(&old));
         ASSERT_EQUAL(0, splinterdb_update(data->db, older_key, alias, &old));
         blob_test_check_result(&old, slice_create(4, bytes));
         ASSERT_EQUAL(0, splinterdb_update(data->db, active_key, alias, &old));
         blob_test_check_result(&old, slice_create(4, bytes));
         splinterdb_lookup_result_deinit(&old);
         // Operations take message by value and must not consume or retag it.
         ASSERT_EQUAL(MESSAGE_TYPE_DELETE, message_class(payload));
         ASSERT_TRUE(message_is_blob(payload));
         // A copied message can carry the single caller-owned reference.
         splinterdb_blob_release(&alias);
         ASSERT_FALSE(message_is_blob(alias));
         ASSERT_EQUAL(0, message_length(alias));
         payload =
            message_create(0, NULL); // the borrowed original is now invalid

         for (unsigned phase = 0; phase < 3; phase++) {
            splinterdb_lookup_result result;
            splinterdb_lookup_result_init(data->db, &result);
            const slice keys[] = {
               direct_key, absent_key, older_key, active_key};
            for (unsigned i = 0; i < ARRAY_SIZE(keys); i++) {
               ASSERT_EQUAL(0, splinterdb_lookup(data->db, keys[i], &result));
               blob_test_check_result(&result,
                                      i < 2 ? slice_create(length, bytes + 4)
                                            : slice_create(length + 4, bytes));
            }
            splinterdb_lookup_result_deinit(&result);
            if (phase == 0) {
               ASSERT_EQUAL(
                  0,
                  splinterdb_optimize(data->db, .full_leaf_compactions = TRUE));
            } else if (phase == 1) {
               ASSERT_EQUAL(0, splinterdb_durable_barrier(data->db, NULL));
               ASSERT_EQUAL(0, splinterdb_close(&data->db, FALSE));
               ASSERT_EQUAL(0, splinterdb_open(&data->cfg, &data->db));
            }
         }
         free(bytes);
         ASSERT_EQUAL(0, splinterdb_close(&data->db, FALSE));
      }
   }
}
