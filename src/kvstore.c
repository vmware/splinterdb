// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * kvstore.c --
 *
 *     This file contains the implementation of external kvstore interfaces
 *     based on splinterdb
 */

#include "kvstore.h"
#include "clockcache.h"
#include "config.h"
#include "platform.h"
#include "poison.h"
#include "rc_allocator.h"
#include "splinter.h"

typedef struct kvstore {
   task_system *        taskSystem;
   data_config          dataCfg;
   io_config            ioCfg;
   platform_io_handle   ioHandle;
   rc_allocator_config  allocatorCfg;
   rc_allocator         allocatorHandle;
   clockcache_config    cacheCfg;
   clockcache           cacheHandle;
   allocator_root_id    splinterId;
   splinter_config      splinterCfg;
   splinter_handle *    splinterHandle;
   platform_heap_handle heap_handle; // for platform_buffer_create
   platform_heap_id     heap_id;
} kvstore;


/*
 * FIXME
 * platform.h cannot yet be consumed outside of splinter. Temporary translation
 * of errors to int.
 */
static inline int
platform_status_to_int(platform_status status) // IN
{
   if (!SUCCESS(status)) {
      return -1;
   }
   return 0;
}


/*
 *-----------------------------------------------------------------------------
 *
 * kvstore_init_config --
 *
 *      Translate kvstore_config to configs for individual subsystems.
 *
 * Results:
 *      STATUS_OK on success, appopriate error on failure.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

static platform_status
kvstore_init_config(const kvstore_config *kvsCfg, // IN
                    kvstore *             kvs)                 // OUT
{
   master_config masterCfg;

   if (kvsCfg->filename == NULL || kvsCfg->cache_size == 0 ||
       kvsCfg->key_size == 0 || kvsCfg->data_size == 0 ||
       kvsCfg->key_compare == NULL || kvsCfg->key_hash == NULL ||
       kvsCfg->merge_tuples == NULL || kvsCfg->merge_tuples_final == NULL ||
       kvsCfg->disk_size == 0 || kvsCfg->message_class == NULL ||
       kvsCfg->key_to_str == NULL || kvsCfg->message_to_str == NULL) {
      return STATUS_BAD_PARAM;
   }

   config_set_defaults(&masterCfg);
   snprintf(masterCfg.io_filename,
            sizeof(masterCfg.io_filename),
            "%s",
            kvsCfg->filename);
   masterCfg.allocator_capacity = kvsCfg->disk_size;
   masterCfg.cache_capacity     = kvsCfg->cache_size;
   masterCfg.use_log            = FALSE;
   masterCfg.use_stats          = TRUE;
   masterCfg.key_size           = kvsCfg->key_size;
   masterCfg.message_size       = kvsCfg->data_size;

   kvs->dataCfg.key_size           = kvsCfg->key_size;
   kvs->dataCfg.message_size       = kvsCfg->data_size;
   kvs->dataCfg.key_compare        = kvsCfg->key_compare;
   kvs->dataCfg.key_hash           = kvsCfg->key_hash;
   kvs->dataCfg.message_class      = kvsCfg->message_class;
   kvs->dataCfg.merge_tuples       = kvsCfg->merge_tuples;
   kvs->dataCfg.merge_tuples_final = kvsCfg->merge_tuples_final;
   kvs->dataCfg.key_to_string      = kvsCfg->key_to_str;
   kvs->dataCfg.message_to_string  = kvsCfg->message_to_str;
   memset(kvs->dataCfg.min_key, 0, kvs->dataCfg.key_size);
   memset(kvs->dataCfg.max_key, 0xff, kvs->dataCfg.key_size);

   kvs->heap_handle = kvsCfg->heap_handle;
   kvs->heap_id     = kvsCfg->heap_id;

   io_config_init(&kvs->ioCfg,
                  masterCfg.page_size,
                  masterCfg.extent_size,
                  masterCfg.io_flags,
                  masterCfg.io_perms,
                  masterCfg.io_async_queue_depth,
                  masterCfg.io_filename);

   rc_allocator_config_init(&kvs->allocatorCfg,
                            masterCfg.page_size,
                            masterCfg.extent_size,
                            masterCfg.allocator_capacity);

   clockcache_config_init(&kvs->cacheCfg,
                          masterCfg.page_size,
                          masterCfg.extent_size,
                          masterCfg.cache_capacity,
                          masterCfg.cache_logfile,
                          masterCfg.use_stats);

   splinter_config_init(&kvs->splinterCfg,
                        &kvs->dataCfg,
                        NULL,
                        masterCfg.memtable_capacity,
                        masterCfg.fanout,
                        masterCfg.max_branches_per_node,
                        masterCfg.btree_rough_count_height,
                        masterCfg.page_size,
                        masterCfg.extent_size,
                        masterCfg.filter_remainder_size,
                        masterCfg.filter_index_size,
                        masterCfg.reclaim_threshold,
                        masterCfg.use_log,
                        masterCfg.use_stats);
   return STATUS_OK;
}


/*
 *-----------------------------------------------------------------------------
 *
 * kvstore_init --
 *
 *      Init a kvstore.
 *
 *      Init splinter to use as a kvstore.  Relevant config parameters are
 *      provided via kvstore_config, which are translated to appropriate configs
 *      of each subsystem. For unspecified/internal parameters, defaults are
 *      used.
 *
 *      TODO
 *      Txn, logging and mounting existing tables to be added in the future
 *
 * Results:
 *      0 on success, -1 on failure
 *      FIXME
 *      Change to platform_status once it can be consumed outside
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

int
kvstore_init(const kvstore_config *kvsCfg, // IN
             kvstore_handle *      kvsHandle)    // OUT
{
   kvstore *       kvs;
   platform_status status;

   platform_assert(kvsHandle != NULL);

   kvs = TYPED_ZALLOC(kvsCfg->heap_id, kvs);
   if (kvs == NULL) {
      status = STATUS_NO_MEMORY;
      return platform_status_to_int(status);
   }

   status = kvstore_init_config(kvsCfg, kvs);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init io handle: %s\n",
                         platform_status_to_string(status));
      goto deinit_kvhandle;
   }

   status = io_handle_init(
      &kvs->ioHandle, &kvs->ioCfg, kvs->heap_handle, kvs->heap_id);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init io handle: %s\n",
                         platform_status_to_string(status));
      goto deinit_kvhandle;
   }

   uint8 num_bg_threads[NUM_TASK_TYPES] = {0}; // no bg threads
   // FIXME: [aconway 2020-09-09] Not sure how to get use_stats from here
   status = task_system_create(kvs->heap_id,
                               &kvs->ioHandle,
                               &kvs->taskSystem,
                               TRUE,
                               FALSE,
                               num_bg_threads,
                               0);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init splinter state: %s\n",
                         platform_status_to_string(status));
      goto deinit_iohandle;
   }

   status = rc_allocator_init(&kvs->allocatorHandle,
                              &kvs->allocatorCfg,
                              (io_handle *)&kvs->ioHandle,
                              kvs->heap_handle,
                              kvs->heap_id,
                              platform_get_module_id());
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init allocator: %s\n",
                         platform_status_to_string(status));
      goto deinit_system;
   }

   status = clockcache_init(&kvs->cacheHandle,
                            &kvs->cacheCfg,
                            (io_handle *)&kvs->ioHandle,
                            (allocator *)&kvs->allocatorHandle,
                            "kvStore",
                            kvs->taskSystem,
                            kvs->heap_handle,
                            kvs->heap_id,
                            platform_get_module_id());
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init cache: %s\n",
                         platform_status_to_string(status));
      goto deinit_allocator;
   }

   kvs->splinterId     = 1;
   kvs->splinterHandle = splinter_create(&kvs->splinterCfg,
                                         (allocator *)&kvs->allocatorHandle,
                                         (cache *)&kvs->cacheHandle,
                                         kvs->taskSystem,
                                         kvs->splinterId,
                                         kvs->heap_id);
   if (kvs->splinterHandle == NULL) {
      platform_error_log("Failed to init splinter\n");
      platform_assert(kvs->splinterHandle != NULL);
      goto deinit_cache;
   }

   *kvsHandle = kvs;
   return platform_status_to_int(status);

deinit_cache:
   clockcache_deinit(&kvs->cacheHandle);
deinit_allocator:
   rc_allocator_deinit(&kvs->allocatorHandle);
deinit_system:
   task_system_destroy(kvs->heap_id, kvs->taskSystem);
deinit_iohandle:
   io_handle_deinit(&kvs->ioHandle);
deinit_kvhandle:
   platform_free(kvsCfg->heap_id, kvs);

   return platform_status_to_int(status);
}


/*
 *-----------------------------------------------------------------------------
 *
 * kvstore_deinit --
 *
 *      Deinit a kvstore.
 *
 *      TODO
 *      Unmount support to be added in the future
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

void
kvstore_deinit(kvstore_handle kvsHandle) // IN
{
   kvstore *kvs = kvsHandle;

   platform_assert(kvs != NULL);

   splinter_destroy(kvs->splinterHandle);
   clockcache_deinit(&kvs->cacheHandle);
   rc_allocator_deinit(&kvs->allocatorHandle);
   io_handle_deinit(&kvs->ioHandle);
   task_system_destroy(kvs->heap_id, kvs->taskSystem);
   platform_free(kvs->heap_id, kvs);
}


/*
 *-----------------------------------------------------------------------------
 *
 * kvstore_register_thread --
 *
 *      Register a thread for kvstore operations. Needs to be called from the
 *      threads execution context.
 *
 *      This function must be called by a thread before it performs any
 *      KV operations.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

void
kvstore_register_thread(const kvstore_handle kvsHandle) // IN
{
   kvstore *kvs = kvsHandle;

   platform_assert(kvs != NULL);
   task_system_register_thread(kvs->taskSystem);
}


/*
 *-----------------------------------------------------------------------------
 *
 * kvstore_insert --
 *
 *      Insert a tuple into splinter
 *
 * Results:
 *      0 on success, -1 on failure
 *      FIXME
 *      Change to platform_status once it can be consumed outside
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

int
kvstore_insert(const kvstore_handle kvsHandle, // IN
               char *               key,       // IN
               char *               value)                    // IN
{
   kvstore *       kvs = kvsHandle;
   platform_status status;

   platform_assert(kvs != NULL);
   status = splinter_insert(kvsHandle->splinterHandle, key, value);
   return platform_status_to_int(status);
}


/*
 *-----------------------------------------------------------------------------
 *
 * kvstore_lookup --
 *
 *      Look up a key from splinter
 *
 * Results:
 *      0 on success, -1 on failure
 *      FIXME
 *      Change to platform_status once it can be consumed outside
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

int
kvstore_lookup(const kvstore_handle kvsHandle, // IN
               char *               key,       // IN
               char *               value,     // OUT
               bool *               found)                    // OUT
{
   kvstore *       kvs = kvsHandle;
   platform_status status;

   platform_assert(kvs != NULL);
   status = splinter_lookup(kvsHandle->splinterHandle, key, value, found);
   return platform_status_to_int(status);
}
