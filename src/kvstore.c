// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * kvstore.c --
 *
 *     This file contains the implementation of external kvstore interfaces
 *     based on splinterdb
 */

#include "platform.h"
#include "kvstore.h"
#include "config.h"
#include "rc_allocator.h"
#include "clockcache.h"
#include "splinter.h"
#include "poison.h"

typedef struct KVStore {
  task_system *taskSystem;
  data_config dataCfg;
  io_config ioCfg;
  platform_io_handle ioHandle;
  rc_allocator_config allocatorCfg;
  rc_allocator allocatorHandle;
  clockcache_config cacheCfg;
  clockcache cacheHandle;
  allocator_root_id splinterId;
  splinter_config splinterCfg;
  splinter_handle *splinterHandle;
  platform_heap_handle heapHandle; // for platform_buffer_create
  platform_heap_id heapID;
} KVStore;

/*
 * FIXME
 * platform.h cannot yet be consumed outside of splinter. Temporary translation
 * of errors to int.
 */
static inline int platform_status_to_int(platform_status status) // IN
{
  if (!SUCCESS(status)) {
    return -1;
  }
  return 0;
}

/*
 *-----------------------------------------------------------------------------
 *
 * KVStoreInitConfig --
 *
 *      Translate KVStoreConfig to configs for individual subsystems.
 *
 * Results:
 *      STATUS_OK on success, appopriate error on failure.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

static platform_status KVStoreInitConfig(const KVStoreConfig *kvsCfg, // IN
                                         KVStore *kvs)                // OUT
{
  master_config masterCfg;

  if (kvsCfg->filename == NULL || kvsCfg->cacheSize == 0 ||
      kvsCfg->keySize == 0 || kvsCfg->dataSize == 0 ||
      kvsCfg->keyCompare == NULL || kvsCfg->keyHash == NULL ||
      kvsCfg->mergeTuples == NULL || kvsCfg->mergeTuplesFinal == NULL ||
      kvsCfg->diskSize == 0 || kvsCfg->messageClass == NULL ||
      kvsCfg->keyToStr == NULL || kvsCfg->messageToStr == NULL) {
    return STATUS_BAD_PARAM;
  }

  config_set_defaults(&masterCfg);
  snprintf(masterCfg.io_filename, sizeof(masterCfg.io_filename), "%s",
           kvsCfg->filename);
  masterCfg.allocator_capacity = kvsCfg->diskSize;
  masterCfg.cache_capacity = kvsCfg->cacheSize;
  masterCfg.use_log = FALSE;
  masterCfg.use_stats = TRUE;
  masterCfg.key_size = kvsCfg->keySize;
  masterCfg.message_size = kvsCfg->dataSize;

  kvs->dataCfg.key_size = kvsCfg->keySize;
  kvs->dataCfg.message_size = kvsCfg->dataSize;
  kvs->dataCfg.key_compare = kvsCfg->keyCompare;
  kvs->dataCfg.key_hash = kvsCfg->keyHash;
  kvs->dataCfg.message_class = kvsCfg->messageClass;
  kvs->dataCfg.merge_tuples = kvsCfg->mergeTuples;
  kvs->dataCfg.merge_tuples_final = kvsCfg->mergeTuplesFinal;
  kvs->dataCfg.key_to_string = kvsCfg->keyToStr;
  kvs->dataCfg.message_to_string = kvsCfg->messageToStr;
  memset(kvs->dataCfg.min_key, 0, kvs->dataCfg.key_size);
  memset(kvs->dataCfg.max_key, 0xff, kvs->dataCfg.key_size);

  kvs->heapHandle = kvsCfg->heapHandle;
  kvs->heapID = kvsCfg->heapID;

  io_config_init(&kvs->ioCfg, masterCfg.page_size, masterCfg.extent_size,
                 masterCfg.io_flags, masterCfg.io_perms,
                 masterCfg.io_async_queue_depth, masterCfg.io_filename);

  rc_allocator_config_init(&kvs->allocatorCfg, masterCfg.page_size,
                           masterCfg.extent_size, masterCfg.allocator_capacity);

  clockcache_config_init(&kvs->cacheCfg, masterCfg.page_size,
                         masterCfg.extent_size, masterCfg.cache_capacity,
                         masterCfg.cache_logfile, masterCfg.use_stats);

  splinter_config_init(&kvs->splinterCfg, &kvs->dataCfg, NULL,
                       masterCfg.memtable_capacity, masterCfg.fanout,
                       masterCfg.max_branches_per_node,
                       masterCfg.btree_rough_count_height, masterCfg.page_size,
                       masterCfg.extent_size, masterCfg.filter_remainder_size,
                       masterCfg.filter_index_size, masterCfg.reclaim_threshold,
                       masterCfg.use_log, masterCfg.use_stats);
  return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 *
 * KVStore_Init --
 *
 *      Init a KVStore.
 *
 *      Init splinter to use as a KVStore.  Relevant config parameters are
 *      provided via KVStoreConifg, which are translated to appropriate configs
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

int KVStore_Init(const KVStoreConfig *kvsCfg, // IN
                 KVStoreHandle *kvsHandle)    // OUT
{
  KVStore *kvs;
  platform_status status;

  platform_assert(kvsHandle != NULL);

  kvs = TYPED_ZALLOC(kvsCfg->heapID, kvs);
  if (kvs == NULL) {
    status = STATUS_NO_MEMORY;
    return platform_status_to_int(status);
  }

  status = KVStoreInitConfig(kvsCfg, kvs);
  if (!SUCCESS(status)) {
    platform_error_log("Failed to init io handle: %s\n",
                       platform_status_to_string(status));
    goto deinit_kvhandle;
  }

  status =
      io_handle_init(&kvs->ioHandle, &kvs->ioCfg, kvs->heapHandle, kvs->heapID);
  if (!SUCCESS(status)) {
    platform_error_log("Failed to init io handle: %s\n",
                       platform_status_to_string(status));
    goto deinit_kvhandle;
  }

  uint8 num_bg_threads[NUM_TASK_TYPES] = {0}; // no bg threads
  // FIXME: [aconway 2020-09-09] Not sure how to get use_stats from here
  status = task_system_create(kvs->heapID, &kvs->ioHandle, &kvs->taskSystem,
                              TRUE, FALSE, num_bg_threads, 0);
  if (!SUCCESS(status)) {
    platform_error_log("Failed to init splinter state: %s\n",
                       platform_status_to_string(status));
    goto deinit_iohandle;
  }

  status = rc_allocator_init(&kvs->allocatorHandle, &kvs->allocatorCfg,
                             (io_handle *)&kvs->ioHandle, kvs->heapHandle,
                             kvs->heapID, platform_get_module_id());
  if (!SUCCESS(status)) {
    platform_error_log("Failed to init allocator: %s\n",
                       platform_status_to_string(status));
    goto deinit_system;
  }

  status = clockcache_init(
      &kvs->cacheHandle, &kvs->cacheCfg, (io_handle *)&kvs->ioHandle,
      (allocator *)&kvs->allocatorHandle, "kvStore", kvs->taskSystem,
      kvs->heapHandle, kvs->heapID, platform_get_module_id());
  if (!SUCCESS(status)) {
    platform_error_log("Failed to init cache: %s\n",
                       platform_status_to_string(status));
    goto deinit_allocator;
  }

  kvs->splinterId = 1;
  kvs->splinterHandle =
      splinter_create(&kvs->splinterCfg, (allocator *)&kvs->allocatorHandle,
                      (cache *)&kvs->cacheHandle, kvs->taskSystem,
                      kvs->splinterId, kvs->heapID);
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
  task_system_destroy(kvs->heapID, kvs->taskSystem);
deinit_iohandle:
  io_handle_deinit(&kvs->ioHandle);
deinit_kvhandle:
  platform_free(kvsCfg->heapID, kvs);

  return platform_status_to_int(status);
}

/*
 *-----------------------------------------------------------------------------
 *
 * KVStore_Deinit --
 *
 *      Deinit a KVStore.
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

void KVStore_Deinit(KVStoreHandle kvsHandle) // IN
{
  KVStore *kvs = kvsHandle;

  platform_assert(kvs != NULL);

  splinter_destroy(kvs->splinterHandle);
  clockcache_deinit(&kvs->cacheHandle);
  rc_allocator_deinit(&kvs->allocatorHandle);
  io_handle_deinit(&kvs->ioHandle);
  task_system_destroy(kvs->heapID, kvs->taskSystem);
  platform_free(kvs->heapID, kvs);
}

/*
 *-----------------------------------------------------------------------------
 *
 * KVStore_RegisterThread --
 *
 *      Register a thread for KVStore operations. Needs to be called from the
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

void KVStore_RegisterThread(const KVStoreHandle kvsHandle) // IN
{
  KVStore *kvs = kvsHandle;

  platform_assert(kvs != NULL);
  task_system_register_thread(kvs->taskSystem);
}

/*
 *-----------------------------------------------------------------------------
 *
 * KVStore_Insert --
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

int KVStore_Insert(const KVStoreHandle kvsHandle, // IN
                   char *key,                     // IN
                   char *value)                   // IN
{
  KVStore *kvs = kvsHandle;
  platform_status status;

  platform_assert(kvs != NULL);
  status = splinter_insert(kvsHandle->splinterHandle, key, value);
  return platform_status_to_int(status);
}

/*
 *-----------------------------------------------------------------------------
 *
 * KVStore_Lookup --
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

int KVStore_Lookup(const KVStoreHandle kvsHandle, // IN
                   char *key,                     // IN
                   char *value,                   // OUT
                   bool *found)                   // OUT
{
  KVStore *kvs = kvsHandle;
  platform_status status;

  platform_assert(kvs != NULL);
  status = splinter_lookup(kvsHandle->splinterHandle, key, value, found);
  return platform_status_to_int(status);
}
