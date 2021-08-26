// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * kvstore.c --
 *
 *     This file contains the implementation of external kvstore interfaces
 *     based on splinterdb
 */

#include "platform.h"

#include "clockcache.h"
#include "config.h"
#include "kvstore.h"
#include "rc_allocator.h"
#include "splinter.h"

#include "poison.h"

typedef struct kvstore {
   task_system *        system;
   data_config          data_cfg;
   io_config            io_cfg;
   platform_io_handle   io_handle;
   rc_allocator_config  allocator_cfg;
   rc_allocator         allocator_handle;
   clockcache_config    cache_cfg;
   clockcache           cache_handle;
   allocator_root_id    splinter_id;
   splinter_config      splinter_cfg;
   splinter_handle *    spl;
   platform_heap_handle heap_handle; // for platform_buffer_create
   platform_heap_id     heap_id;
} kvstore;


/*
 * Extract errno.h -style status int from a platform_status
 *
 * Note this currently relies on the implementation of the splinterdb
 * platform_linux. But at least it doesn't leak the dependency to callers.
 */
static inline int
platform_status_to_int(const platform_status status) // IN
{
   return status.r;
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
kvstore_init_config(const kvstore_config *kvs_cfg, // IN
                    kvstore *             kvs      // OUT
)
{
   if (!data_validate_config(&kvs_cfg->data_cfg)) {
      platform_error_log("data_validate_config error\n");
      return STATUS_BAD_PARAM;
   }

   if (kvs_cfg->filename == NULL || kvs_cfg->cache_size == 0 ||
       kvs_cfg->disk_size == 0) {
      platform_error_log(
         "expect filename, cache_size and disk_size to be set\n");
      return STATUS_BAD_PARAM;
   }

   master_config masterCfg;
   config_set_defaults(&masterCfg);
   snprintf(masterCfg.io_filename,
            sizeof(masterCfg.io_filename),
            "%s",
            kvs_cfg->filename);
   masterCfg.allocator_capacity = kvs_cfg->disk_size;
   masterCfg.cache_capacity     = kvs_cfg->cache_size;
   masterCfg.use_log            = FALSE;
   masterCfg.use_stats          = TRUE;
   masterCfg.key_size           = kvs_cfg->data_cfg.key_size;
   masterCfg.message_size       = kvs_cfg->data_cfg.message_size;
   kvs->data_cfg                = kvs_cfg->data_cfg;

   // check if min_key and max_key are set
   if (0 == memcmp(kvs->data_cfg.min_key,
                   kvs->data_cfg.max_key,
                   sizeof(kvs->data_cfg.min_key))) {
      // application hasn't set them, so provide defaults
      memset(kvs->data_cfg.min_key, 0, kvs->data_cfg.key_size);
      memset(kvs->data_cfg.max_key, 0xff, kvs->data_cfg.key_size);
   }

   kvs->heap_handle = kvs_cfg->heap_handle;
   kvs->heap_id     = kvs_cfg->heap_id;

   io_config_init(&kvs->io_cfg,
                  masterCfg.page_size,
                  masterCfg.extent_size,
                  masterCfg.io_flags,
                  masterCfg.io_perms,
                  masterCfg.io_async_queue_depth,
                  masterCfg.io_filename);

   rc_allocator_config_init(&kvs->allocator_cfg,
                            masterCfg.page_size,
                            masterCfg.extent_size,
                            masterCfg.allocator_capacity);

   clockcache_config_init(&kvs->cache_cfg,
                          masterCfg.page_size,
                          masterCfg.extent_size,
                          masterCfg.cache_capacity,
                          masterCfg.cache_logfile,
                          masterCfg.use_stats);

   splinter_config_init(&kvs->splinter_cfg,
                        &kvs->data_cfg,
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
 *      kvs_cfg may be stack-allocated.  The reference is not retained.
 *
 *      TODO
 *      Txn, logging and mounting existing tables to be added in the future
 *
 * Results:
 *      0 on success, otherwise an errno
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

int
kvstore_init(const kvstore_config *kvs_cfg, // IN
             kvstore **            kvs_out  // OUT
)
{
   kvstore *       kvs;
   platform_status status;

   platform_assert(kvs_out != NULL);

   kvs = TYPED_ZALLOC(kvs_cfg->heap_id, kvs);
   if (kvs == NULL) {
      status = STATUS_NO_MEMORY;
      return platform_status_to_int(status);
   }

   status = kvstore_init_config(kvs_cfg, kvs);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init config: %s\n",
                         platform_status_to_string(status));
      goto deinit_kvhandle;
   }

   status = io_handle_init(
      &kvs->io_handle, &kvs->io_cfg, kvs->heap_handle, kvs->heap_id);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init io handle: %s\n",
                         platform_status_to_string(status));
      goto deinit_kvhandle;
   }

   uint8 num_bg_threads[NUM_TASK_TYPES] = {0}; // no bg threads
   // FIXME: [aconway 2020-09-09] Not sure how to get use_stats from here
   status = task_system_create(kvs->heap_id,
                               &kvs->io_handle,
                               &kvs->system,
                               TRUE,
                               FALSE,
                               num_bg_threads,
                               0);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init splinter state: %s\n",
                         platform_status_to_string(status));
      goto deinit_iohandle;
   }

   status = rc_allocator_init(&kvs->allocator_handle,
                              &kvs->allocator_cfg,
                              (io_handle *)&kvs->io_handle,
                              kvs->heap_handle,
                              kvs->heap_id,
                              platform_get_module_id());
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init allocator: %s\n",
                         platform_status_to_string(status));
      goto deinit_system;
   }

   status = clockcache_init(&kvs->cache_handle,
                            &kvs->cache_cfg,
                            (io_handle *)&kvs->io_handle,
                            (allocator *)&kvs->allocator_handle,
                            "kvStore",
                            kvs->system,
                            kvs->heap_handle,
                            kvs->heap_id,
                            platform_get_module_id());
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init cache: %s\n",
                         platform_status_to_string(status));
      goto deinit_allocator;
   }

   kvs->splinter_id = 1;
   kvs->spl         = splinter_create(&kvs->splinter_cfg,
                              (allocator *)&kvs->allocator_handle,
                              (cache *)&kvs->cache_handle,
                              kvs->system,
                              kvs->splinter_id,
                              kvs->heap_id);
   if (kvs->spl == NULL) {
      platform_error_log("Failed to init splinter\n");
      platform_assert(kvs->spl != NULL);
      goto deinit_cache;
   }

   *kvs_out = kvs;
   return platform_status_to_int(status);

deinit_cache:
   clockcache_deinit(&kvs->cache_handle);
deinit_allocator:
   rc_allocator_deinit(&kvs->allocator_handle);
deinit_system:
   task_system_destroy(kvs->heap_id, kvs->system);
deinit_iohandle:
   io_handle_deinit(&kvs->io_handle);
deinit_kvhandle:
   platform_free(kvs_cfg->heap_id, kvs);

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
kvstore_deinit(kvstore *kvs) // IN
{
   platform_assert(kvs != NULL);

   splinter_destroy(kvs->spl);
   clockcache_deinit(&kvs->cache_handle);
   rc_allocator_deinit(&kvs->allocator_handle);
   io_handle_deinit(&kvs->io_handle);
   task_system_destroy(kvs->heap_id, kvs->system);
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
kvstore_register_thread(const kvstore *kvs) // IN
{
   platform_assert(kvs != NULL);
   task_system_register_thread(kvs->system);
}


/*
 *-----------------------------------------------------------------------------
 *
 * kvstore_insert --
 *
 *      Insert a tuple into splinter
 *
 * Results:
 *      0 on success, otherwise an errno
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

int
kvstore_insert(const kvstore *kvs,  // IN
               char *         key,  // IN
               char *         value // IN
)
{
   platform_status status;

   platform_assert(kvs != NULL);
   status = splinter_insert(kvs->spl, key, value);
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
 *      0 on success, otherwise an errno
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

int
kvstore_lookup(const kvstore *kvs,   // IN
               char *         key,   // IN
               char *         value, // OUT
               bool *         found  // OUT
)
{
   platform_status status;

   platform_assert(kvs != NULL);
   status = splinter_lookup(kvs->spl, key, value, found);
   return platform_status_to_int(status);
}

struct kvstore_iterator {
   splinter_range_iterator sri;
   platform_status         last_rc;
};

int
kvstore_iterator_init(const kvstore *    kvs,      // IN
                      kvstore_iterator **iter,     // OUT
                      char *             start_key // IN
)
{
   kvstore_iterator *it = TYPED_MALLOC(kvs->spl->heap_id, it);
   if (it == NULL) {
      platform_error_log("TYPED_MALLOC error\n");
      return platform_status_to_int(STATUS_NO_MEMORY);
   }
   it->last_rc = STATUS_OK;

   splinter_range_iterator *range_itor = &(it->sri);

   platform_status rc = splinter_range_iterator_init(
      kvs->spl, range_itor, start_key, NULL, UINT64_MAX);
   if (!SUCCESS(rc)) {
      // TODO(gabe): copied in from splinter.c::splinter_range
      // but is this even right?  Like, if init fails, de-init
      // is typically a no-op?
      splinter_range_iterator_deinit(range_itor);
      platform_free(kvs->spl->heap_id, *iter);
      return platform_status_to_int(rc);
   }

   *iter = it;
   return EXIT_SUCCESS;
}

void
kvstore_iterator_deinit(kvstore_iterator *iter)
{
   splinter_range_iterator *range_itor = &(iter->sri);

   splinter_handle *spl = range_itor->spl;
   splinter_range_iterator_deinit(range_itor);
   platform_free(spl->heap_id, range_itor);
}

bool
kvstore_iterator_valid(kvstore_iterator *kvi)
{
   if (!SUCCESS(kvi->last_rc)) {
      return FALSE;
   }
   bool      at_end;
   iterator *itor = &(kvi->sri.super);
   kvi->last_rc   = iterator_at_end(itor, &at_end);
   if (!SUCCESS(kvi->last_rc)) {
      return FALSE;
   }
   return !at_end;
}

void
kvstore_iterator_next(kvstore_iterator *kvi)
{
   iterator *itor = &(kvi->sri.super);
   kvi->last_rc   = iterator_advance(itor);
}

void
kvstore_iterator_get_current(kvstore_iterator *kvi,    // IN
                             const char **     key,    // OUT
                             const char **     message // OUT
)
{
   iterator *itor = &(kvi->sri.super);
   iterator_get_curr(itor, (char **)key, (char **)message);
   // TODO(gabe): casting away the const is gross
   // Maybe we can change the signature of iterator_get_curr?
}

int
kvstore_iterator_status(const kvstore_iterator *iter)
{
   return platform_status_to_int(iter->last_rc);
}
