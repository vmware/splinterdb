// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *-----------------------------------------------------------------------------
 * splinterdb.c --
 *
 *     Implementation of the key/message API to SplinterDB
 *
 *     The user must provide a data_config that encodes values into messages.
 *
 *     For simple use cases, start with splinterdb_kv, which provides
 *     a key-value abstraction.
 *-----------------------------------------------------------------------------
 */

#include "platform.h"

#include "clockcache.h"
#include "splinterdb/splinterdb.h"
#include "rc_allocator.h"
#include "trunk.h"

#include "poison.h"

const char *BUILD_VERSION = "splinterdb_build_version " GIT_VERSION;

typedef struct splinterdb {
   task_system         *task_sys;
   data_config          data_cfg;
   io_config            io_cfg;
   platform_io_handle   io_handle;
   rc_allocator_config  allocator_cfg;
   rc_allocator         allocator_handle;
   clockcache_config    cache_cfg;
   clockcache           cache_handle;
   allocator_root_id    trunk_id;
   trunk_config         trunk_cfg;
   trunk_handle        *spl;
   platform_heap_handle heap_handle; // for platform_buffer_create
   platform_heap_id     heap_id;
} splinterdb;


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


static void splinterdb_config_set_defaults(splinterdb_config* cfg) {
   if (!cfg->page_size) {
      cfg->page_size = 4096;
   }
   if (!cfg->extent_size) {
      cfg->extent_size = 128 * 1024;
   }
   if (!cfg->io_flags) {
      cfg->io_flags =  O_RDWR | O_CREAT;
   }
   if (!cfg->io_perms) {
      cfg->io_perms =  0755;
   }

   if (!cfg->io_async_queue_depth) {
      cfg-> io_async_queue_depth=  256;
   }

   if (!cfg->btree_rough_count_height) {
      cfg-> btree_rough_count_height=  1;
   }

   if (!cfg->filter_index_size) {
      cfg-> filter_index_size=  256;
   }
   if (!cfg->filter_remainder_size) {
      cfg-> filter_remainder_size=  6;
   }

   if (!cfg->memtable_capacity) {
      cfg-> memtable_capacity=  MiB_TO_B(24) ;
   }
   if (!cfg->fanout) {
      cfg-> fanout=  8;
   }
   if (!cfg->max_branches_per_node) {
      cfg-> max_branches_per_node=  24;
   }
   if (!cfg->reclaim_threshold) {
      cfg-> reclaim_threshold=  UINT64_MAX;
   }
}




/*
 *-----------------------------------------------------------------------------
 * splinterdb_init_config --
 *
 *      Translate splinterdb_config to configs for individual subsystems.
 *
 * Results:
 *      STATUS_OK on success, appopriate error on failure.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static platform_status
splinterdb_init_config(const splinterdb_config *kvs_cfg, // IN
                       splinterdb              *kvs      // OUT
)
{
   if (!data_validate_config(&kvs_cfg->data_cfg)) {
      platform_error_log("data_validate_config error\n");
      return STATUS_BAD_PARAM;
   }

   if (kvs_cfg->filename == NULL || kvs_cfg->cache_size == 0
       || kvs_cfg->disk_size == 0)
   {
      platform_error_log(
         "expect filename, cache_size and disk_size to be set\n");
      return STATUS_BAD_PARAM;
   }

   // mutable local config block, where we can set defaults
   splinterdb_config cfg = {0};
   memcpy(&cfg, kvs_cfg, sizeof(cfg));
   splinterdb_config_set_defaults(&cfg);

   kvs->data_cfg = cfg.data_cfg;
   // check if min_key and max_key are set
   if (0
       == memcmp(kvs->data_cfg.min_key,
                 kvs->data_cfg.max_key,
                 sizeof(kvs->data_cfg.min_key)))
   {
      // application hasn't set them, so provide defaults
      memset(kvs->data_cfg.min_key, 0, kvs->data_cfg.key_size);
      memset(kvs->data_cfg.max_key, 0xff, kvs->data_cfg.key_size);
   }

   kvs->heap_handle = cfg.heap_handle;
   kvs->heap_id     = cfg.heap_id;

   io_config_init(&kvs->io_cfg,
                  cfg.page_size,
                  cfg.extent_size,
                  cfg.io_flags,
                  cfg.io_perms,
                  cfg.io_async_queue_depth,
                  cfg.filename);

   rc_allocator_config_init(&kvs->allocator_cfg,
                            cfg.page_size,
                            cfg.extent_size,
                            cfg.disk_size);

   clockcache_config_init(&kvs->cache_cfg,
                          cfg.page_size,
                          cfg.extent_size,
                          cfg.cache_size,
                          cfg.cache_logfile,
                          cfg.use_stats);

   trunk_config_init(&kvs->trunk_cfg,
                     &kvs->data_cfg,
                     NULL,
                     cfg.memtable_capacity,
                     cfg.fanout,
                     cfg.max_branches_per_node,
                     cfg.btree_rough_count_height,
                     cfg.page_size,
                     cfg.extent_size,
                     cfg.filter_remainder_size,
                     cfg.filter_index_size,
                     cfg.reclaim_threshold,
                     cfg.use_log,
                     cfg.use_stats);
   return STATUS_OK;
}


/*
 * Internal function for create or open
 */
int
splinterdb_create_or_open(const splinterdb_config *kvs_cfg,      // IN
                          splinterdb             **kvs_out,      // OUT
                          bool                     open_existing // IN
)
{
   splinterdb     *kvs;
   platform_status status;

   platform_assert(kvs_out != NULL);

   kvs = TYPED_ZALLOC(kvs_cfg->heap_id, kvs);
   if (kvs == NULL) {
      status = STATUS_NO_MEMORY;
      return platform_status_to_int(status);
   }

   status = splinterdb_init_config(kvs_cfg, kvs);
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
   status                               = task_system_create(kvs->heap_id,
                               &kvs->io_handle,
                               &kvs->task_sys,
                               TRUE,
                               FALSE,
                               num_bg_threads,
                               trunk_get_scratch_size());
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init splinter state: %s\n",
                         platform_status_to_string(status));
      goto deinit_iohandle;
   }

   if (open_existing) {
      status = rc_allocator_mount(&kvs->allocator_handle,
                                  &kvs->allocator_cfg,
                                  (io_handle *)&kvs->io_handle,
                                  kvs->heap_handle,
                                  kvs->heap_id,
                                  platform_get_module_id());
   } else {
      status = rc_allocator_init(&kvs->allocator_handle,
                                 &kvs->allocator_cfg,
                                 (io_handle *)&kvs->io_handle,
                                 kvs->heap_handle,
                                 kvs->heap_id,
                                 platform_get_module_id());
   }
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init allocator: %s\n",
                         platform_status_to_string(status));
      goto deinit_system;
   }

   status = clockcache_init(&kvs->cache_handle,
                            &kvs->cache_cfg,
                            (io_handle *)&kvs->io_handle,
                            (allocator *)&kvs->allocator_handle,
                            "splinterdb",
                            kvs->task_sys,
                            kvs->heap_handle,
                            kvs->heap_id,
                            platform_get_module_id());
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init cache: %s\n",
                         platform_status_to_string(status));
      goto deinit_allocator;
   }

   kvs->trunk_id = 1;
   if (open_existing) {
      kvs->spl = trunk_mount(&kvs->trunk_cfg,
                             (allocator *)&kvs->allocator_handle,
                             (cache *)&kvs->cache_handle,
                             kvs->task_sys,
                             kvs->trunk_id,
                             kvs->heap_id);
   } else {
      kvs->spl = trunk_create(&kvs->trunk_cfg,
                              (allocator *)&kvs->allocator_handle,
                              (cache *)&kvs->cache_handle,
                              kvs->task_sys,
                              kvs->trunk_id,
                              kvs->heap_id);
   }
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
   rc_allocator_dismount(&kvs->allocator_handle);
deinit_system:
   task_system_destroy(kvs->heap_id, kvs->task_sys);
deinit_iohandle:
   io_handle_deinit(&kvs->io_handle);
deinit_kvhandle:
   platform_free(kvs_cfg->heap_id, kvs);

   return platform_status_to_int(status);
}

int
splinterdb_create(const splinterdb_config *cfg, // IN
                  splinterdb             **kvs  // OUT
)
{
   return splinterdb_create_or_open(cfg, kvs, FALSE);
}

int
splinterdb_open(const splinterdb_config *cfg, // IN
                splinterdb             **kvs  // OUT
)
{
   return splinterdb_create_or_open(cfg, kvs, TRUE);
}


/*
 *-----------------------------------------------------------------------------
 * splinterdb_close --
 *
 *      Close a splinterdb, flushing to disk and releasing resources
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
void
splinterdb_close(splinterdb *kvs) // IN
{
   platform_assert(kvs != NULL);

   trunk_dismount(kvs->spl);
   clockcache_deinit(&kvs->cache_handle);
   rc_allocator_dismount(&kvs->allocator_handle);
   io_handle_deinit(&kvs->io_handle);
   task_system_destroy(kvs->heap_id, kvs->task_sys);

   platform_free(kvs->heap_id, kvs);
}


/*
 *-----------------------------------------------------------------------------
 * splinterdb_register_thread --
 *
 *      Allocate scratch space and register the current thread.
 *
 *      Any thread, other than the initializing thread, must call this function
 *      exactly once before using the splinterdb.
 *
 *      Notes:
 *      - The task system imposes a limit of MAX_THREADS live at any time
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Allocates memory
 *-----------------------------------------------------------------------------
 */
void
splinterdb_register_thread(splinterdb *kvs) // IN
{
   platform_assert(kvs != NULL);

   size_t scratch_size = trunk_get_scratch_size();
   task_register_this_thread(kvs->task_sys, scratch_size);
}

/*
 *-----------------------------------------------------------------------------
 * splinterdb_deregister_thread --
 *
 *      Free scratch space.
 *      Call this function before exiting a registered thread.
 *      Otherwise, you'll leak memory.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Frees memory
 *-----------------------------------------------------------------------------
 */
void
splinterdb_deregister_thread(splinterdb *kvs)
{
   platform_assert(kvs != NULL);

   task_deregister_this_thread(kvs->task_sys);
}


/*
 *-----------------------------------------------------------------------------
 * splinterdb_insert --
 *
 *      Insert a tuple into splinter
 *
 * Results:
 *      0 on success, otherwise an errno
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
int
splinterdb_insert(const splinterdb *kvs,            // IN
                  char             *key,            // IN
                  size_t            message_length, // IN
                  char             *message         // IN
)
{
   platform_status status;
   slice           message_slice = slice_create(message_length, message);
   platform_assert(kvs != NULL);
   status = trunk_insert(kvs->spl, key, message_slice);
   return platform_status_to_int(status);
}

/*
 *-----------------------------------------------------------------------------
 * _splinterdb_lookup_result structure --
 *-----------------------------------------------------------------------------
 */
typedef struct _splinterdb_lookup_result {
   writable_buffer value;
} _splinterdb_lookup_result;

_Static_assert(sizeof(_splinterdb_lookup_result)
                  <= sizeof(splinterdb_lookup_result),
               "sizeof(splinterdb_lookup_result) is too small");

void
splinterdb_lookup_result_init(const splinterdb         *kvs,        // IN
                              splinterdb_lookup_result *result,     // IN/OUT
                              size_t                    buffer_len, // IN
                              char                     *buffer      // IN
)
{
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
   writable_buffer_init(&_result->value, NULL, buffer_len, buffer);
}

void
splinterdb_lookup_result_deinit(splinterdb_lookup_result *result) // IN
{
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
   writable_buffer_reinit(&_result->value);
}

_Bool
splinterdb_lookup_result_found(splinterdb_lookup_result *result) // IN
{
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
   return trunk_lookup_found(&_result->value);
}

size_t
splinterdb_lookup_result_size(splinterdb_lookup_result *result) // IN
{
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
   return writable_buffer_length(&_result->value);
}

void *
splinterdb_lookup_result_data(splinterdb_lookup_result *result) // IN
{
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
   return writable_buffer_data(&_result->value);
}

/*
 *-----------------------------------------------------------------------------
 * splinterdb_lookup --
 *
 *      Look up a key from splinter
 *
 * Results:
 *      0 on success, otherwise and error number
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
int
splinterdb_lookup(const splinterdb         *kvs,    // IN
                  char                     *key,    // IN
                  splinterdb_lookup_result *result) // OUT
{
   platform_status            status;
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;

   platform_assert(kvs != NULL);
   status = trunk_lookup(kvs->spl, key, &_result->value);
   return platform_status_to_int(status);
}

struct splinterdb_iterator {
   trunk_range_iterator sri;
   platform_status      last_rc;
};

int
splinterdb_iterator_init(const splinterdb     *kvs,      // IN
                         splinterdb_iterator **iter,     // OUT
                         char                 *start_key // IN
)
{
   splinterdb_iterator *it = TYPED_MALLOC(kvs->spl->heap_id, it);
   if (it == NULL) {
      platform_error_log("TYPED_MALLOC error\n");
      return platform_status_to_int(STATUS_NO_MEMORY);
   }
   it->last_rc = STATUS_OK;

   trunk_range_iterator *range_itor = &(it->sri);

   platform_status rc = trunk_range_iterator_init(
      kvs->spl, range_itor, start_key, NULL, UINT64_MAX);
   if (!SUCCESS(rc)) {
      trunk_range_iterator_deinit(range_itor);
      platform_free(kvs->spl->heap_id, *iter);
      return platform_status_to_int(rc);
   }

   *iter = it;
   return EXIT_SUCCESS;
}

void
splinterdb_iterator_deinit(splinterdb_iterator *iter)
{
   trunk_range_iterator *range_itor = &(iter->sri);

   trunk_handle *spl = range_itor->spl;
   trunk_range_iterator_deinit(range_itor);
   platform_free(spl->heap_id, range_itor);
}

bool
splinterdb_iterator_valid(splinterdb_iterator *kvi)
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
splinterdb_iterator_next(splinterdb_iterator *kvi)
{
   iterator *itor = &(kvi->sri.super);
   kvi->last_rc   = iterator_advance(itor);
}

void
splinterdb_iterator_get_current(splinterdb_iterator *kvi,            // IN
                                const char         **key,            // OUT
                                size_t              *message_length, // IN
                                const char         **message         // OUT
)
{
   slice     key_slice;
   slice     message_slice;
   iterator *itor = &(kvi->sri.super);
   iterator_get_curr(itor, &key_slice, &message_slice);
   *key            = slice_data(key_slice);
   *message_length = slice_length(message_slice);
   *message        = slice_data(message_slice);
}

int
splinterdb_iterator_status(const splinterdb_iterator *iter)
{
   return platform_status_to_int(iter->last_rc);
}
