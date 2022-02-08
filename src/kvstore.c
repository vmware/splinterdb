// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *-----------------------------------------------------------------------------
 * kvstore.c --
 *
 *     This file contains the implementation of external kvstore interfaces
 *     based on splinterdb
 *
 *     Note: despite the name, the current API is centered around
 *     keys & _messages_, not keys & values.
 *
 *     The user must provide a data_config that encodes
 *     values into messages.
 *
 *     For simple use cases, start with kvstore_basic, which provides
 *     a key-value abstraction.
 *-----------------------------------------------------------------------------
 */

#include "platform.h"

#include "clockcache.h"
#include "config.h"
#include "splinterdb/kvstore.h"
#include "rc_allocator.h"
#include "splinter.h"
#include "data_internal.h"

#include "poison.h"

typedef struct kvstore {
   task_system         *task_sys;
   data_config          data_cfg;
   io_config            io_cfg;
   platform_io_handle   io_handle;
   rc_allocator_config  allocator_cfg;
   rc_allocator         allocator_handle;
   clockcache_config    cache_cfg;
   clockcache           cache_handle;
   allocator_root_id    splinter_id;
   splinter_config      splinter_cfg;
   splinter_handle     *spl;
   platform_heap_handle heap_handle; // for platform_buffer_create
   platform_heap_id     heap_id;
} kvstore;

/*
 * Provide a copy of default_data_config, just to get builds working.
 * This config data will have to be retrieved from the super-block eventually.
 */
static data_config default_data_config = {
   .key_size           = 24,
   .message_size       = 24,
   .min_key            = {0},
   .max_key            = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
               0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
               0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
   .key_compare        = default_data_key_cmp,
   .key_hash           = platform_hash32,
   .key_to_string      = default_data_key_to_string,
   .message_to_string  = default_data_message_to_string,
   .merge_tuples       = default_data_merge_tuples,
   .merge_tuples_final = default_data_merge_tuples_final,
   .message_class      = default_data_message_class,
};

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

/* Function prototypes */
static int
kvstore_create_or_open(const kvstore_config *kvs_cfg,      // IN
                       kvstore **            kvs_out,      // OUT
                       bool                  open_existing // IN
);

// kvstore_bootstrap_configs(kvstore_config *kvs_cfg, kvstore ** kvs);
static int
kvstore_bootstrap_configs(kvstore_config *kvs_cfg);

static platform_status
kvstore_bootstrap_init_config(kvstore_config *kvs_cfg, // IN, OUT
                              kvstore *       kvs);           // OUT

/* **** External Interfaces **** */

int
kvstore_create(const kvstore_config *cfg, // IN
               kvstore **            kvs  // OUT
)
{
   return kvstore_create_or_open(cfg, kvs, FALSE);
}

int
kvstore_open(const kvstore_config *cfg, // IN
             kvstore **            kvs  // OUT
)
{
   return kvstore_create_or_open(cfg, kvs, TRUE);
}

int
kvstore_reopen(kvstore **  kvs, // OUT
               const char *filename)
{
   kvstore_config ZERO_STRUCT_AT_DECL(kvs_cfg);

   // Init the kvstore config that was used to create the device previously
   kvs_cfg.filename = filename;

   /*
    * Provide some default data_config, just so that basic key / message
    * param lengths [etc.] are initialized while Splinter configuration
    * is done during bootstrapping. In this phase, it really does not
    * matter what the key/message function pointers are specified in this
    * default data_config, as we will really not be trying to access any
    * key / message values.
    */
   kvs_cfg.data_cfg = default_data_config;

   // This will crack open the Splinter device specified by filename, and
   // will extract required config options from the super block.
   // The target configuration is returned via kvs_cfg.
   kvstore_bootstrap_configs(&kvs_cfg);

   return kvstore_create_or_open(&kvs_cfg, kvs, TRUE);
}


/*
 *-----------------------------------------------------------------------------
 * kvstore_init_config --
 *
 *      Translate kvstore_config to configs for individual subsystems.
 *
 * Results:
 *      STATUS_OK on success, appopriate error on failure.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static platform_status
kvstore_init_config(const kvstore_config *kvs_cfg, // IN
                    kvstore              *kvs      // OUT
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

   master_config masterCfg;
   config_set_defaults(&masterCfg);
   snprintf(masterCfg.io_filename,
            sizeof(masterCfg.io_filename),
            "%s",
            kvs_cfg->filename);
   masterCfg.allocator_capacity = kvs_cfg->disk_size;
   masterCfg.cache_capacity     = kvs_cfg->cache_size;
   if (kvs_cfg->page_size) {
      // Both are either 0, or non-zero, together
      debug_assert(kvs_cfg->extent_size > 0);

      masterCfg.page_size   = kvs_cfg->page_size;
      masterCfg.extent_size = kvs_cfg->extent_size;
   }
   masterCfg.use_log            = FALSE;
   masterCfg.use_stats          = TRUE;
   masterCfg.key_size           = kvs_cfg->data_cfg.key_size;
   masterCfg.message_size       = kvs_cfg->data_cfg.message_size;
   kvs->data_cfg                = kvs_cfg->data_cfg;

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
 * Internal function for create or open
 */
static int
kvstore_create_or_open(const kvstore_config *kvs_cfg,      // IN
                       kvstore             **kvs_out,      // OUT
                       bool                  open_existing // IN
)
{
   kvstore        *kvs;
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
   status                               = task_system_create(kvs->heap_id,
                               &kvs->io_handle,
                               &kvs->task_sys,
                               TRUE,
                               FALSE,
                               num_bg_threads,
                               splinter_get_scratch_size());
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
                            "kvStore",
                            kvs->task_sys,
                            kvs->heap_handle,
                            kvs->heap_id,
                            platform_get_module_id());
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init cache: %s\n",
                         platform_status_to_string(status));
      goto deinit_allocator;
   }

   kvs->splinter_id = 1;
   if (open_existing) {
      kvs->spl = splinter_mount(&kvs->splinter_cfg,
                                (allocator *)&kvs->allocator_handle,
                                (cache *)&kvs->cache_handle,
                                kvs->task_sys,
                                kvs->splinter_id,
                                kvs->heap_id);
   } else {
      kvs->spl = splinter_create(&kvs->splinter_cfg,
                                 (allocator *)&kvs->allocator_handle,
                                 (cache *)&kvs->cache_handle,
                                 kvs->task_sys,
                                 kvs->splinter_id,
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

/*
 *-----------------------------------------------------------------------------
 * kvstore_boostrap_configs()
 *
 * Given a basic kvstore_config which names just the Splinter device, do minimal
 * setup of required sub-systems. Crack open the [existing] Splinter device and
 * extract out cardinal configs that are (should have been) written to the
 * super-block.
 *
 * This function is a synthesis of the work done under kvstore_create_or_open()
 * and kvstore_init_config(), adjusted to do only minimal work that is
 * necessary to get bootstrapping the super-block to succeed.
 *
 *-----------------------------------------------------------------------------
 */
static int
kvstore_bootstrap_configs(kvstore_config *kvs_cfg)
{
   platform_status status;

   kvstore *kvs;
   kvs = TYPED_ZALLOC(kvs_cfg->heap_id, kvs);
   if (kvs == NULL) {
      status = STATUS_NO_MEMORY;
      return platform_status_to_int(status);
   }

   status = kvstore_bootstrap_init_config(kvs_cfg, kvs);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to bootstrap init config: %s\n",
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

   status = rc_allocator_mount(&kvs->allocator_handle,
                               &kvs->allocator_cfg,
                               (io_handle *)&kvs->io_handle,
                               kvs->heap_handle,
                               kvs->heap_id,
                               platform_get_module_id());
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init allocator: %s\n",
                         platform_status_to_string(status));
      goto deinit_kvhandle;
   }

   status = clockcache_init(&kvs->cache_handle,
                            &kvs->cache_cfg,
                            (io_handle *)&kvs->io_handle,
                            (allocator *)&kvs->allocator_handle,
                            "kvStore",
                            kvs->task_sys,
                            kvs->heap_handle,
                            kvs->heap_id,
                            platform_get_module_id());
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init cache: %s\n",
                         platform_status_to_string(status));
      goto deinit_allocator;
   }

   kvs->splinter_id = 1;
   splinter_bootstrap(&kvs->splinter_cfg,
                      (allocator *)&kvs->allocator_handle,
                      (cache *)&kvs->cache_handle,
                      kvs->heap_id,
                      kvs->splinter_id,
                      &kvs_cfg->disk_size,
                      &kvs_cfg->cache_size);

   kvs_cfg->page_size   = kvs->splinter_cfg.page_size;
   kvs_cfg->extent_size = kvs->splinter_cfg.extent_size;

   // Before exiting, free the kvs struct allocated locally
   platform_free(kvs_cfg->heap_id, kvs);
   return platform_status_to_int(status);

   clockcache_deinit(&kvs->cache_handle);
deinit_allocator:
   rc_allocator_dismount(&kvs->allocator_handle);
deinit_kvhandle:
   platform_free(kvs_cfg->heap_id, kvs);

   // Before exiting, free the kvs struct allocated locally
   platform_free(kvs_cfg->heap_id, kvs);
   return platform_status_to_int(status);
}

/*
 *-----------------------------------------------------------------------------
 * kvstore_bootstrap_init_config() --
 *
 * Initialize the configuration for different sub-systems enough to get Splinter
 * through a bootstrapping process.
 *
 * NOTE: This is heavily derived from kvstore_init_config() and could
 * possibly be folded back into that, parametrizing under 'bootstrap'
 * flag.
 *-----------------------------------------------------------------------------
 */
static platform_status
kvstore_bootstrap_init_config(kvstore_config *kvs_cfg, // IN, OUT
                              kvstore *       kvs)            // OUT
{
   // Even though we are boostrapping, we expect the caller / application
   // to provide us a valid data config, giving key/value specs and
   // providing key/message fn-ptr handles.
   if (!data_validate_config(&kvs_cfg->data_cfg)) {
      platform_error_log("data_validate_config error\n");
      return STATUS_BAD_PARAM;
   }

   if (kvs_cfg->filename == NULL) {
      platform_error_log("Expect filename to be set.\n");
      return STATUS_BAD_PARAM;
   }

   // These two should not be set, as bootstrapping means finding out
   // these values from the super-block ( and setting them in the cfg )
   if ((kvs_cfg->cache_size != 0) || (kvs_cfg->disk_size != 0)) {
      platform_error_log(
         "Expect cache_size (%lu) and disk_size (%lu) to be clear\n",
         kvs_cfg->cache_size,
         kvs_cfg->disk_size);
      return STATUS_BAD_PARAM;
   }

   // Setup a master config struct w/ defaults, used to init configs for
   // other sub-systems. These defaults specify stuff about filters, key sizes
   // etc., but those really should not come into play while bootstrapping
   // the super-block. So ... we skate on thin ice ...
   master_config masterCfg;
   config_set_defaults(&masterCfg);
   snprintf(masterCfg.io_filename,
            sizeof(masterCfg.io_filename),
            "%s",
            kvs_cfg->filename);

   // RESOLVE: This one is tricky. Bootstrapping code path uses a default
   // data_config struct, which is good enough for tests. But for the real
   // usage scenario, the application will need to provide this struct.
   // And we don't quite have a way to validate that that's the correct one.
   kvs->data_cfg = kvs_cfg->data_cfg;

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
 * kvstore_close --
 *
 *      Close a kvstore, flushing to disk and releasing resources
 *      Initialize kvstore handle to NULL, to prevent caller dereferencing
 *      a now invalid kvstore handle.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
void
kvstore_close(kvstore **kvspp) // IN
{
   kvstore *kvs = *kvspp;
   platform_assert(kvs != NULL);

   splinter_dismount(&kvs->spl);
   clockcache_deinit(&kvs->cache_handle);
   rc_allocator_dismount(&kvs->allocator_handle);
   io_handle_deinit(&kvs->io_handle);
   task_system_destroy(kvs->heap_id, kvs->task_sys);

   platform_free(kvs->heap_id, kvs);
   *kvspp = (kvstore *)NULL;
}


/*
 *-----------------------------------------------------------------------------
 * kvstore_register_thread --
 *
 *      Allocate scratch space and register the current thread.
 *
 *      Any thread, other than the initializing thread, must call this function
 *      exactly once before using the kvstore.
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
kvstore_register_thread(kvstore *kvs) // IN
{
   platform_assert(kvs != NULL);

   size_t scratch_size = splinter_get_scratch_size();
   task_register_this_thread(kvs->task_sys, scratch_size);
}

/*
 *-----------------------------------------------------------------------------
 * kvstore_deregister_thread --
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
kvstore_deregister_thread(kvstore *kvs)
{
   platform_assert(kvs != NULL);

   task_deregister_this_thread(kvs->task_sys);
}


/*
 *-----------------------------------------------------------------------------
 * kvstore_insert --
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
kvstore_insert(const kvstore *kvs,            // IN
               char          *key,            // IN
               size_t         message_length, // IN
               char          *message         // IN
)
{
   platform_status status;
   slice           message_slice = slice_create(message_length, message);
   platform_assert(kvs != NULL);
   status = splinter_insert(kvs->spl, key, message_slice);
   return platform_status_to_int(status);
}

/*
 *-----------------------------------------------------------------------------
 * _kvstore_lookup_result structure --
 *-----------------------------------------------------------------------------
 */
typedef struct _kvstore_lookup_result {
   writable_buffer value;
} _kvstore_lookup_result;

_Static_assert(sizeof(_kvstore_lookup_result) <= sizeof(kvstore_lookup_result),
               "sizeof(kvstore_lookup_result) is too small");

void
kvstore_lookup_result_init(const kvstore         *kvs,        // IN
                           kvstore_lookup_result *result,     // IN/OUT
                           size_t                 buffer_len, // IN
                           char                  *buffer      // IN
)
{
   _kvstore_lookup_result *_result = (_kvstore_lookup_result *)result;
   writable_buffer_init(&_result->value, NULL, buffer_len, buffer);
}

void
kvstore_lookup_result_deinit(kvstore_lookup_result *result) // IN
{
   _kvstore_lookup_result *_result = (_kvstore_lookup_result *)result;
   writable_buffer_reinit(&_result->value);
}

_Bool
kvstore_lookup_result_found(kvstore_lookup_result *result) // IN
{
   _kvstore_lookup_result *_result = (_kvstore_lookup_result *)result;
   return splinter_lookup_found(&_result->value);
}

size_t
kvstore_lookup_result_size(kvstore_lookup_result *result) // IN
{
   _kvstore_lookup_result *_result = (_kvstore_lookup_result *)result;
   return writable_buffer_length(&_result->value);
}

void *
kvstore_lookup_result_data(kvstore_lookup_result *result) // IN
{
   _kvstore_lookup_result *_result = (_kvstore_lookup_result *)result;
   return writable_buffer_data(&_result->value);
}

/*
 *-----------------------------------------------------------------------------
 * kvstore_lookup --
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
kvstore_lookup(const kvstore         *kvs,    // IN
               char                  *key,    // IN
               kvstore_lookup_result *result) // OUT
{
   platform_status         status;
   _kvstore_lookup_result *_result = (_kvstore_lookup_result *)result;

   platform_assert(kvs != NULL);
   status = splinter_lookup(kvs->spl, key, &_result->value);
   return platform_status_to_int(status);
}

struct kvstore_iterator {
   splinter_range_iterator sri;
   platform_status         last_rc;
};

int
kvstore_iterator_init(const kvstore     *kvs,      // IN
                      kvstore_iterator **iter,     // OUT
                      char              *start_key // IN
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
kvstore_iterator_get_current(kvstore_iterator *kvi,            // IN
                             const char      **key,            // OUT
                             size_t           *message_length, // IN
                             const char      **message         // OUT
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
kvstore_iterator_status(const kvstore_iterator *iter)
{
   return platform_status_to_int(iter->last_rc);
}

/*
 * Lookup routines, to return KVStore configuration settings.
 */
uint64
kvstore_page_size(kvstore *kvs)
{
   return kvs->allocator_cfg.page_size;
}

uint64
kvstore_extent_size(kvstore *kvs)
{
   return kvs->allocator_cfg.extent_size;
}

uint64
kvstore_disk_size(kvstore *kvs)
{
   return kvs->allocator_cfg.capacity;
}

uint64
kvstore_cache_size(kvstore *kvs)
{
   return kvs->cache_cfg.capacity;
}
