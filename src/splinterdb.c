// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *-----------------------------------------------------------------------------
 * splinterdb.c --
 *
 *     Implementation of SplinterDB's public API
 *
 *     The user must provide a data_config that encodes values into messages.
 *     A simple default data_config is available in default_data_config.h
 *
 *-----------------------------------------------------------------------------
 */

#include "splinterdb/splinterdb.h"
#include "platform.h"
#include "clockcache.h"
#include "rc_allocator.h"
#include "trunk.h"
#include "btree_private.h"
#include "shard_log.h"
#include "poison.h"

const char *BUILD_VERSION = "splinterdb_build_version " GIT_VERSION;
const char *
splinterdb_get_version()
{
   return BUILD_VERSION;
}

typedef struct splinterdb {
   task_system         *task_sys;
   io_config            io_cfg;
   platform_io_handle   io_handle;
   allocator_config     allocator_cfg;
   rc_allocator         allocator_handle;
   clockcache_config    cache_cfg;
   clockcache           cache_handle;
   shard_log_config     log_cfg;
   task_system_config   task_cfg;
   allocator_root_id    trunk_id;
   trunk_config         trunk_cfg;
   trunk_handle        *spl;
   platform_heap_handle heap_handle; // for platform_buffer_create
   platform_heap_id     heap_id;
   data_config         *data_cfg;
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


static void
splinterdb_config_set_defaults(splinterdb_config *cfg)
{
   if (!cfg->page_size) {
      cfg->page_size = LAIO_DEFAULT_PAGE_SIZE;
   }
   if (!cfg->extent_size) {
      cfg->extent_size = LAIO_DEFAULT_EXTENT_SIZE;
   }
   if (!cfg->io_flags) {
      cfg->io_flags = O_RDWR | O_CREAT;
   }
   if (!cfg->io_perms) {
      cfg->io_perms = 0755;
   }

   if (!cfg->io_async_queue_depth) {
      cfg->io_async_queue_depth = 256;
   }

   if (!cfg->btree_rough_count_height) {
      cfg->btree_rough_count_height = 1;
   }

   if (!cfg->filter_index_size) {
      cfg->filter_index_size = 512;
   }
   if (!cfg->filter_remainder_size) {
      cfg->filter_remainder_size = 4;
   }

   if (!cfg->memtable_capacity) {
      cfg->memtable_capacity = MiB_TO_B(24);
   }
   if (!cfg->fanout) {
      cfg->fanout = 8;
   }
   if (!cfg->max_branches_per_node) {
      cfg->max_branches_per_node = 24;
   }
   if (!cfg->reclaim_threshold) {
      cfg->reclaim_threshold = UINT64_MAX;
   }
}

static platform_status
splinterdb_validate_app_data_config(const data_config *cfg)
{
   platform_assert(cfg->max_key_size > 0);
   platform_assert(cfg->key_compare != NULL);
   platform_assert(cfg->key_hash != NULL);
   platform_assert(cfg->key_to_string != NULL);
   platform_assert(cfg->message_to_string != NULL);

   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 * splinterdb_init_config --
 *
 *      Translate splinterdb_config to configs for individual subsystems.
 *
 *      The resulting splinterdb object will retain a reference to data_config
 *      So kvs_cfg->data_config must live at least that long.
 *
 * Results:
 *      STATUS_OK on success, appropriate error on failure.
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
   platform_status rc = STATUS_OK;

   rc = splinterdb_validate_app_data_config(kvs_cfg->data_cfg);
   if (!SUCCESS(rc)) {
      return rc;
   }
   kvs->data_cfg = kvs_cfg->data_cfg;

   if (kvs_cfg->filename == NULL || kvs_cfg->cache_size == 0
       || kvs_cfg->disk_size == 0)
   {
      platform_error_log(
         "Expect filename, cache_size and disk_size to be set.\n");
      return STATUS_BAD_PARAM;
   }

   // mutable local config block, where we can set defaults
   splinterdb_config cfg = {0};
   memcpy(&cfg, kvs_cfg, sizeof(cfg));
   splinterdb_config_set_defaults(&cfg);

   kvs->heap_handle = cfg.heap_handle;
   kvs->heap_id     = cfg.heap_id;

   io_config_init(&kvs->io_cfg,
                  cfg.page_size,
                  cfg.extent_size,
                  cfg.io_flags,
                  cfg.io_perms,
                  cfg.io_async_queue_depth,
                  cfg.filename);

   // Validate IO-configuration parameters
   rc = laio_config_valid(&kvs->io_cfg);
   if (!SUCCESS(rc)) {
      return rc;
   }

   allocator_config_init(&kvs->allocator_cfg, &kvs->io_cfg, cfg.disk_size);

   clockcache_config_init(&kvs->cache_cfg,
                          &kvs->io_cfg,
                          cfg.cache_size,
                          cfg.cache_logfile,
                          cfg.use_stats);

   shard_log_config_init(&kvs->log_cfg, &kvs->cache_cfg.super, kvs->data_cfg);

   uint64 num_bg_threads[NUM_TASK_TYPES] = {0};
   num_bg_threads[TASK_TYPE_MEMTABLE]    = kvs_cfg->num_memtable_bg_threads;
   num_bg_threads[TASK_TYPE_NORMAL]      = kvs_cfg->num_normal_bg_threads;

   rc = task_system_config_init(
      &kvs->task_cfg, cfg.use_stats, num_bg_threads, trunk_get_scratch_size());
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = trunk_config_init(&kvs->trunk_cfg,
                          &kvs->cache_cfg.super,
                          kvs->data_cfg,
                          (log_config *)&kvs->log_cfg,
                          cfg.memtable_capacity,
                          cfg.fanout,
                          cfg.max_branches_per_node,
                          cfg.btree_rough_count_height,
                          cfg.filter_remainder_size,
                          cfg.filter_index_size,
                          cfg.reclaim_threshold,
                          cfg.queue_scale_percent,
                          cfg.use_log,
                          cfg.use_stats,
                          FALSE,
                          NULL);
   if (!SUCCESS(rc)) {
      return rc;
   }

   return STATUS_OK;
}


/*
 * Internal function for create or open
 */
int
splinterdb_create_or_open(const splinterdb_config *kvs_cfg,      // IN
                          splinterdb             **kvs_out,      // OUT
                          bool32                   open_existing // IN
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
      platform_error_log("Failed to %s SplinterDB device '%s' with specified "
                         "configuration: %s\n",
                         (open_existing ? "open existing" : "initialize"),
                         kvs_cfg->filename,
                         platform_status_to_string(status));
      goto deinit_kvhandle;
   }

   status = io_handle_init(
      &kvs->io_handle, &kvs->io_cfg, kvs->heap_handle, kvs->heap_id);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to initialize IO handle: %s\n",
                         platform_status_to_string(status));
      goto deinit_kvhandle;
   }

   status = task_system_create(
      kvs->heap_id, &kvs->io_handle, &kvs->task_sys, &kvs->task_cfg);
   if (!SUCCESS(status)) {
      platform_error_log(
         "Failed to initialize SplinterDB task system state: %s\n",
         platform_status_to_string(status));
      goto deinit_iohandle;
   }

   if (open_existing) {
      status = rc_allocator_mount(&kvs->allocator_handle,
                                  &kvs->allocator_cfg,
                                  (io_handle *)&kvs->io_handle,
                                  kvs->heap_id,
                                  platform_get_module_id());
   } else {
      status = rc_allocator_init(&kvs->allocator_handle,
                                 &kvs->allocator_cfg,
                                 (io_handle *)&kvs->io_handle,
                                 kvs->heap_id,
                                 platform_get_module_id());
   }
   if (!SUCCESS(status)) {
      platform_error_log("Failed to initialize SplinterDB allocator: %s\n",
                         platform_status_to_string(status));
      goto deinit_system;
   }

   status = clockcache_init(&kvs->cache_handle,
                            &kvs->cache_cfg,
                            (io_handle *)&kvs->io_handle,
                            (allocator *)&kvs->allocator_handle,
                            "splinterdb",
                            kvs->heap_id,
                            platform_get_module_id());
   if (!SUCCESS(status)) {
      platform_error_log("Failed to initialize SplinterDB cache: %s\n",
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
      platform_error_log("Failed to %s SplinterDB instance.\n",
                         (open_existing ? "mount existing" : "initialize"));

      // Return a generic 'something went wrong' error
      status = STATUS_INVALID_STATE;
      goto deinit_cache;
   }

   *kvs_out = kvs;
   return platform_status_to_int(status);

deinit_cache:
   clockcache_deinit(&kvs->cache_handle);
deinit_allocator:
   rc_allocator_unmount(&kvs->allocator_handle);
deinit_system:
   task_system_destroy(kvs->heap_id, &kvs->task_sys);
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
splinterdb_close(splinterdb **kvs_in) // IN
{
   splinterdb *kvs = *kvs_in;
   platform_assert(kvs != NULL);

   /*
    * NOTE: These dismantling routines must appear in exactly the reverse
    * order when these sub-systems were init'ed when a Splinter device was
    * created or re-opened. Otherwise, asserts will trip.
    */
   trunk_unmount(&kvs->spl);
   clockcache_deinit(&kvs->cache_handle);
   rc_allocator_unmount(&kvs->allocator_handle);
   task_system_destroy(kvs->heap_id, &kvs->task_sys);
   io_handle_deinit(&kvs->io_handle);

   platform_free(kvs->heap_id, kvs);
   *kvs_in = (splinterdb *)NULL;
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
 * splinterdb_insert_raw_message --
 *
 *      Insert a key and a raw message into splinter
 *
 * Results:
 *      0 on success, otherwise an errno
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static int
splinterdb_insert_message(const splinterdb *kvs,      // IN
                          slice             user_key, // IN
                          message           msg       // IN
)
{
   key tuple_key = key_create_from_slice(user_key);
   platform_assert(kvs != NULL);
   platform_status status = trunk_insert(kvs->spl, tuple_key, msg);
   return platform_status_to_int(status);
}

int
splinterdb_insert(const splinterdb *kvsb, slice user_key, slice value)
{
   message msg = message_create(MESSAGE_TYPE_INSERT, value);
   return splinterdb_insert_message(kvsb, user_key, msg);
}

int
splinterdb_delete(const splinterdb *kvsb, slice user_key)
{
   return splinterdb_insert_message(kvsb, user_key, DELETE_MESSAGE);
}

int
splinterdb_update(const splinterdb *kvsb, slice user_key, slice update)
{
   message msg = message_create(MESSAGE_TYPE_UPDATE, update);
   platform_assert(kvsb->data_cfg->merge_tuples);
   return splinterdb_insert_message(kvsb, user_key, msg);
}

/*
 *-----------------------------------------------------------------------------
 * _splinterdb_lookup_result structure --
 *-----------------------------------------------------------------------------
 */
typedef struct {
   merge_accumulator value;
} _splinterdb_lookup_result;

_Static_assert(sizeof(_splinterdb_lookup_result)
                  <= sizeof(splinterdb_lookup_result),
               "sizeof(splinterdb_lookup_result) is too small");

_Static_assert(alignof(splinterdb_lookup_result)
                  == alignof(_splinterdb_lookup_result),
               "mismatched alignment for splinterdb_lookup_result");

void
splinterdb_lookup_result_init(const splinterdb         *kvs,        // IN
                              splinterdb_lookup_result *result,     // IN/OUT
                              uint64                    buffer_len, // IN
                              char                     *buffer      // IN
)
{
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
   merge_accumulator_init_with_buffer(&_result->value,
                                      NULL,
                                      buffer_len,
                                      buffer,
                                      WRITABLE_BUFFER_NULL_LENGTH,
                                      MESSAGE_TYPE_INVALID);
}

void
splinterdb_lookup_result_deinit(splinterdb_lookup_result *result) // IN
{
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
   merge_accumulator_deinit(&_result->value);
}

_Bool
splinterdb_lookup_found(const splinterdb_lookup_result *result) // IN
{
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
   return trunk_lookup_found(&_result->value);
}

int
splinterdb_lookup_result_value(const splinterdb_lookup_result *result, // IN
                               slice                          *value)
{
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;

   if (!splinterdb_lookup_found(result)) {
      return EINVAL;
   }

   *value = merge_accumulator_to_value(&_result->value);
   return 0;
}

/*
 *-----------------------------------------------------------------------------
 * splinterdb_lookup --
 *
 *      Lookup a single tuple
 *
 *      result must have been initialized via splinterdb_lookup_result_init()
 *
 *      Use splinterdb_lookup_result_parse to interpret the result
 *
 *      A single result may be used for multiple lookups
 *
 * Results:
 *      0 on success (including key not found), otherwise an error number.
 *      Check for not-found via splinterdb_lookup_result_parse
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
int
splinterdb_lookup(const splinterdb         *kvs, // IN
                  slice                     user_key,
                  splinterdb_lookup_result *result) // IN/OUT
{
   platform_status            status;
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
   key                        target  = key_create_from_slice(user_key);

   platform_assert(kvs != NULL);
   status = trunk_lookup(kvs->spl, target, &_result->value);
   return platform_status_to_int(status);
}


struct splinterdb_iterator {
   trunk_range_iterator sri;
   platform_status      last_rc;
   const splinterdb    *parent;
};

int
splinterdb_iterator_init(const splinterdb     *kvs,           // IN
                         splinterdb_iterator **iter,          // OUT
                         slice                 user_start_key // IN
)
{
   splinterdb_iterator *it = TYPED_MALLOC(kvs->spl->heap_id, it);
   if (it == NULL) {
      platform_error_log("TYPED_MALLOC error\n");
      return platform_status_to_int(STATUS_NO_MEMORY);
   }
   it->last_rc = STATUS_OK;

   trunk_range_iterator *range_itor = &(it->sri);
   key                   start_key;

   if (slice_is_null(user_start_key)) {
      start_key = NEGATIVE_INFINITY_KEY;
   } else {
      start_key = key_create_from_slice(user_start_key);
   }

   platform_status rc = trunk_range_iterator_init(kvs->spl,
                                                  range_itor,
                                                  NEGATIVE_INFINITY_KEY,
                                                  POSITIVE_INFINITY_KEY,
                                                  start_key,
                                                  greater_than_or_equal,
                                                  UINT64_MAX);
   if (!SUCCESS(rc)) {
      platform_free(kvs->spl->heap_id, *iter);
      return platform_status_to_int(rc);
   }
   it->parent = kvs;

   *iter = it;
   return EXIT_SUCCESS;
}

void
splinterdb_iterator_deinit(splinterdb_iterator *iter)
{
   trunk_range_iterator *range_itor = &(iter->sri);
   trunk_range_iterator_deinit(range_itor);

   trunk_handle *spl = range_itor->spl;
   platform_free(spl->heap_id, range_itor);
}

_Bool
splinterdb_iterator_valid(splinterdb_iterator *kvi)
{
   if (!SUCCESS(kvi->last_rc)) {
      return FALSE;
   }
   iterator *itor = &(kvi->sri.super);
   return iterator_can_curr(itor);
}

_Bool
splinterdb_iterator_can_prev(splinterdb_iterator *kvi)
{
   if (!SUCCESS(kvi->last_rc)) {
      return FALSE;
   }
   iterator *itor = &(kvi->sri.super);
   return iterator_can_prev(itor);
}

_Bool
splinterdb_iterator_can_next(splinterdb_iterator *kvi)
{
   if (!SUCCESS(kvi->last_rc)) {
      return FALSE;
   }
   iterator *itor = &(kvi->sri.super);
   return iterator_can_next(itor);
}

void
splinterdb_iterator_next(splinterdb_iterator *kvi)
{
   iterator *itor = &(kvi->sri.super);
   kvi->last_rc   = iterator_next(itor);
}

void
splinterdb_iterator_prev(splinterdb_iterator *kvi)
{
   iterator *itor = &(kvi->sri.super);
   kvi->last_rc   = iterator_prev(itor);
}

int
splinterdb_iterator_status(const splinterdb_iterator *iter)
{
   return platform_status_to_int(iter->last_rc);
}

void
splinterdb_iterator_get_current(splinterdb_iterator *iter,   // IN
                                slice               *outkey, // OUT
                                slice               *value   // OUT
)
{
   key       result_key;
   message   msg;
   iterator *itor = &(iter->sri.super);

   iterator_curr(itor, &result_key, &msg);
   *value  = message_slice(msg);
   *outkey = key_slice(result_key);
}

void
splinterdb_stats_print_insertion(const splinterdb *kvs)
{
   trunk_print_insertion_stats(Platform_default_log_handle, kvs->spl);
}

void
splinterdb_stats_print_lookup(const splinterdb *kvs)
{
   trunk_print_lookup_stats(Platform_default_log_handle, kvs->spl);
}

void
splinterdb_stats_reset(splinterdb *kvs)
{
   trunk_reset_stats(kvs->spl);
}
