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
#include "platform_linux/platform.h"
#include "rc_allocator.h"
#include "core.h"
#include "btree_private.h"
#include "shard_log.h"
#include "splinterdb_tests_private.h"
#include "poison.h"

const char *BUILD_VERSION = "splinterdb_build_version " GIT_VERSION;

// Function prototypes

static void
splinterdb_close_print_stats(splinterdb *kvs);

const char *
splinterdb_get_version()
{
   return BUILD_VERSION;
}

typedef struct splinterdb {
   task_system       *task_sys;
   io_config          io_cfg;
   platform_io_handle io_handle;
   allocator_config   allocator_cfg;
   rc_allocator       allocator_handle;
   clockcache_config  cache_cfg;
   clockcache         cache_handle;
   shard_log_config   log_cfg;
   task_system_config task_cfg;
   allocator_root_id  trunk_id;
   routing_config     filter_cfg;
   btree_config       btree_cfg;
   trunk_config       trunk_node_cfg;
   core_config        trunk_cfg;
   core_handle       *spl;
   platform_heap_id   heap_id;
   data_config       *data_cfg;
   bool               we_created_heap;
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

   if (!cfg->filter_log_index_size) {
      cfg->filter_log_index_size = 9;
   }
   if (!cfg->filter_hash_size) {
      cfg->filter_hash_size = 26;
   }

   if (!cfg->memtable_capacity) {
      cfg->memtable_capacity = MiB_TO_B(24);
   }
   if (!cfg->fanout) {
      cfg->fanout = 8;
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
      &kvs->task_cfg, cfg.use_stats, num_bg_threads, core_get_scratch_size());
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = routing_config_init(&kvs->filter_cfg,
                            &kvs->cache_cfg.super,
                            kvs->data_cfg,
                            cfg.filter_hash_size,
                            cfg.filter_log_index_size,
                            kvs->data_cfg->key_hash,
                            42);

   btree_config_init(&kvs->btree_cfg, &kvs->cache_cfg.super, kvs->data_cfg);

   trunk_config_init(&kvs->trunk_node_cfg,
                     kvs->data_cfg,
                     &kvs->btree_cfg,
                     &kvs->filter_cfg,
                     cfg.memtable_capacity,
                     cfg.fanout,
                     cfg.btree_rough_count_height,
                     cfg.use_stats);

   rc = core_config_init(&kvs->trunk_cfg,
                         &kvs->cache_cfg.super,
                         kvs->data_cfg,
                         &kvs->btree_cfg,
                         (log_config *)&kvs->log_cfg,
                         &kvs->trunk_node_cfg,
                         cfg.queue_scale_percent,
                         cfg.use_log,
                         cfg.use_stats,
                         FALSE,
                         Platform_default_log_handle);
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
                          bool                     open_existing // IN
)
{
   splinterdb     *kvs = NULL;
   platform_status status;

   bool             we_created_heap  = FALSE;
   platform_heap_id use_this_heap_id = kvs_cfg->heap_id;

   // Allocate a shared segment if so requested. For now, we hard-code
   // the required size big enough to run most tests. Eventually this
   // has to be calculated here based on other run-time params.
   // (Some tests externally create the platform_heap, so we should
   // only create one if it does not already exist.)
   if (kvs_cfg->use_shmem && (use_this_heap_id == NULL)) {
      size_t shmem_size = (kvs_cfg->shmem_size ? kvs_cfg->shmem_size : 2 * GiB);
      status            = platform_heap_create(
         platform_get_module_id(), shmem_size, TRUE, &use_this_heap_id);
      if (!SUCCESS(status)) {
         platform_error_log(
            "Shared memory creation failed. "
            "Failed to %s SplinterDB device '%s' with specified "
            "configuration: %s\n",
            (open_existing ? "open existing" : "initialize"),
            kvs_cfg->filename,
            platform_status_to_string(status));
         goto deinit_kvhandle;
      }
      we_created_heap = TRUE;
   }

   platform_assert(kvs_out != NULL);

   kvs = TYPED_ZALLOC(use_this_heap_id, kvs);
   if (kvs == NULL) {
      status = STATUS_NO_MEMORY;
      goto deinit_kvhandle;
   }
   // Remember, so at close() we only destroy heap if we created it here.
   kvs->we_created_heap = we_created_heap;

   // All memory allocation after this call should -ONLY- use heap handles
   // from the handle to the running Splinter instance; i.e. 'kvs'.
   status = splinterdb_init_config(kvs_cfg, kvs);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to %s SplinterDB device '%s' with specified "
                         "configuration: %s\n",
                         (open_existing ? "open existing" : "initialize"),
                         kvs_cfg->filename,
                         platform_status_to_string(status));
      goto deinit_kvhandle;
   }

   // All future memory allocation should come from shared memory, if so
   // configured.
   kvs->heap_id = use_this_heap_id;
   if (we_created_heap) {
      platform_shm_set_splinterdb_handle(use_this_heap_id, (void *)kvs);
   }

   status = io_handle_init(&kvs->io_handle, &kvs->io_cfg, kvs->heap_id);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to initialize IO handle: %s\n",
                         platform_status_to_string(status));
      goto io_handle_init_failed;
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
      platform_error_log("Failed to %s SplinterDB allocator: %s\n",
                         (open_existing ? "mount existing" : "initialize"),
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
      kvs->spl = core_mount(&kvs->trunk_cfg,
                            (allocator *)&kvs->allocator_handle,
                            (cache *)&kvs->cache_handle,
                            kvs->task_sys,
                            kvs->trunk_id,
                            kvs->heap_id);
   } else {
      kvs->spl = core_create(&kvs->trunk_cfg,
                             (allocator *)&kvs->allocator_handle,
                             (cache *)&kvs->cache_handle,
                             kvs->task_sys,
                             kvs->trunk_id,
                             kvs->heap_id);
   }
   if (kvs->spl == NULL || !SUCCESS(status)) {
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
io_handle_init_failed:
deinit_kvhandle:
   // Depending on the place where a configuration / setup error lead
   // us to here via a 'goto', heap_id handle, if in use, may be in a
   // different place. Use one carefully, to avoid ASAN-errors.
   if (we_created_heap) {
      // => Caller did not setup a platform-heap on entry.
      debug_assert(kvs_cfg->heap_id == NULL);

      if (kvs) {
         platform_free(use_this_heap_id, kvs);
      }
      platform_heap_destroy(&use_this_heap_id);
   } else {
      if (kvs) {
         platform_free(use_this_heap_id, kvs);
      }
   }

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
 *      Close a splinterdb, flushing to disk and releasing resources.
 *      Platform heap memory is also destroyed when closing SplinterDB.
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

   // Print stats if shared memory is enabled.
   if (kvs->heap_id) {
      splinterdb_close_print_stats(kvs);
   }
   /*
    * NOTE: These dismantling routines must appear in exactly the reverse
    * order when these sub-systems were init'ed when a Splinter device was
    * created or re-opened. Otherwise, asserts will trip.
    */
   core_unmount(&kvs->spl);
   clockcache_deinit(&kvs->cache_handle);
   rc_allocator_unmount(&kvs->allocator_handle);
   task_system_destroy(kvs->heap_id, &kvs->task_sys);
   io_handle_deinit(&kvs->io_handle);

   // Free resources carefully to avoid ASAN-test failures
   platform_heap_id heap_id         = kvs->heap_id;
   bool             we_created_heap = kvs->we_created_heap;
   platform_free(kvs->heap_id, kvs);
   if (we_created_heap) {
      platform_heap_destroy(&heap_id);
   }
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

   size_t          scratch_size = core_get_scratch_size();
   platform_status rc = task_register_this_thread(kvs->task_sys, scratch_size);
   platform_assert_status_ok(rc);
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
   platform_status status = core_insert(kvs->spl, tuple_key, msg);
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
   return core_lookup_found(&_result->value);
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
   status = core_lookup(kvs->spl, target, &_result->value);
   return platform_status_to_int(status);
}


struct splinterdb_iterator {
   core_range_iterator sri;
   platform_status     last_rc;
   const splinterdb   *parent;
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

   core_range_iterator *range_itor = &(it->sri);
   key                  start_key;

   if (slice_is_null(user_start_key)) {
      start_key = NEGATIVE_INFINITY_KEY;
   } else {
      start_key = key_create_from_slice(user_start_key);
   }

   platform_status rc = core_range_iterator_init(kvs->spl,
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
   core_range_iterator *range_itor = &(iter->sri);
   core_range_iterator_deinit(range_itor);

   core_handle *spl = range_itor->spl;
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
   core_print_insertion_stats(Platform_default_log_handle, kvs->spl);
}

void
splinterdb_stats_print_lookup(const splinterdb *kvs)
{
   core_print_lookup_stats(Platform_default_log_handle, kvs->spl);
}

void
splinterdb_stats_reset(splinterdb *kvs)
{
   core_reset_stats(kvs->spl);
}

static void
splinterdb_close_print_stats(splinterdb *kvs)
{
   task_print_stats(kvs->task_sys);
   splinterdb_stats_print_insertion(kvs);
}

/*
 * -------------------------------------------------------------------------
 * External "APIs" provided mainly to invoke lower-level functions intended
 * for use -ONLY- as testing interfaces.
 * -------------------------------------------------------------------------
 */
void
splinterdb_cache_flush(const splinterdb *kvs)
{
   cache_flush(kvs->spl->cc);
}

platform_heap_id
splinterdb_get_heap_id(const splinterdb *kvs)
{
   return kvs->heap_id;
}

const task_system *
splinterdb_get_task_system_handle(const splinterdb *kvs)
{
   return kvs->task_sys;
}

const platform_io_handle *
splinterdb_get_io_handle(const splinterdb *kvs)
{
   return &kvs->io_handle;
}

const allocator *
splinterdb_get_allocator_handle(const splinterdb *kvs)
{
   return (allocator *)&kvs->allocator_handle;
}

const cache *
splinterdb_get_cache_handle(const splinterdb *kvs)
{
   return (cache *)&kvs->cache_handle;
}

const core_handle *
splinterdb_get_trunk_handle(const splinterdb *kvs)
{
   return kvs->spl;
}

const memtable_context *
splinterdb_get_memtable_context_handle(const splinterdb *kvs)
{
   return kvs->spl->mt_ctxt;
}
