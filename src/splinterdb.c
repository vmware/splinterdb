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

#include "platform.h"

#include "clockcache.h"
#include "splinterdb/splinterdb.h"
#include "rc_allocator.h"
#include "trunk.h"

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
   rc_allocator_config  allocator_cfg;
   rc_allocator         allocator_handle;
   clockcache_config    cache_cfg;
   clockcache           cache_handle;
   allocator_root_id    trunk_id;
   trunk_config         trunk_cfg;
   trunk_handle        *spl;
   platform_heap_handle heap_handle; // for platform_buffer_create
   platform_heap_id     heap_id;

   // This is the data config provided by application, which assumes
   // all keys are variable-length, and the functions will be called
   // with the correct key lenghts.
   data_config app_data_cfg;

   // A data config constructed by this layer, and passed down
   // to lower layers.  Keys are fixed-length and functions will be
   // called with key-length set to 0.
   // This is a temporary shim until variable-length key support lands in trunk.
   data_config shim_data_cfg;
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
      cfg->page_size = 4096;
   }
   if (!cfg->extent_size) {
      cfg->extent_size = 128 * 1024;
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
      cfg->filter_index_size = 256;
   }
   if (!cfg->filter_remainder_size) {
      cfg->filter_remainder_size = 6;
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

static void
splinterdb_validate_app_data_config(const data_config *cfg)
{
   platform_assert(cfg->key_size > 0);
   platform_assert(cfg->message_size > 0);
   platform_assert(cfg->key_compare != NULL);
   platform_assert(cfg->key_hash != NULL);
   platform_assert(cfg->merge_tuples != NULL);
   platform_assert(cfg->merge_tuples_final != NULL);
   platform_assert(cfg->message_class != NULL);
   platform_assert(cfg->key_to_string != NULL);
   platform_assert(cfg->message_to_string != NULL);

   platform_assert(cfg->key_size <= SPLINTERDB_MAX_KEY_SIZE,
                   "key_size=%lu cannot exceed SPLINTERDB_MAX_KEY_SIZE=%d",
                   cfg->key_size,
                   SPLINTERDB_MAX_KEY_SIZE);

   platform_assert(cfg->max_key_length > 0,
                   "length of maximum key must be positive");
   platform_assert(cfg->max_key_length <= cfg->key_size,
                   "length of maximum key=%lu cannot exceed key_size=%lu",
                   cfg->max_key_length,
                   cfg->key_size);
   platform_assert(cfg->min_key_length <= cfg->key_size,
                   "length of minimum key=%lu cannot exceed key_size=%lu",
                   cfg->min_key_length,
                   cfg->key_size);

   int min_max_cmp = cfg->key_compare(cfg,
                                      cfg->min_key_length,
                                      cfg->min_key,
                                      cfg->max_key_length,
                                      cfg->max_key);
   platform_assert(min_max_cmp < 0, "min_key must compare < max_key");
}

// Variable-length key encoding and decoding virtual functions

// Length-prefix encoding of a variable-sized key
// We do this so that key comparison can be variable-length
typedef struct PACKED {
   uint8 length;
   uint8 data[0];
} var_len_key_encoding;

static_assert((SPLINTERDB_MAX_KEY_SIZE + sizeof(var_len_key_encoding)
               == MAX_KEY_SIZE),
              "Variable-length key encoding header size mismatch");
static_assert((SPLINTERDB_MAX_KEY_SIZE <= UINT8_MAX),
              "Variable-length key support is currently cappted at 255 bytes");

static int
encode_key(void       *key_buffer,
           uint64      key_buffer_len,
           const void *key,
           uint64      key_len)
{
   if (key_len > SPLINTERDB_MAX_KEY_SIZE) {
      platform_error_log("splinterdb.encode_key requires "
                         "key_len (%lu) <= SPLINTERDB_MAX_KEY_SIZE (%u)\n",
                         key_len,
                         SPLINTERDB_MAX_KEY_SIZE);
      return EINVAL;
   }
   platform_assert(key_buffer_len == MAX_KEY_SIZE,
                   "key buffer must always be of size MAX_KEY_SIZE");

   memset(key_buffer, 0, key_buffer_len);
   var_len_key_encoding *key_enc = (var_len_key_encoding *)key_buffer;
   key_enc->length               = (uint8)key_len;
   if (key_len > 0) {
      memmove(key_enc->data, key, key_len);
   }
   return 0;
}


static int
splinterdb_shim_key_compare(const data_config *cfg,
                            uint64             unused_key1_raw_len,
                            const void        *key1_raw,
                            uint64             unused_key2_raw_len,
                            const void        *key2_raw)
{
   var_len_key_encoding *key1 = (var_len_key_encoding *)key1_raw;
   var_len_key_encoding *key2 = (var_len_key_encoding *)key2_raw;

   platform_assert(key1->length <= SPLINTERDB_MAX_KEY_SIZE);
   platform_assert(key2->length <= SPLINTERDB_MAX_KEY_SIZE);

   data_config *app_cfg = (data_config *)(cfg->context);
   return app_cfg->key_compare(
      app_cfg, key1->length, key1->data, key2->length, key2->data);
}

static message_type
splinterdb_shim_message_class(const data_config *cfg,
                              uint64             raw_message_len,
                              const void        *raw_message)
{
   data_config *app_cfg = (data_config *)(cfg->context);
   return app_cfg->message_class(app_cfg, raw_message_len, raw_message);
}

static int
splinterdb_shim_merge_tuple(const data_config *cfg,
                            uint64             unused_key_len,
                            const void        *key_raw,
                            uint64             old_message_len,
                            const void        *old_message,
                            writable_buffer   *new_message)
{
   data_config          *app_cfg = (data_config *)(cfg->context);
   var_len_key_encoding *key     = (var_len_key_encoding *)key_raw;

   platform_assert(key->length <= SPLINTERDB_MAX_KEY_SIZE);
   return app_cfg->merge_tuples(app_cfg,
                                key->length,
                                key->data,
                                old_message_len,
                                old_message,
                                new_message);
}

static int
splinterdb_shim_merge_tuple_final(const data_config *cfg,
                                  uint64             unused_key_len,
                                  const void        *key_raw,
                                  writable_buffer   *oldest_message)
{
   data_config          *app_cfg = (data_config *)(cfg->context);
   var_len_key_encoding *key     = (var_len_key_encoding *)key_raw;

   platform_assert(key->length <= SPLINTERDB_MAX_KEY_SIZE);
   return app_cfg->merge_tuples_final(
      app_cfg, key->length, key->data, oldest_message);
}

static void
splinterdb_shim_key_to_string(const data_config *cfg,
                              uint64             unused_key_len,
                              const void        *key_raw,
                              char              *str,
                              uint64             max_len)
{

   data_config          *app_cfg = (data_config *)(cfg->context);
   var_len_key_encoding *key     = (var_len_key_encoding *)key_raw;

   platform_assert(key->length <= SPLINTERDB_MAX_KEY_SIZE);
   app_cfg->key_to_string(app_cfg, key->length, key->data, str, max_len);
}


// create a shim data_config that handles variable-length key encoding
// the output retains a reference to app_cfg (via the context field)
// so the lifetime of app_cfg must be at least as long as out_shim
static int
splinterdb_shim_data_config(const data_config *app_cfg, data_config *out_shim)
{
   data_config shim  = {0};
   shim.key_size     = app_cfg->key_size + sizeof(var_len_key_encoding);
   shim.message_size = app_cfg->message_size;

   int rc = encode_key(shim.min_key,
                       sizeof(shim.min_key),
                       app_cfg->min_key,
                       app_cfg->min_key_length);
   if (rc != 0) {
      return rc;
   }
   shim.min_key_length = 0; // lower layer ignores this

   rc = encode_key(shim.max_key,
                   sizeof(shim.max_key),
                   app_cfg->max_key,
                   app_cfg->max_key_length);
   if (rc != 0) {
      return rc;
   }
   shim.max_key_length = 0; // lower layer ignores this

   shim.key_compare = splinterdb_shim_key_compare;

   // this fn signature doesn't support passing in a data_config, so there's no
   // way to shim it.  This might be a bug, in a corner case, but let's defer
   // it.
   shim.key_hash = app_cfg->key_hash;

   shim.message_class      = splinterdb_shim_message_class;
   shim.merge_tuples       = splinterdb_shim_merge_tuple;
   shim.merge_tuples_final = splinterdb_shim_merge_tuple_final;
   shim.key_to_string      = splinterdb_shim_key_to_string;

   shim.message_to_string = app_cfg->message_to_string;
   shim.context           = (void *)app_cfg;
   *out_shim              = shim;
   return 0;
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
   splinterdb_validate_app_data_config(&kvs_cfg->data_cfg);

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

   kvs->app_data_cfg = kvs_cfg->data_cfg;
   platform_assert(
      0 == splinterdb_shim_data_config(&kvs->app_data_cfg, &kvs->shim_data_cfg),
      "error shimming data_config.  This is probably an invalid data_config");

   kvs->heap_handle = cfg.heap_handle;
   kvs->heap_id     = cfg.heap_id;

   io_config_init(&kvs->io_cfg,
                  cfg.page_size,
                  cfg.extent_size,
                  cfg.io_flags,
                  cfg.io_perms,
                  cfg.io_async_queue_depth,
                  cfg.filename);

   rc_allocator_config_init(&kvs->allocator_cfg, &kvs->io_cfg, cfg.disk_size);

   clockcache_config_init(&kvs->cache_cfg,
                          &kvs->io_cfg,
                          cfg.cache_size,
                          cfg.cache_logfile,
                          cfg.use_stats);

   trunk_config_init(&kvs->trunk_cfg,
                     &kvs->cache_cfg.super,
                     &kvs->shim_data_cfg,
                     NULL,
                     cfg.memtable_capacity,
                     cfg.fanout,
                     cfg.max_branches_per_node,
                     cfg.btree_rough_count_height,
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

static int
validate_key_length(const splinterdb *kvs, uint64 key_length)
{
   if (key_length > kvs->app_data_cfg.key_size) {
      platform_error_log("key of size %lu exceeds data_config.key_size %lu",
                         key_length,
                         kvs->app_data_cfg.key_size);
      return EINVAL;
   }
   return 0;
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
int
splinterdb_insert_raw_message(const splinterdb *kvs,            // IN
                              uint64            key_length,     // IN
                              const char       *key,            // IN
                              uint64            message_length, // IN
                              const char       *message         // IN
)
{
   platform_assert(kvs != NULL);
   int rc = validate_key_length(kvs, key_length);
   if (rc != 0) {
      return rc;
   }

   slice message_slice = slice_create(message_length, message);

   char key_buffer[MAX_KEY_SIZE] = {0};
   rc = encode_key(key_buffer, sizeof(key_buffer), key, key_length);
   if (rc != 0) {
      return rc;
   }

   platform_status status = trunk_insert(kvs->spl, key_buffer, message_slice);
   return platform_status_to_int(status);
}

int
splinterdb_insert(const splinterdb *kvsb,
                  uint64            key_len,
                  const char       *key,
                  uint64            val_len,
                  const char       *value)
{
   platform_assert(kvsb->app_data_cfg.encode_message != NULL);

   uint64 max_msg_len                  = kvsb->app_data_cfg.message_size;
   char   msg_buffer[MAX_MESSAGE_SIZE] = {0};
   uint64 encoded_len;
   int    rc = kvsb->app_data_cfg.encode_message(MESSAGE_TYPE_INSERT,
                                              val_len,
                                              value,
                                              max_msg_len,
                                              msg_buffer,
                                              &encoded_len);
   if (rc != 0) {
      return rc;
   }
   return splinterdb_insert_raw_message(
      kvsb, key_len, key, encoded_len, msg_buffer);
}

int
splinterdb_delete(const splinterdb *kvsb, uint64 key_len, const char *key)
{
   platform_assert(kvsb->app_data_cfg.encode_message != NULL);

   uint64 max_msg_len                  = kvsb->app_data_cfg.message_size;
   char   msg_buffer[MAX_MESSAGE_SIZE] = {0};
   uint64 encoded_len;
   int    rc = kvsb->app_data_cfg.encode_message(
      MESSAGE_TYPE_DELETE, 0, NULL, max_msg_len, msg_buffer, &encoded_len);
   if (rc != 0) {
      return rc;
   }
   return splinterdb_insert_raw_message(
      kvsb, key_len, key, encoded_len, msg_buffer);
}

/*
 *-----------------------------------------------------------------------------
 * _splinterdb_lookup_result structure --
 *-----------------------------------------------------------------------------
 */
typedef struct {
   writable_buffer value;
} _splinterdb_lookup_result;

_Static_assert(sizeof(_splinterdb_lookup_result)
                  <= sizeof(splinterdb_lookup_result),
               "sizeof(splinterdb_lookup_result) is too small");

void
splinterdb_lookup_result_init(const splinterdb         *kvs,        // IN
                              splinterdb_lookup_result *result,     // IN/OUT
                              uint64                    buffer_len, // IN
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

bool
splinterdb_lookup_found(const splinterdb_lookup_result *result) // IN
{
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
   return trunk_lookup_found(&_result->value);
}

static uint64
splinterdb_lookup_result_size(const splinterdb_lookup_result *result) // IN
{
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
   return writable_buffer_length(&_result->value);
}

static void *
splinterdb_lookup_result_data(const splinterdb_lookup_result *result) // IN
{
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;
   return writable_buffer_data(&_result->value);
}

int
splinterdb_lookup_result_value(const splinterdb               *kvs,
                               const splinterdb_lookup_result *result, // IN
                               uint64      *value_size,                // OUT
                               const char **value)
{

   if (!splinterdb_lookup_found(result)) {
      return EINVAL;
   }

   return kvs->app_data_cfg.decode_message(
      splinterdb_lookup_result_size(result),
      splinterdb_lookup_result_data(result),
      value_size,
      value);
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
splinterdb_lookup(const splinterdb         *kvs,        // IN
                  uint64                    key_length, // IN
                  const char               *key,        // IN
                  splinterdb_lookup_result *result)     // IN/OUT
{
   int rc = validate_key_length(kvs, key_length);
   if (rc != 0) {
      return rc;
   }

   platform_status            status;
   _splinterdb_lookup_result *_result = (_splinterdb_lookup_result *)result;

   platform_assert(kvs != NULL);
   char key_buffer[MAX_KEY_SIZE] = {0};
   rc = encode_key(key_buffer, sizeof(key_buffer), key, key_length);
   if (rc != 0) {
      return rc;
   }

   status = trunk_lookup(kvs->spl, key_buffer, &_result->value);
   return platform_status_to_int(status);
}


struct splinterdb_iterator {
   trunk_range_iterator sri;
   platform_status      last_rc;
   const splinterdb    *parent;
};

int
splinterdb_iterator_init(const splinterdb     *kvs,              // IN
                         splinterdb_iterator **iter,             // OUT
                         uint64                start_key_length, // IN
                         const char           *start_key         // IN
)
{
   splinterdb_iterator *it = TYPED_MALLOC(kvs->spl->heap_id, it);
   if (it == NULL) {
      platform_error_log("TYPED_MALLOC error\n");
      return platform_status_to_int(STATUS_NO_MEMORY);
   }
   it->last_rc = STATUS_OK;

   trunk_range_iterator *range_itor = &(it->sri);

   char start_key_buffer[MAX_KEY_SIZE] = {0};
   if (start_key) {
      int rc = encode_key(start_key_buffer,
                          sizeof(start_key_buffer),
                          start_key,
                          start_key_length);
      if (rc != 0) {
         return rc;
      }
   }

   platform_status rc = trunk_range_iterator_init(
      kvs->spl, range_itor, start_key_buffer, NULL, UINT64_MAX);
   if (!SUCCESS(rc)) {
      trunk_range_iterator_deinit(range_itor);
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

int
splinterdb_iterator_status(const splinterdb_iterator *iter)
{
   return platform_status_to_int(iter->last_rc);
}

void
splinterdb_iterator_get_current(splinterdb_iterator *iter,    // IN
                                uint64              *key_len, // OUT
                                const char         **key,     // OUT
                                uint64              *val_len, // OUT
                                const char         **value    // OUT
)
{
   platform_assert(iter->parent->app_data_cfg.decode_message != NULL);

   slice     key_slice;
   slice     message_slice;
   iterator *itor = &(iter->sri.super);

   iterator_get_curr(itor, &key_slice, &message_slice);

   var_len_key_encoding *kenc = (var_len_key_encoding *)(slice_data(key_slice));
   platform_assert(kenc->length <= SPLINTERDB_MAX_KEY_SIZE);

   *key     = (const char *)(&kenc->data);
   *key_len = kenc->length;

   uint64      message_length = slice_length(message_slice);
   const char *message_buffer = slice_data(message_slice);

   int rc = iter->parent->app_data_cfg.decode_message(
      message_length, message_buffer, val_len, value);
   if (rc != 0) {
      iter->last_rc = STATUS_BAD_PARAM;
   }
}
