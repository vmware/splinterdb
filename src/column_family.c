
#include "platform.h"
#include "poison.h"

#include "splinterdb/column_family.h"
#include "splinterdb/splinterdb.h"
#include "splinterdb_internal.h"
#include "util.h"

#include <stdlib.h>

// This is just a normal splinterdb_iterator
// but to have a clean interface, we pretend it's not
struct splinterdb_cf_iterator {
   splinterdb_iterator iter;
};

typedef struct cf_data_config {
   data_config        general_config;
   platform_semaphore table_sema;
   column_family_id   num_families;
   column_family_id   capacity;
   data_config      **config_table;
} cf_data_config;

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

// Some helper functions we'll use for managing the column family identifiers
// and the data config table
static inline column_family_id
get_cf_id(slice cf_key)
{
   // the cf id is a prefix of the key
   const void      *data = slice_data(cf_key);
   column_family_id id;
   memcpy(&id, data, sizeof(id));
   return id;
}

static inline slice
userkey_to_cf_key(slice            userkey,
                  column_family_id cf_id,
                  writable_buffer *cf_key_wb)
{
   uint64      key_len = slice_length(userkey);
   const void *data    = slice_data(userkey);

   // write the cf id and the user's key data to the writable_buffer
   writable_buffer_append(cf_key_wb, sizeof(cf_id), &cf_id);
   writable_buffer_append(cf_key_wb, key_len, data);
   return writable_buffer_to_slice(cf_key_wb);
}

static inline slice
cf_key_to_userkey(slice cf_key)
{
   uint64      key_len = slice_length(cf_key);
   const void *data    = slice_data(cf_key);

   return slice_create(key_len - sizeof(column_family_id),
                       data + sizeof(column_family_id));
}

static inline column_family_id
cfg_table_insert(splinterdb *kvs, cf_data_config *cf_cfg, data_config *data_cfg)
{
   platform_semaphore_wait(&cf_cfg->table_sema);
   column_family_id new_id = cf_cfg->num_families;
   cf_cfg->num_families += 1;

   // reallocate table memory if necessary
   if (cf_cfg->capacity <= new_id) {
      cf_cfg->capacity *= 2;
      data_config **new_table = TYPED_ARRAY_MALLOC(
         platform_get_heap_id(), new_table, cf_cfg->capacity);
      if (new_table == NULL) {
         platform_error_log("TYPED_ARRAY_MALLOC error\n");
         return -1;
      }

      memcpy(new_table,
             cf_cfg->config_table,
             cf_cfg->num_families * sizeof(data_config *));
      data_config **old_table = cf_cfg->config_table;
      cf_cfg->config_table    = new_table;

      // wait until all threads have the change to observe this table change
      // 1. make copy of counters
      uint64 local_counter_copy[MAX_THREADS];
      for (int i = 0; i < MAX_THREADS; i++)
         local_counter_copy[i] = kvs->spl->cfg_crit_count[i].counter;

      // 2. for each thread wait until copy is even or that current is > copy
      for (int i = 0; i < MAX_THREADS; i++) {
         if (local_counter_copy[i] % 2 == 1)
            while (local_counter_copy[i] >= kvs->spl->cfg_crit_count[i].counter)
               ;
      }
      // free the old table
      platform_free(platform_get_heap_id(), old_table);
   }

   // place new data_config in table
   cf_cfg->config_table[new_id] = data_cfg;
   platform_semaphore_post(&cf_cfg->table_sema);

   return new_id;
}

// lookup a data_config in the table.
// Returns NULL if the column family ID does not exist
// otherwise, returns the associated data_config for the cf
static inline data_config *
cfg_table_lookup(cf_data_config *cf_cfg, column_family_id cf_id)
{
   if (cf_cfg->num_families <= cf_id)
      return NULL;

   return cf_cfg->config_table[cf_id];
}

// Beginning of column family interface

// Create a new column family
// Returns a new column family struct
splinterdb_column_family
column_family_create(splinterdb  *kvs,
                     const uint64 max_key_size,
                     data_config *new_data_cfg)
{
   platform_assert(kvs->data_cfg->max_key_size
                   >= max_key_size + sizeof(column_family_id));

   // convert from data_config to cf_data_config
   cf_data_config *cf_cfg = (cf_data_config *)kvs->data_cfg;

   column_family_id new_id = cfg_table_insert(kvs, cf_cfg, new_data_cfg);

   // return new column family
   splinterdb_column_family cf;
   cf.id  = new_id;
   cf.kvs = kvs;

   return cf;
}

// Delete the column family cf
void
column_family_delete(splinterdb_column_family cf)
{
   // TODO: Issue deletion messages to all keys within the column family!
}

// SplinterDB Functions
// We wrap these for column family support
// Column families and standard splinterdb should not be mixed
int
splinterdb_cf_insert(const splinterdb_column_family cf, slice key, slice value)
{
   // zero len key reserved, negative infinity
   platform_assert(slice_length(key) > 0);

   // assert that this is a valid column family
   cf_data_config *cf_cfg = (cf_data_config *)cf.kvs->data_cfg;
   platform_assert(cfg_table_lookup(cf_cfg, cf.id) != NULL);

   // convert to column family key by prefixing the cf id
   char            key_buf[CF_KEY_DEFAULT_SIZE];
   writable_buffer cf_key_wb;
   writable_buffer_init_with_buffer(
      &cf_key_wb, platform_get_heap_id(), CF_KEY_DEFAULT_SIZE, key_buf, 0);
   slice cf_key = userkey_to_cf_key(key, cf.id, &cf_key_wb);

   // call splinter's insert function and return
   int rc = splinterdb_insert(cf.kvs, cf_key, value);
   writable_buffer_deinit(&cf_key_wb);
   return rc;
}

int
splinterdb_cf_delete(const splinterdb_column_family cf, slice key)
{
   // zero len key reserved, negative infinity
   platform_assert(slice_length(key) > 0);

   // assert that this is a valid column family
   cf_data_config *cf_cfg = (cf_data_config *)cf.kvs->data_cfg;
   platform_assert(cfg_table_lookup(cf_cfg, cf.id) != NULL);

   // convert to column family key by prefixing the cf id
   char            key_buf[CF_KEY_DEFAULT_SIZE];
   writable_buffer cf_key_wb;
   writable_buffer_init_with_buffer(
      &cf_key_wb, platform_get_heap_id(), CF_KEY_DEFAULT_SIZE, key_buf, 0);
   slice cf_key = userkey_to_cf_key(key, cf.id, &cf_key_wb);

   // call splinter's delete function and return
   int rc = splinterdb_delete(cf.kvs, cf_key);
   writable_buffer_deinit(&cf_key_wb);
   return rc;
}

int
splinterdb_cf_update(const splinterdb_column_family cf, slice key, slice delta)
{
   // zero len key reserved, negative infinity
   platform_assert(slice_length(key) > 0);

   // assert that this is a valid column family
   cf_data_config *cf_cfg = (cf_data_config *)cf.kvs->data_cfg;
   platform_assert(cfg_table_lookup(cf_cfg, cf.id) != NULL);

   // convert to column family key by prefixing the cf id
   char            key_buf[CF_KEY_DEFAULT_SIZE];
   writable_buffer cf_key_wb;
   writable_buffer_init_with_buffer(
      &cf_key_wb, platform_get_heap_id(), CF_KEY_DEFAULT_SIZE, key_buf, 0);
   slice cf_key = userkey_to_cf_key(key, cf.id, &cf_key_wb);

   // call splinter's update function and return
   int rc = splinterdb_update(cf.kvs, cf_key, delta);
   writable_buffer_deinit(&cf_key_wb);
   return rc;
}

// column family lookups

void
splinterdb_cf_lookup_result_init(const splinterdb_column_family cf,    // IN
                                 splinterdb_lookup_result *result,     // IN/OUT
                                 uint64                    buffer_len, // IN
                                 char                     *buffer      // IN
)
{
   splinterdb_lookup_result_init(cf.kvs, result, buffer_len, buffer);
}

void
splinterdb_cf_lookup_result_deinit(splinterdb_lookup_result *result) // IN
{
   splinterdb_lookup_result_deinit(result);
}

_Bool
splinterdb_cf_lookup_found(const splinterdb_lookup_result *result) // IN
{
   return splinterdb_lookup_found(result);
}

int
splinterdb_cf_lookup_result_value(const splinterdb_lookup_result *result, // IN
                                  slice                          *value   // OUT
)
{
   return splinterdb_lookup_result_value(result, value);
}

int
splinterdb_cf_lookup(const splinterdb_column_family cf,    // IN
                     slice                          key,   // IN
                     splinterdb_lookup_result      *result // IN/OUT
)
{
   // zero len key reserved, negative infinity
   platform_assert(slice_length(key) > 0);

   // assert that this is a valid column family
   cf_data_config *cf_cfg = (cf_data_config *)cf.kvs->data_cfg;
   platform_assert(cfg_table_lookup(cf_cfg, cf.id) != NULL);

   // convert to column family key by prefixing the cf id
   char            key_buf[CF_KEY_DEFAULT_SIZE];
   writable_buffer cf_key_wb;
   writable_buffer_init_with_buffer(
      &cf_key_wb, platform_get_heap_id(), CF_KEY_DEFAULT_SIZE, key_buf, 0);
   slice cf_key = userkey_to_cf_key(key, cf.id, &cf_key_wb);

   // call splinter's lookup function and return
   int rc = splinterdb_lookup(cf.kvs, cf_key, result);
   if (rc != 0)
      return rc;
   writable_buffer_deinit(&cf_key_wb);
   return 0;
}

// Range iterators for column families

int
splinterdb_cf_iterator_init(const splinterdb_column_family cf,            // IN
                            splinterdb_cf_iterator       **cf_iter,       // OUT
                            slice                          user_start_key // IN
)
{
   // assert that this is a valid column family
   cf_data_config *cf_cfg = (cf_data_config *)cf.kvs->data_cfg;
   platform_assert(cfg_table_lookup(cf_cfg, cf.id) != NULL);

   // The minimum key contains no key data only consists of
   // the column id.
   // This is what a NULL key will become
   // convert to column family key by prefixing the cf id
   char            key_buf[CF_KEY_DEFAULT_SIZE];
   writable_buffer cf_key_wb;
   writable_buffer_init_with_buffer(
      &cf_key_wb, platform_get_heap_id(), CF_KEY_DEFAULT_SIZE, key_buf, 0);
   slice cf_key = userkey_to_cf_key(user_start_key, cf.id, &cf_key_wb);

   column_family_id next_cf = cf.id + 1;
   slice curr_cf_key = slice_create(sizeof(column_family_id), (void *)&cf.id);
   slice next_cf_key = slice_create(sizeof(column_family_id), (void *)&next_cf);

   splinterdb_iterator *it = TYPED_MALLOC(cf.kvs->spl->heap_id, it);
   if (it == NULL) {
      platform_error_log("TYPED_MALLOC error\n");
      return platform_status_to_int(STATUS_NO_MEMORY);
   }
   it->last_rc                      = STATUS_OK;
   trunk_range_iterator *range_itor = &(it->sri);

   key             min_key   = key_create_from_slice(curr_cf_key);
   key             start_key = key_create_from_slice(cf_key);
   key             end_key   = key_create_from_slice(next_cf_key);
   platform_status rc        = trunk_range_iterator_init(cf.kvs->spl,
                                                  range_itor,
                                                  min_key,
                                                  end_key,
                                                  start_key,
                                                  greater_than_or_equal,
                                                  UINT64_MAX);
   if (!SUCCESS(rc)) {
      platform_free(cf.kvs->spl->heap_id, it);
      writable_buffer_deinit(&cf_key_wb);
      return platform_status_to_int(rc);
   }
   it->parent = cf.kvs;

   *cf_iter = (splinterdb_cf_iterator *)it;
   writable_buffer_deinit(&cf_key_wb);
   return EXIT_SUCCESS;
}

void
splinterdb_cf_iterator_deinit(splinterdb_cf_iterator *cf_iter)
{
   splinterdb_iterator_deinit(&cf_iter->iter);
}

_Bool
splinterdb_cf_iterator_valid(splinterdb_cf_iterator *cf_iter)
{
   return splinterdb_iterator_valid(&cf_iter->iter);
}

_Bool
splinterdb_cf_iterator_can_prev(splinterdb_cf_iterator *cf_iter)
{
   return splinterdb_iterator_can_prev(&cf_iter->iter);
}

_Bool
splinterdb_cf_iterator_can_next(splinterdb_cf_iterator *cf_iter)
{
   return splinterdb_iterator_can_next(&cf_iter->iter);
}

void
splinterdb_cf_iterator_get_current(splinterdb_cf_iterator *cf_iter, // IN
                                   slice                  *key,     // OUT
                                   slice                  *value    // OUT
)
{
   splinterdb_iterator_get_current(&cf_iter->iter, key, value);
   *key = cf_key_to_userkey(*key);
}

void
splinterdb_cf_iterator_next(splinterdb_cf_iterator *cf_iter)
{
   return splinterdb_iterator_next(&cf_iter->iter);
}

void
splinterdb_cf_iterator_prev(splinterdb_cf_iterator *cf_iter)
{
   return splinterdb_iterator_prev(&cf_iter->iter);
}

int
splinterdb_cf_iterator_status(const splinterdb_cf_iterator *cf_iter)
{
   return splinterdb_iterator_status(&cf_iter->iter);
}


static int
cf_key_compare(const data_config *cfg, slice key1, slice key2)
{
   // sort first by the column family ids
   column_family_id cf_id1 = get_cf_id(key1);
   column_family_id cf_id2 = get_cf_id(key2);

   if (cf_id1 < cf_id2)
      return -1;
   if (cf_id1 > cf_id2)
      return 1;

   // Now we are comparing two keys from the same column family
   // so we will use the user defined func for this cf
   slice userkey1 = cf_key_to_userkey(key1);
   slice userkey2 = cf_key_to_userkey(key2);

   // empty keys are defined to be the minimum among the column family
   if (slice_length(userkey1) == 0 && slice_length(userkey2) == 0)
      return 0;
   if (slice_length(userkey1) == 0)
      return -1;
   if (slice_length(userkey2) == 0)
      return 1;

   // get the data_config for this column family
   data_config *cf_cfg = cfg_table_lookup((cf_data_config *)cfg, cf_id1);
   platform_assert(cf_cfg != NULL);

   // call column family's function
   return cf_cfg->key_compare(cf_cfg, userkey1, userkey2);
}

static int
cf_merge_tuples(const data_config *cfg,
                slice              key,
                message            old_raw_message,
                merge_accumulator *new_message)
{
   column_family_id cf_id = get_cf_id(key);

   // get the data_config for this column family and call its function
   data_config *cf_cfg = cfg_table_lookup((cf_data_config *)cfg, cf_id);
   platform_assert(cf_cfg != NULL);

   // call column family's function
   return cf_cfg->merge_tuples(
      cf_cfg, cf_key_to_userkey(key), old_raw_message, new_message);
}

static int
cf_merge_tuples_final(const data_config *cfg,
                      slice              key,
                      merge_accumulator *oldest_data // IN/OUT
)
{
   column_family_id cf_id = get_cf_id(key);

   // get the data_config for this column family
   data_config *cf_cfg = cfg_table_lookup((cf_data_config *)cfg, cf_id);
   platform_assert(cf_cfg != NULL);

   // call column family's function
   return cf_cfg->merge_tuples_final(
      cf_cfg, cf_key_to_userkey(key), oldest_data);
}

// These functions we don't allow the column families to overwrite

static void
cf_key_to_string(const data_config *cfg, slice key, char *str, size_t max_len)
{
   debug_hex_encode(str, max_len, slice_data(key), slice_length(key));
}

static void
cf_message_to_string(const data_config *cfg,
                     message            msg,
                     char              *str,
                     size_t             max_len)
{
   debug_hex_encode(str, max_len, message_data(msg), message_length(msg));
}

// Initialize the data_config stored in the cf_data_config.
// Its data_config is then passed to SplinterDB to add support for
// column families. The memory for the column_family_config is managed
// by splinter. Not the user.
void
column_family_config_init(const uint64  max_key_size, // IN
                          data_config **data_cfg      // OUT
)
{
   data_config cfg = {
      .max_key_size       = max_key_size + sizeof(column_family_id),
      .key_compare        = cf_key_compare,
      .key_hash           = platform_hash32,
      .merge_tuples       = cf_merge_tuples,
      .merge_tuples_final = cf_merge_tuples_final,
      .key_to_string      = cf_key_to_string,
      .message_to_string  = cf_message_to_string,
   };

   platform_heap_id hid    = platform_get_heap_id();
   cf_data_config  *cf_cfg = TYPED_MALLOC(hid, cf_cfg);

   cf_cfg->general_config = cfg;
   cf_cfg->num_families   = 0;
   cf_cfg->capacity       = 1;
   cf_cfg->config_table   = TYPED_MALLOC(hid, cf_cfg->config_table);

   platform_semaphore_init(&cf_cfg->table_sema, 1, hid);
   *data_cfg = (data_config *)cf_cfg;
}

void
column_family_config_deinit(data_config *data_cfg)
{
   cf_data_config *cf_cfg = (cf_data_config *)data_cfg;

   // user deallocates their cf's data configs
   // we need to deallocate everything else
   platform_semaphore_destroy(&cf_cfg->table_sema);
   platform_free(platform_get_heap_id(), cf_cfg->config_table);
   platform_free(platform_get_heap_id(), cf_cfg);
}
