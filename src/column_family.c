
#include "platform.h"
// #include "poison.h"

#include "splinterdb/column_family.h"
#include "splinterdb/splinterdb.h"
#include "splinterdb_internal.h"
#include "util.h"

#include <stdlib.h>

struct splinterdb_cf_iterator {
   column_family_id    id;
   splinterdb_iterator iter;
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

// Some helper functions we'll use for managing the column family identifiers
// and the data config table
column_family_id
get_cf_id(slice cf_key)
{
   // the cf id is a prefix of the key
   const void      *data = slice_data(cf_key);
   column_family_id id;
   memcpy(&id, data, sizeof(id));
   return id;
}

slice
userkey_to_cf_key(slice            userkey,
                  column_family_id cf_id,
                  writable_buffer *cf_key_wb)
{
   // extract from the user's key and resize the buffer
   uint64      key_len = slice_length(userkey);
   const void *data    = slice_data(userkey);

   // write the cf id and the user's key data to the writable_buffer
   writable_buffer_append(cf_key_wb, sizeof(cf_id), &cf_id);
   writable_buffer_append(cf_key_wb, key_len, data);
   return writable_buffer_to_slice(cf_key_wb);
}

slice
cf_key_to_userkey(slice cf_key)
{
   uint64      key_len = slice_length(cf_key);
   const void *data    = slice_data(cf_key);

   return slice_create(key_len - sizeof(column_family_id),
                       data + sizeof(column_family_id));
}

column_family_id
cfg_table_insert(cf_data_config *cf_cfg, data_config *data_cfg)
{
   column_family_id new_id = cf_cfg->num_families;
   cf_cfg->num_families += 1;

   // reallocate table memory if necessary
   if (cf_cfg->table_mem <= new_id) {
      cf_cfg->config_table = (data_config **)realloc(
         cf_cfg->config_table, (new_id + 1) * 2 * sizeof(data_config *));
   }

   // place new data_config in table
   cf_cfg->config_table[new_id] = data_cfg;

   return new_id;
}

void
cfg_table_delete(cf_data_config *cf_cfg, column_family_id cf_id)
{
   // memory is held by user so don't free it
   // just mark the config_table entry as NULL
   cf_cfg->config_table[cf_id] = NULL;

   // TODO: Reuse this slot somehow?
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

   column_family_id new_id = cfg_table_insert(cf_cfg, new_data_cfg);

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
   // convert from data_config to cf_data_config
   cf_data_config *cf_cfg = (cf_data_config *)cf.kvs->data_cfg;

   cfg_table_delete(cf_cfg, cf.id);
}

// SplinterDB Functions
// We wrap these for column family support
// Column families and standard splinterdb should not be mixed
int
splinterdb_cf_insert(const splinterdb_column_family cf, slice key, slice value)
{
   // zero len key reserved, negative infinity
   platform_assert(slice_length(key) > 0);

   // convert to column family key by prefixing the cf id
   char            key_buf[CF_KEY_DEFAULT_SIZE];
   writable_buffer cf_key_wb;
   writable_buffer_init_with_buffer(
      &cf_key_wb, platform_get_heap_id(), CF_KEY_DEFAULT_SIZE, key_buf, 0);
   slice cf_key = userkey_to_cf_key(key, cf.id, &cf_key_wb);

   // call splinter's insert function and return
   int rc = splinterdb_insert(cf.kvs, cf_key, value);
   if (rc != 0)
      return rc;
   writable_buffer_deinit(&cf_key_wb);
   return 0;
}

int
splinterdb_cf_delete(const splinterdb_column_family cf, slice key)
{
   // zero len key reserved, negative infinity
   platform_assert(slice_length(key) > 0);

   // convert to column family key by prefixing the cf id
   char            key_buf[CF_KEY_DEFAULT_SIZE];
   writable_buffer cf_key_wb;
   writable_buffer_init_with_buffer(
      &cf_key_wb, platform_get_heap_id(), CF_KEY_DEFAULT_SIZE, key_buf, 0);
   slice cf_key = userkey_to_cf_key(key, cf.id, &cf_key_wb);

   // call splinter's delete function and return
   int rc = splinterdb_delete(cf.kvs, cf_key);
   if (rc != 0)
      return rc;
   writable_buffer_deinit(&cf_key_wb);
   return 0;
}

int
splinterdb_cf_update(const splinterdb_column_family cf, slice key, slice delta)
{
   // zero len key reserved, negative infinity
   platform_assert(slice_length(key) > 0);

   // convert to column family key by prefixing the cf id
   char            key_buf[CF_KEY_DEFAULT_SIZE];
   writable_buffer cf_key_wb;
   writable_buffer_init_with_buffer(
      &cf_key_wb, platform_get_heap_id(), CF_KEY_DEFAULT_SIZE, key_buf, 0);
   slice cf_key = userkey_to_cf_key(key, cf.id, &cf_key_wb);

   // call splinter's update function and return
   int rc = splinterdb_update(cf.kvs, cf_key, delta);
   if (rc != 0)
      return rc;
   writable_buffer_deinit(&cf_key_wb);
   return 0;
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
   // The minimum key contains no key data only consists of
   // the column id.
   // This is what a NULL key will become
   // convert to column family key by prefixing the cf id
   char            key_buf[CF_KEY_DEFAULT_SIZE];
   writable_buffer cf_key_wb;
   writable_buffer_init_with_buffer(
      &cf_key_wb, platform_get_heap_id(), CF_KEY_DEFAULT_SIZE, key_buf, 0);
   slice cf_key = userkey_to_cf_key(user_start_key, cf.id, &cf_key_wb);

   splinterdb_cf_iterator *cf_it = TYPED_MALLOC(cf.kvs->spl->heap_id, cf_it);
   if (cf_it == NULL) {
      platform_error_log("TYPED_MALLOC error\n");
      return platform_status_to_int(STATUS_NO_MEMORY);
   }
   cf_it->iter.last_rc              = STATUS_OK;
   trunk_range_iterator *range_itor = &(cf_it->iter.sri);

   key             start_key = key_create_from_slice(cf_key);
   platform_status rc        = trunk_range_iterator_init(
      cf.kvs->spl, range_itor, start_key, POSITIVE_INFINITY_KEY, UINT64_MAX);
   if (!SUCCESS(rc)) {
      platform_free(cf.kvs->spl->heap_id, *cf_iter);
      writable_buffer_deinit(&cf_key_wb);
      return platform_status_to_int(rc);
   }
   cf_it->iter.parent = cf.kvs;
   cf_it->id          = cf.id;

   *cf_iter = cf_it;
   writable_buffer_deinit(&cf_key_wb);
   return EXIT_SUCCESS;
}

void
splinterdb_cf_iterator_deinit(splinterdb_cf_iterator *cf_iter)
{
   trunk_range_iterator *range_itor = &(cf_iter->iter.sri);
   trunk_range_iterator_deinit(range_itor);

   platform_free(range_itor->spl->heap_id, cf_iter);
}

_Bool
splinterdb_cf_iterator_get_current(splinterdb_cf_iterator *cf_iter, // IN
                                   slice                  *key,     // OUT
                                   slice                  *value    // OUT
)
{
   _Bool valid = splinterdb_iterator_valid(&cf_iter->iter);

   if (!valid)
      return FALSE;

   // if valid, check the key to ensure it's within this column family
   splinterdb_iterator_get_current(&cf_iter->iter, key, value);
   column_family_id key_cf = get_cf_id(*key);

   if (key_cf != cf_iter->id)
      return FALSE;

   *key = cf_key_to_userkey(*key);
   return TRUE;
}

void
splinterdb_cf_iterator_next(splinterdb_cf_iterator *cf_iter)
{
   return splinterdb_iterator_next(&cf_iter->iter);
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
   if (slice_length(userkey1) == 0)
      return -1;
   if (slice_length(userkey2) == 0)
      return 1;

   // get the data_config for this column family and call its function
   data_config *cf_cfg = ((cf_data_config *)cfg)->config_table[cf_id1];
   return cf_cfg->key_compare(cf_cfg, userkey1, userkey2);
}

static int
cf_merge_tuples(const data_config *cfg,
                slice              key,
                message            old_raw_message,
                merge_accumulator *new_data)
{
   column_family_id cf_id = get_cf_id(key);

   // get the data_config for this column family and call its function
   data_config *cf_cfg = ((cf_data_config *)cfg)->config_table[cf_id];
   return cf_cfg->merge_tuples(
      cf_cfg, cf_key_to_userkey(key), old_raw_message, new_data);
}

static int
cf_merge_tuples_final(const data_config *cfg,
                      slice              key,
                      merge_accumulator *oldest_data // IN/OUT
)
{
   column_family_id cf_id = get_cf_id(key);

   // get the data_config for this column family and call its function
   data_config *cf_cfg = ((cf_data_config *)cfg)->config_table[cf_id];
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
// column families
//
// TODO: The key_hash function cannot be overwritten by column families
//       at this time. This is because we do not have access to the cfg
//       in the key_hash. So we have no way of accessing a user defined
//       key_hash for the column family. This should probably be fixed.
//       Likely requires adding the cfg to the key_hash_fn type.
void
column_family_config_init(const uint64    max_key_size, // IN
                          cf_data_config *cf_cfg        // OUT
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

   cf_cfg->general_config = cfg;
   cf_cfg->num_families   = 0;
   cf_cfg->config_table   = NULL;
   cf_cfg->table_mem      = 0;
}

void
column_family_config_deinit(cf_data_config *cf_cfg)
{
   // we assume that the user will handle deallocating the table entries
   // we just need to dealloc our array of pointers
   if (cf_cfg->config_table != NULL)
      free(cf_cfg->config_table);
   cf_cfg->config_table = NULL;
   cf_cfg->table_mem    = 0;
}
