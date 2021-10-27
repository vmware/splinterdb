// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinterdb_kv.c --
 *
 *     Implementation for a simplified key-value store interface.
 *
 *     API deals with keys/values rather than keys/messages.
 */

#include "platform.h"

#include "splinterdb/splinterdb.h"
#include "splinterdb/splinterdb_kv.h"
#include "util.h"

#include "poison.h"

typedef struct {
   key_comparator_fn key_comparator;
   void *            key_comparator_context;
} kvsb_data_config_context;

struct splinterdb_kv {
   size_t max_app_key_size; // size of keys provided by app
   size_t max_app_val_size; // size of values provided by app

   platform_heap_handle heap_handle; // for platform_buffer_create
   platform_heap_id     heap_id;

   kvsb_data_config_context *data_config_context;

   splinterdb *kvs;
};

// Length-prefix encoding of a variable-sized key
// We do this so that key comparison can be variable-length
typedef struct PACKED {
   uint8 length;  // offset 0
   uint8 data[0]; // offset 1: 1st byte of value, aligned for 64-bit access
} basic_key_encoding;

static_assert((sizeof(basic_key_encoding) == SPLINTERDB_KV_KEY_HDR_SIZE),
              "basic_key_encoding header should have length 1");

static_assert(offsetof(basic_key_encoding, data[0]) == sizeof(uint8),
              "start of data should equal header size");

static_assert((MAX_KEY_SIZE ==
               (SPLINTERDB_KV_MAX_KEY_SIZE + SPLINTERDB_KV_KEY_HDR_SIZE)),
              "SPLINTERDB_KV_MAX_KEY_SIZE should be updated when "
              "MAX_KEY_SIZE changes");

#define KVSB_PACK_PTR __attribute__((packed, aligned(__alignof__(void *))))

// basic implementation of data_config
// uses a pointer-sized header to hold the message type (insert vs delete)
// and the size of the value in bytes
typedef struct KVSB_PACK_PTR {
   uint8  type;         // offset 0
   uint8  _reserved1;   // offset 1
   uint16 _reserved2;   // offset 2
   uint32 value_length; // offset 4
   uint8  value[0]; // offset 8: 1st byte of value, aligned for 64-bit access
} basic_message;

static_assert(sizeof(basic_message) == SPLINTERDB_KV_MSG_HDR_SIZE,
              "basic_message header should have the same size as a pointer");

static_assert(offsetof(basic_message, value[0]) == sizeof(void *),
              "start of data value should be aligned to pointer access");

static int
variable_len_compare(const void *context,
                     const void *key1,
                     size_t      key1_len,
                     const void *key2,
                     size_t      key2_len)
{
   // implementation copied from RocksDB Slice::compare
   // https://github.com/facebook/rocksdb/blob/2e062b222720d45d5a4f99b8de1824a2aae7b0c1/include/rocksdb/slice.h#L247-L258
   assert(key1 != NULL);
   assert(key2 != NULL);
   assert(key1_len <= SPLINTERDB_KV_MAX_KEY_SIZE);
   assert(key2_len <= SPLINTERDB_KV_MAX_KEY_SIZE);
   size_t min_len = (key1_len <= key2_len ? key1_len : key2_len);
   int    r       = memcmp(key1, key2, min_len);
   if (r == 0) {
      if (key1_len < key2_len) {
         r = -1;
      } else if (key1_len > key2_len) {
         r = +1;
      }
   }
   return r;
}
const static key_comparator_fn default_key_comparator = (&variable_len_compare);

static int
basic_key_compare(const data_config *cfg,
                  const void *       key1_raw,
                  const void *       key2_raw)
{
   basic_key_encoding *key1 = (basic_key_encoding *)key1_raw;
   basic_key_encoding *key2 = (basic_key_encoding *)key2_raw;

   assert(key1->length <= SPLINTERDB_KV_MAX_KEY_SIZE);
   assert(key2->length <= SPLINTERDB_KV_MAX_KEY_SIZE);

   kvsb_data_config_context *ctx = (kvsb_data_config_context *)(cfg->context);
   return ctx->key_comparator(ctx->key_comparator_context,
                              &(key1->data),
                              key1->length,
                              &(key2->data),
                              key2->length);
}

static void
basic_merge_tuples(const data_config *cfg,
                   const void *       key,
                   const void *       old_raw_data,
                   void *             new_raw_data)
{
   // we don't implement UPDATEs, so this is a no-op:
   // new is always left intact
}

static void
basic_merge_tuples_final(const data_config *cfg,
                         const void *       key,            // IN
                         void *             oldest_raw_data // IN/OUT
)
{
   // we don't implement UPDATEs, so this is a no-op:
   // new is always left intact
}

static message_type
basic_message_class(const data_config *cfg, const void *raw_data)
{
   const basic_message *msg = raw_data;
   switch (msg->type) {
      case MESSAGE_TYPE_INSERT:
         return MESSAGE_TYPE_INSERT;
      case MESSAGE_TYPE_DELETE:
         return MESSAGE_TYPE_DELETE;
      default:
         platform_error_log("data class error: %u\n", msg->type);
         platform_assert(0);
   }
   return MESSAGE_TYPE_INVALID;
}

static void
encode_key(void *key_buffer, const void *key, size_t key_len)
{
   basic_key_encoding *key_enc = (basic_key_encoding *)key_buffer;
   platform_assert(key_len <= UINT8_MAX &&
                   key_len <= SPLINTERDB_KV_MAX_KEY_SIZE);
   key_enc->length = key_len;
   memmove(&(key_enc->data), key, key_len);
}

static void
encode_value(void *       msg_buffer,
             message_type type,
             const void * value,
             size_t       value_len)
{
   basic_message *msg = (basic_message *)msg_buffer;
   msg->type          = type;
   platform_assert(value_len <= UINT32_MAX &&
                   value_len <= SPLINTERDB_KV_MAX_VALUE_SIZE);
   msg->value_length = value_len;
   memmove(&(msg->value), value, value_len);
}

static void
basic_key_to_string(const data_config *cfg,
                    const void *       key,
                    char *             str,
                    size_t             max_len)
{
   debug_hex_encode(str, max_len, key, MAX_KEY_SIZE);
}

static void
basic_message_to_string(const data_config *cfg,
                        const void *       raw_data,
                        char *             str,
                        size_t             max_len)
{
   debug_hex_encode(str, max_len, raw_data, MAX_MESSAGE_SIZE);
}

static data_config _template_basic_data_config = {
   .key_size           = 0,
   .message_size       = 0,
   .min_key            = {0},
   .max_key            = {0}, // splinterdb_init_config sets this, we can ignore
   .key_compare        = basic_key_compare,
   .key_hash           = platform_hash32,
   .merge_tuples       = basic_merge_tuples,
   .merge_tuples_final = basic_merge_tuples_final,
   .message_class      = basic_message_class,
   .key_to_string      = basic_key_to_string,
   .message_to_string  = basic_message_to_string,
};

static int
new_basic_data_config(const size_t max_key_size,   // IN
                      const size_t max_value_size, // IN
                      data_config *out_cfg         // OUT
)
{
   if (max_key_size > SPLINTERDB_KV_MAX_KEY_SIZE ||
       max_key_size < SPLINTERDB_KV_MIN_KEY_SIZE) {
      platform_error_log("max key size %lu must be <= %lu and >= %lu\n",
                         max_key_size,
                         (size_t)(SPLINTERDB_KV_MAX_KEY_SIZE),
                         (size_t)(SPLINTERDB_KV_MIN_KEY_SIZE));
      return EINVAL;
   }
   if (max_value_size > SPLINTERDB_KV_MAX_VALUE_SIZE) {
      platform_error_log("max value size %lu must be <= %lu\n",
                         max_value_size,
                         (size_t)(SPLINTERDB_KV_MAX_VALUE_SIZE));
      return EINVAL;
   }
   // compute sizes for data_config, which are larger than the sizes the
   // application sees
   const size_t key_size = max_key_size + SPLINTERDB_KV_KEY_HDR_SIZE;
   const size_t msg_size = max_value_size + SPLINTERDB_KV_MSG_HDR_SIZE;
   *out_cfg              = _template_basic_data_config;
   out_cfg->key_size     = key_size;
   out_cfg->message_size = msg_size;

   // set max_key
   char max_app_key[SPLINTERDB_KV_MAX_KEY_SIZE];
   memset(max_app_key, 0xFF, sizeof(max_app_key));
   encode_key(out_cfg->max_key, max_app_key, max_key_size);

   // set min key
   // we should only need to set 0-length, but zero the whole thing to be safe
   memset(out_cfg->min_key, 0, sizeof(out_cfg->min_key));

   return 0;
}

// Implementation of public API begins here...

int
splinterdb_kv_create_or_open(const splinterdb_kv_cfg *cfg,      // IN
                             splinterdb_kv **         kvsb_out, // OUT
                             bool                     open_existing)
{
   data_config data_cfg = {0};
   int         res =
      new_basic_data_config(cfg->max_key_size, cfg->max_value_size, &data_cfg);
   if (res != 0) {
      return res; // new_basic_data_config already logs on error
   }

   splinterdb_kv *kvsb = TYPED_ZALLOC(cfg->heap_id, kvsb);
   if (kvsb == NULL) {
      platform_error_log("zalloc error\n");
      return ENOMEM;
   }
   *kvsb = (splinterdb_kv){
      .max_app_key_size = cfg->max_key_size,
      .max_app_val_size = cfg->max_value_size,
      .heap_id          = cfg->heap_id,
      .heap_handle      = cfg->heap_handle,
   };
   kvsb_data_config_context *ctxt = TYPED_ZALLOC(cfg->heap_id, ctxt);
   ctxt->key_comparator =
      cfg->key_comparator ? cfg->key_comparator : default_key_comparator;
   ctxt->key_comparator_context =
      cfg->key_comparator ? cfg->key_comparator_context : NULL;
   kvsb->data_config_context = ctxt; // store, so we can free it later
   data_cfg.context          = ctxt; // make it usable from the callbacks

   // this can be stack-allocated since splinterdb_create does a shallow-copy
   splinterdb_config kvs_config = {
      .filename    = cfg->filename,
      .cache_size  = cfg->cache_size,
      .disk_size   = cfg->disk_size,
      .data_cfg    = data_cfg,
      .heap_id     = cfg->heap_id,
      .heap_handle = cfg->heap_handle,
   };

   if (open_existing) {
      res = splinterdb_open(&kvs_config, &(kvsb->kvs));
   } else {
      res = splinterdb_create(&kvs_config, &(kvsb->kvs));
   }
   if (res != 0) {
      platform_error_log("splinterdb_create error\n");
      return res;
   }
   *kvsb_out = kvsb;
   return res;
}

int
splinterdb_kv_create(const splinterdb_kv_cfg *cfg,     // IN
                     splinterdb_kv **         kvsb_out // OUT
)
{
   return splinterdb_kv_create_or_open(cfg, kvsb_out, FALSE);
}

int
splinterdb_kv_open(const splinterdb_kv_cfg *cfg,     // IN
                   splinterdb_kv **         kvsb_out // OUT
)
{
   return splinterdb_kv_create_or_open(cfg, kvsb_out, TRUE);
}

void
splinterdb_kv_close(splinterdb_kv *kvsb)
{
   splinterdb_close(kvsb->kvs);
   if (kvsb->data_config_context != NULL) {
      platform_free(kvsb->heap_id, kvsb->data_config_context);
   }
   platform_free(kvsb->heap_id, kvsb);
}

void
splinterdb_kv_register_thread(const splinterdb_kv *kvsb)
{
   splinterdb_register_thread(kvsb->kvs);
}

int
splinterdb_kv_insert(const splinterdb_kv *kvsb,
                     const char *         key,
                     const size_t         key_len,
                     const char *         value,
                     const size_t         val_len)
{
   if (key_len > kvsb->max_app_key_size) {
      platform_error_log("splinterdb_kv_insert: key_len, %lu, exceeds"
                         " max_key_size, %lu bytes; key='%.*s'\n",
                         key_len,
                         kvsb->max_app_key_size,
                         (int)key_len,
                         key);
      return EINVAL;
   }

   if (val_len > kvsb->max_app_val_size) {
      platform_error_log("splinterdb_kv_insert: val_len, %lu, exceeds"
                         " max_value_size, %lu bytes; value='%.*s ...'\n",
                         val_len,
                         kvsb->max_app_val_size,
                         (int)kvsb->max_app_val_size,
                         value);
      return EINVAL;
   }

   char key_buffer[MAX_KEY_SIZE]     = {0};
   char msg_buffer[MAX_MESSAGE_SIZE] = {0};
   encode_key(key_buffer, key, key_len);
   encode_value(msg_buffer, MESSAGE_TYPE_INSERT, value, val_len);
   return splinterdb_insert(kvsb->kvs, key_buffer, msg_buffer);
}

int
splinterdb_kv_delete(const splinterdb_kv *kvsb,
                     const char *         key,
                     const size_t         key_len)
{
   if (key_len > kvsb->max_app_key_size) {
      platform_error_log("splinterdb_kv_delete: key_len, %lu, exceeds"
                         " max_key_size, %lu bytes; key='%.*s'\n",
                         key_len,
                         kvsb->max_app_key_size,
                         (int)key_len,
                         key);
      return EINVAL;
   }

   char key_buffer[MAX_KEY_SIZE]     = {0};
   char msg_buffer[MAX_MESSAGE_SIZE] = {0};
   encode_key(key_buffer, key, key_len);
   encode_value(msg_buffer, MESSAGE_TYPE_DELETE, NULL, 0);
   return splinterdb_insert(kvsb->kvs, key_buffer, msg_buffer);
}


int
splinterdb_kv_lookup(const splinterdb_kv *kvsb,
                     const char *         key,           // IN
                     const size_t         key_len,       // IN
                     char *               val,           // OUT
                     const size_t         val_max_len,   // IN
                     size_t *             val_bytes,     // OUT
                     _Bool *              val_truncated, // OUT
                     _Bool *              found_out      // OUT
)
{
   if (key_len > kvsb->max_app_key_size) {
      platform_error_log("splinterdb_kv_lookup: key_len, %lu, exceeds"
                         " max_key_size, %lu bytes; key='%.*s'\n",
                         key_len,
                         kvsb->max_app_key_size,
                         (int)key_len,
                         key);
      return EINVAL;
   }
   char key_buffer[MAX_KEY_SIZE] = {0};
   encode_key(key_buffer, key, key_len);

   char msg_buffer[MAX_MESSAGE_SIZE] = {0};

   basic_message *msg = (basic_message *)msg_buffer;

   // splinterdb_kv.h aims to be public API surface, and so this function
   // exposes the _Bool type rather than  typedef int32 bool
   // which is used elsewhere in the codebase
   // So, we pass in int32 found here, and convert to _Bool below
   int32 found;
   int   result = splinterdb_lookup(kvsb->kvs, key_buffer, msg_buffer, &found);
   if (result != 0) {
      return result;
   }
   *found_out     = found ? TRUE : FALSE;
   size_t n_bytes = SPLINTERDB_KV_MAX_VALUE_SIZE;
   if (val_max_len < n_bytes) {
      n_bytes = val_max_len;
   }
   if (msg->value_length < n_bytes) {
      n_bytes = msg->value_length;
   }
   *val_bytes = n_bytes;
   memmove(val, msg->value, n_bytes);

   *val_truncated = (msg->value_length > n_bytes);

   return result;
}

struct splinterdb_kv_iterator {
   splinterdb_iterator *super;

   void *heap_id;
};

int
splinterdb_kv_iter_init(const splinterdb_kv *    kvsb,         // IN
                        splinterdb_kv_iterator **iter,         // OUT
                        const char *             start_key,    // IN
                        const size_t             start_key_len // IN
)
{
   splinterdb_kv_iterator *it = TYPED_ZALLOC(kvsb->heap_id, it);
   if (it == NULL) {
      platform_error_log("zalloc error\n");
      return ENOMEM;
   }
   it->heap_id = kvsb->heap_id;

   char start_key_buffer[MAX_KEY_SIZE] = {0};
   if (start_key) {
      encode_key(start_key_buffer, start_key, start_key_len);
   }

   int rc = splinterdb_iterator_init(
      kvsb->kvs, &(it->super), start_key ? start_key_buffer : NULL);
   if (rc != 0) {
      platform_error_log("iterator init: %d\n", rc);
      platform_free(kvsb->heap_id, it);
      return rc;
   }

   *iter = it;
   return 0;
}

void
splinterdb_kv_iter_deinit(splinterdb_kv_iterator **iterpp)
{
   splinterdb_kv_iterator *iter = *iterpp;
   splinterdb_iterator_deinit(iter->super);
   platform_free(iter->heap_id, iter);
   *iterpp = NULL;
}

_Bool
splinterdb_kv_iter_valid(splinterdb_kv_iterator *iter)
{
   return splinterdb_iterator_valid(iter->super);
}

void
splinterdb_kv_iter_next(splinterdb_kv_iterator *iter)
{
   splinterdb_iterator_next(iter->super);
}

int
splinterdb_kv_iter_status(const splinterdb_kv_iterator *iter)
{
   return splinterdb_iterator_status(iter->super);
}

void
splinterdb_kv_iter_get_current(splinterdb_kv_iterator *iter,    // IN
                               const char **           key,     // OUT
                               size_t *                key_len, // OUT
                               const char **           value,   // OUT
                               size_t *                val_len  // OUT
)
{
   const char *msg_buffer;
   const char *key_buffer;
   splinterdb_iterator_get_current(iter->super, &key_buffer, &msg_buffer);
   basic_message *     msg     = (basic_message *)msg_buffer;
   basic_key_encoding *key_enc = (basic_key_encoding *)key_buffer;
   *key_len                    = key_enc->length;
   *val_len                    = msg->value_length;
   *key                        = (char *)(key_enc->data);
   *value                      = (char *)(msg->value);
}
