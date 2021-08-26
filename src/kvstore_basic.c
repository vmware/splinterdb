// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * kvstore_basic.c --
 *
 *     Implementation for a simplified key-value store interface.
 *
 *     API deals with keys/values rather than keys/messages.
 */

#include "platform.h"

#include "kvstore.h"
#include "kvstore_basic.h"
#include "util.h"

#include "poison.h"

typedef struct {
   key_comparator_fn key_comparator;
   void *            key_comparator_context;
} kvsb_data_config_context;

struct kvstore_basic {
   size_t max_app_key_size; // size of keys provided by app
   size_t max_app_val_size; // size of values provided by app

   platform_heap_handle heap_handle; // for platform_buffer_create
   platform_heap_id     heap_id;

   kvsb_data_config_context *data_config_context;

   kvstore *kvs;
};

// Length-prefix encoding of a variable-sized key
// We do this so that key comparison can be variable-length
typedef struct PACKED {
   uint8 length;  // offset 0
   uint8 data[0]; // offset 1: 1st byte of value, aligned for 64-bit access
} basic_key_encoding;

#define BASIC_KEY_HDR_SIZE ((uint8)(sizeof(basic_key_encoding)))
static_assert(sizeof(basic_key_encoding) == sizeof(uint8),
              "basic_key_encoding header should have length 1");
static_assert(offsetof(basic_key_encoding, data[0]) == sizeof(uint8),
              "start of data should equal header size");
static_assert((MAX_KEY_SIZE == KVSTORE_BASIC_MAX_KEY_SIZE + BASIC_KEY_HDR_SIZE),
              "KVSTORE_BASIC_MAX_KEY_SIZE should be updated when "
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

#define BASIC_MSG_HDR_SIZE ((uint8)(sizeof(basic_message)))
static_assert(sizeof(basic_message) == sizeof(void *),
              "basic_message header should have the same size as a pointer");
static_assert(offsetof(basic_message, value[0]) == sizeof(void *),
              "start of data value should be aligned to pointer access");
static_assert((MAX_MESSAGE_SIZE ==
               KVSTORE_BASIC_MAX_VALUE_SIZE + BASIC_MSG_HDR_SIZE),
              "KVSTORE_BASIC_MAX_VALUE_SIZE should be updated when "
              "MAX_MESSAGE_SIZE changes");


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
   assert(key1_len <= KVSTORE_BASIC_MAX_KEY_SIZE);
   assert(key2_len <= KVSTORE_BASIC_MAX_KEY_SIZE);
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

   assert(key1->length <= KVSTORE_BASIC_MAX_KEY_SIZE);
   assert(key2->length <= KVSTORE_BASIC_MAX_KEY_SIZE);

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
                   key_len <= KVSTORE_BASIC_MAX_KEY_SIZE);
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
                   value_len <= KVSTORE_BASIC_MAX_VALUE_SIZE);
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
   .max_key            = {0}, // kvstore_init_config sets this, we can ignore
   .key_compare        = basic_key_compare,
   .key_hash           = platform_hash32,
   .merge_tuples       = basic_merge_tuples,
   .merge_tuples_final = basic_merge_tuples_final,
   .message_class      = basic_message_class,
   .key_to_string      = basic_key_to_string,
   .message_to_string  = basic_message_to_string,
   .clobber_message_with_range_delete = NULL};

static int
new_basic_data_config(const size_t max_key_size,   // IN
                      const size_t max_value_size, // IN
                      data_config *out_cfg         // OUT
)
{
   if (max_key_size > KVSTORE_BASIC_MAX_KEY_SIZE ||
       max_key_size < KVSTORE_BASIC_MIN_KEY_SIZE) {
      platform_error_log("max key size %lu must be <= %lu and >= %lu\n",
                         max_key_size,
                         (size_t)(KVSTORE_BASIC_MAX_KEY_SIZE),
                         (size_t)(KVSTORE_BASIC_MIN_KEY_SIZE));
      return EINVAL;
   }
   if (max_value_size > KVSTORE_BASIC_MAX_VALUE_SIZE) {
      platform_error_log("max value size %lu must be <= %lu\n",
                         max_value_size,
                         (size_t)(KVSTORE_BASIC_MAX_VALUE_SIZE));
      return EINVAL;
   }
   // compute sizes for data_config, which are larger than the sizes the
   // application sees
   const size_t key_size = max_key_size + BASIC_KEY_HDR_SIZE;
   const size_t msg_size = max_value_size + BASIC_MSG_HDR_SIZE;
   *out_cfg              = _template_basic_data_config;
   out_cfg->key_size     = key_size;
   out_cfg->message_size = msg_size;

   // set max_key
   char max_app_key[KVSTORE_BASIC_MAX_KEY_SIZE];
   memset(max_app_key, 0xFF, sizeof(max_app_key));
   encode_key(out_cfg->max_key, max_app_key, max_key_size);

   // set min key
   // we should only need to set 0-length, but zero the whole thing to be safe
   memset(out_cfg->min_key, 0, sizeof(out_cfg->min_key));

   return 0;
}

// Implementation of public API begins here...

int
kvstore_basic_init(const kvstore_basic_cfg *cfg,     // IN
                   kvstore_basic **         kvsb_out // OUT
)
{
   data_config data_cfg = {0};
   int         res =
      new_basic_data_config(cfg->max_key_size, cfg->max_value_size, &data_cfg);
   if (res != 0) {
      return res; // new_basic_data_config already logs on error
   }

   kvstore_basic *kvsb = TYPED_ZALLOC(cfg->heap_id, kvsb);
   if (kvsb == NULL) {
      platform_error_log("zalloc error\n");
      return ENOMEM;
   }
   *kvsb = (kvstore_basic){
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

   // this can be stack-allocated since kvstore_init does a shallow-copy
   kvstore_config kvs_config = {
      .filename    = cfg->filename,
      .cache_size  = cfg->cache_size,
      .disk_size   = cfg->disk_size,
      .data_cfg    = data_cfg,
      .heap_id     = cfg->heap_id,
      .heap_handle = cfg->heap_handle,
   };

   res = kvstore_init(&kvs_config, &(kvsb->kvs));
   if (res != 0) {
      platform_error_log("kvstore_init error\n");
      return res;
   }
   *kvsb_out = kvsb;
   return res;
}

void
kvstore_basic_deinit(kvstore_basic *kvsb)
{
   kvstore_deinit(kvsb->kvs);
   if (kvsb->data_config_context != NULL) {
      platform_free(kvsb->heap_id, kvsb->data_config_context);
   }
   platform_free(kvsb->heap_id, kvsb);
}

void
kvstore_basic_register_thread(const kvstore_basic *kvsb)
{
   kvstore_register_thread(kvsb->kvs);
}

int
kvstore_basic_insert(const kvstore_basic *kvsb,
                     const char *         key,
                     const size_t         key_len,
                     const char *         value,
                     const size_t         val_len)
{
   if (key_len > kvsb->max_app_key_size) {
      platform_error_log(
         "kvstore_basic_insert: key_len exceeds max_key_size\n");
      return EINVAL;
   }

   if (val_len > kvsb->max_app_val_size) {
      platform_error_log(
         "kvstore_basic_insert: val_len exceeds max_value_size\n");
      return EINVAL;
   }

   char key_buffer[MAX_KEY_SIZE]     = {0};
   char msg_buffer[MAX_MESSAGE_SIZE] = {0};
   encode_key(key_buffer, key, key_len);
   encode_value(msg_buffer, MESSAGE_TYPE_INSERT, value, val_len);
   return kvstore_insert(kvsb->kvs, key_buffer, msg_buffer);
}

int
kvstore_basic_delete(const kvstore_basic *kvsb,
                     const char *         key,
                     const size_t         key_len)
{
   if (key_len > kvsb->max_app_key_size) {
      platform_error_log(
         "kvstore_basic_delete: key_len exceeds max_key_size\n");
      return EINVAL;
   }

   char key_buffer[MAX_KEY_SIZE]     = {0};
   char msg_buffer[MAX_MESSAGE_SIZE] = {0};
   encode_key(key_buffer, key, key_len);
   encode_value(msg_buffer, MESSAGE_TYPE_DELETE, NULL, 0);
   return kvstore_insert(kvsb->kvs, key_buffer, msg_buffer);
}


int
kvstore_basic_lookup(const kvstore_basic *kvsb,
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
      platform_error_log(
         "kvstore_basic_lookup: key_len exceeds max_key_size\n");
      return EINVAL;
   }
   char key_buffer[MAX_KEY_SIZE] = {0};
   encode_key(key_buffer, key, key_len);

   char msg_buffer[MAX_MESSAGE_SIZE] = {0};

   basic_message *msg = (basic_message *)msg_buffer;

   // kvstore_basic.h aims to be public API surface, and so this function
   // exposes the _Bool type rather than  typedef int32 bool
   // which is used elsewhere in the codebase
   // So, we pass in int32 found here, and convert to _Bool below
   int32 found;
   int   result = kvstore_lookup(kvsb->kvs, key_buffer, msg_buffer, &found);
   if (result != 0) {
      return result;
   }
   *found_out     = found ? TRUE : FALSE;
   size_t n_bytes = KVSTORE_BASIC_MAX_VALUE_SIZE;
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

struct kvstore_basic_iterator {
   kvstore_iterator *super;

   void *heap_id;
};

int
kvstore_basic_iter_init(const kvstore_basic *    kvsb,         // IN
                        kvstore_basic_iterator **iter,         // OUT
                        const char *             start_key,    // IN
                        const size_t             start_key_len // IN
)
{
   kvstore_basic_iterator *it = TYPED_ZALLOC(kvsb->heap_id, it);
   if (it == NULL) {
      platform_error_log("zalloc error\n");
      return ENOMEM;
   }
   it->heap_id = kvsb->heap_id;

   char start_key_buffer[MAX_KEY_SIZE] = {0};
   if (start_key) {
      encode_key(start_key_buffer, start_key, start_key_len);
   }

   int rc = kvstore_iterator_init(
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
kvstore_basic_iter_deinit(kvstore_basic_iterator *iter)
{
   kvstore_iterator_deinit(iter->super);
   platform_free(iter->heap_id, iter);
}

_Bool
kvstore_basic_iter_valid(kvstore_basic_iterator *iter)
{
   return kvstore_iterator_valid(iter->super);
}

void
kvstore_basic_iter_next(kvstore_basic_iterator *iter)
{
   kvstore_iterator_next(iter->super);
}

int
kvstore_basic_iter_status(const kvstore_basic_iterator *iter)
{
   return kvstore_iterator_status(iter->super);
}

void
kvstore_basic_iter_get_current(kvstore_basic_iterator *iter,    // IN
                               const char **           key,     // OUT
                               size_t *                key_len, // OUT
                               const char **           value,   // OUT
                               size_t *                val_len  // OUT
)
{
   const char *msg_buffer;
   const char *key_buffer;
   kvstore_iterator_get_current(iter->super, &key_buffer, &msg_buffer);
   basic_message *     msg     = (basic_message *)msg_buffer;
   basic_key_encoding *key_enc = (basic_key_encoding *)key_buffer;
   *key_len                    = key_enc->length;
   *val_len                    = msg->value_length;
   *key                        = (char *)(key_enc->data);
   *value                      = (char *)(msg->value);
}