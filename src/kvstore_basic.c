// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * kvstore_basic.c --
 *
 *     Implementation for a simplified key-value store interface.
 *
 *     API deals with keys/values rather than keys/messages.
 */
#include <unistd.h>
#include "platform.h"

#include "splinterdb/kvstore.h"
#include "splinterdb/kvstore_basic.h"
#include "util.h"

#include "poison.h"

// clang-format off

// Construct a simplified "external" limits descriptor that one can probe
// using 'strings'
const char SplinterDB_Limits[] =
                "SplinterDB Limits"
                ": MIN_KEY_SIZE=" STRINGIFY_VALUE(KVSTORE_BASIC_MIN_KEY_SIZE)
                ", MAX_KEY_SIZE=" STRINGIFY_VALUE(MAX_KEY_SIZE)
                ", MAX_MESSAGE_SIZE=" STRINGIFY_VALUE(MAX_MESSAGE_SIZE)
                ;

// clang-format on

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

static_assert((sizeof(basic_key_encoding) == KVSTORE_BASIC_KEY_HDR_SIZE),
              "basic_key_encoding header should have length 1");

static_assert(offsetof(basic_key_encoding, data[0]) == sizeof(uint8),
              "start of data should equal header size");

static_assert((MAX_KEY_SIZE ==
               (KVSTORE_BASIC_MAX_KEY_SIZE + KVSTORE_BASIC_KEY_HDR_SIZE)),
              "KVSTORE_BASIC_MAX_KEY_SIZE should be updated when "
              "MAX_KEY_SIZE changes");

#define KVSB_PACK_PTR __attribute__((packed, aligned(__alignof__(void *))))

// basic implementation of data_config
// uses a pointer-sized header to hold the message type (insert vs delete)
// and the size of the value in bytes
typedef struct KVSB_PACK_PTR {
   uint8  type;         // offset 0
   uint8  n_chunks;     // # of chunks of max-value size inserted
   uint16 _reserved2;   // offset 2
   uint32 value_length; // offset 4
   uint8  value[0]; // offset 8: 1st byte of value, aligned for 64-bit access
} basic_message;

static_assert(sizeof(basic_message) == KVSTORE_BASIC_MSG_HDR_SIZE,
              "basic_message header should have the same size as a pointer");

static_assert(offsetof(basic_message, value[0]) == sizeof(void *),
              "start of data value should be aligned to pointer access");

// Return values from check on insert's physical limits
typedef enum {
     KV_INS_FAIL         = 0
   , KV_INS_WIDEVAL
   , KV_INS_INROW
} kvs_ins_phycheck_rv;

// Funtion prototypes

static int kvstore_insert_wideval(const kvstore_basic *kvsb,
                     const char *         key,
                     const size_t         key_len,
                     const char *         value,
                     const size_t         val_len,
                     int *                nchunks);

static kvs_ins_phycheck_rv kvstore_basic_check_insert_limits(const kvstore_basic *kvsb,
                     const char *         key,
                     const size_t         key_len,
                     const char *         value,
                     const size_t         val_len);

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

int
kvstore_basic_key_compare(const kvstore_basic *kvsb,
                          const char *key1,
                          size_t      key1_len,
                          const char *key2,
                          size_t      key2_len)
{
   kvsb_data_config_context *ctx = kvsb->data_config_context;
   return ctx->key_comparator(ctx->key_comparator_context,
                              key1, key1_len, key2, key2_len);

}

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

/*
 * "Encode" a value into the message buffer. Sets up the fields in the
 * basic_message{} struct, and copies over user-supplied value to the
 * msg buffer.
 */
static void
encode_value(void *       msg_buffer,
             message_type type,
             const void * value,
             size_t       value_len,
             int          n_chunks)
{
   basic_message *msg = (basic_message *)msg_buffer;
   msg->type          = type;
   platform_assert(value_len <= UINT32_MAX &&
                   value_len <= KVSTORE_BASIC_MAX_VALUE_SIZE);
   msg->n_chunks = n_chunks;
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
};

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
   const size_t key_size = max_key_size + KVSTORE_BASIC_KEY_HDR_SIZE;
   const size_t msg_size = max_value_size + KVSTORE_BASIC_MSG_HDR_SIZE;
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
kvstore_basic_create_or_open(const kvstore_basic_cfg *cfg,      // IN
                             kvstore_basic **         kvsb_out, // OUT
                             bool                     open_existing)
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

   // this can be stack-allocated since kvstore_create does a shallow-copy
   kvstore_config kvs_config = {
      .filename    = cfg->filename,
      .cache_size  = cfg->cache_size,
      .disk_size   = cfg->disk_size,
      .data_cfg    = data_cfg,
      .heap_id     = cfg->heap_id,
      .heap_handle = cfg->heap_handle,
   };

   if (open_existing) {
      res = kvstore_open(&kvs_config, &(kvsb->kvs));
   } else {
      res = kvstore_create(&kvs_config, &(kvsb->kvs));
   }
   if (res != 0) {
      platform_error_log("kvstore_create error\n");
      return res;
   }
   *kvsb_out = kvsb;
   return res;
}

int
kvstore_basic_create(const kvstore_basic_cfg *cfg,     // IN
                     kvstore_basic **         kvsb_out // OUT
)
{
   return kvstore_basic_create_or_open(cfg, kvsb_out, FALSE);
}

int
kvstore_basic_open(const kvstore_basic_cfg *cfg,     // IN
                   kvstore_basic **         kvsb_out // OUT
)
{
   return kvstore_basic_create_or_open(cfg, kvsb_out, TRUE);
}

void
kvstore_basic_close(kvstore_basic *kvsb)
{
   kvstore_close(kvsb->kvs);
   if (kvsb->data_config_context != NULL) {
      platform_free(kvsb->heap_id, kvsb->data_config_context);
   }
   platform_free(kvsb->heap_id, kvsb);
}

void
kvstore_basic_register_thread(const kvstore_basic *kvsb)
{
   kvstore_register_thread(kvsb->kvs);
   /*
   platform_error_log("\n[Sp: ThreadID=%lu, %d] Registered\n",
                      platform_get_tid(), gettid());
   */
}

int
kvstore_basic_insert(const kvstore_basic *kvsb,
                     const char *         key,
                     const size_t         key_len,
                     const char *         value,
                     const size_t         val_len)
{
   kvs_ins_phycheck_rv ins_rv;
   ins_rv = kvstore_basic_check_insert_limits(kvsb,
                                          key,
                                          key_len,
                                          value,
                                          val_len);
   if (!ins_rv) {
      return EINVAL;
   }

   char key_buffer[MAX_KEY_SIZE]     = {0};
   char msg_buffer[MAX_MESSAGE_SIZE] = {0};
   int rv = 0;
   if (ins_rv == KV_INS_WIDEVAL) {

       // Also used to return chunk ctr at which insertion fails
       int nchunks = 0;
       rv = kvstore_insert_wideval(kvsb, key, key_len, value, val_len,
                                   &nchunks);
       if (rv) {
           platform_error_log("\n*** [Sp: ThreadID=%lu, %d] Insert wide values"
                              " failed while inserting"
                              " chunk number %d;"
                              " key_len=%lu, val_len=%lu, key='%.*s'\n",
                              platform_get_tid(), gettid(),
                              nchunks, key_len, val_len,
                              (int) key_len, key);
           return rv;
       }
   }
   else {
       encode_key(key_buffer, key, key_len);
       encode_value(msg_buffer, MESSAGE_TYPE_INSERT, value, val_len, 0);
       rv = kvstore_insert(kvsb->kvs, key_buffer, msg_buffer);
   }
   /*
   platform_error_log("\n[Sp: ThreadID=%lu, %d] Insert %s succeeded:"
                      " key_len=%lu, key='%.*s'\n",
                      platform_get_tid(), gettid(),
                      ((ins_rv == KV_INS_WIDEVAL) ? "wide row" : ""),
                      key_len, (int) key_len, key);
    */
   return rv;
}

/*
 * Wrapper routine to drive the insertion of wide values into the KVS.
 * Chunk up the value into max–value-size pieces, and insert multiple
 * chunks. The 2nd chunk onwards, the key is extended with a 1-byte counter
 * to number the chunks. (1st chunk is implicitly ctr==0.)
 * Caller has verified that we have sufficient space in the key to track
 * counter of chunks.
 *
 * Returns: Insert return value. # of chunks inserted is returned as an
 *  output parameter. If function fails overall, nchunks will give the
 *  chunk number at which last attempted insert failed.
 */
static int
kvstore_insert_wideval(const kvstore_basic *kvsb,
                       const char *         key,
                       const size_t         key_len,
                       const char *         value,
                       const size_t         val_len,
                       int *                nchunks)
{
   int ins_rv = 0;
   char key_buffer[MAX_KEY_SIZE]     = {0};
   char msg_buffer[MAX_MESSAGE_SIZE] = {0};

   // Init local variables to insert 0th chunk
   char * valp = (char *) value;
   int cctr = 0;
   int rem_len = (int) val_len;
   size_t keylen = key_len;
   size_t vallen = val_len;

   // # of chunks for entire value @max_app_val_size per key
   int n_chunks = CEILING(val_len, kvsb->max_app_val_size);

   platform_error_log("**** Info! %s: %d chunks inserted\n",
                      __FUNCTION__, n_chunks);
    // Chunk up values into max-value pieces, and insert each key
    while (rem_len > 0) {
        char thiskey[KVSTORE_BASIC_MAX_KEY_SIZE];
        if (cctr) {
            snprintf(thiskey, sizeof(thiskey), "%.*s%d",
                     (int) key_len, key, cctr);
        }
        else {
            snprintf(thiskey, sizeof(thiskey), "%.*s", (int) key_len, key);
        }

       vallen = ((rem_len >= kvsb->max_app_val_size)
                    ? kvsb->max_app_val_size : rem_len);
       encode_key(key_buffer, thiskey, keylen);
       encode_value(msg_buffer, MESSAGE_TYPE_INSERT, valp, vallen, n_chunks);

       // Insert new chunk, and return immediately on failure.
       ins_rv = kvstore_insert(kvsb->kvs, key_buffer, msg_buffer);
       if (ins_rv)
           break;

        // Position to start of next chunk of value
        valp += kvsb->max_app_val_size;
        rem_len -= kvsb->max_app_val_size;

        // All keys after the 0th key are byte-extended with chunk counter.
        // Total # of chunks in entire stream is tracked only in the 0th
        // chunk.
        if (!cctr) {
            keylen++;
            n_chunks = 0;
        }
        cctr++;
    }
    // Return # of chunks we inserted, which will also be the chunk #
    // we last tried to insert and failed.
    if (nchunks)
        *nchunks = cctr;
    return ins_rv;
}

int
kvstore_basic_delete(const kvstore_basic *kvsb,
                     const char *         key,
                     const size_t         key_len)
{
   if (key_len > kvsb->max_app_key_size) {
      platform_error_log("kvstore_basic_delete: key_len, %lu, exceeds"
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
   encode_value(msg_buffer, MESSAGE_TYPE_DELETE, NULL, 0, 0);
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
      platform_error_log("kvstore_basic_lookup: key_len, %lu, exceeds"
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

   // Track lookups of wide-values, where we are missing out on data
   if (msg->n_chunks) {
      platform_error_log("\n**** Warning! kvstore_basic_lookup: %d chunks"
                         " of wide-value are missing. Key_len, %lu"
                         " max_key_size, %lu bytes; key='%.*s'\n",
                         (msg->n_chunks - 1), key_len,
                         kvsb->max_app_key_size,
                         (int)key_len,
                         key);
      return -1;
   }
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
kvstore_basic_iter_deinit(kvstore_basic_iterator **iterpp)
{
   kvstore_basic_iterator *iter = *iterpp;
   kvstore_iterator_deinit(iter->super);
   platform_free(iter->heap_id, iter);
   *iterpp = NULL;
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

/*
 * Error checking for physical limits that we can support during inserts.
 *
 * Returns:
 */
static kvs_ins_phycheck_rv
kvstore_basic_check_insert_limits(const kvstore_basic *kvsb,
                                  const char *         key,
                                  const size_t         key_len,
                                  const char *         value,
                                  const size_t         val_len)
{
   if (key_len > kvsb->max_app_key_size) {
      platform_error_log("kvstore_basic_insert: key_len, %lu, exceeds"
                         " max_key_size, %lu bytes; key='%.*s'\n",
                         key_len,
                         kvsb->max_app_key_size,
                         (int)key_len,
                         key);
      return KV_INS_FAIL;
   }

   // In order to support value sizes that are larger than the limit
   // supported, we chunk up large values, followed by multiple inserts.
   // These chunks are tracked by a 1-byte value-counter snuck away at
   // end of each key. So, it's a physical limitation error if the value
   // is "too big" and there is no spare byte in the key.
   if (val_len > kvsb->max_app_val_size) {
      if (key_len == kvsb->max_app_key_size) {
          platform_error_log("kvstore_basic_insert: Cannot insert large"
                             " value. val_len, %lu, exceeds"
                             " max_value_size, %lu bytes, and key is at"
                             " max_key_size, %lu bytes. key='%.*s'."
                             " Value=\n",
                             val_len,
                             kvsb->max_app_val_size,
                             kvsb->max_app_key_size,
                             (int) key_len, key);
          prBytes(value, kvsb->max_app_val_size);
          return KV_INS_FAIL;
      }
      return KV_INS_WIDEVAL;
   }

   if (val_len > kvsb->max_app_val_size) {
      platform_error_log("kvstore_basic_insert: val_len, %lu, exceeds"
                         " max_value_size, %lu bytes; key='%.*s', value:\n",
                         val_len,
                         kvsb->max_app_val_size,
                         (int) key_len, key);
      prBytes(value, val_len);
      return KV_INS_FAIL;
   }
   return KV_INS_INROW;
}
