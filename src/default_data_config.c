// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

// A default data_config suitable for simple key/value applications
// using a lexicographical sort-order (memcmp)
//
// This data_config does not support blind mutation operations

#include "platform.h"

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "util.h"

#include "poison.h"


typedef struct ONDISK {
   uint8 type;
   uint8 value[0];
} message_encoding;

static int
key_compare(const data_config *cfg, slice key1, slice key2)
{
   platform_assert(slice_data(key1) != NULL);
   platform_assert(slice_data(key2) != NULL);

   return slice_lex_cmp(key1, key2);
}


static message_type
message_class(const data_config *cfg, slice raw_msg)
{
   const message_encoding *msg = slice_data(raw_msg);
   switch (msg->type) {
      case MESSAGE_TYPE_INSERT:
         return MESSAGE_TYPE_INSERT;
      case MESSAGE_TYPE_DELETE:
         return MESSAGE_TYPE_DELETE;
      default:
         platform_assert(FALSE, "unknown message type: %u", msg->type);
   }
   return MESSAGE_TYPE_INVALID; // unreachable
}

static int
merge_tuples(const data_config *cfg,
             const slice        key,
             const slice        old_raw_message,
             writable_buffer   *new_data)
{
   // we don't implement UPDATEs, so this is a no-op:
   // new is always left intact
   return 0;
}

static int
merge_tuples_final(const data_config *cfg,
                   uint64             key_len,
                   const void        *key,        // IN
                   writable_buffer   *oldest_data // IN/OUT
)
{
   // we don't implement UPDATEs, so this is a no-op:
   // new is always left intact
   return 0;
}


static void
key_or_message_to_string(const data_config *cfg,
                         const slice        key_or_msg,
                         char              *str,
                         size_t             max_len)
{
   debug_hex_encode(
      str, max_len, slice_data(key_or_msg), slice_length(key_or_msg));
}


static int
encode_message(message_type type,
               slice        in_value,
               size_t       dst_msg_buffer_len,
               void        *dst_msg_buffer,
               size_t      *out_encoded_len)
{
   message_encoding *msg = (message_encoding *)dst_msg_buffer;
   msg->type             = type;
   if (slice_length(in_value) + sizeof(message_encoding) > dst_msg_buffer_len) {
      platform_error_log("encode_message: "
                         "length of value %lu + encoding header %lu exceeds "
                         "buffer size %lu bytes.",
                         slice_length(in_value),
                         sizeof(message_encoding),
                         dst_msg_buffer_len);
      return EINVAL;
   }
   if (slice_length(in_value) > 0) {
      memmove(&(msg->value), slice_data(in_value), slice_length(in_value));
   }
   *out_encoded_len = sizeof(message_encoding) + slice_length(in_value);
   return 0;
}

static int
decode_message(slice in_msg, size_t *out_value_len, const char **out_value)
{
   if (slice_length(in_msg) < sizeof(message_encoding)) {
      platform_error_log("decode_message: message_buffer_len=%lu must be "
                         "at least %lu bytes.",
                         slice_length(in_msg),
                         sizeof(message_encoding));
      return EINVAL;
   }
   const message_encoding *msg = (const message_encoding *)slice_data(in_msg);
   *out_value_len = slice_length(in_msg) - sizeof(message_encoding);
   *out_value     = (const void *)(msg->value);
   return 0;
}


void
default_data_config_init(const size_t max_key_size, // IN
                         data_config *out_cfg       // OUT
)
{
   platform_assert(max_key_size <= SPLINTERDB_MAX_KEY_SIZE && max_key_size > 0,
                   "default_data_config_init: must have 0 < max_key_size (%lu) "
                   "< SPLINTERDB_MAX_KEY_SIZE (%d)",
                   max_key_size,
                   SPLINTERDB_MAX_KEY_SIZE);

   data_config cfg = {
      .key_size           = max_key_size,
      .min_key            = {0},
      .min_key_length     = 0,
      .max_key            = {0}, // see memset below
      .max_key_length     = max_key_size,
      .key_compare        = key_compare,
      .key_hash           = platform_hash32,
      .message_class      = message_class,
      .merge_tuples       = merge_tuples,
      .merge_tuples_final = merge_tuples_final,
      .key_to_string      = key_or_message_to_string,
      .message_to_string  = key_or_message_to_string,
      .encode_message     = encode_message,
      .decode_message     = decode_message,
   };

   memset(cfg.max_key, 0xFF, sizeof(cfg.max_key));

   *out_cfg = cfg;
}
