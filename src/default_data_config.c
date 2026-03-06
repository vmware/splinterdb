// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

// A default data_config suitable for simple key/value applications
// using a lexicographical sort-order (memcmp)
//
// This data_config does not support blind mutation operations

#include "splinterdb/default_data_config.h"
#include "util.h"
#include "platform_hash.h"

#include "poison.h"


typedef struct ONDISK {
   uint8 type;
   uint8 value[0];
} message_encoding;

static int
key_compare(const data_config *cfg, user_key key1, user_key key2)
{
   platform_assert(slice_data(key1.key) != NULL);
   platform_assert(slice_data(key2.key) != NULL);

   return slice_lex_cmp(key1.key, key2.key);
}

static uint32
key_hash(const data_config *cfg, user_key key, uint32 seed)
{
   platform_assert(slice_data(key.key) != NULL);
   return platform_hash32(slice_data(key.key), slice_length(key.key), seed);
}

static void
key_to_string(const data_config *cfg, user_key key, char *str, size_t max_len)
{
   debug_hex_encode(str, max_len, slice_data(key.key), slice_length(key.key));
}

static void
message_to_string(const data_config *cfg,
                  message            msg,
                  char              *str,
                  size_t             max_len)
{
   debug_hex_encode(str, max_len, message_data(msg), message_length(msg));
}


/*
 * Function to initialize application-specific data_config{} struct
 * with default values.
 */
void
default_data_config_init(const size_t max_key_size, // IN
                         data_config *out_cfg       // OUT
)
{
   data_config cfg = {
      .max_key_size       = max_key_size,
      .key_compare        = key_compare,
      .key_hash           = key_hash,
      .merge_tuples       = NULL,
      .merge_tuples_final = NULL,
      .key_to_string      = key_to_string,
      .message_to_string  = message_to_string,
   };

   *out_cfg = cfg;
}
