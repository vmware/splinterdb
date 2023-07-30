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


static void
key_to_string(const data_config *cfg, slice key, char *str, size_t max_len)
{
   debug_hex_encode(str, max_len, slice_data(key), slice_length(key));
}

static void
message_to_string(const data_config *cfg,
                  message            msg,
                  char              *str,
                  size_t             max_len)
{
   debug_hex_encode(str, max_len, message_data(msg), message_length(msg));
}


void
default_data_config_init(const size_t max_key_size, // IN
                         data_config *out_cfg       // OUT
)
{
   data_config cfg = {
      .max_key_size       = max_key_size,
      .key_compare        = key_compare,
      .key_hash           = platform_hash32,
      .merge_tuples       = NULL,
      .merge_tuples_final = NULL,
      .key_to_string      = key_to_string,
      .message_to_string  = message_to_string,
   };

   *out_cfg = cfg;
}
