// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "test_data.h"
#include "data_internal.h"

static int
test_data_key_cmp(const data_config *cfg,
                  uint64             key1_len,
                  const void        *key1,
                  uint64             key2_len,
                  const void        *key2)
{
   return default_data_key_cmp(cfg, key1_len, key1, key2_len, key2);
}

/*
 *-----------------------------------------------------------------------------
 * data_merge_tuples --
 *
 *      Given two data messages, merges them by decoding the type of messages.
 *      Returns the result in new_data.
 *-----------------------------------------------------------------------------
 */
static int
test_data_merge_tuples(const data_config *cfg,
                       uint64             key_len,
                       const void        *key,
                       uint64             old_raw_data_len,
                       const void        *old_raw_data,
                       writable_buffer   *new_raw_data)
{
   return default_data_merge_tuples(
      cfg, key_len, key, old_raw_data_len, old_raw_data, new_raw_data);
}

/*
 *-----------------------------------------------------------------------------
 * data_merge_tuples_final --
 *
 *      Called for non-MESSAGE_TYPE_INSERT messages when they are determined to
 *be the oldest message in the system.
 *
 *      Can change data_class or contents.  If necessary, update new_data.
 *-----------------------------------------------------------------------------
 */
static int
test_data_merge_tuples_final(const data_config *cfg,
                             uint64             key_len,
                             const void        *key,           // IN
                             writable_buffer   *oldest_raw_data) // IN/OUT
{
   return default_data_merge_tuples_final(cfg, key_len, key, oldest_raw_data);
}

/*
 *-----------------------------------------------------------------------------
 * data_class --
 *
 *      Given a data message, returns its message class.
 *-----------------------------------------------------------------------------
 */
static message_type
test_data_message_class(const data_config *cfg,
                        uint64             raw_data_len,
                        const void        *raw_data)
{
   return default_data_message_class(cfg, raw_data_len, raw_data);
}

static void
test_data_key_to_string(const data_config *cfg,
                        uint64             key_len,
                        const void        *key,
                        char              *str,
                        size_t             len)
{
   return default_data_key_to_string(cfg, key_len, key, str, len);
}

static void
test_data_message_to_string(const data_config *cfg,
                            uint64             raw_data_len,
                            const void        *raw_data,
                            char              *str,
                            size_t             len)
{
   return default_data_message_to_string(cfg, raw_data_len, raw_data, str, len);
}

/*
 * Provide a test-version of data_config, just to get tests working.
 */
data_config test_data_config = {
   .key_size           = DEFAULT_KEY_SIZE,
   .message_size       = DEFAULT_KEY_SIZE,
   .min_key            = {0},
   .max_key            = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
               0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
               0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
   .key_compare        = test_data_key_cmp,
   .key_hash           = platform_hash32,
   .message_class      = test_data_message_class,
   .merge_tuples       = test_data_merge_tuples,
   .merge_tuples_final = test_data_merge_tuples_final,
   .key_to_string      = test_data_key_to_string,
   .message_to_string  = test_data_message_to_string,
};
