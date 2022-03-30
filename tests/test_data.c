// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "test_data.h"
#include "data_internal.h"

typedef struct data_test_config {
   data_config super;
   uint64      payload_size_limit;
} data_test_config;

static int
test_data_key_cmp(const data_config *cfg, slice key1, slice key2)
{
   return slice_lex_cmp(key1, key2);
}

void
test_data_generate_message(const data_config *cfg,
                           message_type       type,
                           uint8              ref_count,
                           merge_accumulator *msg)
{
   uint64 payload_size = 0;
   if (type == MESSAGE_TYPE_INSERT) {
      const data_test_config *tdcfg = (const data_test_config *)cfg;
      // A coupla good ol' random primes
      payload_size = (253456363ULL + (uint64)ref_count * 750599937895091ULL)
                     % tdcfg->payload_size_limit;
   }
   merge_accumulator_set_class(msg, type);
   merge_accumulator_resize(msg, sizeof(data_handle) + payload_size);
   data_handle *dh = merge_accumulator_data(msg);
   dh->ref_count   = ref_count;
   memset(dh->data, ref_count, payload_size);
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
                       slice              key,
                       message            old_raw_message,
                       merge_accumulator *new_raw_message)
{
   platform_assert(sizeof(data_handle) <= message_length(old_raw_message));
   platform_assert(sizeof(data_handle)
                   <= merge_accumulator_length(new_raw_message));
   platform_assert(merge_accumulator_message_class(new_raw_message)
                   == MESSAGE_TYPE_UPDATE);
   platform_assert(message_class(old_raw_message) != MESSAGE_TYPE_DELETE);

   const data_handle *old_data = message_data(old_raw_message);
   data_handle       *new_data = merge_accumulator_data(new_raw_message);
   debug_assert(old_data != NULL);
   debug_assert(new_data != NULL);

   int8         result_ref_count = old_data->ref_count + new_data->ref_count;
   message_type result_type      = message_class(old_raw_message);
   if (result_ref_count == 0 && result_type == MESSAGE_TYPE_INSERT) {
      result_type = MESSAGE_TYPE_DELETE;
   }
   test_data_generate_message(
      cfg, result_type, result_ref_count, new_raw_message);

   return 0;
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
                             slice              key,
                             merge_accumulator *oldest_raw_data) // IN/OUT
{
   platform_assert(merge_accumulator_message_class(oldest_raw_data)
                   == MESSAGE_TYPE_UPDATE);
   assert(sizeof(data_handle) <= merge_accumulator_length(oldest_raw_data));

   data_handle *old_data = merge_accumulator_data(oldest_raw_data);
   debug_assert(old_data != NULL);

   test_data_generate_message(cfg,
                              (old_data->ref_count == 0) ? MESSAGE_TYPE_DELETE
                                                         : MESSAGE_TYPE_INSERT,
                              old_data->ref_count,
                              oldest_raw_data);
   return 0;
}

static void
test_data_key_to_string(const data_config *cfg,
                        slice              key,
                        char              *str,
                        size_t             len)
{
   debug_hex_encode(str, len, slice_data(key), slice_length(key));
}

static void
test_data_message_to_string(const data_config *cfg,
                            message            message,
                            char              *str,
                            size_t             len)
{
   debug_hex_encode(str, len, message_data(message), message_length(message));
}

static data_test_config data_test_config_internal = {
   .super =
      {
         .key_size           = 24,
         .min_key            = {0},
         .min_key_length     = 0,
         .max_key            = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                     0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                     0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
         .max_key_length     = 24,
         .key_compare        = test_data_key_cmp,
         .key_hash           = platform_hash32,
         .key_to_string      = test_data_key_to_string,
         .message_to_string  = test_data_message_to_string,
         .merge_tuples       = test_data_merge_tuples,
         .merge_tuples_final = test_data_merge_tuples_final,
      },
   .payload_size_limit = 24};

data_config *test_data_config = &data_test_config_internal.super;
