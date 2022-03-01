// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "test_data.h"

typedef struct data_test_config {
   data_config super;
   uint64      payload_size_limit;
} data_test_config;

static int
test_data_key_cmp(const data_config *cfg,
                  uint64             key1_len,
                  const void        *key1,
                  uint64             key2_len,
                  const void        *key2)
{
   uint64 mlen = key1_len < key2_len ? key1_len : key2_len;
   int    r    = memcmp(key1, key2, mlen);
   if (r) {
      return r;
   } else if (key1_len < key2_len) {
      return -1;
   } else if (key2_len < key1_len) {
      return 1;
   }
   return 0;
}

void
test_data_generate_message(const data_config *cfg,
                           message_type       type,
                           uint8              ref_count,
                           writable_buffer   *msg)
{
   uint64 payload_size = 0;
   if (type == MESSAGE_TYPE_INSERT) {
      const data_test_config *tdcfg = (const data_test_config *)cfg;
      // A coupla good ol' random primes
      payload_size = (253456363ULL + (uint64)ref_count * 750599937895091ULL)
                     % tdcfg->payload_size_limit;
   }
   writable_buffer_set_length(msg, sizeof(data_handle) + payload_size);
   data_handle *dh  = writable_buffer_data(msg);
   dh->message_type = type;
   dh->ref_count    = ref_count;
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
                       uint64             key_len,
                       const void        *key,
                       uint64             old_raw_data_len,
                       const void        *old_raw_data,
                       writable_buffer   *new_raw_data)
{
   assert(sizeof(data_handle) <= old_raw_data_len);
   assert(sizeof(data_handle) <= writable_buffer_length(new_raw_data));

   const data_handle *old_data = old_raw_data;
   data_handle       *new_data = writable_buffer_data(new_raw_data);
   debug_assert(old_data != NULL);
   debug_assert(new_data != NULL);
   // platform_log("data_merge_tuples: op=%d old_op=%d key=0x%08lx old=%d
   // new=%d\n",
   //         new_data->message_type, old_data->message_type, htobe64(*(uint64
   //         *)key), old_data->ref_count, new_data->ref_count);

   switch (new_data->message_type) {
      case MESSAGE_TYPE_INSERT:
      case MESSAGE_TYPE_DELETE:
         break;
      case MESSAGE_TYPE_UPDATE:
         switch (old_data->message_type) {
            case MESSAGE_TYPE_INSERT:
               new_data->message_type = MESSAGE_TYPE_INSERT;
               new_data->ref_count += old_data->ref_count;
               break;
            case MESSAGE_TYPE_UPDATE:
               new_data->ref_count += old_data->ref_count;
               break;
            case MESSAGE_TYPE_DELETE:
               if (new_data->ref_count == 0) {
                  new_data->message_type = MESSAGE_TYPE_DELETE;
               } else {
                  new_data->message_type = MESSAGE_TYPE_INSERT;
               }
               break;
            default:
               platform_assert(0);
         }
         test_data_generate_message(
            cfg, new_data->message_type, new_data->ref_count, new_raw_data);
         break;
      default:
         platform_assert(0);
   }

   return 0;
   // if (new_data->message_type == MESSAGE_TYPE_INSERT) {
   //   ;
   //} else if (new_data->message_type == MESSAGE_TYPE_DELETE) {
   //   ;
   //} else if (old_data == NULL || old_data->message_type ==
   // MESSAGE_TYPE_DELETE) {
   //   if (new_data->ref_count == 0)
   //      new_data->message_type = MESSAGE_TYPE_DELETE;
   //   else
   //      new_data->message_type = MESSAGE_TYPE_INSERT;
   //} else if (old_data->message_type == MESSAGE_TYPE_INSERT) {
   //   new_data->message_type = MESSAGE_TYPE_INSERT;
   //   new_data->ref_count += old_data->ref_count;
   //} else {
   //   new_data->ref_count += old_data->ref_count;
   //}
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
   assert(sizeof(data_handle) <= writable_buffer_length(oldest_raw_data));

   data_handle *old_data = writable_buffer_data(oldest_raw_data);
   debug_assert(old_data != NULL);

   if (old_data->message_type == MESSAGE_TYPE_UPDATE) {
      test_data_generate_message(
         cfg,
         (old_data->ref_count == 0) ? MESSAGE_TYPE_DELETE : MESSAGE_TYPE_INSERT,
         old_data->ref_count,
         oldest_raw_data);
   }
   return 0;
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
   assert(sizeof(data_handle) <= raw_data_len);

   const data_handle *data = raw_data;
   switch (data->message_type) {
      case MESSAGE_TYPE_INSERT:
         return data->ref_count == 0 ? MESSAGE_TYPE_DELETE
                                     : MESSAGE_TYPE_INSERT;
      case MESSAGE_TYPE_DELETE:
         return MESSAGE_TYPE_DELETE;
      case MESSAGE_TYPE_UPDATE:
         return MESSAGE_TYPE_UPDATE;
      default:
         platform_error_log("data class error: %d\n", data->message_type);
         platform_assert(0);
   }
   return MESSAGE_TYPE_INVALID;
}

static void
test_data_key_to_string(const data_config *cfg,
                        uint64             key_len,
                        const void        *key,
                        char              *str,
                        size_t             len)
{
   debug_hex_encode(str, len, key, key_len);
}

static void
test_data_message_to_string(const data_config *cfg,
                            uint64             raw_data_len,
                            const void        *raw_data,
                            char              *str,
                            size_t             len)
{
   debug_hex_encode(str, len, raw_data, raw_data_len);
}

static data_test_config data_test_config_internal = {
   .super =
      {
         .key_size           = 24,
         .min_key            = {0},
         .max_key            = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                     0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                     0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
         .key_compare        = test_data_key_cmp,
         .key_hash           = platform_hash32,
         .key_to_string      = test_data_key_to_string,
         .message_to_string  = test_data_message_to_string,
         .merge_tuples       = test_data_merge_tuples,
         .merge_tuples_final = test_data_merge_tuples_final,
         .message_class      = test_data_message_class,
      },
   .payload_size_limit = 24};

data_config *test_data_config = &data_test_config_internal.super;
