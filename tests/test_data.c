// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "test_data.h"

static int
test_data_key_cmp(const data_config *cfg,
                  const void *key1,
                  const void *key2)
{
  return memcmp(key1, key2, cfg->key_size);
}

/*
 *-----------------------------------------------------------------------------
 *
 * data_merge_tuples --
 *
 *      Given two data messages, merges them by decoding the type of messages.
 *      Returns the result in new_data.
 *
 *-----------------------------------------------------------------------------
 */

static void
test_data_merge_tuples(const data_config *cfg,
                       const void *key,
                       const void *old_raw_data,
                       void *new_raw_data)
{
   const data_handle *old_data = old_raw_data;
   data_handle *new_data = new_raw_data;
   debug_assert(old_data != NULL);
   debug_assert(new_data != NULL);
   //platform_log("data_merge_tuples: op=%d old_op=%d key=0x%08lx old=%d new=%d\n",
   //         new_data->message_type, old_data->message_type, htobe64(*(uint64 *)key),
   //         old_data->ref_count, new_data->ref_count);

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
               } else  {
                  new_data->message_type = MESSAGE_TYPE_INSERT;
               }
               break;
            default:
               platform_assert(0);
         }
         break;
      default:
         platform_assert(0);
   }

   //if (new_data->message_type == MESSAGE_TYPE_INSERT) {
   //   ;
   //} else if (new_data->message_type == MESSAGE_TYPE_DELETE) {
   //   ;
   //} else if (old_data == NULL || old_data->message_type == MESSAGE_TYPE_DELETE) {
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
 *
 * data_merge_tuples_final --
 *
 *      Called for non-MESSAGE_TYPE_INSERT messages when they are determined to be the oldest
 *      message in the system.
 *
 *      Can change data_class or contents.  If necessary, update new_data.
 *
 *-----------------------------------------------------------------------------
 */
static void
test_data_merge_tuples_final(const data_config *cfg,
                             const void *key,       // IN
                             void *oldest_raw_data) // IN/OUT
{
   data_handle *old_data = oldest_raw_data;
   debug_assert(old_data != NULL);

   if (old_data->message_type == MESSAGE_TYPE_UPDATE) {
      old_data->message_type = (old_data->ref_count == 0)
                               ? MESSAGE_TYPE_DELETE
                               : MESSAGE_TYPE_INSERT;
   }
}

/*
 *-----------------------------------------------------------------------------
 *
 * data_class --
 *
 *      Given a data message, returns its message class.
 *
 *-----------------------------------------------------------------------------
 */

static message_type
test_data_message_class(const data_config *cfg,
                        const void *raw_data)
{
   const data_handle *data = raw_data;
   switch(data->message_type) {
      case MESSAGE_TYPE_INSERT:
         return data->ref_count == 0 ? MESSAGE_TYPE_DELETE : MESSAGE_TYPE_INSERT;
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
                        const void *key,
                        char       *str,
                        size_t      len)
{
  snprintf(str, len, "0x%016lx", be64toh(*(uint64 *)key));
}

// FIXME: [yfogel 2020-03-17] need to be passing in the size of the string as
//        well and use snprintf
static void
test_data_message_to_string(const data_config *cfg,
                            const void *raw_data,
                            char       *str_p,
                            size_t      max_len)
{
   const data_handle *data = raw_data;
   snprintf(str_p, max_len, "%d:%d:%lu", data->message_type, data->ref_count,
          *(uint64 *)data->data);
}

static data_config config =
  {
   .key_size = 24,
   .message_size = 24,
   .min_key = { 0 },
   .max_key = { 0xff, 0xff, 0xff, 0xff,   0xff, 0xff, 0xff, 0xff,
                0xff, 0xff, 0xff, 0xff,   0xff, 0xff, 0xff, 0xff,
                0xff, 0xff, 0xff, 0xff,   0xff, 0xff, 0xff, 0xff },
   .key_compare                       = test_data_key_cmp,
   .key_hash                          = platform_hash32,
   .key_to_string                     = test_data_key_to_string,
   .message_to_string                 = test_data_message_to_string,
   .merge_tuples                      = test_data_merge_tuples,
   .merge_tuples_final                = test_data_merge_tuples_final,
   .message_class                     = test_data_message_class,
   .clobber_message_with_range_delete = NULL
  };

data_config *test_data_config = &config;
