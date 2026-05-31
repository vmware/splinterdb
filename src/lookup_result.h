// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/splinterdb.h"
#include "data_internal.h"
#include "ondisk_ref.h"
#include "platform_assert.h"

typedef struct lookup_result {
   const data_config      *data_cfg;
   splinterdb_lookup_flags flags;
   merge_accumulator       value;
   ondisk_ref              blob_ref;
} lookup_result;

_Static_assert(sizeof(lookup_result) <= sizeof(splinterdb_lookup_result),
               "sizeof(splinterdb_lookup_result) is too small");

_Static_assert(__alignof__(splinterdb_lookup_result)
                  == __alignof__(lookup_result),
               "mismatched alignment for splinterdb_lookup_result");

static inline lookup_result *
lookup_result_from_splinterdb(splinterdb_lookup_result *result)
{
   return (lookup_result *)result;
}

static inline const lookup_result *
lookup_result_from_const_splinterdb(const splinterdb_lookup_result *result)
{
   return (const lookup_result *)result;
}

static inline void
lookup_result_init(lookup_result          *result,
                   const data_config      *data_cfg,
                   splinterdb_lookup_flags flags,
                   uint64                  buffer_len,
                   char                   *buffer)
{
   platform_assert((flags & ~SPLINTERDB_LOOKUP_MIGHT_EXIST) == 0);

   result->data_cfg = data_cfg;
   result->flags    = flags;

   merge_accumulator_init_with_buffer(&result->value,
                                      PROCESS_PRIVATE_HEAP_ID,
                                      buffer_len,
                                      buffer,
                                      WRITABLE_BUFFER_NULL_LENGTH,
                                      MESSAGE_TYPE_INVALID);
   ondisk_ref_init_null(&result->blob_ref);
}

static inline void
lookup_result_set_data_config(lookup_result     *result,
                              const data_config *data_cfg)
{
   result->data_cfg = data_cfg;
}

static inline void
lookup_result_deinit(lookup_result *result)
{
   ondisk_ref_deinit(&result->blob_ref);
   merge_accumulator_deinit(&result->value);
}

static inline bool32
lookup_result_is_existence(const lookup_result *result)
{
   return (result->flags & SPLINTERDB_LOOKUP_MIGHT_EXIST) != 0;
}

static inline void
lookup_result_reset(lookup_result *result)
{
   ondisk_ref_deinit(&result->blob_ref);
   merge_accumulator_set_to_null(&result->value);
}

static inline merge_accumulator *
lookup_result_accumulator(lookup_result *result)
{
   return &result->value;
}

static inline const merge_accumulator *
lookup_result_const_accumulator(const lookup_result *result)
{
   return &result->value;
}

static inline bool32
lookup_result_found(const lookup_result *result)
{
   message_type type = merge_accumulator_message_class(&result->value);
   if (lookup_result_is_existence(result)) {
      return type != MESSAGE_TYPE_INVALID && type != MESSAGE_TYPE_DELETE;
   }
   return type != MESSAGE_TYPE_INVALID;
}

static inline platform_status
lookup_result_update(lookup_result    *result,
                     key               found_key,
                     message           msg,
                     const ondisk_ref *msg_blob_ref)
{
   if (lookup_result_is_existence(result)) {
      platform_assert(merge_accumulator_length(&result->value) == 0);
      platform_assert(ondisk_ref_is_null(&result->blob_ref));
      merge_accumulator_set_class(&result->value, message_class(msg));
      return STATUS_OK;
   }

   if (merge_accumulator_is_null(&result->value)) {
      debug_assert(!message_is_blob(msg) || !ondisk_ref_is_null(msg_blob_ref));
      bool32 success = merge_accumulator_copy_message(&result->value, msg);
      if (!success) {
         return STATUS_NO_MEMORY;
      }
      ondisk_ref_replace(&result->blob_ref,
                         message_is_blob(msg) ? msg_blob_ref : NULL);
      return STATUS_OK;
   }

   platform_assert(result->data_cfg != NULL);
   int rc = data_merge_tuples(result->data_cfg, found_key, msg, &result->value);
   if (!merge_accumulator_is_blob(&result->value)) {
      ondisk_ref_deinit(&result->blob_ref);
   }
   return rc == 0 ? STATUS_OK : STATUS_NO_MEMORY;
}

static inline platform_status
lookup_result_ensure_materialized(lookup_result *result)
{
   platform_status rc = merge_accumulator_ensure_materialized(&result->value);
   if (SUCCESS(rc)) {
      ondisk_ref_deinit(&result->blob_ref);
   }
   return rc;
}

static inline bool32
lookup_result_should_continue(const lookup_result *result)
{
   if (lookup_result_is_existence(result)) {
      return merge_accumulator_message_class(&result->value)
             == MESSAGE_TYPE_INVALID;
   }
   return !merge_accumulator_is_definitive(&result->value);
}

static inline bool32
lookup_result_note_filter_hit(lookup_result *result)
{
   if (lookup_result_is_existence(result)) {
      bool32 success = merge_accumulator_resize(&result->value, 0);
      platform_assert(success);
      ondisk_ref_deinit(&result->blob_ref);
      merge_accumulator_set_class(&result->value, MESSAGE_TYPE_UPDATE);
   }
   return lookup_result_should_continue(result);
}

static inline void
lookup_result_finalize(lookup_result *result, key query_key)
{
   if (!lookup_result_is_existence(result)
       && !merge_accumulator_is_null(&result->value)
       && !merge_accumulator_is_definitive(&result->value))
   {
      platform_assert(result->data_cfg != NULL);
      data_merge_tuples_final(result->data_cfg, query_key, &result->value);
      if (!merge_accumulator_is_blob(&result->value)) {
         ondisk_ref_deinit(&result->blob_ref);
      }
   }

   if (!lookup_result_is_existence(result)
       && !merge_accumulator_is_null(&result->value)
       && merge_accumulator_message_class(&result->value)
             == MESSAGE_TYPE_DELETE)
   {
      ondisk_ref_deinit(&result->blob_ref);
      merge_accumulator_set_to_null(&result->value);
   }
}
