// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * data_blob_build.c --
 *
 * Functions for building blobs from key and message types
 */

#include "data_blob_build.h"

platform_status
message_to_blob(const blob_build_config *cfg,
                cache                   *cc,
                mini_allocator          *mini,
                key                      tuple_key,
                message                  msg,
                merge_accumulator       *ma)
{
   platform_assert(!message_isblob(msg));
   ma->type = message_class(msg);
   ma->cc   = cc;
   return blob_build(cfg, cc, mini, tuple_key, message_slice(msg), &ma->data);
}

platform_status
message_clone(const blob_build_config *cfg,
              cache                   *cc,
              mini_allocator          *mini,
              key                      tuple_key,
              message                  msg,
              merge_accumulator       *result)
{
   debug_assert(message_isblob(msg));
   result->type = message_class(msg);
   result->cc   = cc;
   return blob_clone(
      cfg, cc, mini, tuple_key, message_slice(msg), &result->data);
}

platform_status
merge_accumulator_convert_to_blob(const blob_build_config *cfg,
                                  cache                   *cc,
                                  mini_allocator          *mini,
                                  key                      tuple_key,
                                  merge_accumulator       *ma)
{
   if (ma->cc) {
      return STATUS_OK;
   }

   platform_status rc;
   writable_buffer blob;
   writable_buffer_init(&blob, ma->data.heap_id);
   slice data = writable_buffer_to_slice(&ma->data);
   rc         = blob_build(cfg, cc, mini, tuple_key, data, &blob);
   if (!SUCCESS(rc)) {
      writable_buffer_deinit(&blob);
      return rc;
   }
   rc = writable_buffer_copy_slice(&ma->data, writable_buffer_to_slice(&blob));
   // Should never fail since blob should be strictly smaller than original data
   writable_buffer_deinit(&blob);
   debug_assert(SUCCESS(rc));
   ma->cc = cc;
   return STATUS_OK;
}
