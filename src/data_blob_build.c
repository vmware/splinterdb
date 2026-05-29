// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * data_blob_build.c --
 *
 * Functions for building blob-backed messages.
 */

#include "data_blob_build.h"
#include "poison.h"

platform_status
message_to_blob(const blob_build_config *cfg,
                cache                   *cc,
                mini_allocator          *mini,
                message                  msg,
                merge_accumulator       *ma)
{
   platform_assert(!message_is_blob(msg));
   ma->type = message_class(msg);
   ma->cc   = cc;
   platform_status rc =
      blob_build(cfg, cc, mini, message_slice(msg), &ma->data);
   if (SUCCESS(rc)) {
      merge_accumulator_release_blob_ref(ma);
   }
   return rc;
}

platform_status
message_clone(const blob_build_config *cfg,
              cache                   *cc,
              mini_allocator          *mini,
              message                  msg,
              merge_accumulator       *result)
{
   platform_assert(message_is_blob(msg));
   result->type = message_class(msg);
   result->cc   = cc;
   platform_status rc =
      blob_clone(cfg, cc, mini, message_slice(msg), &result->data);
   if (SUCCESS(rc)) {
      merge_accumulator_release_blob_ref(result);
   }
   return rc;
}

platform_status
merge_accumulator_convert_to_blob(const blob_build_config *cfg,
                                  cache                   *cc,
                                  mini_allocator          *mini,
                                  merge_accumulator       *ma)
{
   if (merge_accumulator_isblob(ma)) {
      return STATUS_OK;
   }

   writable_buffer blob;
   writable_buffer_init(&blob, ma->data.heap_id);
   platform_status rc =
      blob_build(cfg, cc, mini, writable_buffer_to_slice(&ma->data), &blob);
   if (!SUCCESS(rc)) {
      writable_buffer_deinit(&blob);
      return rc;
   }

   rc = writable_buffer_copy_slice(&ma->data, writable_buffer_to_slice(&blob));
   writable_buffer_deinit(&blob);
   if (!SUCCESS(rc)) {
      return rc;
   }
   merge_accumulator_release_blob_ref(ma);
   ma->cc = cc;
   return rc;
}
