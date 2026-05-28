// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "data_internal.h"
#include "poison.h"

static void
message_blob_ref_dec(message_blob_ref *blob_ref)
{
   if (!message_blob_ref_is_null(blob_ref)) {
      message_blob_ref ref = *blob_ref;
      *blob_ref            = MESSAGE_BLOB_REF_NULL;
      ref.dec(&ref);
   }
}

message_type
merge_accumulator_message_class(const merge_accumulator *ma)
{
   return ma->type;
}

void *
merge_accumulator_data(const merge_accumulator *ma)
{
   return writable_buffer_data(&ma->data);
}

uint64
merge_accumulator_length(const merge_accumulator *ma)
{
   return writable_buffer_length(&ma->data);
}

slice
merge_accumulator_to_slice(const merge_accumulator *ma)
{
   return writable_buffer_to_slice(&ma->data);
}

void
merge_accumulator_release_blob_ref(merge_accumulator *ma)
{
   message_blob_ref_dec(&ma->blob_ref);
}

/* Copy a message into an already-initialized merge_accumulator. */
_Bool
merge_accumulator_copy_message(merge_accumulator *ma, message msg)
{
   return merge_accumulator_copy_message_with_blob_ref(ma, msg, NULL);
}

_Bool
merge_accumulator_copy_message_with_blob_ref(merge_accumulator       *ma,
                                             message                  msg,
                                             const message_blob_ref *blob_ref)
{
   platform_status rc =
      writable_buffer_copy_slice(&ma->data, message_slice(msg));
   if (!SUCCESS(rc)) {
      return FALSE;
   }
   message_blob_ref old_ref = ma->blob_ref;
   ma->blob_ref             = MESSAGE_BLOB_REF_NULL;

   if (message_isblob(msg) && !message_blob_ref_is_null(blob_ref)) {
      blob_ref->inc(blob_ref);
      ma->blob_ref = *blob_ref;
   }
   message_blob_ref_dec(&old_ref);

   ma->type = message_class(msg);
   ma->cc   = msg.cc;
   return TRUE;
}

_Bool
merge_accumulator_resize(merge_accumulator *ma, uint64 newsize)
{
   platform_status rc = writable_buffer_resize(&ma->data, newsize);
   if (SUCCESS(rc) && newsize == 0) {
      merge_accumulator_release_blob_ref(ma);
      ma->cc = NULL;
   }
   return SUCCESS(rc);
}

void
merge_accumulator_set_class(merge_accumulator *ma, message_type type)
{
   ma->type = type;
   if (type == MESSAGE_TYPE_INVALID || type == MESSAGE_TYPE_DELETE) {
      merge_accumulator_release_blob_ref(ma);
      ma->cc = NULL;
   }
}
