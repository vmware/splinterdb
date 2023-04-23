#include "data_internal.h"

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

/* Copy a message into an already-initialized merge_accumulator. */
_Bool
merge_accumulator_copy_message(merge_accumulator *ma, message msg)
{
   platform_status rc =
      writable_buffer_copy_slice(&ma->data, message_slice(msg));
   if (!SUCCESS(rc)) {
      return FALSE;
   }
   ma->type = message_class(msg);
   return TRUE;
}

_Bool
merge_accumulator_resize(merge_accumulator *ma, uint64 newsize)
{
   platform_status rc = writable_buffer_resize(&ma->data, newsize);
   return SUCCESS(rc);
}

void
merge_accumulator_set_class(merge_accumulator *ma, message_type type)
{
   ma->type = type;
}
