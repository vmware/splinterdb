
#include "data_internal.h"

static bool
writable_buffer_ensure_space(writable_buffer *wb, uint64 minspace)
{
   if (minspace <= wb->allocation_size) {
      return TRUE;
   }

   if (minspace <= wb->original_size) {
      wb->data            = wb->original_pointer;
      wb->allocation_size = wb->original_size;
      return TRUE;
   }

   void *oldptr  = wb->data == wb->original_pointer ? NULL : wb->data;
   void *newdata = platform_realloc(wb->heap_id, oldptr, minspace);
   if (newdata == NULL) {
      return FALSE;
   }

   if (wb->data == wb->original_pointer) {
      memcpy(newdata, wb->data, wb->length);
   }

   wb->allocation_size = minspace;
   wb->data            = newdata;
   return TRUE;
}

uint64
writable_buffer_length(writable_buffer *wb)
{
   return wb->length;
}

bool
writable_buffer_set_length(writable_buffer *wb, uint64 newlength)
{
   if (!writable_buffer_ensure_space(wb, newlength)) {
      return FALSE;
   }
   wb->length = newlength;
   return TRUE;
}

void *
writable_buffer_data(writable_buffer *wb)
{
   return wb->data;
}
