// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * data_internal.h --
 *
 *     A slice-based interface to the datatype definitions
 */

#ifndef __DATA_INTERNAL_H
#define __DATA_INTERNAL_H

#include "splinterdb/data.h"
#include "util.h"

/* Writable buffers can be in one of four states:
   - uninitialized
   - null
     - data == NULL
     - allocation_size == length == 0
   - non-null
     - data != NULL
     - length <= allocation_size

   No operation (other than destroy) ever shrinks allocation_size, so
   writable_buffers can only go down the above list, e.g. once a
   writable_buffer is out-of-line, it never becomes inline again.

   writable_buffer_create can create any of the initialized,
   non-user-provided-buffer states, based on the allocation_size
   specified.

   writable_buffer_destroy returns the writable_buffer to the null state.

   Note that the null state is not isolated.  writable_buffer_realloc
   can move a null writable_buffer to the inline or the platform_malloced
   states.  Thus it is possible to, e.g. perform
   writable_buffer_copy_slice on a null writable_buffer.

   Also note that the user-provided state can move to the
   platform-malloced state.
*/
struct writable_buffer {
   void *           original_pointer;
   uint64           original_size;
   platform_heap_id heap_id;
   uint64           allocation_size;
   uint64           length;
   void *           data;
};

static inline bool
writable_buffer_is_null(const writable_buffer *wb)
{
   return wb->data == NULL && wb->length == 0 && wb->allocation_size == 0;
}

static inline void
writable_buffer_init(writable_buffer *wb,
                     platform_heap_id heap_id,
                     uint64           allocation_size,
                     void *           data)
{
   wb->original_pointer = data;
   wb->original_size    = allocation_size;
   wb->heap_id          = heap_id;
   wb->allocation_size  = 0;
   wb->length           = 0;
   wb->data             = NULL;
}

/*
 * Creates a null buffer using the platform memory-allocation system.
 */
static inline platform_status
writable_buffer_create(writable_buffer *wb, platform_heap_id heap_id)
{
   writable_buffer_init(wb, heap_id, 0, NULL);
   return STATUS_OK;
}

static inline void
writable_buffer_reset_to_null(writable_buffer *wb)
{
   if (wb->data && wb->data != wb->original_pointer) {
      platform_realloc(wb->heap_id, wb->data, 0);
   }
   wb->data            = NULL;
   wb->allocation_size = 0;
   wb->length          = 0;
}

static inline platform_status
writable_buffer_copy_slice(writable_buffer *wb, slice src)
{
   if (!writable_buffer_set_length(wb, slice_length(src))) {
      return STATUS_NO_MEMORY;
   }
   memcpy(wb->data, slice_data(src), slice_length(src));
   return STATUS_OK;
}

static inline platform_status
writable_buffer_create_from_slice(writable_buffer *wb,
                                  platform_heap_id heap_id,
                                  slice            contents)
{
   platform_status rc = writable_buffer_create(wb, heap_id);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = writable_buffer_copy_slice(wb, contents);
   debug_assert(SUCCESS(rc));
   return rc;
}

static inline slice
writable_buffer_to_slice(const writable_buffer *wb)
{
   return slice_create(wb->length, wb->data);
}


static inline int
data_key_compare(const data_config *cfg, const slice key1, const slice key2)
{
   return cfg->key_compare(cfg,
                           slice_length(key1),
                           slice_data(key1),
                           slice_length(key2),
                           slice_data(key2));
}

static inline message_type
data_message_class(const data_config *cfg, const slice raw_message)
{
   return cfg->message_class(
      cfg, slice_length(raw_message), slice_data(raw_message));
}

static inline bool
data_merge_tuples(const data_config *cfg,
                  const slice        key,
                  const slice        old_raw_message,
                  writable_buffer *  new_message)
{
   return cfg->merge_tuples(cfg,
                            slice_length(key),
                            slice_data(key),
                            slice_length(old_raw_message),
                            slice_data(old_raw_message),
                            new_message);
}

static inline bool
data_merge_tuples_final(const data_config *cfg,
                        const slice        key,
                        writable_buffer *  oldest_message)
{
   return cfg->merge_tuples_final(
      cfg, slice_length(key), slice_data(key), oldest_message);
}

static inline void
data_key_to_string(const data_config *cfg,
                   const slice        key,
                   char *             str,
                   size_t             size)
{
   cfg->key_to_string(cfg, slice_length(key), slice_data(key), str, size);
}

#define key_string(cfg, key)                                                   \
   (({                                                                         \
       struct {                                                                \
          char buffer[128];                                                    \
       } b;                                                                    \
       data_key_to_string((cfg), (key), b.buffer, 128);                        \
       b;                                                                      \
    }).buffer)

static inline void
data_message_to_string(const data_config *cfg,
                       const slice        message,
                       char *             str,
                       size_t             size)
{
   cfg->message_to_string(
      cfg, slice_length(message), slice_data(message), str, size);
}

#define message_string(cfg, key)                                               \
   (({                                                                         \
       struct {                                                                \
          char buffer[128];                                                    \
       } b;                                                                    \
       data_message_to_string((cfg), (key), b.buffer, 128);                    \
       b;                                                                      \
    }).buffer)

#endif // __DATA_INTERNAL_H
