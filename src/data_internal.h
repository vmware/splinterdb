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

static inline char *
message_type_string(message_type type)
{
   if (type == MESSAGE_TYPE_INSERT) {
      return "insert";
   } else if (type == MESSAGE_TYPE_UPDATE) {
      return "update";
   } else if (type == MESSAGE_TYPE_DELETE) {
      return "delete";
   } else {
      debug_assert(FALSE, "Invalid message type");
      return "invalid";
   }
}

extern message NULL_MESSAGE;
extern message DELETE_MESSAGE;

static inline message
message_create(message_type type, slice data)
{
   return (message){.type = type, .data = data};
}

static inline bool
message_is_null(message m)
{
   return slice_is_null(m.data);
}

static inline int
message_lex_cmp(message a, message b)
{
   if (a.type < b.type)
      return -1;
   else if (b.type < a.type)
      return 1;
   else
      return slice_lex_cmp(a.data, b.data);
}

static inline const char *
message_class_string(message msg)
{
   return message_type_string(msg.type);
}

struct merge_accumulator {
   message_type    type;
   writable_buffer data;
};

static inline void
merge_accumulator_init(merge_accumulator *ma, platform_heap_id heap_id)
{
   writable_buffer_init(&ma->data, heap_id);
   ma->type = MESSAGE_TYPE_INVALID;
}

static inline void
merge_accumulator_init_with_buffer(merge_accumulator *ma,
                                   platform_heap_id   heap_id,
                                   uint64             allocation_size,
                                   void              *data,
                                   uint64             logical_size,
                                   message_type       type)

{
   writable_buffer_init_with_buffer(
      &ma->data, heap_id, allocation_size, data, logical_size);
   ma->type = type;
}

static inline void
merge_accumulator_deinit(merge_accumulator *ma)
{
   writable_buffer_deinit(&ma->data);
   ma->type = MESSAGE_TYPE_INVALID;
}

static inline message
merge_accumulator_to_message(const merge_accumulator *ma)
{
   return message_create(ma->type, writable_buffer_to_slice(&ma->data));
}

static inline slice
merge_accumulator_to_slice(const merge_accumulator *ma)
{
   return writable_buffer_to_slice(&ma->data);
}

static inline slice
merge_accumulator_to_value(const merge_accumulator *ma)
{
   debug_assert(ma->type == MESSAGE_TYPE_INSERT);
   return writable_buffer_to_slice(&ma->data);
}

static inline platform_status
merge_accumulator_copy_message(merge_accumulator *ma, message msg)
{
   ma->type = message_class(msg);
   return writable_buffer_copy_slice(&ma->data, message_slice(msg));
}

static inline platform_status
merge_accumulator_init_from_message(merge_accumulator *ma,
                                    platform_heap_id   heap_id,
                                    message            msg)
{
   merge_accumulator_init(ma, heap_id);
   return merge_accumulator_copy_message(ma, msg);
}

static inline void
merge_accumulator_set_to_null(merge_accumulator *ma)
{
   ma->type = MESSAGE_TYPE_INVALID;
   writable_buffer_set_to_null(&ma->data);
}

static inline bool
merge_accumulator_resize(merge_accumulator *ma, uint64 newsize)
{
   return writable_buffer_resize(&ma->data, newsize);
}

static inline void
merge_accumulator_set_class(merge_accumulator *ma, message_type type)
{
   ma->type = type;
}

static inline message_type
merge_accumulator_message_class(const merge_accumulator *ma)
{
   return ma->type;
}

static inline bool
merge_accumulator_is_null(const merge_accumulator *ma)
{
   return writable_buffer_is_null(&ma->data);
}

static inline void *
merge_accumulator_data(const merge_accumulator *ma)
{
   return writable_buffer_data(&ma->data);
}

static inline uint64
merge_accumulator_length(const merge_accumulator *ma)
{
   return writable_buffer_length(&ma->data);
}

static inline int
data_key_compare(const data_config *cfg, slice key1, slice key2)
{
   return cfg->key_compare(cfg, key1, key2);
}

static inline int
data_merge_tuples(const data_config *cfg,
                  slice              key,
                  message            old_raw_message,
                  merge_accumulator *new_message)
{
   message_type newclass = merge_accumulator_message_class(new_message);
   if (newclass != MESSAGE_TYPE_UPDATE) {
      return 0;
   }

   message_type oldclass = message_class(old_raw_message);
   if (oldclass == MESSAGE_TYPE_DELETE) {
      return cfg->merge_tuples_final(cfg, key, new_message);
   }

   // newclass is UPDATE and oldclass is INSERT or UPDATE
   return cfg->merge_tuples(cfg, key, old_raw_message, new_message);
}

static inline int
data_merge_tuples_final(const data_config *cfg,
                        slice              key,
                        merge_accumulator *oldest_message)
{
   message_type class = merge_accumulator_message_class(oldest_message);
   if (class != MESSAGE_TYPE_UPDATE) {
      return 0;
   }
   return cfg->merge_tuples_final(cfg, key, oldest_message);
}

static inline void
data_key_to_string(const data_config *cfg, slice key, char *str, size_t size)
{
   cfg->key_to_string(cfg, key, str, size);
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
                       message            msg,
                       char              *str,
                       size_t             size)
{
   cfg->message_to_string(cfg, msg, str, size);
}

#define message_string(cfg, msg)                                               \
   (({                                                                         \
       struct {                                                                \
          char buffer[128];                                                    \
       } b;                                                                    \
       data_message_to_string((cfg), (msg), b.buffer, 128);                    \
       b;                                                                      \
    }).buffer)

#endif // __DATA_INTERNAL_H
