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
#include "cache.h"
#include "util.h"
#include "blob.h"

static inline char *
message_type_string(message_type type)
{
   switch (type) {
      case MESSAGE_TYPE_INSERT:
         return "insert";
      case MESSAGE_TYPE_UPDATE:
         return "update";
      case MESSAGE_TYPE_DELETE:
         return "delete";
      case MESSAGE_TYPE_PIVOT_DATA:
         return "pivot_data";
      case MESSAGE_TYPE_INVALID:
      default:
         debug_assert(FALSE, "Invalid message type=%d", type);
         return "invalid";
   }
}

extern message NULL_MESSAGE;
extern message DELETE_MESSAGE;

static inline message
message_create(message_type type, bool isblob, slice data)
{
   return (message){.type = type, .isblob = isblob, .data = data};
}

static inline bool
message_is_null(message msg)
{
   bool r = slice_is_null(msg.data);
   debug_assert(IMPLIES(r, msg.type == MESSAGE_TYPE_INVALID));
   return r;
}

static inline bool
message_is_definitive(message msg)
{
   return msg.type == MESSAGE_TYPE_INSERT || msg.type == MESSAGE_TYPE_DELETE;
}

static inline bool
message_is_invalid_user_type(message msg)
{
   return msg.type == MESSAGE_TYPE_INVALID
          || msg.type > MESSAGE_TYPE_MAX_VALID_USER_TYPE;
}

static inline void
message_materialize_if_needed(cache           *cc,
                              platform_heap_id heap_id,
                              message          msg,
                              writable_buffer *tmp,
                              message         *result)
{
   if (msg.isblob) {
      writable_buffer_init(tmp, heap_id);
      slice sblob = message_slice(msg);
      blob_materialize(cc, sblob, 0, blob_length(sblob), PAGE_TYPE_MISC, tmp);
      *result = message_create(
         message_class(msg), FALSE, writable_buffer_to_slice(tmp));

   } else {
      *result = msg;
   }
}

static inline void
message_dematerialize_if_needed(message msg, writable_buffer *tmp)
{
   if (msg.isblob) {
      writable_buffer_deinit(tmp);
   }
}

/* Define an arbitrary ordering on messages.  In practice, all we care
 * about is equality, but this is written to follow the same
 * comparison interface as for ordered types. */
static inline int
message_lex_cmp(cache *cc, message a, message b)
{
   if (a.type < b.type) {
      return -1;
   } else if (b.type < a.type) {
      return 1;
   } else {
      writable_buffer a_tmp;
      message         a_materialized;
      writable_buffer b_tmp;
      message         b_materialized;
      message_materialize_if_needed(cc, NULL, a, &a_tmp, &a_materialized);
      message_materialize_if_needed(cc, NULL, b, &b_tmp, &b_materialized);
      int result = slice_lex_cmp(a_materialized.data, b_materialized.data);
      message_dematerialize_if_needed(a, &a_tmp);
      message_dematerialize_if_needed(b, &b_tmp);
      return result;
   }
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

static inline bool
merge_accumulator_is_definitive(const merge_accumulator *ma)
{
   return ma->type == MESSAGE_TYPE_INSERT || ma->type == MESSAGE_TYPE_DELETE;
}

static inline message
merge_accumulator_to_message(const merge_accumulator *ma)
{
   return message_create(ma->type, FALSE, writable_buffer_to_slice(&ma->data));
}

static inline slice
merge_accumulator_to_value(const merge_accumulator *ma)
{
   debug_assert(ma->type == MESSAGE_TYPE_INSERT);
   return writable_buffer_to_slice(&ma->data);
}

/* Initialize an uninitialized merge_accumulator and copy msg into it. */
static inline bool
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
merge_accumulator_is_null(const merge_accumulator *ma)
{
   bool r = writable_buffer_is_null(&ma->data);
   debug_assert(IMPLIES(r, ma->type == MESSAGE_TYPE_INVALID));
   return r;
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
   if (merge_accumulator_is_definitive(new_message)) {
      return 0;
   }

   message_type oldclass = message_class(old_raw_message);
   if (oldclass == MESSAGE_TYPE_DELETE) {
      return cfg->merge_tuples_final(cfg, key, new_message);
   }

   // new class is UPDATE and old class is INSERT or UPDATE
   int result = cfg->merge_tuples(cfg, key, old_raw_message, new_message);
   if (result
       && merge_accumulator_message_class(new_message) == MESSAGE_TYPE_DELETE)
   {
      merge_accumulator_resize(new_message, 0);
   }
   return result;
}

static inline int
data_merge_tuples_final(const data_config *cfg,
                        slice              key,
                        merge_accumulator *oldest_message)
{
   if (merge_accumulator_is_definitive(oldest_message)) {
      return 0;
   }
   int result = cfg->merge_tuples_final(cfg, key, oldest_message);
   if (result
       && merge_accumulator_message_class(oldest_message)
             == MESSAGE_TYPE_DELETE)
   {
      merge_accumulator_resize(oldest_message, 0);
   }
   return result;
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
                       cache             *cc,
                       message            msg,
                       char              *str,
                       size_t             size)
{
   writable_buffer tmp;
   message         materialized;
   message_materialize_if_needed(cc, NULL, msg, &tmp, &materialized);
   cfg->message_to_string(cfg, materialized, str, size);
   message_dematerialize_if_needed(msg, &tmp);
}

#define message_string(cfg, cc, msg)                                           \
   (({                                                                         \
       struct {                                                                \
          char buffer[128];                                                    \
       } b;                                                                    \
       data_message_to_string((cfg), (cc), (msg), b.buffer, 128);              \
       b;                                                                      \
    }).buffer)

#endif // __DATA_INTERNAL_H
