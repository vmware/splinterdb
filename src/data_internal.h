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

#define NULL_MESSAGE                                                           \
   ((message){.type = MESSAGE_TYPE_INVALID, .data = NULL_SLICE})
#define DELETE_MESSAGE                                                         \
   ((message){.type = MESSAGE_TYPE_DELETE, .data = NULL_SLICE})

static inline message
message_create(message_type type, slice data)
{
   return (message){.type = type, .data = data};
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

/* Define an arbitrary ordering on messages.  In practice, all we care
 * about is equality, but this is written to follow the same
 * comparison interface as for ordered types. */
static inline int
message_lex_cmp(message a, message b)
{
   if (a.type < b.type) {
      return -1;
   } else if (b.type < a.type) {
      return 1;
   } else {
      return slice_lex_cmp(a.data, b.data);
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
   return message_create(ma->type, writable_buffer_to_slice(&ma->data));
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

typedef enum {
   POSITIVE_INFINITY = 1,
   USER_KEY          = 2,
   NEGATIVE_INFINITY = 3
} key_type;

typedef struct key {
   key_type kind;
   slice    user_slice;
} key;

#define NEGATIVE_INFINITY_KEY                                                  \
   ((key){.kind = NEGATIVE_INFINITY, .user_slice = INVALID_SLICE})
#define POSITIVE_INFINITY_KEY                                                  \
   ((key){.kind = POSITIVE_INFINITY, .user_slice = INVALID_SLICE})
#define NULL_KEY ((key){.kind = USER_KEY, .user_slice = NULL_SLICE})

static inline bool
key_is_negative_infinity(key k)
{
   return k.kind == NEGATIVE_INFINITY;
}

static inline bool
key_is_positive_infinity(key k)
{
   return k.kind == POSITIVE_INFINITY;
}

static inline bool
key_is_user_key(key k)
{
   return k.kind == USER_KEY;
}

static inline slice
key_slice(key k)
{
   debug_assert(k.kind == USER_KEY);
   return k.user_slice;
}

static inline key
key_create_from_slice(slice user_slice)
{
   return (key){.kind = USER_KEY, .user_slice = user_slice};
}

static inline key
key_create(uint64 length, const void *data)
{
   return (key){.kind = USER_KEY, .user_slice = slice_create(length, data)};
}

static inline bool
keys_equal(key a, key b)
{
   return a.kind == b.kind
          && IMPLIES(a.kind == USER_KEY,
                     slices_equal(a.user_slice, b.user_slice));
}

static inline bool
key_is_null(key k)
{
   return k.kind == USER_KEY && slice_is_null(k.user_slice);
}

static inline uint64
key_length(key k)
{
   return k.kind == USER_KEY ? slice_length(k.user_slice) : 0;
}

static inline const void *
key_data(key k)
{
   debug_assert(k.kind == USER_KEY);
   return slice_data(k.user_slice);
}

static inline void
key_copy_contents(void *dst, key k)
{
   debug_assert(k.kind == USER_KEY);
   slice_copy_contents(dst, k.user_slice);
}

static inline key
data_min_key(const data_config *cfg)
{
   return NEGATIVE_INFINITY_KEY;
}

static inline key
data_max_key(const data_config *cfg)
{
   return POSITIVE_INFINITY_KEY;
}

static inline int
data_key_compare(const data_config *cfg, key key1, key key2)
{
   if (key_is_negative_infinity(key1)) {
      return key_is_negative_infinity(key2) ? 0 : -1;
   } else if (key_is_positive_infinity(key1)) {
      return key_is_positive_infinity(key2) ? 0 : 1;
   } else if (key_is_negative_infinity(key2)) {
      return 1;
   } else if (key_is_positive_infinity(key2)) {
      return -1;
   } else {
      return cfg->key_compare(cfg, key1.user_slice, key2.user_slice);
   }
}

static inline int
data_merge_tuples(const data_config *cfg,
                  key                tuple_key,
                  message            old_raw_message,
                  merge_accumulator *new_message)
{
   debug_assert(key_is_user_key(tuple_key));

   if (merge_accumulator_is_definitive(new_message)) {
      return 0;
   }

   message_type oldclass = message_class(old_raw_message);
   if (oldclass == MESSAGE_TYPE_DELETE) {
      return cfg->merge_tuples_final(cfg, tuple_key.user_slice, new_message);
   }

   // new class is UPDATE and old class is INSERT or UPDATE
   int result = cfg->merge_tuples(
      cfg, tuple_key.user_slice, old_raw_message, new_message);
   if (result
       && merge_accumulator_message_class(new_message) == MESSAGE_TYPE_DELETE)
   {
      merge_accumulator_resize(new_message, 0);
   }
   return result;
}

static inline int
data_merge_tuples_final(const data_config *cfg,
                        key                tuple_key,
                        merge_accumulator *oldest_message)
{
   debug_assert(key_is_user_key(tuple_key));

   if (merge_accumulator_is_definitive(oldest_message)) {
      return 0;
   }
   int result =
      cfg->merge_tuples_final(cfg, tuple_key.user_slice, oldest_message);
   if (result
       && merge_accumulator_message_class(oldest_message)
             == MESSAGE_TYPE_DELETE)
   {
      merge_accumulator_resize(oldest_message, 0);
   }
   return result;
}

static inline void
data_key_to_string(const data_config *cfg, key k, char *str, size_t size)
{
   if (key_is_negative_infinity(k)) {
      snprintf(str, size, "(negaitive_infinity)");
   } else if (key_is_negative_infinity(k)) {
      snprintf(str, size, "(positive_infinity)");
   } else {
      cfg->key_to_string(cfg, k.user_slice, str, size);
   }
}

#define key_string(cfg, key_to_print)                                          \
   (({                                                                         \
       struct {                                                                \
          char buffer[128];                                                    \
       } b;                                                                    \
       data_key_to_string((cfg), (key_to_print), b.buffer, 128);               \
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
