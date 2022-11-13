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

/*
 * KEYS
 *
 * Structures for passing around and manipulating keys in memory.
 * These functions are like those for slices: they do not manage the
 * underlying memory that holds the bytes of they key.
 *
 * We also define two specuial keys, +/-infinity, for specifying
 * iterator ranges and similar applications.
 *
 * The key type is not exposed to the user in any way, since the API
 * does not require them to deal with infinite keys.
 */

typedef enum {
   NEGATIVE_INFINITY = 1,
   USER_KEY          = 2,
   POSITIVE_INFINITY = 3,
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
key_equals(key a, key b)
{
   return a.kind == b.kind
          && IMPLIES(a.kind == USER_KEY,
                     slice_equals(a.user_slice, b.user_slice));
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

/*
 * KEY BUFFERS
 *
 * A key buffer is a piece of managed memory for holding a key.
 * There are two primary uses.  Occasionally, we need to make a local
 * copy of a key (see, e.g. uses of KEY_CREATE_LOCAL_COPY).  And, in
 * the tests, we need to construct keys for inserts, queries, etc.
 */

#define TRUNK_DEFAULT_KEY_BUFFER_SIZE (128)
typedef struct {
   key_type        kind;
   writable_buffer wb;
   char            default_buffer[TRUNK_DEFAULT_KEY_BUFFER_SIZE];
} key_buffer;

/*
 * key_buffer functions
 */

static inline key
key_buffer_key(key_buffer *kb)
{
   if (kb->kind == NEGATIVE_INFINITY) {
      return NEGATIVE_INFINITY_KEY;
   } else if (kb->kind == POSITIVE_INFINITY) {
      return POSITIVE_INFINITY_KEY;
   } else {
      return key_create_from_slice(writable_buffer_to_slice(&kb->wb));
   }
}

static inline key
key_buffer_init_from_key(key_buffer *kb, platform_heap_id hid, key src)
{
   if (key_is_negative_infinity(src)) {
      kb->kind = NEGATIVE_INFINITY;
   } else if (key_is_positive_infinity(src)) {
      kb->kind = POSITIVE_INFINITY;
   } else {
      kb->kind = USER_KEY;
      writable_buffer_init_with_buffer(
         &kb->wb, hid, sizeof(kb->default_buffer), kb->default_buffer, 0);
      writable_buffer_copy_slice(&kb->wb, key_slice(src));
   }
   return key_buffer_key(kb);
}

static inline void
key_buffer_deinit(key_buffer *kb)
{
   if (kb->kind == USER_KEY) {
      writable_buffer_deinit(&kb->wb);
   }
}

#define KEY_CREATE_LOCAL_COPY(dst, hid, src)                                   \
   key_buffer dst##kb;                                                         \
   key        dst = key_buffer_init_from_key(&dst##kb, hid, src)


/*
 * ON-DISK KEY REPRESENTATION
 */
typedef uint16 ondisk_key_length;

#define BLOB_FLAG_BITS                 (1)
#define ONDISK_KEY_LENGTH_BITS         (bitsizeof(ondisk_key_length) - BLOB_FLAG_BITS)
#define ONDISK_KEY_NEGATIVE_INFINITY                                           \
   ((((ondisk_key_length)1) << ONDISK_KEY_LENGTH_BITS) - 1)
#define ONDISK_KEY_POSITIVE_INFINITY                                           \
   ((((ondisk_key_length)1) << ONDISK_KEY_LENGTH_BITS) - 2)

typedef struct ONDISK ondisk_key {
   ondisk_key_length length : ONDISK_KEY_LENGTH_BITS;
   ondisk_key_length isblob : BLOB_FLAG_BITS;
   char              bytes[];
} ondisk_key;

static inline uint64
sizeof_ondisk_key_bytes(const ondisk_key *odk)
{
   if (odk->length == ONDISK_KEY_NEGATIVE_INFINITY
       || odk->length == ONDISK_KEY_POSITIVE_INFINITY)
   {
      return 0;
   } else {
      return odk->length;
   }
}

static inline uint64
ondisk_key_bytes_size(key k)
{
   return key_length(k);
}

static inline key
ondisk_key_to_key(const ondisk_key *odk)
{
   if (odk->length == ONDISK_KEY_NEGATIVE_INFINITY) {
      return NEGATIVE_INFINITY_KEY;
   } else if (odk->length == ONDISK_KEY_POSITIVE_INFINITY) {
      return POSITIVE_INFINITY_KEY;
   } else {
      return key_create(odk->length, odk->bytes);
   }
}

static inline void
copy_key_to_ondisk_key(ondisk_key *odk, key k)
{
   odk->isblob = FALSE;
   if (key_is_user_key(k)) {
      odk->length = key_length(k);
      memcpy(odk->bytes, key_data(k), key_length(k));
   } else if (key_is_negative_infinity(k)) {
      odk->length = ONDISK_KEY_NEGATIVE_INFINITY;
   } else if (key_is_positive_infinity(k)) {
      odk->length = ONDISK_KEY_POSITIVE_INFINITY;
   }
}

/*
 * MESSAGES
 *
 * Note: The message data type is exposed to user callbacks, so it is
 * defined in include/splinterdb/data.h.
 *
 * Convenience functions for dealing with messages.
 */

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

/*
 * MERGE ACCUMULATORS
 *
 * Merge accumulators are basically the message version of a writable buffer.
 */

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

/*
 * USER CALLBACK WRAPPERS
 *
 * These functions handle any special processing needed before calling
 * the user's callback functions.  For example, the user callback
 * functions are not required to deal with infinite keys, so we handle
 * them here.
 */

static inline int
data_key_compare(const data_config *cfg, key key1, key key2)
{
   if (key_is_user_key(key1) && key_is_user_key(key2)) {
      return cfg->key_compare(cfg, key1.user_slice, key2.user_slice);
   } else {
      return (int)key1.kind - (int)key2.kind;
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

static inline void
data_message_to_string(const data_config *cfg,
                       message            msg,
                       char              *str,
                       size_t             size)
{
   cfg->message_to_string(cfg, msg, str, size);
}

#define key_string(cfg, key_to_print)                                          \
   (({                                                                         \
       struct {                                                                \
          char buffer[128];                                                    \
       } b;                                                                    \
       data_key_to_string((cfg), (key_to_print), b.buffer, 128);               \
       b;                                                                      \
    }).buffer)

#define message_string(cfg, msg)                                               \
   (({                                                                         \
       struct {                                                                \
          char buffer[128];                                                    \
       } b;                                                                    \
       data_message_to_string((cfg), (msg), b.buffer, 128);                    \
       b;                                                                      \
    }).buffer)

#endif // __DATA_INTERNAL_H
