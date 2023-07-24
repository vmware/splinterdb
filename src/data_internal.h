// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * data_internal.h --
 *
 *     A slice-based interface to the datatype definitions
 */

#pragma once

#include "splinterdb/data.h"
#include "util.h"

/*
 * KEYS
 *
 * Structures for passing around and manipulating keys in memory.
 * These functions are like those for slices: they do not manage the
 * underlying memory that holds the bytes of they key.  So they are
 * essentially just pointers that carry around a little metadata.
 *
 * We also define two special keys, +/-infinity, for specifying
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

static inline bool32
key_is_negative_infinity(key k)
{
   return k.kind == NEGATIVE_INFINITY;
}

static inline bool32
key_is_positive_infinity(key k)
{
   return k.kind == POSITIVE_INFINITY;
}

static inline bool32
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

static inline bool32
key_equals(key a, key b)
{
   return a.kind == b.kind
          && IMPLIES(a.kind == USER_KEY,
                     slice_equals(a.user_slice, b.user_slice));
}

static inline bool32
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

#define DEFAULT_KEY_BUFFER_SIZE (128)
typedef struct {
   key_type        kind;
   writable_buffer wb;
   char            default_buffer[DEFAULT_KEY_BUFFER_SIZE];
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

static inline void
key_buffer_init(key_buffer *kb, platform_heap_id hid)
{
   kb->kind = USER_KEY;
   writable_buffer_init_with_buffer(
      &kb->wb, hid, sizeof(kb->default_buffer), kb->default_buffer, 0);
}

static inline platform_status
key_buffer_copy_slice(key_buffer *kb, slice src)
{
   kb->kind = USER_KEY;
   return writable_buffer_copy_slice(&kb->wb, src);
}

static inline platform_status
key_buffer_copy_key(key_buffer *kb, key src)
{
   if (key_is_negative_infinity(src)) {
      kb->kind = NEGATIVE_INFINITY;
   } else if (key_is_positive_infinity(src)) {
      kb->kind = POSITIVE_INFINITY;
   } else {
      return key_buffer_copy_slice(kb, key_slice(src));
   }
   return STATUS_OK;
}

static inline platform_status
key_buffer_init_from_key(key_buffer *kb, platform_heap_id hid, key src)
{
   key_buffer_init(kb, hid);
   return key_buffer_copy_key(kb, src);
}

/* Converts kb to a user key if it isn't already. */
static inline platform_status
key_buffer_resize(key_buffer *kb, uint64 length)
{
   kb->kind = USER_KEY;
   return writable_buffer_resize(&kb->wb, length);
}

static inline void *
key_buffer_data(key_buffer *kb)
{
   debug_assert(kb->kind == USER_KEY);
   return writable_buffer_data(&kb->wb);
}

static inline void
key_buffer_deinit(key_buffer *kb)
{
   writable_buffer_deinit(&kb->wb);
}

#define DECLARE_AUTO_KEY_BUFFER(kb, hid)                                       \
   key_buffer kb __attribute__((cleanup(key_buffer_deinit)));                  \
   key_buffer_init(&kb, hid)

/* Success will be stored in rc, which must already be declared. */
#define KEY_CREATE_LOCAL_COPY(rc, dst, hid, src)                               \
   DECLARE_AUTO_KEY_BUFFER(dst##kb, hid);                                      \
   rc      = key_buffer_copy_key(&dst##kb, src);                               \
   key dst = key_buffer_key(&dst##kb)


/*
 * ON-DISK KEY REPRESENTATION
 */
typedef uint16 ondisk_key_length;
typedef uint8  ondisk_flags;

typedef struct ONDISK ondisk_key {
   ondisk_key_length length;
   ondisk_flags      flags;
   char              bytes[];
} ondisk_key;

#define ONDISK_KEY_LENGTH_BITS (bitsizeof(ondisk_key_length))
#define ONDISK_KEY_NEGATIVE_INFINITY                                           \
   ((((ondisk_key_length)1) << ONDISK_KEY_LENGTH_BITS) - 1)
#define ONDISK_KEY_POSITIVE_INFINITY                                           \
   ((((ondisk_key_length)1) << ONDISK_KEY_LENGTH_BITS) - 2)

#define ONDISK_KEY_DEFAULT_FLAGS (0)

/* How big is the flexible array field of the given ondisk_key */
static inline uint64
sizeof_ondisk_key_data(const ondisk_key *odk)
{
   if (odk->length == ONDISK_KEY_NEGATIVE_INFINITY
       || odk->length == ONDISK_KEY_POSITIVE_INFINITY)
   {
      return 0;
   } else {
      return odk->length;
   }
}

/*
 * How much space would be required for the bytes field of an ondisk_key
 * holding k.
 */
static inline uint64
ondisk_key_required_data_capacity(key k)
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
   odk->flags = ONDISK_KEY_DEFAULT_FLAGS;
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

static inline bool32
message_is_null(message msg)
{
   bool32 r = slice_is_null(msg.data);
   debug_assert(IMPLIES(r, msg.type == MESSAGE_TYPE_INVALID));
   return r;
}

static inline bool32
message_is_definitive(message msg)
{
   return msg.type == MESSAGE_TYPE_INSERT || msg.type == MESSAGE_TYPE_DELETE;
}

static inline bool32
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
 * ON-DISK TUPLE REPRESENTATION
 *
 * The key and message data are laid out abutting each other. This structure
 * describes that layout in terms of the length of the key-portion and the
 * message-portion, following which appears the concatenated [<key>, <message>]
 * datum.
 *
 * Tuple keys cannot be infinite.
 */

typedef uint16 ondisk_message_length;

typedef struct ONDISK ondisk_tuple {
   ondisk_key_length     key_length;
   ondisk_flags          key_flags;
   ondisk_message_length message_length;
   ondisk_flags          flags;
   char                  key_and_message[];
} ondisk_tuple;

#define ONDISK_MESSAGE_TYPE_BITS (2)
_Static_assert(MESSAGE_TYPE_MAX_VALID_USER_TYPE
                  < (1ULL << ONDISK_MESSAGE_TYPE_BITS),
               "ONDISK_MESSAGE_TYPE_BITS is too small");
#define ONDISK_MESSAGE_TYPE_MASK ((0x1 << ONDISK_MESSAGE_TYPE_BITS) - 1)

/* Size of the data part of an existing ondisk_tuple */
static inline uint64
sizeof_ondisk_tuple_data(const ondisk_tuple *odt)
{
   return odt->key_length + odt->message_length;
}

/*
 * Amount of space required to hold the data part of an ondisk_tuple
 * containing k and msg
 */
static inline uint64
ondisk_tuple_required_data_capacity(key k, message msg)
{
   debug_assert(key_is_user_key(k));
   return key_length(k) + message_length(msg);
}

static inline key
ondisk_tuple_key(const ondisk_tuple *odt)
{
   return key_create(odt->key_length, odt->key_and_message);
}

static inline message_type
ondisk_tuple_message_class(const ondisk_tuple *odt)
{
   return odt->flags & ONDISK_MESSAGE_TYPE_MASK;
}

static inline message
ondisk_tuple_message(const ondisk_tuple *odt)
{
   return message_create(ondisk_tuple_message_class(odt),
                         slice_create(odt->message_length,
                                      odt->key_and_message + odt->key_length));
}

static inline void
copy_message_to_ondisk_tuple(ondisk_tuple *odt, message msg)
{
   odt->message_length = message_length(msg);
   odt->flags          = message_class(msg);
   memcpy(odt->key_and_message + odt->key_length,
          message_data(msg),
          message_length(msg));
}

static inline void
copy_tuple_to_ondisk_tuple(ondisk_tuple *odt, key k, message msg)
{
   debug_assert(key_is_user_key(k));
   odt->key_length = key_length(k);
   odt->flags      = ONDISK_KEY_DEFAULT_FLAGS;
   memcpy(odt->key_and_message, key_data(k), key_length(k));
   copy_message_to_ondisk_tuple(odt, msg);
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

static inline bool32
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
static inline bool32
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

static inline bool32
merge_accumulator_is_null(const merge_accumulator *ma)
{
   bool32 r = writable_buffer_is_null(&ma->data);
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
      snprintf(str, size, "(negative_infinity)");
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
