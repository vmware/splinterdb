// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * data.h --
 *
 * SplinterDB allows the application to specify the encoding of keys and
 * values in order to support features like a custom sort order on keys and
 * blind-write mutations.
 *
 * Internally, SplinterDB operates on keys and _messages_, where a message
 * encodes a verb like "insert with overwrite", "delete" or "update/mutate".
 *
 * To initialize a SplinterDB instance, an application must provide a
 * data_config (defined below).  Either the application provides a custom one
 * or it may use the default_data_config.
 *
 */

#ifndef __DATA_H
#define __DATA_H

#include "splinterdb/limits.h"
#include "splinterdb/platform_public.h"
#include "splinterdb/public_util.h"

typedef enum message_type {
   MESSAGE_TYPE_INSERT,
   MESSAGE_TYPE_UPDATE,
   MESSAGE_TYPE_DELETE,
   MESSAGE_TYPE_INVALID,
} message_type;

typedef struct data_config data_config;

typedef int (*key_compare_fn)(const data_config *cfg,
                              uint64             key1_len,
                              const void        *key1,
                              uint64             key2_len,
                              const void        *key2);

typedef uint32 (*key_hash_fn)(const void *input, size_t length, uint32 seed);

typedef message_type (*message_class_fn)(const data_config *cfg,
                                         uint64             raw_message_len,
                                         const void        *raw_message);

// Given two messages, old_message and new_message, merge them
// and return the result in new_raw_message.
//
// Returns 0 on success.  non-zero indicates an internal error.
typedef int (*merge_tuple_fn)(const data_config *cfg,
                              uint64             key_len,
                              const void        *key,
                              uint64             old_message_len,
                              const void        *old_message,
                              writable_buffer   *new_message); // IN/OUT

// Called for non-MESSAGE_TYPE_INSERT messages when they are
// determined to be the oldest message Can change data_class or
// contents.  If necessary, update new_data.
//
// Returns 0 on success.  non-zero indicates an internal error.
typedef int (*merge_tuple_final_fn)(const data_config *cfg,
                                    uint64             key_len,
                                    const void        *key,
                                    writable_buffer   *oldest_message);

typedef void (*key_or_message_to_str_fn)(const data_config *cfg,
                                         uint64             key_or_message_len,
                                         const void        *key_or_message,
                                         char              *str,
                                         uint64             max_len);


// Encodes a message from a type and (optionally) a value
// Returns the length of the fully-encoded message via out_encoded_len
// Is allowed to fail by returning a non-zero exit code
typedef int (*encode_message_fn)(message_type type,
                                 uint64       value_len,
                                 const void  *value,
                                 uint64       dst_msg_buffer_len,
                                 void        *dst_msg_buffer,
                                 uint64      *out_encoded_len);

// Extract the value from a message
// This shouldn't need to do any allocation or copying, just pointer arithmetic
// Is allowed to fail by returning a non-zero exit code
typedef int (*decode_message_fn)(uint64       msg_buffer_len,
                                 const void  *msg_buffer,
                                 uint64      *out_value_len,
                                 const char **out_value);

/*
 * data_config: This structure defines the handshake between an
 * application and the SplinterDB library.
 *
 * The application needs to tell SplinterDB some things about keys and values:
 *
 *  1. The sorting order of keys - defined by the key_compare function
 *  2. How to hash keys - defined by the key_hash function
 *  3. How to merge update messages - defined by the pair of merge_tuples* fns.
 *  4. How to convert between messages and values (encode and decode functions)
 *  4. Few other debugging aids on how-to print & diagnose messages.
 *
 * default_data_config.c is a simple reference implementation, provided
 * as a "batteries included" solution. If an application wishes
 * to do something differently, it has to provide these implementations.
 */
struct data_config {
   uint64 key_size;
   uint64 message_size;

   // FIXME: Planned for deprecation.
   char   min_key[MAX_KEY_SIZE];
   uint64 min_key_length;

   char   max_key[MAX_KEY_SIZE];
   uint64 max_key_length;

   key_compare_fn           key_compare;
   key_hash_fn              key_hash;
   message_class_fn         message_class;
   merge_tuple_fn           merge_tuples;
   merge_tuple_final_fn     merge_tuples_final;
   key_or_message_to_str_fn key_to_string;
   key_or_message_to_str_fn message_to_string;

   // required by splinterdb_insert and splinterdb_delete
   encode_message_fn encode_message;

   // required by splinterdb_lookup_result_parse and
   // splinterdb_iterator_get_current
   decode_message_fn decode_message;

   // additional context, available to the above callbacks
   void *context;
};


#endif // __DATA_H
