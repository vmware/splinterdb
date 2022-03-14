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
#include "splinterdb/public_platform.h"
#include "splinterdb/public_util.h"

typedef enum message_type {
   MESSAGE_TYPE_INSERT,
   MESSAGE_TYPE_UPDATE,
   MESSAGE_TYPE_DELETE,
   MESSAGE_TYPE_INVALID,
} message_type;

typedef struct message {
   message_type type;
   slice        data;
} message;

static inline message_type
message_class(message msg)
{
   return msg.type;
}

static inline slice
message_slice(message msg)
{
   return msg.data;
}

static inline uint64
message_length(message m)
{
   return slice_length(m.data);
}

static inline const void *
message_data(message m)
{
   return slice_data(m.data);
}

typedef struct merge_accumulator merge_accumulator;

typedef struct data_config data_config;

typedef int (*key_compare_fn)(const data_config *cfg, slice key1, slice key2);

typedef uint32 (*key_hash_fn)(const void *input, size_t length, uint32 seed);

// Given two messages, old_message and new_message, merge them
// and return the result in new_raw_message.  new_message is guaranteed to be an
// UPDATE and old_messge is guaranteed to be an INSERT or an UPDATE.
//
// Returns 0 on success.  non-zero indicates an internal error.
typedef int (*merge_tuple_fn)(const data_config *cfg,
                              slice              key,          // IN
                              message            old_message,  // IN
                              merge_accumulator *new_message); // IN/OUT

// Called for MESSAGE_TYPE_UPDATE messages when they are
// determined to be the oldest message. Can change data_class or
// contents.  If necessary, update new_data.
//
// Returns 0 on success.  non-zero indicates an internal error.
typedef int (*merge_tuple_final_fn)(const data_config *cfg,
                                    slice              key,
                                    merge_accumulator *oldest_message);

typedef void (*key_to_str_fn)(const data_config *cfg,
                              slice              key,
                              char              *str,
                              uint64             max_len);

typedef void (*message_to_str_fn)(const data_config *cfg,
                                  message            msg,
                                  char              *str,
                                  uint64             max_len);

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
   // FIXME: Planned for deprecation.
   uint64 key_size;

   // FIXME: Planned for deprecation.
   char   min_key[MAX_KEY_SIZE];
   uint64 min_key_length;

   char   max_key[MAX_KEY_SIZE];
   uint64 max_key_length;

   key_compare_fn       key_compare;
   key_hash_fn          key_hash;
   merge_tuple_fn       merge_tuples;
   merge_tuple_final_fn merge_tuples_final;
   key_to_str_fn        key_to_string;
   message_to_str_fn    message_to_string;
};

#endif // __DATA_H
