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

#pragma once

#include "splinterdb/public_platform.h"
#include "splinterdb/public_util.h"

/*
 * Message type up to MESSAGE_TYPE_MAX_VALID_USER_TYPE is a
 * disk-resident value (not including MESSAGE_TYPE_INVALID).
 */
typedef enum message_type {
   MESSAGE_TYPE_INVALID = 0,
   MESSAGE_TYPE_INSERT,
   MESSAGE_TYPE_UPDATE,
   MESSAGE_TYPE_DELETE,
   MESSAGE_TYPE_MAX_VALID_USER_TYPE = MESSAGE_TYPE_DELETE,
   MESSAGE_TYPE_PIVOT_DATA          = 1000
} message_type;

/*
 * Messages
 *
 * Messages are similar to slices in that they are essentially just
 * pointers to the message contents.  They just help carry around a
 * little extra information about the message (i.e. its type and
 * length).
 */
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
message_length(message msg)
{
   return slice_length(msg.data);
}

static inline const void *
message_data(message msg)
{
   return slice_data(msg.data);
}

/*
 * Merge accumulators
 */

typedef struct merge_accumulator merge_accumulator;

message_type
merge_accumulator_message_class(const merge_accumulator *ma);

void *
merge_accumulator_data(const merge_accumulator *ma);

uint64
merge_accumulator_length(const merge_accumulator *ma);

slice
merge_accumulator_to_slice(const merge_accumulator *ma);

_Bool
merge_accumulator_copy_message(merge_accumulator *ma, message msg);

_Bool
merge_accumulator_resize(merge_accumulator *ma, uint64 newsize);

void
merge_accumulator_set_class(merge_accumulator *ma, message_type type);

/*
 * Data configuration callback function types
 */

typedef struct data_config data_config;

typedef int (*key_compare_fn)(const data_config *cfg, slice key1, slice key2);

typedef uint32 (*key_hash_fn)(const void *input, size_t length, uint32 seed);

// Given two messages, old_message and new_message, merge them
// and return the result in new_message.
//
// Upon entry, new_message is guaranteed to be an UPDATE and
// old_messge is guaranteed to be an INSERT or an UPDATE.
//
// Returns 0 on success.  Non-zero indicates an internal error.
typedef int (*merge_tuple_fn)(const data_config *cfg,
                              slice              key,          // IN
                              message            old_message,  // IN
                              merge_accumulator *new_message); // IN/OUT

// Called for MESSAGE_TYPE_UPDATE messages when there is no older
// record to apply the update to.
//
// Upon return, merge_accumulator_message_class(old_message) must be
// either MESSAGE_TYPE_INSERT or MESSAGE_TYPE_DELETE.
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
   uint64 max_key_size;

   key_compare_fn key_compare;
   key_hash_fn    key_hash;
   /* The merge functions may be NULL, in which case
      splinterdb_update() is not allowed. */
   merge_tuple_fn       merge_tuples;
   merge_tuple_final_fn merge_tuples_final;
   key_to_str_fn        key_to_string;
   message_to_str_fn    message_to_string;
};
