// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * data.h --
 *
 *     This file contains constants and functions that pertain to
 *     keys and messages
 *
 *     A message encodes a value and an operation,
 *     like insert, delete or update.
 *
 */

#ifndef __DATA_H
#define __DATA_H

#include <string.h> // for memmove
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
                                         size_t             max_len);

/*
 * ----------------------------------------------------------------------------
 * data_config: This structure defines the handshake between a
 * Key-Value Store application built using Splinter primitives. In order to
 * build this integration, the application needs to tell Splinter few different
 * things about keys and values:
 *
 *  1. The sorting order of keys - defined by the key_compare function
 *  2. How to hash keys - defined by the key_hash function
 *  3. How to merge update messages - defined by the pair of merge_tuples* fns.
 *  4. Few other debugging aids on how-to print & diagnose messages.
 *
 * kvstore_basic.c is a reference implementation of this integration provided
 * as a "batteries included" implementation. If any other application wishes
 * to do something differently, it has to provide these implementations.
 * ----------------------------------------------------------------------------
 */
struct data_config {
   uint64 key_size;
   uint64 message_size;

   // FIXME: Planned for deprecation.
   char min_key[MAX_KEY_SIZE];
   char max_key[MAX_KEY_SIZE];

   key_compare_fn           key_compare;
   key_hash_fn              key_hash;
   message_class_fn         message_class;
   merge_tuple_fn           merge_tuples;
   merge_tuple_final_fn     merge_tuples_final;
   key_or_message_to_str_fn key_to_string;
   key_or_message_to_str_fn message_to_string;

   // additional context, available to the above callbacks
   void *context;
};

static inline bool
data_validate_config(const data_config *cfg)
{
   bool bad =
      (cfg->key_size == 0 || cfg->message_size == 0 || cfg->key_compare == NULL
       || cfg->key_hash == NULL || cfg->merge_tuples == NULL
       || cfg->merge_tuples_final == NULL || cfg->message_class == NULL
       || cfg->key_to_string == NULL || cfg->message_to_string == NULL);
   return !bad;
}

#endif // __DATA_H
