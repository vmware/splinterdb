// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * data.h --
 *
 *     This file contains constants and functions the pertain to
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

typedef enum message_type {
   MESSAGE_TYPE_INSERT,
   MESSAGE_TYPE_UPDATE,
   MESSAGE_TYPE_DELETE,
   MESSAGE_TYPE_INVALID,
} message_type;

typedef struct data_config data_config;

typedef int (*key_compare_fn)(const data_config *cfg,
                              const void *       key1,
                              const void *       key2);

typedef uint32 (*key_hash_fn)(const void *input, size_t length, uint32 seed);

typedef message_type (*message_class_fn)(const data_config *cfg,
                                         const void *       raw_message);

// Given two messages, merge them, based on their types
// And return the result in new_raw_message
typedef void (*merge_tuple_fn)(const data_config *cfg,
                               const void *       key,
                               const void *       old_raw_message,
                               void *             new_raw_message);

// Called for non-MESSAGE_TYPE_INSERT messages
// when they are determined to be the oldest message
//
// Can change data_class or contents.  If necessary, update new_data.
typedef void (*merge_tuple_final_fn)(const data_config *cfg,
                                     const void *       key,
                                     void *             oldest_raw_message);

typedef void (*key_or_message_to_str_fn)(const data_config *cfg,
                                         const void *       key_or_message,
                                         char *             str,
                                         size_t             max_len);

struct data_config {
   uint64 key_size;
   uint64 message_size;

   // robj: we should get rid of min/max key
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

static inline int
data_key_compare(const data_config *cfg, const void *key1, const void *key2)
{
   return cfg->key_compare(cfg, key1, key2);
}

static inline message_type
data_message_class(const data_config *cfg, void *raw_message)
{
   return cfg->message_class(cfg, raw_message);
}

static inline void
data_merge_tuples(const data_config *cfg,
                  const void *       key,
                  const void *       old_raw_message,
                  void *             new_raw_message)
{
   cfg->merge_tuples(cfg, key, old_raw_message, new_raw_message);
}

static inline void
data_merge_tuples_final(const data_config *cfg,
                        const void *       key,
                        void *             oldest_raw_message)
{
   return cfg->merge_tuples_final(cfg, key, oldest_raw_message);
}

static inline void
data_key_to_string(const data_config *cfg,
                   const void *       key,
                   char *             str,
                   size_t             size)
{
   cfg->key_to_string(cfg, key, str, size);
}

static inline void
data_message_to_string(const data_config *cfg,
                       const void *       message,
                       char *             str,
                       size_t             size)
{
   cfg->message_to_string(cfg, message, str, size);
}

// robj: this is really just a convenience function.  Key copying is
// _not_ an operation that the application can hook into.
static inline void
data_key_copy(const data_config *cfg, void *dst, const void *src)
{
   memmove(dst, src, cfg->key_size);
}

static inline bool
data_validate_config(const data_config *cfg)
{
   bool bad = (cfg->key_size == 0 || cfg->message_size == 0 ||
               cfg->key_compare == NULL || cfg->key_hash == NULL ||
               cfg->merge_tuples == NULL || cfg->merge_tuples_final == NULL ||
               cfg->message_class == NULL || cfg->key_to_string == NULL ||
               cfg->message_to_string == NULL);
   return !bad;
}

#endif // __DATA_H
