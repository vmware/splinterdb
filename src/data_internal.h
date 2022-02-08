// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * data_internal.h --
 *
 *     A slice-based interface to the datatype definitions
 */

#ifndef __DATA_INTERNAL_H
#define __DATA_INTERNAL_H

#include "splinterdb/data.h" // Required to access data_config{} definition

/*
 * Function prototypes for data_config->{key, message fn ptrs}
 * If the user / application has not provided these function pointer handles via
 * data_config{} struct, these default handlers will be invoked.
 */
int
default_data_key_cmp(const data_config *cfg,
                     uint64             key1_len,
                     const void *       key1,
                     uint64             key2_len,
                     const void *       key2);

int
default_data_merge_tuples(const data_config *cfg,
                          uint64             key_len,
                          const void *       key,
                          uint64             old_raw_data_len,
                          const void *       old_raw_data,
                          writable_buffer *  new_raw_data);

message_type
default_data_message_class(const data_config *cfg,
                           uint64             raw_data_len,
                           const void *       raw_data);
int
default_data_merge_tuples_final(const data_config *cfg,
                                uint64             key_len,
                                const void *       key,            // IN
                                writable_buffer *  oldest_raw_data); // IN/OUT

void
default_data_key_to_string(const data_config *cfg,
                           uint64             key_len,
                           const void *       key,
                           char *             str,
                           size_t             len);

void
default_data_message_to_string(const data_config *cfg,
                               uint64             raw_data_len,
                               const void *       raw_data,
                               char *             str,
                               size_t             len);

/*
 * --------------------------------------------------------------------------------
 * A collection of static inline functions to invoke the key / message /tuple
 * handling user-defined functions that are specified via the data_config * cfg
 * parameter.
 * --------------------------------------------------------------------------------
 */
static inline int
data_key_compare(const data_config *cfg, const slice key1, const slice key2)
{
   return cfg->key_compare(cfg,
                           slice_length(key1),
                           slice_data(key1),
                           slice_length(key2),
                           slice_data(key2));
}

static inline message_type
data_message_class(const data_config *cfg, const slice raw_message)
{
   return cfg->message_class(
      cfg, slice_length(raw_message), slice_data(raw_message));
}

static inline int
data_merge_tuples(const data_config *cfg,
                  const slice        key,
                  const slice        old_raw_message,
                  writable_buffer   *new_message)
{
   return cfg->merge_tuples(cfg,
                            slice_length(key),
                            slice_data(key),
                            slice_length(old_raw_message),
                            slice_data(old_raw_message),
                            new_message);
}

static inline int
data_merge_tuples_final(const data_config *cfg,
                        const slice        key,
                        writable_buffer   *oldest_message)
{
   return cfg->merge_tuples_final(
      cfg, slice_length(key), slice_data(key), oldest_message);
}

static inline void
data_key_to_string(const data_config *cfg,
                   const slice        key,
                   char              *str,
                   size_t             size)
{
   cfg->key_to_string(cfg, slice_length(key), slice_data(key), str, size);
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
                       const slice        message,
                       char              *str,
                       size_t             size)
{
   cfg->message_to_string(
      cfg, slice_length(message), slice_data(message), str, size);
}

#define message_string(cfg, key)                                               \
   (({                                                                         \
       struct {                                                                \
          char buffer[128];                                                    \
       } b;                                                                    \
       data_message_to_string((cfg), (key), b.buffer, 128);                    \
       b;                                                                      \
    }).buffer)

#endif // __DATA_INTERNAL_H
