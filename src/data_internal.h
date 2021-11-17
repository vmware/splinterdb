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

extern const void *data_key_negative_infinity_buffer;
extern const void *data_key_positive_infinity_buffer;
extern const slice data_key_negative_infinity;
extern const slice data_key_positive_infinity;

static inline int
data_key_compare(const data_config *cfg, const slice key1, const slice key2)
{
   if (slices_equal(key1, key2))
      return 0;
   if (slices_equal(key1, data_key_negative_infinity))
      return -1;
   if (slices_equal(key1, data_key_positive_infinity))
      return 1;
   if (slices_equal(key2, data_key_negative_infinity))
      return 1;
   if (slices_equal(key2, data_key_positive_infinity))
      return -1;
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

static inline void
data_merge_tuples(const data_config *cfg,
                  const slice        key,
                  const slice        old_raw_message,
                  slice *            new_raw_message)
{
   cfg->merge_tuples(cfg,
                     slice_length(key),
                     slice_data(key),
                     slice_length(old_raw_message),
                     slice_data(old_raw_message),
                     &new_raw_message->length,
                     slice_data(*new_raw_message));
}

static inline void
data_merge_tuples_final(const data_config *cfg,
                        const slice        key,
                        slice *            oldest_raw_message)
{
   return cfg->merge_tuples_final(cfg,
                                  slice_length(key),
                                  slice_data(key),
                                  &oldest_raw_message->length,
                                  slice_data(*oldest_raw_message));
}

static inline void
data_key_to_string(const data_config *cfg,
                   const slice        key,
                   char *             str,
                   size_t             size)
{
   if (slices_equal(key, data_key_negative_infinity)) {
      memmove(str, "(-infinity)", size < 12 ? size : 12);
      if (size)
         str[size - 1] = 0;

   } else if (slices_equal(key, data_key_positive_infinity)) {
      memmove(str, "(+infinity)", size < 12 ? size : 12);
      if (size)
         str[size - 1] = 0;
   } else {
      cfg->key_to_string(cfg, slice_length(key), slice_data(key), str, size);
   }
}

static inline void
data_message_to_string(const data_config *cfg,
                       const slice        message,
                       char *             str,
                       size_t             size)
{
   cfg->message_to_string(
      cfg, slice_length(message), slice_data(message), str, size);
}

// robj: this is really just a convenience function.  Key copying is
// _not_ an operation that the application can hook into.
static inline void
data_key_copy(const data_config *cfg, void *dst, const slice src)
{
   memmove(dst, slice_data(src), slice_length(src));
}

/*
 * The fixed-size wrappers are compatibility code while transitioning the
 * rest of the system to use slices.
 */

static inline int
fixed_size_data_key_compare(const data_config *cfg,
                            const void *       key1,
                            const void *       key2)
{
   if (key1 == key2)
      return 0;
   if (key1 == data_key_negative_infinity_buffer)
      return -1;
   if (key1 == data_key_positive_infinity_buffer)
      return 1;
   if (key2 == data_key_negative_infinity_buffer)
      return 1;
   if (key2 == data_key_positive_infinity_buffer)
      return -1;
   return cfg->key_compare(cfg, cfg->key_size, key1, cfg->key_size, key2);
}

static inline message_type
fixed_size_data_message_class(const data_config *cfg, const void *raw_message)
{
   return cfg->message_class(cfg, cfg->message_size, raw_message);
}

static inline void
fixed_size_data_merge_tuples(const data_config *cfg,
                             const void *       key,
                             const void *       old_raw_message,
                             void *             new_raw_message)
{
   uint64 msglen = cfg->message_size;
   cfg->merge_tuples(cfg,
                     cfg->key_size,
                     key,
                     cfg->message_size,
                     old_raw_message,
                     &msglen,
                     new_raw_message);
}

static inline void
fixed_size_data_merge_tuples_final(const data_config *cfg,
                                   const void *       key,
                                   void *             oldest_raw_message)
{
   uint64 msglen = cfg->message_size;
   return cfg->merge_tuples_final(
      cfg, cfg->key_size, key, &msglen, oldest_raw_message);
}

static inline void
fixed_size_data_key_to_string(const data_config *cfg,
                              const void *       key,
                              char *             str,
                              size_t             size)
{
   cfg->key_to_string(cfg, cfg->key_size, key, str, size);
}

static inline void
fixed_size_data_message_to_string(const data_config *cfg,
                                  const void *       message,
                                  char *             str,
                                  size_t             size)
{
   cfg->message_to_string(cfg, cfg->message_size, message, str, size);
}

// robj: this is really just a convenience function.  Key copying is
// _not_ an operation that the application can hook into.
static inline void
fixed_size_data_key_copy(const data_config *cfg, void *dst, const void *src)
{
   memmove(dst, src, cfg->key_size);
}


#endif // __DATA_INTERNAL_H
