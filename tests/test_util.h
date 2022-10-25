// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef __TEST_UTIL_H__
#define __TEST_UTIL_H__

#include "data_internal.h"

static inline bool
keys_equal_contents(key a, key b)
{
   return slice_lex_cmp(a.user_slice, b.user_slice) == 0;
}

static inline key
writable_buffer_to_key(writable_buffer *wb)
{
   return key_create_from_slice(writable_buffer_to_slice(wb));
}

static inline void
writable_buffer_copy_key(writable_buffer *wb, key src)
{
   writable_buffer_copy_slice(wb, key_slice(src));
}

#endif /* __TEST_UTIL_H__ */
