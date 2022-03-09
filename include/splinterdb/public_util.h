// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef __PUBLIC_UTIL_H__
#define __PUBLIC_UTIL_H__

#include "splinterdb/public_platform.h"

/* Opaque resiable buffer.
 * Internally, it may allocate and deallocate memory. */
typedef struct writable_buffer writable_buffer;

uint64
writable_buffer_length(writable_buffer *wb);

/* Allocates memory as needed. Returns TRUE on success. */
bool
writable_buffer_set_length(writable_buffer *wb, uint64 newlength);

/* Returns a ptr to the data region held by this writable_buffer */
void *
writable_buffer_data(writable_buffer *wb);


/*
 * Non-disk resident descriptor for a [<length>, <value ptr>] pair
 * Used to pass-around references to keys and values of different lengths.
 *
 * Avoid accessing these fields directly.
 * Instead, use the slice_length and slice_data accessor functions.
 */
typedef struct slice {
   uint64      length;
   const void *data;
} slice;

extern const slice NULL_SLICE;

static inline bool
slice_is_null(const slice b)
{
   return b.length == 0 && b.data == NULL;
}

static inline slice
slice_create(uint64 len, const void *data)
{
   return (slice){.length = len, .data = data};
}

static inline uint64
slice_length(const slice b)
{
   return b.length;
}

static inline const void *
slice_data(const slice b)
{
   return b.data;
}

#endif /* __PUBLIC_UTIL_H__ */
