// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef __PUBLIC_UTIL_H__
#define __PUBLIC_UTIL_H__

#include "splinterdb/public_platform.h"

/*
 * A writable buffer is a resizable buffer whose contents can be
 * updated.
 *
 * Writable buffers can be in the following states:
 * - a special "null" state (analogous to a null pointer)
 * - non-null of length L.  Note that L may be 0, i.e. the null state
 *   is not the same as the length-0 state.
 *
 * You can query for the current length, which returns the special
 * WRITABLE_BUFFER_NULL_LENGTH when the writable_buffer is in the null
 * state.
 *
 * You can change the logical length of the buffer by calling
 * writable_buffer_resize.  Passing WRITABLE_BUFFER_NULL_LENGTH for
 * the newlength is not allowed.
 *
 * writable_buffer_data returns a pointer to the memory managed by the
 * writable_buffer.  You can then update the contents of the data as
 * you see fit.  You must not access beyond the logical length of the
 * data.
 *
 * CAUTION: Performing a resize invalidates any pointers returned by
 * prior calls to writable_buffer_data.
 *
 */
typedef struct writable_buffer writable_buffer;

#define WRITABLE_BUFFER_NULL_LENGTH UINT64_MAX

/* Returns 0 if wb is in the null state */
uint64
writable_buffer_length(writable_buffer *wb);

/* Allocates memory as needed. Returns TRUE on success. */
bool
writable_buffer_resize(writable_buffer *wb, uint64 newlength);

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
