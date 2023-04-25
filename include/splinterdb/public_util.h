// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/public_platform.h"

/*
 * A slice is just a const pointer with a length.  Slices do not
 * manage the memory to which they point, i.e. slices do not allocate,
 * free, realloc, or do anything else to the underlying memory
 * allocation.
 *
 * The best way to think of slices is: they're just const pointers.
 *
 * Slices are not disk resident.  They are used to pass-around
 * references to keys, values, etc, of different lengths in memory.
 *
 * Avoid accessing these fields directly.
 * Instead, use the slice_length and slice_data accessor functions.
 */
typedef struct slice {
   uint64      length;
   const void *data;
} slice;

#define NULL_SLICE    ((slice){.length = 0, .data = NULL})
#define INVALID_SLICE ((slice){.length = (uint64)-1, .data = NULL})

static inline _Bool
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
