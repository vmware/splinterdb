// From https://github.com/gpakosz/PackedArray
// Lightly edited to match the routing filter API

#ifndef PACKEDARRAY_H
#define PACKEDARRAY_H

#include "platform.h"
#include "poison.h"

/*
 * -----------------------------------------------------------------------------
 * PackedArray principle:
 *  . Compact storage of <= 32 bits items
 *  . Items are tightly packed into a buffer of uint32 integers
 *
 * PackedArray requirements:
 *  . You must know in advance how many bits are needed to hold a single item
 *  . You must know in advance how many items you want to store
 *  . When packing, behavior is undefined if items have more than bitsPerItem
 *    bits
 *
 * PackedArray general in memory representation:
 *
 *  |----------------+----------------+----------------+- - -
 *  |       b0       |       b1       |       b2       |
 *  |----------------+----------------+----------------+- - -
 *  | i0 | i1 | i2 | i3 | i4 | i5 | i6 | i7 | i8 | i9 |
 *  |-------------------------------------------------- - - -
 *
 * . Items are tightly packed together
 * . Several items end up inside the same buffer cell, e.g. i0, i1, i2
 * . Some items span two buffer cells, e.g. i3, i6
 * -----------------------------------------------------------------------------
 */

// packing / unpacking
// offset is expressed in number of elements
void
PackedArray_pack(uint32       *a,
                 const uint32  offset,
                 const uint32 *in,
                 uint32        count,
                 size_t        bitsPerItem);
void
PackedArray_unpack(const uint32 *a,
                   const uint32  offset,
                   uint32       *out,
                   uint32        count,
                   size_t        bitsPerItem);

// single item access
void
PackedArray_set(uint32      *a,
                const uint32 offset,
                const uint32 in,
                size_t       bitsPerItem);
uint32
PackedArray_get(const uint32 *a, const uint32 offset, size_t bitsPerItem);

#endif // #ifndef PACKEDARRAY_H
