// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _SPLINTER_UTIL_H_
#define _SPLINTER_UTIL_H_

#include "platform.h"


// Macros
#ifdef IMPLIES
   // Replace any existing implementation if it exists (for consistency)
#  undef IMPLIES
#endif
#define IMPLIES(p, q)  (!(p) || !!(q))

#define SET_ARRAY_INDEX_TO_STRINGIFY(x) \
   [x] = STRINGIFY(x)


static inline const void *
const_pointer_byte_offset(const void *base, int64 offset)
{
   return (const uint8 *)base + offset;
}

static inline void *
pointer_byte_offset(void *base, int64 offset)
{
   return (uint8 *)base + offset;
}

/*
 *-----------------------------------------------------------------------------
 *
 * int64abs --
 *
 *    This function takes an int64 value and it returns the abolute value.
 *    Note the return type is uint64, therefore it can return the absolute
 *    value of the smallest negative number as well.
 *
 * Results:
 *      Returns abolute value of the given value
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

static inline uint64
int64abs(int64 j)
{
   return (j >= 0)
          ? (uint64) j
          : ((uint64) -(j+1)) + 1;
}

typedef struct fraction {
  uint64 numerator;
  uint64 denominator;
} fraction;

static inline fraction
init_fraction(uint64 numerator, uint64 denominator)
{
   return (fraction) {
      .numerator = numerator,
      .denominator = denominator,
   };
}

#define zero_fraction ((fraction) { \
      .numerator = 0,               \
      .denominator = 1,             \
   })

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

static inline slice
slice_copy_contents(void *dst, const slice src)
{
   memmove(dst, src.data, src.length);
   return slice_create(src.length, dst);
}

static inline bool
slices_equal(const slice a, const slice b)
{
   return a.length == b.length && a.data == b.data;
}

static inline int
slice_lex_cmp(const slice a, const slice b)
{
   uint64 len1   = slice_length(a);
   uint64 len2   = slice_length(b);
   uint64 minlen = len1 < len2 ? len1 : len2;
   int    cmp    = memcmp(slice_data(a), slice_data(b), minlen);
   if (cmp) {
      return cmp;
   } else if (len1 < len2) {
      return -1;
   } else {
      return len1 - len2;
   }
}

/*
 * try_string_to_(u)int64
 *
 * Convert a string to a (u)int64.
 * Roughly equivalent to using strtoul/strtol with base=0.
 * Allows leading and trailing spaces but expects the entire string to be a
 * single number.
 * Expects strings to be optional leading spaces, optional sign identifier [+-],
 * base identifier, actual digits, and trailing spaces.
 *
 * Will return failure (FALSE) if the string does not exactly (fully) match one
 * of the following regular expressions:
 *    Hex:     "[ ]*[+-]?0[Xx][0-9a-fA-F]+[ ]*"
 *    Octal:   "[ ]*[+-]?0[0-7]*[ ]*"
 *    Decimal: "[ ]*[+-]?[1-9][0-9]*[ ]*"
 * Will return failure (FALSE) if any of
 * - overflow or underflow occurs
 * - asking for uint64 and you provide a negative number
 *
 * Base is automatically detected based on the regular expressions above
 */
bool
try_string_to_uint64(const char *nptr, // IN
                     uint64 *n);       // OUT

bool
try_string_to_int64(const char *nptr, // IN
                    int64 *n);        // OUT

bool
try_string_to_uint32(const char *nptr, // IN
                     uint32 *n);       // OUT

bool
try_string_to_int32(const char *nptr, // IN
                    int32 *n);        // OUT

bool
try_string_to_uint16(const char *nptr, // IN
                     uint16 *n);       // OUT

bool
try_string_to_int16(const char *nptr, // IN
                    int16 *n);        // OUT

bool
try_string_to_uint8(const char *nptr, // IN
                    uint8 *n);        // OUT

bool
try_string_to_int8(const char *nptr, // IN
                   int8 *n);         // OUT


/*
 * The following macros are used to automate type-safe string comparison
 * between a const char * and a string literal.
 */

#define REQUIRE_STRING_LITERAL(x) (x"")

#define SIZEOF_STRING_LITERAL(s) (sizeof(REQUIRE_STRING_LITERAL(s)))

#define STRING_EQUALS_LITERAL(arg, str) \
   (strncmp(arg, str, SIZEOF_STRING_LITERAL(str)) == 0)

#define PACKED  __attribute__((__packed__))

// Hex-encode arbitrary bytes to a destination buffer
//    e.g. 0xc0de4f00de
//
// Unless dst_len is 0, the result is always \0-terminated.
//
// It should be suitable for basic debug output, printf-style.
//
// Do not rely on the result for comparison or equality checking,
// because the data may be truncated when dst_len is too small.
//
// To avoid truncation, ensure dst_len >= 3 + 2 * data_len.
void debug_hex_encode(char *      dst,
                      size_t      dst_len,
                      const char *data,
                      size_t      data_len);

#endif
