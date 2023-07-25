// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _SPLINTER_UTIL_H_
#define _SPLINTER_UTIL_H_

#include "platform.h"
#include "splinterdb/public_util.h"

#define bitsizeof(x) (8 * sizeof(x))

// Macros
#ifdef IMPLIES
// Replace any existing implementation if it exists (for consistency)
#   undef IMPLIES
#endif
#define IMPLIES(p, q) (!(p) || !!(q))

#define SET_ARRAY_INDEX_TO_STRINGIFY(x) [x] = STRINGIFY(x)


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
 * int64abs --
 *
 *    This function takes an int64 value and it returns the absolute value.
 *    Note the return type is uint64, therefore it can return the absolute
 *    value of the smallest negative number as well.
 *
 * Results:
 *      Returns absolute value of the given value
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static inline uint64
int64abs(int64 j)
{
   return (j >= 0) ? (uint64)j : ((uint64) - (j + 1)) + 1;
}

typedef struct fraction {
   uint64 numerator;
   uint64 denominator;
} fraction;

static inline fraction
init_fraction(uint64 numerator, uint64 denominator)
{
   return (fraction){
      .numerator   = numerator,
      .denominator = denominator,
   };
}

#define zero_fraction                                                          \
   ((fraction){                                                                \
      .numerator   = 0,                                                        \
      .denominator = 1,                                                        \
   })


static inline slice
slice_copy_contents(void *dst, const slice src)
{
   memmove(dst, src.data, src.length);
   return slice_create(src.length, dst);
}

static inline bool32
slice_equals(const slice a, const slice b)
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
 * ----------------------------------------------------------------------
 * Writable buffers can be in one of four states:
 * - uninitialized
 * - null
 *   - length == WRITABLE_BUFFER_NULL_LENGTH
 * - non-null
 *   - length <= allocation_size
 *
 * The writable_buffer maintains two size fields: (1) the logical size
 * of the buffer and (2) the size of the memory it has allocated.  The
 * amount of allocated memory never decreases until the buffer is
 * deinited.
 *
 * When initializing a writable_buffer, you can provide an initial
 * buffer for it to use.  The writable_buffer will _never_ free the
 * buffer you give it during initialization.
 */
typedef struct writable_buffer {
   platform_heap_id heap_id;
   void            *buffer;
   uint64           buffer_capacity;
   uint64           length;
   bool32           can_free;
} writable_buffer;

#define WRITABLE_BUFFER_NULL_LENGTH UINT64_MAX

/* Returns 0 if wb is in the null state */
static inline uint64
writable_buffer_length(const writable_buffer *wb)
{
   if (wb->length == WRITABLE_BUFFER_NULL_LENGTH) {
      return 0;
   }
   return wb->length;
}

/* May allocate memory */
platform_status
writable_buffer_resize(writable_buffer *wb, uint64 newlength);

static inline void *
writable_buffer_data(const writable_buffer *wb)
{
   if (wb->length == WRITABLE_BUFFER_NULL_LENGTH) {
      return NULL;
   } else {
      return wb->buffer;
   }
}

static inline bool32
writable_buffer_is_null(const writable_buffer *wb)
{
   return wb->length == WRITABLE_BUFFER_NULL_LENGTH;
}

static inline void
writable_buffer_init_with_buffer(writable_buffer *wb,
                                 platform_heap_id heap_id,
                                 uint64           allocation_size,
                                 void            *data,
                                 uint64           logical_size)
{
   wb->heap_id         = heap_id;
   wb->buffer          = data;
   wb->buffer_capacity = allocation_size;
   wb->length          = logical_size;
   wb->can_free        = FALSE;
}

static inline void
writable_buffer_init(writable_buffer *wb, platform_heap_id heap_id)
{
   writable_buffer_init_with_buffer(
      wb, heap_id, 0, NULL, WRITABLE_BUFFER_NULL_LENGTH);
}

/*
 * Convenience macro for declaring and initializing an automatically
 * destroyed writable buffer with a stack allocated array.  Usage:
 *
 * DECLARE_AUTO_WRITABLE_BUFFER_N(wb, heapid, 128);
 * // wb is now initialized and ready for use, e.g.
 * writable_buffer_copy_slice(&wb, some_slice);
 * ...
 * //writable_buffer_deinit(&wb); // DO NOT CALL writable_buffer_deinit!
 */
#define DECLARE_AUTO_WRITABLE_BUFFER_N(wb, hid, n)                             \
   char            wb##_tmp[n];                                                \
   writable_buffer wb __attribute__((cleanup(writable_buffer_deinit)));        \
   writable_buffer_init_with_buffer(&wb, hid, n, wb##_tmp, 0)

#define WRITABLE_BUFFER_DEFAULT_AUTO_BUFFER_SIZE (128)
#define DECLARE_AUTO_WRITABLE_BUFFER(wb, hid)                                  \
   DECLARE_AUTO_WRITABLE_BUFFER_N(                                             \
      wb, hid, WRITABLE_BUFFER_DEFAULT_AUTO_BUFFER_SIZE)

static inline void
writable_buffer_set_to_null(writable_buffer *wb)
{
   wb->length = WRITABLE_BUFFER_NULL_LENGTH;
}

static inline void
writable_buffer_deinit(writable_buffer *wb)
{
   if (wb->can_free) {
      platform_free(wb->heap_id, wb->buffer);
   }
   wb->buffer   = NULL;
   wb->can_free = FALSE;
}

static inline void
writable_buffer_memset(writable_buffer *wb, int c)
{
   if (wb->length == WRITABLE_BUFFER_NULL_LENGTH) {
      return;
   }
   memset(wb->buffer, 0, wb->length);
}

static inline platform_status
writable_buffer_copy_slice(writable_buffer *wb, slice src)
{
   platform_status rc = writable_buffer_resize(wb, slice_length(src));
   if (!SUCCESS(rc)) {
      return rc;
   }
   memcpy(wb->buffer, slice_data(src), slice_length(src));
   return rc;
}

static inline platform_status
writable_buffer_init_from_slice(writable_buffer *wb,
                                platform_heap_id heap_id,
                                slice            contents)
{
   writable_buffer_init(wb, heap_id);
   return writable_buffer_copy_slice(wb, contents);
}

static inline slice
writable_buffer_to_slice(const writable_buffer *wb)
{
   if (wb->length == WRITABLE_BUFFER_NULL_LENGTH) {
      return NULL_SLICE;
   } else {
      return slice_create(wb->length, wb->buffer);
   }
}

/* Returns the old length of wb */
static inline uint64
writable_buffer_append(writable_buffer *wb, uint64 length, const void *newdata)
{
   uint64 oldsize = writable_buffer_length(wb);
   platform_assert(SUCCESS(writable_buffer_resize(wb, oldsize + length)));
   char *data = writable_buffer_data(wb);
   memcpy(data + oldsize, newdata, length);
   return oldsize;
}

/*
 * Creates a copy of src in newly declared slice dst.  Everything is
 * automatically cleaned up when dst goes out of scope.
 */
#define SLICE_CREATE_LOCAL_COPY(dst, hid, src)                                 \
   WRITABLE_BUFFER_DEFAULT(dst##wb, hid);                                      \
   writable_buffer_copy_slice(&dst##wb, src);                                  \
   slice dst = writable_buffer_to_slice(&dst##wb);

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
bool32
try_string_to_uint64(const char *nptr, // IN
                     uint64     *n);       // OUT

bool32
try_string_to_int64(const char *nptr, // IN
                    int64      *n);        // OUT

bool32
try_string_to_uint32(const char *nptr, // IN
                     uint32     *n);       // OUT

bool32
try_string_to_int32(const char *nptr, // IN
                    int32      *n);        // OUT

bool32
try_string_to_uint16(const char *nptr, // IN
                     uint16     *n);       // OUT

bool32
try_string_to_int16(const char *nptr, // IN
                    int16      *n);        // OUT

bool32
try_string_to_uint8(const char *nptr, // IN
                    uint8      *n);        // OUT

bool32
try_string_to_int8(const char *nptr, // IN
                   int8       *n);         // OUT


/*
 * The following macros are used to automate type-safe string comparison
 * between a const char * and a string literal.
 */

#define REQUIRE_STRING_LITERAL(x) (x "")

#define SIZEOF_STRING_LITERAL(s) (sizeof(REQUIRE_STRING_LITERAL(s)))

#define STRING_EQUALS_LITERAL(arg, str)                                        \
   (strncmp(arg, str, SIZEOF_STRING_LITERAL(str)) == 0)

/* In-memory structures that should be packed are tagged with this. */
#define PACKED __attribute__((__packed__))

/* Disk-resident structures that should be packed are tagged with this. */
#define ONDISK __attribute__((__packed__))

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
void
debug_hex_encode(char *dst, size_t dst_len, const char *data, size_t data_len);

void
debug_hex_dump(platform_log_handle *,
               uint64      grouping,
               uint64      length,
               const char *bytes);

void
debug_hex_dump_slice(platform_log_handle *, uint64 grouping, slice data);


/*
 * Evaluates to a print format specifier based on the value being printed.
 * (Modeled after similar PRIxx #defines seen in inttypes.h .)
 */
#define DECIMAL_STRING_WIDTH(intval)                                           \
   (((intval) < 10)     ? "d"                                                  \
    : ((intval) < 100)  ? "2d"                                                 \
    : ((intval) < 1000) ? "3d"                                                 \
                        : "4d")

// Length of output buffer to snprintf()-into size as string w/ unit specifier
#define SIZE_TO_STR_LEN 20

// Format a size value with unit-specifiers, in an output buffer.
char *
size_to_str(char *outbuf, size_t outbuflen, size_t size);

char *
size_to_fmtstr(char *outbuf, size_t outbuflen, const char *fmtstr, size_t size);

/*
 * Convenience caller macros to convert 'sz' bytes to return a string,
 * formatting the input size as human-readable value with unit-specifiers.
 */
// char *size_str(size_t sz)
#define size_str(sz)                                                           \
   (({                                                                         \
       struct {                                                                \
          char buffer[SIZE_TO_STR_LEN];                                        \
       } onstack_chartmp;                                                      \
       size_to_str(                                                            \
          onstack_chartmp.buffer, sizeof(onstack_chartmp.buffer), sz);         \
       onstack_chartmp;                                                        \
    }).buffer)

// char *size_fmtstr(const char *fmtstr, size_t sz)
#define size_fmtstr(fmtstr, sz)                                                \
   (({                                                                         \
       struct {                                                                \
          char buffer[SIZE_TO_STR_LEN];                                        \
       } onstack_chartmp;                                                      \
       size_to_fmtstr(                                                         \
          onstack_chartmp.buffer, sizeof(onstack_chartmp.buffer), fmtstr, sz); \
       onstack_chartmp;                                                        \
    }).buffer)

#endif // _SPLINTER_UTIL_H_
