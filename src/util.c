// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform.h"

#include "util.h"

#include "poison.h"

static platform_status
writable_buffer_ensure_space(writable_buffer *wb, uint64 minspace)
{
   if (minspace <= wb->buffer_capacity) {
      return STATUS_OK;
   }

   if (minspace < 2 * wb->buffer_capacity) {
      minspace = 2 * wb->buffer_capacity;
   }

   void *oldptr  = wb->can_free ? wb->buffer : NULL;
   void *newdata = platform_realloc(wb->heap_id, oldptr, minspace);
   if (newdata == NULL) {
      return STATUS_NO_MEMORY;
   }

   if (oldptr == NULL && wb->length != WRITABLE_BUFFER_NULL_LENGTH) {
      memcpy(newdata, wb->buffer, wb->length);
   }

   wb->buffer_capacity = minspace;
   wb->buffer          = newdata;
   wb->can_free        = TRUE;
   return STATUS_OK;
}

platform_status
writable_buffer_resize(writable_buffer *wb, uint64 newlength)
{
   platform_assert(newlength != WRITABLE_BUFFER_NULL_LENGTH);
   platform_status rc = writable_buffer_ensure_space(wb, newlength);
   if (!SUCCESS(rc)) {
      return rc;
   }
   wb->length = newlength;
   return rc;
}

/*
 *----------------------------------------------------------------------
 * Utility function; you should not use this directly
 * negative_limit and positive_limit are absolute values
 *----------------------------------------------------------------------
 */
static inline bool32
try_string_to_uint64_limit(const char  *nptr,           // IN
                           const uint64 negative_limit, // IN
                           const uint64 positive_limit, // IN
                           uint64      *n)                   // OUT
{
   unsigned char c;
   const char   *s = nptr;

   // Skip leading spaces
   do {
      c = *s++;
   } while (isspace(c));

   // Skip (single) leading '+', treat single leading '-' as negative
   bool32 negative = FALSE;
   if (c == '-') {
      if (negative_limit == 0) {
         goto negative_disallowed;
      }
      negative = TRUE;
      c        = *s++;
   } else if (c == '+') {
      c = *s++;
   }

   /* Detect base */
   int base;
   if (c == '0' && (*s == 'x' || *s == 'X')) {
      // 0x.* and 0X.* are hexadecimal
      c = s[1];
      s += 2;
      base = 16;
   } else if (c == '0') {
      // 0.* is octal
      base = 8;
   } else {
      // decimal is default
      base = 10;
   }

   /*
    * Check whether we're handling a negative signed value and must
    * adjust the limit appropriately.
    */
   const uint64 limit = negative ? negative_limit : positive_limit;

   /*
    * If the value passes cutoff and we have another character we will
    * overflow.
    * If the value is cutoff and we have a new character, and the new 'digit' is
    * past cutlim, we will overflow.
    */
   const uint64 cutoff = limit / (uint64)base;
   const int    cutlim = limit % (uint64)base;

   uint64 value;
   bool32 converted_any = FALSE;
   for (value = 0; c != '\0'; c = *s++) {
      if (isspace(c)) {
         break;
      }
      if (!isascii(c)) {
         goto invalid_characters;
      }

      if (isdigit(c)) {
         c -= '0';
      } else if (isalpha(c)) {
         c -= isupper(c) ? 'A' - 10 : 'a' - 10;
      } else {
         goto invalid_characters;
      }

      if (c >= base) {
         // Invalid character and/or too high for our base
         goto invalid_characters;
      }

      if (value > cutoff || (value == cutoff && c > cutlim)) {
         goto overflow;
      }
      converted_any = TRUE;
      value *= base;
      value += c;
   }
   // Trim any trailing spaces
   while (isspace(c)) {
      c = *s++;
   }
   if (c != '\0') {
      /*
       * We had trailing space(s) followed by something else.
       * This function is intended to convert an entire string.
       */
      goto multiple_strings;
   }

   if (!converted_any) {
      goto no_digits;
   }
   if (negative) {
      value = -value;
   }
   *n = value;
   return TRUE;

   /*
    * Right now we just return FALSE but if necessary we can later provide
    * an error enum (or some other return value) indicating the reason we failed
    */
no_digits:
negative_disallowed:
invalid_characters:
overflow:
multiple_strings:
   return FALSE;
}


/*
 *----------------------------------------------------------------------
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
 *----------------------------------------------------------------------
 */
bool32
try_string_to_uint64(const char *nptr, // IN
                     uint64     *n)        // OUT
{
   const uint64 negative_limit = 0;
   const uint64 positive_limit = UINT64_MAX;
   return try_string_to_uint64_limit(nptr, negative_limit, positive_limit, n);
}

bool32
try_string_to_int64(const char *nptr, // IN
                    int64      *n)         // OUT
{
   _Static_assert(((-INT64_MAX) - 1) == INT64_MIN, "algorithm mistake");
   const uint64 negative_limit = ((uint64)INT64_MAX) + 1;
   const uint64 positive_limit = (uint64)INT64_MAX;
   uint64       u64;
   if (!try_string_to_uint64_limit(nptr, negative_limit, positive_limit, &u64))
   {
      return FALSE;
   }
   *n = (int64)u64;
   return TRUE;
}

bool32
try_string_to_uint32(const char *nptr, // IN
                     uint32     *n)        // OUT
{
   uint64 tmp;
   if (!try_string_to_uint64(nptr, &tmp) || tmp > UINT32_MAX) {
      return FALSE;
   }
   *n = tmp;
   return TRUE;
}

bool32
try_string_to_uint16(const char *nptr, // IN
                     uint16     *n)        // OUT
{
   uint64 tmp;
   if (!try_string_to_uint64(nptr, &tmp) || tmp > UINT16_MAX) {
      return FALSE;
   }
   *n = tmp;
   return TRUE;
}

bool32
try_string_to_uint8(const char *nptr, // IN
                    uint8      *n)         // OUT
{
   uint64 tmp;
   if (!try_string_to_uint64(nptr, &tmp) || tmp > UINT8_MAX) {
      return FALSE;
   }
   *n = tmp;
   return TRUE;
}

bool32
try_string_to_int32(const char *nptr, // IN
                    int32      *n)         // OUT
{
   int64 tmp;
   if (!try_string_to_int64(nptr, &tmp) || tmp > INT32_MAX || tmp < INT32_MIN) {
      return FALSE;
   }
   *n = tmp;
   return TRUE;
}

bool32
try_string_to_int16(const char *nptr, // IN
                    int16      *n)         // OUT
{
   int64 tmp;
   if (!try_string_to_int64(nptr, &tmp) || tmp > INT16_MAX || tmp < INT16_MIN) {
      return FALSE;
   }
   *n = tmp;
   return TRUE;
}

bool32
try_string_to_int8(const char *nptr, // IN
                   int8       *n)          // OUT
{
   int64 tmp;
   if (!try_string_to_int64(nptr, &tmp) || tmp > INT8_MAX || tmp < INT8_MIN) {
      return FALSE;
   }
   *n = tmp;
   return TRUE;
}

static const char table[16] = {'0',
                               '1',
                               '2',
                               '3',
                               '4',
                               '5',
                               '6',
                               '7',
                               '8',
                               '9',
                               'a',
                               'b',
                               'c',
                               'd',
                               'e',
                               'f'};

void
debug_hex_encode(char        *dst,
                 const size_t dst_len,
                 const char  *data,
                 const size_t data_len)
{
   if (dst_len == 0) {
      return;
   }

   // 0x prefix + 2 bytes per octet + \0 terminator
   size_t max_len = 2 + 2 * data_len + 1;
   if (max_len > dst_len) {
      max_len = dst_len;
   }

   if (max_len == 1) {
      goto null_terminate;
   }

   dst[0] = '0';
   dst[1] = 'x';
   int dp = 2;
   for (int i = 0; i < data_len; i++) {
      if (dp >= max_len - 1) {
         // only remaining space will be used for \0
         goto null_terminate;
      }

      unsigned char x = data[i];
      dst[dp]         = table[(x & 0xF0) >> 4];
      dst[dp + 1]     = table[(x & 0x0F)];
      dp += 2;
   }
null_terminate:
   dst[max_len - 1] = '\0';
}

void
debug_hex_dump(platform_log_handle *plh,
               uint64               grouping,
               uint64               length,
               const char          *bytes)
{
   for (uint64 i = 0; i < length; i++) {
      platform_log(
         plh, "%02x%s", bytes[i], grouping && !((i + 1) % grouping) ? " " : "");
   }
}

void
debug_hex_dump_slice(platform_log_handle *plh, uint64 grouping, slice data)
{
   debug_hex_dump(plh, grouping, slice_length(data), slice_data(data));
}

/*
 * Format a size value with unit-specifiers, in an output buffer.
 * Returns 'outbuf', as ptr to size-value snprintf()'ed as a string.
 */
char *
size_to_str(char *outbuf, size_t outbuflen, size_t size)
{
   debug_assert(outbuflen >= SIZE_TO_STR_LEN, "outbuflen=%lu.\n", outbuflen);
   size_t unit_val  = 0;
   size_t frac_val  = 0;
   bool32 is_approx = FALSE;

   char *units = NULL;
   if (size >= TiB) {
      unit_val  = B_TO_TiB(size);
      frac_val  = B_TO_TiB_FRACT(size);
      is_approx = (size > TiB_TO_B(unit_val));
      units     = "TiB";

   } else if (size >= GiB) {
      unit_val  = B_TO_GiB(size);
      frac_val  = B_TO_GiB_FRACT(size);
      is_approx = (size > GiB_TO_B(unit_val));
      units     = "GiB";

   } else if (size >= MiB) {
      unit_val  = B_TO_MiB(size);
      frac_val  = B_TO_MiB_FRACT(size);
      is_approx = (size > MiB_TO_B(unit_val));
      units     = "MiB";

   } else if (size >= KiB) {
      unit_val  = B_TO_KiB(size);
      frac_val  = B_TO_KiB_FRACT(size);
      is_approx = (size > KiB_TO_B(unit_val));
      units     = "KiB";
   } else {
      unit_val = size;
      units    = "bytes";
   }

   if (frac_val || is_approx) {
      snprintf(outbuf, outbuflen, "~%ld.%02ld %s", unit_val, frac_val, units);
   } else {
      snprintf(outbuf, outbuflen, "%ld %s", unit_val, units);
   }
   return outbuf;
}

/*
 * Sibling of size_to_str(), but uses user-provided print format specifier.
 * 'fmtstr' is expected to have just one '%s', and whatever other text user
 * wishes to print with the output string.
 */
char *
size_to_fmtstr(char *outbuf, size_t outbuflen, const char *fmtstr, size_t size)
{
   snprintf(outbuf, outbuflen, fmtstr, size_str(size));
   return outbuf;
}
