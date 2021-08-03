// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform.h"

#include "util.h"

#include "poison.h"

/*
 * Utility function; you should not use this directly
 * negative_limit and positive_limit are absolute values
 */
static inline bool try_string_to_uint64_limit(const char *nptr,            // IN
                                              const uint64 negative_limit, // IN
                                              const uint64 positive_limit, // IN
                                              uint64 *n) // OUT
{
  unsigned char c;
  const char *s = nptr;

  // Skip leading spaces
  do {
    c = *s++;
  } while (isspace(c));

  // Skip (single) leading '+', treat single leading '-' as negative
  bool negative = FALSE;
  if (c == '-') {
    if (negative_limit == 0) {
      goto negative_disallowed;
    }
    negative = TRUE;
    c = *s++;
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
  const int cutlim = limit % (uint64)base;

  uint64 value;
  bool converted_any = FALSE;
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
bool try_string_to_uint64(const char *nptr, // IN
                          uint64 *n)        // OUT
{
  const uint64 negative_limit = 0;
  const uint64 positive_limit = UINT64_MAX;
  return try_string_to_uint64_limit(nptr, negative_limit, positive_limit, n);
}

bool try_string_to_int64(const char *nptr, // IN
                         int64 *n)         // OUT
{
  _Static_assert(((-INT64_MAX) - 1) == INT64_MIN, "algorithm mistake");
  const uint64 negative_limit = ((uint64)INT64_MAX) + 1;
  const uint64 positive_limit = (uint64)INT64_MAX;
  uint64 u64;
  if (!try_string_to_uint64_limit(nptr, negative_limit, positive_limit, &u64)) {
    return FALSE;
  }
  *n = (int64)u64;
  return TRUE;
}

bool try_string_to_uint32(const char *nptr, // IN
                          uint32 *n)        // OUT
{
  uint64 tmp;
  if (!try_string_to_uint64(nptr, &tmp) || tmp > UINT32_MAX) {
    return FALSE;
  }
  *n = tmp;
  return TRUE;
}

bool try_string_to_uint16(const char *nptr, // IN
                          uint16 *n)        // OUT
{
  uint64 tmp;
  if (!try_string_to_uint64(nptr, &tmp) || tmp > UINT16_MAX) {
    return FALSE;
  }
  *n = tmp;
  return TRUE;
}

bool try_string_to_uint8(const char *nptr, // IN
                         uint8 *n)         // OUT
{
  uint64 tmp;
  if (!try_string_to_uint64(nptr, &tmp) || tmp > UINT8_MAX) {
    return FALSE;
  }
  *n = tmp;
  return TRUE;
}

bool try_string_to_int32(const char *nptr, // IN
                         int32 *n)         // OUT
{
  int64 tmp;
  if (!try_string_to_int64(nptr, &tmp) || tmp > INT32_MAX || tmp < INT32_MIN) {
    return FALSE;
  }
  *n = tmp;
  return TRUE;
}

bool try_string_to_int16(const char *nptr, // IN
                         int16 *n)         // OUT
{
  int64 tmp;
  if (!try_string_to_int64(nptr, &tmp) || tmp > INT16_MAX || tmp < INT16_MIN) {
    return FALSE;
  }
  *n = tmp;
  return TRUE;
}

bool try_string_to_int8(const char *nptr, // IN
                        int8 *n)          // OUT
{
  int64 tmp;
  if (!try_string_to_int64(nptr, &tmp) || tmp > INT8_MAX || tmp < INT8_MIN) {
    return FALSE;
  }
  *n = tmp;
  return TRUE;
}
