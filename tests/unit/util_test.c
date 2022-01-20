// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * util_test.c --
 *
 *  Validate various utility interfaces.
 * -----------------------------------------------------------------------------
 */
#include "util.h"
#include "ctest.h" // This is required for all test-case files.

static const char debug_hex_encode_sample_data[10] =
   {0, 1, 2, 3, 0xa4, 5, 0xd6, 7, 8, 9};

static int
check_one_debug_hex_encode(size_t      dst_len,
                           size_t      data_len,
                           const char *expected);

/*
 * Global data declaration macro:
 */
CTEST_DATA(util){};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(util) {}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(util) {}

/*
 * Test debug_hex_encode() in all its variations.
 */
CTEST2(util, test_debug_hex_encode)
{
   // output = 0x <data> \0
   //          2 + 2*10 + 1 = 23 bytes to fully encode that
   const int dst_len = 23;

   int rc = check_one_debug_hex_encode(dst_len,
                                       sizeof(debug_hex_encode_sample_data),
                                       "0x00010203a405d6070809\0xx");
   ASSERT_EQUAL(
      0, rc, "When lengths are exact, it fills without overflowing. ");

   rc = check_one_debug_hex_encode(dst_len, 7, "0x00010203a405d6\0xxxxxxxx");
   ASSERT_EQUAL(
      0, rc, "When dst_len is larger then required, it stops early. ");

   rc = check_one_debug_hex_encode(
      0, sizeof(debug_hex_encode_sample_data), "xxxxxxxxxxxxxxxxxxxxxxxxx");
   ASSERT_EQUAL(0, rc, "When dst_len is 0, it writes nothing.");

   rc = check_one_debug_hex_encode(
      1, sizeof(debug_hex_encode_sample_data), "\0xxxxxxxxxxxxxxxxxxxxxxxx");
   ASSERT_EQUAL(0, rc, "When dst_len is 1, it just writes the null char. ");

   rc = check_one_debug_hex_encode(
      2, sizeof(debug_hex_encode_sample_data), "0\0xxxxxxxxxxxxxxxxxxxxxxx");
   ASSERT_EQUAL(0, rc, "When dst_len is 2, it just writes the prefix 0 char. ");

   rc = check_one_debug_hex_encode(
      3, sizeof(debug_hex_encode_sample_data), "0x\0xxxxxxxxxxxxxxxxxxxxxx");
   ASSERT_EQUAL(
      0, rc, "When dst_len is 3, it just writes the prefix 0x char. ");

   rc = check_one_debug_hex_encode(
      4, sizeof(debug_hex_encode_sample_data), "0x0\0xxxxxxxxxxxxxxxxxxxxx");
   ASSERT_EQUAL(0, rc, "When dst_len is 4, it writes only the first digit. ");

   rc = check_one_debug_hex_encode(
      5, sizeof(debug_hex_encode_sample_data), "0x00\0xxxxxxxxxxxxxxxxxxxx");
   ASSERT_EQUAL(0, rc, "When dst_len is 5, it writes the first octet. ");

   rc = check_one_debug_hex_encode(
      7, sizeof(debug_hex_encode_sample_data), "0x0001\0xxxxxxxxxxxxxxxxxx");
   ASSERT_EQUAL(
      0, rc, "When dst_len is too short (odd), it doesn't overflow. ");

   rc = check_one_debug_hex_encode(
      8, sizeof(debug_hex_encode_sample_data), "0x00010\0xxxxxxxxxxxxxxxxx");
   ASSERT_EQUAL(
      0, rc, "When dst_len is too short (even), it doesn't overflow. ");

   rc = check_one_debug_hex_encode((dst_len - 1),
                                   sizeof(debug_hex_encode_sample_data),
                                   "0x00010203a405d607080\0xxx");
   ASSERT_EQUAL(0, rc, "When dst_len is too short by 1, it doesn't overflow. ");
}

static int
check_one_debug_hex_encode(size_t      dst_len,
                           size_t      data_len,
                           const char *expected)
{
   // make a slightly oversized buffer, and fill with xs
   // so we can detect overflow
   const int dst_full_size = 25;
   char      dst[25];
   memset(dst, 'x', dst_full_size);

   debug_hex_encode(dst, dst_len, debug_hex_encode_sample_data, data_len);
   if (memcmp(dst, expected, dst_full_size) != 0) {

      // Indentation is intentional, so both outputs line up for easier
      // visual comparison in case of failures.
      fprintf(stderr,
              "unexpected output: '%.*s',\n"
              "                                      expected output: '%s'\n",
              dst_full_size,
              dst,
              expected);
      return -1;
   }
   return 0;
}
