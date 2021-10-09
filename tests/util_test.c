#include "util.h"
#include <string.h>


// Function prototypes
static void
util__prBytes(const char *teststr);

static const char debug_hex_encode_sample_data[10] =
   {0, 1, 2, 3, 0xa4, 5, 0xd6, 7, 8, 9};

static int
check_one_debug_hex_encode(size_t      dst_len,
                           size_t      data_len,
                           const char *expected,
                           const char *test_case)
{

   // make a slightly oversized buffer, and fill with xs
   // so we can detect overflow
   const int dst_full_size = 25;
   char      dst[25];
   memset(dst, 'x', dst_full_size);


   fprintf(stderr, "case: %s: ", test_case);

   debug_hex_encode(dst, dst_len, debug_hex_encode_sample_data, data_len);
   if (memcmp(dst, expected, dst_full_size) != 0) {
      fprintf(stderr, "unexpected output: %.*s\n", dst_full_size, dst);
      return -1;
   }
   fprintf(stderr, "OK\n");
   return 0;
}

#define test_assert(expression)                                                \
   {                                                                           \
      int rc = (expression);                                                   \
      if (rc != 0) {                                                           \
         return rc;                                                            \
      }                                                                        \
   }


int
test_util_debug_hex_encode()
{
   fprintf(stderr, "start: test_util_debug_hex_encode\n");


   // output = 0x <data> \0
   //          2 + 2*10 + 1 = 23 bytes to fully encode that
   const int dst_len = 23;

   test_assert(check_one_debug_hex_encode(
      dst_len,
      sizeof(debug_hex_encode_sample_data),
      "0x00010203a405d6070809\0xx",
      "when lengths are exact, it fills without overflowing"));

   test_assert(check_one_debug_hex_encode(
      dst_len,
      7,
      "0x00010203a405d6\0xxxxxxxx",
      "when dst_len is larger then required, it stops early"));

   test_assert(
      check_one_debug_hex_encode(0,
                                 sizeof(debug_hex_encode_sample_data),
                                 "xxxxxxxxxxxxxxxxxxxxxxxxx",
                                 "when dst_len is 0, it writes nothing"));

   test_assert(check_one_debug_hex_encode(
      1,
      sizeof(debug_hex_encode_sample_data),
      "\0xxxxxxxxxxxxxxxxxxxxxxxx",
      "when dst_len is 1, it just writes the null char"));

   test_assert(check_one_debug_hex_encode(
      2,
      sizeof(debug_hex_encode_sample_data),
      "0\0xxxxxxxxxxxxxxxxxxxxxxx",
      "when dst_len is 2, it just writes the prefix 0"));

   test_assert(check_one_debug_hex_encode(
      3,
      sizeof(debug_hex_encode_sample_data),
      "0x\0xxxxxxxxxxxxxxxxxxxxxx",
      "when dst_len is 3, it just writes the prefix 0x"));

   test_assert(check_one_debug_hex_encode(
      4,
      sizeof(debug_hex_encode_sample_data),
      "0x0\0xxxxxxxxxxxxxxxxxxxxx",
      "when dst_len is 4, it writes only the first digit"));

   test_assert(check_one_debug_hex_encode(
      5,
      sizeof(debug_hex_encode_sample_data),
      "0x00\0xxxxxxxxxxxxxxxxxxxx",
      "when dst_len is 5, it writes the first octet"));

   test_assert(check_one_debug_hex_encode(
      7,
      sizeof(debug_hex_encode_sample_data),
      "0x0001\0xxxxxxxxxxxxxxxxxx",
      "when dst_len is too short (odd), it doesn't overflow"));

   test_assert(check_one_debug_hex_encode(
      8,
      sizeof(debug_hex_encode_sample_data),
      "0x00010\0xxxxxxxxxxxxxxxxx",
      "when dst_len is short (even), it doesn't overflow"));

   test_assert(check_one_debug_hex_encode(
      dst_len - 1,
      sizeof(debug_hex_encode_sample_data),
      "0x00010203a405d607080\0xxx",
      "when dst_len is too short by 1, it doesn't overflow"));

   return 0;
}

/*
 * Exercise debug print utility method, prBytes(), to see if it works
 * reliably for few cases.
 * Test fn copied from test_util_debug_hex_encode()
 */
int
test_util_prBytes(void)
{
   fprintf(stderr, "start: test_util_prBytes\n");

   char *teststr = "0x00010203a405d6070809\0xx";
   util__prBytes(teststr);

   teststr = "abcdefghijklmnopqrstuvwxyz";
   util__prBytes(teststr);

   teststr = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
   util__prBytes(teststr);

   teststr = "aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQrRsStTuUvVwWxXyYzZ";
   util__prBytes(teststr);

   int nbytes = 200;
   fprintf(stderr, "\n-- prBytes() on prBytes() for %d bytes\n", nbytes);
   prBytes((char *)prBytes, nbytes);

   char outbuf[NBYTES2HEX(200)];
   fprintf(
      stderr, "\n-- debug_hex_encode() on prBytes() for %d bytes\n", nbytes);
   debug_hex_encode(outbuf, sizeof(outbuf), (char *)prBytes, nbytes);
   fprintf(stderr, "%p: %s\n", (void *)prBytes, outbuf);
   return 0;
}

/*
 * Helper function to invoke prBytes() and debug_hex_encode() on an input
 * string. Useful for manual comparison of outputs.
 */
static void
util__prBytes(const char *teststr)
{
   fprintf(stderr,
           "\n-- prBytes() v/s debug_hex_encode() for %lu bytes\n",
           strlen(teststr));
   prBytes(teststr, strlen(teststr));

   char outbuf[NBYTES2HEX(200)];
   debug_hex_encode(outbuf, sizeof(outbuf), teststr, strlen(teststr));
   fprintf(stderr, "%p: %s\n", (void *)prBytes, outbuf);
}

int
util_test(int argc, char *argv[])
{
   test_util_prBytes();

   int rc = test_util_debug_hex_encode();
   if (rc == 0) {
      fprintf(stderr, "util_test: succeeded\n");
      return 0;
   } else {
      fprintf(stderr, "util_test: FAILED\n");
      return -1;
   }
}
