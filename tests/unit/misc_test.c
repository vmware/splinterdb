// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * misc_test.c --
 *
 * Miscellaneous test cases of non-core but important functionality.
 * -----------------------------------------------------------------------------
 */
#include <stdarg.h>
#include "platform.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.
#include "util.h"

#define ASSERT_OUTBUF_LEN 200

/* String constants needed to invoke assertion macros. */
#define MISC_MSG_WITH_NO_ARGS "Test basic assertion msg with no arguments."
#define MISC_MSG_WITH_ARGS                                                     \
   "Test assertion msg with arguments id = %d, name = '%s'."

/* Function prototypes and caller-macros to invoke testing interfaces */
static void
test_platform_assert_msg(platform_log_handle *log_handle,
                         const char          *filename,
                         int                  linenumber,
                         const char          *functionname,
                         const char          *expr,
                         const char          *message,
                         ...);

/*
 * Same as platform_assert() but simply exercises the message-raising
 * aspect of platform_assert_false(). All we really test here is that the
 * user-supplied message with arguments is being printed as expected.
 */
#define test_platform_assert(log_handle, expr, ...)                            \
   test_platform_assert_msg(                                                   \
      log_handle, __FILE__, __LINE__, __FUNCTION__, #expr, "" __VA_ARGS__)

static void
test_vfprintf_usermsg(platform_log_handle *log_handle,
                      const char          *message,
                      ...);

/*
 * Caller macro to exercise and validate the message printed by
 * vfprintf_usermsg(). That interface is what gets exercised, to print a
 * user-supplied message with parameters, when a CTest ASSERT_*() check were to
 * fail.
 */
#define test_assert_usermsg(log_handle, ...)                                   \
   test_vfprintf_usermsg(log_handle, "" __VA_ARGS__)

static void
test_streqn(char *expstr, char *actstr, int caseno);

/*
 * Global data declaration macro:
 */
CTEST_DATA(misc)
{
   platform_log_handle *log_output;
};

CTEST_SETUP(misc)
{
   // All test cases in this test usually deal with error handling
   set_log_streams_for_tests(MSG_LEVEL_ERRORS);
   data->log_output = platform_get_stdout_stream();
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(misc) {}

/*
 * Basic test case that exercises assertion checking code with a message
 * but no parameters.
 */
CTEST2(misc, test_assert_basic_msg)
{
   platform_stream_handle stream;
   platform_open_log_stream(&stream);
   platform_log_handle *log_handle = platform_log_stream_to_log_handle(&stream);
   int                  lineno     = __LINE__ + 1;
   test_platform_assert(log_handle, 1 == 2, MISC_MSG_WITH_NO_ARGS);
   char *assert_str = platform_log_stream_to_string(&stream);

   // Skip other pid/tid details prepended to the message
   assert_str = strstr(assert_str, "Assertion");

   // Construct an expected assertion message, using parts we know about.
   char expmsg[ASSERT_OUTBUF_LEN];
   snprintf(expmsg,
            sizeof(expmsg),
            "Assertion failed at %s:%d:%s(): \"1 == 2\". %s",
            __FILE__,
            lineno,
            "ctest_misc_test_assert_basic_msg_run",
            MISC_MSG_WITH_NO_ARGS);

   ASSERT_STREQN(expmsg, assert_str, strlen(expmsg));
   platform_close_log_stream(&stream, data->log_output);
}

/*
 * Basic test case that exercises assertion checking code with a message
 * and a couple of parameters.
 */
CTEST2(misc, test_assert_msg_with_args)
{
   int   arg_id   = 42;
   char *arg_name = "Some-name-string";

   platform_stream_handle stream;
   platform_open_log_stream(&stream);
   platform_log_handle *log_handle = platform_log_stream_to_log_handle(&stream);
   int                  lineno     = __LINE__ + 2;
   // clang-format off
   test_platform_assert(log_handle, 1 == 2, MISC_MSG_WITH_ARGS, arg_id, arg_name);
   // clang-format on

   char *assert_str = platform_log_stream_to_string(&stream);

   // Skip other pid/tid details prepended to the message
   assert_str = strstr(assert_str, "Assertion");

   // Construct an expected assertion message, using parts we know about.
   char expmsg[ASSERT_OUTBUF_LEN];
   snprintf(expmsg,
            sizeof(expmsg),
            "Assertion failed at %s:%d:%s(): \"1 == 2\". " MISC_MSG_WITH_ARGS,
            __FILE__,
            lineno,
            "ctest_misc_test_assert_msg_with_args_run",
            arg_id,
            arg_name);

   ASSERT_STREQN(expmsg, assert_str, strlen(expmsg));
   platform_close_log_stream(&stream, data->log_output);
}

/*
 * Test case to exercise testing-variant of ASSERT_EQUAL() to demonstrate that
 * the user-supplied message with parameters gets printed correctly.
 */
CTEST2(misc, test_ctest_assert_prints_user_msg_with_params)
{
   int   value       = 42;
   char *some_string = "This is an expected failure message string.";

   platform_stream_handle stream;
   platform_open_log_stream(&stream);
   platform_log_handle *log_handle = platform_log_stream_to_log_handle(&stream);
   test_assert_usermsg(
      log_handle, "Unexpected failure, value=%d, msg='%s'", value, some_string);
   char *assert_str = platform_log_stream_to_string(&stream);

   // Construct an expected user message, using parts we know about.
   char expmsg[ASSERT_OUTBUF_LEN];
   snprintf(expmsg,
            sizeof(expmsg),
            "Unexpected failure, value=%d, msg='%s'",
            value,
            some_string);

   const int expmsg_len = strlen(expmsg);
   ASSERT_STREQN(expmsg,
                 assert_str,
                 expmsg_len,
                 "\nExpected: %.*s\n"
                 "Received: %.*s\n",
                 expmsg_len,
                 expmsg,
                 expmsg_len,
                 assert_str);
   platform_close_log_stream(&stream, data->log_output);
}

/*
 * Simple test to verify lower-level conversion macros used by size_to_str()
 */
CTEST2(misc, test_bytes_to_fractional_value_macros)
{
   // Run through a few typical cases to check lower-level macros
   size_t size = (KiB + ((25 * KiB) / 100)); // 1.25 KiB
   ASSERT_EQUAL(25, B_TO_KiB_FRACT(size));

   size = (MiB + ((5 * MiB) / 10)); // 1.5 MiB
   ASSERT_EQUAL(50, B_TO_MiB_FRACT(size));

   size = (GiB + ((75 * GiB) / 100)); // 1.75 GiB
   ASSERT_EQUAL(75, B_TO_GiB_FRACT(size));

   size = (TiB + ((5 * TiB) / 10)); // 1.5 TiB
   ASSERT_EQUAL(50, B_TO_TiB_FRACT(size));
}

/*
 * Test case to verify conversion of size value to a string with
 * unit-specifiers. Do quick cross-check of the lower-level conversion macros
 * involved.
 */
CTEST2(misc, test_size_to_str)
{
   char size_str[SIZE_TO_STR_LEN];
   char size_str2[SIZE_TO_STR_LEN + 2]; // size-as-string enclosed in ()
   typedef struct size_to_expstr {
      size_t size;
      char  *expstr;
   } size_to_expstr;

   // Verify base-case, and validate ptr returned to output string buffer.
   size_t size   = 0;
   char  *expstr = "0 bytes";
   char  *outstr = size_to_str(size_str, sizeof(size_str), size);
   // Conversion fn should return input-buffer's start addr as outstr.
   ASSERT_TRUE(outstr == size_str);
   test_streqn(expstr, outstr, 0);

   // Built expected string enclosed in ()
   snprintf(size_str2, sizeof(size_str2), "(%s)", size_str);

   // Size as string enclosed in () will be generated in this buffer
   char *expstr2 = size_str2;
   outstr        = size_to_fmtstr(size_str2, sizeof(size_str2), "(%s)", size);

   // Conversion fn should return input-str's start addr as outstr.
   ASSERT_TRUE(outstr == size_str2);
   test_streqn(expstr2, size_str2, 0);

   // Exercise the string formatter with typical data values.
   // clang-format off
   size_to_expstr size_to_str_cases[] =
   {
          { 129                            , "129 bytes"  }

        , { KiB                            , "1 KiB"      }
        , { MiB                            , "1 MiB"      }
        , { GiB                            , "1 GiB"      }
        , { TiB                            , "1 TiB"      }

        , { KiB + 128                      , "~1.12 KiB"  }
        , { KiB + ((25 * KiB) / 100)       , "~1.25 KiB"  }
        , { MiB + 128                      , "~1.00 MiB"   }
        , { MiB + ((5 * MiB) / 10)         , "~1.50 MiB"  }
        , { GiB + 128                      , "~1.00 GiB"   }
        , { GiB + ((75 * GiB) / 100)       , "~1.75 GiB"  }
        , { (3 * GiB) + ((5 * GiB) / 10)   , "~3.50 GiB"  }
        , { TiB + 128                      , "~1.00 TiB"   }
        , { (2 * TiB) + ((25 * TiB) / 100) , "~2.25 TiB"  }

        // Specific data-values that tripped bugs in formatting output string
        , { 2222981120                     , "~2.07 GiB"  }
        , { KiB + 28                       , "~1.02 KiB"  }
        , { MiB + (98 * KiB)               , "~1.09 MiB"   }
        , { GiB + (55 * MiB)               , "~1.05 GiB"   }
   };
   // clang-format on

   for (int cctr = 0; cctr < ARRAY_SIZE(size_to_str_cases); cctr++) {
      size         = size_to_str_cases[cctr].size;
      expstr       = size_to_str_cases[cctr].expstr;
      char *outstr = size_to_str(size_str, sizeof(size_str), size);
      test_streqn(expstr, outstr, cctr);

      // Build expected string enclosed in ()
      snprintf(size_str2, sizeof(size_str2), "(%s)", size_str);
      size_to_fmtstr(size_str, sizeof(size_str), "(%s)", size);
      test_streqn(expstr2, size_str, cctr);
   }
}

/* Helper functions follow all test case methods */

/*
 * Wrapper function to exercise platform_assert_msg() into an output
 * buffer. Used to test that the message is generated as expected.
 */
static void
test_platform_assert_msg(platform_log_handle *log_handle,
                         const char          *filename,
                         int                  linenumber,
                         const char          *functionname,
                         const char          *expr,
                         const char          *message,
                         ...)
{
   va_list varargs;
   va_start(varargs, message);
   platform_assert_msg(
      log_handle, filename, linenumber, functionname, expr, message, varargs);
   va_end(varargs);
}

/*
 * Wrapper function to exercise the message-printing logic of vfprintf_usermsg()
 * into an output buffer. Used to test that the message is generated as
 * expected.
 */
static void
test_vfprintf_usermsg(platform_log_handle *log_handle, const char *message, ...)
{
   VFPRINTF_USERMSG(log_handle, message);
}

/* Helper fn to check that expected / actual null-terminated strings match */
static void
test_streqn(char *expstr, char *actstr, int caseno)
{
   ASSERT_STREQN(expstr,
                 actstr,
                 strlen(expstr),
                 "Test case=%d failed, Expected: '%.*s', "
                 "Received: '%.*s'\n",
                 caseno,
                 strlen(expstr),
                 expstr,
                 strlen(expstr),
                 actstr);
}
