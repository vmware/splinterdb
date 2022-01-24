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

#define ASSERT_OUTBUF_LEN 200

/* String constants needed to invoke assertion macros. */
#define MISC_MSG_WITH_NO_ARGS "Test basic assertion msg with no arguments."
#define MISC_MSG_WITH_ARGS                                                     \
   "Test assertion msg with arguments id = %d, name = '%s'."

/* Function prototype */
void
test_platform_assert_msg(platform_stream_handle stream,
                         const char *           filename,
                         int                    linenumber,
                         const char *           functionname,
                         const char *           expr,
                         const char *           message,
                         ...);

/*
 * Same as platform_assert() but simply exercises the message-raising
 * aspect of platform_assert_impl(). All we really test here is that the
 * user-supplied message with arguments is being printed as expected.
 */
#define test_platform_assert(expr, ...)                                        \
   test_platform_assert_msg(                                                   \
      stream, __FILE__, __LINE__, __FUNCTION__, #expr, "" __VA_ARGS__)

/*
 * Global data declaration macro:
 */
CTEST_DATA(misc){};

CTEST_SETUP(misc) {}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(misc) {}

/*
 * Basic test case that exercises assertion checking code with a message
 * but no parameters.
 */
CTEST2(misc, test_assert_basic_msg)
{
   platform_open_log_stream();
   int lineno = __LINE__ + 1;
   test_platform_assert(1 == 2, MISC_MSG_WITH_NO_ARGS);
   fflush(stream);

   // Construct an expected assertion message, using parts we know about.
   char expmsg[ASSERT_OUTBUF_LEN];
   snprintf(expmsg,
            sizeof(expmsg),
            "Assertion failed at %s:%d:%s(): \"1 == 2\". %s",
            __FILE__,
            lineno,
            "ctest_misc_test_assert_basic_msg_run",
            MISC_MSG_WITH_NO_ARGS);

   ASSERT_STREQN(expmsg, bp, strlen(expmsg));
   platform_close_log_stream(Platform_stderr_fh);
}

/*
 * Basic test case that exercises assertion checking code with a message
 * and a couple of parameters.
 */
CTEST2(misc, test_assert_msg_with_args)
{
   int   arg_id   = 42;
   char *arg_name = "Some-name-string";

   platform_open_log_stream();

   int lineno = __LINE__ + 1;
   test_platform_assert(1 == 2, MISC_MSG_WITH_ARGS, arg_id, arg_name);

   fflush(stream);

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

   ASSERT_STREQN(expmsg, bp, strlen(expmsg));
   platform_close_log_stream(Platform_stderr_fh);
}

/*
 * Wrapper function to exercise platform_assert_msg() into an output
 * buffer. Used to test that the message is generated as expected.
 */
void
test_platform_assert_msg(platform_stream_handle stream,
                         const char *           filename,
                         int                    linenumber,
                         const char *           functionname,
                         const char *           expr,
                         const char *           message,
                         ...)
{
   va_list varargs;
   va_start(varargs, message);
   platform_assert_msg(
      stream, filename, linenumber, functionname, expr, message, varargs);
   va_end(varargs);
}
