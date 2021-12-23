// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * misc_test.c --
 *
 * Miscellaneous test cases of non-core but important functionality.
 * -----------------------------------------------------------------------------
 */
#include "platform.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

#define ASSERT_OUTBUF_LEN 200

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
   // test_platform_assert(1 == 2, DO_NOT_ABORT_ON_ASSERT_FAIL);
   fflush(stream);

   // Construct an expected assertion message, using parts we know about.
   char expmsg[ASSERT_OUTBUF_LEN];
   snprintf(expmsg,
            sizeof(expmsg),
            "Assertion failed at %s:%d:%s(): \"1 == 2\". %s",
            __FILE__,
            lineno,
            "ctest_misc_test_assert_basic_msg_run",
            DO_NOT_ABORT_ON_ASSERT_FAIL);

   ASSERT_STREQN(expmsg, bp, strlen(expmsg));
   platform_close_log_stream(Platform_stderr_fh);
}

/*
 * Basic test case that exercises assertion checking code with a message
 * and a couple of no parameters.
 */
CTEST2(misc, test_assert_msg_with_args)
{
   int   arg_id   = 42;
   char *arg_name = "Some-name-string";

   platform_open_log_stream();
   /*
    * Test code below is carefully constructing an expected assertion message
    * containing a line number. The value generated for __LINE__ seems to be
    * different for gcc v/s clang when the test_platform_assert() is expanded.
    * To get consistent test results, turn off clang-formatting temporarily so
    * that we can squish the call to test_platform_assert() one one line.
    */
   // clang-format off
   int lineno = __LINE__ + 1;
   /*
   test_platform_assert(1 == 2, DO_NOT_ABORT_ON_ASSERT_FAIL ", with args id=%d, name='%s'", arg_id, arg_name);
   */
   // clang-format on

   fflush(stream);
   fprintf(stderr, "Msg: '%s'\n", bp);

   // Construct an expected assertion message, using parts we know about.
   char expmsg[ASSERT_OUTBUF_LEN];
   snprintf(
      expmsg,
      sizeof(expmsg),
      "Assertion failed at %s:%d:%s(): \"1 == 2\". " DO_NOT_ABORT_ON_ASSERT_FAIL
      ", with args id=%d, name='%s'",
      __FILE__,
      lineno,
      "ctest_misc_test_assert_msg_with_args_run",
      arg_id,
      arg_name);

   ASSERT_STREQN(expmsg, bp, strlen(expmsg));
   platform_close_log_stream(Platform_stderr_fh);
}
