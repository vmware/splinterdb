// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * unit_tests_common.c
 *
 * Common functions shared across unit test sources.
 * -----------------------------------------------------------------------------
 */
#include "ctest.h" // This is required for all test-case files.
#include "unit_tests.h"

/*
 * Setup function to manage output log streams from most (unit) tests.
 * By default, there will be no output. Info / error messages are only printed
 * when the caller sets the VERBOSE env var to opt-in.
 */
void
set_log_streams_for_tests()
{
   if (Ctest_verbose) {
      platform_set_log_streams(stdout, stderr);
   }
}

/*
 * Setup function is provided to manage output log streams from (unit) tests
 * that are induce error conditions and check that error messages are raised
 * as expected.
 *
 * Some unit tests exercise error cases, so even when everything succeeds, they
 * generate lots of "error" messages. By default, that would go to stderr, which
 * would pollute the test output. This function sets up output file handles such
 * that those expected error messages are only printed when the caller sets the
 * VERBOSE env var to opt-in. By default, execution from unit-tests will be
 * silent, redirecting output handles to /dev/null.
 *
 * Returns: Output file handles (out/error) if caller has requested for them.
 */
void
set_log_streams_for_error_tests(platform_log_handle **log_stdout,
                                platform_log_handle **log_stderr)
{
   // Here we ensure those expected error messages are only printed
   // when the caller sets the VERBOSE env var to opt-in.
   if (Ctest_verbose) {
      platform_set_log_streams(stdout, stderr);
      if (log_stdout) {
         *log_stdout = stdout;
      }
      if (log_stderr) {
         *log_stderr = stderr;
      }
      CTEST_LOG_INFO("\nVerbose mode on. This test may exercise an error case, "
                     "so on success it will print a message that appears to "
                     "be an error. "
                     "Or, the test may exercise print-diagnostic code.\n");
   } else {
      FILE *dev_null = fopen("/dev/null", "w");
      ASSERT_NOT_NULL(dev_null);
      platform_set_log_streams(dev_null, dev_null);
      if (log_stdout) {
         *log_stdout = dev_null;
      }
      if (log_stderr) {
         *log_stderr = dev_null;
      }
   }
}
