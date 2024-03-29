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
 * Setup function is provided to manage output log streams from (unit) tests.
 * By default, execution from unit-tests will be silent, redirecting output
 * handles to /dev/null. Info / error messages are only printed when the caller
 * sets the VERBOSE env var to opt-in.
 *
 * Some unit tests exercise error cases, so even when everything succeeds, they
 * generate lots of "error" messages. By default, that would go to stderr, which
 * would pollute the test output. This function sets up output file handles such
 * that those expected error messages are only printed when the caller sets the
 * VERBOSE env var to opt-in.
 *
 * **** How to use this interface in test code: *****
 *
 * Tests which exercise error paths in the library should first call this
 * function with MSG_LEVEL_ERRORS, so that any error messages generated by
 * the library are only shown when the test is run with VERBOSE >= 3.
 *
 * Tests which cause the library to generate debug output (e.g. by printing
 * internal data structures) should call this function with MSG_LEVEL_DEBUG,
 * so that that output is only shown when the test is run with VERBOSE >= 7.
 *
 * Tests would usually have to call this interface with MSG_LEVEL_INFO to
 * reset the output streams to the default behaviour. (See existing usages
 * for sample invocations.)
 */
void
set_log_streams_for_tests(msg_level exp_msg_level)
{
   // Here we ensure those expected error messages are only printed
   // when the caller sets the VERBOSE env var to opt-in.
   if (Ctest_verbosity >= exp_msg_level) {
      // If user is running tests at a level asking for info messages ...
      if (Ctest_verbosity >= MSG_LEVEL_INFO) {
         // Print info-messages to stdout also.
         platform_set_log_streams(stdout, stderr);
      } else {
         // Otherwise, only error messages will be printed
         FILE *dev_null = fopen("/dev/null", "w");
         platform_set_log_streams(dev_null, stderr);
      }

      // Inform the user as to what kind of messages / output to expect
      if (exp_msg_level == MSG_LEVEL_DEBUG) {
         CTEST_LOG(
            "Verbose mode on. This test may exercise print-diagnostic code.\n");
      } else if (exp_msg_level == MSG_LEVEL_ERRORS) {
         CTEST_LOG(
            "Verbose mode on. This test may exercise an error case, so on"
            " success it may print a message that appears to be an error.\n");
      }
   } else if (exp_msg_level == MSG_LEVEL_INFO) {
      // Equivalent to 'reset' functionality where only errors will be reported.
      FILE *dev_null = fopen("/dev/null", "w");
      ASSERT_NOT_NULL(dev_null);
      platform_set_log_streams(dev_null, stderr);
   } else {
      // Fall-back to 'silent' behaviour; all output is suppressed.
      FILE *dev_null = fopen("/dev/null", "w");
      ASSERT_NOT_NULL(dev_null);
      platform_set_log_streams(dev_null, dev_null);
   }
}
