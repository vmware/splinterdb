/* Copyright 2011-2021 Bas van den Berg
 * Copyright 2021 VMware, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */
#include <stdio.h>

#define CTEST_MAIN

// uncomment lines below to enable/disable features. See README.md for details
#define CTEST_SEGFAULT
// #define CTEST_NO_COLORS
// #define CTEST_COLOR_OK

#include "ctest.h"

int
main(int argc, const char *argv[])
{
   int result = ctest_main(argc, argv);
   return result;
}

/*
 * Identify the name of the program being executed (argv[0]). The usage will
 * vary depending on whether the top-level 'unit_test' or a stand-alone
 * unit-test binary is being invoked.
 *
 * Returns:
 *  1 - If it is 'unit_test'
 *  0 - For all other stand-alone unit-test binaries being invoked.
 * -1 - For any other error / unknown conditions.
 */
int
ctest_is_unit_test(const char *argv0)
{
   const char *unit_test_str = "unit_test";
   const int   unit_test_len = strlen(unit_test_str);

   if (!argv0) {
      return -1;
   }

   // If we are running some other standalone program of shorter binary length.
   if (strlen(argv0) < unit_test_len) {
      return 0;
   }

   // Check for match of trailing portion of full binary name with 'unit_test'
   const char *startp = (argv0 + strlen(argv0) - unit_test_len);
   if (strncmp(unit_test_str, startp, unit_test_len) == 0) {
      return 1;
   }

   return 0;
}

/*
 * Process the command-line arguments. Handle the following cases:
 *
 * 1. We can invoke the top-level unit_test as follows:
 *
 *      unit_test [ --one-or-more-config options ] [ <suite-name> [
 * <test-case-name> ] ]
 *
 * 2. We can invoke an individual stand-alone unit_test as follows:
 *
 *      sub_unit_test [ --one-or-more-config options ] [ <test-case-name> ]
 *
 * This routines handles both cases to identify suite_name and test-case_name.
 *
 * Returns:
 *  # of trailing arguments processed. -1, in case of any usage / invocation
 * error.
 */
int
ctest_process_args(const int    argc,
                   const char * argv[],
                   int          program_is_unit_test,
                   const char **suite_name,
                   const char **testcase_name)
{
   if (argc <= 1) {
      return 0;
   }

   // If the last arg is a --<arg>, then it's a config option. No need for
   // further processing to extract "name"-args.
   if (strncmp(argv[argc - 1], "--", 2) == 0) {
      return 0;
   }

   // We expect up to 2 trailing "name"-args to be provided.
   if (program_is_unit_test) {
      *suite_name = argv[argc - 1];
   } else {
      *testcase_name = argv[argc - 1];
   }

   if (argc == 2) {
      // We stripped off 1 "name"-argument from list.
      return 1;
   }
   if (strncmp(argv[argc - 2], "--", 2) == 0) {
      // We stripped off 1 "name"-argument from list.
      return 1;
   }

   if (program_is_unit_test) {
      *suite_name    = argv[argc - 2];
      *testcase_name = argv[argc - 1];
      return 2;
   } else {
      // It's an error to issue: sub_unit-test <suite-name> <testcase-name>
      return -1;
   }
   return 0;
}

/*
 * Generate brief usage information to run CTests.
 */
void
ctest_usage(const char *progname, int program_is_unit_test)
{
   printf("Usage: %s [ --<config options> ]* ", progname);
   if (program_is_unit_test) {
      printf("[ <suite-name> [ <test-case-name> ] ]\n");
   } else {
      printf("[ <test-case-name> ]\n");
   }
}
