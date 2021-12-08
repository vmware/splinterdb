#include <stdio.h>

#define CTEST_MAIN

// uncomment lines below to enable/disable features. See README.md for details
#define CTEST_SEGFAULT
// #define CTEST_NO_COLORS
// #define CTEST_COLOR_OK

#include "platform.h"
#include "ctest.h"

int
main(int argc, const char *argv[])
{
   int result = ctest_main(argc, argv);
   return result;
}

/*
 * Generate brief usage information to run CTests.
 */
void
ctest_usage(const char *progname)
{
   printf("%s [ <suite-name> [ <test-case-name> ] ]\n", progname);
}
