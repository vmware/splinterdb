// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * test_misc_common.c --
 *
 * Module contains functions shared between functional/ and unit/ test sources.
 * -----------------------------------------------------------------------------
 */
#include "util.h"


/*
 * Has user requested that the test, and therefore SplinterDB, be
 * run using shared memory? The expectation is that the very 1st
 * argument should be "--use-shmem".
 */
bool
test_using_shmem(int argc, char *argv[])
{
   bool retval = FALSE;

   if (argc && (STRING_EQUALS_LITERAL(argv[0], "--use-shmem"))) {
      retval = TRUE;
   }
   return retval;
}
