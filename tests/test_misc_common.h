// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * test_misc_common.h --
 *
 * Header file with shared prototypes and definitions for miscellaneous
 * functions that are shared between functional/ and unit/ test sources.
 * This interface is provided so that stand-alone unit-tests that need these
 * routines can be built w/o having to link-in all of SplinterDB.
 *
 * NOTE: Right now, we don't see a need for a .c file, which will require
 *       listing that object in the Makefile for every unit-test that needs to
 *       use these helper methods.
 * -----------------------------------------------------------------------------
 */
#ifndef __TEST_MISC_COMMON_H__
#define __TEST_MISC_COMMON_H__

#include "util.h" // For STRING_EQUALS_LITERAL()

/*
 * Check if the user has supplied very 1st arg as "--use-shmem", which will
 * run the test(s) using shared memory segment setup & allocation.
 */
static inline bool
test_using_shmem(int argc, char *argv[])
{
   return (argc && (STRING_EQUALS_LITERAL(argv[0], "--use-shmem")));
}

#endif /* __TEST_MISC_COMMON_H__ */
