// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * test_misc_common.h --
 *
 * Header file with shared prototypes and definitions for miscellaneous
 * functions defined in test_misc_common.c, that are shared between functional/
 * and unit/ test sources. This interface is provided so that stand-alone
 * unit-tests that need these routines can be built w/o having to link-in all of
 * SplinterDB.
 * -----------------------------------------------------------------------------
 */
#ifndef __TEST_MISC_COMMON_H__
#define __TEST_MISC_COMMON_H__


bool
test_using_shmem(int argc, char *argv[]);

#endif /* __TEST_MISC_COMMON_H__ */
