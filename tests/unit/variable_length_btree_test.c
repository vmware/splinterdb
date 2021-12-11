// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * variable_length_btree_test.c --
 *
 *  Exercises the BTree interfaces in variable_length_btree_test.c, and related
 *  files. Validates correctness of variable key-value size support in BTree.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/platform_public.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

/*
 * Global data declaration macro:
 */
CTEST_DATA(variable_length_btree)
{
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(variable_length_btree)
{
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(variable_length_btree)
{
}

/*
 *
 * Basic test case.
 */
CTEST2(variable_length_btree, test_basic_flow)
{
}

