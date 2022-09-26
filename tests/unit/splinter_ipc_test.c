// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinter_ipc_test.c --
 *
 * This is a correctness test to validate that inserts performed by one
 * thread are "seen" correctly by a concurrently running thread. This test:q
 *  Exercises the interfaces in splinter_shmem.c.
 *  This is just a template file. Fill it out for your specific test suite.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

/*
 * Global data declaration macro:
 */
CTEST_DATA(splinter_ipc){};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(splinter_ipc) {}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(splinter_ipc) {}

/*
 * Basic test case.
 */
CTEST2(splinter_ipc, test_basic_flow) {}
