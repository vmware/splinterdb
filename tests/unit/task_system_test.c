// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * task_system_test.c --
 *
 *  Exercises the interfaces in src/task.c .
 *  These unit-tests are constructed to verify basic execution of interfaces
 *  in the SplinterDB task system.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

/*
 * Global data declaration macro:
 */
CTEST_DATA(task_system_test){};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(task_system) {}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(task_system) {}

/*
 * Basic test case.
 */
CTEST2(module_name, test_basic_flow) {}
