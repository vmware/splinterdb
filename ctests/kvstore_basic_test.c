// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * kvstore_basic_test.c --
 *
 *     Exercises the kvstore_basic API, which exposes keys & values
 *     instead of the keys & messages of the lower layers.
 *
 *     This test code can be easily modified to be an example of a standalone
 *     program that integrates with SplinterDB.
 *
 *     To compile this into a standalone program, just rename the function
 *     kvstore_basic_test() to be main(), and ensure you've got the
 *     kvstore_basic.h header and libsplinterdb.a available for linking.
 *
 *     $ cc -L splinterdb/lib -I splinterdb/include \
 *          my_program.c -lsplinterdb -lxxhash -laio -lpthread -lm
 *
 * NOTE: This test case file also serves as an example for how-to build
 *  CTests, and the syntax for different commands etc. Note the
 *  annotations to learn how to write new unit-tests using Ctests.
 *
 * Naming Conventions:
 *
 *  o The file containing unit-test cases for a module / functionality is
 *    expected to be named <something>_test.c
 *
 *  o Individual test cases [ see below ] in a file are prefaced with a
 *    term naming the test suite, for the module / functionality being tested.
 *    Usually it will just be <something>; i.e. in kvstore_basic_test.c
 *    the suite-name is 'kvstore_basic'.
 *
 *  o Each test case should be named <operation>_test
 */
#include <stdlib.h>     // Include this if you need system calls; e.g. free
#include "ctest.h"      // This is required for all test-case files.

/*
 * All tests in this file are named with one term, which represents the
 * module / functionality you are testing. Here, it is: kvstore_basic
 *
 * This is an individual test case, testing [usually] just one thing.
 * The 2nd term is the test-case name. Here, just: 'empty_test'
 */
CTEST(kvstore_basic, empty_test) {}

/*
 * A test suite with a setup/teardown function
 * This is converted into a struct that's automatically passed to all tests
 * in the test suite.
 */
CTEST_DATA(kvstore_basic)
{
   // unsigned char *buffer;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(kvstore_basic)
{
    /*
   CTEST_LOG(
      "%s() data=%p buffer=%p", __func__, (void *)data, (void *)data->buffer);
   data->buffer = (unsigned char *)malloc(1024);
   */
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(kvstore_basic)
{
   /*
   CTEST_LOG(
      "%s() data=%p buffer=%p", __func__, (void *)data, (void *)data->buffer);
   if (data->buffer)
      free(data->buffer);
   */
}

CTEST2(kvstore_basic, test1)
{
   (void)data;
   CTEST_LOG("%s()", __func__);
}
