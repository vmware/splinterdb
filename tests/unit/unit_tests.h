// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0
/*
 * unit_tests.h: SplinterDB Unit tests common header file.
 *  Define things in here that are going to be needed across most unit tests.
 */

#include "splinterdb/public_platform.h"
#include "ctest.h"

/* Name of SplinterDB device created for unit-tests */
#define TEST_DB_NAME "splinterdb_unit_tests_db"

#define Kilo (1024UL)
#define Mega (1024UL * Kilo)
#define Giga (1024UL * Mega)

void
set_log_streams_for_tests(msg_level exp_msg_level);
