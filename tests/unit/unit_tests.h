// Copyright 2021-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0
/*
 * unit_tests.h: SplinterDB Unit tests common header file.
 *  Define things in here that are going to be needed across most unit tests.
 */

#include "ctest.h"

#define Kilo (1024UL)
#define Mega (1024UL * Kilo)
#define Giga (1024UL * Mega)

void
set_log_streams_for_tests(msg_level exp_msg_level);
