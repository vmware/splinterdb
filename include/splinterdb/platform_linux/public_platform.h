// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * public_platform.h --
 *
 *     Minimal common header for both external users (e.g. splinterdb) and
 *     for internal use.
 */

#pragma once

#include <stdio.h>

/*
 * C99 header that provides a set of typedefs that specify exact-width
 * integer types, together with the defined min and max allowable
 * values for each type, using macros.
 *
 * The naming convention for exact-width integer types is intN_t and
 * uintN_t. Ranges are defined using upper case letters, e.g.
 * INT8_MIN, UINT64_MAX.
 *
 * Additionally stdint.h defines limits of integer types capable
 * of holding object pointers such as UINTPTR_MAX, the value of
 * which depends on the processor and its address range.
 * The type and ranges are only included if they exist for the specific
 * compiler/processor.
 */
#include <stdint.h>
#include <stdbool.h>

// Types
typedef unsigned char uchar;
typedef int8_t        int8;
typedef uint8_t       uint8;
typedef int16_t       int16;
typedef uint16_t      uint16;
typedef int32_t       int32;
typedef uint32_t      uint32;
typedef int64_t       int64;
typedef uint64_t      uint64;
typedef uint64        timestamp;
typedef uint64        threadid;

#include <assert.h>
static_assert(sizeof(int8) == 1, "incorrect type");
static_assert(sizeof(uint8) == 1, "incorrect type");
static_assert(sizeof(int16) == 2, "incorrect type");
static_assert(sizeof(uint16) == 2, "incorrect type");
static_assert(sizeof(int32) == 4, "incorrect type");
static_assert(sizeof(uint32) == 4, "incorrect type");
static_assert(sizeof(int64) == 8, "incorrect type");
static_assert(sizeof(uint64) == 8, "incorrect type");

// Bools
#ifndef TRUE
#   define TRUE (1)
#endif

#ifndef FALSE
#   define FALSE (0)
#endif

typedef FILE platform_log_handle;

// By default, info messages sent from platform_default_log() go to /dev/null
// and error messages sent from platform_error_log() go to stderr.
//
// Use platform_set_log_streams() to send those log messages elsewhere.
//
// For example, to send info messages to stdout and errors to stderr, run:
//    platform_set_log_streams(stdout, stderr);
void
platform_set_log_streams(platform_log_handle *info_stream,
                         platform_log_handle *error_stream);
