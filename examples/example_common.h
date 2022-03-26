// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * SplinterDB - Common header file for example programs.
 */
#ifndef __EXAMPLE_COMMON_H__
#define __EXAMPLE_COMMON_H__

#include <time.h>

/* Wrapper macros to print msg (w/args) to stdout / stderr */
#define ex_msg(msg, ...) fprintf(stdout, "%s: " msg, APP_ME, __VA_ARGS__)

#define ex_err(msg, ...) fprintf(stderr, "%s: Error: " msg, APP_ME, __VA_ARGS__)

/* Useful constants */
#define K_KiB 1024
#define K_MiB (K_KiB * K_KiB)

#ifndef MIN
#   define MIN(a, b) (((a) < (b)) ? (a) : (b))
#endif /* MIN */

#define ARRAY_LEN(a) (int)(sizeof(a) / sizeof(*a))

// Time unit constants
#define THOUSAND (1000UL)
#define MILLION  (THOUSAND * THOUSAND)
#define BILLION  (THOUSAND * MILLION)

#define NSEC_TO_MSEC(x) ((x) / MILLION)
#define SEC_TO_NSEC(x)  ((x)*BILLION)

/* Convert timespec quantity into units of nanoseconds */
#define TIMESPEC_TO_NS(ts) ((uint64)SEC_TO_NSEC((ts)->tv_sec) + (ts)->tv_nsec)

#endif /* __EXAMPLE_COMMON_H__ */
