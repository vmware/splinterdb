// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * SplinterDB - Common header file for example programs.
 */
#ifndef __EXAMPLE_COMMON_H__
#define __EXAMPLE_COMMON_H__

// #define ex_msg(msg, ...) fprintf(stdout, "%s: " msg, APP_ME, "" __VA_ARGS__)
#define ex_msg(msg, ...) fprintf(stdout, "%s: " msg, APP_ME, __VA_ARGS__)

#define ex_err(msg, ...) fprintf(stderr, "%s: Error: " msg, APP_ME, __VA_ARGS__)

/* Useful constants */
#define K_KiB 1024
#define K_MiB (K_KiB * K_KiB)

#ifndef MIN
#   define MIN(a, b) (((a) < (b)) ? (a) : (b))
#endif /* MIN */

#define ARRAY_LEN(a) (int)(sizeof(a) / sizeof(*a))

#endif /* __EXAMPLE_COMMON_H__ */
