// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef __LIMITS_H__
#define __LIMITS_H__

// MAX_KEY_SIZE is the limit, in bytes, on the length of *raw* keys
// inserted into the trunk and branches.
//
// It must be at least 8, and no more than 105.
//
// When using the splinterdb.h API, the key size limit is MAX_KEY_SIZE-1
// to accomodate the variable-length encoding.
// See include/splinterdb/splinterdb.h for details.
//
// If your keys are always shorter than 105 bytes, you may reduce MAX_KEY_SIZE.
//
// To modify this at build time without editing this file, set it via
// the CFLAGS env var, e.g.
//   make clean && CFLAGS='-DMAX_KEY_SIZE=24' make
//
// Tests and example programs use test-data with keys of varying sizes.
// When building with a smaller MAX_KEY_SIZE, expect some breakage.
#ifndef MAX_KEY_SIZE
#   define MAX_KEY_SIZE 80
#endif // MAX_KEY_SIZE

#endif // __LIMITS_H__
