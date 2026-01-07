// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/platform_linux/public_platform.h"

#define PLATFORM_CACHELINE_SIZE 64
#define PLATFORM_CACHELINE_ALIGNED                                             \
   __attribute__((__aligned__(PLATFORM_CACHELINE_SIZE)))

typedef struct {
   uint32 v;
} PLATFORM_CACHELINE_ALIGNED cache_aligned_uint32;


_Static_assert(sizeof(cache_aligned_uint32) == PLATFORM_CACHELINE_SIZE,
               "Attribute set wrong");
