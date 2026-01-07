// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once
#include <execinfo.h>

static inline int
platform_backtrace(void **buffer, int size)
{
   return backtrace(buffer, size);
}
