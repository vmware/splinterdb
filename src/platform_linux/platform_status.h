// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/platform_linux/public_platform.h"
#include <errno.h>
#include <string.h>

typedef typeof(EINVAL) internal_platform_status;

// Platform status
typedef struct {
   internal_platform_status r;
} platform_status;

#define CONST_STATUS(status) ((const platform_status){.r = status})

// platform status
#define STATUS_OK             CONST_STATUS(0)
#define STATUS_NO_MEMORY      CONST_STATUS(ENOMEM)
#define STATUS_BUSY           CONST_STATUS(EAGAIN)
#define STATUS_LIMIT_EXCEEDED CONST_STATUS(ENOSPC)
#define STATUS_NO_SPACE       CONST_STATUS(ENOSPC)
#define STATUS_TIMEDOUT       CONST_STATUS(ETIMEDOUT)
#define STATUS_NO_PERMISSION  CONST_STATUS(EPERM)
#define STATUS_BAD_PARAM      CONST_STATUS(EINVAL)
#define STATUS_INVALID_STATE  CONST_STATUS(EINVAL)
#define STATUS_NOT_FOUND      CONST_STATUS(ENOENT)
#define STATUS_IO_ERROR       CONST_STATUS(EIO)
#define STATUS_NOTSUP         CONST_STATUS(ENOTSUP)
#define STATUS_TEST_FAILED    CONST_STATUS(-1)

// platform predicates
static inline bool32
STATUS_IS_EQ(const platform_status s1, const platform_status s2)
{
   return s1.r == s2.r;
}

static inline bool32
STATUS_IS_NE(const platform_status s1, const platform_status s2)
{
   return s1.r != s2.r;
}

static inline bool32
SUCCESS(const platform_status s)
{
   return STATUS_IS_EQ(s, STATUS_OK);
}

static inline const char *
platform_status_to_string(const platform_status status)
{
   return strerror(status.r);
}
