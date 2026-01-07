// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/platform_linux/public_platform.h"
#include "platform_units.h"
#include <time.h>

#pragma once

static inline timestamp
platform_get_timestamp(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return SEC_TO_NSEC(ts.tv_sec) + ts.tv_nsec;
}

static inline timestamp
platform_timestamp_elapsed(timestamp tv)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return SEC_TO_NSEC(ts.tv_sec) + ts.tv_nsec - tv;
}

static inline timestamp
platform_timestamp_diff(timestamp start, timestamp end)
{
   return end - start;
}

static inline timestamp
platform_get_real_time(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_REALTIME, &ts);
   return SEC_TO_NSEC(ts.tv_sec) + ts.tv_nsec;
}
