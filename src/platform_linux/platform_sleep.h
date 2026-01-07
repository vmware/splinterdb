// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/platform_linux/public_platform.h"
#include "platform_units.h"
#include <time.h>

static inline void
platform_pause()
{
#if defined(__i386__) || defined(__x86_64__)
   __builtin_ia32_pause();
#elif defined(__aarch64__) // ARM64
   // pause + memory fence for x64 and ARM
   // https://chromium.googlesource.com/chromium/src/third_party/WebKit/Source/wtf/+/823d62cdecdbd5f161634177e130e5ac01eb7b48/SpinLock.cpp
   __asm__ __volatile__("yield");
#else
#   error Unknown CPU arch
#endif
}

static inline void
platform_sleep_ns(uint64 ns)
{
   if (ns < USEC_TO_NSEC(50)) {
      for (uint64 i = 0; i < ns / 5 + 1; i++) {
         platform_pause();
      }
   } else {
      struct timespec res;
      res.tv_sec  = ns / SEC_TO_NSEC(1);
      res.tv_nsec = (ns - (res.tv_sec * SEC_TO_NSEC(1)));
      clock_nanosleep(CLOCK_MONOTONIC, 0, &res, NULL);
   }
}

static inline void
platform_yield()
{
}
