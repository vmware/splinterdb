// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*

SplinterDB uses a platform layer to abstract away non-portable definitions
and functionality.

For a platform X, the public API headers live at
   include/splinterdb/platform_X/public_platform.h
and the internal headers are at
   src/platform_X

The external definitions include:
- uint64, which is used for buffer sizes, etc.
- bool, typedef'd to int32 on linux

Programs linking against SplinterDB should define SPLINTERDB_PLATFORM_DIR
before including any SplinterDB headers.  For example, to set it at
compile time for the linux platform, you might
  cc -DSPLINTERDB_PLATFORM_DIR=platform_linux -I splinterdb-src/include ...

*/

#ifndef __SPLINTERDB_PUBLIC_PLATFORM_H
#define __SPLINTERDB_PUBLIC_PLATFORM_H

#ifndef SPLINTERDB_PLATFORM_DIR
#   error Define SPLINTERDB_PLATFORM_DIR for your target, e.g. compile with flag -DSPLINTERDB_PLATFORM_DIR=platform_linux
#endif

// Build an include path from the PLATFORM_DIR define
// see: https://gcc.gnu.org/onlinedocs/cpp/Stringizing.html
#define TEMP_XSTRINGIFY(s) TEMP_STRINGIFY(s)
#define TEMP_STRINGIFY(s)  #s
// clang-format off
#define PUBLIC_PLATFORM_H TEMP_XSTRINGIFY(splinterdb/SPLINTERDB_PLATFORM_DIR/public_platform.h)
// clang-format on

#include PUBLIC_PLATFORM_H

#undef PUBLIC_PLATFORM_H
#undef TEMP_STRINGIFY
#undef TEMP_XSTRINGIFY

#endif // __SPLINTERDB_PUBLIC_PLATFORM_H
