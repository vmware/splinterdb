// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * platform_random.h --
 *
 *      Operating-system entropy for persistent random identifiers.
 */

#pragma once

#include "platform_status.h"

#include <errno.h>
#include <stddef.h>
#include <sys/random.h>

/*
 * Fill buf from the kernel CSPRNG.  Short reads and EINTR are normal and are
 * handled here so callers either receive every requested byte or an error.
 */
static inline platform_status
platform_random_bytes(void *buf, size_t length)
{
   char *cursor = buf;

   while (length != 0) {
      ssize_t got = getrandom(cursor, length, 0);
      if (got < 0 && errno == EINTR) {
         continue;
      }
      if (got <= 0) {
         return STATUS_IO_ERROR;
      }
      cursor += got;
      length -= got;
   }

   return STATUS_OK;
}
