// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform_io.h"
#include "laio.h"
#include "platform_log.h"
#include <errno.h>
#include <string.h>
#include <unistd.h>
#if defined(__has_feature)
#   if __has_feature(memory_sanitizer)
#      include <sanitizer/msan_interface.h>
#   endif
#endif

io_handle *
io_handle_create(io_config *cfg, platform_heap_id hid)
{
   return laio_handle_create(cfg, hid);
}

void
io_handle_destroy(io_handle *ioh)
{
   laio_handle_destroy(ioh);
}

platform_status
io_config_valid(io_config *cfg)
{
   return laio_config_valid(cfg);
}

platform_status
io_read_bootstrap(const char *filename, void *buf, uint64 bytes, uint64 addr)
{
   if (filename == NULL || buf == NULL) {
      return STATUS_BAD_PARAM;
   }

   if (bytes == 0) {
      return STATUS_OK;
   }

   int open_flags = O_RDONLY;
#ifdef O_CLOEXEC
   open_flags |= O_CLOEXEC;
#endif
#ifdef O_LARGEFILE
   open_flags |= O_LARGEFILE;
#endif

   int fd = open(filename, open_flags);
   if (fd == -1) {
      int saved_errno = errno;
      platform_error_log(
         "open() '%s' failed: %s\n", filename, strerror(saved_errno));
      return CONST_STATUS(saved_errno);
   }

   uint64 bytes_read = 0;
   while (bytes_read < bytes) {
      ssize_t ret = pread(
         fd, (char *)buf + bytes_read, bytes - bytes_read, addr + bytes_read);
      if (ret == -1) {
         if (errno == EINTR) {
            continue;
         }
         int saved_errno = errno;
         platform_error_log(
            "io_read_bootstrap: pread failed for addr %lu, bytes %lu, "
            "ret %ld: %s\n",
            addr + bytes_read,
            bytes - bytes_read,
            (long)ret,
            strerror(saved_errno));
         close(fd);
         return CONST_STATUS(saved_errno);
      }
      if (ret == 0) {
         platform_error_log(
            "io_read_bootstrap: short read for addr %lu, bytes %lu\n",
            addr + bytes_read,
            bytes - bytes_read);
         close(fd);
         return STATUS_IO_ERROR;
      }
      bytes_read += ret;
   }

   if (close(fd) != 0) {
      int saved_errno = errno;
      platform_error_log(
         "close() '%s' failed: %s\n", filename, strerror(saved_errno));
      return CONST_STATUS(saved_errno);
   }

   return STATUS_OK;
}
