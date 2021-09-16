// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/* ---------------------------------------------------------------------------
 * laio.c --
 *
 *  This file contains the implementation for a Mac/OSX libaio wrapper.
 *  Based off of reference implementation in platform_linux/laio.c
 * ---------------------------------------------------------------------------
 */
#define POISON_FROM_PLATFORM_IMPLEMENTATION
#include "platform.h"

#include "laio.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#define LAIO_HAND_BATCH_SIZE 32

platform_status laio_read          (io_handle *ioh, void *buf, uint64 bytes, uint64 addr);

static io_ops laio_ops = {
   .read          = laio_read,
   /*
   .write         = laio_write,
   .get_iovec     = laio_get_iovec,
   .get_async_req = laio_get_async_req,
   .get_metadata  = laio_get_metadata,
   .read_async    = laio_read_async,
   .write_async   = laio_write_async,
   .cleanup       = laio_cleanup,
   .cleanup_all   = laio_cleanup_all,
   */
};

platform_status
laio_read(io_handle *ioh,
          void      *buf,
          uint64     bytes,
          uint64     addr)
{
   laio_handle *io;
   int ret;

   io = (laio_handle *)ioh;
   ret = pread(io->fd, buf, bytes, addr);
   if (ret == bytes) {
      return STATUS_OK;
   }
   return STATUS_IO_ERROR;
}
