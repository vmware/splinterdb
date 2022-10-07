// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * io.h --
 *
 *     This file contains the abstract interface for IO.
 */

#ifndef __IO_H
#define __IO_H

#include "platform.h"

typedef struct io_handle    io_handle;
typedef struct io_async_req io_async_req;

/*
 * IO Configuration structure - used to setup the run-time IO system.
 */
typedef struct io_config {
   uint64 async_queue_size;
   uint64 kernel_queue_size;
   uint64 async_max_pages;
   uint64 page_size;
   uint64 extent_size;
   char   filename[MAX_STRING_LENGTH];
   int    flags;
   uint32 perms;
} io_config;

typedef void (*io_callback_fn)(void           *metadata,
                               struct iovec   *iovec,
                               uint64          count,
                               platform_status status);

typedef platform_status (*io_read_fn)(io_handle *io,
                                      void      *buf,
                                      uint64     bytes,
                                      uint64     addr);
typedef platform_status (*io_write_fn)(io_handle *io,
                                       void      *buf,
                                       uint64     bytes,
                                       uint64     addr);
typedef io_async_req *(*io_get_async_req_fn)(io_handle *io, bool blocking);
typedef struct iovec *(*io_get_iovec_fn)(io_handle *io, io_async_req *req);
typedef void *(*io_get_metadata_fn)(io_handle *io, io_async_req *req);
typedef platform_status (*io_read_async_fn)(io_handle     *io,
                                            io_async_req  *req,
                                            io_callback_fn callback,
                                            uint64         count,
                                            uint64         addr);
typedef platform_status (*io_write_async_fn)(io_handle     *io,
                                             io_async_req  *req,
                                             io_callback_fn callback,
                                             uint64         count,
                                             uint64         addr);
typedef void (*io_cleanup_fn)(io_handle *io, uint64 count);
typedef void (*io_cleanup_all_fn)(io_handle *io);
typedef void (*io_thread_register_fn)(io_handle *io);
typedef bool (*io_max_latency_elapsed_fn)(io_handle *io, timestamp ts);

/*
 * An abstract IO interface, holding different IO Ops function pointers.
 */
typedef struct io_ops {
   io_read_fn                read;
   io_write_fn               write;
   io_get_async_req_fn       get_async_req;
   io_get_iovec_fn           get_iovec;
   io_get_metadata_fn        get_metadata;
   io_read_async_fn          read_async;
   io_write_async_fn         write_async;
   io_cleanup_fn             cleanup;
   io_cleanup_all_fn         cleanup_all;
   io_thread_register_fn     thread_register;
   io_max_latency_elapsed_fn max_latency_elapsed;
} io_ops;

// to sub-class io, make an io your first field;
struct io_handle {
   const io_ops *ops;
   int           nbytes_rw; // # of bytes read / written by IO call.
};

platform_status
io_handle_init(platform_io_handle  *ioh,
               io_config           *cfg,
               platform_heap_handle hh,
               platform_heap_id     hid);

void
io_handle_deinit(platform_io_handle *ioh);

static inline platform_status
io_read(io_handle *io, void *buf, uint64 bytes, uint64 addr)
{
   return io->ops->read(io, buf, bytes, addr);
}

static inline platform_status
io_write(io_handle *io, void *buf, uint64 bytes, uint64 addr)
{
   return io->ops->write(io, buf, bytes, addr);
}

static inline io_async_req *
io_get_async_req(io_handle *io, bool blocking)
{
   return io->ops->get_async_req(io, blocking);
}

static inline struct iovec *
io_get_iovec(io_handle *io, io_async_req *req)
{
   return io->ops->get_iovec(io, req);
}

static inline void *
io_get_metadata(io_handle *io, io_async_req *req)
{
   return io->ops->get_metadata(io, req);
}

static inline platform_status
io_read_async(io_handle     *io,
              io_async_req  *req,
              io_callback_fn callback,
              uint64         count,
              uint64         addr)
{
   return io->ops->read_async(io, req, callback, count, addr);
}

static inline platform_status
io_write_async(io_handle     *io,
               io_async_req  *req,
               io_callback_fn callback,
               uint64         count,
               uint64         addr)
{
   return io->ops->write_async(io, req, callback, count, addr);
}

static inline void
io_cleanup(io_handle *io, uint64 count)
{
   return io->ops->cleanup(io, count);
}

// Guarantees all in-flight IOs are complete before return
static inline void
io_cleanup_all(io_handle *io)
{
   return io->ops->cleanup_all(io);
}

static inline void
io_thread_register(io_handle *io)
{
   if (io->ops->thread_register) {
      return io->ops->thread_register(io);
   }
}

static inline bool
io_max_latency_elapsed(io_handle *io, timestamp ts)
{
   if (io->ops->max_latency_elapsed) {
      return io->ops->max_latency_elapsed(io, ts);
   }
   return TRUE;
}

/*
 *-----------------------------------------------------------------------------
 * io_config_init --
 *
 *      Initialize io config values
 *
 *  (Stores a copy of io_filename to io_cfg->filename, so the caller may
 *  deallocate io_filename once this returns)
 *-----------------------------------------------------------------------------
 */
static inline void
io_config_init(io_config  *io_cfg,
               uint64      page_size,
               uint64      extent_size,
               int         flags,
               uint32      perms,
               uint64      async_queue_depth,
               const char *io_filename)
{
   ZERO_CONTENTS(io_cfg);

   io_cfg->page_size   = page_size;
   io_cfg->extent_size = extent_size;

   int rc = snprintf(io_cfg->filename, MAX_STRING_LENGTH, "%s", io_filename);
   platform_assert(rc < MAX_STRING_LENGTH);

   io_cfg->flags             = flags;
   io_cfg->perms             = perms;
   io_cfg->async_queue_size  = async_queue_depth;
   io_cfg->kernel_queue_size = async_queue_depth;
   io_cfg->async_max_pages   = extent_size / page_size;
}

#endif //__IO_H
