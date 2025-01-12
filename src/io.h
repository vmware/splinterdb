// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * io.h --
 *
 *     This file contains the abstract interface for IO.
 */

#pragma once

#include "async.h"
#include "platform.h"

typedef struct io_handle      io_handle;
typedef struct io_async_req   io_async_req;
typedef struct io_async_state io_async_state;

/*
 * IO Configuration structure - used to setup the run-time IO system.
 */
typedef struct io_config {
   uint64 kernel_queue_size;
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

#define IO_ASYNC_STATE_BUFFER_SIZE (1024)
typedef uint8 io_async_state_buffer[IO_ASYNC_STATE_BUFFER_SIZE];
typedef enum { io_async_preadv, io_async_pwritev } io_async_cmd;
typedef platform_status (*io_async_state_init_fn)(io_async_state   *state,
                                                  io_handle        *io,
                                                  io_async_cmd      cmd,
                                                  uint64            addr,
                                                  async_callback_fn callback,
                                                  void *callback_arg);

typedef void (*io_cleanup_fn)(io_handle *io, uint64 count);
typedef void (*io_wait_all_fn)(io_handle *io);
typedef void (*io_register_thread_fn)(io_handle *io);
typedef void (*io_deregister_thread_fn)(io_handle *io);
typedef bool32 (*io_max_latency_elapsed_fn)(io_handle *io, timestamp ts);

typedef void *(*io_get_context_fn)(io_handle *io);

/*
 * An abstract IO interface, holding different IO Ops function pointers.
 */
typedef struct io_ops {
   io_read_fn                read;
   io_write_fn               write;
   io_async_state_init_fn    async_state_init;
   io_cleanup_fn             cleanup;
   io_wait_all_fn            wait_all;
   io_register_thread_fn     register_thread;
   io_deregister_thread_fn   deregister_thread;
   io_max_latency_elapsed_fn max_latency_elapsed;
   io_get_context_fn         get_context;
} io_ops;

/*
 * To sub-class io, make an io your first field;
 */
struct io_handle {
   const io_ops *ops;
};

typedef void (*io_async_state_deinit_fn)(io_async_state *state);
typedef platform_status (*io_async_state_append_page_fn)(io_async_state *state,
                                                         void           *buf);
typedef const struct iovec *(
   *io_async_state_get_iovec_fn)(io_async_state *state, uint64 *iovlen);
typedef async_status (*io_async_io_fn)(io_async_state *state);

typedef platform_status (*io_async_state_get_result_fn)(io_async_state *state);

typedef struct io_async_state_ops {
   io_async_state_append_page_fn append_page;
   io_async_io_fn                run;
   io_async_state_get_result_fn  get_result;
   io_async_state_get_iovec_fn   get_iovec;
   io_async_state_deinit_fn      deinit;
} io_async_state_ops;

struct io_async_state {
   const io_async_state_ops *ops;
};

platform_status
io_handle_init(platform_io_handle *ioh, io_config *cfg, platform_heap_id hid);

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

static inline platform_status
io_async_state_init(io_async_state_buffer buffer,
                    io_handle            *io,
                    io_async_cmd          cmd,
                    uint64                addr,
                    async_callback_fn     callback,
                    void                 *callback_arg)
{
   io_async_state *state = (io_async_state *)buffer;
   return io->ops->async_state_init(
      state, io, cmd, addr, callback, callback_arg);
}

static inline void
io_async_state_deinit(io_async_state_buffer buffer)
{
   io_async_state *state = (io_async_state *)buffer;
   return state->ops->deinit(state);
}

static inline platform_status
io_async_state_append_page(io_async_state_buffer buffer, void *buf)
{
   io_async_state *state = (io_async_state *)buffer;
   return state->ops->append_page(state, buf);
}

static inline const struct iovec *
io_async_state_get_iovec(io_async_state_buffer buffer, uint64 *iovlen)
{
   io_async_state *state = (io_async_state *)buffer;
   return state->ops->get_iovec(state, iovlen);
}

static inline async_status
io_async_run(io_async_state_buffer buffer)
{
   io_async_state *state = (io_async_state *)buffer;
   return state->ops->run(state);
}

static inline platform_status
io_async_state_get_result(io_async_state_buffer buffer)
{
   io_async_state *state = (io_async_state *)buffer;
   return state->ops->get_result(state);
}

static inline void
io_cleanup(io_handle *io, uint64 count)
{
   return io->ops->cleanup(io, count);
}

// Guarantees all in-flight IOs are complete before return
static inline void
io_wait_all(io_handle *io)
{
   return io->ops->wait_all(io);
}

static inline void
io_register_thread(io_handle *io)
{
   if (io->ops->register_thread) {
      return io->ops->register_thread(io);
   }
}

static inline void
io_deregister_thread(io_handle *io)
{
   if (io->ops->deregister_thread) {
      return io->ops->deregister_thread(io);
   }
}

static inline bool32
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
   io_cfg->kernel_queue_size = async_queue_depth;
}
