// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * io.h --
 *
 *     This file contains the abstract interface for IO.
 */

#pragma once

#include "async.h"
#include "platform_status.h"
#include "platform_string.h"
#include "platform_util.h"
#include "platform_heap.h"
#include "platform_time.h"
#include <fcntl.h>

typedef struct io_handle      io_handle;
typedef struct io_async_req   io_async_req;
typedef struct io_async_state io_async_state;

struct iovec;

/*
 * SplinterDB can be configured with different page-sizes, given by these
 * min & max values.
 */
#define IO_MIN_PAGE_SIZE (4096)
#define IO_MAX_PAGE_SIZE (8192)

#define IO_DEFAULT_PAGE_SIZE        IO_MIN_PAGE_SIZE
#define IO_DEFAULT_PAGES_PER_EXTENT 32
#define IO_DEFAULT_EXTENT_SIZE                                                 \
   (IO_DEFAULT_PAGES_PER_EXTENT * IO_DEFAULT_PAGE_SIZE)

#define IO_DEFAULT_FLAGS             (O_RDWR | O_CREAT)
#define IO_DEFAULT_PERMS             (0600)
#define IO_DEFAULT_KERNEL_QUEUE_SIZE (256)
#define IO_DEFAULT_ASYNC_QUEUE_DEPTH (256)

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
/*
 * Whether a read of device addresses that have never been written should yield
 * zeros instead of failing.  Off by default; see
 * io_permit_unwritten_reads().
 */
typedef void (*io_permit_unwritten_reads_fn)(io_handle *io, bool32 permit);

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
typedef platform_status (*io_durable_barrier_fn)(io_handle *io);
typedef void (*io_register_thread_fn)(io_handle *io);
typedef void (*io_deregister_thread_fn)(io_handle *io);
typedef bool32 (*io_max_latency_elapsed_fn)(io_handle *io, timestamp ts);
typedef void (*io_print_stats_fn)(io_handle *io, platform_log_handle *log);
typedef void (*io_reset_stats_fn)(io_handle *io);

typedef void *(*io_get_context_fn)(io_handle *io);

/*
 * An abstract IO interface, holding different IO Ops function pointers.
 */
typedef struct io_ops {
   io_read_fn                   read;
   io_write_fn                  write;
   io_permit_unwritten_reads_fn permit_unwritten_reads;
   io_async_state_init_fn       async_state_init;
   io_cleanup_fn                cleanup;
   io_wait_all_fn               wait_all;
   io_durable_barrier_fn        durable_barrier;
   io_register_thread_fn        register_thread;
   io_deregister_thread_fn      deregister_thread;
   io_max_latency_elapsed_fn    max_latency_elapsed;
   io_print_stats_fn            print_stats;
   io_reset_stats_fn            reset_stats;
   io_get_context_fn            get_context;
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

static inline platform_status
io_read(io_handle *io, void *buf, uint64 bytes, uint64 addr)
{
   return io->ops->read(io, buf, bytes, addr);
}

/*
 * Permit reads of device addresses that were never written, yielding zeros
 * rather than an error.
 *
 * A device backed by a file is only as long as what has been written to it, so
 * reading an address past that fails outright -- where the same address on a
 * block device would simply read as zeros.  Normal operation only ever reads
 * what it wrote, so that failure is a real error and stays one.  Crash recovery
 * is the exception: it follows on-disk links to find out what exists, and a log
 * page's next-extent link names the extent the allocator had reserved next,
 * which the stream may never have reached.  Recovery has to be able to look
 * there and be told there is nothing, which is what zeros give it -- every
 * reader validates by magic and checksum, so a page of zeros is rejected as a
 * page that was never written.
 *
 * Deliberately not the default.  Outside recovery, a read past the end of the
 * device means a bug or a corrupt address, and turning that into zeros would
 * hide it.  So recovery turns it on for its own duration and off again.
 *
 * Not thread safe, and not meant to be: the caller must have exclusive use of
 * the device, as a mount performing recovery does.
 */
static inline void
io_permit_unwritten_reads(io_handle *io, bool32 permit)
{
   io->ops->permit_unwritten_reads(io, permit);
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

// Wait until we have seen each process's I/O be quiescent.
//
// This means that all I/Os that were in-flight at the start of this call are
// guaranteed to have finished before this call returns.
//
// But I/Os that are issued after this call starts may not be complete when
// this call returns.
static inline void
io_wait_all(io_handle *io)
{
   return io->ops->wait_all(io);
}

/*
 *--------------------------------------------------------------------------
 * io_durable_barrier
 *
 * Ensure that writes which completed before this call survive a power loss.
 * This is deliberately distinct from io_wait_all(), which only waits for
 * asynchronous I/O completion.
 *--------------------------------------------------------------------------
 */
static inline platform_status
io_durable_barrier(io_handle *io)
{
   return io->ops->durable_barrier(io);
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

static inline void
io_print_stats(io_handle *io, platform_log_handle *log_handle)
{
   if (io->ops->print_stats) {
      io->ops->print_stats(io, log_handle);
   }
}

static inline void
io_reset_stats(io_handle *io)
{
   if (io->ops->reset_stats) {
      io->ops->reset_stats(io);
   }
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

platform_status
io_config_valid(io_config *cfg);

platform_status
io_read_bootstrap(const char *filename, void *buf, uint64 bytes, uint64 addr);


io_handle *
io_handle_create(io_config *cfg, platform_heap_id hid);

void
io_handle_destroy(io_handle *ioh);
