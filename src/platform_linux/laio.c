// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * laio.c --
 *
 *     This file contains the implementation for a libaio wrapper.
 *
 * The external callable interfaces are defined in io.h. This module
 * supports both synchronous and async IO.
 *
 * - Sync  IO interfaces: io_read(), io_write()
 * - Async IO interfaces: io_read_async(), io_write_async()
 * - Async IO completion interfaces: io_cleanup(), io_cleanup_all()
 * - The Async IO functions require obtaining an io_async_req via
 *   laio_get_async_req(), followed by filling in its metadata and iovec
 *   members using laio_get_metadata() and laio_get_iovec().
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

static platform_status
laio_read(io_handle *ioh, void *buf, uint64 bytes, uint64 addr);

static platform_status
laio_write(io_handle *ioh, void *buf, uint64 bytes, uint64 addr);

static io_async_req *
laio_get_async_req(io_handle *ioh, bool32 blocking);

struct iovec *
laio_get_iovec(io_handle *ioh, io_async_req *req);

static void *
laio_get_metadata(io_handle *ioh, io_async_req *req);

static void *
laio_get_context(io_handle *ioh);

static platform_status
laio_read_async(io_handle     *ioh,
                io_async_req  *req,
                io_callback_fn callback,
                uint64         count,
                uint64         addr);

static platform_status
laio_write_async(io_handle     *ioh,
                 io_async_req  *req,
                 io_callback_fn callback,
                 uint64         count,
                 uint64         addr);

static void
laio_cleanup(io_handle *ioh, uint64 count);

static void
laio_cleanup_all(io_handle *ioh);

static io_async_req *
laio_get_kth_req(laio_handle *io, uint64 k);

/*
 * Define an implementation of the abstract IO Ops interface methods.
 */
static io_ops laio_ops = {
   .read          = laio_read,
   .write         = laio_write,
   .get_iovec     = laio_get_iovec,
   .get_async_req = laio_get_async_req,
   .get_metadata  = laio_get_metadata,
   .read_async    = laio_read_async,
   .write_async   = laio_write_async,
   .cleanup       = laio_cleanup,
   .cleanup_all   = laio_cleanup_all,
   .get_context   = laio_get_context,
};

/*
 * Given an IO configuration, validate it. Allocate memory for various
 * structures and initialize the IO sub-system.
 */
platform_status
io_handle_init(laio_handle         *io,
               io_config           *cfg,
               platform_heap_handle hh,
               platform_heap_id     hid)
{
   int           status;
   uint64        req_size;
   uint64        total_req_size;
   uint64        i, j;
   io_async_req *req;

   // Validate IO-configuration parameters
   platform_status rc = laio_config_valid(cfg);
   if (!SUCCESS(rc)) {
      return rc;
   }

   platform_assert(cfg->async_queue_size % LAIO_HAND_BATCH_SIZE == 0);
   memset(io, 0, sizeof(*io));
   io->super.ops = &laio_ops;
   io->cfg       = cfg;
   io->heap_id   = hid;

   status = io_setup(cfg->kernel_queue_size, &io->ctx);
   platform_assert(status == 0);

   bool32 is_create = ((cfg->flags & O_CREAT) != 0);
   if (is_create) {
      io->fd = open(cfg->filename, cfg->flags, cfg->perms);
   } else {
      io->fd = open(cfg->filename, cfg->flags);
   }
   if (io->fd == -1) {
      platform_error_log(
         "open() '%s' failed: %s\n", cfg->filename, strerror(errno));
      return CONST_STATUS(errno);
   }

   if (is_create) {
      fallocate(io->fd, 0, 0, 128 * 1024);
   }

   /*
    * Allocate memory for an array of async_queue_size Async request
    * structures. Each request struct nests within it async_max_pages
    * pages on which IO can be outstanding.
    */
   req_size =
      sizeof(io_async_req) + cfg->async_max_pages * sizeof(struct iovec);
   total_req_size = req_size * cfg->async_queue_size;
   io->req        = TYPED_MANUAL_ZALLOC(io->heap_id, io->req, total_req_size);
   platform_assert((io->req != NULL),
                   "Failed to allocate memory for array of %lu Async IO"
                   " request structures, for %ld outstanding IOs on pages.",
                   cfg->async_queue_size,
                   cfg->async_max_pages);

   // Initialize each Async IO request structure
   for (i = 0; i < cfg->async_queue_size; i++) {
      req         = laio_get_kth_req(io, i);
      req->iocb_p = &req->iocb;
      req->number = i;
      req->busy   = FALSE;
      for (j = 0; j < cfg->async_max_pages; j++)
         req->iovec[j].iov_len = cfg->page_size;
   }
   io->max_batches_nonblocking_get =
      cfg->async_queue_size / LAIO_HAND_BATCH_SIZE;

   // leave req_hand set to 0
   return STATUS_OK;
}

/*
 * Dismantle the handle for the IO sub-system, close file and release memory.
 */
void
io_handle_deinit(laio_handle *io)
{
   int status;

   status = io_destroy(io->ctx);
   if (status != 0)
      platform_error_log(
         "io_destroy failed with error=%d: %s\n", -status, strerror(-status));
   platform_assert(status == 0);

   status = close(io->fd);
   if (status != 0) {
      platform_error_log("close failed, status=%d, with error %d: %s\n",
                         status,
                         errno,
                         strerror(errno));
   }
   platform_assert(status == 0);

   platform_free(io->heap_id, io->req);
}

/*
 * laio_read() - Basically a wrapper around pread().
 */
static platform_status
laio_read(io_handle *ioh, void *buf, uint64 bytes, uint64 addr)
{
   laio_handle *io;
   int          ret;

   io  = (laio_handle *)ioh;
   ret = pread(io->fd, buf, bytes, addr);
   if (ret == bytes) {
      return STATUS_OK;
   }
   return STATUS_IO_ERROR;
}

/*
 * laio_write() - Basically a wrapper around pwrite().
 */
static platform_status
laio_write(io_handle *ioh, void *buf, uint64 bytes, uint64 addr)
{
   laio_handle *io;
   int          ret;

   io  = (laio_handle *)ioh;
   ret = pwrite(io->fd, buf, bytes, addr);
   if (ret == bytes) {
      return STATUS_OK;
   }
   return STATUS_IO_ERROR;
}

/*
 * Return a ptr to the k'th Async IO request structure, accounting
 * for a nested array of 'async_max_pages' pages of IO vector structures
 * at the end of each Async IO request structure.
 */
static io_async_req *
laio_get_kth_req(laio_handle *io, uint64 k)
{
   char  *cursor;
   uint64 req_size;

   req_size =
      sizeof(io_async_req) + io->cfg->async_max_pages * sizeof(struct iovec);
   cursor = (char *)io->req;
   return (io_async_req *)(cursor + k * req_size);
}

/*
 * laio_get_async_req() - Return an Async IO request structure for this thread.
 */
static io_async_req *
laio_get_async_req(io_handle *ioh, bool32 blocking)
{
   laio_handle   *io;
   io_async_req  *req;
   uint64         batches = 0;
   const threadid tid     = platform_get_tid();

   io = (laio_handle *)ioh;
   debug_assert(tid < MAX_THREADS, "Invalid tid=%lu", tid);
   while (1) {
      if (io->req_hand[tid] % LAIO_HAND_BATCH_SIZE == 0) {
         if (!blocking && batches++ >= io->max_batches_nonblocking_get) {
            return NULL;
         }
         io->req_hand[tid] = __sync_fetch_and_add(&io->req_hand_base, 32)
                             % io->cfg->async_queue_size;
         laio_cleanup(ioh, 0);
      }
      req = laio_get_kth_req(io, io->req_hand[tid]++);
      if (__sync_bool_compare_and_swap(&req->busy, FALSE, TRUE)) {
         return req;
      }
   }
   // should not get here
   platform_assert(0,
                   "Could not find a free Async IO request structure"
                   " for thread ID=%lu\n",
                   tid);
   return NULL;
}

struct iovec *
laio_get_iovec(io_handle *ioh, io_async_req *req)
{
   return req->iovec;
}

static void *
laio_get_metadata(io_handle *ioh, io_async_req *req)
{
   return req->metadata;
}

static void *
laio_get_context(io_handle *ioh)
{
   return ((laio_handle *)ioh)->ctx;
}

void
laio_callback(io_context_t ctx, struct iocb *iocb, long res, long res2)
{
   io_async_req   *req;
   platform_status status = STATUS_OK;

   platform_assert(res2 == 0);
   req = (io_async_req *)((char *)iocb - offsetof(io_async_req, iocb));
   req->callback(req->metadata, req->iovec, req->count, status);
   req->busy = FALSE;
}

/*
 * io_read_async() - Submit an Async read request. Async request 'req' needs
 * to have its eq->metadata and req->iovec filled in for the IO to work.
 */
static platform_status
laio_read_async(io_handle     *ioh,
                io_async_req  *req,
                io_callback_fn callback,
                uint64         count,
                uint64         addr)
{
   laio_handle *io;
   int          status;

   io = (laio_handle *)ioh;
   io_prep_preadv(&req->iocb, io->fd, req->iovec, count, addr);
   req->callback = callback;
   req->count    = count;
   io_set_callback(&req->iocb, laio_callback);
   do {
      status = io_submit(io->ctx, 1, &req->iocb_p);
      if (status < 0) {
         platform_error_log("io_submit error %s\n", strerror(-status));
      }
      io_cleanup(ioh, 0);
   } while (status != 1);

   return STATUS_OK;
}

/*
 * laio_write_async() - Submit an Async write request.
 */
static platform_status
laio_write_async(io_handle     *ioh,
                 io_async_req  *req,
                 io_callback_fn callback,
                 uint64         count,
                 uint64         addr)
{
   laio_handle *io;
   int          status;

   io = (laio_handle *)ioh;
   io_prep_pwritev(&req->iocb, io->fd, req->iovec, count, addr);
   req->callback = callback;
   req->count    = count;
   io_set_callback(&req->iocb, laio_callback);
   do {
      status = io_submit(io->ctx, 1, &req->iocb_p);
      if (status < 0) {
         platform_error_log("io_submit error %s\n", strerror(-status));
      }
      io_cleanup(ioh, 0);
   } while (status != 1);

   return STATUS_OK;
}

/*
 * laio_cleanup() - Handle completion of outstanding IO requests.
 * Up to 'count' outstanding IO requests will be processed.
 * Specify 'count' as 0 to process completion of all pending IO requests.
 */
static void
laio_cleanup(io_handle *ioh, uint64 count)
{
   laio_handle    *io;
   struct io_event event = {0};
   uint64          i;
   int             status;

   io = (laio_handle *)ioh;

   // Check for completion of up to 'count' events, one event at a time.
   // Or, check for all outstanding events (count == 0)
   for (i = 0; ((count == 0) || (i < count)); i++) {
      status = io_getevents(io->ctx, 0, 1, &event, NULL);
      if (status < 0) {
         platform_error_log("io_getevents[%lu], count=%lu, "
                            "failed with errorno=%d: %s\n",
                            i,
                            count,
                            -status,
                            strerror(-status));
         i--;
         continue;
      }
      // No event has completed, so we are done. Exit.
      if (status == 0) {
         break;
      }
      // Invoke the callback for the one event that completed.
      laio_callback(io->ctx, event.obj, event.res, 0);
   }
}

/*
 * laio_cleanup_all() - Handle completion of outstanding IO requests,
 * for all async requests in the queue.
 */
static void
laio_cleanup_all(io_handle *ioh)
{
   laio_handle  *io;
   uint64        i;
   io_async_req *req;

   io = (laio_handle *)ioh;
   for (i = 0; i < io->cfg->async_queue_size; i++) {
      req = laio_get_kth_req(io, i);
      while (req->busy) {
         io_cleanup(ioh, 0);
      }
   }
}

static inline bool32
laio_config_valid_page_size(io_config *cfg)
{
   return (cfg->page_size == LAIO_DEFAULT_PAGE_SIZE);
}

static inline bool32
laio_config_valid_extent_size(io_config *cfg)
{
   return (cfg->extent_size == LAIO_DEFAULT_EXTENT_SIZE);
}

/*
 * Do basic validation of IO configuration so we don't have to deal
 * with unsupported configurations that may creep through there.
 */
platform_status
laio_config_valid(io_config *cfg)
{
   if (!laio_config_valid_page_size(cfg)) {
      platform_error_log(
         "Page-size, %lu bytes, is an invalid IO configuration.\n",
         cfg->page_size);
      return STATUS_BAD_PARAM;
   }
   if (!laio_config_valid_extent_size(cfg)) {
      platform_error_log(
         "Extent-size, %lu bytes, is an invalid IO configuration.\n",
         cfg->extent_size);
      return STATUS_BAD_PARAM;
   }
   return STATUS_OK;
}
