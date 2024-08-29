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
#include <errno.h>
#include <string.h>

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
laio_wait_all(io_handle *ioh);

static void
laio_register_thread(io_handle *ioh);

static void
laio_deregister_thread(io_handle *ioh);

static io_async_req *
laio_get_kth_req(laio_handle *io, uint64 k);

/*
 * Define an implementation of the abstract IO Ops interface methods.
 */
static io_ops laio_ops = {
   .read              = laio_read,
   .write             = laio_write,
   .get_iovec         = laio_get_iovec,
   .get_async_req     = laio_get_async_req,
   .get_metadata      = laio_get_metadata,
   .read_async        = laio_read_async,
   .write_async       = laio_write_async,
   .cleanup           = laio_cleanup,
   .wait_all          = laio_wait_all,
   .register_thread   = laio_register_thread,
   .deregister_thread = laio_deregister_thread,
};

static void
lock_ctx(laio_handle *io)
{
   while (__sync_lock_test_and_set(&io->ctx_lock, 1)) {
      while (io->ctx_lock) {
         platform_pause();
      }
   }
}

static void
unlock_ctx(laio_handle *io)
{
   __sync_lock_release(&io->ctx_lock);
}


static platform_status
process_context_init(io_process_context *pctx,
                     io_config          *cfg,
                     platform_heap_id    hid)
{
   pctx->io_contexts =
      TYPED_ARRAY_ZALLOC(hid, pctx->io_contexts, cfg->io_contexts_per_process);
   if (pctx->io_contexts == NULL) {
      platform_error_log("Failed to allocate memory for %lu laio_contexts\n",
                         cfg->io_contexts_per_process);
      return STATUS_NO_MEMORY;
   }

   uint64 nctxts;
   for (nctxts = 0; nctxts < cfg->io_contexts_per_process; nctxts++) {
      int status = io_setup(cfg->kernel_queue_size, &pctx->io_contexts[nctxts]);
      if (status != 0) {
         platform_error_log("io_setup() failed for ctx=%p with error=%d: %s\n",
                            pctx->io_contexts[nctxts],
                            -status,
                            strerror(-status));
         goto cleanup;
      }
   }
   pctx->thread_count = 0;
   return STATUS_OK;

cleanup:
   for (uint64 i = 0; i < nctxts; i++) {
      int status = io_destroy(pctx->io_contexts[i]);
      platform_assert(status == 0,
                      "io_destroy() failed with error=%d: %s\n",
                      -status,
                      strerror(-status));
   }
   platform_free(hid, pctx->io_contexts);

   return STATUS_IO_ERROR;
}

static void
process_context_deinit(io_process_context *pctx,
                       io_config          *cfg,
                       platform_heap_id    hid)
{
   platform_assert(
      pctx->thread_count == 0, "thread_count=%lu", pctx->thread_count);
   platform_assert(pctx->io_count == 0, "io_count=%lu", pctx->io_count);
   for (uint64 i = 0; i < cfg->io_contexts_per_process; i++) {
      int status = io_destroy(pctx->io_contexts[i]);
      platform_assert(status == 0,
                      "io_destroy() failed with error=%d: %s\n",
                      -status,
                      strerror(-status));
   }
   platform_free(hid, pctx->io_contexts);
   memset(pctx, 0, sizeof(*pctx));
}

/*
 * Find the index of the process IO context for this thread. If it doesn't
 * exist, create it.
 */
static uint64
get_ctx_idx(laio_handle *io)
{
   const pid_t pid = platform_getpid();

   lock_ctx(io);

   for (int i = 0; i < MAX_THREADS; i++) {
      if (io->ctx[i].pid == pid) {
         io->ctx[i].thread_count++;
         unlock_ctx(io);
         return i;
      }
   }

   for (int i = 0; i < MAX_THREADS; i++) {
      if (io->ctx[i].pid == 0) {
         platform_status rc =
            process_context_init(&io->ctx[i], io->cfg, io->heap_id);
         if (!SUCCESS(rc)) {
            unlock_ctx(io);
            return INVALID_TID;
         }
         io->ctx[i].pid = pid;
         io->ctx[i].thread_count++;
         unlock_ctx(io);
         return i;
      }
   }

   unlock_ctx(io);
   return INVALID_TID;
}

/*
 * Given an IO configuration, validate it. Allocate memory for various
 * sub-structures and allocate the SplinterDB device. Initialize the IO
 * sub-system, registering the file descriptor for SplinterDB device.
 */
platform_status
io_handle_init(laio_handle *io, io_config *cfg, platform_heap_id hid)
{
   uint64        req_size;
   uint64        total_req_size;
   io_async_req *req = NULL;

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

   struct stat statbuf;
   int         r = fstat(io->fd, &statbuf);
   if (r) {
      platform_error_log("fstat failed: %s\n", strerror(errno));
      return STATUS_IO_ERROR;
   }

   if (S_ISREG(statbuf.st_mode) && statbuf.st_size < 128 * 1024) {
      r = fallocate(io->fd, 0, 0, 128 * 1024);
      if (r) {
         platform_error_log("fallocate failed: %s\n", strerror(errno));
         return STATUS_IO_ERROR;
      }
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
   for (int i = 0; i < cfg->async_queue_size; i++) {
      req           = laio_get_kth_req(io, i);
      req->iocb_p   = &req->iocb;
      req->number   = i;
      req->pctx_idx = INVALID_TID;
      // We only issue IOs in units of one page
      for (int j = 0; j < cfg->async_max_pages; j++) {
         req->iovec[j].iov_len = cfg->page_size;
      }
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

   for (int i = 0; i < MAX_THREADS; i++) {
      if (io->ctx[i].pid != 0) {
         platform_error_log("ERROR: io_handle_deinit(): IO context for PID=%d"
                            " is still active.\n",
                            io->ctx[i].pid);
      }
   }

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
   laio_handle  *io      = (laio_handle *)ioh;
   uint64        batches = 0;
   io_async_req *req;

   const threadid tid = platform_get_tid();
   platform_assert(tid < MAX_THREADS, "Invalid tid=%lu", tid);
   uint64 pctx_idx = io->ctx_idx[tid];
   platform_assert(pctx_idx < MAX_THREADS, "Invalid ctx_idx=%lu", pctx_idx);

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
      if (__sync_bool_compare_and_swap(&req->pctx_idx, INVALID_TID, pctx_idx)) {
         req->ctx_idx =
            __sync_fetch_and_add(&io->ctx[pctx_idx].next_submit_ctx_idx, 1)
            % io->cfg->io_contexts_per_process;
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

/*
 * Accessor method: Return start of nested allocated iovec[], IO-vector array,
 * for specified async-request struct, 'req'.
 */
struct iovec *
laio_get_iovec(io_handle *ioh, io_async_req *req)
{
   return req->iovec;
}

/*
 * Accessor method: Return start of metadata field (issuer callback data).
 */
static void *
laio_get_metadata(io_handle *ioh, io_async_req *req)
{
   return req->metadata;
}

/*
 * Accessor method: Return opaque handle to IO-context setup by io_setup().
 */
static io_process_context *
laio_get_thread_context(io_handle *ioh)
{
   laio_handle *io  = (laio_handle *)ioh;
   threadid     tid = platform_get_tid();
   platform_assert(tid < MAX_THREADS, "Invalid tid=%lu", tid);
   platform_assert(
      io->ctx_idx[tid] < MAX_THREADS, "Invalid ctx_idx=%lu", io->ctx_idx[tid]);
   return &io->ctx[io->ctx_idx[tid]];
}

static io_process_context *
laio_get_req_context(io_handle *ioh, io_async_req *req)
{
   laio_handle *io = (laio_handle *)ioh;
   platform_assert(
      req->pctx_idx < MAX_THREADS, "Invalid pctx_idx=%lu", req->pctx_idx);
   return &io->ctx[req->pctx_idx];
}

void
laio_callback(io_context_t ctx, struct iocb *iocb, long res, long res2)
{
   io_async_req   *req;
   platform_status status = STATUS_OK;

   platform_assert(res2 == 0);
   req = (io_async_req *)((char *)iocb - offsetof(io_async_req, iocb));
   req->callback(req->metadata, req->iovec, req->count, status);
   req->pctx_idx = INVALID_TID;
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
   int                 status;
   laio_handle        *io   = (laio_handle *)ioh;
   io_process_context *pctx = laio_get_req_context(ioh, req);

   io_prep_preadv(&req->iocb, io->fd, req->iovec, count, addr);
   req->callback = callback;
   req->count    = count;
   io_set_callback(&req->iocb, laio_callback);
   do {
      // We increment the io_count before submitting the request to avoid
      // having the io_count go negative if another thread calls io_cleanup
      __sync_fetch_and_add(&pctx->io_count, 1);
      status = io_submit(pctx->io_contexts[req->ctx_idx], 1, &req->iocb_p);
      if (status <= 0) {
         __sync_fetch_and_sub(&pctx->io_count, 1);
      }
      if (status < 0) {
         platform_error_log("%s(): OS-pid=%d, tid=%lu, req=%p"
                            ", io_submit errorno=%d: %s\n",
                            __func__,
                            platform_getpid(),
                            platform_get_tid(),
                            req,
                            -status,
                            strerror(-status));
      }
      io_cleanup(ioh, 1);
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
   int                 status;
   laio_handle        *io   = (laio_handle *)ioh;
   io_process_context *pctx = laio_get_req_context(ioh, req);

   io_prep_pwritev(&req->iocb, io->fd, req->iovec, count, addr);
   req->callback = callback;
   req->count    = count;
   io_set_callback(&req->iocb, laio_callback);

   do {
      // We increment the io_count before submitting the request to avoid
      // having the io_count go negative if another thread calls io_cleanup
      __sync_fetch_and_add(&pctx->io_count, 1);
      status = io_submit(pctx->io_contexts[req->ctx_idx], 1, &req->iocb_p);
      if (status <= 0) {
         __sync_fetch_and_sub(&pctx->io_count, 1);
      }
      if (status < 0) {
         platform_error_log("%s(): OS-pid=%d, tid=%lu, req=%p"
                            ", io_submit errorno=%d: %s\n",
                            __func__,
                            platform_getpid(),
                            platform_get_tid(),
                            req,
                            -status,
                            strerror(-status));
      }
      io_cleanup(ioh, 1);
   } while (status != 1);

   return STATUS_OK;
}

/*
 * laio_cleanup() - Handle completion of outstanding IO requests for currently
 * running process. Up to 'count' outstanding IO requests will be processed.
 * Specify 'count' as 0 to process completions until there are no pending
 * requests
 */
static void
laio_cleanup(io_handle *ioh, uint64 count)
{
   laio_handle        *io   = (laio_handle *)ioh;
   io_process_context *pctx = laio_get_thread_context(ioh);
   struct io_event    *events;
   events = TYPED_ARRAY_ZALLOC(io->heap_id, events, pctx->io_count);
   uint64 i, j;
   int    status;

   // Check for completion of up to 'count' events, one event at a time.
   // Or, check for all outstanding events (count == 0)
   for (i = 0; (count == 0 || i < count) && 0 < pctx->io_count; i++) {
      uint64 ctx_idx = __sync_fetch_and_add(&pctx->next_cleanup_ctx_idx, 1);
      ctx_idx %= io->cfg->io_contexts_per_process;
      status = io_getevents(
         pctx->io_contexts[ctx_idx], 0, pctx->io_count, events, NULL);
      if (status < 0) {
         threadid tid = platform_get_tid();
         platform_error_log("%s(): OS-pid=%d, tid=%lu, io_getevents[%lu], "
                            "count=%lu, io_count=%lu,"
                            "failed with errorno=%d: %s\n",
                            __func__,
                            platform_getpid(),
                            tid,
                            i,
                            count,
                            pctx->io_count,
                            -status,
                            strerror(-status));
         i--;
         continue;
      }
      if (status == 0) {
         if (count == 0 && pctx->io_count > 0) {
            continue;
         }
         break;
      }

      __sync_fetch_and_sub(&pctx->io_count, status);

      // Invoke the callback for the one event that completed.
      for (j = 0; j < status; j++) {
         laio_callback(
            pctx->io_contexts[ctx_idx], events[j].obj, events[j].res, 0);
      }
   }

   platform_free(0, events);
}

/*
 * laio_wait_all() - Handle completion of outstanding IO requests for our
 * process, and wait for all other process's IOs to complete.
 */
static void
laio_wait_all(io_handle *ioh)
{
   laio_handle *io;
   uint64       i;

   io = (laio_handle *)ioh;
   for (i = 0; i < MAX_THREADS; i++) {
      if (io->ctx[i].pid == getpid()) {
         io_cleanup(ioh, 0);
      } else {
         while (0 < io->ctx[i].io_count) {
            io_cleanup(ioh, 0);
         }
      }
   }
}

/*
 * When a thread registers with Splinter's task system, setup its
 * IO-setup opaque handle that will be used by Async IO interfaces.
 */
static void
laio_register_thread(io_handle *ioh)
{
   const threadid tid = platform_get_tid();
   laio_handle   *io  = (laio_handle *)ioh;
   uint64         idx = get_ctx_idx(io);
   platform_assert(
      (idx != INVALID_TID), "Failed to register IO for thread ID=%lu\n", tid);
   io->ctx_idx[tid] = idx;
}

static void
laio_deregister_thread(io_handle *ioh)
{
   laio_handle        *io   = (laio_handle *)ioh;
   io_process_context *pctx = laio_get_thread_context(ioh);

   platform_assert((pctx != NULL),
                   "Attempting to deregister IO for thread ID=%lu"
                   " found an uninitialized IO-context handle.\n",
                   platform_get_tid());

   // Process pending AIO-requests for this thread before deregistering it
   laio_cleanup(ioh, 0);

   lock_ctx(io);
   pctx->thread_count--;
   if (pctx->thread_count == 0) {
      process_context_deinit(pctx, io->cfg, io->heap_id);
   }
   unlock_ctx(io);
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
   if (cfg->io_contexts_per_process == 0) {
      platform_error_log("io_contexts_per_process is 0\n");
      return STATUS_BAD_PARAM;
   }
   return STATUS_OK;
}
