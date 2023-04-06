// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * laio.h --
 *
 *     This file contains the interface for a libaio wrapper.
 */

#pragma once

#include "io.h"
#include <libaio.h>

/*
 * SplinterDB can be configured with different page-sizes, given by these
 * min & max values. But for now, these are defined to just the one page
 * size currently supported.
 */
#define LAIO_MIN_PAGE_SIZE (4096)
#define LAIO_MAX_PAGE_SIZE (8192)

#define LAIO_DEFAULT_PAGE_SIZE        LAIO_MIN_PAGE_SIZE
#define LAIO_DEFAULT_PAGES_PER_EXTENT 32
#define LAIO_DEFAULT_EXTENT_SIZE                                               \
   (LAIO_DEFAULT_PAGES_PER_EXTENT * LAIO_DEFAULT_PAGE_SIZE)

/*
 * Async IO Request structure: Each such request can track up to a configured
 * number of pages, io_config{}->async_max_pages, on which an IO is issued.
 * This number sizes the iovec[] array nested below. An array of these structs,
 * along with the nested sub-array of iovec[], comes from allocated memory
 * which is setup when the IO-sub-system is initialized.
 */
struct io_async_req {
   struct iocb    iocb;         // laio callback
   struct iocb   *iocb_p;       // laio callback pointer
   io_callback_fn callback;     // issuer callback
   char           metadata[64]; // issuer callback data
   uint64         number;       // request number/id
   bool           busy;         // request in-use flag
   uint64         bytes;        // total bytes in the IO request
   uint64         count;        // number of vector elements
   struct iovec   iovec[];      // vector with IO offsets and size
};

/*
 * Async IO context structure handle:
 */
typedef struct laio_handle {
   io_handle        super;
   io_config       *cfg;
   io_context_t     ctx[MAX_THREADS]; // Opaque handle returned by system call
   io_async_req    *req; // Ptr to allocated array of async req structs
   uint64           max_batches_nonblocking_get;
   uint64           req_hand_base;
   uint64           req_hand[MAX_THREADS];
   platform_heap_id heap_id;
   int              fd; // File descriptor to Splinter device/file.
   size_t           req_size;
} laio_handle;

platform_status
laio_config_valid(io_config *cfg);
