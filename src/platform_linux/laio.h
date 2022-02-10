// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * laio.h --
 *
 *     This file contains the interface for a libaio wrapper.
 */

#ifndef __LAIO_H
#define __LAIO_H

#include "io.h"
#include "task.h"
#include <libaio.h>

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

typedef struct laio_handle {
   io_handle        super;
   io_config       *cfg;
   int              fd;
   io_context_t     ctx;
   io_async_req    *req;
   uint64           max_batches_nonblocking_get;
   uint64           req_hand_base;
   uint64           req_hand[MAX_THREADS];
   platform_heap_id heap_id;
} laio_handle;

#endif //__LAIO_H
