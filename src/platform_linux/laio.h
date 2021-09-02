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

//#include <libaio.h>
//#include <aio.h>

struct iocb {
        /* these are internal to the kernel/libc. */
        uint64_t        aio_data;        /* data to be returned in event's data */
        //uint32_t        PADDED(aio_key, aio_reserved1);
                                /* the kernel sets aio_key to the req # */

        /* common fields */
        uint16_t        aio_lio_opcode;        /* see IOCB_CMD_ above */
        int16_t        aio_reqprio;
        uint32_t        aio_fildes;

        uint64_t        aio_buf;
        uint64_t        aio_nbytes;
        int64_t        aio_offset;

        /* extra parameters */
        uint64_t        aio_reserved2;        /* TODO: use this for a (struct sigevent *) */

        /* flags for the "struct iocb" */
        uint32_t        aio_flags;

        /*
         * if the IOCB_FLAG_RESFD flag of "aio_flags" is set, this is an
         * eventfd to signal AIO readiness to
         */
        uint32_t        aio_resfd;
}; /* 64 bytes */

struct io_async_req {
    struct iocb     iocb;        // laio callback
    struct iocb    *iocb_p;      // laio callback pointer
    io_callback_fn  callback;    // issuer callback
    char            metadata[64];// issuer callback data
    uint64          number;      // request number/id
    bool            busy;        // request in-use flag
    uint64          bytes;       // total bytes in the IO request
    uint64          count;       // number of vector elements
    struct iovec    iovec[];     // vector with IO offsets and size
};

typedef struct io_context *io_context_t;

typedef struct laio_handle {
    io_handle         super;
    io_config        *cfg;
    int               fd;
    io_context_t      ctx;
    io_async_req     *req;
    uint64            max_batches_nonblocking_get;
    uint64            req_hand_base;
    uint64            req_hand[MAX_THREADS];
    platform_heap_id  heap_id;
} laio_handle;

#endif //__LAIO_H
