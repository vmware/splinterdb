// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * laio.h --
 *
 *     This file contains the interface for a libaio wrapper.
 */

#pragma once

#include "platform_io.h"
#include "platform_threads.h"
#include "platform_status.h"
#include "async.h"
#include <libaio.h>

typedef enum process_context_state {
   PROCESS_CONTEXT_STATE_UNINITIALIZED,
   PROCESS_CONTEXT_STATE_INITIALIZED,
   PROCESS_CONTEXT_STATE_SHUTTING_DOWN,
} process_context_state;

typedef enum laio_backing_type {
   LAIO_BACKING_REGULAR,
   LAIO_BACKING_BLOCK,
   LAIO_BACKING_UNSUPPORTED,
} laio_backing_type;

#define LAIO_QD_HIST_BUCKETS (IO_DEFAULT_KERNEL_QUEUE_SIZE + 2)

typedef struct io_process_context {
   process_context_state state;
   uint64                lock;
   uint64                io_count; // inflight ios
   uint64                io_submit_count;
   uint64                io_submit_eagain;
   uint64                io_submit_hist[LAIO_QD_HIST_BUCKETS];
   uint64                max_observed_io_count;
   io_context_t          ctx;
   pthread_t             io_cleaner;
   async_wait_queue      submit_waiters;
} io_process_context;

/*
 * Async IO context structure handle:
 */
typedef struct laio_handle {
   io_handle          super;
   io_config         *cfg;
   io_process_context ctx[MAX_THREADS];
   platform_heap_id   heap_id;
   int                fd; // File descriptor to Splinter device/file.
   laio_backing_type  backing_type;

   /*
    * Cached logical backing size.  A completed write advances
    * write_generation; range queries may reuse logical_size only when its
    * generation matches.  The fields are accessed with __atomic builtins --
    * keeping writes to one atomic increment and avoiding a mutex on the IO hot
    * path.
    */
   uint64 logical_size;
   uint64 logical_size_generation;
   uint64 write_generation;

   process_event_callback_list_node pecnode;
} laio_handle;

platform_status
laio_config_valid(io_config *cfg);

io_handle *
laio_handle_create(io_config *cfg, platform_heap_id hid);

// The IO system must be quiesced before calling this function.
void
laio_handle_destroy(io_handle *ioh);
