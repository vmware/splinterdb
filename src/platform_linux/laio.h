// Copyright 2018-2021 VMware, Inc.
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

typedef struct io_process_context {
   process_context_state state;
   uint64                lock;
   uint64                io_count; // inflight ios
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
   process_event_callback_list_node pecnode;
} laio_handle;

platform_status
laio_config_valid(io_config *cfg);

io_handle *
laio_handle_create(io_config *cfg, platform_heap_id hid);

// The IO system must be quiesced before calling this function.
void
laio_handle_destroy(io_handle *ioh);