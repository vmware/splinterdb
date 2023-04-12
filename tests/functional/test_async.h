// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * test_async.h --
 *
 *     This file contains interfaces for a toy per-thread ctxt manager.
 */

#pragma once

#include "platform.h"

#include "trunk.h"
#include "cache.h"
#include "pcq.h"

// Per thread max async inflight. This is limited by clockcache
#define TEST_MAX_ASYNC_INFLIGHT MAX_READ_REFCOUNT

// A single async context
typedef struct {
   trunk_async_ctxt ctxt;
   pcq             *ready_q;
   union {
      int8   refcount;   // Used by functionality test
      uint64 lookup_num; // Used by rest
   };
   key_buffer        key;
   merge_accumulator data;
} test_async_ctxt;

/*
 * Per-thread array of async contexts. Tests that write to multiple
 * tables at a time, need to make this per-table too.
 */
typedef struct {
   uint32          max_async_inflight;
   pcq            *ready_q;
   pcq            *avail_q;
   size_t          size; // of memory allocated for this struct
   test_async_ctxt ctxt[];
} test_async_lookup;

typedef void (*async_ctxt_process_cb)(trunk_handle    *spl,
                                      test_async_ctxt *ctxt,
                                      void            *arg);

void
async_ctxt_init(platform_heap_id    hid,
                uint32              max_async_inflight,
                test_async_lookup **out);
void
async_ctxt_deinit(platform_heap_id hid, test_async_lookup *async_lookup);
test_async_ctxt *
async_ctxt_get(test_async_lookup *async_lookup);
void
async_ctxt_unget(test_async_lookup *async_lookup, test_async_ctxt *ctxt);
void
async_ctxt_process_one(trunk_handle         *spl,
                       test_async_lookup    *async_lookup,
                       test_async_ctxt      *ctxt,
                       timestamp            *latency_max,
                       async_ctxt_process_cb process_cb,
                       void                 *process_arg);
bool
async_ctxt_process_ready(trunk_handle         *spl,
                         test_async_lookup    *async_lookup,
                         timestamp            *latency_max,
                         async_ctxt_process_cb process_cb,
                         void                 *process_arg);
