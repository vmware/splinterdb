// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * test_common.h --
 *
 * Header file with shared prototypes and definitions for functions defined
 * in test_common.c, that are shared between functional/ and unit/ test sources.
 * -----------------------------------------------------------------------------
 */
#pragma once

#include "core.h"
#include "functional/test.h"
#include "functional/test_async.h"

typedef struct stats_lookup {
   uint64 num_found;
   uint64 num_not_found;
   uint64 latency_max;
} stats_lookup;

typedef struct {
   bool32        expected_found;
   bool32        stats_only; // update statistic only
   stats_lookup *stats;
} verify_tuple_arg;

/*
 * Tuple verification routine.
 */
void
verify_tuple(core_handle            *spl,
             test_message_generator *gen,
             uint64                  lookup_num,
             key                     tuple_key,
             message                 data,
             bool32                  expected_found);

void
test_wait_for_inflight(core_handle       *spl,
                       test_async_lookup *async_lookup,
                       verify_tuple_arg  *vtarg);

void
verify_tuple_callback(core_handle *spl, test_async_ctxt *ctxt, void *arg);

test_async_ctxt *
test_async_ctxt_get(core_handle       *spl,
                    test_async_lookup *async_lookup,
                    verify_tuple_arg  *vtarg);

static inline bool32
test_show_verbose_progress(test_exec_config *test_exec_cfg)
{
   return (test_exec_cfg->verbose_progress);
}

void
trace_wait_for_gdb(void);

#define test_print_progress(progress_state, progress, ...)                     \
   do {                                                                        \
      if (*progress_state != progress) {                                       \
         *progress_state = progress;                                           \
         platform_default_log(__VA_ARGS__);                                    \
      }                                                                        \
   } while (0)
