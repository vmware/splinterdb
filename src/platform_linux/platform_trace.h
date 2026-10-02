// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "platform_time.h"

/* Diagnostic tracing is off by default. Open/close only with workers stopped.
 * Timestamps use CLOCK_MONOTONIC, shared with the benchmark sampler. */
extern bool32 platform_trace_enabled;

int
platform_trace_open(const char *filename);
int
platform_trace_close(void);

static inline timestamp
platform_trace_begin(void)
{
   return platform_trace_enabled ? platform_get_timestamp() : 0;
}

void
platform_trace_end(const char *event,
                   timestamp   start,
                   uint64      arg0,
                   uint64      arg1);
