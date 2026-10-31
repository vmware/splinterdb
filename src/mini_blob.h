// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "blob_build.h"
#include "mini_allocator.h"

#define NUM_BLOB_BATCHES (2)

/* Mini-allocator adapter configuration. The blob builder itself has no batch
 * numbers or mini-allocator dependency. */
typedef struct mini_blob_config {
   uint64 page_batch;
   uint64 subpage_batch;
   uint64 alignment;
   uint64 blob_max_descriptor_size;
} mini_blob_config;

platform_status
mini_blob_descriptor_size_bound(const mini_blob_config *cfg,
                                uint64                  extent_size,
                                uint64                  length,
                                uint64                 *size);

/* Successful results are borrowed from mini. On failure any allocations or
 * already-adopted roots remain tracked by mini for bulk reclamation; all roots
 * not adopted by mini are released. */
platform_status
mini_blob_build(const mini_blob_config *cfg,
                cache                  *cc,
                mini_allocator         *mini,
                slice                   data,
                writable_buffer        *result);

platform_status
mini_blob_clone(const mini_blob_config *cfg,
                cache                  *cc,
                mini_allocator         *mini,
                slice                   source,
                writable_buffer        *result);
