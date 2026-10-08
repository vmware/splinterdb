// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "blob.h"
#include "mini_allocator.h"

#define NUM_BLOB_BATCHES (3)
#define BLOB_DEFAULT_MAX_DESCRIPTOR_SIZE UINT64_C(512)
/* Enough for every legacy packed-tail descriptor (at most eight full extents
 * plus three tail addresses). Smaller budgets would make tail/tree decoding
 * ambiguous. Zero in blob_build_config selects the default. */
#define BLOB_MIN_MAX_DESCRIPTOR_SIZE (sizeof(blob) + 11 * sizeof(uint64))

typedef struct blob_build_config {
   uint64 extent_batch;
   uint64 page_batch;
   uint64 subpage_batch;
   uint64 alignment;
   uint64 blob_max_descriptor_size;
} blob_build_config;

/* Allocation- and I/O-free bound; exact for extent-only trees. */
platform_status
blob_descriptor_size_bound(const blob_build_config *cfg,
                           uint64                   extent_size,
                           uint64                   length,
                           uint64                  *size);

platform_status
blob_build(const blob_build_config *cfg,
           cache                   *cc,
           mini_allocator          *mini,
           slice                    data,
           writable_buffer         *result);

platform_status
blob_clone(const blob_build_config *cfg,
           cache                   *cc,
           mini_allocator          *mini,
           slice                    sblob,
           writable_buffer         *result);
