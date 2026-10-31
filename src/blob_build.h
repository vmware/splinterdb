// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "blob.h"

#define BLOB_DEFAULT_MAX_DESCRIPTOR_SIZE UINT64_C(256)
/* Enough for every legacy packed-tail descriptor (at most eight full extents
 * plus three tail addresses). Smaller budgets would make tail/tree decoding
 * ambiguous. Zero in blob_build_config selects the default. */
#define BLOB_MIN_MAX_DESCRIPTOR_SIZE (sizeof(blob) + 11 * sizeof(uint64))

typedef struct blob_build_config {
   uint64 alignment;
   uint64 blob_max_descriptor_size;
} blob_build_config;

/* The container owns all returned tail storage. Allocate at most one extent
 * of bytes, in at most two extent-bounded fragments, and initialize newly
 * allocated cache pages before returning. Existing partial pages may be reused.
 * alignment/boundary have mini_alloc_bytes semantics; boundary is zero for
 * whole pages and extent_size for a final subpage fragment. */
typedef platform_status (*blob_tail_alloc_fn)(void  *arg,
                                              uint64 bytes,
                                              uint64 alignment,
                                              uint64 boundary,
                                              uint64 addrs[2]);
typedef struct blob_tail_allocator {
   blob_tail_alloc_fn alloc;
   void              *arg;
} blob_tail_allocator;

/* On success the caller owns the whole-extent references until a container
 * adopts them. On failure those references are released; allocated tails stay
 * with the destination container. Source containers must remain alive through
 * clone. Descriptors and pointer extents retain their existing disk format. */
/* Allocation- and I/O-free bound; exact for extent-only trees. */
platform_status
blob_descriptor_size_bound(const blob_build_config *cfg,
                           uint64                   extent_size,
                           uint64                   length,
                           uint64                  *size);

platform_status
blob_build(const blob_build_config   *cfg,
           cache                     *cc,
           const blob_tail_allocator *tails,
           slice                      data,
           writable_buffer           *result);

platform_status
blob_clone(const blob_build_config   *cfg,
           cache                     *cc,
           const blob_tail_allocator *tails,
           slice                      sblob,
           writable_buffer           *result);
