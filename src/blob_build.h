// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "blob.h"
#include "mini_allocator.h"

#define NUM_BLOB_BATCHES (3)

typedef struct blob_build_config {
   uint64 extent_batch;
   uint64 page_batch;
   uint64 subpage_batch;
   uint64 alignment;
} blob_build_config;

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
