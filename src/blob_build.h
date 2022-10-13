// Copyright 2022 VMware, Inc. All rights reserved. -- VMware Confidential
// SPDX-License-Identifier: Apache-2.0

#ifndef __BLOB_BUILD_H
#define __BLOB_BUILD_H

#include "mini_allocator.h"
#include "blob.h"

#define NUM_BLOB_BATCHES (3)

typedef struct blob_build_config {
   uint64 extent_batch;
   uint64 page_batch;
   uint64 subpage_batch;
   uint64 alignment;
} blob_build_config;

platform_status
blob_build(blob_build_config *cfg,
           cache             *cc,
           mini_allocator    *mini,
           slice              key,
           slice              data,
           page_type          type,
           writable_buffer   *result);

platform_status
blob_clone(blob_build_config *cfg,
           cache             *cc,
           mini_allocator    *mini,
           slice              key,
           slice              sblob,
           page_type          src_type,
           page_type          dst_type,
           writable_buffer   *result);

#endif /* __BLOB_BUILD_H */
