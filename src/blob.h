// Copyright 2022 VMware, Inc. All rights reserved. -- VMware Confidential
// SPDX-License-Identifier: Apache-2.0

#ifndef __BLOB_H
#define __BLOB_H

#include "util.h"
#include "cache.h"
#include "mini_allocator.h"

#define NUM_BLOB_BATCHES (3)

/*
 * The length of the sequence of bytes represented by this
 * blob.
 */
uint64
blob_length(slice sindy);

platform_status
blob_materialize(cache           *cc,
                 slice            sblob,
                 uint64           start,
                 uint64           end,
                 page_type        type,
                 writable_buffer *result);

platform_status
blob_build(cache           *cc,
           mini_allocator  *mini,
           slice            key,
           slice            data,
           page_type        type,
           writable_buffer *result);

platform_status
blob_clone(cache           *cc,
           mini_allocator  *mini,
           slice            key,
           slice            sblob,
           page_type        src_type,
           page_type        dst_type,
           writable_buffer *result);

platform_status
blob_issue_writebacks(cache *cc, slice sblob);

bool
blob_is_durable(cache *cc, slice sblob);

#endif /* __BLOB_H */
