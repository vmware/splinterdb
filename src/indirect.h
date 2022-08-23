// Copyright 2022 VMware, Inc. All rights reserved. -- VMware Confidential
// SPDX-License-Identifier: Apache-2.0

#ifndef __INDIRECT_H
#define __INDIRECT_H

#include "util.h"
#include "cache.h"
#include "mini_allocator.h"

#define NUM_INDIRECTION_BATCHES (2)

/*
 * The length of the sequence of bytes represented by this
 * indirection.
 */
uint64
indirection_length(slice sindy);

platform_status
indirection_materialize(cache           *cc,
                        slice            sindy,
                        uint64           start,
                        uint64           end,
                        page_type        type,
                        writable_buffer *result);

platform_status
indirection_build(cache           *cc,
                  mini_allocator  *mini,
                  slice            key,
                  slice            data,
                  page_type        type,
                  writable_buffer *result);

platform_status
indirection_clone(cache           *cc,
                  mini_allocator  *mini,
                  slice            key,
                  slice            sindy,
                  page_type        src_type,
                  page_type        dst_type,
                  writable_buffer *result);

platform_status
indirection_issue_writebacks(cache *cc, slice sindy);

bool
indirection_is_durable(cache *cc, slice sindy);

#endif /* __INDIRECT_H */
