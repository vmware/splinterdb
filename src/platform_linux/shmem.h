// Copyright 2018-2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef __PLATFORM_SHMEM_H__
#define __PLATFORM_SHMEM_H__

#include <sys/types.h>
#include <sys/shm.h>

typedef struct shmem_info shmem_info;

platform_status
platform_shmcreate(size_t                size,
                   platform_heap_handle *heap_handle,
                   platform_heap_id     *heap_id);

void
platform_shmdestroy(platform_heap_handle *heap_handle);

static inline int
platform_shm_alignment()
{
   return PLATFORM_CACHELINE_SIZE;
}

bool
platform_shm_heap_handle_valid(platform_heap_handle *heap_handle);

size_t
platform_shm_ctrlblock_size();

size_t
platform_shmsize(platform_heap_handle *heap_handle);

size_t
platform_shmfree(platform_heap_handle *heap_handle);

size_t
platform_shmused(platform_heap_handle *heap_handle);

#endif // __PLATFORM_SHMEM_H__
