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

#endif // __PLATFORM_SHMEM_H__
