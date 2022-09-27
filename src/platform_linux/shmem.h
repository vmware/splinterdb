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

void *
platform_shm_alloc(platform_heap_id hid,
                   const size_t     size,
                   const char      *func,
                   const char      *file,
                   const int        lineno);

void
platform_shm_free(platform_heap_id hid,
                  void            *ptr,
                  const char      *func,
                  const char      *file,
                  const int        lineno);

static inline int
platform_shm_alignment()
{
   return PLATFORM_CACHELINE_SIZE;
}

bool
platform_shm_heap_handle_valid(platform_heap_handle heap_handle);

size_t
platform_shm_ctrlblock_size();

/*
 * Interfaces to retrieve size(s) using heap_handle.
 */
size_t
platform_shmsize_by_hh(platform_heap_handle heap_handle);

size_t
platform_shmfree_by_hh(platform_heap_handle heap_handle);

size_t
platform_shmused_by_hh(platform_heap_handle heap_handle);

void *
platform_shm_next_free_addr_by_hh(platform_heap_handle heap_handle);

/*
 * Interfaces to retrieve size(s) using heap_id, which is what's
 * known externally to memory allocation interfaces.
 */
size_t
platform_shmsize(platform_heap_id heap_id);

size_t
platform_shmfree(platform_heap_id heap_id);

size_t
platform_shmused(platform_heap_id heap_id);

void *
platform_shm_next_free_addr(platform_heap_id heap_id);

#endif // __PLATFORM_SHMEM_H__
