// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "platform_status.h"
#include <sys/types.h>
#include <sys/shm.h>

typedef struct shmem_heap shmem_heap;

platform_status
platform_shmcreate(size_t size, shmem_heap **heap);

void
platform_shmdestroy(shmem_heap **heap);

/*
 * Allocate memory fragment from the shared memory of requested 'size'.
 */
void *
platform_shm_alloc(shmem_heap  *heap,
                   const size_t size,
                   const char  *objname,
                   const char  *func,
                   const char  *file,
                   const int    lineno);

/*
 * Free the memory fragment at 'ptr' address.
 */
void
platform_shm_free(shmem_heap *heap,
                  void       *ptr,
                  const char *objname,
                  const char *func,
                  const char *file,
                  const int   lineno);

/*
 * Reallocate the memory (fragment) at 'oldptr' of size 'oldsize' bytes.
 * Any contents at 'oldptr' are copied to 'newptr' for 'oldsize' bytes.
 *
 * NOTE: This interface does -not- do any cache-line alignment for 'newsize'
 * request. Caller is expected to do so. platform_realloc() takes care of it.
 *
 * Returns ptr to re-allocated memory of 'newsize' bytes.
 */
void *
platform_shm_realloc(shmem_heap  *heap,
                     void        *oldptr,
                     const size_t oldsize,
                     const size_t newsize,
                     const char  *func,
                     const char  *file,
                     const int    lineno);

void
platform_shm_tracing_init(const bool trace_shmem,
                          const bool trace_shmem_allocs,
                          const bool trace_shmem_frees);

void
platform_enable_tracing_shm_ops();

void
platform_enable_tracing_shm_allocs();

void
platform_enable_tracing_shm_frees();

void
platform_disable_tracing_shm_ops();

void
platform_disable_tracing_shm_allocs();

void
platform_disable_tracing_shm_frees();

void
platform_enable_tracing_large_frags();

void
platform_disable_tracing_large_frags();

size_t
platform_shm_ctrlblock_size();

/*
 * Interfaces to retrieve size(s) using heap.
 */
size_t
platform_shmsize(shmem_heap *heap);

size_t
platform_shmbytes_free(shmem_heap *heap);

size_t
platform_shmbytes_used(shmem_heap *heap);

void *
platform_shm_next_free_addr(shmem_heap *heap);

bool
platform_valid_addr_in_heap(shmem_heap *heap, const void *addr);

void *
platform_heap_get_splinterdb_handle(shmem_heap *heap);
