// Copyright 2018-2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <sys/types.h>
#include <sys/shm.h>

/*
 * All memory allocations of this size or larger will be tracked in the
 * a fragment tracker array. For large inserts workload, we allocate large
 * memory chunks for fingerprint array, which is more than a MiB. For scans,
 * splinterdb_iterator_init() allocates memory for an iterator which is ~42+KiB.
 * Set this to a lower value so we can re-cycle free fragments for iterators
 * also. (Keep the limit same for release/debug builds to get consistent
 * behaviour.)
 */
#define SHM_LARGE_FRAG_SIZE (32 * KiB)

platform_status
platform_shmcreate(size_t size, platform_heap_id *heap_id);

platform_status
platform_shmdestroy(platform_heap_id *heap_id);

/*
 * Allocate memory fragment from the shared memory of requested 'size'.
 */
void *
platform_shm_alloc(platform_heap_id  hid,
                   const size_t      size,
                   platform_memfrag *memfrag, // OUT
                   const char       *objname,
                   const char       *func,
                   const char       *file,
                   const int         line);

/*
 * Free the memory fragment of 'size' bytes at 'ptr' address. This interface
 * deals with free of both small and large-memory fragments.
 */
void
platform_shm_free(platform_heap_id hid,
                  void            *ptr,
                  const size_t     size,
                  const char      *objname,
                  const char      *func,
                  const char      *file,
                  const int        line);

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
platform_shm_realloc(platform_heap_id hid,
                     void            *oldptr,
                     const size_t     oldsize,
                     size_t          *newsize,
                     const char      *func,
                     const char      *file,
                     const int        line);

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
 * Interfaces to retrieve size(s) using heap_id, which is what's
 * known externally to memory allocation interfaces.
 */
size_t
platform_shmsize(platform_heap_id heap_id);

size_t
platform_shmbytes_free(platform_heap_id heap_id);

size_t
platform_shmbytes_used(platform_heap_id heap_id);

void *
platform_shm_next_free_addr(platform_heap_id heap_id);

bool
platform_isvalid_addr_in_heap(platform_heap_id heap_id, const void *addr);

bool
platform_shm_next_free_cacheline_aligned(platform_heap_id heap_id);

size_t
platform_shm_find_freed_frag(platform_heap_id heap_id,
                             const void      *addr,
                             size_t          *freed_frag_size);
