// Copyright 2018-2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <sys/types.h>
#include <sys/shm.h>

typedef struct shmem_info shmem_info;

platform_status
platform_shmcreate(size_t                size,
                   platform_heap_handle *heap_handle,
                   platform_heap_id     *heap_id);

void
platform_shmdestroy(platform_heap_handle *heap_handle);

/*
 * void * = splinter_shm_alloc(platform_heap_id heap_id, size_t nbytes,
 *                             const char * objname)
 *
 * Caller-macro to invoke lower-level allocator and to pass-down caller's
 * context fields, which are printed for diagnostics under a traceflag.
 */
#define splinter_shm_alloc(heap_id, nbytes, objname)                           \
   platform_shm_alloc(heap_id, nbytes, objname, func, file, lineno)

void *
platform_shm_alloc(platform_heap_id hid,
                   const size_t     size,
                   const char      *objname,
                   const char      *func,
                   const char      *file,
                   const int        lineno);

/*
 * void = splinter_shm_free(platform_heap_id heap_id, void *ptr,
 *                          const char * objname)
 *
 * Caller-macro to invoke lower-level free method and to pass-down caller's
 * context fields, which are printed for diagnostics under a traceflag.
 */
#define splinter_shm_free(heap_id, ptr, objname)                               \
   platform_shm_free(heap_id, ptr, objname, func, file, lineno)

void
platform_shm_free(platform_heap_id hid,
                  void            *ptr,
                  const char      *objname,
                  const char      *func,
                  const char      *file,
                  const int        lineno);

/*
 * void * = splinter_shm_realloc(platform_heap_id heap_id, void *oldptr,
 *                               size_t oldsize, size_t nbytes)
 *
 * Caller-macro to invoke 'realloc' interface from shared-segment. As we
 * do not know how big the old chunk being reallocated is, we need to pass-down
 * the 'oldsize' of the memory chunk pointed by 'oldptr'. Realloc needs to
 * copy over contents of 'oldptr' to new memory allocated.
 */
#define splinter_shm_realloc(heap_id, oldptr, oldsize, nbytes)                 \
   platform_shm_realloc(                                                       \
      heap_id, oldptr, oldsize, nbytes, __func__, __FILE__, __LINE__)

void *
platform_shm_realloc(platform_heap_id hid,
                     void            *oldptr,
                     const size_t     oldsize,
                     const size_t     newsize,
                     const char      *func,
                     const char      *file,
                     const int        lineno);

bool
platform_valid_addr_in_heap(platform_heap_id heap_id, const void *addr);

bool
platform_valid_addr_in_shm(platform_heap_handle heap_handle, const void *addr);

static inline int
platform_shm_alignment()
{
   return PLATFORM_CACHELINE_SIZE;
}

bool
platform_shm_heap_handle_valid(platform_heap_handle heap_handle);

void
platform_shm_tracing_init(const bool trace_shmem,
                          const bool trace_shmem_allocs,
                          const bool trace_shmem_frees);

void
platform_disable_tracing_shm_ops();

void
platform_disable_tracing_shm_allocs();

void
platform_disable_tracing_shm_frees();

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

void
platform_shm_set_splinterdb_handle(platform_heap_handle heap_handle,
                                   void                *addr);

void *
platform_shm_get_splinterdb_handle(const platform_heap_handle heap_handle);
