// Copyright 2018-2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef __PLATFORM_SHMEM_H__
#define __PLATFORM_SHMEM_H__

#include <sys/types.h>
#include <sys/shm.h>

typedef struct shmem_info shmem_info;

// Extern references to boolean shmem-related globals
extern bool Trace_shmem_allocs;
extern bool Trace_shmem_frees;
extern bool Trace_shmem;

platform_status
platform_shmcreate(size_t                size,
                   platform_heap_handle *heap_handle,
                   platform_heap_id     *heap_id);

void
platform_shmdestroy(platform_heap_handle *heap_handle);

bool
platform_valid_addr_in_heap(platform_heap_id heap_id, const void *addr);

bool
platform_valid_addr_in_shm(platform_heap_handle heap_handle, const void *addr);

// Caller-macro to synthesize call-site details.
#define splinter_shm_alloc(heap_id, nbytes, objname)                           \
   platform_shm_alloc(heap_id, nbytes, objname, func, file, lineno)

void *
platform_shm_alloc(platform_heap_id hid,
                   const size_t     size,
                   const char      *objname,
                   const char      *func,
                   const char      *file,
                   const int        lineno);

// Caller-macro to synthesize call-site details.
#define splinter_shm_free(heap_id, ptr, objname)                               \
   platform_shm_free(heap_id, ptr, objname, func, file, lineno)

void
platform_shm_free(platform_heap_id hid,
                  void            *ptr,
                  const char      *objname,
                  const char      *func,
                  const char      *file,
                  const int        lineno);

// Caller-macro to synthesize call-site details.
// RESOLVE: Patch these in for now; Fix it properly later on.
#define splinter_shm_realloc(heap_id, oldptr, nbytes)                          \
   platform_shm_realloc(                                                       \
      heap_id, oldptr, nbytes, __FUNCTION__, __FILE__, __LINE__)

void *
platform_shm_realloc(platform_heap_id hid,
                     void            *oldptr,
                     const size_t     size,
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

#endif // __PLATFORM_SHMEM_H__