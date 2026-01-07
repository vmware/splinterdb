// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "platform_assert.h"
#include "platform_status.h"
#include "platform_util.h"
#include "platform_machine.h"
#include "shmem.h"
#include <stddef.h>
#include <stdlib.h>

typedef void *platform_heap_id;

/* Don't have a good place for this, so putting it here */
typedef void *platform_module_id;

static inline platform_module_id
platform_get_module_id()
{
   // void* NULL since we don't actually need a module id
   return NULL;
}


extern platform_heap_id Heap_id;

/*
 * Provide a tag for callers that do not want to use shared-memory allocation,
 * when configured but want to fallback to default scheme of allocating
 * process-private memory. Typically, this would default to malloc()/free().
 * (Clients that repeatedly allocate and free a large chunk of memory in some
 *  code path would want to use this tag.)
 */
#define PROCESS_PRIVATE_HEAP_ID (platform_heap_id) NULL


static inline platform_heap_id
platform_get_heap_id(void)
{
   // void* NULL since we don't actually need a heap id
   return Heap_id;
}

/*
 * Return # of bytes needed to align requested 'size' bytes at 'alignment'
 * boundary.
 */
static inline size_t
platform_align_bytes_reqd(const size_t alignment, const size_t size)
{
   return ((alignment - (size % alignment)) % alignment);
}

/*
 * platform_aligned_malloc() -- Allocate n-bytes accounting for alignment.
 *
 * This interface will, by default, allocate using aligned_alloc(). Currently
 * this supports alignments up to a cache-line.
 * If Splinter is configured to run with shared memory, we will invoke the
 * shmem-allocation function, working off of the (non-NULL) platform_heap_id.
 */
static inline void *
platform_aligned_malloc(const platform_heap_id heap_id,
                        const size_t           alignment, // IN
                        const size_t           size,      // IN
                        const char            *objname,
                        const char            *func,
                        const char            *file,
                        const int              lineno)
{
   // Requirement for aligned_alloc
   platform_assert(IS_POWER_OF_2(alignment));

   /*
    * aligned_alloc requires size to be a multiple of alignment
    * round up to nearest multiple of alignment
    *
    * Note that since this is inlined, the compiler will turn the constant
    * (power of 2) alignment mod operations into bitwise &
    */
   // RESOLVE: Delete this padding from caller. Push this down to
   // platform_shm_alloc().
   const size_t padding  = platform_align_bytes_reqd(alignment, size);
   const size_t required = (size + padding);

   void *retptr =
      (heap_id
          ? platform_shm_alloc(heap_id, required, objname, func, file, lineno)
          : aligned_alloc(alignment, required));
   return retptr;
}

/*
 * platform_realloc() - Reallocate 'newsize' bytes and copy over old contents.
 *
 * This is a wrapper around C-realloc() but farms over to shared-memory
 * based realloc, when needed.
 *
 * The interface is intentional to avoid inadvertently swapping 'oldsize' and
 * 'newsize' in the call, if they were to appear next to each other.
 *
 * Reallocing to size 0 must be equivalent to freeing.
 * Reallocing from NULL must be equivalent to allocing.
 */
static inline void *
platform_realloc(const platform_heap_id heap_id,
                 const size_t           oldsize,
                 void                  *ptr, // IN
                 const size_t           newsize)       // IN
{
   /* FIXME: alignment? */

   // Farm control off to shared-memory based realloc, if it's configured
   if (heap_id) {
      // The shmem-based allocator is expecting all memory requests to be of
      // aligned sizes, as that's what platform_aligned_malloc() does. So, to
      // keep that allocator happy, align this memory request if needed.
      // As this is the case of realloc, we assume that it would suffice to
      // align at platform's natural cacheline boundary.
      const size_t padding =
         platform_align_bytes_reqd(PLATFORM_CACHELINE_SIZE, newsize);
      const size_t required = (newsize + padding);
      return platform_shm_realloc(
         heap_id, ptr, oldsize, required, __func__, __FILE__, __LINE__);
   } else {
      return realloc(ptr, newsize);
   }
}

static inline void
platform_free_from_heap(platform_heap_id heap_id,
                        void            *ptr,
                        const char      *objname,
                        const char      *func,
                        const char      *file,
                        int              lineno)
{
   if (heap_id) {
      platform_shm_free(heap_id, ptr, objname, func, file, lineno);
   } else {
      free(ptr);
   }
}

typedef struct shmem_heap shmem_heap;

platform_status
platform_heap_create(platform_module_id module_id,
                     size_t             max,
                     bool               use_shmem,
                     platform_heap_id  *heap_id);

void
platform_heap_destroy(platform_heap_id *heap_id);

void
platform_shm_set_splinterdb_handle(platform_heap_id heap_id, void *addr);

shmem_heap *
platform_heap_id_to_shmaddr(platform_heap_id hid);

/*
 * Similar to the TYPED_MALLOC functions, for all the free functions we need to
 * call platform_get_heap_id() from a macro instead of an inline function
 * (which may or may not end up inlined)
 * Wrap free and free_volatile:
 */
#define platform_free(id, p)                                                   \
   do {                                                                        \
      platform_free_from_heap(                                                 \
         id, (p), STRINGIFY(p), __func__, __FILE__, __LINE__);                 \
      (p) = NULL;                                                              \
   } while (0)

#define platform_free_volatile(id, p)                                          \
   do {                                                                        \
      platform_free_volatile_from_heap(                                        \
         id, (p), STRINGIFY(p), __func__, __FILE__, __LINE__);                 \
      (p) = NULL;                                                              \
   } while (0)

// Convenience function to free something volatile
static inline void
platform_free_volatile_from_heap(platform_heap_id heap_id,
                                 volatile void   *ptr,
                                 const char      *objname,
                                 const char      *func,
                                 const char      *file,
                                 int              lineno)
{
   // Ok to discard volatile qualifier for free
   platform_free_from_heap(heap_id, (void *)ptr, objname, func, file, lineno);
}

static inline void *
platform_aligned_zalloc(platform_heap_id heap_id,
                        size_t           alignment,
                        size_t           size,
                        const char      *objname,
                        const char      *func,
                        const char      *file,
                        int              lineno)
{
   void *x = platform_aligned_malloc(
      heap_id, alignment, size, objname, func, file, lineno);
   if (LIKELY(x)) {
      memset(x, 0, size);
   }
   return x;
}
