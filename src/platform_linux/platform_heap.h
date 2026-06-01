// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "platform_assert.h"
#include "platform_status.h"
#include "platform_util.h"
#include "platform_machine.h"
#include "platform_log.h"
#include "shmalloc.h"
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

#ifndef PLATFORM_MEMORY_FAULT_INJECTION
#   define PLATFORM_MEMORY_FAULT_INJECTION 0
#endif

typedef enum platform_memory_fault_mode {
   PLATFORM_MEMORY_FAULT_RANGE = 0,
   PLATFORM_MEMORY_FAULT_RANDOM,
} platform_memory_fault_mode;

#define PLATFORM_MEMORY_FAULT_PROBABILITY_SCALE (1000000U)

typedef struct platform_memory_fault_config {
   platform_memory_fault_mode mode;

   // Allocation numbers are 1-based.
   uint64 range_start;
   uint64 range_count;

   uint64 seed;
   uint32 random_fail_probability;
   uint32 random_burst_start_probability;
   uint32 random_burst_fail_probability;
   uint64 random_burst_min_length;
   uint64 random_burst_max_length;

   uint64 max_failures;
   bool32 verbose;
} platform_memory_fault_config;

typedef struct platform_memory_fault_counters {
   uint64 alloc_count;
   uint64 failure_count;
} platform_memory_fault_counters;

void
platform_memory_fault_config_get(platform_memory_fault_config *cfg);

void
platform_memory_fault_config_set(const platform_memory_fault_config *cfg);

void
platform_memory_fault_enable(void);

void
platform_memory_fault_disable(void);

void
platform_memory_fault_reset_counters(void);

platform_memory_fault_counters
platform_memory_fault_get_counters(void);

bool32
platform_memory_fault_should_fail(platform_heap_id heap_id,
                                  size_t           size,
                                  const char      *objname,
                                  const char      *func,
                                  const char      *file,
                                  int              lineno);

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
   platform_assert(size <= size + alignment - 1);
   size_t aligned_size = (size + alignment - 1) & ~((uintptr_t)alignment - 1);

#if PLATFORM_MEMORY_FAULT_INJECTION                                           \
   && !defined(PLATFORM_MEMORY_FAULT_INJECTION_DISABLED_IN_THIS_FILE)
   if (platform_memory_fault_should_fail(
          heap_id, size, objname, func, file, lineno))
   {
      return NULL;
   }
#endif

   if (heap_id) {
      return shmalloc(heap_id, alignment, size);
   } else {
      return aligned_alloc(alignment, aligned_size);
   }
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
platform_realloc_from_heap(const platform_heap_id heap_id,
                           const size_t           oldsize,
                           void                  *ptr, // IN
                           const size_t           newsize,       // IN
                           const char            *objname,
                           const char            *func,
                           const char            *file,
                           const int              lineno)
{
   /* FIXME: alignment? */

#if PLATFORM_MEMORY_FAULT_INJECTION                                           \
   && !defined(PLATFORM_MEMORY_FAULT_INJECTION_DISABLED_IN_THIS_FILE)
   if (newsize != 0
       && platform_memory_fault_should_fail(
             heap_id, newsize, objname, func, file, lineno))
   {
      return NULL;
   }
#endif

   // Farm control off to shared-memory based realloc, if it's configured
   if (heap_id) {
      return shrealloc(heap_id, ptr, newsize);
   } else {
      return realloc(ptr, newsize);
   }
}

#define platform_realloc(id, oldsize, p, newsize)                              \
   platform_realloc_from_heap(id,                                              \
                              oldsize,                                         \
                              p,                                               \
                              newsize,                                         \
                              STRINGIFY(p),                                    \
                              __func__,                                        \
                              __FILE__,                                        \
                              __LINE__)

static inline void
platform_free_from_heap(platform_heap_id heap_id,
                        void            *ptr,
                        const char      *objname,
                        const char      *func,
                        const char      *file,
                        int              lineno)
{
   if (heap_id) {
      shfree(heap_id, ptr);
   } else {
      free(ptr);
   }
}

platform_status
platform_heap_create(platform_module_id module_id,
                     size_t             max,
                     bool               use_shmem,
                     platform_heap_id  *heap_id);

void
platform_heap_destroy(platform_heap_id *heap_id);

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
