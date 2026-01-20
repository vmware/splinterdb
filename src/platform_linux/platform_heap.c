// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform_heap.h"
#include "platform_status.h"
#include <sys/mman.h>

/*
 * Declare globals to track heap handle/ID that may have been created when
 * using shared memory. We stash away these handles so that we can return the
 * right handle via platform_get_heap_id() interface, in case shared segments
 * are in use.
 */
platform_heap_id Heap_id = NULL;

/*
 * platform_heap_create() - Create a heap for memory allocation.
 *
 * By default, we just revert to process' heap-memory and use malloc() / free()
 * for memory management. If Splinter is run with shared-memory configuration,
 * create a shared-segment which acts as the 'heap' for memory allocation.
 */
platform_status
platform_heap_create(platform_module_id UNUSED_PARAM(module_id),
                     size_t             max,
                     bool               use_shmem,
                     platform_heap_id  *heap_id)
{
   if (use_shmem) {
      shmallocator *shm = mmap(
         NULL, max, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
      if (shm == MAP_FAILED) {
         return STATUS_NO_MEMORY;
      }
      shmallocator_init(shm, max / 4096, max);
      *heap_id = (platform_heap_id)shm;

   } else {
      *heap_id = PROCESS_PRIVATE_HEAP_ID;
   }
   return STATUS_OK;
}

void
platform_heap_destroy(platform_heap_id *heap_id)
{
   if (*heap_id) {
      size_t size = shmallocator_size((shmallocator *)*heap_id);
      shmallocator_deinit((shmallocator *)*heap_id);
      munmap((void *)*heap_id, size);
      *heap_id = NULL;
   }
}
