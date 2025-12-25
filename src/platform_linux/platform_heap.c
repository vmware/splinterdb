#include "platform_heap.h"
#include "platform_status.h"

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
   *heap_id = PROCESS_PRIVATE_HEAP_ID;

   if (use_shmem) {
      platform_status rc = platform_shmcreate(max, heap_id);
      if (SUCCESS(rc)) {
         Heap_id = *heap_id;
      }
      return rc;
   }
   *heap_id = NULL;
   return STATUS_OK;
}

void
platform_heap_destroy(platform_heap_id *heap_id)
{
   // If shared segment was allocated, it's being tracked thru heap ID.
   if (*heap_id) {
      return platform_shmdestroy(heap_id);
   }
}
