// Copyright 2018-2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * shmem.c --
 *
 * This file contains the implementation for managing shared memory created
 * for use by SplinterDB and all its innards.
 */

#include "platform.h"
#include "shmem.h"
#include "util.h"

// SplinterDB's shared segment magic identifier
#define SPLINTERDB_SHMEM_MAGIC (uint64)0xDEFACADE

/*
 * -----------------------------------------------------------------------------
 * Core structure describing shared memory segment created. This lives right
 * at the start of the allocated shared segment.
 * -----------------------------------------------------------------------------
 */
typedef struct shmem_info {
   void  *shm_start;       // Points to start address of shared segment.
   void  *shm_next;        // Points to next 'free' address to allocate from.
   size_t shm_total_bytes; // Total size of shared segment allocated initially.
   size_t shm_free_bytes;  // Free bytes of memory left (that can be allocated)
   size_t shm_used_bytes;  // Used bytes of memory left (that were allocated)
   uint64 shm_magic;       // Magic identifier for shared memory segment
   int    shm_id;          // Shared memory ID returned by shmget()

} PLATFORM_CACHELINE_ALIGNED shmem_info;

/* Permissions for accessing shared memory and IPC objects */
#define PLATFORM_IPC_OBJS_PERMS (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)

/*
 * PLATFORM_HEAP_ID_TO_HANDLE() --
 * PLATFORM_HEAP_ID_TO_SHMADDR() --
 *
 * The shared memory create function returns the address of shmem_info->shm_id
 * as the platform_heap_id heap-ID to the caller. Rest of Splinter will use this
 * heap-ID as a 'handle' to manage / allocate shared memory. This macro converts
 * the heap-ID handle to the shared memory's start address, from which the
 * location of the next-free-byte can be tracked.
 */
static inline platform_heap_handle
platform_heap_id_to_handle(platform_heap_id hid)
{
   return (platform_heap_handle)((void *)hid - offsetof(shmem_info, shm_id));
}

static inline void *
platform_heap_id_to_shmaddr(platform_heap_id hid)
{
   debug_assert(hid != NULL);
   void *shmaddr = (void *)platform_heap_id_to_handle(hid);
   return shmaddr;
}

/*
 * -----------------------------------------------------------------------------
 * platform_shmcreate() -- Create a new / attach to an existing shared segment.
 *
 * For a given set of heap ID/handle, we expect that this create method will
 * only be called once. [ Otherwise, it means some code is erroneously creating
 * the shared segment twice, clobbering previously established handles. ]
 * -----------------------------------------------------------------------------
 */
platform_status
platform_shmcreate(size_t                size,
                   platform_heap_handle *heap_handle,
                   platform_heap_id     *heap_id)
{
   platform_assert(*heap_handle == NULL);
   platform_assert(*heap_id == NULL);

   int shmid = shmget(0, size, (IPC_CREAT | PLATFORM_IPC_OBJS_PERMS));
   if (shmid == -1) {
      platform_error_log(
         "Failed to created shared segment of size %lu bytes (%s).\n",
         size,
         size_str(size));
      return STATUS_NO_MEMORY;
   }
   platform_default_log(
      "Created shared memory of size %lu bytes (%s), shmid=%d.\n",
      size,
      size_str(size),
      shmid);

   // Get start of allocated shared segment
   void *shmaddr = shmat(shmid, NULL, 0);

   if (shmaddr == (void *)-1) {
      platform_error_log("Failed to attach to shared segment, shmid=%d.\n",
                         shmid);
      return STATUS_NO_MEMORY;
   }

   // Setup shared segment's control block at head of shared segment.
   size_t      free_bytes;
   shmem_info *shminfop = (shmem_info *)shmaddr;

   shminfop->shm_start       = shmaddr;
   shminfop->shm_next        = (shmaddr + sizeof(shmem_info));
   shminfop->shm_total_bytes = size;
   free_bytes                = (size - sizeof(shmem_info));
   shminfop->shm_free_bytes  = free_bytes;
   shminfop->shm_used_bytes  = 0;
   shminfop->shm_id          = shmid;
   shminfop->shm_magic       = SPLINTERDB_SHMEM_MAGIC;

   // Return 'heap' handles, if requested, pointing to shared segment handles.
   if (heap_handle) {
      *heap_handle = (platform_heap_handle *)shmaddr;
   }

   if (heap_id) {
      *heap_id = &shminfop->shm_id;
   }

   platform_default_log("Completed setup of shared memory of size "
                        "%lu bytes (%s), shmaddr=%p, shmid=%d,"
                        " available memory = %lu bytes (%s).\n",
                        size,
                        size_str(size),
                        shmaddr,
                        shmid,
                        free_bytes,
                        size_str(free_bytes));
   return STATUS_OK;
}

/*
 * -----------------------------------------------------------------------------
 * platform_shmdestroy() -- Destroy a shared memory created for SplinterDB.
 * -----------------------------------------------------------------------------
 */
void
platform_shmdestroy(platform_heap_handle *heap_handle)
{
   if (!heap_handle) {
      platform_error_log(
         "Error! Attempt to destroy shared memory with NULL heap_handle!");
      return;
   }

   // Establish shared memory handles and validate input addr to shared segment
   const void *shmaddr = (void *)*heap_handle;

   // Heap handle may be coming from the shared segment, itself, that we will
   // be detaching from now and freeing, below. So, an attempt to NULL out
   // this handle after memory is freed will run into an exception. Clear
   // out this handle prior to all this circus.
   *heap_handle = NULL;

   // Use a cached copy in case we are dealing with bogus input shmem address.
   shmem_info shmem_info_struct;
   memmove(&shmem_info_struct, shmaddr, sizeof(shmem_info_struct));

   shmem_info *shminfop = &shmem_info_struct;

   if (shminfop->shm_magic != SPLINTERDB_SHMEM_MAGIC) {
      platform_error_log("Input heap handle, %p, does not seem to be a valid "
                         "SplinterDB shared segment's start address."
                         " Found magic 0x%lX does not match expected"
                         " magic 0x%lX.\n",
                         heap_handle,
                         shminfop->shm_magic,
                         SPLINTERDB_SHMEM_MAGIC);
      return;
   }

   int shmid = shminfop->shm_id;
   int rv    = shmdt(shmaddr);
   if (rv != 0) {
      platform_error_log("Failed to detach from shared segment at address "
                         "%p, shmid=%d.\n",
                         shmaddr,
                         shmid);
      return;
   }

   // Externally, heap_id is pointing to this field. In anticipation that the
   // removal of shared segment will succeed, below, clear this out. This way,
   // any future accesses to this shared segment by its heap-ID will run into
   // assertions.
   shminfop->shm_id = 0;

   rv = shmctl(shmid, IPC_RMID, NULL);
   if (rv != 0) {
      platform_error_log(
         "shmctl failed to remove shared segment at address %p, shmid=%d.\n",
         shmaddr,
         shmid);

      // restore state
      shminfop->shm_id = shmid;
      return;
   }
   platform_default_log(
      "Deallocated shared memory segment at %p, shmid=%d\n", shmaddr, shmid);
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_alloc() -- Allocate n-bytes from shared segment.
 *
 * Allocation request is expected to have added-in pad-bytes required for
 * alignment. As a result, we can assert that the addr-of-next-free-byte is
 * always aligned to PLATFORM_CACHELINE_SIZE.
 * -----------------------------------------------------------------------------
 */
void *
platform_shm_alloc(platform_heap_id hid,
                   const size_t     size,
                   const char      *objname,
                   const char      *func,
                   const char      *file,
                   const int        lineno)
{
   shmem_info *shminfop = platform_heap_id_to_shmaddr(hid);

   debug_assert(
      (platform_shm_heap_handle_valid((platform_heap_handle *)shminfop)
       == TRUE),
      "Shared memory heap ID at %p is not a valid shared memory handle.",
      hid);

   platform_assert(
      ((((uint64)shminfop->shm_next) % PLATFORM_CACHELINE_SIZE) == 0),
      "[%s:%d] Next free-addr is not aligned: "
      "shm_next=%p, shm_total_bytes=%lu, shm_used_bytes=%lu"
      ", shm_free_bytes=%lu",
      file,
      lineno,
      shminfop->shm_next,
      shminfop->shm_total_bytes,
      shminfop->shm_used_bytes,
      shminfop->shm_free_bytes);

   if (shminfop->shm_free_bytes < size) {
      return NULL;
   }
   void *retptr = shminfop->shm_next;

   // Advance next-free-ptr and track memory usage metrics
   shminfop->shm_next += size;
   shminfop->shm_used_bytes += size;
   shminfop->shm_free_bytes -= size;

   return retptr;
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_free() -- 'Free' the memory fragment at given address.
 * -----------------------------------------------------------------------------
 */
void
platform_shm_free(platform_heap_id hid,
                  void            *ptr,
                  const char      *objname,
                  const char      *func,
                  const char      *file,
                  const int        lineno)
{
   return;
}

/*
 * -----------------------------------------------------------------------------
 * Accessor interfaces - mainly intended as testing / debugging hooks.
 * -----------------------------------------------------------------------------
 */
bool
platform_shm_heap_handle_valid(platform_heap_handle heap_handle)
{
   // Establish shared memory handles and validate input addr to shared segment
   const void *shmaddr = (void *)heap_handle;

   // Use a cached copy in case we are dealing with a bogus input shmem address.
   shmem_info shmem_info_struct;
   memmove(&shmem_info_struct, shmaddr, sizeof(shmem_info_struct));

   shmem_info *shminfop = &shmem_info_struct;

   if (shminfop->shm_magic != SPLINTERDB_SHMEM_MAGIC) {
      platform_error_log(
         "Input heap handle, %p, does not seem to be a valid "
         "SplinterDB shared segment's start address."
         " Found magic 0x%lX does not match expected magic 0x%lX.\n",
         heap_handle,
         shminfop->shm_magic,
         SPLINTERDB_SHMEM_MAGIC);
      return FALSE;
   }

   return TRUE;
}

/* Size of control block at start of shared memory describing shared segment */
size_t
platform_shm_ctrlblock_size()
{
   return sizeof(shmem_info);
}

/*
 * Shmem-accessor interfaces by heap_handle.
 */
size_t
platform_shmsize_by_hh(platform_heap_handle heap_handle)
{
   return (platform_shm_heap_handle_valid(heap_handle)
              ? ((shmem_info *)heap_handle)->shm_total_bytes
              : 0);
}

size_t
platform_shmused_by_hh(platform_heap_handle heap_handle)
{
   return (platform_shm_heap_handle_valid(heap_handle)
              ? ((shmem_info *)heap_handle)->shm_used_bytes
              : 0);
}

size_t
platform_shmfree_by_hh(platform_heap_handle heap_handle)
{
   return (platform_shm_heap_handle_valid(heap_handle)
              ? ((shmem_info *)heap_handle)->shm_free_bytes
              : 0);
}

void *
platform_shm_next_free_addr_by_hh(platform_heap_handle heap_handle)
{
   return (platform_shm_heap_handle_valid(heap_handle)
              ? ((shmem_info *)heap_handle)->shm_next
              : NULL);
}

/*
 * Shmem-accessor interfaces by heap_id.
 */
size_t
platform_shmsize(platform_heap_id heap_id)
{
   return (platform_shmsize_by_hh(platform_heap_id_to_handle(heap_id)));
}

size_t
platform_shmused(platform_heap_id heap_id)
{
   return (platform_shmused_by_hh(platform_heap_id_to_handle(heap_id)));
}
size_t
platform_shmfree(platform_heap_id heap_id)
{
   return (platform_shmfree_by_hh(platform_heap_id_to_handle(heap_id)));
}

void *
platform_shm_next_free_addr(platform_heap_id heap_id)
{
   return (
      platform_shm_next_free_addr_by_hh(platform_heap_id_to_handle(heap_id)));
}
