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
 * -----------------------------------------------------------------------------
 * platform_shmcreate() -- Create a new / attach to an existing shared segment.
 * -----------------------------------------------------------------------------
 */
platform_status
platform_shmcreate(size_t                size,
                   platform_heap_handle *heap_handle,
                   platform_heap_id     *heap_id)
{
   int shmid = shmget(0, size, (IPC_CREAT | PLATFORM_IPC_OBJS_PERMS));
   if (shmid == -1) {
      platform_error_log(
         "Failed to created shared segment of size %lu bytes.\n", size);
      return STATUS_NO_MEMORY;
   }
   platform_default_log(
      "Created shared memory of size %lu bytes (%lu GiB), shmid=%d.\n",
      size,
      B_TO_GiB(size),
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

   platform_default_log("Completed setup of shared memory of size %lu bytes, "
                        "shmaddr=%p, shmid=%d,"
                        " available memory = %lu bytes (~%lu.%d GiB).\n",
                        size,
                        shmaddr,
                        shmid,
                        free_bytes,
                        B_TO_GiB(free_bytes),
                        B_TO_GiB_FRACT(free_bytes));
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

   // Use a cached copy in case we are dealing with bogus input shmem address.
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
      return;
   }

   int shmid = shminfop->shm_id;
   int rv    = shmdt(shmaddr);
   if (rv != 0) {
      platform_error_log(
         "Failed to detach from shared segment at address %p, shmid=%d.\n",
         shmaddr,
         shmid);
      return;
   }

   rv = shmctl(shmid, IPC_RMID, NULL);
   if (rv != 0) {
      platform_error_log(
         "shmctl failed to remove shared segment at address %p, shmid=%d.\n",
         shmaddr,
         shmid);
      return;
   }
}
