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

// Boolean globals controlling tracing of shared memory allocs / frees
bool Trace_shmem_allocs = FALSE;
bool Trace_shmem_frees  = FALSE;
bool Trace_shmem        = FALSE;

/*
 * -----------------------------------------------------------------------------
 * Core structure describing shared memory segment created. This lives right
 * at the start of the allocated shared segment.
 * -----------------------------------------------------------------------------
 */
typedef struct shmem_info {
   void  *shm_start; // Points to start address of shared segment.
   void  *shm_end;   // Points to end address; one past end of sh segment
   void  *shm_next;  // Points to next 'free' address to allocate from.
   void  *shm_splinterdb_handle;
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
 *
 * RESOLVE - This ptr-math going back from address of heap-ID is prone to
 *errors. In some code paths, e.g. tests/unit/btree_test.c, we pass-down the
 *stack address of an on-stack scratch-buffer masquerading as the heap-ID
 *handle.
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

/* Evaluates to valid 'low' address within shared segment. */
static inline void *
platform_shm_lop(platform_heap_id hid)
{
   return (void *)platform_heap_id_to_handle(hid);
}

/* Evaluates to valid 'high' address within shared segment. */
static inline void *
platform_shm_hip(platform_heap_id hid)
{
   return (((shmem_info *)platform_heap_id_to_shmaddr(hid))->shm_end - 1);
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
   platform_assert((*heap_handle == NULL),
                   "Heap handle is expected to be NULL while creating a new "
                   "shared segment.\n");
   platform_assert((*heap_id == NULL),
                   "Heap handle is expected to be NULL while creating a new "
                   "shared segment.\n");

   int shmid = shmget(0, size, (IPC_CREAT | PLATFORM_IPC_OBJS_PERMS));
   if (shmid == -1) {
      platform_error_log(
         "Failed to created shared segment of size %lu bytes.\n", size);
      return STATUS_NO_MEMORY;
   }
   platform_default_log(
      "Created shared memory of size %lu bytes, shmid=%d.\n", size, shmid);

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
   shminfop->shm_end         = (shmaddr + size);
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

   // Always trace creation of shared memory segment.
   bool        use_MiB = (size < GiB);
   const char *msg =
      "Completed setup of SplinterDB shared memory of size %lu bytes (%lu %s), "
      "shmaddr=%p, shmid=%d,"
      " available memory = %lu bytes (~%lu.%d %s).\n";
   if (use_MiB) {
      platform_default_log(msg,
                           size,
                           B_TO_MiB(size),
                           "MiB",
                           shmaddr,
                           shmid,
                           free_bytes,
                           B_TO_MiB(free_bytes),
                           B_TO_MiB_FRACT(free_bytes),
                           "MiB");
   } else {
      platform_default_log(msg,
                           size,
                           B_TO_GiB(size),
                           "GiB",
                           shmaddr,
                           shmid,
                           free_bytes,
                           B_TO_GiB(free_bytes),
                           B_TO_GiB_FRACT(free_bytes),
                           "GiB");
   }
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

   // Externally, heap_id is pointing to this field. In anticipation that the
   // removal of shared segment will succeed, below, clear this out. This way,
   // any future accesses to this shared segment by its heap-ID will run into
   // assertions.
   shminfop->shm_id = 0;

   // Retain some memory usage stages before release shmem
   size_t shm_total_bytes = shminfop->shm_total_bytes;
   size_t shm_used_bytes  = shminfop->shm_used_bytes;
   size_t shm_free_bytes  = shminfop->shm_free_bytes;

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

   // Reset globals to NULL; to avoid accessing stale handles.
   Heap_id     = NULL;
   Heap_handle = NULL;

   // Always trace destroy of shared memory segment.
   platform_default_log("Deallocated SplinterDB shared memory "
                        "segment at %p, shmid=%d."
                        " Used=%lu bytes (%d %%), Free=%lu bytes (%d %%).\n",
                        shmaddr,
                        shmid,
                        shm_used_bytes,
                        (int)((shm_used_bytes * 1.0 / shm_total_bytes) * 100),
                        shm_free_bytes,
                        (int)((shm_free_bytes * 1.0 / shm_total_bytes) * 100));
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
      platform_error_log(
         "[%s:%d::%s()]: Insufficient memory in shared segment"
         " to allocate %lu bytes for '%s'. Available space=%lu bytes.\n",
         file,
         lineno,
         func,
         size,
         objname,
         shminfop->shm_free_bytes);
      return NULL;
   }
   void *retptr = shminfop->shm_next;

   // Advance next-free-ptr and track memory usage metrics
   shminfop->shm_next += size;
   shminfop->shm_used_bytes += size;
   shminfop->shm_free_bytes -= size;

   // Trace shared memory allocation; then return memory ptr.
   if (Trace_shmem || Trace_shmem_allocs) {
      bool        use_MiB = (shminfop->shm_free_bytes < GiB);
      const char *msg     = "  [%s:%d::%s()] -> %s: Allocated %lu bytes "
                            "for object '%s', at %p, "
                            "free bytes=%lu (~%lu.%d %s).\n";
      if (use_MiB) {
         platform_default_log(msg,
                              file,
                              lineno,
                              func,
                              __FUNCTION__,
                              size,
                              objname,
                              retptr,
                              shminfop->shm_free_bytes,
                              B_TO_MiB(shminfop->shm_free_bytes),
                              B_TO_MiB_FRACT(shminfop->shm_free_bytes),
                              "MiB");
      } else {
         platform_default_log(msg,
                              file,
                              lineno,
                              func,
                              __FUNCTION__,
                              size,
                              objname,
                              retptr,
                              shminfop->shm_free_bytes,
                              B_TO_GiB(shminfop->shm_free_bytes),
                              B_TO_GiB_FRACT(shminfop->shm_free_bytes),
                              "GiB");
      }
   }
   return retptr;
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_realloc() -- Re-allocate n-bytes from shared segment.
 *
 * Functionally is similar to 'realloc' system call. We do a fake free and then
 * allocate required # of bytes.
 */
void *
platform_shm_realloc(platform_heap_id hid,
                     void            *oldptr,
                     const size_t     size,
                     const char      *func,
                     const char      *file,
                     const int        lineno)
{
   if (oldptr) {
      splinter_shm_free(hid, oldptr, "Unknown");
   }
   return splinter_shm_alloc(hid, size, "Unknown");
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_free() -- 'Free' the memory fragment at given address in shmem.
 *
 * We expect that the 'ptr' is a valid address within the shared segment.
 * Otherwise, it means that Splinter was configured to run with shared memory,
 * -and- in some code path we allocated w/o using shared memory
 * (i.e. NULL_HEAP_ID interface), but ended up calling shmem-free interface.
 * That would be a code error that results in a memory leak.
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
   /*
    * RESOLVE: This handling is broken but just gets us going through tests.
        * There is at least one instance where we trip up while running
        *
        * $ ASAN_OPTIONS=detect_odr_violation=0
    build/debug-asan/bin/unit/splinter_test --use-shmem test_inserts
        *
        * with this message:
        *
        [src/trunk.c:4031::trunk_bundle_build_filters()] -> platform_shm_free:
    Requesting to free memory at 0x7fbeb991b800, for object
    'compact_req->fp_arr' which is a memory chunk not allocated from shared
    memory {start=0x7fbf09e00000, end=0x7fbf89dfffff}.
        */
   if (!platform_valid_addr_in_heap(hid, ptr)) {
      platform_error_log(
         "[%s:%d::%s()] -> %s: Requesting to free memory at %p,"
         " for object '%s' which is a memory chunk not allocated"
         " from shared memory {start=%p, end=%p}.\n",
         file,
         lineno,
         func,
         __FUNCTION__,
         ptr,
         objname,
         platform_shm_lop(hid),
         platform_shm_hip(hid));

      platform_free_from_heap(NULL_HEAP_ID, ptr, objname, func, file, lineno);
   }

   if (Trace_shmem || Trace_shmem_frees) {
      platform_default_log("  [%s:%d::%s()] -> %s: Request to free memory at "
                           "%p for object '%s'.\n",
                           file,
                           lineno,
                           func,
                           __FUNCTION__,
                           ptr,
                           objname);
   }

   return;
}

/*
 * -----------------------------------------------------------------------------
 * Accessor interfaces - mainly intended as assert / testing / debugging hooks.
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

/*
 * platform_valid_addr_in_heap(), platform_valid_addr_in_shm()
 *
 * Validate that input address 'addr' is a valid address within shared segment
 * region.
 */
bool
platform_valid_addr_in_heap(platform_heap_id heap_id, const void *addr)
{
   return platform_valid_addr_in_shm(platform_heap_id_to_handle(heap_id), addr);
}

/*
 * Address 'addr' is valid if it's just past end of control block and within
 * shared segment.
 */
bool
platform_valid_addr_in_shm(platform_heap_handle heap_handle, const void *addr)
{
   debug_assert(platform_shm_heap_handle_valid(heap_handle),
                "Shared memory heap_handle %p is invalid.\n",
                heap_handle);

   const shmem_info *shmaddr = (shmem_info *)heap_handle;
   return ((addr >= ((void *)shmaddr + platform_shm_ctrlblock_size()))
           && (addr < shmaddr->shm_end));
}

/*
 * -----------------------------------------------------------------------------
 * Warning! Testing interfaces, which are written to support verification of
 * splinterdb handle from forked child processes when running Splinter
 * configured with shared-segment. These interfaces are provided mainly as
 * a diagnostic & testing hooks.
 *
 * platform_heap_set_splinterdb_handle() - Save-off the handle to splinterdb *
 *    in the shared segment's control block.
 *
 * platform_heap_get_splinterdb_handle() - Return the handle to splinterdb *
 *    saved-off in the shared segment's control block.
 * -----------------------------------------------------------------------------
 */
void
platform_shm_set_splinterdb_handle(platform_heap_handle heap_handle, void *addr)
{
   debug_assert(platform_shm_heap_handle_valid(heap_handle));
   shmem_info *shmaddr            = (shmem_info *)heap_handle;
   shmaddr->shm_splinterdb_handle = addr;
}

void *
platform_shm_get_splinterdb_handle(const platform_heap_handle heap_handle)
{
   debug_assert(platform_shm_heap_handle_valid(heap_handle));
   shmem_info *shmaddr = (shmem_info *)heap_handle;
   return shmaddr->shm_splinterdb_handle;
}

/*
 * Initialize tracing of shared memory allocs / frees. This is invoked as a
 * result of parsing command-line args:
 */
void
platform_shm_tracing_init(const bool trace_shmem,
                          const bool trace_shmem_allocs,
                          const bool trace_shmem_frees)
{
   if (trace_shmem) {
      Trace_shmem = TRUE;
   }
   if (trace_shmem_allocs) {
      Trace_shmem_allocs = TRUE;
   }
   if (trace_shmem_frees) {
      Trace_shmem_frees = TRUE;
   }
}

/*
 * Action-methods to enable / disable tracing of shared memory operations:
 *  ops == allocs & frees. These
 */
void
platform_enable_tracing_shm_ops()
{
   Trace_shmem = TRUE;
}

void
platform_enable_tracing_shm_allocs()
{
   Trace_shmem_allocs = TRUE;
}

void
platform_enable_tracing_shm_frees()
{
   Trace_shmem_frees = TRUE;
}

void
platform_disable_tracing_shm_ops()
{
   Trace_shmem        = FALSE;
   Trace_shmem_allocs = FALSE;
   Trace_shmem_frees  = FALSE;
}

void
platform_disable_tracing_shm_allocs()
{
   Trace_shmem_allocs = FALSE;
}

void
platform_disable_tracing_shm_frees()
{
   Trace_shmem_frees = FALSE;
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
