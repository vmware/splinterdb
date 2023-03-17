// Copyright 2018-2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * shmem.c --
 *
 * This file contains the implementation for managing shared memory created
 * for use by SplinterDB and all its innards.
 */
#include <unistd.h>

#include "platform.h"
#include "shmem.h"
#include "util.h"

// SplinterDB's shared segment magic identifier
#define SPLINTERDB_SHMEM_MAGIC (uint64)0xDEFACADE

// Boolean globals controlling tracing of shared memory allocs / frees
bool Trace_shmem_allocs = FALSE;
bool Trace_shmem_frees  = FALSE;
bool Trace_shmem        = FALSE;

/*
 * shm_frag_info{} - Struct describing a memory fragment allocation.
 *
 * This is a specialized memory-fragment tracker solely constructed to satisfy
 * large memory requests. In Splinter large memory requests, i.e. something over
 * 1 M bytes, occur rarely, mainly when we do stuff like compact or pack.
 * And these operations are generally short-lived but could occur very
 * frequently in heavy insert workloads. This mechanism is a way to track a
 * free-list of such memory fragments that were allocated previously and are now
 * 'freed'. They will be re-allocated to the next requestor, thereby, keeping
 * the overall space required in shared memory to be somewhat optimal.
 *
 * NOTE: {to_pid, to_tid} and {by_pid, by_tid} fields go hand-in-hand.
 *       We track both for improved debugging.
 *
 * Lifecyle:
 *  - When a large fragment is initially allocated, frag_addr / frag_size will
 *    be set.
 *  - (allocated_to_pid != 0) && (freed_by_pid == 0) - Fragment is in use.
 *  - (allocated_to_pid != 0) && (freed_by_pid != 0) - Fragment is free.
 */
typedef struct shm_frag_info {
   void    *shm_frag_addr;             // Start address of this memory fragment
   size_t   shm_frag_size;             // bytes
   int      shm_frag_allocated_to_pid; // Allocated to this OS-pid
   threadid shm_frag_allocated_to_tid; // Allocated to this Splinter thread-ID
   int      shm_frag_freed_by_pid;     // OS-pid that freed this
   threadid shm_frag_freed_by_tid;     // Splinter thread-ID that freed this
} shm_frag_info;

/*
 * All memory allocations of this size or larger will be tracked in the
 * above fragment tracker array.
 */
#define SHM_LARGE_FRAG_SIZE (MiB)

/*
 * In the worst case we may have all threads performing activities that need
 * such large memory fragments. We track up to twice the # of configured
 * threads, which is still a small array to search.
 */
#define SHM_NUM_LARGE_FRAGS (MAX_THREADS * 2)

/*
 * -----------------------------------------------------------------------------
 * Core structure describing shared memory segment created. This lives right
 * at the start of the allocated shared segment.
 * -----------------------------------------------------------------------------
 */
typedef struct shmem_info {
   void *shm_start; // Points to start address of shared segment.
   void *shm_end;   // Points to end address; one past end of sh segment
   void *shm_next;  // Points to next 'free' address to allocate from.
   void *shm_splinterdb_handle;

   platform_spinlock shm_mem_frags_lock;
   // Protected by shm_mem_frags_lock. Must hold to read or modify.
   shm_frag_info shm_mem_frags[SHM_NUM_LARGE_FRAGS];
   uint32        shm_num_frags_tracked;
   uint32        shm_num_frags_inuse;
   uint32        shm_num_frags_inuse_HWM;
   size_t        shm_used_by_large_frags; // Actually reserved

   size_t shm_total_bytes; // Total size of shared segment allocated initially.
   size_t shm_free_bytes;  // Free bytes of memory left (that can be allocated)
   size_t shm_used_bytes;  // Used bytes of memory left (that were allocated)
   uint64 shm_magic;       // Magic identifier for shared memory segment
   int    shm_id;          // Shared memory ID returned by shmget()

} PLATFORM_CACHELINE_ALIGNED shmem_info;

/* Permissions for accessing shared memory and IPC objects */
#define PLATFORM_IPC_OBJS_PERMS (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)

// Function prototypes

static void
platform_shm_track_alloc(shmem_info *shm, void *addr, size_t size);

static void
platform_shm_track_free(shmem_info *shm, void *addr);

static void *
platform_shm_find_free(shmem_info *shm,
                       size_t      size,
                       const char *objname,
                       const char *func,
                       const char *file,
                       const int   lineno);

static void
platform_shm_trace_allocs(shmem_info  *shminfo,
                          const size_t size,
                          const char  *verb,
                          const void  *retptr,
                          const char  *objname,
                          const char  *func,
                          const char  *file,
                          const int    lineno);

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

static inline void
shm_lock_mem_frags(shmem_info *shminfo)
{
   platform_spin_lock(&shminfo->shm_mem_frags_lock);
}

static inline void
shm_unlock_mem_frags(shmem_info *shminfo)
{
   platform_spin_unlock(&shminfo->shm_mem_frags_lock);
}

/*
 * Address 'addr' is valid if it's just past end of control block and within
 * shared segment.
 */
static inline bool
platform_valid_addr_in_shm(platform_heap_handle heap_handle, void *addr)
{
   debug_assert(platform_shm_heap_handle_valid(heap_handle),
                "Shared memory heap_handle %p is invalid.\n",
                heap_handle);

   const shmem_info *shmaddr = (shmem_info *)heap_handle;
   return ((addr >= ((void *)shmaddr + platform_shm_ctrlblock_size()))
           && (addr < shmaddr->shm_end));
}

/*
 * platform_valid_addr_in_heap(), platform_valid_addr_in_shm()
 *
 * Validate that input address 'addr' is a valid address within shared segment
 * region.
 */
static inline bool
platform_valid_addr_in_heap(platform_heap_id heap_id, void *addr)
{
   return platform_valid_addr_in_shm(platform_heap_id_to_handle(heap_id), addr);
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
                   platform_heap_handle *heap_handle, // Out
                   platform_heap_id     *heap_id)         // Out
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
   shmem_info *shminfo = (shmem_info *)shmaddr;

   shminfo->shm_start       = shmaddr;
   shminfo->shm_end         = (shmaddr + size);
   shminfo->shm_next        = (shmaddr + sizeof(shmem_info));
   shminfo->shm_total_bytes = size;
   free_bytes               = (size - sizeof(shmem_info));
   shminfo->shm_free_bytes  = free_bytes;
   shminfo->shm_used_bytes  = 0;
   shminfo->shm_id          = shmid;
   shminfo->shm_magic       = SPLINTERDB_SHMEM_MAGIC;

   // Return 'heap' handles, if requested, pointing to shared segment handles.
   if (heap_handle) {
      *heap_handle = (platform_heap_handle *)shmaddr;
   }

   if (heap_id) {
      *heap_id = &shminfo->shm_id;
   }

   // Initialize spinlock needed to access memory fragments tracker
   platform_spinlock_init(
      &shminfo->shm_mem_frags_lock, platform_get_module_id(), *heap_id);

   // Always trace creation of shared memory segment.
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

   shmem_info *shminfo = &shmem_info_struct;

   if (shminfo->shm_magic != SPLINTERDB_SHMEM_MAGIC) {
      platform_error_log("Input heap handle, %p, does not seem to be a valid "
                         "SplinterDB shared segment's start address."
                         " Found magic 0x%lX does not match expected"
                         " magic 0x%lX.\n",
                         heap_handle,
                         shminfo->shm_magic,
                         SPLINTERDB_SHMEM_MAGIC);
      return;
   }

   int shmid = shminfo->shm_id;
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
   shminfo->shm_id = 0;

   // Retain some memory usage stats before releasing shmem
   size_t shm_total_bytes           = shminfo->shm_total_bytes;
   size_t shm_used_bytes            = shminfo->shm_used_bytes;
   size_t shm_free_bytes            = shminfo->shm_free_bytes;
   uint32 frags_inuse_HWM           = shminfo->shm_num_frags_inuse_HWM;
   size_t used_by_large_frags_bytes = shminfo->shm_used_by_large_frags;

   rv = shmctl(shmid, IPC_RMID, NULL);
   if (rv != 0) {
      platform_error_log(
         "shmctl failed to remove shared segment at address %p, shmid=%d.\n",
         shmaddr,
         shmid);

      // restore state
      shminfo->shm_id = shmid;
      return;
   }

   // Reset globals to NULL; to avoid accessing stale handles.
   Heap_id     = NULL;
   Heap_handle = NULL;

   // Always trace destroy of shared memory segment.
   platform_default_log("Deallocated SplinterDB shared memory "
                        "segment at %p, shmid=%d. Used=%lu bytes (%s %d %%)"
                        ", Free=%lu bytes (%s %d %%)"
                        ", Large fragments in-use HWM=%u"
                        ", consumed=%lu bytes (%s)"
                        ".\n",
                        shmaddr,
                        shmid,
                        shm_used_bytes,
                        size_str(shm_used_bytes),
                        (int)((shm_used_bytes * 1.0 / shm_total_bytes) * 100),
                        shm_free_bytes,
                        size_str(shm_free_bytes),
                        (int)((shm_free_bytes * 1.0 / shm_total_bytes) * 100),
                        frags_inuse_HWM,
                        used_by_large_frags_bytes,
                        size_str(used_by_large_frags_bytes));
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
   shmem_info *shminfo = platform_heap_id_to_shmaddr(hid);

   debug_assert(
      (platform_shm_heap_handle_valid((platform_heap_handle *)shminfo) == TRUE),
      "Shared memory heap ID at %p is not a valid shared memory handle.",
      hid);

   platform_assert(
      ((((uint64)shminfo->shm_next) % PLATFORM_CACHELINE_SIZE) == 0),
      "[%s:%d] Next free-addr is not aligned: "
      "shm_next=%p, shm_total_bytes=%lu, shm_used_bytes=%lu"
      ", shm_free_bytes=%lu",
      file,
      lineno,
      shminfo->shm_next,
      shminfo->shm_total_bytes,
      shminfo->shm_used_bytes,
      shminfo->shm_free_bytes);

   void *retptr = NULL;

   // See if we can satisfy requests for large memory fragments from a cached
   // list of used/free fragments that are tracked separately.
   if ((size >= SHM_LARGE_FRAG_SIZE)
       && ((retptr = platform_shm_find_free(
               shminfo, size, objname, func, file, lineno))
           != NULL))
   {
      return retptr;
   }

   _Static_assert(sizeof(void *) == sizeof(size_t),
                  "check our casts are valid");
   // Optimistically, allocate the requested 'size' bytes of memory.
   retptr = __sync_fetch_and_add(&shminfo->shm_next, (void *)size);

   if (shminfo->shm_next >= shminfo->shm_end) {
      // This memory request cannot fit in available space. Reset.
      __sync_fetch_and_sub(&shminfo->shm_next, (void *)size);

      platform_error_log(
         "[%s:%d::%s()]: Insufficient memory in shared segment"
         " to allocate %lu bytes for '%s'. Approx free space=%lu bytes."
         " shm_num_frags_tracked=%u, shm_num_frags_inuse=%u (HWM=%u).\n",
         file,
         lineno,
         func,
         size,
         objname,
         shminfo->shm_free_bytes,
         shminfo->shm_num_frags_tracked,
         shminfo->shm_num_frags_inuse,
         shminfo->shm_num_frags_inuse_HWM);
      return NULL;
   }
   // Track approx memory usage metrics; mainly for troubleshooting
   __sync_fetch_and_add(&shminfo->shm_used_bytes, size);
   __sync_fetch_and_sub(&shminfo->shm_free_bytes, size);

   if (size >= SHM_LARGE_FRAG_SIZE) {
      platform_shm_track_alloc(shminfo, retptr, size);
   }

   // Trace shared memory allocation; then return memory ptr.
   if (Trace_shmem || Trace_shmem_allocs) {
      platform_shm_trace_allocs(shminfo,
                                size,
                                "Allocated new fragment",
                                retptr,
                                objname,
                                func,
                                file,
                                lineno);
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
   shmem_info *shminfo = platform_heap_id_to_shmaddr(hid);

   debug_assert(
      (platform_shm_heap_handle_valid((platform_heap_handle *)shminfo) == TRUE),
      "Shared memory heap ID at %p is not a valid shared memory handle.",
      hid);

   if (!platform_valid_addr_in_heap(hid, ptr)) {
      platform_error_log("[%s:%d::%s()] -> %s: Requesting to free memory"
                         " at %p, for object '%s' which is a memory chunk not"
                         " allocated from shared memory {start=%p, end=%p}.\n",
                         file,
                         lineno,
                         func,
                         __FUNCTION__,
                         ptr,
                         objname,
                         platform_shm_lop(hid),
                         platform_shm_hip(hid));
   }

   platform_shm_track_free(shminfo, ptr);

   if (Trace_shmem || Trace_shmem_frees) {
      platform_default_log("  [%s:%d::%s()] -> %s: Request to free memory at "
                           "%p for object '%s'.\n",
                           file,
                           lineno,
                           func,
                           __func__,
                           ptr,
                           objname);
   }
   return;
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_track_alloc() - Track the allocation of this large fragment.
 * -----------------------------------------------------------------------------
 */
static void
platform_shm_track_alloc(shmem_info *shm, void *addr, size_t size)
{
   debug_assert(
      (size >= SHM_LARGE_FRAG_SIZE),
      "Incorrect usage of this interface for requested size=%lu bytes."
      " Size should be >= %lu bytes.\n",
      size,
      SHM_LARGE_FRAG_SIZE);

   shm_lock_mem_frags(shm);

   shm_frag_info *frag = shm->shm_mem_frags;
   for (int fctr = 0; fctr < ARRAY_SIZE(shm->shm_mem_frags); fctr++, frag++) {

      if (frag->shm_frag_addr) {
         // As this is a newly allocated fragment being tracked, it should
         // not be found elsewhere in the tracker array.
         platform_assert((frag->shm_frag_addr != addr),
                         "Error! Newly allocated large memory fragment at %p"
                         " is already tracked at slot %d."
                         " Fragment is allocated to PID=%d, to tid=%lu,"
                         " and is %s (freed by PID=%d, by tid=%lu)\n.",
                         addr,
                         fctr,
                         frag->shm_frag_allocated_to_pid,
                         frag->shm_frag_allocated_to_tid,
                         (frag->shm_frag_freed_by_pid ? "free" : "in use"),
                         frag->shm_frag_freed_by_pid,
                         frag->shm_frag_freed_by_tid);
         continue;
      }

      shm->shm_num_frags_tracked++;
      shm->shm_num_frags_inuse++;
      shm->shm_used_by_large_frags += size;
      if (shm->shm_num_frags_inuse > shm->shm_num_frags_inuse_HWM) {
         shm->shm_num_frags_inuse_HWM = shm->shm_num_frags_inuse;
      }

      // We should really assert that the other fields are zero, but for now
      // re-init this fragment tracker.
      memset(frag, 0, sizeof(*frag));
      frag->shm_frag_addr = addr;
      frag->shm_frag_size = size;

      frag->shm_frag_allocated_to_pid = getpid();
      frag->shm_frag_allocated_to_tid = platform_get_tid();
      // The freed_by_pid/freed_by_tid == 0 means fragment is still allocated.
      break;
   }
   shm_unlock_mem_frags(shm);
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_track_free() - See if this memory fragment being freed is
 * already being tracked. If so, it's a large fragment allocation, which can be
 * re-cycled after this free. Do the book-keeping accordingly.
 * -----------------------------------------------------------------------------
 */
static void
platform_shm_track_free(shmem_info *shm, void *addr)
{
   shm_lock_mem_frags(shm);

   shm_frag_info *frag = shm->shm_mem_frags;
   for (int fctr = 0; fctr < ARRAY_SIZE(shm->shm_mem_frags); fctr++, frag++) {
      if (!frag->shm_frag_addr || (frag->shm_frag_addr != addr)) {
         continue;
      }

      // Record the process/thread that's doing the free.
      frag->shm_frag_freed_by_pid = getpid();
      frag->shm_frag_freed_by_tid = platform_get_tid();

      if (Trace_shmem || Trace_shmem_frees) {
         platform_default_log("OS-pid=%d, ThreadID=%lu"
                              ", Track freed fragment at slot=%d"
                              ", addr=%p"
                              ", allocated_to_pid=%d"
                              ", allocated_to_tid=%lu\n",
                              frag->shm_frag_freed_by_pid,
                              frag->shm_frag_freed_by_tid,
                              fctr,
                              addr,
                              frag->shm_frag_allocated_to_pid,
                              frag->shm_frag_allocated_to_tid);
      }
      break;
   }
   shm_unlock_mem_frags(shm);
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_find_free() - See if there is an already allocated and now free
 * large memory fragment. If so, allocate that to this requester.Do the
 * book-keeping accordingly.
 * -----------------------------------------------------------------------------
 */
static void *
platform_shm_find_free(shmem_info *shm,
                       size_t      size,
                       const char *objname,
                       const char *func,
                       const char *file,
                       const int   lineno)
{
   debug_assert(
      (size >= SHM_LARGE_FRAG_SIZE),
      "Incorrect usage of this interface for requested size=%lu bytes."
      " Size should be >= %lu bytes.\n",
      size,
      SHM_LARGE_FRAG_SIZE);

   void          *retptr   = NULL;
   shm_frag_info *frag     = shm->shm_mem_frags;
   int  local_in_use       = 0; // Tracked while iterating in this fn, locally
   bool found_tracked_frag = FALSE;

   shm_lock_mem_frags(shm);

   for (int fctr = 0; fctr < ARRAY_SIZE(shm->shm_mem_frags); fctr++, frag++) {
      if (!frag->shm_frag_addr || (frag->shm_frag_size < size)) {
         continue;
      }

      // Skip fragment if it's still in-use
      if (frag->shm_frag_freed_by_pid == 0) {
         platform_assert((frag->shm_frag_freed_by_tid == 0),
                         "Invalid state found for fragment at index %d,"
                         "freed_by_pid=%d but freed_by_tid=%lu "
                         "(which should also be 0)\n",
                         fctr,
                         frag->shm_frag_freed_by_pid,
                         frag->shm_frag_freed_by_tid);

         local_in_use++;
         continue;
      }

      // Fragment is free, but is it big enough for current request?
      if (frag->shm_frag_size < size) {
         continue;
      }
      found_tracked_frag = TRUE;

      // Record the process/thread to which free fragment is being allocated
      frag->shm_frag_allocated_to_pid = getpid();
      frag->shm_frag_allocated_to_tid = platform_get_tid();

      shm->shm_num_frags_inuse++;
      if (shm->shm_num_frags_inuse > shm->shm_num_frags_inuse_HWM) {
         shm->shm_num_frags_inuse_HWM = shm->shm_num_frags_inuse;
      }

      // Now, mark that this fragment is in-use
      frag->shm_frag_freed_by_pid = 0;
      frag->shm_frag_freed_by_tid = 0;

      retptr = frag->shm_frag_addr;

      // Zero out reallocated memory fragment, just to be sure ...
      memset(retptr, 0, frag->shm_frag_size);

      if (Trace_shmem || Trace_shmem_allocs) {
         char msg[80];

         snprintf(
            msg, sizeof(msg), "Reallocated free fragment at slot=%d", fctr);
         platform_shm_trace_allocs(
            shm, size, msg, retptr, objname, func, file, lineno);
      }
      break;
   }
   shm_unlock_mem_frags(shm);

   if (!found_tracked_frag && (Trace_shmem || Trace_shmem_allocs)) {
      char msg[200];

      snprintf(msg,
               sizeof(msg),
               "Did not find free fragment of size=%lu bytes to reallocate."
               " shm_num_frags_tracked=%u, shm_num_frags_inuse=%u"
               " (local_in_use=%d)",
               size,
               shm->shm_num_frags_tracked,
               shm->shm_num_frags_inuse,
               local_in_use);

      platform_shm_trace_allocs(
         shm, size, msg, retptr, objname, func, file, lineno);
   }
   return retptr;
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

   shmem_info *shminfo = &shmem_info_struct;

   if (shminfo->shm_magic != SPLINTERDB_SHMEM_MAGIC) {
      platform_error_log(
         "Input heap handle, %p, does not seem to be a valid "
         "SplinterDB shared segment's start address."
         " Found magic 0x%lX does not match expected magic 0x%lX.\n",
         heap_handle,
         shminfo->shm_magic,
         SPLINTERDB_SHMEM_MAGIC);
      return FALSE;
   }

   return TRUE;
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
 *  ops == allocs & frees.
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

static void
platform_shm_trace_allocs(shmem_info  *shminfo,
                          const size_t size,
                          const char  *verb,
                          const void  *retptr,
                          const char  *objname,
                          const char  *func,
                          const char  *file,
                          const int    lineno)
{
   platform_default_log("  [OS-pid=%d,ThreadID=%lu, %s:%d::%s()] "
                        "-> %s: %s %lu bytes (%s)"
                        " for object '%s', at %p, "
                        "free bytes=%lu (%s).\n",
                        getpid(),
                        platform_get_tid(),
                        file,
                        lineno,
                        func,
                        __func__,
                        verb,
                        size,
                        size_str(size),
                        objname,
                        retptr,
                        shminfo->shm_free_bytes,
                        size_str(shminfo->shm_free_bytes));
}
