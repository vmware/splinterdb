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
bool Trace_large_frags  = FALSE;

/*
 * ----------------------------------------------------------------------------
 * shm_frag_info{} - Struct describing a [large] memory fragment allocation.
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
 * ----------------------------------------------------------------------------
 */
typedef struct shm_frag_info {
   void    *shm_frag_addr;             // Start address of this memory fragment
   size_t   shm_frag_size;             // bytes
   int      shm_frag_allocated_to_pid; // Allocated to this OS-pid
   threadid shm_frag_allocated_to_tid; // Allocated to this Splinter thread-ID
   int      shm_frag_freed_by_pid;     // OS-pid that freed this
   threadid shm_frag_freed_by_tid;     // Splinter thread-ID that freed this

#if SPLINTER_DEBUG

   timestamp shm_alloc_ts;        // Timestamp when frag was alloc / realloc'ed
   uint64    shm_elapsed_free_ns; // Elapsed nanos between alloc & free.

#endif // SPLINTER_DEBUG

} shm_frag_info;

/*
 * ----------------------------------------------------------------------------
 * Design of free / used memory fragment list manager:
 *
 * - struct shm_memfrag{} describes a memory fragment. It has:
 *    - A 'links' field to manage this fragment via doubly-linked list.
 *    - Size
 *
 * - Allocated memory addr 'ptr' starts right at end of shm_memfrag{}
 *
 * - shmem_info{} has two SHM_QUEUE fields: free_list, used_list
 *   - Also has ptr to some # of pre-allocated shm_memfrag{} structs [cache]
 *
 * - Spinlocks:
 *    - used_list_lock
 *    - free_list_lock
 *
 * - Allocation: void * shm_alloc(size):
 *    - Search free_list. If a fragment with sufficient space is found, use it.
 *    - Grab a free shm_memfrag struct.
 *    - Fill it out: sets addr / size
 *    - Insert new fragment to used_list at the head.
 *
 * - Free: shm_free(ptr):
 *    - Do ptr-math to get fragptr given void * ptr to free
 *    - Remove fragment from used_list
 *    - Insert fragment to free_list
 * ----------------------------------------------------------------------------
 */

/*
 * All memory allocations of this size or larger will be tracked in the
 * above fragment tracker array. For large inserts workload, we allocate large
 * memory chunks for fingerprint array, which is more than a MiB. For scans,
 * splinterdb_iterator_init() allocates memory for an iterator which is ~92+KiB.
 * Set this to a lower value so we can re-cycle free fragments for iterators
 * also.
 */
#if SPLINTER_DEBUG
#   define SHM_LARGE_FRAG_SIZE (90 * KiB)
#else
#   define SHM_LARGE_FRAG_SIZE (38 * KiB)
#endif // SPLINTER_DEBUG

/*
 * In the worst case we may have all threads performing activities that need
 * such large memory fragments. We track up to twice the # of configured
 * threads, which is still a small array to search.
 */
#define SHM_NUM_LARGE_FRAGS (MAX_THREADS * 2)

/*
 * -----------------------------------------------------------------------------
 * shmem_info{}: Shared memory Control Block:
 *
 * Core structure describing shared memory segment created. This lives right
 * at the start of the allocated shared segment.
 *
 * NOTE(s):
 *  - shm_large_frag_hip tracks the highest-address of all the large fragments
 *    that are tracked. This is an optimization to short-circuit the search
 *    done to see if a fragment being freed is a large-fragment.
 *  - shm_nf_search_skipped counts the # of times we were able to short-circuit
 *    this search using above optimization.
 * -----------------------------------------------------------------------------
 */
typedef struct shmem_info {
   void *shm_start; // Points to start address of shared segment.
   void *shm_end;   // Points to end address; one past end of sh segment
   void *shm_next;  // Points to next 'free' address to allocate from.
   void *shm_splinterdb_handle;
   void *shm_large_frag_hip; // Highest addr of large-fragments tracked

   platform_spinlock shm_mem_frags_lock;
   // Protected by shm_mem_frags_lock. Must hold to read or modify.
   shm_frag_info shm_mem_frags[SHM_NUM_LARGE_FRAGS];
   uint32        shm_num_frags_tracked;
   uint32        shm_num_frags_inuse;
   uint32        shm_num_frags_inuse_HWM;
   size_t        shm_used_by_large_frags; // Actually reserved
   size_t        shm_nfrees;              // # of calls to free memory
   size_t        shm_nf_search_skipped;   // See above;

   size_t shm_total_bytes; // Total size of shared segment allocated initially.
   int64  shm_free_bytes;  // Free bytes of memory left (that can be allocated)
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
platform_shm_track_free(shmem_info *shm,
                        void       *addr,
                        const char *objname,
                        const char *func,
                        const char *file,
                        const int   lineno);

static void *
platform_shm_find_free(shmem_info *shm,
                       size_t      size,
                       const char *objname,
                       const char *func,
                       const char *file,
                       const int   lineno);

static void
platform_shm_trace_allocs(shmem_info  *shminfop,
                          const size_t size,
                          const char  *verb,
                          const void  *retptr,
                          const char  *objname,
                          const char  *func,
                          const char  *file,
                          const int    lineno);

static int
platform_trace_large_frags(shmem_info *shm);

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
 * errors. In some code paths, e.g. tests/unit/btree_test.c, we pass-down the
 * stack address of an on-stack scratch-buffer masquerading as the heap-ID
 * handle.
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

_Pragma("GCC diagnostic push")
   _Pragma("GCC diagnostic ignored \"-Wunused-function\"")
   /* Evaluates to valid 'low' address within shared segment. */
   static inline void *platform_shm_lop(platform_heap_id hid)
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
shm_lock_mem_frags(shmem_info *shminfop)
{
   platform_spin_lock(&shminfop->shm_mem_frags_lock);
}

static inline void
shm_unlock_mem_frags(shmem_info *shminfop)
{
   platform_spin_unlock(&shminfop->shm_mem_frags_lock);
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
   shmem_info *shminfop = (shmem_info *)shmaddr;
   memset(shminfop, 0, sizeof(*shminfop));

   shminfop->shm_start       = shmaddr;
   shminfop->shm_end         = (shmaddr + size);
   shminfop->shm_next        = (shmaddr + sizeof(shmem_info));
   shminfop->shm_total_bytes = size;

   int64 free_bytes;
   free_bytes               = (size - sizeof(shmem_info));
   shminfop->shm_free_bytes = free_bytes;
   shminfop->shm_used_bytes = 0;
   shminfop->shm_id         = shmid;
   shminfop->shm_magic      = SPLINTERDB_SHMEM_MAGIC;

   // Return 'heap' handles, if requested, pointing to shared segment handles.
   if (heap_handle) {
      *heap_handle = (platform_heap_handle *)shmaddr;
   }

   if (heap_id) {
      *heap_id = &shminfop->shm_id;
   }

   // Initialize spinlock needed to access memory fragments tracker
   platform_spinlock_init(
      &shminfop->shm_mem_frags_lock, platform_get_module_id(), *heap_id);

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

   // Retain some memory usage stats before releasing shmem
   size_t shm_total_bytes           = shminfop->shm_total_bytes;
   size_t shm_used_bytes            = shminfop->shm_used_bytes;
   int64  shm_free_bytes            = shminfop->shm_free_bytes;
   uint32 frags_inuse_HWM           = shminfop->shm_num_frags_inuse_HWM;
   size_t used_by_large_frags_bytes = shminfop->shm_used_by_large_frags;
   size_t shm_nfrees                = shminfop->shm_nfrees;
   size_t shm_nf_search_skipped     = shminfop->shm_nf_search_skipped;

   // Print large-fragment tracking metrics (for deep debugging)
   int found_in_use = platform_trace_large_frags(shminfop);

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
   char used_bytes_str[SIZE_TO_STR_LEN];
   char free_bytes_str[SIZE_TO_STR_LEN];
   char used_by_large_frags_bytes_str[SIZE_TO_STR_LEN];

   size_to_str(used_bytes_str, SIZE_TO_STR_LEN, shm_used_bytes);
   size_to_str(free_bytes_str, SIZE_TO_STR_LEN, shm_free_bytes);
   size_to_str(used_by_large_frags_bytes_str,
               SIZE_TO_STR_LEN,
               used_by_large_frags_bytes);

   platform_default_log("Deallocated SplinterDB shared memory "
                        "segment at %p, shmid=%d."
                        " Used=%lu bytes (%s, ~%d %%)"
                        ", Free=%lu bytes (%s, ~%d %%)"
                        ".\n",
                        shmaddr,
                        shmid,
                        shm_used_bytes,
                        used_bytes_str,
                        pctof(shm_used_bytes, shm_total_bytes),
                        shm_free_bytes,
                        free_bytes_str,
                        pctof(shm_free_bytes, shm_total_bytes));

   platform_default_log("SplinterDB shared memory stats:"
                        " nfrees=%lu, nf_search_skipped=%lu (%d %%)"
                        ", Large fragments in-use HWM=%u (found in-use=%d)"
                        ", consumed=%lu bytes (%s)"
                        ".\n",
                        shm_nfrees,
                        shm_nf_search_skipped,
                        pctof(shm_nf_search_skipped, shm_nfrees),
                        frags_inuse_HWM,
                        found_in_use,
                        used_by_large_frags_bytes,
                        used_by_large_frags_bytes_str);
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_alloc() -- Allocate n-bytes from shared memory segment.
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

   void *retptr = NULL;

   // See if we can satisfy requests for large memory fragments from a cached
   // list of used/free fragments that are tracked separately.
   if ((size >= SHM_LARGE_FRAG_SIZE)
       && ((retptr = platform_shm_find_free(
               shminfop, size, objname, func, file, lineno))
           != NULL))
   {
      return retptr;
   }

   // Optimistically, allocate the requested 'size' bytes of memory.
   _Static_assert(sizeof(void *) == sizeof(size_t),
                  "check our casts are valid");
   retptr = __sync_fetch_and_add(&shminfop->shm_next, (void *)size);

   if (shminfop->shm_next > shminfop->shm_end) {
      // This memory request cannot fit in available space. Reset.
      __sync_fetch_and_sub(&shminfop->shm_next, (void *)size);

      char size_str[SIZE_TO_STR_LEN];
      char free_bytes_str[SIZE_TO_STR_LEN];
      size_to_str(size_str, sizeof(size_str), size);
      size_to_str(
         free_bytes_str, sizeof(free_bytes_str), shminfop->shm_free_bytes);

      platform_error_log("[%s:%d::%s()]: Insufficient memory in shared segment"
                         " to allocate %lu bytes (%s) for '%s'."
                         " Approx free space=%lu bytes (%s)."
                         " shm_num_frags_tracked=%u, shm_num_frags_inuse=%u "
                         "(in-use-HWM=%u).\n",
                         file,
                         lineno,
                         func,
                         size,
                         size_str,
                         objname,
                         shminfop->shm_free_bytes,
                         free_bytes_str,
                         shminfop->shm_num_frags_tracked,
                         shminfop->shm_num_frags_inuse,
                         shminfop->shm_num_frags_inuse_HWM);
      return NULL;
   }
   // Track approx memory usage metrics; mainly for troubleshooting
   __sync_fetch_and_add(&shminfop->shm_used_bytes, size);
   __sync_fetch_and_sub(&shminfop->shm_free_bytes, size);

   if (size >= SHM_LARGE_FRAG_SIZE) {
      platform_shm_track_alloc(shminfop, retptr, size);
   }

   // Trace shared memory allocation; then return memory ptr.
   if (Trace_shmem || Trace_shmem_allocs
       || (Trace_large_frags && (size >= SHM_LARGE_FRAG_SIZE)))
   {

      platform_shm_trace_allocs(shminfop,
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
      splinter_shm_free(hid, oldptr, "Unknown-realloc");
   }
   return splinter_shm_alloc(hid, size, "Unknown-realloc");
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
   shmem_info *shminfop = platform_heap_id_to_shmaddr(hid);

   debug_assert(
      (platform_shm_heap_handle_valid((platform_heap_handle *)shminfop)
       == TRUE),
      "Shared memory heap ID at %p is not a valid shared memory handle.",
      hid);

   platform_shm_track_free(shminfop, ptr, objname, func, file, lineno);

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

   int      pid = getpid();
   threadid tid = platform_get_tid();

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

      frag->shm_frag_allocated_to_pid = pid;
      frag->shm_frag_allocated_to_tid = tid;
      // The freed_by_pid/freed_by_tid == 0 means fragment is still allocated.

#if SPLINTER_DEBUG
      frag->shm_alloc_ts = platform_get_timestamp();
#endif // SPLINTER_DEBUG

      // Track highest address of large-fragment that is being tracked.
      if (shm->shm_large_frag_hip < addr) {
         shm->shm_large_frag_hip = addr;
      }
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
platform_shm_track_free(shmem_info *shm,
                        void       *addr,
                        const char *objname,
                        const char *func,
                        const char *file,
                        const int   lineno)
{
   debug_assert(addr != NULL);

   int      pid      = getpid();
   threadid tid      = platform_get_tid();
   bool     do_trace = (Trace_shmem || Trace_shmem_frees || Trace_large_frags);

   shm_lock_mem_frags(shm);

   shm->shm_nfrees++;

   // If we are freeing a fragment beyond the high-address of all
   // large fragments tracked, then this is certainly not a large
   // fragment. So, no further need to see if it's a tracked fragment.
   if (addr > shm->shm_large_frag_hip) {
      shm->shm_nf_search_skipped++;
      shm_unlock_mem_frags(shm);
      return;
   }
   bool found_tracked_frag = FALSE;

   shm_frag_info *frag = shm->shm_mem_frags;

   // Walk thru fragment array to see if this fragment-being-freed is tracked.
   for (int fctr = 0; fctr < ARRAY_SIZE(shm->shm_mem_frags); fctr++, frag++) {
      if (frag->shm_frag_addr != addr) {
         continue;
      }
      found_tracked_frag = TRUE;
      shm->shm_num_frags_inuse--;

      // Record the process/thread that's doing the free.
      frag->shm_frag_freed_by_pid = pid;
      frag->shm_frag_freed_by_tid = tid;

#if SPLINTER_DEBUG
      frag->shm_elapsed_free_ns =
         platform_timestamp_elapsed(frag->shm_alloc_ts);
#endif // SPLINTER_DEBUG

      if (do_trace) {
         platform_default_log("[OS-pid=%d, ThreadID=%lu, %s:%d::%s()]"
                              ", Track freed fragment of size=%lu bytes"
                              ", at slot=%d, addr=%p"
                              ", allocated_to_pid=%d, allocated_to_tid=%lu"
                              ", shm_num_frags_inuse=%d"
                              ", shm_large_frag_hip=%p"
#if SPLINTER_DEBUG
                              ", used for elapsed=%lu ns (~%lu ms)"
#endif // SPLINTER_DEBUG
                              "\n",
                              frag->shm_frag_freed_by_pid,
                              frag->shm_frag_freed_by_tid,
                              file,
                              lineno,
                              func,
                              frag->shm_frag_size,
                              fctr,
                              addr,
                              frag->shm_frag_allocated_to_pid,
                              frag->shm_frag_allocated_to_tid,
                              shm->shm_num_frags_inuse,
                              shm->shm_large_frag_hip

#if SPLINTER_DEBUG
                              ,
                              frag->shm_elapsed_free_ns,
                              NSEC_TO_MSEC(frag->shm_elapsed_free_ns)
#endif // SPLINTER_DEBUG

         );
      }
      break;
   }
   shm_unlock_mem_frags(shm);

   if (!found_tracked_frag && do_trace && FALSE) {
      platform_default_log("[OS-pid=%d, ThreadID=%lu, %s:%d::%s()] "
                           ", Fragment %p for object '%s' is not tracked\n",
                           pid,
                           tid,
                           file,
                           lineno,
                           func,
                           addr,
                           objname);
   }
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_find_free() - See if there is an already-allocated and now-free
 * large memory fragment. If so, allocate that to this requester. Do the
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

   int      pid      = getpid();
   threadid tid      = platform_get_tid();
   bool     do_trace = (Trace_shmem || Trace_shmem_frees || Trace_large_frags);

   shm_lock_mem_frags(shm);

   // Walk the tracked-fragments array looking for a free-fragment >= size
   for (int fctr = 0; fctr < ARRAY_SIZE(shm->shm_mem_frags); fctr++, frag++) {
      if (!frag->shm_frag_addr) {
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
      frag->shm_frag_allocated_to_pid = pid;
      frag->shm_frag_allocated_to_tid = tid;

      shm->shm_num_frags_inuse++;
      if (shm->shm_num_frags_inuse > shm->shm_num_frags_inuse_HWM) {
         shm->shm_num_frags_inuse_HWM = shm->shm_num_frags_inuse;
      }

      // Now, mark that this fragment is in-use
      frag->shm_frag_freed_by_pid = 0;
      frag->shm_frag_freed_by_tid = 0;

#if SPLINTER_DEBUG
      // Track timestamp when fragment was reallocated.
      frag->shm_alloc_ts = platform_get_timestamp();
#endif // SPLINTER_DEBUG
      retptr = frag->shm_frag_addr;

      // Zero out reallocated memory fragment, just to be sure ...
      memset(retptr, 0, frag->shm_frag_size);

      if (do_trace) {
         char msg[160];

         /*
          * In this trace message, don't be confused if you see a wide gap
          * between local_in_use and shm_num_frags_inuse. The latter is a global
          * counter while the former is a local counter. We may have found a
          * free fragment to reallocate early in the array w/o processing the
          * full array. Hence, these two values are likely to diff (by a big
          * margin, even).
          */
         snprintf(msg,
                  sizeof(msg),
                  "Reallocated free fragment at slot=%d, addr=%p, "
                  "shm_num_frags_tracked=%u, shm_num_frags_inuse=%u"
                  " (local_in_use=%d)",
                  fctr,
                  retptr,
                  shm->shm_num_frags_tracked,
                  shm->shm_num_frags_inuse,
                  local_in_use);

         platform_shm_trace_allocs(
            shm, size, msg, retptr, objname, func, file, lineno);
      }
      break;
   }
   shm_unlock_mem_frags(shm);

   if (!found_tracked_frag && do_trace) {
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
 * platform_trace_large_frags() - Walk through large-fragments tracking array
 * and dump info about fragments that still appear "in-use".
 * -----------------------------------------------------------------------------
 */
static int
platform_trace_large_frags(shmem_info *shm)
{
   int local_in_use    = 0; // Tracked while iterating in this fn, locally
   shm_frag_info *frag = shm->shm_mem_frags;

   // Walk the tracked-fragments array looking for an in-use fragment
   for (int fctr = 0; fctr < ARRAY_SIZE(shm->shm_mem_frags); fctr++, frag++) {
      if (!frag->shm_frag_addr) {
         continue;
      }

      // Skip freed fragments.
      if (frag->shm_frag_freed_by_pid != 0) {
         continue;
      }
      local_in_use++;
      platform_assert((frag->shm_frag_freed_by_tid == 0),
                      "Invalid state found for fragment at index %d,"
                      "freed_by_pid=%d but freed_by_tid=%lu "
                      "(which should also be 0)\n",
                      fctr,
                      frag->shm_frag_freed_by_pid,
                      frag->shm_frag_freed_by_tid);

      platform_default_log("  Fragment at slot=%d, addr=%p, size=%lu is in-use"
                           ", allocated_to_pid=%d, allocated_to_tid=%lu\n",
                           fctr,
                           frag->shm_frag_addr,
                           frag->shm_frag_size,
                           frag->shm_frag_allocated_to_pid,
                           frag->shm_frag_allocated_to_tid);
   }
   return local_in_use;
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

void
platform_enable_tracing_large_frags()
{
   Trace_large_frags = TRUE;
}

void
platform_disable_tracing_large_frags()
{
   Trace_large_frags = FALSE;
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

/*
 * Helper fn to trace large-fragment handling; alloc and reallocation
 */
static void
platform_shm_trace_allocs(shmem_info  *shminfop,
                          const size_t size,
                          const char  *verb,
                          const void  *retptr,
                          const char  *objname,
                          const char  *func,
                          const char  *file,
                          const int    lineno)
{
   bool     use_MiB = (shminfop->shm_free_bytes < GiB);
   int      pid     = getpid();
   threadid tid     = platform_get_tid();

   // Convert 'size' in units as a string
   char size_str[SIZE_TO_STR_LEN];
   size_to_str(size_str, sizeof(size_str), size);

   const char *msg = "[OS-pid=%d,ThreadID=%lu, %s:%d::%s()] "
                     "-> %s: %s %lu bytes (%s)"
                     " for object '%s', at %p, "
                     "free bytes=%lu (~%lu.%d %s).\n";

   size_t free_bytes = shminfop->shm_free_bytes;
   size_t free_bytes_units;
   int    free_bytes_fract_units;
   char  *units_str = NULL;

   if (use_MiB) {
      free_bytes_units       = B_TO_MiB(free_bytes);
      free_bytes_fract_units = B_TO_MiB_FRACT(free_bytes);
      units_str              = "MiB";
   } else {
      free_bytes_units       = B_TO_GiB(free_bytes);
      free_bytes_fract_units = B_TO_GiB_FRACT(free_bytes);
      units_str              = "GiB";
   }

   platform_default_log(msg,
                        pid,
                        tid,
                        file,
                        lineno,
                        func,
                        __FUNCTION__,
                        verb,
                        size,
                        size_str,
                        objname,
                        retptr,
                        shminfop->shm_free_bytes,
                        free_bytes_units,
                        free_bytes_fract_units,
                        units_str);
}
