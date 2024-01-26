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

// SplinterDB's shared segment magic identifier. Mainly needed for diagnostics.
#define SPLINTERDB_SHMEM_MAGIC (uint64)0x543e4a6d

// Boolean globals controlling tracing of shared memory allocs / frees
static bool Trace_shmem_allocs = FALSE;
static bool Trace_shmem_frees  = FALSE;
static bool Trace_shmem        = FALSE;
static bool Trace_large_frags  = FALSE;

/*
 * ---------------------------------------------------------------------------
 * shm_large_frag_info{} - Struct describing a large memory fragment allocation.
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
 * ---------------------------------------------------------------------------
 */
typedef struct shm_large_frag_info {
   void *frag_addr;  // Start address of this memory fragment
                     // NULL => tracking fragment is empty
   size_t frag_size; // bytes (Used in re-allocation logic.)

   // Following fields are used mainly for assertions and diagnostics.
   int      frag_allocated_to_pid; // Allocated to this OS-pid
   threadid frag_allocated_to_tid; // Allocated to this Splinter thread-ID
   int      frag_freed_by_pid;     // OS-pid that freed this large fragment
   threadid frag_freed_by_tid;     // Splinter thread-ID that freed fragment
} shm_large_frag_info;

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
 * ------------------------------------------------------------------------
 * Shared-memory usage statistics & metrics:
 *
 * Set of usage-stats fields copied from shmem_info{} struct, so that we
 * can print these after shared segment has been destroyed.
 * ------------------------------------------------------------------------
 */
typedef struct shminfo_usage_stats {
   size_t total_bytes;      // Total size of shared segment allocated initially.
   size_t used_bytes;       // Used bytes of memory left (that were allocated)
   size_t free_bytes;       // Free bytes of memory left (that can be allocated)
   size_t used_bytes_HWM;   // High-water mark of memory used bytes
   size_t nfrees;           // # of calls to free memory
   size_t nfrees_last_frag; // Freed last small-fragment
   size_t nf_search_skipped;
   size_t used_by_large_frags_bytes; // Actually reserved
   uint32 nlarge_frags_tracked;
   uint32 nlarge_frags_inuse;
   uint32 nlarge_frags_inuse_HWM;
   int    nlarge_frags_found_in_use;
   int    shmid;
} shminfo_usage_stats;

/*
 * -----------------------------------------------------------------------------
 * shmem_heap{}: Shared memory Control Block: Used as a heap for memory allocs
 *
 * Core structure describing shared memory segment created. This lives right
 * at the start of the allocated shared segment.
 *
 * NOTE(s):
 *  - shm_large_frag_hip tracks the highest-address of all the large fragments
 *    that are tracked. This is an optimization to short-circuit the search
 *    done when freeing any fragment, to see if it's a large-fragment.
 * -----------------------------------------------------------------------------
 */
typedef struct shmem_heap {
   void *shm_start;      // Points to start address of shared segment.
   void *shm_end;        // Points to end address; one past end of sh segment
   void *shm_next;       // Points to next 'free' address to allocate from.
   void *shm_last_alloc; // Points to address most-recently allocated
   void *shm_splinterdb_handle;
   void *shm_large_frag_hip; // Highest addr of large-fragments tracked

   platform_spinlock shm_mem_lock; // To sync alloc / free

   platform_spinlock shm_mem_frags_lock;
   // Protected by shm_mem_frags_lock. Must hold to read or modify.
   shm_large_frag_info shm_large_frags[SHM_NUM_LARGE_FRAGS];

   shminfo_usage_stats usage;
   uint64              shm_magic; // Magic identifier for shared memory segment
   int                 shm_id;    // Shared memory ID returned by shmget()

} PLATFORM_CACHELINE_ALIGNED shmem_heap;

/* Permissions for accessing shared memory and IPC objects */
#define PLATFORM_IPC_OBJS_PERMS (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)

// Function prototypes

static void
platform_shm_track_large_alloc(shmem_heap *shm, void *addr, size_t size);

static void
platform_shm_track_free(shmem_heap *shm,
                        void       *addr,
                        const char *objname,
                        const char *func,
                        const char *file,
                        const int   lineno);

static void *
platform_shm_find_large(shmem_heap *shm,
                        size_t      size,
                        const char *objname,
                        const char *func,
                        const char *file,
                        const int   lineno);

static void
platform_shm_trace_allocs(shmem_heap  *shm,
                          const size_t size,
                          const char  *verb,
                          const void  *retptr,
                          const char  *objname,
                          const char  *func,
                          const char  *file,
                          const int    lineno);

static int
platform_trace_large_frags(shmem_heap *shm);

bool
platform_shm_heap_valid(shmem_heap *shmheap);

/*
 * PLATFORM_HEAP_ID_TO_SHMADDR() --
 *
 * The shared memory create function returns the address of shmem_heap->shm_id
 * as the platform_heap_id heap-ID to the caller. Rest of Splinter will use this
 * heap-ID as a 'handle' to manage / allocate shared memory. This macro converts
 * the heap-ID handle to the shared memory's start address, from which the
 * location of the next-free-byte can be tracked.
 */
shmem_heap *
platform_heap_id_to_shmaddr(platform_heap_id hid)
{
   debug_assert(hid != NULL);
   return (shmem_heap *)hid;
}

/* Evaluates to valid 'low' address within shared segment. */
static inline void *
platform_shm_lop(platform_heap_id hid)
{
   return (void *)platform_heap_id_to_shmaddr(hid);
}

/*
 * Evaluates to valid 'high' address within shared segment.
 * shm_end points to one-byte-just-past-end of shared segment.
 * Valid 'high' byte is last byte just before shm_end inside segment.
 */
static inline void *
platform_shm_hip(platform_heap_id hid)
{
   return (void *)(platform_heap_id_to_shmaddr(hid)->shm_end - 1);
}

static inline void
shm_lock_mem(shmem_heap *shm)
{
   platform_spin_lock(&shm->shm_mem_lock);
}

static inline void
shm_unlock_mem(shmem_heap *shm)
{
   platform_spin_unlock(&shm->shm_mem_lock);
}

static inline void
shm_lock_mem_frags(shmem_heap *shm)
{
   platform_spin_lock(&shm->shm_mem_frags_lock);
}

static inline void
shm_unlock_mem_frags(shmem_heap *shm)
{
   platform_spin_unlock(&shm->shm_mem_frags_lock);
}

/*
 * platform_valid_addr_in_heap(), platform_valid_addr_in_shm()
 *
 * Address 'addr' is valid if it's just past end of control block and within
 * shared segment.
 */
static inline bool
platform_valid_addr_in_shm(shmem_heap *shmaddr, const void *addr)
{
   return ((addr >= ((void *)shmaddr + platform_shm_ctrlblock_size()))
           && (addr < shmaddr->shm_end));
}

/*
 * Validate that input address 'addr' is a valid address within shared segment
 * region.
 */
bool
platform_valid_addr_in_heap(platform_heap_id heap_id, const void *addr)
{
   return platform_valid_addr_in_shm(platform_heap_id_to_shmaddr(heap_id),
                                     addr);
}

/*
 * Produce a formatted one-line output of shared memory usage stats / metrics.
 */
void
platform_shm_print_usage_stats(shminfo_usage_stats *usage)
{
   fraction used_bytes_pct;
   fraction used_bytes_HWM_pct;
   fraction free_bytes_pct;
   fraction freed_last_frag_pct   = zero_fraction;
   fraction nf_search_skipped_pct = zero_fraction;

   used_bytes_pct = init_fraction(usage->used_bytes, usage->total_bytes);
   used_bytes_HWM_pct =
      init_fraction(usage->used_bytes_HWM, usage->total_bytes);
   free_bytes_pct = init_fraction(usage->free_bytes, usage->total_bytes);
   if (usage->nfrees) {
      freed_last_frag_pct =
         init_fraction(usage->nfrees_last_frag, usage->nfrees);
      nf_search_skipped_pct =
         init_fraction(usage->nf_search_skipped, usage->nfrees);
   }

   // clang-format off
   platform_default_log(
      "Shared memory usage stats shmid=%d:"
      " Total=%lu bytes (%s)"
      ", Used=%lu bytes (%s, " FRACTION_FMT(4, 2) " %%)"
      ", UsedHWM=%lu bytes (%s, " FRACTION_FMT(4, 2) " %%)"
      ", Free=%lu bytes (%s, " FRACTION_FMT(4, 2) " %%)"
      ", nfrees=%lu"
      ", nfrees-last-small-frag=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nf_search_skipped=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", Large fragments in-use HWM=%u (found in-use=%d)"
      ", consumed=%lu bytes (%s)"
      ".\n",
      usage->shmid,
      usage->total_bytes, size_str(usage->total_bytes),

      usage->used_bytes, size_str(usage->used_bytes),
      (FRACTION_ARGS(used_bytes_pct) * 100),

      usage->used_bytes_HWM, size_str(usage->used_bytes_HWM),
      (FRACTION_ARGS(used_bytes_HWM_pct) * 100),

      usage->free_bytes, size_str(usage->free_bytes),
      (FRACTION_ARGS(free_bytes_pct) * 100),

      usage->nfrees,
      usage->nfrees_last_frag,
      (FRACTION_ARGS(freed_last_frag_pct) * 100),

      usage->nf_search_skipped,
      (FRACTION_ARGS(nf_search_skipped_pct) * 100),

      usage->nlarge_frags_inuse_HWM,
      usage->nlarge_frags_found_in_use,
      usage->used_by_large_frags_bytes,
      size_str(usage->used_by_large_frags_bytes));
   // clang-format on
}

/*
 * Save off shared memory usage stats in a usage struct. This is really
 * needed to print usage-stats before we dismantle the shared segment
 * entirely.
 *
 * Returns: # of large free-fragments found in-use. Usually when this
 * function is called for diagnostics, this may not matter. When the
 * shared segment is dismantled, a non-zero count for "in-use" large fragments
 * is an indication that something is amiss.
 */
int
platform_save_usage_stats(shminfo_usage_stats *usage, shmem_heap *shm)
{
   *usage                           = shm->usage;
   usage->nlarge_frags_found_in_use = platform_trace_large_frags(shm);
   return usage->nlarge_frags_found_in_use;
}

/*
 * -----------------------------------------------------------------------------
 * Interface to print shared memory usage stats. (Callable from the debugger)
 * This is mainly intended as a diagnostics tool, so we don't work too hard
 * to grab metrics under exclusive access.
 */
void
platform_shm_print_usage(platform_heap_id hid)
{
   shmem_heap         *shm = platform_heap_id_to_shmaddr(hid);
   shminfo_usage_stats usage;
   platform_save_usage_stats(&usage, shm);
   platform_shm_print_usage_stats(&usage);
}

/*
 * -----------------------------------------------------------------------------
 * platform_shmcreate() -- Create a new shared memory segment.
 *
 * For a given heap ID, we expect that this create method will only be called
 * once. [ Otherwise, it means some code is erroneously creating
 * the shared segment twice, clobbering previously established handles. ]
 * -----------------------------------------------------------------------------
 */
platform_status
platform_shmcreate(size_t            size,
                   platform_heap_id *heap_id) // Out
{
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
   shmem_heap *shm = (shmem_heap *)shmaddr;

   shm->shm_start = shmaddr;
   shm->shm_end   = (shmaddr + size);
   shm->shm_next  = (shmaddr + sizeof(shmem_heap));
   shm->shm_id    = shmid;
   shm->shm_magic = SPLINTERDB_SHMEM_MAGIC;

   size_t free_bytes      = (size - sizeof(shmem_heap));
   shm->usage.total_bytes = size;
   shm->usage.free_bytes  = free_bytes;

   // Return 'heap-ID' handle pointing to start addr of shared segment.
   if (heap_id) {
      *heap_id = (platform_heap_id *)shmaddr;
   }

   platform_spinlock_init(
      &shm->shm_mem_lock, platform_get_module_id(), *heap_id);

   // Initialize spinlock needed to access memory fragments tracker
   platform_spinlock_init(
      &shm->shm_mem_frags_lock, platform_get_module_id(), *heap_id);

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
platform_shmdestroy(platform_heap_id *hid_out)
{
   if (!hid_out) {
      platform_error_log(
         "Error! Attempt to destroy shared memory with NULL heap ID!");
      return;
   }

   const void *shmaddr = (const void *)platform_heap_id_to_shmaddr(*hid_out);

   // Heap ID may be coming from the shared segment, itself, that we will
   // be detaching from now and freeing, below. So, an attempt to NULL out
   // this handle after memory is freed will run into an exception. Clear
   // out this handle prior to all this circus.
   *hid_out = NULL;

   // Use a cached copy in case we are dealing with bogus input shmem address.
   shmem_heap shmem_heap_struct;
   memmove(&shmem_heap_struct, shmaddr, sizeof(shmem_heap_struct));

   shmem_heap *shm = &shmem_heap_struct;

   if (shm->shm_magic != SPLINTERDB_SHMEM_MAGIC) {
      platform_error_log("%s(): Input heap ID, %p, does not seem to be a "
                         "valid SplinterDB shared segment's start address."
                         " Found magic 0x%lX does not match expected"
                         " magic 0x%lX.\n",
                         __func__,
                         hid_out,
                         shm->shm_magic,
                         SPLINTERDB_SHMEM_MAGIC);
      return;
   }

   // Retain some memory usage stats before releasing shmem
   shminfo_usage_stats usage;
   platform_save_usage_stats(&usage, shm);

   int shmid = shm->shm_id;
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
   shm->shm_id = 0;

   rv = shmctl(shmid, IPC_RMID, NULL);
   if (rv != 0) {
      platform_error_log(
         "shmctl failed to remove shared segment at address %p, shmid=%d.\n",
         shmaddr,
         shmid);

      // restore state
      shm->shm_id = shmid;
      return;
   }

   // Reset globals to NULL; to avoid accessing stale handles.
   Heap_id = NULL;

   // Always trace destroy of shared memory segment.
   platform_default_log("Deallocated SplinterDB shared memory "
                        "segment at %p, shmid=%d.\n",
                        shmaddr,
                        shmid);

   platform_shm_print_usage_stats(&usage);
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_alloc() -- Allocate n-bytes from shared memory segment.
 *
 * Allocation request is expected to have added-in pad-bytes required for
 * alignment to some power-of-2 # of bytes. As a result, we can assert that
 * the addr-of-next-free-byte is always aligned to PLATFORM_CACHELINE_SIZE.
 * -----------------------------------------------------------------------------
 */
// RESOLVE: Pass down user requested alignment and handle it here.
void *
platform_shm_alloc(platform_heap_id hid,
                   const size_t     size,
                   const char      *objname,
                   const char      *func,
                   const char      *file,
                   const int        lineno)
{
   shmem_heap *shm = platform_heap_id_to_shmaddr(hid);

   debug_assert((platform_shm_heap_valid(shm) == TRUE),
                "Shared memory heap ID at %p is not a valid shared memory ptr.",
                hid);

   debug_assert(((size % PLATFORM_CACHELINE_SIZE) == 0),
                "size=%lu is not aligned to PLATFORM_CACHELINE_SIZE",
                size);
   platform_assert(((((uint64)shm->shm_next) % PLATFORM_CACHELINE_SIZE) == 0),
                   "[%s:%d] Next free-addr is not aligned: "
                   "shm_next=%p, total_bytes=%lu, used_bytes=%lu"
                   ", free_bytes=%lu",
                   file,
                   lineno,
                   shm->shm_next,
                   shm->usage.total_bytes,
                   shm->usage.used_bytes,
                   shm->usage.free_bytes);

   void *retptr = NULL;

   // See if we can satisfy requests for large memory fragments from a cached
   // list of used/free fragments that are tracked separately.
   if ((size >= SHM_LARGE_FRAG_SIZE)
       && ((retptr =
               platform_shm_find_large(shm, size, objname, func, file, lineno))
           != NULL))
   {
      return retptr;
   }

   _Static_assert(sizeof(void *) == sizeof(size_t),
                  "check our casts are valid");

   shm_lock_mem(shm);

   // Optimistically, allocate the requested 'size' bytes of memory.
   retptr = __sync_fetch_and_add(&shm->shm_next, (void *)size);

   if ((retptr + size) > shm->shm_end) {
      // This memory request cannot fit in available space. Reset.
      __sync_fetch_and_sub(&shm->shm_next, (void *)size);
      shm_unlock_mem(shm);

      platform_error_log(
         "[%s:%d::%s()]: Insufficient memory in shared segment"
         " to allocate %lu bytes for '%s'. Approx free space=%lu bytes."
         " nlarge_frags_tracked=%u, nlarge_frags_inuse=%u (HWM=%u).\n",
         file,
         lineno,
         func,
         size,
         objname,
         shm->usage.free_bytes,
         shm->usage.nlarge_frags_tracked,
         shm->usage.nlarge_frags_inuse,
         shm->usage.nlarge_frags_inuse_HWM);
      platform_trace_large_frags(shm);
      return NULL;
   }

   shm->shm_last_alloc = retptr;
   // Track approx memory usage metrics; mainly for troubleshooting
   __sync_fetch_and_add(&shm->usage.used_bytes, size);
   __sync_fetch_and_sub(&shm->usage.free_bytes, size);
   if (shm->usage.used_bytes > shm->usage.used_bytes_HWM) {
      shm->usage.used_bytes_HWM = shm->usage.used_bytes;
   }
   shm_unlock_mem(shm);

   if (size >= SHM_LARGE_FRAG_SIZE) {
      platform_shm_track_large_alloc(shm, retptr, size);
   }

   // Trace shared memory allocation; then return memory ptr.
   if (Trace_shmem || Trace_shmem_allocs
       || (Trace_large_frags && (size >= SHM_LARGE_FRAG_SIZE)))
   {
      platform_shm_trace_allocs(shm,
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
 * Functionally is similar to 'realloc' system call. We allocate required # of
 * bytes, copy over the old contents (if any), and do a fake free of the oldptr.
 * -----------------------------------------------------------------------------
 */
void *
platform_shm_realloc(platform_heap_id hid,
                     void            *oldptr,
                     const size_t     oldsize,
                     const size_t     newsize,
                     const char      *func,
                     const char      *file,
                     const int        lineno)
{
   debug_assert(((oldptr == NULL) && (oldsize == 0)) || (oldptr && oldsize),
                "oldptr=%p, oldsize=%lu",
                oldptr,
                oldsize);

   // We can only realloc from an oldptr that's allocated from shmem
   debug_assert(!oldptr || platform_valid_addr_in_heap(hid, oldptr),
                "oldptr=%p is not allocated from shared memory",
                oldptr);

   void *retptr =
      platform_shm_alloc(hid, newsize, "Unknown", func, file, lineno);
   if (retptr) {

      // Copy over old contents, if any, and free that memory piece
      if (oldptr) {
         memcpy(retptr, oldptr, oldsize);
         platform_shm_free(hid, oldptr, "Unknown", func, file, lineno);
      }
   } else {
      // Report approx memory usage metrics w/o spinlock (diagnostics)
      shmem_heap *shm         = platform_heap_id_to_shmaddr(hid);
      size_t      total_bytes = shm->usage.total_bytes;
      size_t      used_bytes  = shm->usage.used_bytes;
      size_t      free_bytes  = shm->usage.free_bytes;
      size_t      num_frees   = shm->usage.nfrees;
      fraction    used_bytes_pct;
      fraction    free_bytes_pct;
      used_bytes_pct = init_fraction(used_bytes, total_bytes);
      free_bytes_pct = init_fraction(free_bytes, total_bytes);

      // clang-format off
      platform_error_log("%s() failed to reallocate newsize=%lu bytes (%s)"
                         ", oldsize=%lu bytes (%s)"
                         ", Used=%lu bytes (%s, " FRACTION_FMT(4, 2)
                         " %%), Free=%lu bytes (%s, " FRACTION_FMT(4, 2)
                         " %%)"
                         ", num-free-calls=%lu\n",
                        __func__,
                        newsize,
                        size_str(newsize),
                        oldsize,
                        size_str(oldsize),
                        used_bytes,
                        size_str(used_bytes),
                        (FRACTION_ARGS(used_bytes_pct) * 100),
                        free_bytes,
                        size_str(free_bytes),
                        (FRACTION_ARGS(free_bytes_pct) * 100),
                        num_frees);
      // clang-format off
   }
   return retptr;
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_free() -- 'Free' the memory fragment at given address in shmem.
 *
 * We expect that the 'ptr' is a valid address within the shared segment.
 * Otherwise, it means that Splinter was configured to run with shared memory,
 * -and- in some code path we allocated w/o using shared memory
 * (i.e. PROCESS_PRIVATE_HEAP_ID interface), but ended up calling shmem-free
 * interface. That would be a code error which results in a memory leak.
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
   shmem_heap *shm = platform_heap_id_to_shmaddr(hid);

   debug_assert(
      (platform_shm_heap_valid(shm) == TRUE),
      "Shared memory heap ID at %p is not a valid shared memory handle.",
      hid);

   if (!platform_valid_addr_in_heap(hid, ptr)) {
      platform_error_log("[%s:%d::%s()] -> %s: Requesting to free memory"
                         " at %p, for object '%s' which is a memory chunk not"
                         " allocated from shared memory {start=%p, end=%p}.\n",
                         file,
                         lineno,
                         func,
                         __func__,
                         ptr,
                         objname,
                         platform_shm_lop(hid),
                         platform_shm_hip(hid));
      return;
   }

   // Micro-optimization for very-last-fragment-allocated being freed
   bool   maybe_large_frag = TRUE;
   size_t frag_size        = 0;

   shm_lock_mem(shm);
   shm->usage.nfrees++;
   if (shm->shm_last_alloc == ptr) {
      debug_assert(
         shm->shm_next > ptr, "shm_next=%p, free-ptr=%p", shm->shm_next, ptr);
      frag_size = (shm->shm_next - ptr);
      if (frag_size < SHM_LARGE_FRAG_SIZE) {
         // Recycle the most-recently-allocated-small-fragment, now being freed.
         shm->shm_next       = ptr;
         shm->shm_last_alloc = NULL;
         shm->usage.free_bytes += frag_size;
         shm->usage.used_bytes -= frag_size;
         shm->usage.nfrees_last_frag += 1;

         // We know fragment being freed is not a large fragment
         maybe_large_frag = FALSE;
      }
   }
   shm_unlock_mem(shm);

   if (maybe_large_frag) {
      platform_shm_track_free(shm, ptr, objname, func, file, lineno);
   }

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
 * platform_shm_track_large_alloc() - Track the allocation of this large fragment.
 * 'Tracking' here means we record this large-fragment in an array tracking
 * large-memory fragments allocated.
 * -----------------------------------------------------------------------------
 */
static void
platform_shm_track_large_alloc(shmem_heap *shm, void *addr, size_t size)
{
   debug_assert(
      (size >= SHM_LARGE_FRAG_SIZE),
      "Incorrect usage of this interface for requested size=%lu bytes."
      " Size should be >= %lu bytes.\n",
      size,
      SHM_LARGE_FRAG_SIZE);


   // Iterate through the list of memory fragments being tracked.
   int            fctr = 0;
   shm_large_frag_info *frag = shm->shm_large_frags;
   shm_lock_mem_frags(shm);
   while ((fctr < ARRAY_SIZE(shm->shm_large_frags)) && frag->frag_addr) {
      // As this is a newly allocated fragment being tracked, it should
      // not be found elsewhere in the tracker array.
      platform_assert((frag->frag_addr != addr),
                      "Error! Newly allocated large memory fragment at %p"
                      " is already tracked at slot %d."
                      " Fragment is allocated to PID=%d, to tid=%lu,"
                      " and is %s (freed by PID=%d, by tid=%lu)\n.",
                      addr,
                      fctr,
                      frag->frag_allocated_to_pid,
                      frag->frag_allocated_to_tid,
                      (frag->frag_freed_by_pid ? "free" : "in use"),
                      frag->frag_freed_by_pid,
                      frag->frag_freed_by_tid);
      fctr++;
      frag++;
   }
   // If we found a free slot, track our memory fragment at fctr'th slot.
   if (fctr < ARRAY_SIZE(shm->shm_large_frags)) {
      shm->usage.nlarge_frags_tracked++;
      shm->usage.nlarge_frags_inuse++;
      shm->usage.used_by_large_frags_bytes += size;
      if (shm->usage.nlarge_frags_inuse > shm->usage.nlarge_frags_inuse_HWM) {
         shm->usage.nlarge_frags_inuse_HWM = shm->usage.nlarge_frags_inuse;
      }

      // We should really assert that the other fields are zero, but for now
      // re-init this fragment tracker.
      memset(frag, 0, sizeof(*frag));
      frag->frag_addr = addr;
      frag->frag_size = size;

      frag->frag_allocated_to_pid = platform_getpid();
      frag->frag_allocated_to_tid = platform_get_tid();

      // The freed_by_pid/freed_by_tid == 0 means fragment is still allocated.

      // Track highest address of large-fragment that is being tracked.
      if (shm->shm_large_frag_hip < addr) {
         shm->shm_large_frag_hip = addr;
      }
   }

   shm_unlock_mem_frags(shm);
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_track_free() - See if this memory fragment being freed is
 * already being tracked. If so, it's a large fragment allocation, which can be
 * re-cycled after this free. Do the book-keeping accordingly to record that
 * this large-fragment is no longer in-use and can be recycled.
 * -----------------------------------------------------------------------------
 */
static void
platform_shm_track_free(shmem_heap *shm,
                        void       *addr,
                        const char *objname,
                        const char *func,
                        const char *file,
                        const int   lineno)
{
   shm_lock_mem_frags(shm);

   // If we are freeing a fragment beyond the high-address of all
   // large fragments tracked, then this is certainly not a large
   // fragment. So, no further need to see if it's a tracked fragment.
   if (addr > shm->shm_large_frag_hip) {
      shm_unlock_mem_frags(shm);
      return;
   }
   bool found_tracked_frag = FALSE;
   bool trace_shmem        = (Trace_shmem || Trace_shmem_frees);

   shm_large_frag_info *frag = shm->shm_large_frags;
   int            fctr = 0;
   while ((fctr < ARRAY_SIZE(shm->shm_large_frags))
          && (!frag->frag_addr || (frag->frag_addr != addr)))
   {
      fctr++;
      frag++;
   }
   if (fctr < ARRAY_SIZE(shm->shm_large_frags)) {
      debug_assert(frag->frag_addr == addr);
      found_tracked_frag = TRUE;

      // Cross-check the recording we did when fragment was allocated
      // We cannot check the tid as the parent process' tid may be 0.
      // We could have come across a free fragment that was previously
      // used by the parent process.
      // debug_assert(frag->frag_allocated_to_tid != 0);
      debug_assert(frag->frag_allocated_to_pid != 0);
      debug_assert(frag->frag_size != 0);

      shm->usage.nlarge_frags_inuse--;

      // Mark the fragment as in-use by recording the process/thread that's
      // doing the free.
      frag->frag_freed_by_pid = platform_getpid();
      frag->frag_freed_by_tid = platform_get_tid();

      if (trace_shmem) {
         platform_default_log("OS-pid=%d, ThreadID=%lu"
                              ", Track freed fragment of size=%lu bytes"
                              ", at slot=%d, addr=%p"
                              ", allocated_to_pid=%d, allocated_to_tid=%lu"
                              ", shm_large_frag_hip=%p\n",
                              frag->frag_freed_by_pid,
                              frag->frag_freed_by_tid,
                              frag->frag_size,
                              fctr,
                              addr,
                              frag->frag_allocated_to_pid,
                              frag->frag_allocated_to_tid,
                              shm->shm_large_frag_hip);
      }
   }
   shm_unlock_mem_frags(shm);

   if (!found_tracked_frag && trace_shmem) {
      platform_default_log("[OS-pid=%d, ThreadID=%lu, %s:%d::%s()] "
                           ", Fragment %p for object '%s' is not tracked\n",
                           platform_getpid(),
                           platform_get_tid(),
                           file,
                           lineno,
                           func,
                           addr,
                           objname);
   }
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_find_large() - Search the array of large-fragments being tracked
 * to see if there is an already allocated and now-free large memory fragment.
 * If so, allocate that fragment to this requester. Do the book-keeping
 * accordingly.
 * -----------------------------------------------------------------------------
 */
static void *
platform_shm_find_large(shmem_heap *shm,
                       size_t      size,
                       const char *objname,
                       const char *func,
                       const char *file,
                       const int   lineno)
{
   debug_assert((size >= SHM_LARGE_FRAG_SIZE),
                "Incorrect usage of this interface for requested"
                " size=%lu bytes. Size should be >= %lu bytes.\n",
                size,
                SHM_LARGE_FRAG_SIZE);

   void          *retptr = NULL;
   shm_large_frag_info *frag   = shm->shm_large_frags;
   int local_in_use      = 0; // Tracked while iterating in this fn, locally

   int  found_at_fctr      = -1;
   bool found_tracked_frag = FALSE;

   shm_lock_mem_frags(shm);

   uint32 nlarge_frags_tracked = shm->usage.nlarge_frags_tracked;
   uint32 nlarge_frags_inuse   = shm->usage.nlarge_frags_inuse;

   for (int fctr = 0; fctr < ARRAY_SIZE(shm->shm_large_frags); fctr++, frag++) {
      if (!frag->frag_addr || (frag->frag_size < size)) {
         continue;
      }

      // Skip fragment if it's still in-use
      if (frag->frag_freed_by_pid == 0) {
         platform_assert((frag->frag_freed_by_tid == 0),
                         "Invalid state found for fragment at index %d,"
                         "freed_by_pid=%d but freed_by_tid=%lu "
                         "(which should also be 0)\n",
                         fctr,
                         frag->frag_freed_by_pid,
                         frag->frag_freed_by_tid);

         local_in_use++;
         continue;
      }
      found_tracked_frag = TRUE;
      found_at_fctr      = fctr;

      // Record the process/thread to which free fragment is being allocated
      frag->frag_allocated_to_pid = platform_getpid();
      frag->frag_allocated_to_tid = platform_get_tid();

      shm->usage.nlarge_frags_inuse++;
      if (shm->usage.nlarge_frags_inuse > shm->usage.nlarge_frags_inuse_HWM) {
         shm->usage.nlarge_frags_inuse_HWM = shm->usage.nlarge_frags_inuse;
      }
      nlarge_frags_inuse = shm->usage.nlarge_frags_inuse;

      // Now, mark that this fragment is in-use
      frag->frag_freed_by_pid = 0;
      frag->frag_freed_by_tid = 0;

      retptr = frag->frag_addr;

      // Zero out the recycled large-memory fragment, just to be sure ...
      memset(retptr, 0, frag->frag_size);
      break;
   }
   shm_unlock_mem_frags(shm);

   // Trace whether we found tracked fragment or not.
   if (Trace_shmem || Trace_shmem_allocs) {
      char msg[200];
      if (found_tracked_frag) {
         /*
          * In this trace message, don't be confused if you see a wide gap
          * between local_in_use and nlarge_frags_inuse. The latter is a global
          * counter while the former is a local counter. We may have found a
          * free fragment to reallocate early in the array w/o processing the
          * full array. Hence, these two values are likely to diff (by a big
          * margin, even).
          */
         snprintf(msg,
                  sizeof(msg),
                  "Reallocated free fragment at slot=%d, addr=%p, "
                  "nlarge_frags_tracked=%u, nlarge_frags_inuse=%u"
                  " (local_in_use=%d)",
                  found_at_fctr,
                  retptr,
                  nlarge_frags_tracked,
                  nlarge_frags_inuse,
                  local_in_use);
      } else {
         snprintf(msg,
                  sizeof(msg),
                  "Did not find free fragment of size=%lu bytes to reallocate."
                  " nlarge_frags_tracked=%u, nlarge_frags_inuse=%u"
                  " (local_in_use=%d)",
                  size,
                  nlarge_frags_tracked,
                  nlarge_frags_inuse,
                  local_in_use);
      }
      platform_shm_trace_allocs(
         shm, size, msg, retptr, objname, func, file, lineno);
   }
   return retptr;
}

/*
 * -----------------------------------------------------------------------------
 * platform_trace_large_frags() - Walk through large-fragments tracking array
 * and dump info about fragments that still appear "in-use".
 * This diagnostic routine will -always- be called when shared segment is
 * being destroyed. If any "in-use" large-fragments are found, a message will
 * be generated.
 * -----------------------------------------------------------------------------
 */
static int
platform_trace_large_frags(shmem_heap *shm)
{
   int local_in_use    = 0; // Tracked while iterating in this fn, locally
   shm_large_frag_info *frag = shm->shm_large_frags;

   threadid thread_tid     = platform_get_tid();
   bool     print_new_line = false;
   // Walk the tracked-fragments array looking for an in-use fragment
   for (int fctr = 0; fctr < ARRAY_SIZE(shm->shm_large_frags); fctr++, frag++) {
      if (!frag->frag_addr) {
         continue;
      }

      // Skip freed fragments.
      if (frag->frag_freed_by_pid != 0) {
         continue;
      }
      // Found a large fragment that is still "in-use"
      local_in_use++;

      // Do -NOT- assert here. As this is a diagnostic routine, report
      // the inconsistency, and continue to find more stray large fragments.
      if (frag->frag_freed_by_tid != 0) {
         platform_error_log("Invalid state found for fragment at index %d,"
                            "freed_by_pid=%d but freed_by_tid=%lu "
                            "(which should also be 0)\n",
                            fctr,
                            frag->frag_freed_by_pid,
                            frag->frag_freed_by_tid);
      }

      if (!print_new_line) {
         platform_error_log("\n**** [TID=%lu] Large fragment usage "
                            "diagnostics:\n",
                            thread_tid);
         print_new_line = true;
      }

      platform_error_log("  **** [TID=%lu] Fragment at slot=%d, addr=%p"
                         ", size=%lu (%s) is in-use, allocated_to_pid=%d"
                         ", allocated_to_tid=%lu\n",
                         thread_tid,
                         fctr,
                         frag->frag_addr,
                         frag->frag_size,
                         size_str(frag->frag_size),
                         frag->frag_allocated_to_pid,
                         frag->frag_allocated_to_tid);
   }
   return local_in_use;
}

/*
 * -----------------------------------------------------------------------------
 * Accessor interfaces - mainly intended as assert / testing / debugging
 * hooks.
 * -----------------------------------------------------------------------------
 */
bool
platform_shm_heap_valid(shmem_heap *shmheap)
{
   // Use a cached copy in case we are dealing with a bogus input shmem
   // address.
   shmem_heap shmem_heap_struct;
   memmove(&shmem_heap_struct, (void *)shmheap, sizeof(shmem_heap_struct));

   shmem_heap *shm = &shmem_heap_struct;

   if (shm->shm_magic != SPLINTERDB_SHMEM_MAGIC) {
      platform_error_log(
         "%s(): Input shared memory heap, %p, does not seem to be a valid "
         "SplinterDB shared segment's start address."
         " Found magic 0x%lX does not match expected magic 0x%lX.\n",
         __func__,
         shmheap,
         shm->shm_magic,
         SPLINTERDB_SHMEM_MAGIC);
      return FALSE;
   }

   return TRUE;
}

/*
 * -----------------------------------------------------------------------------
 * Warning! Testing & Diagnostics interfaces, which are written to support
 * verification of splinterdb handle from forked child processes when running
 * Splinter configured with shared-segment.
 *
 * platform_heap_set_splinterdb_handle() - Save-off the handle to splinterdb *
 *    in the shared segment's control block.
 *
 * platform_heap_get_splinterdb_handle() - Return the handle to splinterdb *
 *    saved-off in the shared segment's control block.
 * -----------------------------------------------------------------------------
 */
bool
platform_shm_heap_id_valid(const platform_heap_id heap_id)
{
   const shmem_heap *shm = platform_heap_id_to_shmaddr(heap_id);
   return (shm->shm_magic == SPLINTERDB_SHMEM_MAGIC);
}

void
platform_shm_set_splinterdb_handle(platform_heap_id heap_id, void *addr)
{
   debug_assert(platform_shm_heap_id_valid(heap_id));
   shmem_heap *shm = platform_heap_id_to_shmaddr(heap_id);
   shm->shm_splinterdb_handle = addr;
}

void *
platform_heap_get_splinterdb_handle(const platform_heap_id heap_id)
{
   debug_assert(platform_shm_heap_id_valid(heap_id));
   shmem_heap *shm = platform_heap_id_to_shmaddr(heap_id);
   return shm->shm_splinterdb_handle;
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

/* Size of control block at start of shared memory describing shared segment
 */
size_t
platform_shm_ctrlblock_size()
{
   return sizeof(shmem_heap);
}

/*
 * Shmem-accessor interfaces by heap_id.
 */
size_t
platform_shmsize(platform_heap_id heap_id)
{
   return (platform_heap_id_to_shmaddr(heap_id)->usage.total_bytes);
}

size_t
platform_shmbytes_used(platform_heap_id heap_id)
{
   return (platform_heap_id_to_shmaddr(heap_id)->usage.used_bytes);
}
size_t
platform_shmbytes_free(platform_heap_id heap_id)
{
   return (platform_heap_id_to_shmaddr(heap_id)->usage.free_bytes);
}

void *
platform_shm_next_free_addr(platform_heap_id heap_id)
{
   return (platform_heap_id_to_shmaddr(heap_id)->shm_next);
}

static void
platform_shm_trace_allocs(shmem_heap  *shm,
                          const size_t size,
                          const char  *verb,
                          const void  *retptr,
                          const char  *objname,
                          const char  *func,
                          const char  *file,
                          const int    lineno)
{
   platform_default_log("  [OS-pid=%d,ThreadID=%lu, %s:%d::%s()] "
                        "-> %s: %s size=%lu bytes (%s)"
                        " for object '%s', at %p, "
                        "free bytes=%lu (%s).\n",
                        platform_getpid(),
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
                        shm->usage.free_bytes,
                        size_str(shm->usage.free_bytes));
}
