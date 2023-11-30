// Copyright 2018-2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * ---------------------------------------------------------------------------
 * shmem.c --
 *
 * This file contains the implementation for managing shared memory created
 * for use by SplinterDB and all its innards.
 *
 * Here's a quick code-flow of important entry point functions:
 *
 * - platform_shmcreate(), platform_shmdestroy()
 *   Bootstrap and dismantle shared memory segment.
 *
 *  platform_shm_alloc() - Main allocation interface
 *   │
 *   ├──►platform_shm_find_large() - Find (or recycle) a large free fragment
 *   │
 *   └──► platform_shm_find_small() - Find (or recycle) a small free fragment
 *
 *
 * platform_shm_free() - Main free interface
 *   │
 *   └─► platform_shm_track_free() - Manage free'd fragment in lists
 *        │
 *        └─► platform_shm_hook_free_frag() - Add small fragment to its list
 *
 * platform_shm_realloc() - Main realloc() interface
 *   │
 *   ├─► platform_shm_alloc()
 *   │
 *   └─► platform_shm_free()
 *
 * platform_shm_print_usage() - Useful to dump shm-usage metrics
 *   │
 *   └─► platform_shm_print_usage_stats()
 *
 * There are many other debugging, tracing and diagnostics functions.
 * Best to read them inline in code.
 * ---------------------------------------------------------------------------
 */
#include <unistd.h>

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
 * Here is a figure showing how large-fragments are tracked
 *
 *      ┌──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┐
 *     ┌┼--│  │  │  │  │  │  │  │  │  │  │  │  │  │  │  │  │  │  │
 *     ├───┴─┬┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┘
 *     │     │
 *     │     │
 * ┌───▼─┐   │
 * │     │   ▼                      ┌─────────────────────────┐
 * │     │ ┌─────────┐              │                         │
 * │     │ │         │◄─────────────┼─addr                    │
 * │     │ │         │              │ size = 32K              │
 * │     │ │         │              │                         │
 * └─────┘ │         │              │ allocated_to_tid = T1   │
 *         │         │              │                         │
 *         │         │              │ allocated_to_pid = PID1 │
 *         │         │              │                         │
 *         └─────────┘              │ freed_by_pid = 0        │
 *                                  │                         │
 *                                  │ freed_by_tid = 0        │
 *                                  │                         │
 *                                  └─────────────────────────┘
 *
 * ---- Lifecyle: ----
 *
 *  - Initially all frag_addr will be NULL => tracking fragment is empty
 *  - When a large fragment is initially allocated, frag_addr / frag_size will
 *    be set.
 *  - (allocated_to_pid != 0) && (freed_by_pid == 0) - Fragment is in use.
 *  - (allocated_to_pid != 0) && (freed_by_pid != 0) - Fragment is free.
 * ---------------------------------------------------------------------------
 */
// clang-format off
typedef struct shm_large_frag_info {
   void       *frag_addr;             // Start addr of this memory fragment
   const char *frag_func;             // Calling func which lead to creation
   size_t      frag_size;             // bytes
   threadid    frag_allocated_to_tid; // Alloc'ed to this Splinter thread-ID
   int         frag_allocated_to_pid; // Allocated to this OS-pid
   int         frag_freed_by_pid;     // OS-pid that freed this fragment
   threadid    frag_freed_by_tid;     // Splinter thread-ID that freed this
   int         frag_line;
} shm_large_frag_info;

// clang-format on

/*
 * In the worst case we may have all threads performing activities that need
 * such large memory fragments. We track up to twice the # of configured
 * threads, which is still a small array to search.
 */
#define SHM_NUM_LARGE_FRAGS (MAX_THREADS * 2)

/*
 * Currently, we track free-fragments in lists limited to these sizes.
 * Each such free-list has a head-list field in shared memory control block.
 * of sizes. This list of tracked small-fragments can be easily expanded.
 */
#define SHM_SMALL_FRAG_MIN_SIZE 64  // Will not track for size < min bytes
#define SHM_SMALL_FRAG_MAX_SIZE 512 // Will not track for size > max bytes

/*
 * We track a small number of allocated fragments, mainly to assert that the
 * free() call is being correctly issued with the right ptr / size arguments.
 */
typedef struct frag_hdr {
   void  *frag_addr;
   size_t frag_size;
} frag_hdr;

// Small number of allocated small fragments are tracked, for assertion
// checking of fragment-addr-vs-its-size at the time of free.
#define SHM_NUM_SMALL_FRAGS (MAX_THREADS * 32)

/*
 * Each free small fragment that is hooked into the free-list is described
 * by this tiny tracking structure.
 */
typedef struct free_frag_hdr {
   struct free_frag_hdr *free_frag_next;
   size_t                free_frag_size;
} free_frag_hdr;

/*
 * ------------------------------------------------------------------------
 * Shared-memory usage statistics & metrics:
 *
 * Set of usage-stats fields copied from shmem_heap{} struct, so that we
 * can print these after shared segment has been destroyed.
 * ------------------------------------------------------------------------
 */
typedef struct shminfo_usage_stats {
   int    shmid;          // Shared memory ID returned by shmget()
   size_t total_bytes;    // Total size of shared segment allocated initially.
   size_t used_bytes;     // Used bytes of memory left (that were allocated)
   size_t free_bytes;     // Free bytes of memory left (that can be allocated)
   size_t bytes_freed;    // Bytes of memory that underwent 'free' (can be
                          // reallocated).
   size_t used_bytes_HWM; // High-water mark of memory used bytes
   size_t nfrees;         // Total # of calls to free memory

   size_t nfrags_allocated; // Tracked in shm_allocated_frag[] array.

   // Distribution of 'free' calls based on fragment size
   size_t nfrees_eq0;
   size_t nfrees_le32;
   size_t nfrees_le64;
   size_t nfrees_le128;
   size_t nfrees_le256;
   size_t nfrees_le512;
   size_t nfrees_le1K;
   size_t nfrees_le2K;
   size_t nfrees_le4K;
   size_t nfrees_large_frags;

   size_t nf_search_skipped;
   size_t used_by_large_frags_bytes;

   // # of times search in large-fragments array found array full.
   // Non-zero counter implies that there were more concurrent
   // requesters to track a large fragment than we have room to track.
   size_t nlarge_frags_full;

   uint32 nlarge_frags_inuse;
   uint32 nlarge_frags_inuse_HWM;
   int    large_frags_found_in_use;
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
   void *shm_start; // Points to start address of shared segment.
   void *shm_end;   // Points to end address; one past end of sh segment
   void *shm_next;  // Points to next 'free' address to allocate from.

   // Every alloc() and free() will be tracked in this array.
   frag_hdr shm_allocated_frag[SHM_NUM_SMALL_FRAGS];

   free_frag_hdr *shm_free_le64;  // Chain of free-fragments <= 64 bytes
   free_frag_hdr *shm_free_le128; // Chain of free-fragments <= 128 bytes
   free_frag_hdr *shm_free_le256; // Chain of free-fragments <= 256 bytes
   free_frag_hdr *shm_free_le512; // Chain of free-fragments <= 512 bytes
   void          *shm_splinterdb_handle;
   void          *shm_large_frag_hip; // Highest addr of large-fragments tracked

   platform_mutex shm_mem_mutex; // To synchronize alloc & free

   platform_mutex shm_largemem_frags_mutex;
   // Protected by shm_largemem_frags_mutex. Must hold to read or modify.
   shm_large_frag_info shm_largemem_frags[SHM_NUM_LARGE_FRAGS];

   uint32              shm_num_large_frags_tracked;
   int                 shm_id; // Shared memory ID returned by shmget()
   shminfo_usage_stats usage;
   uint64              shm_magic; // Magic identifier for shared memory segment

} PLATFORM_CACHELINE_ALIGNED shmem_heap;

/* Permissions for accessing shared memory and IPC objects */
#define PLATFORM_IPC_OBJS_PERMS (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)

// Function prototypes

static void
platform_shm_track_large_alloc(shmem_heap *shm,
                               void       *addr,
                               size_t      size,
                               const char *func,
                               const int   line);

static void
platform_shm_track_free(shmem_heap  *shm,
                        void        *addr,
                        const size_t size,
                        const char  *objname,
                        const char  *func,
                        const char  *file,
                        const int    line);

static void *
platform_shm_find_large(shmem_heap       *shm,
                        size_t            size,
                        platform_memfrag *memfrag,
                        const char       *objname,
                        const char       *func,
                        const char       *file,
                        const int         line);

static void *
platform_shm_find_small(shmem_heap *shm,
                        size_t      size,
                        const char *objname,
                        const char *func,
                        const char *file,
                        const int   line);

static void
platform_shm_trace_allocs(shmem_heap  *shm,
                          const size_t size,
                          const char  *verb,
                          const void  *retptr,
                          const char  *objname,
                          const char  *func,
                          const char  *file,
                          const int    line);

static int
platform_trace_large_frags(shmem_heap *shm);

bool
platform_shm_heap_valid(shmem_heap *shmheap);

/*
 * platform_heap_id_to_shmaddr() --
 *
 * The shared memory create function returns the address of shmem_heap->shm_id
 * as the platform_heap_id heap-ID to the caller. Rest of Splinter will use this
 * heap-ID as a 'handle' to manage / allocate shared memory. This macro maps
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
   platform_mutex_lock(&shm->shm_mem_mutex);
}

static inline void
shm_unlock_mem(shmem_heap *shm)
{
   platform_mutex_unlock(&shm->shm_mem_mutex);
}

static inline void
shm_lock_mem_frags(shmem_heap *shm)
{
   platform_mutex_lock(&shm->shm_largemem_frags_mutex);
}

static inline void
shm_unlock_mem_frags(shmem_heap *shm)
{
   platform_mutex_unlock(&shm->shm_largemem_frags_mutex);
}

/*
 * platform_isvalid_addr_in_heap(), platform_isvalid_addr_in_shm()
 *
 * Address 'addr' is valid if it's just past end of control block and within
 * shared segment.
 */
static inline bool
platform_isvalid_addr_in_shm(shmem_heap *shmaddr, const void *addr)
{
   return ((addr >= ((void *)shmaddr + platform_shm_ctrlblock_size()))
           && (addr < shmaddr->shm_end));
}

/*
 * Validate that input address 'addr' is a valid address within shared segment
 * region.
 */
bool
platform_isvalid_addr_in_heap(platform_heap_id heap_id, const void *addr)
{
   return platform_isvalid_addr_in_shm(platform_heap_id_to_shmaddr(heap_id),
                                       addr);
}

/*
 * Produce a formatted one-line output of shared memory usage stats / metrics.
 */
void
platform_shm_print_usage_stats(shminfo_usage_stats *usage)
{
   fraction used_bytes_pct;
   fraction free_bytes_pct;  // # of bytes that are free now
   fraction bytes_freed_pct; // # of bytes that were freed over time
   fraction nf_le64_pct;
   fraction nf_le128_pct;
   fraction nf_le256_pct;
   fraction nf_le512_pct;
   fraction nf_le1K_pct;
   fraction nf_le2K_pct;
   fraction nf_le4K_pct;

   used_bytes_pct  = init_fraction(usage->used_bytes, usage->total_bytes);
   free_bytes_pct  = init_fraction(usage->free_bytes, usage->total_bytes);
   bytes_freed_pct = init_fraction(usage->bytes_freed, usage->total_bytes);
   nf_le64_pct     = init_fraction(usage->nfrees_le64, usage->nfrees);
   nf_le128_pct    = init_fraction(usage->nfrees_le128, usage->nfrees);
   nf_le256_pct    = init_fraction(usage->nfrees_le256, usage->nfrees);
   nf_le512_pct    = init_fraction(usage->nfrees_le512, usage->nfrees);
   nf_le1K_pct     = init_fraction(usage->nfrees_le1K, usage->nfrees);
   nf_le2K_pct     = init_fraction(usage->nfrees_le2K, usage->nfrees);
   nf_le4K_pct     = init_fraction(usage->nfrees_le4K, usage->nfrees);

   // clang-format off
   platform_default_log(
      "Shared memory usage stats shmid=%d:"
      " Total=%lu bytes (%s)"
      ", Used=%lu bytes (%s, " FRACTION_FMT(4, 2) " %%)"
      ", Free=%lu bytes (%s, " FRACTION_FMT(4, 2) " %%)"
      ", Freed=%lu bytes (%s, " FRACTION_FMT(4, 2) " %%)"
      ", nfrees=%lu, nf_search_skipped=%lu (%d %%)"
      ", nfrees_eq0=%lu"
      ", nfrees_le32=%lu"
      ", nfrees_le64=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nfrees_le128=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nfrees_le256=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nfrees_le512=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nfrees_le1K=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nfrees_le2K=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nfrees_le4K=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nlarge_frags_inuse=%u"
      ", Large fragments in-use HWM=%u (found in-use=%d)"
      ", consumed=%lu bytes (%s), nlarge_frags_full=%lu"
      ".\n",
      usage->shmid,
      usage->total_bytes,
      size_str(usage->total_bytes),
      usage->used_bytes,
      size_str(usage->used_bytes),
      (FRACTION_ARGS(used_bytes_pct) * 100),
      usage->free_bytes,
      size_str(usage->free_bytes),
      (FRACTION_ARGS(free_bytes_pct) * 100),
      usage->bytes_freed,
      size_str(usage->bytes_freed),
      (FRACTION_ARGS(bytes_freed_pct) * 100),
      usage->nfrees,
      usage->nf_search_skipped,
      (usage->nfrees ? (int)((usage->nf_search_skipped * 100) / usage->nfrees)
                     : 0),
      usage->nfrees_eq0,
      usage->nfrees_le32,
      usage->nfrees_le64,
      (FRACTION_ARGS(nf_le64_pct) * 100),
      usage->nfrees_le128,
      (FRACTION_ARGS(nf_le128_pct) * 100),
      usage->nfrees_le256,
      (FRACTION_ARGS(nf_le256_pct) * 100),
      usage->nfrees_le512,
      (FRACTION_ARGS(nf_le512_pct) * 100),
      usage->nfrees_le1K,
      (FRACTION_ARGS(nf_le1K_pct) * 100),
      usage->nfrees_le2K,
      (FRACTION_ARGS(nf_le2K_pct) * 100),
      usage->nfrees_le4K,
      (FRACTION_ARGS(nf_le4K_pct) * 100),
      usage->nlarge_frags_inuse,
      usage->nlarge_frags_inuse_HWM,
      usage->large_frags_found_in_use,
      usage->used_by_large_frags_bytes,
      size_str(usage->used_by_large_frags_bytes),
      usage->nlarge_frags_full);
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
   *usage                          = shm->usage;
   usage->large_frags_found_in_use = platform_trace_large_frags(shm);
   return usage->large_frags_found_in_use;
}

/*
 * -----------------------------------------------------------------------------
 * Interface to print shared memory usage stats. (Callable from the debugger)
 * This is mainly intended as a diagnostics tool, so we don't work too hard
 * to grab metrics under exclusive access.
 * -----------------------------------------------------------------------------
 */
void
platform_shm_print_usage(platform_heap_id hid)
{
   shmem_heap         *shm = platform_heap_id_to_shmaddr(hid);
   shminfo_usage_stats usage;
   ZERO_STRUCT(usage);

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
   size_t      free_bytes;
   shmem_heap *shm = (shmem_heap *)shmaddr;
   memset(shm, 0, sizeof(*shm));

   shm->shm_start = shmaddr;
   shm->shm_end   = (shmaddr + size);
   shm->shm_next  = (shmaddr + sizeof(shmem_heap));
   shm->shm_id    = shmid;
   shm->shm_magic = SPLINTERDB_SHMEM_MAGIC;

   shm->usage.total_bytes = size;
   free_bytes             = (size - sizeof(shmem_heap));
   shm->usage.free_bytes  = free_bytes;

   // Return 'heap-ID' handle pointing to start addr of shared segment.
   if (heap_id) {
      *heap_id = (platform_heap_id *)shmaddr;
   }

   // Initialize mutexes needed to access shared memory & fragments tracker
   platform_mutex_init(&shm->shm_mem_mutex, platform_get_module_id(), *heap_id);

   platform_mutex_init(
      &shm->shm_largemem_frags_mutex, platform_get_module_id(), *heap_id);

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
platform_status
platform_shmdestroy(platform_heap_id *hid_out)
{
   if (!hid_out) {
      platform_error_log(
         "Error! Attempt to destroy shared memory with NULL heap_handle!");
      return STATUS_BAD_PARAM;
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
      return STATUS_BAD_PARAM;
   }

   // Retain some memory usage stats before releasing shmem
   shminfo_usage_stats usage;
   ZERO_STRUCT(usage);
   int nfrags_in_use = platform_save_usage_stats(&usage, shm);

   platform_status rc = platform_mutex_destroy(&shm->shm_largemem_frags_mutex);
   platform_assert(SUCCESS(rc));

   int shmid = shm->shm_id;
   int rv    = shmdt(shmaddr);
   if (rv != 0) {
      platform_error_log("Failed to detach from shared segment at address "
                         "%p, shmid=%d: %s.\n",
                         shmaddr,
                         shmid,
                         strerror(rv));
      return CONST_STATUS(rv);
   }

   // Externally, heap_id is pointing to this field. In anticipation that the
   // removal of shared segment will succeed, below, clear this out. This way,
   // any future accesses to this shared segment by its heap-ID will run into
   // assertions.
   shm->shm_id = 0;

   rv = shmctl(shmid, IPC_RMID, NULL);
   if (rv != 0) {
      platform_error_log("shmctl failed to remove shared segment at address %p"
                         ", shmid=%d: %s.\n",
                         shmaddr,
                         shmid,
                         strerror(rv));

      // restore state
      shm->shm_id = shmid;
      return CONST_STATUS(rv);
   }

   // Reset globals to NULL; to avoid accessing stale handles.
   Heap_id = NULL;

   // Always trace destroy of shared memory segment.
   platform_default_log("Deallocated SplinterDB shared memory "
                        "segment at %p, shmid=%d.\n",
                        shmaddr,
                        shmid);

   platform_shm_print_usage_stats(&usage);

   // If any fragments were found in-use, that's likely due to something
   // going wrong while free()'ing memory. (This could lead to bloated
   // shared memory usage, if not rectified.)
   return CONST_STATUS(nfrags_in_use);
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
platform_shm_alloc(platform_heap_id  hid,
                   const size_t      size,
                   platform_memfrag *memfrag, // OUT
                   const char       *objname,
                   const char       *func,
                   const char       *file,
                   const int         line)
{
   shmem_heap *shm = platform_heap_id_to_shmaddr(hid);

   debug_assert((platform_shm_heap_valid(shm) == TRUE),
                "Shared memory heap ID at %p is not a valid shared memory ptr.",
                hid);

   platform_assert(((((uint64)shm->shm_next) % PLATFORM_CACHELINE_SIZE) == 0),
                   "[%s:%d] Next free-addr is not aligned: "
                   "shm_next=%p, shm_total_bytes=%lu, shm_used_bytes=%lu"
                   ", shm_free_bytes=%lu",
                   file,
                   line,
                   shm->shm_next,
                   shm->usage.total_bytes,
                   shm->usage.used_bytes,
                   shm->usage.free_bytes);

   void *retptr = NULL;

   // See if we can satisfy requests for large memory fragments from a cached
   // list of used/free fragments that are tracked separately.
   if (size >= SHM_LARGE_FRAG_SIZE) {
      retptr =
         platform_shm_find_large(shm, size, memfrag, objname, func, file, line);
      // Else, fall-back to allocating a new large fragment
      if (retptr != NULL) {
         return retptr;
      }
   } else {
      // Try to satisfy small memory fragments based on requested size, from
      // cached list of free-fragments.
      retptr = platform_shm_find_small(shm, size, objname, func, file, line);
      if (retptr) {
         // Return fragment's details to caller. We may have recycled a free
         // fragment that is larger than the requested size.
         if (memfrag) {
            memfrag->addr = (void *)retptr;
            memfrag->size = ((free_frag_hdr *)retptr)->free_frag_size;
         }
         return retptr;
      }
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
         " to allocate %lu bytes (%s) for '%s'. Approx free"
         " space=%lu bytes (%s)."
         " shm_num_large_frags_tracked=%u, nm_frags_inuse=%u (HWM=%u).\n",
         file,
         line,
         func,
         size,
         size_str(size),
         objname,
         shm->usage.free_bytes,
         size_str(shm->usage.free_bytes),
         shm->shm_num_large_frags_tracked,
         shm->usage.nlarge_frags_inuse,
         shm->usage.nlarge_frags_inuse_HWM);

      // Trace diagnostics
      if (Trace_large_frags) {
         platform_shm_print_usage(hid);
      }
      return NULL;
   }

   // Track approx memory usage metrics; mainly for troubleshooting
   __sync_fetch_and_add(&shm->usage.used_bytes, size);
   __sync_fetch_and_sub(&shm->usage.free_bytes, size);
   if (shm->usage.used_bytes > shm->usage.used_bytes_HWM) {
      shm->usage.used_bytes_HWM = shm->usage.used_bytes;
   }
   // Track new fragment being allocated ...
   if (shm->usage.nfrags_allocated < SHM_NUM_SMALL_FRAGS) {
      frag_hdr *newfrag = &shm->shm_allocated_frag[shm->usage.nfrags_allocated];
      newfrag->frag_addr = retptr;
      newfrag->frag_size = size;
      shm->usage.nfrags_allocated++;
   }

   shm_unlock_mem(shm);

   if (size >= SHM_LARGE_FRAG_SIZE) {
      platform_shm_track_large_alloc(shm, retptr, size, func, line);
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
                                line);
   }
   // A new fragment was carved out of shm. Inform caller of its properties.
   if (memfrag) {
      memfrag->size = size;
      memfrag->addr = retptr;
   }
   return retptr;
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_realloc() -- Re-allocate n-bytes from shared segment.
 *
 * Functionally is similar to 'realloc' system call. We allocate requested #
 * of bytes, *newsize, copy over the old contents (if any), and free the
 * memory for the oldptr.
 *
 * NOTE(s):
 *  - This interface does -not- do any cache-line alignment for '*newsize'.
 *    Caller is expected to do so. platform_realloc() takes care of it.
 *  - However, it is quite likely that for a fragment request, we might be
 *    recycling a (small/large) free-fragment, whose size may be bigger
 *    than requested 'newsize' (but will guranteed to be cache line aligned).
 *
 * Returns ptr to re-allocated memory. May return a bigger *newsize, if
 * a free fragment was recycled and re-allocated.
 * -----------------------------------------------------------------------------
 */
void *
platform_shm_realloc(platform_heap_id hid,
                     void            *oldptr,
                     const size_t     oldsize,
                     size_t          *newsize, // IN/OUT
                     const char      *func,
                     const char      *file,
                     const int        line)
{
   static const char *unknown_obj = "UnknownObj";

   platform_memfrag realloc_memfrag = {0};

   // clang-format off
   void *retptr = platform_shm_alloc(hid, *newsize, &realloc_memfrag,
                                     unknown_obj, func, file, line);
   // clang-format on
   if (retptr) {

      // Copy over old contents, if any, and free that old memory piece
      if (oldptr && oldsize) {
         memcpy(retptr, oldptr, oldsize);
         platform_shm_free(hid, oldptr, oldsize, unknown_obj, func, file, line);
      }
      // A large free-fragment might have been recycled. Its size may be
      // bigger than the requested '*newsize'. Return new size to caller.
      // (This is critical, otherwise, asserts will trip when an attempt
      // is made by caller to free this fragment.)
      *newsize = memfrag_size(&realloc_memfrag);
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
                  const size_t     size,
                  const char      *objname,
                  const char      *func,
                  const char      *file,
                  const int        line)
{
   shmem_heap *shm = platform_heap_id_to_shmaddr(hid);

   debug_assert(
      (platform_shm_heap_valid(shm) == TRUE),
      "Shared memory heap ID at %p is not a valid shared memory handle.",
      hid);

   if (!platform_isvalid_addr_in_heap(hid, ptr)) {
      platform_assert(FALSE,
                      "[%s:%d::%s()] -> %s: Requesting to free memory at %p"
                      ", for object '%s' which is a memory fragment not"
                      " allocated from shared memory {start=%p, end=%p}.\n",
                      file,
                      line,
                      func,
                      __func__,
                      ptr,
                      objname,
                      platform_shm_lop(hid),
                      platform_shm_hip(hid));
      return;
   }

   debug_assert((size > 0),
                "[%s:%d::%s()] -> %s: Attempting to free memory fragment at %p"
                " of size=%lu bytes, for object '%s'.",
                file,
                line,
                func,
                __func__,
                ptr,
                size,
                objname);

   platform_shm_track_free(shm, ptr, size, objname, func, file, line);

   if (Trace_shmem || Trace_shmem_frees) {
      platform_default_log("  [%s:%d::%s()] -> %s: Request to free memory at "
                           "%p for object '%s'.\n",
                           file,
                           line,
                           func,
                           __func__,
                           ptr,
                           objname);
   }
   return;
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_track_large_alloc() - Track the allocation of this large
 * fragment. 'Tracking' here means we record this large-fragment in an array
 * tracking large-memory fragments allocated.
 * -----------------------------------------------------------------------------
 */
static void
platform_shm_track_large_alloc(shmem_heap *shm,
                               void       *addr,
                               size_t      size,
                               const char *func,
                               const int   line)
{
   debug_assert(
      (size >= SHM_LARGE_FRAG_SIZE),
      "Incorrect usage of this interface for requested size=%lu bytes."
      " Size should be >= %lu bytes.\n",
      size,
      SHM_LARGE_FRAG_SIZE);

   bool                 found_free_slot = FALSE;
   shm_large_frag_info *frag            = shm->shm_largemem_frags;
   int                  fctr            = 0;

   shm_lock_mem_frags(shm);

   // Iterate through the list of memory fragments being tracked.
   while ((fctr < ARRAY_SIZE(shm->shm_largemem_frags)) && frag->frag_addr) {

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
   if (fctr < ARRAY_SIZE(shm->shm_largemem_frags)) {
      found_free_slot = TRUE;
      shm->shm_num_large_frags_tracked++;
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

      frag->frag_allocated_to_pid = getpid();
      frag->frag_allocated_to_tid = platform_get_tid();

      frag->frag_func = func;
      frag->frag_line = line;

      // The freed_by_pid/freed_by_tid == 0 means fragment is still allocated.

      // Track highest address of large-fragment that is being tracked.
      if (shm->shm_large_frag_hip < addr) {
         shm->shm_large_frag_hip = addr;
      }
   }
   if (!found_free_slot) {
      shm->usage.nlarge_frags_full++;
   }
   shm_unlock_mem_frags(shm);
}

/*
 * Connect a free fragment to the chain provided. Record free-fragment's
 * size, so we can find it when next required by-size.
 */
static inline void
platform_shm_hook_free_frag(free_frag_hdr **here, void *ptr, size_t size)
{
   ((free_frag_hdr *)ptr)->free_frag_next = *here;
   ((free_frag_hdr *)ptr)->free_frag_size = size;
   *here                                  = ptr;
}

/*
 * -----------------------------------------------------------------------------
 * Helper functions for finding 'free' fragment and to walk free-lists.
 * -----------------------------------------------------------------------------
 */
/*
 * Simple lookup routine to return the free-fragment header off of which
 * free-fragments of a specific 'size' will be hung off of.
 * No mutex is required here, as we are simply mapping size to a field's addr.
 */
static free_frag_hdr **
platform_shm_free_frag_hdr(const shmem_heap *shm, size_t size)
{
   free_frag_hdr **next_frag;
   if (size <= 64) {
      next_frag = (free_frag_hdr **)&shm->shm_free_le64;
   } else if (size <= 128) {
      next_frag = (free_frag_hdr **)&shm->shm_free_le128;
   } else if (size <= 256) {
      next_frag = (free_frag_hdr **)&shm->shm_free_le256;
   } else if (size <= 512) {
      next_frag = (free_frag_hdr **)&shm->shm_free_le512;
   } else {
      // Currently unsupported fragment size for recycling
      next_frag = NULL;
   }
   return next_frag;
}

/*
 * -----------------------------------------------------------------------------
 * When a memory fragment is being free'd, check if this fragment is already
 * in some free-list. If found, it means we are [incorrectly] doing a
 * double-free, which indicates a code error. User has possibly messed-up their
 * handling of memfrag handles to this memory fragment.
 *
 * NOTE: This, being a convenience routine, provides for next_frag, which is
 * the start of the free-list for given 'size'. If caller has established it,
 * pass that here. Otherwise, we will establish it in this routine.
 *
 * Shared memory mutex is expected to be held for this function.
 * -----------------------------------------------------------------------------
 */
static void *
platform_shm_find_frag_in_free_list(const shmem_heap *shm,
                                    free_frag_hdr   **next_frag,
                                    const void       *ptr,
                                    const size_t      size)
{
   if (!next_frag) {
      free_frag_hdr **next_frag = platform_shm_free_frag_hdr(shm, size);
      // We are searching for a fragment whose size is not tracked.
      if (next_frag == NULL) { // Nothing found.
         return NULL;
      }
   }

   // Walk the free-list to see if our being-free'd ptr lives there already
   while (*next_frag && ((*next_frag) != ptr)) {
      next_frag = &(*next_frag)->free_frag_next;
   }
   // Returns the 'ptr' if found; null otherwise.
   return (*next_frag);
}

/*
 * -----------------------------------------------------------------------------
 * Diagnostic routine: Iterate through all small-fragment free-lists that
 * we currently manage and try to find if the small fragment at address 'ptr'
 * is found in any such list. That means, the fragment was previously freed.
 *
 * Returns: The size of the free-fragment list in which this 'ptr' was found.
 *  0, otherwise; (i.e. 'ptr' is not an already-freed-fragment.)
 *  Optionally, the size marked in this freed-fragment is returned via
 *  'freed_size'. If a client incorrectly specified the memfrag's size at
 *  the time of free(), that will be reported here, and can be detected.
 * -----------------------------------------------------------------------------
 */
static size_t
platform_shm_find_frag_in_freed_lists(const shmem_heap *shm,
                                      const void       *ptr,
                                      size_t           *freed_size)
{
   size_t free_list_size = SHM_SMALL_FRAG_MIN_SIZE;

   // Process all free-list sizes, till we find the being-freed fragment
   while (free_list_size <= SHM_SMALL_FRAG_MAX_SIZE) {
      free_frag_hdr **next_frag =
         platform_shm_free_frag_hdr(shm, free_list_size);

      free_frag_hdr *found_free_frag;
      if ((found_free_frag = platform_shm_find_frag_in_free_list(
              shm, next_frag, ptr, free_list_size)))
      {
         // Return the size as marked on the fragment when it was freed.
         if (freed_size) {
            *freed_size = found_free_frag->free_frag_size;
         }
         // We found this fragment 'ptr' in this free-fragment-list!
         return free_list_size;
      }
      free_list_size *= 2;
   }
   return 0;
}

/*
 * -----------------------------------------------------------------------------
 * Walk the list tracking allocated small fragments to see if our fragment at
 * 'addr' (which is being freed) is an allocated fragment.
 * Shared-memory alloc/free mutex should be held on entry.
 *
 * Return the index of this tracked fragment, if found. -1, otherwise.
 * -----------------------------------------------------------------------------
 */
static int
platform_shm_find_small_frag_in_allocated_list(shmem_heap *shm, void *addr)
{
   int ictr = (shm->usage.nfrags_allocated - 1);
   while (ictr >= 0) {
      if (shm->shm_allocated_frag[ictr].frag_addr == addr) {
         break;
      }
      ictr--;
   }
   return ictr;
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_track_free_small_frag() - Track 'free' of small fragments.
 *
 * Free this small fragment, and using the 'size' specified, connect it to
 * the free-list tracking fragments of this size.
 * -----------------------------------------------------------------------------
 */
static void
platform_shm_track_free_small_frag(shmem_heap  *shm,
                                   void        *addr,
                                   const size_t size,
                                   const char  *objname,
                                   const char  *func,
                                   const char  *file,
                                   const int    line)
{
   shm_lock_mem(shm);

   int frag_idx = platform_shm_find_small_frag_in_allocated_list(shm, addr);

   if (frag_idx >= 0) {
      // A fragment being freed should be freed with the same size it
      // was allocated with.
      platform_assert((shm->shm_allocated_frag[frag_idx].frag_size == size),
                      "objname=%s, func=%s, file=%s, line=%d: "
                      "Attempt to free fragment at %p of size=%lu bytes"
                      ", but the size of the small fragment, %lu bytes"
                      ", tracked at index=%d, does not match requested size.",
                      objname,
                      func,
                      file,
                      line,
                      addr,
                      size,
                      shm->shm_allocated_frag[frag_idx].frag_size,
                      frag_idx);

      // Allocated fragment is being freed; shuffle remaining items to the left.
      int items_to_move = (shm->usage.nfrags_allocated - (frag_idx + 1));
      debug_assert((items_to_move >= 0), "items_to_move=%d", items_to_move);
      if (items_to_move > 0) {
         memmove(&shm->shm_allocated_frag[frag_idx],
                 &shm->shm_allocated_frag[frag_idx + 1],
                 (items_to_move * sizeof(shm->shm_allocated_frag[0])));
      }

      shm->usage.nfrags_allocated--;
   }

   shm->usage.nfrees++;
   shm->usage.bytes_freed += size;
   // If this fragment-being-free'd is one of a size we track, find
   // the free-list into which the free'd-fragment should be linked.
   free_frag_hdr **next_frag = platform_shm_free_frag_hdr(shm, size);
   if (next_frag) {

      // clang-format off
         debug_code(size_t found_in_free_list_size = 0);
         debug_code(size_t free_frag_size = 0);
         debug_assert(((found_in_free_list_size
                         = platform_shm_find_frag_in_freed_lists(shm,
                                                                 addr,
                                                                 &free_frag_size))
                                == 0),
                        "Memory fragment being-freed, %p, of size=%lu bytes"
                        " was found in freed-fragment-list of size=%lu bytes"
                        ", and marked as %lu bytes size.",
                        addr, size, found_in_free_list_size, free_frag_size);
      // clang-format on

      /*
       * ------------------------------------------------------------------
       * Hook this now-free fragment into its free-list.
       *
       * NOTE: There is a potential for a small memory-leak, which may
       * just be benign. We may have allocated via TYPED_MALLOC() for a
       * single structure. alloc() and free() interfaces -do- round-up
       * the sizeof(struct) for alignment. But it could well be that we
       * recycled a small fragment from a free-list, where the fragment's
       * original size was larger than sizeof(struct). So, come now to
       * free()-time, caller only knows of sizeof(struct) which will be
       * smaller than the actual fragment's size. This leads to a small
       * bit of unused space in the to-be-freed-fragment, but should not
       * cause any memory corruption.
       * ------------------------------------------------------------------
       */
      platform_shm_hook_free_frag(next_frag, addr, size);
   }

   // Maintain metrics here onwards
   shm->usage.nf_search_skipped++; // Track # of optimizations done

   if (size == 0) {
      shm->usage.nfrees_eq0++;
   } else if (size <= 32) {
      shm->usage.nfrees_le32++;
   } else if (size <= 64) {
      shm->usage.nfrees_le64++;
   } else if (size <= 128) {
      shm->usage.nfrees_le128++;
   } else if (size <= 256) {
      shm->usage.nfrees_le256++;
   } else if (size <= 512) {
      shm->usage.nfrees_le512++;
   } else if (size <= KiB) {
      shm->usage.nfrees_le1K++;
   } else if (size <= (2 * KiB)) {
      shm->usage.nfrees_le2K++;
   } else if (size <= (4 * KiB)) {
      shm->usage.nfrees_le4K++;
   }
   shm->usage.used_bytes -= size;
   shm->usage.free_bytes += size;

   shm_unlock_mem(shm);
   return;
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_track_free_large_frag() - Track 'free' of large fragments.
 *
 * See if this large memory fragment being freed is already being tracked. If
 * so, it can be re-cycled after this free. Do the book-keeping accordingly to
 * record that this large-fragment is no longer in-use and can be recycled.
 * -----------------------------------------------------------------------------
 */
static void
platform_shm_track_free_large_frag(shmem_heap  *shm,
                                   void        *addr,
                                   const size_t size,
                                   const char  *objname,
                                   const char  *func,
                                   const char  *file,
                                   const int    line)
{
   shm_lock_mem_frags(shm);
   shm->usage.nfrees++;
   shm->usage.bytes_freed += size;
   shm->usage.nfrees_large_frags++;

   bool found_tracked_frag = FALSE;
   bool trace_shmem        = (Trace_shmem || Trace_shmem_frees);

   shm_large_frag_info *frag = shm->shm_largemem_frags;
   int                  fctr = 0;

   // Search the large-fragment tracking array for this fragment being freed.
   // If found, mark its tracker that this fragment is free & can be recycled.
   while ((fctr < ARRAY_SIZE(shm->shm_largemem_frags))
          && (!frag->frag_addr || (frag->frag_addr != addr)))
   {
      fctr++;
      frag++;
   }

   if (fctr < ARRAY_SIZE(shm->shm_largemem_frags)) {
      debug_assert(frag->frag_addr == addr);
      found_tracked_frag = TRUE;

      // Cross-check the recording we did when fragment was allocated
      // We cannot check the tid as the parent process' tid may be 0.
      // We could have come across a free fragment that was previously
      // used by the parent process.
      // debug_assert(frag->frag_allocated_to_tid != 0);
      debug_assert(frag->frag_allocated_to_pid != 0);
      debug_assert(frag->frag_size != 0);

      // -----------------------------------------------------------------
      // If a client allocated a large-fragment previously, it should be
      // freed with the right original size (for hygiene). It's not
      // really a correctness error as the fragment's size has been
      // recorded initially when it was allocated. Initially, we tried
      // to make this a strict "size == frag->frag_size" check.
      // But, it trips for various legit reasons. One example is when
      // we call TYPED_MALLOC() to allocate memory for a large struct.
      // This request may have gone through large free-fragment recycling
      // scheme, in which case we could have allocated a free-fragment with
      // a size, frag->frag_size, much larger than requested 'size'.
      // -----------------------------------------------------------------
      debug_assert((size <= frag->frag_size),
                   "Attempt to free a large fragment, %p, with size=%lu"
                   ", but fragment has size of %lu bytes (%s).",
                   addr,
                   size,
                   frag->frag_size,
                   size_str(frag->frag_size));

      // Record the process/thread's identity to mark the fragment as free.
      frag->frag_freed_by_pid = getpid();
      frag->frag_freed_by_tid = platform_get_tid();
      shm->usage.nlarge_frags_inuse--;

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

   // We expect that callers invoke the free correctly with the right memory
   // fragment handle. Not finding a large fragment requested to be freed
   // indicates some coding error.
   debug_assert(found_tracked_frag,
                "[%s:%d:%s()] Request to track large fragment failed."
                " Fragment %p, %lu bytes, for object '%s' is not tracked\n",
                file,
                line,
                func,
                addr,
                size,
                objname);

   if (!found_tracked_frag && trace_shmem) {
      platform_default_log("[OS-pid=%d, ThreadID=%lu, %s:%d::%s()] "
                           ", Fragment %p for object '%s' is not tracked\n",
                           getpid(),
                           platform_get_tid(),
                           file,
                           line,
                           func,
                           addr,
                           objname);
   }
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_track_free() - Track 'free' of small and large fragments.
 * -----------------------------------------------------------------------------
 */
static void
platform_shm_track_free(shmem_heap  *shm,
                        void        *addr,
                        const size_t size,
                        const char  *objname,
                        const char  *func,
                        const char  *file,
                        const int    line)
{
   // All callers of either platform_free() or platform_free_mem() are required
   // to declare the size of the memory fragment being freed. We use that info
   // to manage free lists.
   platform_assert((size > 0),
                   "objname=%s, func=%s, file=%s, line=%d",
                   objname,
                   func,
                   file,
                   line);

   // If we are freeing a fragment beyond the high-address of all
   // large fragments tracked, then this is certainly not a large
   // fragment. So, no further need to see if it's a tracked large-fragment.
   if ((addr > shm->shm_large_frag_hip) || (size && size < SHM_LARGE_FRAG_SIZE))
   {
      /* **** Tracking 'free' on smaller fragments. **** */
      platform_shm_track_free_small_frag(
         shm, addr, size, objname, func, file, line);
   } else {

      /* **** Tracking 'free' on large fragments. **** */
      platform_shm_track_free_large_frag(
         shm, addr, size, objname, func, file, line);
   }
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_find_large() - Search the array of large-fragments being tracked
 * to see if there is an already allocated and now-free large memory fragment.
 * As the array of free large-fragments is small, we do a best-fit search.
 * If so, allocate that fragment to this requester. Do the book-keeping
 * accordingly.
 * -----------------------------------------------------------------------------
 */
static void *
platform_shm_find_large(shmem_heap       *shm,
                        size_t            size,
                        platform_memfrag *memfrag,
                        const char       *objname,
                        const char       *func,
                        const char       *file,
                        const int         line)
{
   debug_assert((size >= SHM_LARGE_FRAG_SIZE),
                "Incorrect usage of this interface for requested"
                " size=%lu bytes. Size should be >= %lu bytes.\n",
                size,
                SHM_LARGE_FRAG_SIZE);

   void                *retptr = NULL;
   shm_large_frag_info *frag   = shm->shm_largemem_frags;
   int local_in_use = 0; // Tracked while iterating in this fn, locally

   int  found_at_fctr      = -1;
   bool found_tracked_frag = FALSE;

   shm_lock_mem_frags(shm);

   uint32 shm_num_large_frags_tracked = shm->shm_num_large_frags_tracked;
   uint32 shm_num_large_frags_inuse   = shm->usage.nlarge_frags_inuse;

   for (int fctr = 0; fctr < ARRAY_SIZE(shm->shm_largemem_frags);
        fctr++, frag++) {
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

      // Fragment is free, but is it big enough for current request?
      if (frag->frag_size < size) {
         continue;
      }
      found_tracked_frag = TRUE;
      found_at_fctr      = fctr;

      // Record the process/thread to which free fragment is being allocated
      frag->frag_allocated_to_pid = getpid();
      frag->frag_allocated_to_tid = platform_get_tid();

      shm->usage.nlarge_frags_inuse++;
      if (shm->usage.nlarge_frags_inuse > shm->usage.nlarge_frags_inuse_HWM) {
         shm->usage.nlarge_frags_inuse_HWM = shm->usage.nlarge_frags_inuse;
      }
      shm_num_large_frags_inuse = shm->usage.nlarge_frags_inuse;

      // Now, mark that this fragment is in-use
      frag->frag_freed_by_pid = 0;
      frag->frag_freed_by_tid = 0;

      frag->frag_func = func;
      frag->frag_line = line;

      retptr = frag->frag_addr;
      if (memfrag) {
         memfrag->addr = retptr;
         memfrag->size = frag->frag_size;
      }

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
          * between local_in_use and shm_num_large_frags_inuse. The latter is a
          * global counter while the former is a local counter. We may have
          * found a free fragment to reallocate early in the array w/o
          * processing the full array. Hence, these two values are likely to
          * diff (by a big margin, even).
          */
         snprintf(msg,
                  sizeof(msg),
                  "Reallocated free fragment at slot=%d, addr=%p, "
                  "shm_num_large_frags_tracked=%u, shm_num_large_frags_inuse=%u"
                  " (local_in_use=%d)",
                  found_at_fctr,
                  retptr,
                  shm_num_large_frags_tracked,
                  shm_num_large_frags_inuse,
                  local_in_use);
      } else {
         snprintf(
            msg,
            sizeof(msg),
            "Did not find free fragment of size=%lu bytes to reallocate."
            " shm_num_large_frags_tracked=%u, shm_num_large_frags_inuse=%u"
            " (local_in_use=%d)",
            size,
            shm_num_large_frags_tracked,
            shm_num_large_frags_inuse,
            local_in_use);
      }
      platform_shm_trace_allocs(
         shm, size, msg, retptr, objname, func, file, line);
   }
   return retptr;
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_find_small() - Find a small free-fragment in a cached list of
 * free fragments that we track, for specific buckets of fragment sizes.
 * If one is found of the suitable size, detach it from the list and return
 * start address of the free fragment. Otherwise, return NULL.
 *
 * NOTE: As the free-fragments are linked using free_frag_hdr{}, we return
 * the address of the recycled free-fragment, which can be temporarily
 * read as free_frag_hdr{} *.
 * -----------------------------------------------------------------------------
 */
static void *
platform_shm_find_small(shmem_heap *shm,
                        size_t      size,
                        const char *objname,
                        const char *func,
                        const char *file,
                        const int   line)
{
   // Currently, we have only implemented tracking small free fragments of
   // 'known' sizes that appear in our workloads.
   if ((size < SHM_SMALL_FRAG_MIN_SIZE) || (size > SHM_SMALL_FRAG_MAX_SIZE)) {
      return NULL;
   }

   // If we are not tracking fragments of this size, nothing further to do.
   free_frag_hdr **next_frag = platform_shm_free_frag_hdr(shm, size);
   if (next_frag == NULL) {
      return NULL;
   }
   shm_lock_mem(shm);

   // Find the next free frag which is big enough
   while (*next_frag && ((*next_frag)->free_frag_size < size)) {
      next_frag = &(*next_frag)->free_frag_next;
   }

   // If we ran thru the list of free fragments, we are done
   free_frag_hdr *retptr = *next_frag;
   if (!retptr) {
      shm_unlock_mem(shm);
      return NULL;
   }
   *next_frag             = retptr->free_frag_next;
   retptr->free_frag_next = NULL;

   shm->usage.used_bytes += retptr->free_frag_size;
   shm->usage.free_bytes -= retptr->free_frag_size;

   // Track new fragment being allocated ...
   if (shm->usage.nfrags_allocated < SHM_NUM_SMALL_FRAGS) {
      frag_hdr *newfrag = &shm->shm_allocated_frag[shm->usage.nfrags_allocated];
      newfrag->frag_addr = retptr;
      newfrag->frag_size = retptr->free_frag_size;
      shm->usage.nfrags_allocated++;
   }

   shm_unlock_mem(shm);
   return (void *)retptr;
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
   int local_in_use          = 0; // Tracked while iterating in this fn, locally
   shm_large_frag_info *frag = shm->shm_largemem_frags;

   const threadid tid = platform_get_tid();

   // Walk the tracked-fragments array looking for an in-use fragment
   for (int fctr = 0; fctr < ARRAY_SIZE(shm->shm_largemem_frags);
        fctr++, frag++) {
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

      platform_error_log("  **** [TID=%lu] Large Fragment at slot=%d"
                         ", addr=%p, size=%lu (%s)."
                         " Allocated at func=%s, line=%d, is in-use"
                         ", allocated_to_pid=%d, allocated_to_tid=%lu\n",
                         tid,
                         fctr,
                         frag->frag_addr,
                         frag->frag_size,
                         size_str(frag->frag_size),
                         frag->frag_func,
                         frag->frag_line,
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
   shmem_heap *shm            = platform_heap_id_to_shmaddr(heap_id);
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

bool
platform_shm_next_free_cacheline_aligned(platform_heap_id heap_id)
{
   return (
      (((uint64)platform_shm_next_free_addr(heap_id)) % PLATFORM_CACHELINE_SIZE)
      == 0);
}

/*
 * Test helper-method: Find out if a memory fragment is found in any
 * free-lists?
 *
 * Returns - If found, the 'size' that free-list tracks. 0, otherwise.
 */
size_t
platform_shm_find_freed_frag(platform_heap_id heap_id,
                             const void      *addr,
                             size_t          *freed_frag_size)
{
   shmem_heap *shm = platform_heap_id_to_shmaddr(heap_id);
   return platform_shm_find_frag_in_freed_lists(shm, addr, freed_frag_size);
}

static void
platform_shm_trace_allocs(shmem_heap  *shm,
                          const size_t size,
                          const char  *verb,
                          const void  *retptr,
                          const char  *objname,
                          const char  *func,
                          const char  *file,
                          const int    line)
{
   platform_default_log("  [OS-pid=%d,ThreadID=%lu, %s:%d::%s()] "
                        "-> %s: %s size=%lu bytes (%s)"
                        " for object '%s', at %p, "
                        "free bytes=%lu (%s).\n",
                        getpid(),
                        platform_get_tid(),
                        file,
                        line,
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
