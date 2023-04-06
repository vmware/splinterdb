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
static bool Trace_shmem_allocs = FALSE;
static bool Trace_shmem_frees  = FALSE;
static bool Trace_shmem        = FALSE;
static bool Trace_large_frags  = FALSE;

/*
 * ---------------------------------------------------------------------------
 * shm_frag_info{} - Struct describing a large memory fragment allocation.
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
typedef struct shm_frag_info {
   void *shm_frag_addr; // Start address of this memory fragment
                        // NULL => tracking fragment is empty
   const char *shm_func;
   size_t      shm_frag_size;             // bytes
   int         shm_frag_allocated_to_pid; // Allocated to this OS-pid
   threadid shm_frag_allocated_to_tid; // Allocated to this Splinter thread-ID
   int      shm_frag_freed_by_pid;     // OS-pid that freed this
   threadid shm_frag_freed_by_tid;     // Splinter thread-ID that freed this
   int      shm_line;
} shm_frag_info;

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
 * Each free fragment that is hooked into the free-list is described by this
 * tiny tracking structure.
 */
typedef struct free_frag_hdr {
   struct free_frag_hdr *free_frag_next;
   size_t                free_frag_size;
} free_frag_hdr;

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
 *    done when freeing any fragment, to see if it's a large-fragment.
 *  - shm_nf_search_skipped counts the # of times we were able to short-circuit
 *    this search using above optimization.
 * -----------------------------------------------------------------------------
 */
typedef struct shmem_info {
   void *shm_start; // Points to start address of shared segment.
   void *shm_end;   // Points to end address; one past end of sh segment
   void *shm_next;  // Points to next 'free' address to allocate from.

   free_frag_hdr *shm_free_le64; // Chain of free-fragments of size <= 64 bytes
   free_frag_hdr
      *shm_free_le128; // Chain of free-fragments of size <= 128 bytes

   void *shm_splinterdb_handle;
   void *shm_large_frag_hip; // Highest addr of large-fragments tracked

   platform_mutex shm_mem_frags_mutex;
   // Protected by shm_mem_frags_mutex. Must hold to read or modify.
   shm_frag_info shm_mem_frags[SHM_NUM_LARGE_FRAGS];
   uint32        shm_num_frags_tracked;
   uint32        shm_num_frags_inuse;
   uint32        shm_num_frags_inuse_HWM;
   size_t        shm_used_by_large_frags; // Actually reserved
   size_t        shm_nfrees;              // # of calls to free memory

   // Distribution of free calls to diff sizes of memory fragments
   size_t shm_nfrees_le32;
   size_t shm_nfrees_le64;
   size_t shm_nfrees_le128;
   size_t shm_nfrees_le256;
   size_t shm_nfrees_le512;
   size_t shm_nfrees_le1K;
   size_t shm_nfrees_le2K;
   size_t shm_nfrees_le4K;
   size_t shm_nfrees_large_frags;

   size_t shm_nf_search_skipped; // See above;

   size_t shm_total_bytes; // Total size of shared segment allocated initially.
   size_t shm_free_bytes;  // Free bytes of memory left (that can be allocated)
   size_t shm_used_bytes;  // Used bytes of memory left (that were allocated)
   size_t shm_bytes_freed; // Bytes of memory that underwent 'free' (can be
                           // reallocated)
   uint64 shm_magic;       // Magic identifier for shared memory segment
   int    shm_id;          // Shared memory ID returned by shmget()

} PLATFORM_CACHELINE_ALIGNED shmem_info;

/* Permissions for accessing shared memory and IPC objects */
#define PLATFORM_IPC_OBJS_PERMS (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)

// Function prototypes

static void
platform_shm_track_alloc(shmem_info *shm,
                         void       *addr,
                         size_t      size,
                         const char *func,
                         const int   line);

static void
platform_shm_track_free(shmem_info  *shm,
                        void        *addr,
                        const size_t size,
                        const char  *objname,
                        const char  *func,
                        const char  *file,
                        const int    line);

static void *
platform_shm_find_large(shmem_info *shm,
                        size_t      size,
                        const char *objname,
                        const char *func,
                        const char *file,
                        const int   line);

static void *
platform_shm_find_frag(shmem_info *shm,
                       size_t      size,
                       const char *objname,
                       const char *func,
                       const char *file,
                       const int   line);

static void
platform_shm_trace_allocs(shmem_info  *shminfo,
                          const size_t size,
                          const char  *verb,
                          const void  *retptr,
                          const char  *objname,
                          const char  *func,
                          const char  *file,
                          const int    line);

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

/*
 * Evaluates to valid 'high' address within shared segment.
 * shm_end points to one-byte-just-past-end of shared segment.
 * Valid 'high' byte is last byte just before shm_end inside segment.
 */
static inline void *
platform_shm_hip(platform_heap_id hid)
{
   return (((shmem_info *)platform_heap_id_to_shmaddr(hid))->shm_end - 1);
}

static inline void
shm_lock_mem_frags(shmem_info *shminfo)
{
   platform_mutex_lock(&shminfo->shm_mem_frags_mutex);
}

static inline void
shm_unlock_mem_frags(shmem_info *shminfo)
{
   platform_mutex_unlock(&shminfo->shm_mem_frags_mutex);
}

/*
 * platform_valid_addr_in_heap(), platform_valid_addr_in_shm()
 *
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
 * Validate that input address 'addr' is a valid address within shared segment
 * region.
 */
bool
platform_valid_addr_in_heap(platform_heap_id heap_id, const void *addr)
{
   return platform_valid_addr_in_shm(platform_heap_id_to_handle(heap_id), addr);
}

/*
 * ------------------------------------------------------------------------
 * Shared-memory usage statistics & metrics:
 *
 * Set of usage-stats fields copied from shmem_info{} struct, so that we
 * can print these after shared segment has been destroyed.
 * ------------------------------------------------------------------------
 */
typedef struct shminfo_usage_stats {
   size_t total_bytes;
   size_t used_bytes;
   size_t free_bytes;
   size_t bytes_freed;
   size_t nfrees;
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
   uint32 frags_inuse_HWM;
   int    large_frags_found_in_use;
   int    shmid;
} shminfo_usage_stats;

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
      ", nfrees_le32=%lu"
      ", nfrees_le64=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nfrees_le128=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nfrees_le256=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nfrees_le512=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nfrees_le1K=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nfrees_le2K=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", nfrees_le4K=%lu (" FRACTION_FMT(4, 2) " %%)"
      ", Large fragments in-use HWM=%u (found in-use=%d)"
      ", consumed=%lu bytes (%s)"
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
      usage->frags_inuse_HWM,
      usage->large_frags_found_in_use,
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
platform_save_usage_stats(shminfo_usage_stats *usage, shmem_info *shminfo)
{
   usage->shmid                     = shminfo->shm_id;
   usage->total_bytes               = shminfo->shm_total_bytes;
   usage->used_bytes                = shminfo->shm_used_bytes;
   usage->free_bytes                = shminfo->shm_free_bytes;
   usage->bytes_freed               = shminfo->shm_bytes_freed;
   usage->nfrees                    = shminfo->shm_nfrees;
   usage->nfrees_le32               = shminfo->shm_nfrees_le32;
   usage->nfrees_le64               = shminfo->shm_nfrees_le64;
   usage->nfrees_le128              = shminfo->shm_nfrees_le128;
   usage->nfrees_le256              = shminfo->shm_nfrees_le256;
   usage->nfrees_le512              = shminfo->shm_nfrees_le512;
   usage->nfrees_le1K               = shminfo->shm_nfrees_le1K;
   usage->nfrees_le2K               = shminfo->shm_nfrees_le2K;
   usage->nfrees_le4K               = shminfo->shm_nfrees_le4K;
   usage->nfrees_large_frags        = shminfo->shm_nfrees_large_frags;
   usage->nf_search_skipped         = shminfo->shm_nf_search_skipped;
   usage->used_by_large_frags_bytes = shminfo->shm_used_by_large_frags;
   usage->frags_inuse_HWM           = shminfo->shm_num_frags_inuse_HWM;
   usage->large_frags_found_in_use  = platform_trace_large_frags(shminfo);
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
   shmem_info         *shminfo = (shmem_info *)platform_heap_id_to_shmaddr(hid);
   shminfo_usage_stats usage;
   ZERO_STRUCT(usage);

   platform_save_usage_stats(&usage, shminfo);
   platform_shm_print_usage_stats(&usage);
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
   platform_mutex_init(
      &shminfo->shm_mem_frags_mutex, platform_get_module_id(), *heap_id);

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
platform_shmdestroy(platform_heap_handle *heap_handle)
{
   if (!heap_handle) {
      platform_error_log(
         "Error! Attempt to destroy shared memory with NULL heap_handle!");
      return STATUS_BAD_PARAM;
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
      return STATUS_BAD_PARAM;
   }

   // Retain some memory usage stats before releasing shmem
   shminfo_usage_stats usage;
   ZERO_STRUCT(usage);
   int nfrags_in_use = platform_save_usage_stats(&usage, shminfo);

   platform_status rc = platform_mutex_destroy(&shminfo->shm_mem_frags_mutex);
   platform_assert(SUCCESS(rc));

   int shmid = shminfo->shm_id;
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
   shminfo->shm_id = 0;

   rv = shmctl(shmid, IPC_RMID, NULL);
   if (rv != 0) {
      platform_error_log("shmctl failed to remove shared segment at address %p"
                         ", shmid=%d: %s.\n",
                         shmaddr,
                         shmid,
                         strerror(rv));

      // restore state
      shminfo->shm_id = shmid;
      return CONST_STATUS(rv);
   }

   // Reset globals to NULL; to avoid accessing stale handles.
   Heap_id     = NULL;
   Heap_handle = NULL;

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
                   const int        line)
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
      line,
      shminfo->shm_next,
      shminfo->shm_total_bytes,
      shminfo->shm_used_bytes,
      shminfo->shm_free_bytes);

   void *retptr = NULL;

   // See if we can satisfy requests for large memory fragments from a cached
   // list of used/free fragments that are tracked separately.
   if (size >= SHM_LARGE_FRAG_SIZE) {
      if ((retptr =
              platform_shm_find_large(shminfo, size, objname, func, file, line))
          != NULL)
      {
         return retptr;
      }
   } else {
      // Try to satisfy small memory fragments based on requested size
      retptr = platform_shm_find_frag(shminfo, size, objname, func, file, line);
      if (retptr) {
         return retptr;
      }
   }

   _Static_assert(sizeof(void *) == sizeof(size_t),
                  "check our casts are valid");
   // Optimistically, allocate the requested 'size' bytes of memory.
   retptr = __sync_fetch_and_add(&shminfo->shm_next, (void *)size);

   if (shminfo->shm_next > shminfo->shm_end) {
      // This memory request cannot fit in available space. Reset.
      __sync_fetch_and_sub(&shminfo->shm_next, (void *)size);

      platform_error_log(
         "[%s:%d::%s()]: Insufficient memory in shared segment"
         " to allocate %lu bytes for '%s'. Approx free space=%lu bytes."
         " shm_num_frags_tracked=%u, shm_num_frags_inuse=%u (HWM=%u).\n",
         file,
         line,
         func,
         size,
         objname,
         shminfo->shm_free_bytes,
         shminfo->shm_num_frags_tracked,
         shminfo->shm_num_frags_inuse,
         shminfo->shm_num_frags_inuse_HWM);

      // Trace diagnostics
      if (Trace_large_frags) {
         platform_shm_print_usage(hid);
      }

      return NULL;
   }
   // Track approx memory usage metrics; mainly for troubleshooting
   __sync_fetch_and_add(&shminfo->shm_used_bytes, size);
   __sync_fetch_and_sub(&shminfo->shm_free_bytes, size);

   if (size >= SHM_LARGE_FRAG_SIZE) {
      platform_shm_track_alloc(shminfo, retptr, size, func, line);
   }

   // Trace shared memory allocation; then return memory ptr.
   if (Trace_shmem || Trace_shmem_allocs
       || (Trace_large_frags && (size >= SHM_LARGE_FRAG_SIZE)))
   {
      platform_shm_trace_allocs(shminfo,
                                size,
                                "Allocated new fragment",
                                retptr,
                                objname,
                                func,
                                file,
                                line);
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
                     const int        line)
{
   void *retptr = splinter_shm_alloc(hid, newsize, "Unknown", file, line);
   if (retptr) {

      // Copy over old contents, if any, and free that memory piece
      if (oldptr) {
         memcpy(retptr, oldptr, oldsize);
         splinter_shm_free(hid, oldptr, oldsize, "Unknown", func, line);
      }
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
                         line,
                         func,
                         __func__,
                         ptr,
                         objname,
                         platform_shm_lop(hid),
                         platform_shm_hip(hid));
      return;
   }

   platform_shm_track_free(shminfo, ptr, size, objname, func, file, line);

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
 * platform_shm_track_alloc() - Track the allocation of this large fragment.
 * 'Tracking' here means we record this large-fragment in an array tracking
 * large-memory fragments allocated.
 * -----------------------------------------------------------------------------
 */
static void
platform_shm_track_alloc(shmem_info *shm,
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

   shm_lock_mem_frags(shm);

   // Iterate through the list of memory fragments being tracked.
   shm_frag_info *frag = shm->shm_mem_frags;
   for (int fctr = 0; fctr < ARRAY_SIZE(shm->shm_mem_frags); fctr++, frag++) {
      // If tracking entry points to a valid large-memory fragment ...
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

      // We found a free slot. Track our memory fragment at fctr'th slot.
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

      frag->shm_func = func;
      frag->shm_line = line;

      // The freed_by_pid/freed_by_tid == 0 means fragment is still allocated.

      // Track highest address of large-fragment that is being tracked.
      if (shm->shm_large_frag_hip < addr) {
         shm->shm_large_frag_hip = addr;
      }
      break;
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
 * platform_shm_track_free() - See if this memory fragment being freed is
 * already being tracked. If so, it's a large fragment allocation, which can be
 * re-cycled after this free. Do the book-keeping accordingly to record that
 * this large-fragment is no longer in-use and can be recycled.
 * -----------------------------------------------------------------------------
 */
static void
platform_shm_track_free(shmem_info  *shm,
                        void        *addr,
                        const size_t size,
                        const char  *objname,
                        const char  *func,
                        const char  *file,
                        const int    line)
{
   // All callers of either platform_free() or platform_free_mem() are required
   // to declare the size of the memory fragment being freed. We used that info
   // to manage free lists.
   /* FIXME: This is tripping in Pg-insert workloads.
   platform_assert((size > 0),
                  "objname=%s, func=%s, line=%d",
                  objname, func, line);
   */

   shm_lock_mem_frags(shm);
   shm->shm_nfrees++;
   shm->shm_bytes_freed += size;

   // If we are freeing a fragment beyond the high-address of all
   // large fragments tracked, then this is certainly not a large
   // fragment. So, no further need to see if it's a tracked fragment.
   if ((addr > shm->shm_large_frag_hip) || (size && size < SHM_LARGE_FRAG_SIZE))
   {
      shm->shm_nf_search_skipped++; // Track # of optimizations done

      if (size <= 32) {
         shm->shm_nfrees_le32++;
      } else if (size <= 64) {
         platform_shm_hook_free_frag(&shm->shm_free_le64, addr, size);
         shm->shm_nfrees_le64++;
      } else if (size <= 128) {
         platform_shm_hook_free_frag(&shm->shm_free_le128, addr, size);
         shm->shm_nfrees_le128++;
      } else if (size <= 256) {
         shm->shm_nfrees_le256++;
      } else if (size <= 512) {
         shm->shm_nfrees_le512++;
      } else if (size <= KiB) {
         shm->shm_nfrees_le1K++;
      } else if (size <= (2 * KiB)) {
         shm->shm_nfrees_le2K++;
      } else if (size <= (4 * KiB)) {
         shm->shm_nfrees_le4K++;
      }
      shm_unlock_mem_frags(shm);
      return;
   }

   shm->shm_nfrees_large_frags++;
   bool found_tracked_frag = FALSE;
   bool trace_shmem        = (Trace_shmem || Trace_shmem_frees);

   shm_frag_info *frag = shm->shm_mem_frags;
   for (int fctr = 0; fctr < ARRAY_SIZE(shm->shm_mem_frags); fctr++, frag++) {
      if (!frag->shm_frag_addr || (frag->shm_frag_addr != addr)) {
         continue;
      }
      found_tracked_frag = TRUE;

      // Cross-check the recording we did when fragment was allocated
      // We cannot check the tid as the parent process' tid may be 0.
      // We could have come across a free fragment that was previously
      // used by the parent process.
      // debug_assert(frag->shm_frag_allocated_to_tid != 0);
      debug_assert(frag->shm_frag_allocated_to_pid != 0);
      debug_assert(frag->shm_frag_size != 0);

      // Mark the fragment as in-use by recording the process/thread that's
      // doing the free.
      frag->shm_frag_freed_by_pid = getpid();
      frag->shm_frag_freed_by_tid = platform_get_tid();

      if (trace_shmem) {
         platform_default_log("OS-pid=%d, ThreadID=%lu"
                              ", Track freed fragment of size=%lu bytes"
                              ", at slot=%d, addr=%p"
                              ", allocated_to_pid=%d, allocated_to_tid=%lu"
                              ", shm_large_frag_hip=%p\n",
                              frag->shm_frag_freed_by_pid,
                              frag->shm_frag_freed_by_tid,
                              frag->shm_frag_size,
                              fctr,
                              addr,
                              frag->shm_frag_allocated_to_pid,
                              frag->shm_frag_allocated_to_tid,
                              shm->shm_large_frag_hip);
      }
      break;
   }
   shm_unlock_mem_frags(shm);

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
 * platform_shm_find_large() - Search the array of large-fragments being tracked
 * to see if there is an already allocated and now-free large memory fragment.
 * If so, allocate that fragment to this requester. Do the book-keeping
 * accordingly.
 * -----------------------------------------------------------------------------
 */
static void *
platform_shm_find_large(shmem_info *shm,
                        size_t      size,
                        const char *objname,
                        const char *func,
                        const char *file,
                        const int   line)
{
   debug_assert((size >= SHM_LARGE_FRAG_SIZE),
                "Incorrect usage of this interface for requested"
                " size=%lu bytes. Size should be >= %lu bytes.\n",
                size,
                SHM_LARGE_FRAG_SIZE);

   void          *retptr = NULL;
   shm_frag_info *frag   = shm->shm_mem_frags;
   int local_in_use      = 0; // Tracked while iterating in this fn, locally

   int  found_at_fctr      = -1;
   bool found_tracked_frag = FALSE;

   shm_lock_mem_frags(shm);

   uint32 shm_num_frags_tracked = shm->shm_num_frags_tracked;
   uint32 shm_num_frags_inuse   = shm->shm_num_frags_inuse;

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
      found_at_fctr      = fctr;

      // Record the process/thread to which free fragment is being allocated
      frag->shm_frag_allocated_to_pid = getpid();
      frag->shm_frag_allocated_to_tid = platform_get_tid();

      shm->shm_num_frags_inuse++;
      if (shm->shm_num_frags_inuse > shm->shm_num_frags_inuse_HWM) {
         shm->shm_num_frags_inuse_HWM = shm->shm_num_frags_inuse;
      }
      shm_num_frags_inuse = shm->shm_num_frags_inuse;

      // Now, mark that this fragment is in-use
      frag->shm_frag_freed_by_pid = 0;
      frag->shm_frag_freed_by_tid = 0;

      frag->shm_func = func;
      frag->shm_line = line;

      retptr = frag->shm_frag_addr;

      // Zero out the recycled large-memory fragment, just to be sure ...
      memset(retptr, 0, frag->shm_frag_size);
      break;
   }
   shm_unlock_mem_frags(shm);

   // Trace whether we found tracked fragment or not.
   if (Trace_shmem || Trace_shmem_allocs) {
      char msg[200];
      if (found_tracked_frag) {
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
                  found_at_fctr,
                  retptr,
                  shm_num_frags_tracked,
                  shm_num_frags_inuse,
                  local_in_use);
      } else {
         snprintf(msg,
                  sizeof(msg),
                  "Did not find free fragment of size=%lu bytes to reallocate."
                  " shm_num_frags_tracked=%u, shm_num_frags_inuse=%u"
                  " (local_in_use=%d)",
                  size,
                  shm_num_frags_tracked,
                  shm_num_frags_inuse,
                  local_in_use);
      }
      platform_shm_trace_allocs(
         shm, size, msg, retptr, objname, func, file, line);
   }
   return retptr;
}

/*
 * -----------------------------------------------------------------------------
 * platform_shm_find_frag() - Find a small free-fragment in a cached list of
 * free fragments that we track, for specific buckets of fragment sizes.
 * If one is found of the suitable size, detach it from the list and return
 * its start address. Otherwise, return NULL.
 * -----------------------------------------------------------------------------
 */
static void *
platform_shm_find_frag(shmem_info *shm,
                       size_t      size,
                       const char *objname,
                       const char *func,
                       const char *file,
                       const int   line)
{
   // Currently, we have only implemented tracking small free fragments of
   // 'known' sizes that appear in our workloads.
   if ((size <= 32) || (size > 128)) {
      return NULL;
   }

   free_frag_hdr **next_frag;
   if (size <= 64) {
      next_frag = &shm->shm_free_le64;
   } else {
      next_frag = &shm->shm_free_le128;
   }

   shm_lock_mem_frags(shm);
   // Find the next free frag which is big enough
   while (*next_frag && ((*next_frag)->free_frag_size < size)) {
      next_frag = &(*next_frag)->free_frag_next;
   }

   // If we ran thru the list of free fragments, we are done
   free_frag_hdr *retptr = *next_frag;
   if (!retptr) {
      shm_unlock_mem_frags(shm);
      return NULL;
   }
   *next_frag             = retptr->free_frag_next;
   retptr->free_frag_next = NULL;

   shm_unlock_mem_frags(shm);
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
platform_trace_large_frags(shmem_info *shm)
{
   int local_in_use    = 0; // Tracked while iterating in this fn, locally
   shm_frag_info *frag = shm->shm_mem_frags;

   const threadid tid = platform_get_tid();

   // Walk the tracked-fragments array looking for an in-use fragment
   for (int fctr = 0; fctr < ARRAY_SIZE(shm->shm_mem_frags); fctr++, frag++) {
      if (!frag->shm_frag_addr) {
         continue;
      }

      // Skip freed fragments.
      if (frag->shm_frag_freed_by_pid != 0) {
         continue;
      }
      // Found a large fragment that is still "in-use"
      local_in_use++;

      // Do -NOT- assert here. As this is a diagnostic routine, report
      // the inconsistency, and continue to find more stray large fragments.
      if (frag->shm_frag_freed_by_tid != 0) {
         platform_error_log("Invalid state found for fragment at index %d,"
                            "freed_by_pid=%d but freed_by_tid=%lu "
                            "(which should also be 0)\n",
                            fctr,
                            frag->shm_frag_freed_by_pid,
                            frag->shm_frag_freed_by_tid);
      }

      platform_error_log("  **** [TID=%lu] Fragment at slot=%d"
                         ", addr=%p, size=%lu (%s)"
                         ", func=%s, line=%d, is in-use"
                         ", allocated_to_pid=%d, allocated_to_tid=%lu\n",
                         tid,
                         fctr,
                         frag->shm_frag_addr,
                         frag->shm_frag_size,
                         size_str(frag->shm_frag_size),
                         frag->shm_func,
                         frag->shm_line,
                         frag->shm_frag_allocated_to_pid,
                         frag->shm_frag_allocated_to_tid);
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
platform_shm_heap_handle_valid(platform_heap_handle heap_handle)
{
   // Establish shared memory handles and validate input addr to shared
   // segment
   const void *shmaddr = (void *)heap_handle;

   // Use a cached copy in case we are dealing with a bogus input shmem
   // address.
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
                          const int    line)
{
   platform_default_log("  [OS-pid=%d,ThreadID=%lu, %s:%d::%s()] "
                        "-> %s: %s %lu bytes (%s)"
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
                        shminfo->shm_free_bytes,
                        size_str(shminfo->shm_free_bytes));
}
